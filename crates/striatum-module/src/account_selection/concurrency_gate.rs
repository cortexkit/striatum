//! Concurrency caps at admission. The check runs inside the account-writer
//! command that inserts the reservation, so the counts it reads hold every
//! earlier command of the same writer batch and two requests can never both
//! take an account's last slot.
//!
//! The allocator's own answers come first. A full account is taken out of the
//! roster the allocator chooses from, the way the probe loop takes out a
//! credential whose probe another request holds, and the allocator chooses
//! again. A request kept on its affinity account is never moved by this: it is
//! refused instead. Only when every account the allocator would admit is full
//! is the request refused `concurrency_full`; a request no account would admit
//! with the ledger ignored keeps its existing answer.

use super::*;

/// Retry time when no count is known to fall sooner. Runs report no expected
/// length, so a slot can free at any moment through a finish.
const RESET_CEILING_MS: i64 = 30_000;

pub(super) enum Check {
    /// Allocate on the chosen account as today.
    Room,
    /// The account is full and was taken out of the roster: choose again.
    Next,
    /// The request is refused `concurrency_full`.
    Full,
}

struct FullAccount {
    account_id: String,
    count: usize,
    cap: u64,
}

pub(super) struct Gate {
    enforce: bool,
    head_call: bool,
    /// A head call that found every account full is placed as if the ledger
    /// were ignored, over the cap.
    ignore: bool,
    refused: bool,
    removed: Vec<(String, Vec<CredentialRecord>)>,
    full: Vec<FullAccount>,
}

impl Gate {
    pub(super) fn new(ctx: &CommandContext, params: &AccountForParams) -> Self {
        Self {
            enforce: ctx
                .classes
                .as_ref()
                .is_some_and(|classes| classes.config.concurrency_caps().enforcement_enabled),
            head_call: params.call.as_ref().is_some_and(|call| call.head_call),
            ignore: false,
            refused: false,
            removed: Vec::new(),
            full: Vec::new(),
        }
    }

    /// With enforcement off the would-refuse line still describes the request;
    /// with it on, a refusal writes its own line instead.
    pub(super) fn enforced(&self) -> bool {
        self.enforce
    }

    pub(super) fn refused(&self) -> bool {
        self.refused
    }

    /// Decide whether the allocator's chosen account may take the request.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn check(
        &mut self,
        tx: &mut crate::account_state::AccountCommand<'_>,
        ctx: &CommandContext,
        params: &AccountForParams,
        principal: &str,
        hash: &str,
        provider: &str,
        admission: &striatum_core::accounts::Admission,
        state: &mut AllocatorState,
    ) -> Check {
        if !self.enforce || self.ignore {
            return Check::Room;
        }
        let caps = &ctx.concurrency_caps;
        let account = &admission.account_id;
        let Some(cap) = caps.cap(provider, account, &params.model).limit else {
            return Check::Room;
        };
        // A renewal of a conversation that already holds a slot in this cap
        // count adds no hold, so it is never checked against the cap.
        if params.mode == RequestMode::Renew
            && tx.inflight_conversation(
                caps,
                provider,
                account,
                &params.model,
                principal,
                hash,
                ctx.now_ms,
            )
        {
            return Check::Room;
        }
        let count = tx.inflight_count_for_model(caps, provider, account, &params.model, ctx.now_ms);
        if (count as u64) < cap {
            return Check::Room;
        }
        let kept = matches!(admission.affinity, AffinityOutcome::Retained { .. });
        // A head call is never refused, and it stays on its affinity account.
        if kept && self.head_call {
            return Check::Room;
        }
        let full = FullAccount {
            account_id: account.clone(),
            count,
            cap,
        };
        if kept {
            // Concurrency never moves a conversation: only its own account's
            // count decides, whatever room a sibling has.
            self.full = vec![full];
            self.refused = true;
            return Check::Full;
        }
        self.full.push(full);
        if let Some(roster) = &mut state.roster {
            for view in roster
                .accounts
                .iter_mut()
                .filter(|view| view.account_id == *account)
            {
                self.removed.push((
                    view.account_id.clone(),
                    std::mem::take(&mut view.credentials),
                ));
            }
        }
        Check::Next
    }

    /// The allocator admitted nowhere after full accounts were taken out.
    /// Returns true when the caller should choose again: a head call goes back
    /// to the full accounts and is admitted over the cap.
    pub(super) fn exhausted(&mut self, state: &mut AllocatorState) -> bool {
        if self.full.is_empty() || self.ignore || self.refused {
            return false;
        }
        if !self.head_call {
            self.refused = true;
            return false;
        }
        self.ignore = true;
        if let Some(roster) = &mut state.roster {
            for (account, credentials) in self.removed.drain(..) {
                if let Some(view) = roster
                    .accounts
                    .iter_mut()
                    .find(|view| view.account_id == account)
                {
                    view.credentials = credentials;
                }
            }
        }
        true
    }

    /// Answer the refusal. Its only store write is the decision row: no
    /// admission, hold, fence or affinity change, and no probe was claimed for
    /// a full account because the check runs before the claim.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn refuse(
        &self,
        tx: &mut crate::account_state::AccountCommand<'_>,
        ctx: &CommandContext,
        params: &AccountForParams,
        principal: &str,
        provider: &str,
        decision: &Decision,
        roster: &RosterBuild,
    ) -> RouteStateResult<OpReply> {
        let now = ctx.now_ms;
        let caps = &ctx.concurrency_caps;
        let ceiling = now.saturating_add(RESET_CEILING_MS);
        let reset_at_ms = self
            .full
            .iter()
            .filter_map(|full| {
                tx.inflight_next_free_at(
                    caps,
                    provider,
                    &full.account_id,
                    &params.model,
                    full.cap,
                    now,
                )
            })
            .fold(ceiling, i64::min);
        let mut accounts: Vec<_> = self
            .full
            .iter()
            .map(|full| json!({"account_id":full.account_id,"count":full.count,"cap":full.cap}))
            .collect();
        accounts.sort_by(|a, b| a["account_id"].as_str().cmp(&b["account_id"].as_str()));
        let decision_id = new_id("acd");
        let mut record = decision_record(RecordInput {
            decision_id: &decision_id,
            mode: DECISION_MODE_SERVE,
            trigger: "request",
            principal,
            admission_id: Some(&params.admission_id),
            request_mode: Some(params.mode.as_str()),
            decision,
            context: json!({
                "roster": roster.context,
                "concurrency_full": {"accounts": accounts, "reset_at_ms": reset_at_ms},
            }),
            now_ms: now,
        });
        // The allocator has no concurrency code of its own: the decision it
        // made is kept for the explanation, and its outcome is the refusal.
        record.outcome = "refuse".into();
        record.code = "concurrency_full".into();
        record.account_id = None;
        record.credential_id = None;
        let mut explained = wire(decision);
        explained["outcome"] = json!({
            "kind": "refuse", "code": "concurrency_full", "retryable": true,
            "reset_at_ms": reset_at_ms,
        });
        record.decision_json = explained.to_string();
        tx.insert_decision(&record)?;
        if let Some(call) = &params.call {
            call.refused(ctx, &params.model, accounts);
        }
        let retry_after_s = (reset_at_ms.saturating_sub(now).max(0) as u64).div_ceil(1_000);
        Ok(because(
            "concurrency_full",
            OpReply::typed(
                "concurrency_full",
                "no account admitted: concurrency_full",
                json!({
                    "code": "concurrency_full",
                    "retry_after_s": retry_after_s,
                    "decision_id": decision_id,
                    "exclusions": [],
                    "reset_at_ms": reset_at_ms,
                }),
            ),
        ))
    }
}
