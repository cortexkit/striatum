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
//!
//! A free slot is not always room. A conversation refused for concurrency
//! becomes a waiter, and a request may not take a free slot that a waiter
//! ranked ahead of it needs: such an account counts as full for that request.

use super::*;
use crate::account_state::{CapCount, InflightLedger, InflightScope, Waiter};
use cortexkit_role_routing::declarations::ConsumerPriority;

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
    /// The request's priority class, resolved once and only when a waiter
    /// makes it matter.
    class: std::cell::Cell<Option<ConsumerPriority>>,
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
            class: std::cell::Cell::new(None),
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
        // A conversation already counted in this cap's scope needs no new
        // slot, so neither the cap nor a waiter can block its next step.
        if tx.inflight_conversation(
            caps,
            provider,
            account,
            &params.model,
            principal,
            hash,
            ctx.now_ms,
        ) {
            return Check::Room;
        }
        let count = tx.inflight_count_for_model(caps, provider, account, &params.model, ctx.now_ms);
        // A head call (a head's own turn: head-scoped route, or `head_turn`
        // from an enrolled runner) is never held back for a waiter.
        if (count as u64) < cap
            && (self.head_call
                || !self.held_for_waiters(
                    tx, ctx, params, principal, hash, provider, account, count, cap,
                ))
        {
            return Check::Room;
        }
        let kept = params.fork_of.is_some()
            || matches!(admission.affinity, AffinityOutcome::Retained { .. });
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
    /// a full account because the check runs before the claim. The refused
    /// conversation becomes a waiter in the working state, which is memory
    /// only.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn refuse(
        &self,
        tx: &mut crate::account_state::AccountCommand<'_>,
        ctx: &CommandContext,
        params: &AccountForParams,
        principal: &str,
        hash: &str,
        provider: &str,
        decision: &Decision,
        roster: &RosterBuild,
    ) -> RouteStateResult<OpReply> {
        let now = ctx.now_ms;
        let caps = &ctx.concurrency_caps;
        let ceiling = now.saturating_add(RESET_CEILING_MS);
        let reset_at_ms = match self.waiting_ahead(tx, ctx, params, principal, hash) {
            Some(ahead) if !ahead.is_empty() => {
                self.free_with_waiters(tx, ctx, params, provider, &ahead, ceiling)
            }
            _ => self
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
                .fold(ceiling, i64::min),
        };
        // Head calls never reach this refusal (see `check`). The waiter
        // waits on every count it was refused on: the kept account alone, or
        // each eligible account of the route.
        let mut counts: Vec<_> = self
            .full
            .iter()
            .map(|full| cap_count(caps, provider, &full.account_id, &params.model))
            .collect();
        counts.sort();
        counts.dedup();
        let class = self.class(ctx, params);
        tx.record_waiter(
            (principal.to_owned(), hash.to_owned()),
            class,
            reset_at_ms,
            counts,
            now,
        );
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
        if let Some(parent) = &params.fork_of {
            explained["explanation"]["fork_of"] = json!({"kind":"refused", "admission_id":parent.admission_id, "reason":"concurrency_full"});
        }
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

    /// The request's class: that of the consumer on the decision it is bound
    /// to, or `worker` when it is bound to none.
    fn class(&self, ctx: &CommandContext, params: &AccountForParams) -> ConsumerPriority {
        if let Some(class) = self.class.get() {
            return class;
        }
        let class = params
            .call
            .as_ref()
            .map_or_else(ConsumerPriority::default, |call| call.class(ctx));
        self.class.set(Some(class));
        class
    }

    /// The live waiters ranked ahead of this request, or `None` without
    /// module state.
    fn waiting_ahead(
        &self,
        tx: &crate::account_state::AccountCommand<'_>,
        ctx: &CommandContext,
        params: &AccountForParams,
        principal: &str,
        hash: &str,
    ) -> Option<Vec<Waiter>> {
        let (_, waiters) = tx.concurrency_view()?;
        let conversation = (principal.to_owned(), hash.to_owned());
        // Resolving the class reads the consumer registry: skip it when no
        // other conversation is waiting.
        if waiters
            .live(ctx.now_ms)
            .all(|(other, _)| *other == conversation)
        {
            return Some(Vec::new());
        }
        let class = self.class(ctx, params);
        Some(
            waiters
                .ahead(&conversation, class, ctx.now_ms)
                .into_iter()
                .cloned()
                .collect(),
        )
    }

    /// Whether the free slot this request would take on `account` is needed
    /// by the waiters ranked ahead of it.
    #[allow(clippy::too_many_arguments)]
    fn held_for_waiters(
        &self,
        tx: &crate::account_state::AccountCommand<'_>,
        ctx: &CommandContext,
        params: &AccountForParams,
        principal: &str,
        hash: &str,
        provider: &str,
        account: &str,
        count: usize,
        cap: u64,
    ) -> bool {
        let Some(ahead) = self.waiting_ahead(tx, ctx, params, principal, hash) else {
            return false;
        };
        if ahead.is_empty() {
            return false;
        }
        let Some((ledger, _)) = tx.concurrency_view() else {
            return false;
        };
        let caps = &ctx.concurrency_caps;
        let request = cap_count(caps, provider, account, &params.model);
        let ahead: Vec<_> = ahead.iter().collect();
        let free = (cap as usize).saturating_sub(count);
        !takes(ledger, caps, &request, free, &ahead, ctx.now_ms)
    }

    /// The earliest instant before `ceiling` at which, with no further
    /// command, this request would find a slot it may take on one of the
    /// counts it was refused on. Counts fall only when an entry closes and
    /// waiters only end, so those are the instants to look at, each judged
    /// with the waiters still live then.
    fn free_with_waiters(
        &self,
        tx: &crate::account_state::AccountCommand<'_>,
        ctx: &CommandContext,
        params: &AccountForParams,
        provider: &str,
        ahead: &[Waiter],
        ceiling: i64,
    ) -> i64 {
        let Some((ledger, _)) = tx.concurrency_view() else {
            return ceiling;
        };
        let now = ctx.now_ms;
        let caps = &ctx.concurrency_caps;
        let requests: Vec<_> = self
            .full
            .iter()
            .map(|full| {
                (
                    cap_count(caps, provider, &full.account_id, &params.model),
                    full.cap,
                )
            })
            .collect();
        let mut instants: Vec<i64> = requests
            .iter()
            .map(|(count, _)| count)
            .chain(ahead.iter().flat_map(|waiter| &waiter.counts))
            .flat_map(|count| {
                ledger.deadlines(caps, &count.provider, &count.account, scope(count), now)
            })
            .chain(ahead.iter().map(Waiter::ends_at))
            .filter(|at| *at > now && *at < ceiling)
            .collect();
        instants.sort_unstable();
        instants.dedup();
        instants
            .into_iter()
            .find(|at| {
                let live: Vec<_> = ahead.iter().filter(|waiter| waiter.live(*at)).collect();
                requests.iter().any(|(count, cap)| {
                    let free = (*cap as usize).saturating_sub(count_at(ledger, caps, count, *at));
                    takes(ledger, caps, count, free, &live, *at)
                })
            })
            .unwrap_or(ceiling)
    }

    /// The count an admission of this request on `account` takes a slot on,
    /// if the conversation does not already hold a slot there. A request that
    /// uses an existing slot ends no wait.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn slot_taken(
        &self,
        tx: &crate::account_state::AccountCommand<'_>,
        ctx: &CommandContext,
        params: &AccountForParams,
        principal: &str,
        hash: &str,
        provider: &str,
        account: &str,
    ) -> Option<CapCount> {
        let caps = &ctx.concurrency_caps;
        let holds_slot = tx.inflight_conversation(
            caps,
            provider,
            account,
            &params.model,
            principal,
            hash,
            ctx.now_ms,
        );
        (!holds_slot).then(|| cap_count(caps, provider, account, &params.model))
    }
}

/// The cap count a request for `model` adds to on `account`.
fn cap_count(
    caps: &crate::concurrency_caps::ResolvedCaps,
    provider: &str,
    account: &str,
    model: &str,
) -> CapCount {
    let model = normalize(model);
    CapCount {
        provider: provider.to_owned(),
        account: account.to_owned(),
        model: match caps.cap_count(provider, account, &model) {
            InflightScope::Account => None,
            InflightScope::AccountModel(model) => Some(model.to_owned()),
        },
    }
}

fn scope(count: &CapCount) -> InflightScope<'_> {
    count
        .model
        .as_deref()
        .map_or(InflightScope::Account, InflightScope::AccountModel)
}

/// The cap of a count; a shared count is capped by the account's default.
fn limit(caps: &crate::concurrency_caps::ResolvedCaps, count: &CapCount) -> Option<u64> {
    match &count.model {
        Some(model) => caps.cap(&count.provider, &count.account, model).limit,
        None => {
            let cap = caps.default_cap(&count.provider, &count.account);
            (cap.scope == crate::concurrency_caps::CapScope::Account)
                .then_some(cap.limit)
                .flatten()
        }
    }
}

fn count_at(
    ledger: &InflightLedger,
    caps: &crate::concurrency_caps::ResolvedCaps,
    count: &CapCount,
    at: i64,
) -> usize {
    ledger.count_for_scope(caps, &count.provider, &count.account, scope(count), at)
}

/// Whether a request may take one of the `free` slots on `request` at `at`:
/// there is one, and taking it leaves as many of the waiters `ahead` able to
/// hold distinct free slots of their own as before.
fn takes(
    ledger: &InflightLedger,
    caps: &crate::concurrency_caps::ResolvedCaps,
    request: &CapCount,
    free: usize,
    ahead: &[&Waiter],
    at: i64,
) -> bool {
    if free == 0 {
        return false;
    }
    if ahead.is_empty() {
        return true;
    }
    let mut slots = BTreeMap::new();
    slots.insert(request.clone(), free);
    for count in ahead.iter().flat_map(|waiter| &waiter.counts) {
        slots
            .entry(count.clone())
            .or_insert_with(|| match limit(caps, count) {
                Some(cap) => (cap as usize).saturating_sub(count_at(ledger, caps, count, at)),
                // An uncapped count always has a slot for every waiter.
                None => ahead.len(),
            });
    }
    let before = crate::account_state::matched_waiters(ahead, &slots);
    *slots.get_mut(request).expect("inserted above") -= 1;
    crate::account_state::matched_waiters(ahead, &slots) == before
}
