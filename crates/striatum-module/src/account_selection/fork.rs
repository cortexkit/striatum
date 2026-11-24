//! Forks reuse a granted parent's credential instead of scoring a new account.

use super::*;
use striatum_core::accounts::decision::ForkNote;
use striatum_core::accounts::{policy::ForkParent, Decision, Hold, HoldBasis, Outcome};

pub(super) fn hold_detail(params: &AccountForParams, mut detail: Value) -> Value {
    if params.fork_of.is_some() {
        detail["reason"] = json!("fork_parent_account_unavailable");
    }
    detail
}

/// Why the child's own decision can never use a listed login.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Unqualified {
    /// Outside the decision's account class, or not serving the model.
    Class,
    /// The account's plan doesn't reach the requested model.
    ModelReach,
    /// Outside the decision's bound service tier or its allowed logins.
    Tier,
}

/// A fork is refused, not held, when its parent's exact login can never
/// serve the child's decision: waiting would not change the answer, and a
/// fork never moves to another login.
pub(super) fn unqualified_refusal(
    params: &AccountForParams,
    parent: &ForkParent,
) -> Option<OpReply> {
    use cortexkit_role_routing::errors::AccountUnservableReason;
    let why = params.fork_unqualified.get(&parent.credential_id)?;
    checking("fork_parent_unqualified");
    let reason = match why {
        Unqualified::Tier => AccountUnservableReason::BoundTierUnavailable,
        Unqualified::Class | Unqualified::ModelReach => AccountUnservableReason::NoEligibleAccount,
    };
    Some(unservable_refusal(
        reason,
        vec![parent.credential_id.clone()],
        Vec::new(),
    ))
}

pub(super) fn prepare_and_admit(
    mut parsed: AccountForParams,
    principal: &str,
    ctx: &Ctx<'_>,
    provider: String,
    request_fingerprint: String,
    finish: &mut Option<crate::routing_v1::accounts::AdmissionRecord>,
) -> Result<OpReply, OpReply> {
    let (class, selection) = decision_class(&parsed, ctx)?;
    let priced = class == striatum_core::accounts::AllowedAccountClass::PricedPerTokenOnly;
    let bound = BoundTier::read(&parsed, ctx, &provider, selection.as_ref())?;
    let model = normalize(&parsed.model);
    let mut unqualified = BTreeMap::new();
    // Keep unavailable credentials in this roster. The allocator must hold on
    // the parent's exact binding, not silently choose a healthy sibling.
    // Logins the child's decision can never use are left out and remembered,
    // so a parent on one of them is refused instead.
    let mut roster = match &ctx.classes {
        Some(classes) => {
            let mut accounts = Vec::new();
            if let Some(fact) = classes
                .facts
                .iter()
                .find(|fact| fact.provider_id == provider && fact.model_id == model)
            {
                for row in classes.credentials.listed_credentials() {
                    let mut view = classes.view(row, fact);
                    let unavailable = row.state != "active" || row.condition.is_some();
                    // A signed-out or conditioned login isn't in the serving
                    // index at all, so its class can't be judged until it is
                    // usable again; until then the allocator holds on it.
                    let why = if !unavailable && !classes.serves_decision_class(row, fact, priced) {
                        Some(Unqualified::Class)
                    } else if !unavailable && !view.model_reach.reaches(&model) {
                        Some(Unqualified::ModelReach)
                    } else if bound.as_ref().is_some_and(|bound| !bound.supports(&row.id)) {
                        Some(Unqualified::Tier)
                    } else {
                        None
                    };
                    if let Some(why) = why {
                        unqualified.insert(row.id.clone(), why);
                        continue;
                    }
                    view.credentials[0].needs_reauth = unavailable;
                    accounts.push(view);
                }
            }
            RosterBuild {
                roster: Some(classes.roster(accounts)),
                context: json!({}),
            }
        }
        None => {
            let mut roster = roster_for_model(ctx, &provider, Some(&model));
            if let (Some(bound), Some(roster)) = (&bound, &mut roster.roster) {
                for account in &mut roster.accounts {
                    account.credentials.retain(|credential| {
                        let supported = bound.supports(&credential.credential_id);
                        if !supported {
                            unqualified.insert(credential.credential_id.clone(), Unqualified::Tier);
                        }
                        supported
                    });
                }
                roster
                    .accounts
                    .retain(|account| !account.credentials.is_empty());
            }
            roster
        }
    };
    parsed.fork_unqualified = unqualified;
    parsed.service_tier_chosen = prepare_service_tier(&mut parsed, ctx, &mut roster, class, None)?;
    parsed.continuation_input = Some(continuation::Inputs::prepare(
        &parsed,
        principal,
        ctx.classes.as_ref(),
        class,
        selection.as_ref(),
    ));
    let command_ctx = ctx.command_context();
    let principal = principal.to_owned();
    let finish = finish.take();
    Ok(crate::account_writer::execute_with_error(
        ctx.store,
        striatum_store::AccountCommandKind::Admit,
        move |tx| {
            tx.release_expired(command_ctx.now_ms)?;
            tx.release_expired_continuations(command_ctx.now_ms)?;
            let reply = serve(
                tx,
                &command_ctx,
                &principal,
                &parsed,
                &provider,
                &roster,
                &request_fingerprint,
            )?;
            finish_reply(tx, reply, finish.as_ref(), command_ctx.now_ms)
        },
    )
    .unwrap_or_else(admission_error))
}

pub(super) fn resolve(
    tx: &crate::account_state::AccountCommand<'_>,
    principal: &str,
    params: &AccountForParams,
    provider: &str,
) -> RouteStateResult<Option<ForkParent>> {
    let Some(fork) = &params.fork_of else {
        return Ok(None);
    };
    // Live parents and their latest used continuation are read from memory.
    // Closed parents need one indexed admission lookup, plus one indexed
    // used-binding lookup only when the run moved to another account.
    let Some(parent) = tx.fork_parent(principal, &fork.admission_id, provider)? else {
        return Ok(None);
    };
    Ok(Some(ForkParent {
        admission_id: fork.admission_id.clone(),
        account_id: parent.account_id,
        credential_id: parent.credential_id,
        pin_id: parent.pin_id,
        pin_generation: parent.pin_generation,
        blocked_until_ms: None,
    }))
}

pub(super) fn apply_blocking_origins(
    parent: &mut ForkParent,
    origins: &[striatum_store::FenceRecheck],
    provider: &str,
    params: &AccountForParams,
    now: i64,
) -> Option<i64> {
    let mut claimed = false;
    parent.blocked_until_ms = origins
        .iter()
        .filter(|origin| {
            origin.kind != "account"
                && origin.kind != "principal"
                && origin.applies(
                    provider,
                    &parent.account_id,
                    &parent.credential_id,
                    &params.model,
                )
                && origin.due_at() > now
        })
        .map(|origin| {
            claimed |= origin.last_claimed_at.is_some();
            origin.due_at()
        })
        .max();
    // A fork blocked by an already-claimed origin needs its retry clock,
    // not the generic short retry for an unavailable parent.
    parent.blocked_until_ms.filter(|_| claimed)
}

pub(super) fn hold_after_lost_probe(
    mut decision: Decision,
    parent: &ForkParent,
    until: i64,
    now: i64,
) -> Decision {
    let seconds = (until.saturating_sub(now).max(0) as u64).div_ceil(1_000);
    decision.outcome = Outcome::Hold(Hold {
        account_id: parent.account_id.clone(),
        credential_id: parent.credential_id.clone(),
        pin_id: parent.pin_id.clone(),
        pin_generation: parent.pin_generation,
        reset_at_ms: until,
        base_retry_after_s: seconds,
        retry_after_s: seconds,
        basis: HoldBasis::ForkParentUnavailable,
    });
    if !matches!(decision.explanation.fork_of, Some(ForkNote::Refused { .. })) {
        decision.explanation.fork_of = Some(ForkNote::Refused {
            admission_id: parent.admission_id.clone(),
            reason: "refusal_backed_block".into(),
        });
    }
    decision
}
