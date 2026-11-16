//! Forks reuse a granted parent's credential instead of scoring a new account.

use super::*;

pub(super) fn hold_detail(params: &AccountForParams, mut detail: Value) -> Value {
    if params.fork_of.is_some() {
        detail["reason"] = json!("fork_parent_account_unavailable");
    }
    detail
}

pub(super) fn prepare_and_admit(
    mut parsed: AccountForParams,
    principal: &str,
    ctx: &Ctx<'_>,
    provider: String,
    request_fingerprint: String,
    finish: &mut Option<crate::routing_v1::accounts::AdmissionRecord>,
) -> Result<OpReply, OpReply> {
    // Keep unavailable credentials in this roster. The allocator must hold on
    // the parent's exact binding, not silently choose a healthy sibling.
    let mut roster = match &ctx.classes {
        Some(classes) => {
            let accounts = classes
                .facts
                .iter()
                .find(|fact| {
                    fact.provider_id == provider && fact.model_id == normalize(&parsed.model)
                })
                .into_iter()
                .flat_map(|fact| {
                    classes.credentials.listed_credentials().iter().map(|row| {
                        let mut view = classes.view(row, fact);
                        view.credentials[0].needs_reauth = row.state != "active";
                        view
                    })
                })
                .collect();
            RosterBuild {
                roster: Some(classes.roster(accounts)),
                context: json!({}),
            }
        }
        None => roster_for_model(ctx, &provider, Some(&normalize(&parsed.model))),
    };
    parsed.service_tier_chosen = prepare_service_tier(
        &mut parsed,
        ctx,
        &mut roster,
        striatum_core::accounts::AllowedAccountClass::NotPayPerToken,
        None,
    )?;
    parsed.continuation_input = Some(continuation::Inputs::prepare(
        &parsed,
        principal,
        ctx.classes.as_ref(),
        striatum_core::accounts::AllowedAccountClass::NotPayPerToken,
        None,
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
) -> RouteStateResult<Option<striatum_core::accounts::policy::ForkParent>> {
    let Some(fork) = &params.fork_of else {
        return Ok(None);
    };
    // Live parents and their latest used continuation are read from memory.
    // Closed parents need one indexed admission lookup, plus one indexed
    // used-binding lookup only when the run moved to another account.
    let Some(parent) = tx.fork_parent(principal, &fork.admission_id, provider)? else {
        return Ok(None);
    };
    let blocked_until_ms = tx
        .fence_rechecks()?
        .into_iter()
        .filter(|origin| {
            origin.kind != "account"
                && origin.kind != "principal"
                && origin.applies(
                    provider,
                    &parent.account_id,
                    &parent.credential_id,
                    &params.model,
                )
                && origin.due_at() > now_ms()
        })
        .map(|origin| origin.due_at())
        .max();
    Ok(Some(striatum_core::accounts::policy::ForkParent {
        admission_id: fork.admission_id.clone(),
        account_id: parent.account_id,
        credential_id: parent.credential_id,
        pin_id: parent.pin_id,
        pin_generation: parent.pin_generation,
        blocked_until_ms,
    }))
}
