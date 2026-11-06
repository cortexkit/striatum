//! Run continuation uses the admission allocator, with the previous selection's boundaries.
use super::*;
use cortexkit_role_routing::account_continue::{
    ContinueAnswer, ContinueHold, ContinueOffer, ContinueRefusal, ContinueRequest,
};
use cortexkit_role_routing::errors::Refusal;
use prefrontal_routing_store::V1Recorded;

fn refuse(code: &str, detail: &str) -> ContinueAnswer {
    ContinueAnswer::Refuse(ContinueRefusal::new(code, json!(detail)))
}

fn unavailable(error: impl std::fmt::Display) -> Refusal {
    Refusal::new(
        cortexkit_role_routing::errors::RefusalCode::AccountSelectionUnavailable,
        error.to_string(),
    )
}

fn standing(record: V1Recorded, request: &ContinueRequest) -> Result<Value, Refusal> {
    let first: ContinueRequest = serde_json::from_str(&record.body_json).map_err(unavailable)?;
    if !first.same_question(request) {
        return Err(Refusal::invalid_request(
            "selection_id",
            "selection_id already names a different request",
        ));
    }
    serde_json::from_str(&record.answer_json).map_err(unavailable)
}

pub(crate) fn continue_account(
    request: &ContinueRequest,
    principal: &str,
    runner: &str,
    project: Option<&str>,
    ctx: &Ctx<'_>,
) -> Result<Value, Refusal> {
    if let Some(first) = ctx
        .store
        .account_transaction(|tx| {
            tx.release_expired_continuations(ctx.now_ms)?;
            tx.continuation(principal, &request.selection_id)
        })
        .map_err(unavailable)?
    {
        return standing(first, request);
    }
    for (field, value) in [
        (
            "exhausted.observed_at_ms",
            Some(request.exhausted.observed_at_ms),
        ),
        ("exhausted.resets_at_ms", request.exhausted.resets_at_ms),
    ] {
        if value.is_some_and(|value| value > i64::MAX as u64) {
            return Err(Refusal::invalid_request(
                field,
                "timestamp exceeds the stored range",
            ));
        }
    }
    let previous = ctx
        .store
        .v1_decision(&request.previous_decision_id)
        .map_err(unavailable)?;
    let original = ctx
        .store
        .v1_admission(principal, &request.admission_id)
        .map_err(unavailable)?;
    let original: Option<Value> = original
        .map(|record| serde_json::from_str(&record.body_json))
        .transpose()
        .map_err(unavailable)?;
    let mut parsed = original
        .as_ref()
        .map(|body| {
            let mut body = body.clone();
            body["caller"] = json!({"runner":runner});
            body["model_decision_id"] = json!(request.previous_decision_id);
            body["mode"] = json!("admit");
            body["expected_pin"] = Value::Null;
            body["admission_id"] = json!(new_id("continue"));
            body["pool"] = json!(request.exhausted.pool);
            serde_json::from_value::<AccountForParams>(body)
        })
        .transpose()
        .map_err(unavailable)?;
    let prepared = parsed.as_ref().map(|params| prepare_roster(params, ctx));
    // Quota holds remove credentials before allocation. Keep their compatible roster
    // for wake times without reopening them in the roster that can receive an offer.
    let reset_roster = match (&ctx.classes, &parsed) {
        (Some(classes), Some(params)) => {
            let mut classes = classes.clone();
            classes
                .refusals
                .retain(|fence| !crate::credential_fences::is_billing_code(&fence.code));
            classes.route_fences.clear();
            let reset_ctx = Ctx {
                store: ctx.store,
                accounts: ctx.accounts,
                flags: ctx.flags,
                policy: ctx.policy,
                now_ms: ctx.now_ms,
                classes: Some(classes),
            };
            prepare_roster(params, &reset_ctx)
                .ok()
                .map(|(roster, _, _)| roster)
        }
        _ => None,
    };
    // Read quota state before taking the store writer lock, as ordinary observations do.
    let roster_facts: BTreeMap<_, _> = previous
        .iter()
        .map(|previous| {
            let roster = roster_for(ctx, &previous.provider).roster;
            (
                previous.provider.clone(),
                ProviderRosterFacts {
                    digest: roster
                        .as_ref()
                        .map(|r| r.view_digest.clone())
                        .unwrap_or_default(),
                    listed: roster
                        .iter()
                        .flat_map(|r| &r.accounts)
                        .flat_map(|a| &a.credentials)
                        .map(|c| c.credential_id.clone())
                        .collect(),
                    readings: roster
                        .iter()
                        .flat_map(|r| &r.accounts)
                        .filter_map(|a| Some((a.account_id.clone(), a.quota.clone()?)))
                        .collect(),
                },
            )
        })
        .collect();
    let body_json = serde_json::to_string(request).map_err(unavailable)?;
    ctx.store.account_transaction(|tx| {
        tx.release_expired_continuations(ctx.now_ms)?;
        if let Some(first) = tx.continuation(principal, &request.selection_id)? {
            return Ok(standing(first, request));
        }
        let mut reservation = None;
        let answer = (|| -> RouteStateResult<ContinueAnswer> {
            let Some(previous) = &previous else { return Ok(refuse("unknown_decision", "previous decision is unknown")); };
            let Some(original) = &original else { return Ok(refuse("unknown_decision", "admission is unknown")); };
            let Some(admitted) = tx.admission(principal, &request.admission_id)? else { return Ok(refuse("unknown_decision", "admission is unknown")); };
            let current_id = if original["decision_id"] == request.previous_decision_id {
                request.admission_id.clone()
            } else {
                let Some(id) = tx.continued_reservation(principal, &request.admission_id, &request.run_id, &request.previous_decision_id)? else {
                    return Ok(refuse("unknown_decision", "previous decision does not belong to the run"));
                };
                id
            };
            let Some(current) = tx.admission(principal, &current_id)? else {
                return Ok(refuse("unknown_decision", "previous decision does not belong to the run"));
            };
            let salt = tx.salt(ctx.now_ms, fresh_salt)?;
            if previous.runner_module != runner
                || project.is_some_and(|project| previous.project_id.as_deref() != Some(project))
                || admitted.run_id.as_deref() != Some(&request.run_id)
                || admitted.state != "started"
                || admitted.conversation_hash != conversation_hash(&salt, principal, &request.conversation_key)
                || previous.provider != current.provider || previous.model != current.model
                || current.credential_id != request.exhausted.credential_id || current.account_id != request.exhausted.account {
                return Ok(refuse("unknown_decision", "previous decision does not match the run"));
            }
            if tx.continuation_has_used_successor(principal,&request.admission_id,&request.run_id,&request.previous_decision_id)? {
                return Ok(refuse("unknown_decision", "previous decision is no longer current"));
            }
            // The stored admission authenticates the exhausted account. Record its evidence
            // through the same observation handler used by account_limit_exhausted reports.
            let raw = json!({"event_id":new_id("continue-event"), "caller":{"runner":runner},
                "kind":"account_limit_exhausted", "admission_id":current_id, "run_id":request.run_id,
                "served_account_id":request.exhausted.account, "credential_id":request.exhausted.credential_id,
                "limit_window":request.exhausted.limit_window, "pool":request.exhausted.pool,
                "resets_at_ms":request.exhausted.resets_at_ms, "retry_after_s":request.exhausted.retry_after_s,
                "observed_at_ms":request.exhausted.observed_at_ms});
            let observation: ObserveParams = serde_json::from_value(raw.clone()).map_err(|e|RouteStateStoreError(e.to_string()))?;
            let provider = &previous.provider;
            let policy = ctx.provider_policy(provider);
            let scoped = Ctx { policy:&policy, ..Ctx {store:ctx.store,accounts:ctx.accounts,flags:ctx.flags,policy:ctx.policy,now_ms:ctx.now_ms,classes:ctx.classes.clone()} };
            record_observation(tx,&scoped,principal,principal,false,&observation,&raw,&roster_facts)?;
            if !ctx.flags.enabled { return Ok(refuse("account_selection_disabled", "account selection is disabled")); }
            if original.get("requested_account").is_some_and(|value| !value.is_null()) {
                return Ok(refuse("account_pinned", "the caller pinned an account"));
            }
            let Some(classes) = ctx.classes.as_ref() else { return Ok(refuse("unknown_decision", "previous authentication method is unknown")); };
            let committed: Value = serde_json::from_str(&current.reply_json).map_err(|e|RouteStateStoreError(e.to_string()))?;
            let Some(method) = committed["account"]["method"].as_str().filter(|method|*method!="unknown") else {
                return Ok(refuse("unknown_decision", "previous authentication method is unknown"));
            };
            let Some(Ok((mut roster, route_fenced, _))) = prepared.clone() else {
                return Ok(ContinueAnswer::Hold(ContinueHold::new("account_selection_unavailable",json!("no compatible account is available"))));
            };
            let params = parsed.as_mut().unwrap();
            // The exhausted account must leave even if its quota reading still has headroom.
            if let Some(roster) = &mut roster.roster {
                for account in &mut roster.accounts {
                    account.credentials.retain(|c|classes.method(&c.credential_id)==method);
                }
                roster.accounts.retain(|a| a.account_id != current.account_id && !a.credentials.is_empty());
            }
            refresh_blocks(tx,provider,roster.roster.as_ref().map_or(&[],|r|r.accounts.as_slice()),ctx.now_ms,&policy)?;
            let fences = load_fences(tx,provider)?;
            let family = policy.family_for_model(&previous.model);
            let pool = params.request(principal,false).pool;
            let candidates=params.request(principal,false).candidates;
            let compatible:Vec<_>=reset_roster.iter().filter_map(|r|r.roster.as_ref()).flat_map(|r|&r.accounts)
                .filter(|a|a.account_id!=current.account_id).flat_map(|a|a.credentials.iter().map(move|c|(a,c)))
                .filter(|(a,c)|classes.method(&c.credential_id)==method && candidates.as_ref().is_none_or(|candidates|candidates.contains(&a.account_id,&c.credential_id))).collect();
            let mut until = route_fenced.iter().filter(|(a,_)|compatible.iter().any(|(candidate,_)|candidate.account_id==**a)).map(|(_,at)|*at).filter(|at|*at>ctx.now_ms).min();
            for (_,credential) in compatible {
                if let Some(at)=classes.quota_hold_reset(provider,&credential.credential_id) {
                    until=Some(until.map_or(at,|old|old.min(at)));
                }
            }
            if let Some(roster) = &mut roster.roster {
                roster.accounts.retain(|a| {
                    let held: Vec<_> = fences.iter().filter(|f|f.account_id==a.account_id && f.applies_to(&previous.model,family,pool.as_deref())).collect();
                    for at in held.iter().filter_map(|f|f.reset.at_ms()).filter(|at|*at>ctx.now_ms) {
                        until=Some(until.map_or(at,|old|old.min(at)));
                    }
                    held.is_empty()
                });
            }
            // Class/service-tier restrictions were prepared using the previous decision.
            // Bind the offered account only after creating its model decision, so the
            // previous decision keeps the credential and account that actually served it.
            params.model_decision_id = None;
            let result = serve(tx,&scoped,principal,params,provider,&roster,&fingerprint(params))?;
            match result {
                OpReply::Result(result) => {
                    let id = result["reason"]["decision_id"].as_str().unwrap();
                    let row = tx.admission(principal,&params.admission_id)?.unwrap();
                    tx.link_continued_decision(&request.previous_decision_id,id,ctx.now_ms,row.admission_valid_until)?;
                    tx.bind_model_account(id,provider,&previous.model,&row.account_id,&row.credential_id)?;
                    reservation=Some((id.to_owned(),params.admission_id.clone(),row.admission_valid_until));
                    Ok(ContinueAnswer::Offer(ContinueOffer::new(row.credential_id,row.account_id,id,row.admission_valid_until.max(0) as u64)))
                }
                OpReply::Error {code,detail,..} => {
                    if let Some(at) = detail.as_ref().and_then(|d|d["reset_at_ms"].as_i64()).filter(|at|*at>ctx.now_ms) {
                        until=Some(until.map_or(at,|old|old.min(at)));
                    }
                    let mut hold=ContinueHold::new(code,json!("no compatible account is available"));
                    hold.until_ms=until.map(|at|at as u64);
                    Ok(ContinueAnswer::Hold(hold))
                }
            }
        })()?;
        let answer_json=serde_json::to_string(&answer).map_err(|e|RouteStateStoreError(e.to_string()))?;
        tx.record_continuation(principal,&request.selection_id,&V1Recorded {body_json:body_json.clone(),answer_json:answer_json.clone()},
            &request.run_id,&request.admission_id,&request.previous_decision_id,reservation.as_ref().map(|(id,res,until)|(id.as_str(),res.as_str(),*until)))?;
        Ok(serde_json::from_str(&answer_json).map_err(unavailable))
    }).map_err(unavailable)?
}
