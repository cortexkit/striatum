//! Post-admission reports and releases.
//! These operations update an existing admission or its evidence rather than choose an account.

use super::*;

/// Return the selected identity's actual vault adapter. Catalog vendors and
/// refresh adapters are different namespaces and must not be interchanged.
pub(super) fn refresh_adapter_for(roster: &RosterBuild, account_id: &str) -> Option<String> {
    roster
        .roster
        .as_ref()?
        .accounts
        .iter()
        .find(|view| view.account_id == account_id)
        .map(|view| view.refresh_adapter.clone())
}

fn tracked_providers(ctx: &Ctx<'_>) -> BTreeSet<String> {
    ctx.accounts
        .and_then(|state| state.lock().ok())
        .map(|state| {
            state
                .readings
                .readings
                .values()
                .filter_map(|reading| reading.routing_provider().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// route.account_observe
// ---------------------------------------------------------------------------

/// `route.account_observe` params. The whole params object is also kept
/// verbatim with the observation.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct ObserveParams {
    /// Broca uses `<lineage_id>:<wal_seq>`, where `wal_seq` is the write-ahead
    /// log sequence number. A repeat from the same runner and principal changes nothing.
    pub(super) event_id: String,
    pub(super) caller: CallerParams,
    /// `admission_started`, `run_finished`, `attempt`, `scoped_not_found`,
    /// `account_limit_exhausted`, `transient_backoff` or `credential_refused`.
    /// An unlisted kind is stored and answered but changes no routing state.
    pub(super) kind: String,
    #[serde(default)]
    pub(super) admission_id: Option<String>,
    #[serde(default)]
    pub(super) run_id: Option<String>,
    /// The account that actually served, when the runner knows it.
    #[serde(default)]
    served_account_id: Option<String>,
    #[serde(default)]
    pub(super) credential_id: Option<String>,
    /// The vault view digest at the attempt's credential read.
    #[serde(default)]
    pub(super) view_digest: Option<String>,
    /// The original observation time, never a forwarding time.
    #[serde(default)]
    pub(super) observed_at_ms: Option<i64>,
    /// Which upstream frame the headers came from.
    #[serde(default)]
    upstream_origin: Option<FrameOrigin>,
    /// Lowercase `anthropic-ratelimit-unified-*` names and raw string values,
    /// including unknown names. Absence means no headers were received.
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    pub(super) limit_window: Option<String>,
    #[serde(default)]
    pub(super) resets_at_ms: Option<i64>,
    #[serde(default)]
    pub(super) resets_at_source: Option<ResetSource>,
    /// The quota pool an exhaustion report names (the routing/v1 `pool`
    /// member); absent means the account is one pool.
    #[serde(default)]
    pub(super) pool: Option<String>,
    #[serde(default)]
    retry_at_ms: Option<u64>,
    #[serde(default)]
    provider_code: Option<String>,
    #[serde(default)]
    attempt_outcome: Option<cortexkit_role_routing::attempt::AttemptOutcome>,
}

/// What `route.account_observe` reads from each tracked provider's roster
/// before its transaction opens: the roster digest a scoped not-found is
/// recorded against, the credential ids the vault lists, and each account's
/// latest reading, from which a whole-account block takes its recovery set.
#[derive(Default, Clone)]
pub(super) struct ProviderRosterFacts {
    pub(super) digest: String,
    pub(super) listed: BTreeSet<String>,
    pub(super) readings: BTreeMap<String, QuotaReading>,
}

const OBSERVATION_KINDS: &[&str] = &[
    "used_account",
    "admission_started",
    "run_finished",
    "attempt",
    "scoped_not_found",
    "account_limit_exhausted",
];

pub(super) fn observe(params: &Value, principal: &str, ctx: &Ctx<'_>) -> OpReply {
    observe_authorized(params, principal, principal, false, ctx)
}

/// `routing_v1::accounts::account_observe` may authorize a decision owner to
/// report on its runner's admission. The event still belongs to the observer.
pub(crate) fn observe_authorized(
    params: &Value,
    principal: &str,
    admission_principal: &str,
    allow_served_account: bool,
    ctx: &Ctx<'_>,
) -> OpReply {
    checking("account_selection_enabled");
    if !ctx.flags.enabled {
        return OpReply::typed(
            DISABLED_CODE,
            "route.account_observe is not served: account selection is disabled in routing policy",
            json!({"code":DISABLED_CODE}),
        );
    }
    checking("account_observe_parse");
    // Legacy callers send an older request shape that this handler reads
    // leniently. An `attempt_outcome` member still goes through the published
    // routing/v1 parser, which refuses it unless routing advertises the
    // `account_observe_success` capability and the member is well formed.
    if params.get("attempt_outcome").is_some() {
        if let Err(refusal) = cortexkit_role_routing::observe::parse_account_observe_request_serving(
            params,
            &crate::routing_v1::describe::capabilities(),
        ) {
            let mut detail = refusal.detail;
            detail.insert("code".into(), json!(refusal.code.as_str()));
            return OpReply::typed(
                refusal.code.as_str(),
                refusal.message,
                Value::Object(detail),
            );
        }
    }
    let parsed: ObserveParams = match serde_json::from_value(params.clone()) {
        Ok(parsed) => parsed,
        Err(error) => return invalid(format!("invalid route.account_observe params: {error}")),
    };
    if parsed.event_id.trim().is_empty() {
        return because(
            "observation_event_id",
            invalid("`event_id` must not be empty"),
        );
    }
    if let Some(reply) = invalid_pool(parsed.pool.as_deref()) {
        return because("observation_pool", reply);
    }
    let roster_digests: BTreeMap<String, ProviderRosterFacts> = tracked_providers(ctx)
        .into_iter()
        .map(|provider| {
            let roster = roster_for(ctx, &provider).roster;
            let digest = roster
                .as_ref()
                .map(|r| r.view_digest.clone())
                .unwrap_or_default();
            let listed = roster
                .iter()
                .flat_map(|r| r.accounts.iter())
                .flat_map(|view| view.credentials.iter().map(|c| c.credential_id.clone()))
                .collect();
            let readings = roster
                .iter()
                .flat_map(|r| r.accounts.iter())
                .filter_map(|view| Some((view.account_id.clone(), view.quota.clone()?)))
                .collect();
            (
                provider,
                ProviderRosterFacts {
                    digest,
                    listed,
                    readings,
                },
            )
        })
        .collect();
    checking("observation_transaction");
    let command_ctx = ctx.command_context();
    let principal = principal.to_owned();
    let admission_principal = admission_principal.to_owned();
    let params = params.clone();
    crate::account_writer::execute(
        ctx.store,
        striatum_store::AccountCommandKind::Observe,
        move |tx| {
            record_observation(
                tx,
                &command_ctx,
                &principal,
                &admission_principal,
                allow_served_account,
                false,
                &parsed,
                &params,
                &roster_digests,
            )
        },
    )
    .unwrap_or_else(store_error)
}
/// `fresh_event`: the caller minted `parsed.event_id` itself inside this
/// command (a continuation's own exhaustion report), so no stored observation
/// can carry it and the duplicate lookup in `account_observation` is skipped.
#[allow(clippy::too_many_arguments)]
pub(super) fn record_observation(
    tx: &mut crate::account_state::AccountCommand<'_>,
    ctx: &CommandContext,
    principal: &str,
    admission_principal: &str,
    allow_served_account: bool,
    fresh_event: bool,
    parsed: &ObserveParams,
    raw: &Value,
    roster_digests: &BTreeMap<String, ProviderRosterFacts>,
) -> RouteStateResult<OpReply> {
    let now = ctx.now_ms;
    let runner = normalize(&parsed.caller.runner);
    let existing = if fresh_event {
        None
    } else if parsed.attempt_outcome.is_some()
        || allow_served_account
        || admission_principal != principal
    {
        tx.observation_by_principal_event(principal, &parsed.event_id)?
    } else {
        tx.observation_by_event(principal, &runner, &parsed.event_id)?
    };
    if let Some(existing) = existing {
        return Ok(OpReply::Result(json!({
            "observation": {
                "id": existing.observation_id,
                "duplicate": true,
                "bound": existing.bound,
                "effects": if parsed.attempt_outcome.is_some() { json!([]) } else {
                    serde_json::from_str::<Value>(&existing.effects_json).unwrap_or(Value::Null)
                },
            }
        })));
    }
    let observation_id = new_id("aco");
    // `routing_v1::accounts` resolves the admission's principal before the
    // transaction; a decision owner's event stays under the owner's principal.
    let success_admission = match (&parsed.attempt_outcome, &parsed.admission_id) {
        (Some(_), Some(id)) => tx.admission_for_success(admission_principal, id)?,
        _ => None,
    };
    let admission = match &parsed.admission_id {
        Some(_) if parsed.attempt_outcome.is_some() => {
            success_admission.as_ref().map(|r| r.row.clone())
        }
        Some(id) => tx.admission(admission_principal, id)?,
        None => None,
    };
    tx.insert_observation(&AccountObservationRow {
        observation_id: observation_id.clone(),
        principal: principal.to_string(),
        runner: runner.clone(),
        event_id: parsed.event_id.clone(),
        kind: parsed.kind.clone(),
        admission_id: parsed.admission_id.clone(),
        bound: false,
        observation_json: raw.to_string(),
        effects_json: "[]".into(),
        received_at: now,
    })?;
    let mut effects: Vec<Value> = Vec::new();
    let bound;
    let served_providers: BTreeSet<String> =
        if allow_served_account && parsed.kind == "transient_backoff" {
            ctx.classes
                .as_ref()
                .map(|classes| {
                    classes
                        .credentials
                        .listed_credentials()
                        .iter()
                        .filter(|row| {
                            parsed.served_account_id.as_deref() == Some(classes.account_id(row))
                        })
                        .flat_map(|row| row.serves.iter().chain(&row.provider_ids).cloned())
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Default::default()
        };
    match &admission {
        None => {
            bound = !served_providers.is_empty();
            if !bound && parsed.kind != "credential_refused" {
                effects.push(json!({"effect":"advisory", "why":"names no admission routing issued to this principal"}));
            }
        }
        Some(row) => {
            if matches!(parsed.kind.as_str(), "admission_started" | "attempt")
                && (parsed.kind == "attempt" || parsed.credential_id.is_some())
            {
                if let (Some(run), Some(account)) = (&parsed.run_id, &parsed.served_account_id) {
                    if tx.use_observed_continuation(
                        &row.principal,
                        &row.admission_id,
                        run,
                        account,
                        parsed.credential_id.as_deref(),
                        now,
                    )? {
                        effects.push(json!({"effect":"continuation_used"}));
                    }
                }
            }
            // Attempts served by a different account name the original admission
            // but are stored unbound. Refresh its started reservations before the
            // bound-only effects, including one this observation just started.
            if parsed.kind == "attempt" {
                tx.observe_admission(row, now)?;
            }
            let mismatch = parsed
                .served_account_id
                .as_deref()
                .is_some_and(|served| served != row.account_id);
            if mismatch {
                tracing::warn!(
                    admission = %row.admission_id,
                    selected = %row.account_id,
                    served = ?parsed.served_account_id,
                    "account selection: served account differs from the admitted one"
                );
                effects.push(json!({
                    "effect": "served_account_mismatch",
                    "selected_account_id": row.account_id,
                    "served_account_id": parsed.served_account_id,
                    "pin_generation": row.pin_generation,
                }));
                // Finishing is run-scoped, not scoped to the account that served last.
                bound = parsed.kind == "run_finished"
                    && parsed.run_id.is_some()
                    && parsed.run_id == row.run_id;
            } else {
                bound = true;
            }
        }
    }
    if parsed.kind == "credential_refused" {
        apply_credential_refused(&*tx, ctx, parsed, admission.as_ref(), &mut effects)?;
    } else if parsed.kind == "transient_backoff" && parsed.retry_at_ms.is_some() && bound {
        let until = parsed.retry_at_ms.unwrap().min(
            (now.max(0) as u64)
                .saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS as u64),
        );
        if let Some(admission) = &admission {
            effects.push(
                json!({"effect":"transient_backoff", "provider":admission.provider,
                "account_id":admission.account_id, "retry_at_ms":until}),
            );
        } else if let Some(account) = &parsed.served_account_id {
            for provider in served_providers {
                effects.push(json!({"effect":"transient_backoff", "provider":provider,
                    "account_id":account, "retry_at_ms":until}));
            }
        }
    } else if !OBSERVATION_KINDS.contains(&parsed.kind.as_str()) {
        effects.push(json!({ "effect": "advisory", "why": "unknown observation kind" }));
    } else if let (true, Some(row)) = (bound, admission.as_ref()) {
        let policy = ctx.provider_policy(&row.provider);
        let scoped_ctx = CommandContext {
            flags: ctx.flags,
            policy,
            now_ms: ctx.now_ms,
            classes: ctx.classes.clone(),
            concurrency_caps: ctx.concurrency_caps.clone(),
        };
        apply_bound_observation(
            tx,
            &scoped_ctx,
            principal,
            parsed,
            row,
            success_admission.as_ref(),
            &observation_id,
            roster_digests,
            &mut effects,
        )?;
    }
    let effects_value = Value::Array(effects);
    tx.set_observation_effects(&observation_id, bound, &effects_value.to_string())?;
    Ok(OpReply::Result(json!({
        "observation": {
            "id": observation_id,
            "duplicate": false,
            "bound": bound,
            "effects": effects_value,
        }
    })))
}

fn apply_credential_refused(
    tx: &AccountCommand<'_>,
    ctx: &CommandContext,
    parsed: &ObserveParams,
    admission: Option<&AccountAdmissionRow>,
    effects: &mut Vec<Value>,
) -> RouteStateResult<()> {
    checking("credential_refused_target");
    let credential = parsed
        .credential_id
        .as_deref()
        .or_else(|| admission.map(|row| row.credential_id.as_str()));
    let (Some(credential), Some(classes)) = (credential, &ctx.classes) else {
        return Ok(());
    };
    // Resolve membership even if the vault already marked the credential
    // invalid. Its current usability must not erase the provider's refusal.
    let mut providers: BTreeSet<String> = classes
        .credentials
        .listed_credentials()
        .iter()
        .filter(|row| row.id == credential)
        .flat_map(|row| crate::broca_credentials::credential_providers(row, &classes.catalog))
        .collect();
    if let Some(row) = admission.filter(|row| row.credential_id == credential) {
        providers.insert(row.provider.clone());
    }
    let fingerprint = match classes.credentials.listing_fingerprint(credential) {
        crate::broca_credentials::ListingLookup::Listed(fingerprint) => Some(fingerprint),
        _ => None,
    };
    let until = ctx
        .now_ms
        .saturating_add(crate::credential_fences::CREDENTIAL_REFUSED_REPROBE_MS);
    for provider in providers {
        // A provider with no catalog rows borrows a vendor's rows through its
        // active API keys. Even if the refused key is now invalid and no longer
        // supplies that mapping, record its refusal in the pooled-key wall.
        let pool = classes.targets.contains_key(&provider)
            || (!classes.catalog.contains(&provider)
                && classes.credentials.listed_credentials().iter().any(|row| {
                    row.id == credential
                        && row.credential_type == "apikey"
                        && row
                            .serves
                            .iter()
                            .filter(|vendor| classes.catalog.contains(*vendor))
                            .count()
                            == 1
                }));
        if pool {
            tx.pool_wall(
                &provider,
                credential,
                striatum_store::PoolWallKind::Billing,
                until,
                parsed
                    .provider_code
                    .as_deref()
                    .unwrap_or("credential_refused"),
                ctx.now_ms,
            )?;
        } else {
            // Keep the wall independent of quota readings: an empty balance or
            // a revoked key is not a resettable usage window. The provider's
            // verbatim code remains in the stored observation.
            tx.record_credential_refusal(
                &striatum_core::route_state_store::CredentialRefusal {
                    provider_id: provider.clone(),
                    credential_id: credential.into(),
                    code: "credential_refused".into(),
                },
                fingerprint.as_deref(),
                ctx.now_ms,
            )?;
        }
        effects.push(json!({"effect":"credential_refused", "provider":provider,
            "credential_id":credential}));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_bound_observation(
    tx: &mut crate::account_state::AccountCommand<'_>,
    ctx: &CommandContext,
    principal: &str,
    parsed: &ObserveParams,
    admission: &AccountAdmissionRow,
    success_admission: Option<&striatum_store::AdmissionStateRow>,
    observation_id: &str,
    roster_digests: &BTreeMap<String, ProviderRosterFacts>,
    effects: &mut Vec<Value>,
) -> RouteStateResult<()> {
    let now = ctx.now_ms;
    let id = &admission.admission_id;
    match parsed.kind.as_str() {
        "admission_started" => {
            let late = now >= admission.admission_valid_until;
            if tx.mark_admission_started(principal, id, parsed.run_id.as_deref(), now, late)? {
                effects.push(json!({ "effect": "admission_started", "late": late }));
            } else {
                effects.push(json!({ "effect": "none", "state": admission.state }));
            }
        }
        "run_finished" => {
            // Read while the admission is still open: finishing closes it.
            let size_class = tx.admission_size_class(principal, id)?;
            let release_unanswered = if tx.mark_admission_finished(principal, id, now)? {
                effects.push(json!({ "effect": "admission_finished" }));
                let lease_ended = tx.end_lease(&admission.pin_id, now)?;
                effects.push(json!({ "effect": "lease_ended", "lease_ended": lease_ended }));
                if let Some(run) = &admission.run_id {
                    tx.finish_continuations(&admission.principal, id, run, now)?;
                }
                let media = tx
                    .decision_context(&admission.decision_id)?
                    .and_then(|context| serde_json::from_str::<Value>(&context).ok())
                    .is_some_and(|context| context["media_admission"] == true);
                // A run can finish on another login. Ending its lifecycle does
                // not prove that the admitted account and credential recovered.
                let media_success = media
                    && parsed.served_account_id.as_deref() == Some(admission.account_id.as_str())
                    && parsed.credential_id.as_deref() == Some(admission.credential_id.as_str())
                    && !tx.admission_reported_failure(principal, id)?;
                if media_success
                    && crate::credential_fences::lift_after_success_in(
                        tx,
                        &admission.provider,
                        &admission.credential_id,
                        now,
                        admission.committed_at,
                    )?
                {
                    effects.push(json!({
                        "effect": "credential_fence_lifted",
                        "provider": admission.provider,
                        "credential_id": admission.credential_id,
                    }));
                }
                if media_success {
                    // The shared admission-success healing, with the run's
                    // pool scope, as for a served attempt.
                    tx.heal_admission_success(
                        admission,
                        size_class,
                        success_admission.and_then(|row| row.model_decision_id.as_deref()),
                        Some(&striatum_store::QuotaSuccessScope {
                            family: ctx
                                .provider_policy(&admission.provider)
                                .family_for_model(&admission.model),
                            pool: request_pool(None, &admission.provider, &admission.model),
                        }),
                        now,
                    )?;
                    // The finished run's probe authorizations are spent,
                    // whether or not they healed a block.
                    tx.retire_probe_authorizations(&admission_probe_owner(
                        &admission.principal,
                        &admission.admission_id,
                    ))?;
                }
                !media_success
            } else {
                effects.push(json!({ "effect": "none", "state": admission.state }));
                false
            };
            if parsed
                .served_account_id
                .as_deref()
                .is_none_or(|account| account == admission.account_id)
            {
                header_fences(
                    tx,
                    ctx,
                    principal,
                    parsed,
                    admission,
                    observation_id,
                    effects,
                )?;
            }
            if release_unanswered && !effects.iter().any(|effect| effect["effect"] == "fence") {
                tx.release_unanswered_admission_probes(admission, now)?;
            }
        }
        "attempt" => {
            if principal == admission.principal
                && parsed.served_account_id.as_deref() == Some(admission.account_id.as_str())
                && parsed.credential_id.as_deref() == Some(admission.credential_id.as_str())
                && parsed.attempt_outcome.as_ref().is_some_and(|outcome| {
                    matches!(
                        outcome.status,
                        cortexkit_role_routing::attempt::AttemptStatus::Served
                            | cortexkit_role_routing::attempt::AttemptStatus::ServedEmpty
                    )
                })
            {
                let scope = striatum_store::QuotaSuccessScope {
                    family: ctx
                        .provider_policy(&admission.provider)
                        .family_for_model(&admission.model),
                    pool: request_pool(
                        parsed.pool.as_deref(),
                        &admission.provider,
                        &admission.model,
                    ),
                };
                if let Some(probe) = success_admission {
                    for origin in tx.heal_admission_success(
                        admission,
                        probe.input_size_class,
                        probe.model_decision_id.as_deref(),
                        Some(&scope),
                        now,
                    )? {
                        effects.push(json!({"effect":"block_cleared", "kind":origin.kind,
                            "origin_key":origin.origin_key}));
                    }
                }
            }
            header_fences(
                tx,
                ctx,
                principal,
                parsed,
                admission,
                observation_id,
                effects,
            )?;
        }
        "scoped_not_found" => {
            let (Some(credential_id), Some(view_digest)) = (
                parsed.credential_id.as_deref(),
                parsed.view_digest.as_deref(),
            ) else {
                effects.push(json!({
                    "effect": "none",
                    "why": "scoped_not_found needs credential_id and view_digest",
                }));
                return Ok(());
            };
            let ProviderRosterFacts {
                digest: roster_digest,
                listed,
                ..
            } = roster_digests
                .get(&admission.provider)
                .cloned()
                .unwrap_or_default();
            if credential_id != admission.credential_id {
                effects.push(json!({"effect":"advisory", "why":"credential differs from the bound admission"}));
                return Ok(());
            }
            let listing_fingerprint = ctx.classes.as_ref().and_then(|classes| {
                match classes.credentials.listing_fingerprint(credential_id) {
                    crate::broca_credentials::ListingLookup::Listed(fingerprint) => {
                        Some(fingerprint)
                    }
                    _ => None,
                }
            });
            tx.record_eligibility(&striatum_store::AccountEligibilityFact {
                provider: admission.provider.clone(),
                credential_id: credential_id.into(),
                model: admission.model.clone(),
                listing_fingerprint,
                code: "model_not_on_account".into(),
                observed_at: parsed.observed_at_ms.unwrap_or(now),
            })?;
            tx.insert_exclusion_with_recheck(
                &AccountExclusionRow {
                    principal: principal.to_string(),
                    admission_id: id.clone(),
                    credential_id: credential_id.to_string(),
                    runner_view_digest: view_digest.to_string(),
                    roster_view_digest: roster_digest,
                    observation_id: Some(observation_id.to_string()),
                    recorded_at: now,
                },
                ctx.provider_policy(&admission.provider)
                    .principal_exclusion_max_ms,
            )?;
            effects.push(json!({
                "effect": "excluded_for_admission",
                "credential_id": credential_id,
            }));
            // Only the listing can drop a candidate; a credential refused again
            // while the vault still lists it is an alarm, not a revocation.
            if listed.contains(credential_id) && tx.exclusion_count(principal, credential_id)? > 1 {
                tracing::warn!(
                    principal,
                    credential_id,
                    "account selection: repeated scoped not-found for a credential the vault still lists"
                );
                effects.push(
                    json!({ "effect": "repeated_not_found_alarm", "credential_id": credential_id }),
                );
            }
        }
        "account_limit_exhausted" => {
            if parsed.limit_window.as_deref() == Some("credit") {
                // Credit is a spending wall on a valid credential, not a quota
                // pool. Utilization cannot prove that the account can pay again.
                let cap =
                    now.saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS);
                let retry = parsed
                    .retry_at_ms
                    .map(|at| i64::try_from(at).unwrap_or(i64::MAX))
                    .or_else(|| {
                        (parsed.resets_at_source == Some(ResetSource::RetryAfter))
                            .then_some(parsed.resets_at_ms)
                            .flatten()
                    });
                let until = retry.unwrap_or(cap).min(cap);
                tx.record_non_quota_route_fence(&striatum_store::AccountRouteFence {
                    provider: admission.provider.clone(),
                    credential_id: admission.credential_id.clone(),
                    model: admission.model.clone(),
                    until_ms: until,
                    decision_id: admission.decision_id.clone(),
                    recorded_at: now,
                })?;
                effects.push(json!({"effect":"fence","fence_id":new_id("acf"),
                "fence":wire(&Fence {
                    account_id: admission.account_id.clone(), pool: None, recovery_set: Vec::new(),
                    observed_at_ms: now, reset: ResetTime::Missing, retry_hint_ms: Some(until),
                    source: striatum_core::accounts::ConfirmationSource::BrocaAccountLimit,
                })}));
                return Ok(());
            }
            let observation = ExhaustionObservation {
                account_id: admission.account_id.clone(),
                bound: true,
                kind: ObservationKind::BrocaLimitExhausted {
                    limit_window: parsed.limit_window.clone(),
                    pool: parsed.pool.clone(),
                    requested_family: ctx.policy.family_for_model(&admission.model),
                    resets_at_ms: parsed.resets_at_ms,
                    resets_at_source: parsed.resets_at_source,
                    observed_at_ms: parsed.observed_at_ms.unwrap_or(now),
                },
            };
            apply_confirmation(
                tx,
                ctx,
                principal,
                admission,
                observation_id,
                &observation,
                latest_reading(roster_digests, admission),
                effects,
            )?;
        }
        _ => {}
    }
    Ok(())
}

/// A response from the real upstream (a direct response, an upstream frame an
/// HTTP relay carried, or the WebSocket `response_start` frame; never headers
/// a relay made up) whose own raw 5h or 7d utilization header reads 1.0 or
/// more confirms that window is exhausted, without a usage poll. Everything
/// else a header says is kept with the observation and changes nothing shared.
fn header_fences(
    tx: &AccountTx<'_>,
    ctx: &CommandContext,
    principal: &str,
    parsed: &ObserveParams,
    admission: &AccountAdmissionRow,
    observation_id: &str,
    effects: &mut Vec<Value>,
) -> RouteStateResult<()> {
    let headers: BTreeMap<String, &String> = parsed
        .headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value))
        .collect();
    for (window, label) in [
        (GeneralWindow::FiveHour, "5h"),
        (GeneralWindow::SevenDay, "7d"),
    ] {
        let Some(raw) = headers.get(&format!("anthropic-ratelimit-unified-{label}-utilization"))
        else {
            continue;
        };
        let Some(frame) = parsed.upstream_origin else {
            effects.push(json!({
                "effect": "none",
                "window": wire(&window),
                "why": "header evidence without upstream_origin cannot confirm",
            }));
            continue;
        };
        let raw_utilization = raw.trim().parse::<f64>().unwrap_or(f64::NAN);
        let reset = headers
            .get(&format!("anthropic-ratelimit-unified-{label}-reset"))
            .map(|value| value.trim().parse::<f64>().unwrap_or(f64::NAN));
        let quota_window = header_window(window, raw_utilization, reset, ObservationSource::Header);
        let observed_at_ms = parsed.observed_at_ms.unwrap_or(ctx.now_ms);
        let observation = ExhaustionObservation {
            account_id: admission.account_id.clone(),
            bound: true,
            kind: ObservationKind::ServedHeader(WindowEvidence {
                scope: EvidenceScope::General(window),
                derivation: Derivation::Header {
                    frame,
                    raw_utilization: Some(raw_utilization).filter(|raw| raw.is_finite()),
                },
                carrier: Carrier::Direct,
                observed_at_ms,
                received_at_ms: ctx.now_ms,
                reset: quota_window.resets_at,
            }),
        };
        apply_confirmation(
            tx,
            ctx,
            principal,
            admission,
            observation_id,
            &observation,
            // A header names its window, so the block is never whole-account.
            None,
            effects,
        )?;
    }
    Ok(())
}

/// The admitted account's latest reading, as the observation found it.
fn latest_reading<'r>(
    roster_digests: &'r BTreeMap<String, ProviderRosterFacts>,
    admission: &AccountAdmissionRow,
) -> Option<&'r QuotaReading> {
    roster_digests
        .get(&admission.provider)
        .and_then(|facts| facts.readings.get(&admission.account_id))
}

/// The one Antigravity pool a limit refusal that named no pool belongs to:
/// the pool the refused model draws on. A fresh pooled reading of the account
/// decides which of its pools the model draws on. With no fresh reading the
/// model alone decides, through its family's pool, unless operator config
/// gives the model a second pool. A model drawing on two pools, or on none
/// routing knows, gets no key, and the block stays whole-account as before.
/// A fresh reading without pool windows meters the whole account, so its
/// block stays whole-account too.
fn refused_model_pool(
    reading: Option<&QuotaReading>,
    provider: &str,
    model: &str,
    model_pools: &[striatum_core::quota_extract::ModelPool],
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> Option<striatum_core::accounts::PoolKey> {
    let capacity = format!("{}/{}", normalize(provider), normalize(model));
    let pool = match reading
        .filter(|reading| striatum_core::accounts::pools::fresh_for_recovery(reading, now, policy))
    {
        Some(reading) => {
            let named: BTreeSet<&str> = reading
                .pool_windows
                .iter()
                .flatten()
                .filter_map(|window| window.pool.as_deref())
                .collect();
            if named.is_empty() {
                return None;
            }
            let mut drawn = named.into_iter().filter(|pool| {
                striatum_core::quota_extract::antigravity_capacity_draws_on_pool(
                    &capacity,
                    pool,
                    model_pools,
                )
            });
            match (drawn.next(), drawn.next()) {
                (Some(pool), None) => pool.to_string(),
                _ => return None,
            }
        }
        None => {
            let model_id = normalize(model);
            let family = striatum_core::quota_extract::antigravity_pool_for_capacity(&capacity)?;
            if model_pools
                .iter()
                .any(|entry| model_id.starts_with(entry.model_prefix.as_str()))
            {
                return None;
            }
            family.to_string()
        }
    };
    validate_pool_name(&pool).ok()?;
    // The pool name alone says which models draw on it, so the key covers
    // every model and the request's own pool decides whether it applies.
    Some(striatum_core::accounts::PoolKey {
        window_minutes: None,
        scope: striatum_core::accounts::PoolScope::EveryModel,
        name: Some(pool),
    })
}

#[allow(clippy::too_many_arguments)]
fn apply_confirmation(
    tx: &AccountTx<'_>,
    ctx: &CommandContext,
    principal: &str,
    admission: &AccountAdmissionRow,
    observation_id: &str,
    observation: &ExhaustionObservation,
    latest_reading: Option<&QuotaReading>,
    effects: &mut Vec<Value>,
) -> RouteStateResult<()> {
    match confirm_exhaustion(observation) {
        ConfirmationOutcome::Confirmed { mut fence } => {
            // A runner clock ahead of routing cannot postpone both recovery
            // and the next probe. The original timestamp remains in the
            // append-only observation, separate from this operational fence.
            fence.observed_at_ms = fence.observed_at_ms.min(ctx.now_ms);
            let mut prior = fence.clone();
            prior.observed_at_ms = i64::MIN;
            if matches!(
                observation.kind,
                ObservationKind::BrocaLimitExhausted { .. }
            ) && block_end(
                &prior,
                latest_reading,
                ctx.now_ms,
                &ctx.provider_policy(&admission.provider),
            )
            .is_some()
            {
                tx.record_non_quota_route_fence(&striatum_store::AccountRouteFence {
                    provider: admission.provider.clone(),
                    credential_id: admission.credential_id.clone(),
                    model: admission.model.clone(),
                    until_ms: [
                        match fence.reset {
                            ResetTime::At(at) if at > ctx.now_ms => Some(at),
                            _ => None,
                        },
                        fence.retry_hint_ms.filter(|at| *at > ctx.now_ms),
                    ]
                    .into_iter()
                    .flatten()
                    .chain(std::iter::once(ctx.now_ms.saturating_add(
                        striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS,
                    )))
                    .min()
                    .unwrap(),
                    decision_id: admission.decision_id.clone(),
                    recorded_at: ctx.now_ms,
                })?;
                effects
                    .push(json!({"effect":"fence","fence_id":new_id("acf"),"fence":wire(&fence)}));
                return Ok(());
            }
            if fence.pool.is_none()
                && matches!(
                    observation.kind,
                    ObservationKind::BrocaLimitExhausted { .. }
                )
            {
                let model_pools = ctx
                    .classes
                    .as_ref()
                    .map(|classes| classes.config.quota_model_pools())
                    .unwrap_or_default();
                fence.pool = refused_model_pool(
                    latest_reading,
                    &admission.provider,
                    &admission.model,
                    &model_pools,
                    ctx.now_ms,
                    &ctx.provider_policy(&admission.provider),
                );
            }
            if fence.pool.is_none() {
                // A whole-account block lifts only on a reading that reports
                // every pool the account's latest fresh reading reported.
                fence.recovery_set = recovery_set_from(latest_reading, ctx.now_ms, &ctx.policy);
            }
            let fence_id = new_id("acf");
            tx.insert_pool_fence(&AccountPoolFenceRow {
                evidence: AccountFenceRow {
                    fence_id: fence_id.clone(),
                    provider: admission.provider.clone(),
                    account_id: fence.account_id.clone(),
                    scope_json: wire(&fence.pool).to_string(),
                    fence_json: wire(&fence).to_string(),
                    source: wire_str(&fence.source),
                    observed_at: fence.observed_at_ms,
                    principal: principal.to_string(),
                    admission_id: Some(admission.admission_id.clone()),
                    observation_id: Some(observation_id.to_string()),
                    created_at: ctx.now_ms,
                    cleared_at: None,
                    cleared_reason: None,
                },
                pool_key: fence.pool.clone(),
                recovery_set: fence.recovery_set.clone(),
            })?;
            effects.push(json!({ "effect": "fence", "fence_id": fence_id, "fence": wire(&fence) }));
        }
        other => effects.push(json!({ "effect": "not_confirmed", "evaluation": wire(&other) })),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// route.account_release
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct ReleaseParams {
    pub(super) admission_id: String,
}

pub(super) fn release(params: &Value, principal: &str, ctx: &Ctx<'_>) -> OpReply {
    let parsed: ReleaseParams = match serde_json::from_value(params.clone()) {
        Ok(parsed) => parsed,
        Err(error) => return invalid(format!("invalid route.account_release params: {error}")),
    };
    let now = ctx.now_ms;
    let principal = principal.to_owned();
    crate::account_writer::execute(
        ctx.store,
        striatum_store::AccountCommandKind::Release,
        move |tx| {
            // Only this principal's own admission can be released; any other
            // id changes nothing.
            let Some(row) = tx.admission(&principal, &parsed.admission_id)? else {
                return Ok(OpReply::Result(json!({
                    "release": {
                        "admission_id": parsed.admission_id,
                        "state": null,
                        "released": false,
                        "lease_ended": false,
                    }
                })));
            };
            let released = tx.mark_admission_released(&principal, &row.admission_id, now)?;
            // Release ends the conversation's lease; it never erases affinity
            // and never cancels a request in flight.
            // A completed or already released admission must not end a lease
            // that another admission may since have renewed on the same pin.
            let lease_ended = if matches!(row.state.as_str(), "finished" | "released") {
                false
            } else {
                tx.end_lease(&row.pin_id, now)?
            };
            let state = tx
                .admission(&principal, &row.admission_id)?
                .map_or(row.state.clone(), |current| current.state);
            Ok(OpReply::Result(json!({
                "release": {
                    "admission_id": row.admission_id,
                    "state": state,
                    "released": released,
                    "lease_ended": lease_ended,
                }
            })))
        },
    )
    .unwrap_or_else(store_error)
}

#[cfg(test)]
mod refused_model_pool_tests;
