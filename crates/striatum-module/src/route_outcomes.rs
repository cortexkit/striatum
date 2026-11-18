//! Legacy outcome, usage and cooldown reports.
//! These handlers translate caller evidence into durable route and account state.

use super::*;

pub(super) fn record_cooldown_update(
    store: &striatum_store::SqliteRouteStateStore,
    params: &RecordCooldownParams,
) -> striatum_core::RouteStateResult<()> {
    // Existing machine-formatted account/key annotations take precedence over
    // an identity-free cooldown. Never turn one account's refusal into fanout;
    // only genuinely anonymous signals get the non-quota clock below.
    if params.credential_id.is_none()
        && params.last_error.as_deref().is_some_and(|error| {
            striatum_core::account_wall::account_scoped_error(error)
                || striatum_core::account_wall::credential_refusal_in(error).is_some()
                || serde_json::from_str::<serde_json::Value>(error)
                    .ok()
                    .is_some_and(|value| {
                        let provider = value.get("provider").unwrap_or(&value);
                        provider.get("class").is_some()
                            && provider.get("provider_code").is_some()
                            && ["account_ref", "credential_id"].iter().any(|field| {
                                provider
                                    .get(field)
                                    .and_then(serde_json::Value::as_str)
                                    .is_some_and(|id| !id.trim().is_empty())
                            })
                    })
        })
    {
        return Ok(());
    }
    if let Some(credential) = params
        .credential_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .filter(|_| params.wall_class == Some(RouteWallClass::Transient))
    {
        let Some(until) = params.cooldown_until else {
            return Ok(());
        };
        let Some((provider, model)) = params.capacity_key.split_once('/') else {
            return Ok(());
        };
        return crate::account_writer::record_transient_account_route_fence(
            store,
            &striatum_store::AccountRouteFence {
                provider: provider.into(),
                model: model.into(),
                credential_id: credential.into(),
                until_ms: until as i64,
                decision_id: String::new(),
                recorded_at: now_ms() as i64,
            },
        );
    }
    let update = striatum_core::RouteStateUpdate {
        observed_at_ms: Some(now_ms() as i64),
        // No account-local typed branch handled this signal. Do not infer a
        // subscription identity from its error text or reported quota label.
        unscoped_wall: true,
        cooldown_until: params.cooldown_until,
        quota_exhausted_until: params.quota_exhausted_until,
        wall_class: params.wall_class,
        quota_pressure: params.quota_pressure,
        last_error: params.last_error.clone(),
        increment_quota_error: params.increment_quota_error,
        increment_failure: params.increment_failure,
        ..Default::default()
    };
    if params.provider_wide {
        let provider_key = derive_provider_key(&params.route_key)
            .or_else(|| derive_provider_key(&params.capacity_key))
            .ok_or_else(|| {
                striatum_core::RouteStateStoreError(
                    "provider-wide cooldown has no canonical provider key".into(),
                )
            })?;
        return crate::account_writer::record_provider_cooldown(
            store,
            provider_key,
            &params.route_key,
            &params.capacity_key,
            &update,
        );
    }
    crate::account_writer::update_route_state(
        store,
        &params.route_key,
        &params.capacity_key,
        &update,
    )
}

/// Why a `route.record_cooldown` request must not reach the store, if it must not.
///
/// A route-scoped write stores `capacityKey` on the route's row, replacing the
/// one already there, and soft pressure and walls are shared by every route under
/// that capacity. A capacity that is not the route's own would therefore move the
/// route's walls and pressure onto another model. Routing builds both keys with
/// `make_route_key` and `make_capacity_key`, so a route's capacity is the route
/// key itself or a prefix of it that ends at a `/`. Provider-wide writes are
/// checked by the store, which requires both keys to name the same provider.
/// Quota pressure is a 0..=1 score that selection adds to a route's cost, so a
/// value outside that range is refused rather than stored.
fn record_cooldown_params_error(params: &RecordCooldownParams) -> Option<String> {
    if let Some(pressure) = params.quota_pressure {
        if !(0.0..=1.0).contains(&pressure) {
            return Some(format!(
                "record_cooldown quotaPressure {pressure} is outside 0..=1"
            ));
        }
    }
    if !params.provider_wide {
        let capacity = params.capacity_key.as_str();
        let paired = !capacity.is_empty()
            && params
                .route_key
                .strip_prefix(capacity)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'));
        if !paired {
            return Some(format!(
                "record_cooldown capacityKey {:?} is not the capacity of routeKey {:?}",
                params.capacity_key, params.route_key
            ));
        }
    }
    None
}

/// Record the audit row for a selection that ended without a route. The
/// routing failure is still what the caller receives, so a failed write does
/// not change the reply; it is logged so a missing row can be explained.
pub(super) fn record_failed_selection_decision(
    store: &impl RouteStateStore,
    decision: &DecisionInput,
) {
    if let Err(error) = store.record_decision(decision) {
        warn_operator(format!(
            "[striatum] WARN: a failed route.select for {} (task {}, send {}) has no decision row: {error}",
            decision.target_agent,
            decision.task_id.as_deref().unwrap_or("-"),
            decision.send_id.as_deref().unwrap_or("-"),
        ));
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct LegacyProviderOutcome {
    #[serde(flatten)]
    pub(super) identity: striatum_core::route_state_store::ProviderOutcome,
    // Decode a caller's explicit credit window without adding any member to
    // routing's serialized provider identity or existing replies.
    #[serde(default, alias = "limit_window")]
    pub(super) limit_window: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SetDecisionOutcomeParams {
    #[serde(default)]
    pub(super) provider_outcome: Option<LegacyProviderOutcome>,
    // The wire field is `decisionID` (capital ID) to match what route.select RETURNS
    // (json!{ "decisionID": ... }) and what every TS caller sends: camelCase alone
    // would expect `decisionId`, the lone casing drift that broke the round-trip.
    #[serde(rename = "decisionID")]
    pub(super) decision_id: String,
    /// completed | contract_error | error | session_error | cancelled | interrupted | launch_failed | quota_exhausted
    pub(super) outcome: String,
    /// Host-parsed reset delay for a quota_exhausted outcome.
    #[serde(default)]
    pub(super) retry_after_secs: Option<u64>,
    /// Exact terminal send error surfaced by the harness, when one was available.
    #[serde(default)]
    pub(super) last_error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RecordCooldownParams {
    pub(super) route_key: String,
    pub(super) capacity_key: String,
    #[serde(default)]
    pub(super) credential_id: Option<String>,
    #[serde(default)]
    pub(super) cooldown_until: Option<f64>,
    #[serde(default)]
    pub(super) quota_exhausted_until: Option<f64>,
    #[serde(default)]
    pub(super) quota_pressure: Option<f64>,
    #[serde(default)]
    pub(super) wall_class: Option<RouteWallClass>,
    #[serde(default)]
    pub(super) provider_wide: bool,
    #[serde(default)]
    pub(super) increment_quota_error: bool,
    #[serde(default)]
    pub(super) increment_failure: bool,
    #[serde(default)]
    pub(super) last_error: Option<String>,
}

pub(super) fn parse_outcome(s: &str, retry_after_secs: Option<u64>) -> Option<RouteOutcome> {
    match s {
        "completed" => Some(RouteOutcome::Completed),
        "contract_error" => Some(RouteOutcome::ContractError),
        "error" => Some(RouteOutcome::Error),
        "session_error" => Some(RouteOutcome::SessionError),
        "cancelled" => Some(RouteOutcome::Cancelled),
        "interrupted" => Some(RouteOutcome::Interrupted),
        "launch_failed" => Some(RouteOutcome::LaunchFailed),
        "quota_exhausted" => retry_after_secs
            .filter(|seconds| *seconds > 0)
            .map(|retry_after_secs| RouteOutcome::QuotaExhausted { retry_after_secs }),
        _ => None,
    }
}

pub(super) async fn handle_set_decision_outcome(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    _routing_config_path: &Path,
    broca_credentials: &SharedBrocaCredentials,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: SetDecisionOutcomeParams =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(b) => b.params,
            Err(e) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("set_decision_outcome params not decodable: {e}"),
                )
                .await;
            }
        };
    let Some(outcome) = parse_outcome(&params.outcome, params.retry_after_secs) else {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            &format!("unknown outcome '{}'", params.outcome),
        )
        .await;
    };
    let credit_window = params
        .provider_outcome
        .as_ref()
        .is_some_and(|provider| provider.limit_window.as_deref() == Some("credit"));
    let mut provider_outcome = params.provider_outcome.map(|provider| provider.identity);
    if let Some(provider) = provider_outcome
        .as_mut()
        .filter(|p| p.credential_id.is_none())
    {
        // The provider may name its account differently from the vault. The
        // sole serving identity frozen for this decision resolves that alias.
        if let Some(identity) = store
            .decision_provider_identity(&params.decision_id)
            .map_err(|e| ModuleError::Message(e.to_string()))?
        {
            provider.credential_id = identity.credential_id;
        }
    }
    if outcome == RouteOutcome::Completed && provider_outcome.is_none() {
        // Use the unambiguous serving identity recorded at selection, never
        // today's roster: an account may have changed while the run was active.
        provider_outcome = store
            .decision_provider_identity(&params.decision_id)
            .map_err(|e| ModuleError::Message(e.to_string()))?;
    }
    let outcome = if outcome != RouteOutcome::Completed
        && provider_outcome.as_ref().is_some_and(|provider| {
            striatum_core::account_wall::transient_provider_error(
                provider.error_class.as_deref(),
                provider.provider_code.as_deref(),
            )
        }) {
        RouteOutcome::Interrupted
    } else {
        outcome
    };
    let observed_at_ms = now_ms() as i64;
    let quota = ACCOUNT_QUOTA
        .get()
        .and_then(|state| state.lock().ok().map(|state| state.clone()));
    let fence_config = config_overrides::live_config(_routing_config_path);
    let account_exhausted = provider_outcome.as_ref().is_some_and(|provider| {
        provider.provider_code.as_deref() == Some("account_limit_exhausted")
    }) || params
        .last_error
        .as_deref()
        .is_some_and(|error| error.starts_with("account_limit_exhausted:"));
    let credit_retry_at = (credit_window && account_exhausted).then(|| {
        let cap =
            observed_at_ms.saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS);
        params
            .retry_after_secs
            .map(|seconds| {
                observed_at_ms.saturating_add(
                    i64::try_from(seconds)
                        .unwrap_or(i64::MAX)
                        .saturating_mul(1000),
                )
            })
            .unwrap_or(cap)
            .min(cap)
    });
    if let Err(error) = record_provider_reset_fence(
        store,
        broca_credentials,
        &params.decision_id,
        &outcome,
        provider_outcome.as_ref(),
        account_exhausted,
        credit_retry_at,
        observed_at_ms,
        quota.as_ref(),
        Some(&fence_config),
    ) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "store_error",
            &format!("set_decision_outcome: provider reset fence: {error}"),
        )
        .await;
    }
    // A credential the provider refused permanently is fenced whatever the
    // decision ledger still holds: the report names the provider and the
    // credential itself. Recording renews a repeated refusal, so a fence that
    // keeps failing stays fresh and its next recheck probe waits another
    // interval. Subscription refusals are reconciled against quota readings
    // by account selection, including fences persisted before quota delivery.
    if let Some(refusal) = provider_outcome
        .as_ref()
        .and_then(|provider| provider.credential_refusal.as_ref())
    {
        if let Err(e) = credential_fences::record(store, broca_credentials, refusal, observed_at_ms)
        {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("set_decision_outcome: credential entitlement fence: {e}"),
            )
            .await;
        }
    }
    // Only the credential that actually served the completed run is healed.
    if outcome == RouteOutcome::Completed {
        if let Some(credential) = provider_outcome
            .as_ref()
            .and_then(|provider| provider.credential_id.as_deref())
        {
            match store.get_decision_by_id(&params.decision_id) {
                Ok(Some(decision)) => {
                    if let Some(provider) = decision.selected_provider_id.as_deref() {
                        let frozen = store
                            .decision_provider_identity(&params.decision_id)
                            .map_err(|e| ModuleError::Message(e.to_string()))?;
                        if let Some(identity) = frozen.as_ref().filter(|identity| {
                            identity.credential_id.as_deref() == Some(credential)
                        }) {
                            let model = decision.selected_model_id.as_deref().unwrap_or("");
                            let scope = striatum_store::QuotaSuccessScope {
                                family: fence_config
                                    .allocator_policy(provider)
                                    .family_for_model(model),
                                pool: striatum_core::quota_extract::antigravity_pool_for_capacity(
                                    &format!("{provider}/{model}"),
                                )
                                .map(str::to_string),
                            };
                            let decision_id = params.decision_id.clone();
                            let provider = provider.to_owned();
                            let model = model.to_owned();
                            let credential = credential.to_owned();
                            let identity = identity.clone();
                            account_writer::execute(
                                store,
                                striatum_store::AccountCommandKind::Report,
                                move |tx| {
                                    let provider = provider.as_str();
                                    let model = model.as_str();
                                    let credential = credential.as_str();
                                    tx.heal_authorized_probe(
                                        &decision_id,
                                        provider,
                                        model,
                                        observed_at_ms,
                                    )?;
                                    let size = tx.size_admission_for_model_decision(
                                        &decision_id,
                                        provider,
                                        credential,
                                        model,
                                    )?;
                                    tx.heal_serving_fences_in_scope(
                                        provider,
                                        &identity.account_ref,
                                        credential,
                                        model,
                                        size.map(|(class, _)| class),
                                        size.map_or(decision.created_at as i64, |(_, at)| at),
                                        observed_at_ms,
                                        Some(&scope),
                                    )?;
                                    credential_fences::lift_after_success_in(
                                        tx,
                                        provider,
                                        credential,
                                        observed_at_ms,
                                        decision.created_at as i64,
                                    )?;
                                    Ok(())
                                },
                            )
                            .map_err(|e| ModuleError::Message(e.to_string()))?;
                        } else if frozen
                            .as_ref()
                            .and_then(|identity| identity.credential_id.as_deref())
                            .is_none()
                        {
                            let model = decision.selected_model_id.as_deref().unwrap_or("");
                            let decision_id = params.decision_id.clone();
                            let provider = provider.to_owned();
                            let model = model.to_owned();
                            let credential = credential.to_owned();
                            account_writer::execute(
                                store,
                                striatum_store::AccountCommandKind::Report,
                                move |tx| {
                                    let provider = provider.as_str();
                                    let model = model.as_str();
                                    let credential = credential.as_str();
                                    if tx.probe_authorization_names_credential(
                                        &decision_id,
                                        credential,
                                        model,
                                    )? {
                                        tx.heal_authorized_probe(
                                            &decision_id,
                                            provider,
                                            model,
                                            observed_at_ms,
                                        )?;
                                    }
                                    Ok(())
                                },
                            )
                            .map_err(|e| ModuleError::Message(e.to_string()))?;
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    return send_route_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "store_error",
                        &format!("set_decision_outcome: reading decision: {e}"),
                    )
                    .await
                }
            }
        } else if let Some(decision) = store
            .get_decision_by_id(&params.decision_id)
            .map_err(|error| ModuleError::Message(error.to_string()))?
        {
            let frozen = store
                .decision_provider_identity(&params.decision_id)
                .map_err(|error| ModuleError::Message(error.to_string()))?;
            if frozen
                .as_ref()
                .and_then(|identity| identity.credential_id.as_deref())
                .is_none()
            {
                if let (Some(provider), Some(model)) = (
                    decision.selected_provider_id.as_deref(),
                    decision.selected_model_id.as_deref(),
                ) {
                    let decision_id = params.decision_id.clone();
                    let provider = provider.to_owned();
                    let model = model.to_owned();
                    account_writer::execute(
                        store,
                        striatum_store::AccountCommandKind::Report,
                        move |tx| {
                            tx.heal_authorized_probe(
                                &decision_id,
                                &provider,
                                &model,
                                observed_at_ms,
                            )
                        },
                    )
                    .map_err(|error| ModuleError::Message(error.to_string()))?;
                }
            }
        }
    }
    if striatum_core::fence_lifetimes::probe_is_inconclusive(outcome, params.last_error.as_deref())
    {
        if let Some(decision) = store
            .get_decision_by_id(&params.decision_id)
            .map_err(|error| ModuleError::Message(error.to_string()))?
        {
            if let (Some(provider), Some(model)) = (
                decision.selected_provider_id.as_deref(),
                decision.selected_model_id.as_deref(),
            ) {
                let decision_id = params.decision_id.clone();
                let provider = provider.to_owned();
                let model = model.to_owned();
                account_writer::execute(
                    store,
                    striatum_store::AccountCommandKind::Report,
                    move |tx| {
                        tx.release_inconclusive_probe(
                            &decision_id,
                            &provider,
                            &model,
                            observed_at_ms,
                        )
                    },
                )
                .map_err(|error| ModuleError::Message(error.to_string()))?;
            }
        }
    }
    match store.record_routing_outcome(
        &params.decision_id,
        &outcome,
        params.last_error.as_deref(),
        observed_at_ms,
    ) {
        Ok(()) => send_ok(writer, ver, channel, epoch, corr).await,
        Err(e) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("set_decision_outcome: {e}"),
            )
            .await
        }
    }
}

/// Provider reset evidence accompanies episode outcomes, including interruptions
/// on v1-selected workers. Keep it separate from model health and fence only the
/// route on the credential that served it, never the provider's other accounts.
pub(super) fn record_provider_reset_fence(
    store: &SharedStore,
    credentials: &SharedBrocaCredentials,
    decision_id: &str,
    outcome: &RouteOutcome,
    provider: Option<&striatum_core::route_state_store::ProviderOutcome>,
    account_exhausted: bool,
    credit_retry_at: Option<i64>,
    now: i64,
    quota: Option<&account_quota::AccountQuotaState>,
    config: Option<&config_overrides::LiveConfig>,
) -> Result<(), String> {
    if *outcome == RouteOutcome::Completed {
        return Ok(());
    }
    let Some(provider) = provider.filter(|p| p.credential_refusal.is_none()) else {
        return Ok(());
    };
    // A reset is evidence for an extra check, never an expiry. Zero represents
    // an unknown reset internally; the separate quota retry clock still runs.
    let reset = match provider.resets_at_ms {
        Some(reset)
            if (account_exhausted || reset > now)
                && account_quota::parse_rfc3339_ms(&account_selection::rfc3339(reset))
                    .is_some() =>
        {
            reset
        }
        Some(_) if account_exhausted => 0,
        None if account_exhausted => 0,
        // An ordinary interruption carrying an old or unreadable date is not
        // a new typed quota refusal. Never resurrect a wall from that date.
        _ => return Ok(()),
    };
    let Some(decision) = store
        .get_decision_by_id(decision_id)
        .map_err(|e| e.to_string())?
    else {
        return Ok(());
    };
    let (Some(provider_id), Some(model)) =
        (decision.selected_provider_id, decision.selected_model_id)
    else {
        return Ok(());
    };
    let frozen = store
        .decision_provider_identity(decision_id)
        .map_err(|e| e.to_string())?
        .and_then(|identity| identity.credential_id);
    let credential = if let Some(frozen) = frozen {
        frozen
    } else if let Some(reported) = provider.credential_id.as_deref() {
        // Without a frozen identity, accept only a listed credential that serves
        // the decision's model. Provider account aliases are not credential ids.
        let facts = store.get_facts().map_err(|e| e.to_string())?;
        let catalog = facts.iter().collect();
        let Some(fact) = facts
            .iter()
            .find(|fact| fact.provider_id == provider_id && fact.model_id == model)
        else {
            return Ok(());
        };
        let credentials = credentials.lock().map_err(|e| e.to_string())?;
        if !credentials.listed_credentials().iter().any(|row| {
            row.id == reported
                && broca_credentials::credential_serves_fact(row, fact, &catalog, &credentials)
        }) {
            return Ok(());
        }
        reported.to_owned()
    } else {
        return Ok(());
    };
    // A credit window explicitly identifies a spending wall. Without that
    // typed window, only fresh headroom can contradict quota exhaustion.
    // Preserve provider dates as evidence, never as fixed quota lifetimes.
    let non_quota = credit_retry_at.is_some()
        || quota.is_some_and(|quota| {
            credentials.lock().ok().is_some_and(|credentials| {
                credential_fences::quota_refusal_contradicted(
                    quota,
                    &credentials,
                    &credential,
                    now,
                    &config
                        .map(|config| config.allocator_policy(&provider_id))
                        .unwrap_or_default(),
                )
            })
        });
    let fence = striatum_store::AccountRouteFence {
        provider: provider_id,
        credential_id: credential,
        model,
        until_ms: if non_quota {
            let cap = now.saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS);
            if let Some(retry) = credit_retry_at {
                retry.min(cap)
            } else if reset > now {
                reset.min(cap)
            } else {
                cap
            }
        } else {
            reset
        },
        decision_id: decision_id.into(),
        recorded_at: now,
    };
    if non_quota {
        crate::account_writer::record_billing_account_route_fence(store, &fence)
    } else {
        crate::account_writer::record_account_route_fence(store, &fence)
    }
    .map_err(|e| e.to_string())
}

pub(super) async fn handle_record_usage(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: striatum_core::NativeUsageFacts =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(body) => body.params,
            Err(error) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("record_usage params not decodable: {error}"),
                )
                .await;
            }
        };
    if params.speed_sample {
        let result =
            usage_facts::speed_sample_observation_from_store(&params, store).and_then(|sample| {
                match sample {
                    Some(sample) => store
                        .record_speed_sample_delivery(&sample)
                        .map_err(|e| e.to_string()),
                    None => Ok(striatum_core::route_state_store::UsageRecordResult::default()),
                }
            });
        return match result {
            Ok(result) => {
                let body = serde_json::to_vec(&json!({ "result": {
                    "recorded": result.recorded, "usage_recorded": false,
                }}))
                .map_err(ModuleError::Json)?;
                let response = Frame::build_with_version(
                    ver,
                    FrameType::Response,
                    Flags::new(false, Priority::Interactive, false),
                    channel,
                    epoch,
                    corr,
                    body,
                )
                .map_err(|error| ModuleError::Message(error.to_string()))?;
                send(writer, response).await
            }
            Err(error) => {
                send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "store_error",
                    &format!("record_usage: {error}"),
                )
                .await
            }
        };
    }
    let observation = match usage_facts::usage_observation_from_store(params, store) {
        Ok(observation) => observation,
        Err(error) => {
            return send_route_error(writer, ver, channel, epoch, corr, "store_error", &error)
                .await;
        }
    };
    let Some(observation) = observation else {
        let body = serde_json::to_vec(
            &json!({ "result": { "recorded": false, "usage_recorded": false } }),
        )
        .map_err(ModuleError::Json)?;
        let response = Frame::build_with_version(
            ver,
            FrameType::Response,
            Flags::new(false, Priority::Interactive, false),
            channel,
            epoch,
            corr,
            body,
        )
        .map_err(|error| ModuleError::Message(error.to_string()))?;
        return send(writer, response).await;
    };
    match store.record_usage_delivery(&observation) {
        Ok(result) => {
            let body = serde_json::to_vec(&json!({ "result": {
                "recorded": result.recorded,
                "usage_recorded": result.usage_recorded,
            }}))
            .map_err(ModuleError::Json)?;
            let response = Frame::build_with_version(
                ver,
                FrameType::Response,
                Flags::new(false, Priority::Interactive, false),
                channel,
                epoch,
                corr,
                body,
            )
            .map_err(|error| ModuleError::Message(error.to_string()))?;
            send(writer, response).await
        }
        Err(error) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("record_usage: {error}"),
            )
            .await
        }
    }
}

pub(super) async fn handle_record_cooldown(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    credentials: &SharedBrocaCredentials,
    config_path: &Path,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: RecordCooldownParams =
        match serde_json::from_slice::<MethodParamsBody<_>>(&frame.body) {
            Ok(b) => b.params,
            Err(e) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_request",
                    &format!("record_cooldown params not decodable: {e}"),
                )
                .await;
            }
        };
    if let Some(message) = record_cooldown_params_error(&params) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            &message,
        )
        .await;
    }
    let recorded = if matches!(
        params.wall_class,
        Some(RouteWallClass::Quota | RouteWallClass::Billing)
    ) && params.credential_id.is_some()
    {
        let Some((provider, model)) = params.capacity_key.split_once('/') else {
            return send_ok(writer, ver, channel, epoch, corr).await;
        };
        let credential = params.credential_id.as_deref().unwrap();
        let now = now_ms() as i64;
        let config = config_overrides::live_config(config_path);
        let contradicted = params.wall_class == Some(RouteWallClass::Billing)
            || ACCOUNT_QUOTA
                .get()
                .and_then(|quota| quota.lock().ok())
                .is_some_and(|quota| {
                    credentials.lock().ok().is_some_and(|credentials| {
                        credential_fences::quota_refusal_contradicted(
                            &quota,
                            &credentials,
                            credential,
                            now,
                            &config.allocator_policy(provider),
                        )
                    })
                });
        let fence = striatum_store::AccountRouteFence {
            provider: provider.into(),
            credential_id: credential.into(),
            model: model.into(),
            until_ms: if contradicted {
                let cap =
                    now.saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS);
                params
                    .quota_exhausted_until
                    .or(params.cooldown_until)
                    .map(|at| at as i64)
                    .filter(|at| *at > now)
                    .map_or(cap, |at| at.min(cap))
            } else {
                params
                    .quota_exhausted_until
                    .or(params.cooldown_until)
                    .unwrap_or(0.0) as i64
            },
            decision_id: String::new(),
            recorded_at: now,
        };
        if contradicted {
            crate::account_writer::record_billing_account_route_fence(store, &fence)
        } else {
            crate::account_writer::record_account_route_fence(store, &fence)
        }
    } else {
        record_cooldown_update(store.as_ref(), &params)
    };
    match recorded {
        Ok(()) => send_ok(writer, ver, channel, epoch, corr).await,
        Err(e) => {
            send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("record_cooldown: {e}"),
            )
            .await
        }
    }
}
