//! Operator inspection and recovery for recorded routing state.
//! These requests inspect decisions and lift specific fences without rerunning selection.

use super::*;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CooldownStatusParams {
    pub(super) route_key: String,
    pub(super) capacity_key: String,
    pub(super) now: i64,
}

#[derive(Debug, Default, Deserialize)]
struct RouteExplainParams {
    #[serde(
        default,
        rename = "decisionID",
        alias = "decisionId",
        alias = "decision_id"
    )]
    pub(super) decision_id: Option<String>,
    #[serde(default, rename = "sendID", alias = "sendId", alias = "send_id")]
    pub(super) send_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RouteDecisionsListParams {
    #[serde(default, rename = "sendID", alias = "sendId", alias = "send_id")]
    pub(super) send_id: Option<String>,
    #[serde(default, alias = "target_agent")]
    pub(super) target_agent: Option<String>,
    #[serde(default, alias = "route_key")]
    pub(super) route_key: Option<String>,
    #[serde(default, alias = "since_ms")]
    since_ms: Option<i64>,
    #[serde(default, alias = "until_ms")]
    pub(super) until_ms: Option<i64>,
    #[serde(default)]
    pub(super) limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearFailureBackoffParams {
    pub(super) route_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearCooldownParams {
    pub(super) route_key: String,
}

/// Decode persisted evidence without supplying a current-day fallback. If a legacy
/// row contains malformed JSON, return its exact text rather than inventing a value.
fn recorded_json(raw: &str) -> serde_json::Value {
    serde_json::from_str(raw).unwrap_or_else(|_| json!(raw))
}

fn recorded_optional_json(raw: Option<&str>) -> serde_json::Value {
    raw.map(recorded_json).unwrap_or(serde_json::Value::Null)
}

pub(super) async fn handle_cooldown_status(
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
    let params: CooldownStatusParams =
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
                    &format!("cooldown_status params not decodable: {error}"),
                )
                .await;
            }
        };
    // The provider row and the route-state snapshot both come from one
    // published account state, which holds every route and provider row.
    let blocks = match account_state::published(store, crate::account_selection::now_ms()) {
        Ok(blocks) => blocks,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("cooldown_status: {error}"),
            )
            .await;
        }
    };
    let provider_state = if let Some(provider_key) =
        derive_provider_key(&params.route_key).or_else(|| derive_provider_key(&params.capacity_key))
    {
        match blocks.provider_cooldown(provider_key) {
            Ok(state) => state,
            Err(error) => {
                return send_route_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "store_error",
                    &format!("cooldown_status provider lookup: {error}"),
                )
                .await;
            }
        }
    } else {
        None
    };
    let snapshot = match blocks.route_state_snapshot(params.now as f64) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("cooldown_status: {error}"),
            )
            .await;
        }
    };
    let cooled = striatum_core::route_state_store::route_cooled(
        provider_state.as_ref(),
        &snapshot,
        &params.route_key,
        &params.capacity_key,
        params.now as f64,
    );
    let body =
        serde_json::to_vec(&json!({"result": {"cooled": cooled}})).map_err(ModuleError::Json)?;
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

pub(super) async fn handle_route_explain(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    config_path: &Path,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let params: RouteExplainParams =
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
                    &format!("route.explain params not decodable: {error}"),
                )
                .await;
            }
        };
    let decision_id = params.decision_id.filter(|value| !value.trim().is_empty());
    let send_id = params.send_id.filter(|value| !value.trim().is_empty());
    let (key_name, key, lookup) = match (decision_id.as_deref(), send_id.as_deref()) {
        (Some(decision_id), None) => (
            "decisionID",
            decision_id,
            store.get_decision_by_id(decision_id),
        ),
        (None, Some(send_id)) => ("sendID", send_id, store.get_decision_by_send_id(send_id)),
        _ => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "invalid_request",
                "route.explain requires exactly one of decisionID or sendID",
            )
            .await;
        }
    };
    let decision = match lookup {
        Ok(Some(decision)) => decision,
        Ok(None) => {
            let retention_days =
                retention::retention_windows(config_path).model_route_decisions_days;
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "not_found",
                &format!(
                    "{key_name} '{key}' not found or pruned; retention is {retention_days} days"
                ),
            )
            .await;
        }
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("route.explain {key_name} lookup: {error}"),
            )
            .await;
        }
    };
    let outcome = decision.outcome.as_ref().map(|state| {
        json!({
            "state": state,
            "recordedAtMs": decision.outcome_at,
            "error": decision.outcome_error,
        })
    });
    let provider_identity = store
        .decision_provider_identity(&decision.id)
        .map_err(|error| ModuleError::Message(error.to_string()))?;
    let result = json!({
        "decisionID": decision.id,
        "providerIdentity": provider_identity,
        "createdAtMs": decision.created_at,
        "decisionKind": decision.decision_kind,
        "selectedRoute": {
            "routeKey": decision.selected_route_key,
            "providerID": decision.selected_provider_id,
            "modelID": decision.selected_model_id,
            "variant": decision.selected_variant,
            "harness": decision.selected_harness,
            "workload": decision.selected_workload,
        },
        "why": {
            "requirements": recorded_json(&decision.requirements_json),
            "allowed_providers": recorded_optional_json(decision.allowed_providers_json.as_deref()),
            "score": recorded_json(&decision.score_json),
            "explanation": recorded_optional_json(decision.explanation_json.as_deref()),
            // This is the persisted rejection payload, not a new scoring pass.
            "rejections": recorded_json(&decision.rejected_json),
        },
        "quotaSignalSnapshot": recorded_optional_json(decision.quota_signal_json.as_deref()),
        // Past the full-detail window the score keeps only the picked and
        // adequate candidates and the quota snapshot only its header; every
        // other member is as recorded. `strippedAtMs` says when that happened.
        "detailRetention": {
            "detail": if decision.detail_stripped_at.is_some() { "compact" } else { "full" },
            "strippedAtMs": decision.detail_stripped_at,
        },
        "catalogFingerprint": decision.catalog_fingerprint,
        "policyRevision": decision.policy_revision,
        "anchorVersion": decision.anchor_version,
        "provenance": {
            "sendID": decision.send_id,
            "taskID": decision.task_id,
            "parentSessionID": decision.parent_session_id,
            "targetAgent": decision.target_agent,
            "harness": decision.harness,
            "projectRoot": decision.project_root,
        },
        "outcome": outcome,
    });
    let body = serde_json::to_vec(&json!({ "result": result })).map_err(ModuleError::Json)?;
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

pub(super) async fn handle_route_decisions_list(
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
    let params: RouteDecisionsListParams =
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
                    &format!("route.decisions_list params not decodable: {error}"),
                )
                .await;
            }
        };
    let limit = params.limit.unwrap_or(20);
    if !(1..=100).contains(&limit) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "route.decisions_list limit must be between 1 and 100",
        )
        .await;
    }
    if params
        .since_ms
        .zip(params.until_ms)
        .is_some_and(|(since_ms, until_ms)| since_ms > until_ms)
    {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "route.decisions_list sinceMs must not be after untilMs",
        )
        .await;
    }
    let decisions = match store.list_decisions(&DecisionListFilter {
        send_id: params.send_id.filter(|value| !value.trim().is_empty()),
        target_agent: params.target_agent.filter(|value| !value.trim().is_empty()),
        route_key: params.route_key.filter(|value| !value.trim().is_empty()),
        since_ms: params.since_ms,
        until_ms: params.until_ms,
        limit,
    }) {
        Ok(decisions) => decisions,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("route.decisions_list: {error}"),
            )
            .await;
        }
    };
    let rows: Vec<serde_json::Value> = decisions
        .into_iter()
        .map(|decision| {
            json!({
                "decisionID": decision.id,
                "createdAtMs": decision.created_at,
                "selectedRouteKey": decision.selected_route_key,
                "decisionKind": decision.decision_kind,
                "topRejectionReason": decision.top_rejection_reason,
                "topRejectionCount": decision.top_rejection_count,
            })
        })
        .collect();
    let body = serde_json::to_vec(&json!({ "result": { "decisions": rows } }))
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

pub(super) async fn handle_clear_failure_backoff(
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
    let params: ClearFailureBackoffParams =
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
                    &format!("route.clear_failure_backoff params not decodable: {error}"),
                )
                .await;
            }
        };
    if params.route_key.trim().is_empty() {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "route.clear_failure_backoff routeKey must not be empty",
        )
        .await;
    }
    let prior = match crate::account_writer::clear_failure_backoff(store, &params.route_key) {
        Ok(prior) => prior,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("route.clear_failure_backoff: {error}"),
            )
            .await;
        }
    };
    let prior = prior.map(|state| {
        json!({
            "consecutiveTerminalFailures": state.consecutive_terminal_failures,
            "failureBackoffUntil": state.failure_backoff_until,
            "failureBackoffReason": state.failure_backoff_reason,
        })
    });
    let body =
        serde_json::to_vec(&json!({ "result": { "prior": prior } })).map_err(ModuleError::Json)?;
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

pub(super) async fn handle_clear_cooldown(
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
    let params: ClearCooldownParams =
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
                    &format!("route.clear_cooldown params not decodable: {error}"),
                )
                .await;
            }
        };
    if params.route_key.trim().is_empty() {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "route.clear_cooldown routeKey must not be empty",
        )
        .await;
    }
    let prior = match crate::account_writer::clear_route_cooldowns(store, &params.route_key) {
        Ok(Some(prior)) => prior,
        Ok(None) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "unknown_route",
                &format!("unknown route key {:?}", params.route_key),
            )
            .await;
        }
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("route.clear_cooldown: {error}"),
            )
            .await;
        }
    };
    let cooldown = prior
        .routes
        .iter()
        .filter_map(|(_, row)| row.cooldown_until)
        .max_by(f64::total_cmp);
    let quota = prior
        .routes
        .iter()
        .filter_map(|(_, row)| row.quota_exhausted_until)
        .max_by(f64::total_cmp);
    let body = serde_json::to_vec(&json!({ "result": {
        "routeKey": prior.route_key,
        "clearedCooldownUntil": cooldown,
        "clearedQuotaExhaustedUntil": quota,
        "clearedRoutes": prior.routes.iter().map(|(key, row)| json!({
            "routeKey": key, "clearedCooldownUntil": row.cooldown_until,
            "clearedQuotaExhaustedUntil": row.quota_exhausted_until,
        })).collect::<Vec<_>>(),
        "clearedAccountFences": prior.account_fences.iter().map(|fence| json!({
            "provider": fence.provider, "model": fence.model,
            "credentialId": fence.credential_id, "clearedUntil": fence.until_ms,
        })).collect::<Vec<_>>(),
    } }))
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearCredentialFencesParams {
    pub(super) credential_id: String,
}

/// Whether the route's daemon-attested principal may lift credential fences:
/// the operator on a shell on this machine (`direct`) or on the phone
/// (`reserved:callosum`). Every other module principal, and unverified and
/// unattested routes, are refused: lifting a fence sends the next request back
/// to a credential the provider refused, which only the operator may order
/// after topping it up.
pub(super) fn is_fence_lift_operator(principal: &str) -> bool {
    principal == "direct" || principal == "reserved:callosum"
}

/// Lift every credential entitlement fence on one named vault credential at
/// once, for use right after topping it up. Operator-only (see
/// [`is_fence_lift_operator`]); the reply names what was lifted.
pub(super) async fn handle_clear_credential_fences(
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
    let principal = route_principal(channel, epoch);
    if !is_fence_lift_operator(&principal) {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "not_permitted",
            &format!(
                "route.clear_credential_fences is operator-only: principal '{principal}' may not lift credential fences",
            ),
        )
        .await;
    }
    let params: ClearCredentialFencesParams =
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
                    &format!("route.clear_credential_fences params not decodable: {error}"),
                )
                .await;
            }
        };
    if params.credential_id.trim().is_empty() {
        return send_route_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "invalid_request",
            "route.clear_credential_fences credentialId must not be empty",
        )
        .await;
    }
    let lifted = match credential_fences::lift_for_operator(store, &params.credential_id) {
        Ok(lifted) => lifted,
        Err(error) => {
            return send_route_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "store_error",
                &format!("route.clear_credential_fences: {error}"),
            )
            .await;
        }
    };
    let body = serde_json::to_vec(&json!({ "result": {
        "credentialId": params.credential_id,
        "lifted": lifted.iter().map(|fence| json!({
            "providerId": fence.provider_id,
            "code": fence.code,
        })).collect::<Vec<_>>(),
    } }))
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
