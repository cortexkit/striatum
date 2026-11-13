//! Two-shape dispatch. Markers are checked by presence, not by their value.
//!
//! | Operation | Legacy marker | Contract marker | Markerless |
//! | --- | --- | --- | --- |
//! | select | targetAgent; project_root, workspace_root, tier_consumer on v1 | members in [`SELECT_MARKERS`]; never detail | v1 |
//! | select_panel | panelSpec, selection, roster and camelCase launch members | class, consumer, replace and select's shared contract members | v1 |
//! | explain | decisionID, decisionId, sendID, sendId, send_id | decision_id | v1 |
//! | account_status | none (same provider/account_id request) | same request; additive answer | v1 |
//! | account_for | caller | mode: continue; otherwise the legacy members overlap | v1 |
//! | account_observe | caller | decision_id, retry_at_ms, provider_code | v1 |
//!
//! Legacy-only bodies retain the old handler. Contract-only and mixed bodies
//! go to v1, whose closed-record checks refuse the legacy member by name.
//! Injected select context remains supported only without a new contract marker.
//! The legacy select accepts advisory `detail` too, so that member never
//! overrides `targetAgent` when choosing the request's surface.

use cortexkit_role_routing::{errors::Refusal, ops};
use serde_json::Value;

const SELECT_MARKERS: &[&str] = &[
    "settings",
    "pin_slot",
    "available_routes",
    "prefer_prompt_cache",
    "exclude",
    "exclude_providers",
    "runners",
    "affinity",
    "correlation_id",
    "task_ref",
    "hold_ok",
    "hold_wait_max_s",
    "prefer",
];
pub(super) const INJECTED: &[&str] = &["project_root", "workspace_root", "tier_consumer"];
pub(super) const NEW_SELECT: &[&str] = &[
    "settings",
    "pin_slot",
    "available_routes",
    "prefer_prompt_cache",
];
const PANEL_LEGACY: &[&str] = &[
    "panelSpec",
    "selection",
    "roster",
    "packageTokens",
    "availableModels",
    "excludeRouteKeys",
    "requiredCapabilities",
    "subscriptionKeys",
    "taskId",
    "parentSessionId",
    "projectRoot",
    "sendID",
    "substrate",
    "workspace_root",
    "workspaceRoot",
    "project_root",
];
const PANEL_CONTRACT: &[&str] = &[
    "class",
    "estimate",
    "consumer",
    "replace",
    "requirements",
    "runners",
    "available_routes",
    "exclude",
    "settings",
    "temperatures",
    "package_tokens",
    "task_ref",
    "correlation_id",
];
const EXPLAIN_LEGACY: &[&str] = &["decisionID", "decisionId", "sendID", "sendId", "send_id"];

fn has(params: &Value, names: &[&str]) -> bool {
    params
        .as_object()
        .is_some_and(|object| names.iter().any(|name| object.contains_key(*name)))
}

/// Persist the discriminator, not the selected handler: mixed requests must
/// remain visible even though the contract handler refuses them.
pub(super) fn census_shape(method: &str, params: &Value) -> &'static str {
    let (legacy, contract) = match method {
        ops::ROUTE_SELECT => {
            let injected = has(params, INJECTED);
            let target = has(params, &["targetAgent"]);
            (
                target || injected,
                has(params, NEW_SELECT)
                    || (!injected && has(params, &["consumer"]))
                    || (target && has(params, SELECT_MARKERS)),
            )
        }
        ops::ROUTE_SELECT_PANEL => (has(params, PANEL_LEGACY), has(params, PANEL_CONTRACT)),
        ops::ROUTE_EXPLAIN => (has(params, EXPLAIN_LEGACY), has(params, &["decision_id"])),
        ops::ROUTE_ACCOUNT_FOR => (has(params, &["caller"]), params["mode"] == "continue"),
        ops::ROUTE_ACCOUNT_OBSERVE => (
            has(params, &["caller"]),
            has(params, &["decision_id", "retry_at_ms", "provider_code"]),
        ),
        ops::ROUTE_SELECT_ENGINE
        | ops::ROUTE_ENGINE_REPORT
        | ops::ROUTE_ENGINE_EVAL
        | ops::ROUTE_REPORT
        | ops::ROUTE_ACCOUNT_STATUS
        | ops::ROUTE_HEAD_CHOICES
        | ops::ROUTE_BUDGET_QUOTE
        | ops::ROUTE_CONSUMER_MODELS
        | ops::ROLE_DESCRIBE => (false, true),
        _ => return "legacy",
    };
    match (legacy, contract) {
        (true, true) => "mixed",
        (true, false) => "legacy",
        (false, true) => "contract",
        (false, false) => "markerless",
    }
}

pub(super) fn claims(method: &str, params: &Value) -> bool {
    match method {
        ops::ROLE_DESCRIBE
        | ops::ROUTE_SELECT_ENGINE
        | ops::ROUTE_ENGINE_REPORT
        | ops::ROUTE_ENGINE_EVAL
        | ops::ROUTE_REPORT
        | ops::ROUTE_BUDGET_QUOTE
        | ops::ROUTE_ACCOUNT_STATUS
        | ops::ROUTE_HEAD_CHOICES => true,
        ops::ROUTE_CONSUMER_MODELS => true,
        ops::ROUTE_SELECT => !has(params, &["targetAgent"]) || has(params, SELECT_MARKERS),
        ops::ROUTE_SELECT_PANEL => !has(params, PANEL_LEGACY) || has(params, PANEL_CONTRACT),
        ops::ROUTE_EXPLAIN => !has(params, EXPLAIN_LEGACY) || has(params, &["decision_id"]),
        ops::ROUTE_ACCOUNT_FOR => !has(params, &["caller"]) || params["mode"] == "continue",
        ops::ROUTE_ACCOUNT_OBSERVE => {
            !has(params, &["caller"])
                || has(params, &["decision_id", "retry_at_ms", "provider_code"])
        }
        _ => false,
    }
}

/// Validate closed nested records with a fully qualified field in the refusal.
pub(super) fn closed(params: &Value, field: &str, known: &[&str]) -> Result<(), Refusal> {
    if let Some(value) = params.get(field) {
        if value.is_null() {
            return Ok(());
        }
        if let Err(mut refusal) = super::refuse_unknown_members(value, known) {
            let nested = refusal
                .detail
                .get("field")
                .and_then(Value::as_str)
                .unwrap_or("params");
            let path = if nested == "params" {
                field.to_owned()
            } else {
                format!("{field}.{nested}")
            };
            refusal.detail.insert("field".into(), Value::String(path));
            return Err(refusal);
        }
    }
    Ok(())
}

pub(super) fn closed_array(params: &Value, field: &str, known: &[&str]) -> Result<(), Refusal> {
    if let Some(elements) = params.get(field).and_then(Value::as_array) {
        for (index, value) in elements.iter().enumerate() {
            let name = format!("{field}[{index}]");
            closed(&serde_json::json!({name.clone():value}), &name, known)?;
        }
    }
    Ok(())
}

pub(super) fn settings(params: &Value) -> Result<(), Refusal> {
    closed(params, "settings", &["project_id", "tier_consumer"])
}
