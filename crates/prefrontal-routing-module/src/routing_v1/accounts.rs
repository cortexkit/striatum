//! The runner ops of `routing/v1`: `route.account_for` and
//! `route.account_observe`.
//!
//! Only a runner module may call them: a route whose principal the daemon
//! verified as `reserved:<module>`, for a module on the effective runner
//! registry. The calling runner comes from that principal, never
//! from the body. Both ops run on routing's account allocator, the one the
//! older ops serve Broca with; this module translates the v1 request into it
//! and its answer back.

use cortexkit_role_routing::account::{
    parse_account_for_request, Account, AccountForAnswer, AccountForRequest, AccountPin,
    AccountReason, AccountReasonCode, Admission,
};
use cortexkit_role_routing::errors::{Refusal, RefusalCode};
use cortexkit_role_routing::observe::{
    parse_account_observe_request, AccountObserveAnswer, Observation,
};
use prefrontal_routing_store::V1Recorded;
use serde_json::{json, Value};

use super::{account_ctx, now_ms, provider_refusal, store_refusal, Caller, Deps};
use crate::account_selection::{self, OpReply, ACCOUNT_FOR_OP, ACCOUNT_OBSERVE_OP};
use crate::{account_quota, config_overrides};

const FOR_MEMBERS: &[&str] = &[
    "provider",
    "model",
    "affinity_model",
    "conversation_key",
    "admission_id",
    "mode",
    "expected_pin",
    "initial_input_bytes",
    "initial_input_estimated",
    "candidates",
    "requested_account",
    "hold_wait_max_s",
    "decision_id",
    "parent_conversation_key",
    "pool",
];

const OBSERVE_MEMBERS: &[&str] = &[
    "event_id",
    "kind",
    "admission_id",
    "run_id",
    "served_account_id",
    "credential_id",
    "view_digest",
    "observed_at_ms",
    "upstream_origin",
    "headers",
    "limit_window",
    "resets_at_ms",
    "resets_at_source",
    "pool",
];

/// The calling runner's module, when the caller is an attested runner.
fn runner_module(caller: &Caller, deps: &Deps<'_>) -> Result<String, Refusal> {
    let runners = super::runners::effective(deps);
    caller
        .attested_module()
        .filter(|module| runners.get(module).is_some())
        .map(str::to_owned)
        .ok_or_else(|| {
            Refusal::new(
                RefusalCode::parse("unscoped_caller"),
                "only a runner module routing knows, on a route the daemon attested, may call this",
            )
        })
}

fn same_name(left: &str, right: &str) -> bool {
    left.trim().eq_ignore_ascii_case(right.trim())
}

pub(super) fn account_for(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    let module = runner_module(caller, deps)?;
    super::refuse_unknown_members(params, FOR_MEMBERS)?;
    let request = parse_account_for_request(params)?;
    // The whole body with defaults applied: two bodies that differ only in a
    // member left out versus sent with its default are the same admission.
    let body_json = serde_json::to_string(&request.normalized())
        .map_err(|error| provider_refusal("internal_error", error.to_string()))?;
    if let Some(first) = deps
        .store
        .v1_admission(&caller.principal, &request.admission_id)
        .map_err(store_refusal)?
    {
        return standing_answer(first, &body_json);
    }
    if let Some(decision_id) = &request.decision_id {
        // Honoured only from the runner the decision named, for the model it
        // named; anything else is refused as if the decision never existed.
        let decision = deps.store.v1_decision(decision_id).map_err(store_refusal)?;
        if !decision.is_some_and(|decision| {
            decision.runner_module == module
                && same_name(&decision.provider, &request.provider)
                && same_name(&decision.model, &request.model)
        }) {
            return Err(Refusal::unknown_decision());
        }
    }

    let mut legacy = json!({
        "provider": request.provider,
        "model": request.model,
        "conversation_key": request.conversation_key,
        "admission_id": request.admission_id,
        "mode": request.mode().as_str(),
        "initial_input_bytes": request.initial_input_bytes.unwrap_or(0),
        "initial_input_estimated": request.initial_input_estimated.unwrap_or(false),
        "caller": { "runner": module },
    });
    for (member, value) in [
        ("affinity_model", json!(request.affinity_model)),
        ("expected_pin", json!(request.expected_pin)),
        ("candidates", json!(request.candidates)),
        ("requested_account", json!(request.requested_account)),
        ("hold_wait_max_s", json!(request.hold_wait_max_s)),
        ("model_decision_id", json!(request.decision_id)),
        (
            "parent_conversation_key",
            params
                .get("parent_conversation_key")
                .cloned()
                .unwrap_or(Value::Null),
        ),
        ("pool", params.get("pool").cloned().unwrap_or(Value::Null)),
    ] {
        if !value.is_null() {
            legacy[member] = value;
        }
    }
    let body = serde_json::to_vec(&json!({ "method": ACCOUNT_FOR_OP, "params": legacy }))
        .map_err(|error| provider_refusal("internal_error", error.to_string()))?;
    let flags = config_overrides::live_config(deps.routing_config_path).account_flags();
    let policy =
        config_overrides::live_config(deps.routing_config_path).allocator_policy(&request.provider);
    let ctx = account_ctx(deps, flags, &policy)?;
    let answer = match account_selection::handle(ACCOUNT_FOR_OP, &body, &caller.principal, &ctx) {
        OpReply::Result(result) => admission_answer(&result, &request)?,
        OpReply::Error {
            code,
            message,
            detail,
        } => return Err(account_refusal(&code, message, detail.as_ref())),
    };
    let answer_json = serde_json::to_string(&answer)
        .map_err(|error| provider_refusal("internal_error", error.to_string()))?;
    let standing = deps
        .store
        .v1_record_admission(
            &caller.principal,
            &request.admission_id,
            &V1Recorded {
                body_json: body_json.clone(),
                answer_json,
            },
            now_ms() as i64,
        )
        .map_err(store_refusal)?;
    standing_answer(standing, &body_json)
}

/// The first admission's answer when `body_json` is the same body, and
/// `admission_conflict` when it is not.
fn standing_answer(first: V1Recorded, body_json: &str) -> Result<Value, Refusal> {
    if first.body_json != body_json {
        return Err(Refusal::new(
            RefusalCode::AdmissionConflict,
            "this admission_id was already used with a different body",
        ));
    }
    serde_json::from_str(&first.answer_json)
        .map_err(|error| store_refusal(format!("stored admission answer undecodable: {error}")))
}

/// The allocator's answer in v1 shape: times in Unix milliseconds, the pin's
/// mode as the request's mode, and the account named by public id only.
fn admission_answer(result: &Value, request: &AccountForRequest) -> Result<Value, Refusal> {
    let text = |value: &Value| value.as_str().map(str::to_owned);
    let ms = |value: &Value| {
        value
            .as_str()
            .and_then(account_quota::parse_rfc3339_ms)
            .map(|ms| ms.max(0) as u64)
    };
    let missing = |what: &str| {
        provider_refusal(
            "account_selection_unavailable",
            format!("the allocator's answer has no {what}"),
        )
    };
    let reason = &result["reason"];
    let reason_code = match &reason["code"] {
        Value::String(code) => code.clone(),
        Value::Object(object) => object
            .get("code")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| Value::Object(object.clone()).to_string()),
        other => other.to_string(),
    };
    let answer = AccountForAnswer {
        admission: Admission {
            id: request.admission_id.clone(),
            state: text(&result["admission"]["state"]).ok_or_else(|| missing("admission state"))?,
        },
        account: Account {
            provider: text(&result["account"]["provider"]).ok_or_else(|| missing("provider"))?,
            account_id: text(&result["account"]["account_id"])
                .ok_or_else(|| missing("account id"))?,
            credential_id: text(&result["account"]["credential_id"])
                .ok_or_else(|| missing("credential id"))?,
            method: text(&result["account"]["method"]).unwrap_or_else(|| "oauth".into()),
        },
        pin: AccountPin {
            id: text(&result["pin"]["id"]).ok_or_else(|| missing("pin id"))?,
            generation: result["pin"]["generation"]
                .as_u64()
                .ok_or_else(|| missing("pin generation"))?,
            affinity_expires_at_ms: ms(&result["pin"]["affinity_expires_at"])
                .ok_or_else(|| missing("affinity expiry"))?,
            admission_valid_until_ms: ms(&result["pin"]["admission_valid_until"])
                .ok_or_else(|| missing("admission validity"))?,
            lease_valid_until_ms: ms(&result["pin"]["lease_valid_until"])
                .ok_or_else(|| missing("lease validity"))?,
            mode: request.mode(),
        },
        reason: AccountReason {
            code: AccountReasonCode::parse(&reason_code),
            decision_id: request
                .decision_id
                .clone()
                .or_else(|| text(&reason["decision_id"]))
                .ok_or_else(|| missing("decision id"))?,
            policy_version: match &reason["policy_version"] {
                Value::String(version) => version.clone(),
                other => other.to_string(),
            },
            quota_fetched_at_ms: ms(&reason["quota_fetched_at"]),
            quota_age_band: text(&reason["quota_age_band"]).unwrap_or_else(|| "unknown".into()),
        },
    };
    serde_json::to_value(answer)
        .map_err(|error| provider_refusal("internal_error", error.to_string()))
}

/// The allocator's refusal in v1 shape. A hold and an exhausted pool are
/// timed by the wait the allocator states; every other refusal carries no
/// detail.
fn account_refusal(code: &str, message: String, detail: Option<&Value>) -> Refusal {
    let wait_until = || {
        detail
            .and_then(|detail| detail.get("retry_after_s"))
            .and_then(Value::as_u64)
            .map(|seconds| now_ms() + seconds * 1000)
    };
    let mut refusal = match code {
        "account_hold" => Refusal::account_hold(wait_until().unwrap_or_else(now_ms)),
        "account_pool_exhausted" => Refusal::account_pool_exhausted(wait_until()),
        "invalid_request" => Refusal::invalid_request("params", message.clone()),
        "store_error" => Refusal::new(RefusalCode::AccountSelectionUnavailable, message.clone()),
        other => Refusal::new(RefusalCode::parse(other), message.clone()),
    };
    refusal.message = message;
    if code == "no_eligible_account" {
        if let Some(reset) = detail
            .and_then(|detail| detail.get("reset_at_ms"))
            .filter(|reset| !reset.is_null())
        {
            refusal.detail.insert("reset_at_ms".into(), reset.clone());
        }
    }
    refusal
}

pub(super) fn account_observe(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    let module = runner_module(caller, deps)?;
    super::refuse_unknown_members(params, OBSERVE_MEMBERS)?;
    let request = parse_account_observe_request(params)?;
    // The allocator reads these two as closed sets of its own.
    if let Some(origin) = &request.upstream_origin {
        serde_json::from_value::<prefrontal_routing_core::accounts::evidence::FrameOrigin>(json!(
            origin
        ))
        .map_err(|_| {
            Refusal::invalid_request(
                "upstream_origin",
                format!("{origin:?} is not a frame origin"),
            )
        })?;
    }
    if let Some(source) = &request.resets_at_source {
        serde_json::from_value::<prefrontal_routing_core::accounts::evidence::ResetSource>(json!(
            source
        ))
        .map_err(|_| {
            Refusal::invalid_request(
                "resets_at_source",
                format!("{source:?} is not a reset source"),
            )
        })?;
    }
    let mut legacy = serde_json::to_value(&request)
        .map_err(|error| provider_refusal("internal_error", error.to_string()))?;
    legacy["caller"] = json!({ "runner": module });
    let body = serde_json::to_vec(&json!({ "method": ACCOUNT_OBSERVE_OP, "params": legacy }))
        .map_err(|error| provider_refusal("internal_error", error.to_string()))?;
    let flags = config_overrides::live_config(deps.routing_config_path).account_flags();
    let policy = config_overrides::live_config(deps.routing_config_path).allocator_policy("");
    let ctx = account_ctx(deps, flags, &policy)?;
    let reply = account_selection::handle(ACCOUNT_OBSERVE_OP, &body, &caller.principal, &ctx);
    // Rate-limit headers feed routing's quota view exactly as the older op's do.
    if let (Some(shared), Some(observation)) = (
        crate::USAGE_OBSERVE.get(),
        account_selection::usage_observation(&body, &caller.principal, &ctx),
    ) {
        shared.record(observation);
    }
    match reply {
        OpReply::Result(result) => observation_answer(&result),
        OpReply::Error {
            code,
            message,
            detail,
        } => Err(account_refusal(&code, message, detail.as_ref())),
    }
}

/// The allocator's observation in v1 shape. `effects` names the changes it
/// applied, one sentence each; a note that the observation changed nothing
/// is not an effect, and a repeated event applied nothing.
fn observation_answer(result: &Value) -> Result<Value, Refusal> {
    let observation = &result["observation"];
    let duplicate = observation["duplicate"].as_bool().unwrap_or(false);
    let effects = if duplicate {
        Vec::new()
    } else {
        observation["effects"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|effect| effect["effect"] != "advisory")
            .map(|effect| match effect {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            })
            .collect()
    };
    let answer = AccountObserveAnswer {
        observation: Observation {
            id: observation["id"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| provider_refusal("internal_error", "observation has no id"))?,
            duplicate,
            bound: observation["bound"].as_bool().unwrap_or(false),
            effects,
        },
    };
    serde_json::to_value(answer)
        .map_err(|error| provider_refusal("internal_error", error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repeated_observation_applies_nothing() {
        let answer = observation_answer(&json!({"observation": {
            "id": "aco_1", "duplicate": true, "bound": true,
            "effects": [{"effect": "fenced", "account_id": "a"}]
        }}))
        .unwrap();
        assert_eq!(answer["observation"]["effects"], json!([]));
        let first = observation_answer(&json!({"observation": {
            "id": "aco_1", "duplicate": false, "bound": false,
            "effects": [{"effect": "advisory", "why": "names no admission"}, {"effect": "fenced"}]
        }}))
        .unwrap();
        assert_eq!(
            first["observation"]["effects"],
            json!([r#"{"effect":"fenced"}"#])
        );
    }

    #[test]
    fn a_hold_is_timed_and_other_account_refusals_carry_no_detail() {
        let hold = account_refusal(
            "account_hold",
            "held".into(),
            Some(&json!({"code": "account_hold", "retry_after_s": 30, "decision_id": "x"})),
        );
        assert!(hold.wait_until_ms().is_some_and(|until| until > now_ms()));
        let conflict = account_refusal(
            "admission_conflict",
            "conflict".into(),
            Some(&json!({"code": "admission_conflict", "decision_id": "x", "exclusions": []})),
        );
        assert_eq!(conflict.code, RefusalCode::AdmissionConflict);
        assert!(conflict.detail.is_empty());
    }
}
