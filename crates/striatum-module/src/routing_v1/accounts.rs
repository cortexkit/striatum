//! The runner ops of `routing/v1`: `route.account_for` and
//! `route.account_observe`.
//!
//! Calls require an attested effective runner or a current session scope whose
//! owner provides `agent-host/v1` in the daemon catalog. Neither a plugin name
//! nor a source in the body grants authority. Both ops use the account allocator
//! older ops serve Broca with; this module translates the v1 request into it
//! and its answer back.

use cortexkit_role_routing::account::{
    parse_account_for_request_serving, Account, AccountForAnswer, AccountForRequest, AccountPin,
    AccountReason, AccountReasonCode, Admission,
};
use cortexkit_role_routing::errors::{Refusal, RefusalCode};
use cortexkit_role_routing::observe::{
    parse_account_observe_request_serving, AccountObserveAnswer, Observation,
};
use cortexkit_role_routing::service_tier::{check_chosen, ServiceTier};
use striatum_store::V1Recorded;
use serde_json::{json, Value};

use super::{account_ctx, now_ms, provider_refusal, Caller, Deps};
use crate::account_refusal_log::{because, checking};
use crate::account_selection::{self, OpReply, ACCOUNT_FOR_OP};
use crate::{account_quota, config_overrides};

/// A store or serialization failure inside `route.account_for`. The contract
/// has no `store_error` or `internal_error` for this op; a router that cannot
/// read or render its state cannot choose right now, which is the transient
/// `account_selection_unavailable`.
fn account_unavailable(message: impl Into<String>) -> Refusal {
    Refusal::new(RefusalCode::AccountSelectionUnavailable, message.into())
}

fn store_unavailable(error: impl std::fmt::Display) -> Refusal {
    account_unavailable(format!("route-state store: {error}"))
}

fn internal_unavailable(error: impl std::fmt::Display) -> Refusal {
    account_unavailable(error.to_string())
}

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
    "warm_credential",
    "hold_wait_max_s",
    "decision_id",
    "parent_conversation_key",
    "pool",
    "hold_ok",
    "request_input_tokens",
    "service_tier",
];

const OBSERVE_MEMBERS: &[&str] = &[
    "decision_id",
    "retry_at_ms",
    "provider_code",
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

struct AccountCaller<'a> {
    module: &'a str,
    principal: String,
    session: Option<&'a super::ScopeAuthority>,
}

impl AccountCaller<'_> {
    fn admission_key(&self, id: &str) -> String {
        self.session.map_or_else(
            || id.to_owned(),
            |scope| serde_json::to_string(&(&scope.reference, id)).unwrap(),
        )
    }

    fn recorded_principal(&self) -> String {
        self.session.map_or_else(
            || self.principal.clone(),
            |scope| {
                format!(
                    "session:{}",
                    serde_json::to_string(&(&scope.owner, &scope.reference)).unwrap()
                )
            },
        )
    }
}

/// Session ids are scoped under their owner, not the relay's principal. The
/// allocator hashes the stamped conversation ref within that namespace, so a
/// child can look up its parent's reported use without sharing admission ids.
fn account_caller<'a>(caller: &'a Caller, deps: &Deps<'_>) -> Result<AccountCaller<'a>, Refusal> {
    checking("caller_check");
    let runners = super::operator_config(deps).v1_runners();
    if caller.binding_is_current() {
        if let Some(module) = caller.attested_module().filter(|module| {
            runners.iter().any(|runner| runner.module == *module)
                || super::consumers::is_media_provider(module)
                || deps
                    .broca_credentials
                    .lock()
                    .is_ok_and(|credentials| credentials.classifier_models.contains_key(*module))
        }) {
            return Ok(AccountCaller {
                module,
                principal: caller.principal.clone(),
                session: None,
            });
        }
        if let Some(scope) = caller.scope_authority.as_ref().filter(|scope| {
            scope.kind == subc_protocol::scope::ScopeKind::Head
                && scope
                    .owner
                    .strip_prefix("reserved:")
                    .is_some_and(super::consumers::provides_agent_host)
        }) {
            return Ok(AccountCaller {
                module: scope.owner.strip_prefix("reserved:").unwrap(),
                principal: format!(
                    "session-owner:{}",
                    serde_json::to_string(&scope.owner).unwrap()
                ),
                session: Some(scope),
            });
        }
    }
    Err(Refusal::new(
        // The contract answers any caller that is not an attested runner
        // module as `account_not_authorized`; it has no unscoped-caller code.
        RefusalCode::AccountNotAuthorized,
        "account operations require an effective runner or a current agent-host session scope",
    ))
}

pub(super) fn admission_runner(caller: &Caller, deps: &Deps<'_>) -> Option<String> {
    account_caller(caller, deps)
        .ok()
        .map(|authority| authority.module.to_owned())
}

fn same_name(left: &str, right: &str) -> bool {
    left.trim().eq_ignore_ascii_case(right.trim())
}

/// Observation requests omit the provider. Resolve it only for a slow log,
/// using the caller's recorded principal and admission key, as admission does.
pub(super) fn slow_call_provider(
    operation: &str,
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> String {
    if let Some(provider) = params["provider"]
        .as_str()
        .filter(|_| operation == "route.account_for")
    {
        return provider.to_owned();
    }
    if let Some(id) = params["decision_id"].as_str() {
        if let Ok(Some(decision)) = deps.store.v1_decision(id) {
            return decision.provider;
        }
    }
    if let (Some(id), Ok(authority)) = (
        params["admission_id"].as_str(),
        account_caller(caller, deps),
    ) {
        if let Ok(Some(admission)) = deps.store.v1_admission(
            &authority.recorded_principal(),
            &authority.admission_key(id),
        ) {
            if let Ok(body) = serde_json::from_str::<Value>(&admission.body_json) {
                if let Some(provider) = body["provider"].as_str() {
                    return provider.to_owned();
                }
            }
        }
    }
    "unavailable".into()
}

pub(super) fn account_for(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    crate::account_refusal_log::record("route.account_for", &caller.principal, params, || {
        account_for_inner(params, caller, deps)
    })
}

fn account_for_inner(params: &Value, caller: &Caller, deps: &Deps<'_>) -> Result<Value, Refusal> {
    let authority = account_caller(caller, deps)?;
    if params["mode"] == "continue" {
        if super::consumers::is_media_provider(authority.module) {
            return Err(super::provider_refusal(
                "account_selection_unavailable",
                "Media engine jobs don't continue an earlier admission. Admit each job with its own engine decision (mode admit).",
            ));
        }
        return continue_account(params, caller, &authority, deps);
    }
    checking("account_for_shape");
    super::refuse_unknown_members(params, FOR_MEMBERS)?;
    super::shapes::closed(params, "expected_pin", &["id", "generation"])?;
    super::shapes::closed(
        params,
        "requested_account",
        &["account_id", "credential_id"],
    )?;
    super::shapes::closed(params, "candidates", &["view_digest", "accounts"])?;
    super::shapes::closed_array(
        &params["candidates"],
        "accounts",
        &["account_id", "credential_id"],
    )
    .map_err(|mut refusal| {
        if let Some(field) = refusal.detail.get("field").and_then(Value::as_str) {
            let field = format!("candidates.{field}");
            refusal.detail.insert("field".into(), json!(field));
        }
        refusal
    })?;
    checking("account_for_parse");
    let mut request = parse_account_for_request_serving(params, super::describe::CAPABILITIES)?;
    let media_caller = super::consumers::is_media_provider(authority.module);
    if media_caller
        && !super::consumers::media_pair(authority.module, &request.provider, &request.model)
    {
        return Err(super::provider_refusal(
            "account_not_authorized",
            "This provider and model aren't a remote engine your module declared in its verified media slot, so routing won't choose an account for it.",
        ));
    }
    if let Some(scope) = authority.session {
        request.conversation_key = scope.reference.clone();
    }
    let principal = authority.recorded_principal();
    let mut hold_wait_max_s = request
        .hold_wait_max_s
        .unwrap_or(account_selection::DEFAULT_HOLD_WAIT_S);
    // The whole body with defaults applied: two bodies that differ only in a
    // member left out versus sent with its default are the same admission.
    let body_json = serde_json::to_string(&request.normalized()).map_err(internal_unavailable)?;
    checking("admission_replay");
    if let Some(first) = deps
        .store
        .v1_admission(&principal, &request.admission_id)
        .map_err(store_unavailable)?
    {
        return replay_admission(first, &body_json, &request, &authority, deps);
    }
    let mut bound_tier = None;
    let mut engine_decision = false;
    if let Some(decision_id) = &request.decision_id {
        checking("decision_binding");
        // Honoured only from the runner the decision named, for the model it
        // named; anything else is refused as if the decision never existed.
        let decision = deps
            .store
            .v1_decision(decision_id)
            .map_err(store_unavailable)?;
        engine_decision =
            super::engines::is_decision(deps, decision_id).map_err(store_unavailable)?;
        if !decision.is_some_and(|decision| {
            caller.attested_module() == Some(decision.runner_module.as_str())
                && same_name(&decision.provider, &request.provider)
                && same_name(&decision.model, &request.model)
                && (!engine_decision || super::engines::now(deps) < decision.expires_at_ms as u64)
        }) {
            return Err(because("unknown_decision", Refusal::unknown_decision()));
        }
        bound_tier = deps
            .store
            .decision_service_tier(decision_id)
            .map_err(store_unavailable)?
            .map(|(tier, _)| tier);
        if bound_tier.as_deref().is_some_and(|tier| {
            !check_chosen(
                request.service_tier.or_else(|| ServiceTier::parse(tier)),
                Some(tier),
            )
        }) {
            checking("bound_service_tier");
            return Err(account_unavailable(
                "A request bound to an earlier routing decision keeps that decision's service tier. Send the same tier, or ask routing for a new decision to change it.",
            ));
        }
        if let Some(input) = deps
            .store
            .get_decision_by_id(decision_id)
            .map_err(store_unavailable)?
        {
            let input: Value =
                serde_json::from_str(&input.requirements_json).map_err(store_unavailable)?;
            if let Some(cap) = input["hold_wait_max_s"].as_u64() {
                hold_wait_max_s = hold_wait_max_s.min(cap);
            }
        }
    }

    let mut legacy = json!({
        "provider": request.provider,
        "model": request.model,
        "conversation_key": request.conversation_key,
        "admission_id": authority.admission_key(&request.admission_id),
        "mode": request.mode().as_str(),
        "initial_input_bytes": request.initial_input_bytes.unwrap_or(0),
        "initial_input_estimated": request.initial_input_estimated.unwrap_or(false),
        "caller": { "runner": authority.module },
    });
    for (member, value) in [
        ("affinity_model", json!(request.affinity_model)),
        ("expected_pin", json!(request.expected_pin)),
        ("candidates", json!(request.candidates)),
        ("requested_account", json!(request.requested_account)),
        ("warm_credential", json!(request.warm_credential)),
        ("hold_wait_max_s", json!(hold_wait_max_s)),
        ("model_decision_id", json!(request.decision_id)),
        ("service_tier", json!(request.service_tier)),
        (
            "parent_conversation_key",
            params
                .get("parent_conversation_key")
                .cloned()
                .unwrap_or(Value::Null),
        ),
        ("pool", params.get("pool").cloned().unwrap_or(Value::Null)),
        (
            "hold_ok",
            params.get("hold_ok").cloned().unwrap_or(Value::Null),
        ),
        (
            "request_input_tokens",
            params
                .get("request_input_tokens")
                .cloned()
                .unwrap_or(Value::Null),
        ),
    ] {
        if !value.is_null() {
            legacy[member] = value;
        }
    }
    let body = serde_json::to_vec(&json!({ "method": ACCOUNT_FOR_OP, "params": legacy }))
        .map_err(internal_unavailable)?;
    let flags = config_overrides::live_config(deps.routing_config_path).account_flags();
    let policy =
        config_overrides::live_config(deps.routing_config_path).allocator_policy(&request.provider);
    // The shared context reports a store failure; for `account_for` that is
    // transient unavailability, not the open `store_error` code.
    checking("account_context");
    let mut ctx = account_ctx(deps, flags, &policy)
        .map_err(|refusal| account_unavailable(refusal.message))?;
    if engine_decision || media_caller {
        let credentials = deps.broca_credentials.lock().map_err(store_unavailable)?;
        if !credentials.media_listing_ready(now_ms()) {
            return Err(super::provider_refusal(
                "account_selection_unavailable",
                "Routing can't read the media-provider credential list right now: its listing grant is missing or the last listing is stale or failed. Retry shortly; if it persists, the vault's media-provider list grant for routing needs checking.",
            ));
        }
        if !credentials
            .listed_credentials()
            .iter()
            .any(|row| crate::account_selection::media_credential_serves(row, &request.provider))
        {
            return Err(super::provider_refusal(
                "auth_required",
                "No active API key in the vault's media-provider category serves this engine's provider. Add or enable one before using this engine.",
            ));
        }
        drop(credentials);
        ctx.classes
            .as_mut()
            .ok_or_else(|| account_unavailable("Routing's account context isn't loaded yet, so it can't choose a media account. Retry shortly."))?
            .for_engine(&request.provider, &request.model)
            .map_err(store_unavailable)?;
    }
    let answer = match account_selection::handle(ACCOUNT_FOR_OP, &body, &authority.principal, &ctx)
    {
        OpReply::Result(result) => admission_answer(&result, &request)?,
        OpReply::Error {
            code,
            message,
            detail,
        } => return Err(account_refusal(&code, message, detail.as_ref())),
    };
    let chosen = answer["service_tier_chosen"].as_str();
    let effective_tier = bound_tier
        .as_deref()
        .filter(|tier| *tier != "standard")
        .and_then(ServiceTier::parse)
        .or(request.service_tier);
    // A decision fixes the service tier; the general contract check permits a
    // step-down, so decision-bound admissions must also require equality.
    if !check_chosen(effective_tier, chosen)
        || bound_tier
            .as_deref()
            .is_some_and(|tier| tier != "standard" && chosen != Some(tier))
    {
        checking("chosen_service_tier");
        let mut refusal = account_unavailable(
            "No account can serve this routing decision's service tier any more, and a bound decision never steps down to a lower tier. Ask routing for a new decision.",
        );
        let detail = json!({
            "code": "account_selection_unavailable",
            "retry_after_s": null,
            "decision_id": request.decision_id,
            "exclusions": request.candidates.iter().flat_map(|candidates| &candidates.accounts)
                .map(|candidate| json!({"account_id":candidate.account_id,"reason":"bound_service_tier_unavailable"}))
                .collect::<Vec<_>>(),
        });
        refusal.detail.extend(detail.as_object().unwrap().clone());
        return Err(refusal);
    }
    let answer_json = serde_json::to_string(&answer).map_err(internal_unavailable)?;
    checking("admission_record");
    let standing = deps
        .store
        .v1_record_admission(
            &principal,
            &request.admission_id,
            &V1Recorded {
                body_json: body_json.clone(),
                answer_json,
            },
            now_ms() as i64,
        )
        .map_err(store_unavailable)?;
    replay_admission(standing, &body_json, &request, &authority, deps)
}

fn continue_account(
    params: &Value,
    caller: &Caller,
    authority: &AccountCaller<'_>,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    checking("continuation_parse");
    super::refuse_unknown_members(
        params,
        &[
            "mode",
            "run_id",
            "admission_id",
            "conversation_key",
            "previous_decision_id",
            "exhausted",
            "selection_id",
        ],
    )?;
    super::shapes::closed(
        params,
        "exhausted",
        &[
            "credential_id",
            "account",
            "limit_window",
            "pool",
            "resets_at_ms",
            "retry_after_s",
            "observed_at_ms",
        ],
    )?;
    let describe =
        serde_json::from_value(super::describe::describe(deps)?).map_err(internal_unavailable)?;
    let request =
        cortexkit_role_routing::account_continue::parse_continue_request(params, &describe)?;
    let flags = config_overrides::live_config(deps.routing_config_path).account_flags();
    let policy = config_overrides::live_config(deps.routing_config_path).allocator_policy("");
    checking("account_context");
    let ctx = account_ctx(deps, flags, &policy).map_err(|r| account_unavailable(r.message))?;
    checking("continuation_allocation");
    account_selection::continue_account(
        &request,
        &authority.principal,
        authority.module,
        caller
            .scope
            .as_ref()
            .and_then(|scope| scope.project_id.as_deref()),
        &ctx,
    )
}

/// The first admission's answer when `body_json` is the same body, and
/// `admission_conflict` when it is not.
fn standing_answer(first: V1Recorded, body_json: &str) -> Result<Value, Refusal> {
    checking("admission_replay");
    if first.body_json != body_json {
        return Err(Refusal::new(
            RefusalCode::AdmissionConflict,
            "this admission_id was already used with a different body",
        ));
    }
    serde_json::from_str(&first.answer_json)
        .map_err(|error| store_unavailable(format!("stored admission answer undecodable: {error}")))
}

fn replay_admission(
    first: V1Recorded,
    body_json: &str,
    request: &AccountForRequest,
    authority: &AccountCaller<'_>,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    let original = standing_answer(first, body_json)?;
    if request.mode().as_str() != "admit" {
        return Ok(original);
    }
    let current = deps
        .store
        .account_transaction(|tx| {
            let id = authority.admission_key(&request.admission_id);
            if !tx
                .admission(&authority.principal, &id)?
                .is_some_and(|row| row.state == "started")
            {
                return Ok(None);
            }
            tx.latest_used_continuation(&authority.principal, &id)
        })
        .map_err(store_unavailable)?;
    let Some(current) = current else {
        return Ok(original);
    };
    // Keep the public admission id while returning the serving decision and pin.
    let mut result: Value = serde_json::from_str(&current.reply_json).map_err(store_unavailable)?;
    result["admission"]["state"] = json!(current.state);
    let mut request = request.clone();
    request.decision_id = Some(current.decision_id);
    admission_answer(&result, &request)
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
    let mut answer = AccountForAnswer::new(
        Admission {
            id: request.admission_id.clone(),
            state: text(&result["admission"]["state"]).ok_or_else(|| missing("admission state"))?,
        },
        Account {
            provider: text(&result["account"]["provider"]).ok_or_else(|| missing("provider"))?,
            account_id: text(&result["account"]["account_id"])
                .ok_or_else(|| missing("account id"))?,
            credential_id: text(&result["account"]["credential_id"])
                .ok_or_else(|| missing("credential id"))?,
            method: text(&result["account"]["method"]).unwrap_or_else(|| "oauth".into()),
        },
        AccountPin::new(
            text(&result["pin"]["id"]).ok_or_else(|| missing("pin id"))?,
            result["pin"]["generation"]
                .as_u64()
                .ok_or_else(|| missing("pin generation"))?,
            ms(&result["pin"]["affinity_expires_at"]).ok_or_else(|| missing("affinity expiry"))?,
            ms(&result["pin"]["admission_valid_until"])
                .ok_or_else(|| missing("admission validity"))?,
            ms(&result["pin"]["lease_valid_until"]).ok_or_else(|| missing("lease validity"))?,
            request.mode(),
        ),
        AccountReason {
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
    );
    if let Some(tier) = result["service_tier_chosen"]
        .as_str()
        .and_then(ServiceTier::parse)
    {
        answer = answer.with_service_tier_chosen(tier);
    }
    serde_json::to_value(answer).map_err(internal_unavailable)
}

/// The allocator's refusal in v1 shape. A hold and an unavailable quota pool are
/// timed by the wait the allocator states. Account-selection unavailability
/// also preserves candidate exclusions and an optional short retry delay.
/// Legacy codes the contract does not declare (`store_error`,
/// `internal_error`, `no_eligible_account`, `unscoped_caller`) become the
/// transient `account_selection_unavailable`. `account_selection_disabled`
/// is preserved verbatim so it stays distinguishable from quota or
/// authentication refusals; the contract declares it (0.5.2) as untimed
/// with empty detail. A legacy `account_selection_unsupported` that reaches
/// this adapter means no credential of the right kind served the request,
/// so it becomes the untimed `auth_required`, never a transient retry.
/// This arm is the backstop.
fn account_refusal(code: &str, message: String, detail: Option<&Value>) -> Refusal {
    if let Some(detail) = detail {
        crate::account_refusal_log::allocator_refusal(detail);
    }
    let wait_until = || {
        detail
            .and_then(|detail| detail.get("retry_after_s"))
            .and_then(Value::as_u64)
            .map(|seconds| now_ms() + seconds * 1000)
    };
    let mut refusal = match code {
        "account_hold" => Refusal::account_hold(wait_until().unwrap_or_else(now_ms)),
        "account_pool_exhausted" => Refusal::account_pool_exhausted(
            // A route fence states its end exactly; the allocator's own
            // exhaustion states only a wait.
            detail
                .and_then(|detail| detail.get("reset_at_ms"))
                .and_then(Value::as_u64)
                .or_else(wait_until),
        ),
        "account_pool_reserve_limited" => {
            let mut refusal = Refusal::new(RefusalCode::parse(code), message.clone());
            if let Some(reset) = detail
                .and_then(|detail| detail.get("reset_at_ms"))
                .and_then(Value::as_u64)
                .or_else(wait_until)
            {
                refusal.detail.insert("reset_at_ms".into(), json!(reset));
            }
            refusal
        }
        "invalid_request" => Refusal::invalid_request(
            detail
                .and_then(|detail| detail.get("field"))
                .and_then(Value::as_str)
                .unwrap_or("params"),
            message.clone(),
        ),
        // Declared (0.5.2) and untimed: module-wide, use your own credential.
        "account_selection_disabled" => {
            Refusal::new(RefusalCode::AccountSelectionDisabled, message.clone())
        }
        "account_selection_unsupported" => Refusal::new(RefusalCode::AuthRequired, message.clone()),
        "account_selection_unavailable" => {
            let mut refusal = account_unavailable(message.clone());
            if let Some(detail) = detail.and_then(Value::as_object) {
                for key in [
                    "code",
                    "retry_after_s",
                    "decision_id",
                    "exclusions",
                    "retry_after_ms",
                ] {
                    if let Some(value) = detail.get(key) {
                        refusal.detail.insert(key.into(), value.clone());
                    }
                }
            }
            refusal
        }
        "store_error" | "internal_error" | "no_eligible_account" | "unscoped_caller" => {
            account_unavailable(message.clone())
        }
        other => Refusal::new(RefusalCode::parse(other), message.clone()),
    };
    if matches!(
        code,
        "account_pool_exhausted" | "account_pool_reserve_limited"
    ) {
        if let Some(detail) = detail.and_then(Value::as_object) {
            for key in [
                "code",
                "retry_after_s",
                "decision_id",
                "exclusions",
                "reset_at_ms",
            ] {
                if let Some(value) = detail.get(key) {
                    refusal.detail.insert(key.into(), value.clone());
                }
            }
        }
    }
    refusal.message = message;
    refusal
}

pub(super) fn account_observe(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    crate::account_refusal_log::record("route.account_observe", &caller.principal, params, || {
        account_observe_inner(params, caller, deps)
    })
}

fn account_observe_inner(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    let runner_authority = account_caller(caller, deps);
    if let Err(refusal) = &runner_authority {
        if params["decision_id"].as_str().is_none() || params["admission_id"].as_str().is_none() {
            return Err(refusal.clone());
        }
    }
    checking("account_observe_parse");
    super::refuse_unknown_members(params, OBSERVE_MEMBERS)?;
    let request = parse_account_observe_request_serving(params, super::describe::CAPABILITIES)?;
    let owner_decision;
    let admission_principal;
    let authority = match runner_authority {
        Ok(authority) => {
            admission_principal = authority.principal.clone();
            authority
        }
        Err(refusal) => {
            checking("observation_owner_authorization");
            let (Some(decision_id), Some(admission_id)) =
                (&request.decision_id, &request.admission_id)
            else {
                return Err(refusal);
            };
            owner_decision = deps
                .store
                .v1_decision(decision_id)
                .map_err(store_unavailable)?;
            let Some(decision) = owner_decision
                .as_ref()
                .filter(|decision| decision.principal == caller.principal)
            else {
                return Err(refusal);
            };
            checking("observation_consumer_authorization");
            super::consumers::effective(&super::operator_config(deps))
                .admit(&decision.consumer, caller)?;
            admission_principal = format!("reserved:{}", decision.runner_module);
            let admission = deps
                .store
                .v1_admission(&admission_principal, admission_id)
                .map_err(store_unavailable)?;
            let admission_body = admission
                .as_ref()
                .and_then(|standing| serde_json::from_str::<Value>(&standing.body_json).ok());
            checking("observation_admission_authorization");
            cortexkit_role_routing::observe::check_observe_authorization(
                cortexkit_role_routing::observe::ObserveArm::decision_owner(
                    decision_id,
                    admission_id,
                    admission_body
                        .as_ref()
                        .and_then(|body| body["decision_id"].as_str()),
                ),
                &request,
            )?;
            AccountCaller {
                module: &decision.runner_module,
                principal: caller.principal.clone(),
                session: None,
            }
        }
    };
    // The allocator reads these two as closed sets of its own.
    if let Some(origin) = &request.upstream_origin {
        checking("observation_origin");
        serde_json::from_value::<striatum_core::accounts::evidence::FrameOrigin>(json!(
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
        checking("observation_reset_source");
        serde_json::from_value::<striatum_core::accounts::evidence::ResetSource>(json!(
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
    legacy["caller"] = json!({ "runner": authority.module });
    if authority.session.is_some() {
        legacy["event_id"] = json!(authority.admission_key(&request.event_id));
        if let Some(id) = &request.admission_id {
            legacy["admission_id"] = json!(authority.admission_key(id));
        }
    }
    let flags = config_overrides::live_config(deps.routing_config_path).account_flags();
    let policy = config_overrides::live_config(deps.routing_config_path).allocator_policy("");
    checking("account_context");
    let ctx = account_ctx(deps, flags, &policy)?;
    let reply = account_selection::observe_authorized(
        &legacy,
        &authority.principal,
        &admission_principal,
        admission_principal == authority.principal,
        &ctx,
    );
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

/// The contract mapping for a legacy code, for the per-code tests.
#[cfg(test)]
pub(crate) fn account_refusal_for_test(code: &str) -> Refusal {
    account_refusal(code, format!("test {code}"), None)
}

/// The contract mapping with a legacy detail, for timing tests.
#[cfg(test)]
pub(crate) fn account_refusal_for_test_detail(
    code: &str,
    detail: Option<&serde_json::Value>,
) -> Refusal {
    account_refusal(code, format!("test {code}"), detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_service_tier_absent_preserves_legacy_answer_bytes() {
        let mut params = json!({
            "provider":"openai", "model":"model", "conversation_key":"conversation", "admission_id":"one"
        });
        let mut result = json!({
            "admission":{"state":"reserved"},
            "account":{"provider":"openai","account_id":"account-1","credential_id":"oauth:1","method":"oauth"},
            "pin":{"id":"pin-1","generation":1,
                "affinity_expires_at":"1970-01-01T00:00:01Z",
                "admission_valid_until":"1970-01-01T00:00:02Z",
                "lease_valid_until":"1970-01-01T00:00:03Z"},
            "reason":{"code":"headroom","decision_id":"decision-1","policy_version":1}
        });
        let request =
            parse_account_for_request_serving(&params, super::super::describe::CAPABILITIES)
                .unwrap();
        let answer = admission_answer(&result, &request).unwrap();
        let baseline = json!({
            "admission":{"id":"one","state":"reserved"},
            "account":{"provider":"openai","account_id":"account-1","credential_id":"oauth:1","method":"oauth"},
            "pin":{"id":"pin-1","generation":1,"affinity_expires_at_ms":1000,
                "admission_valid_until_ms":2000,"lease_valid_until_ms":3000,"mode":"admit"},
            "reason":{"code":"headroom","decision_id":"decision-1","policy_version":"1",
                "quota_age_band":"unknown"}
        });
        assert_eq!(
            serde_json::to_vec(&answer).unwrap(),
            serde_json::to_vec(&baseline).unwrap()
        );
        params["service_tier"] = json!("standard");
        result["service_tier_chosen"] = json!("standard");
        let request =
            parse_account_for_request_serving(&params, super::super::describe::CAPABILITIES)
                .unwrap();
        let mut explicit = admission_answer(&result, &request).unwrap();
        assert_eq!(
            explicit
                .as_object_mut()
                .unwrap()
                .remove("service_tier_chosen"),
            Some(json!("standard"))
        );
        assert_eq!(
            serde_json::to_vec(&explicit).unwrap(),
            serde_json::to_vec(&baseline).unwrap()
        );
        for state in ["reserved", "future_admission_state"] {
            result["admission"]["state"] = json!(state);
            let answer = admission_answer(&result, &request).unwrap();
            let decoded: AccountForAnswer = serde_json::from_value(answer).unwrap();
            assert_eq!(decoded.admission.state, state);
        }
    }

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
    fn a_hold_is_timed_and_admission_conflict_carries_no_detail() {
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

    #[test]
    fn account_unavailable_preserves_candidate_reasons_and_retry_delay_only() {
        let detail = json!({
            "code":"account_selection_unavailable", "retry_after_s":3, "retry_after_ms":3_000,
            "decision_id":"decision-1",
            "exclusions":[{"account_id":"account-1","reason":"runner_service_tiers_unobserved"}],
            "unrelated_internal_field":"not public"
        });
        let refusal = account_refusal(
            "account_selection_unavailable",
            "[TODO(user): retry]".into(),
            Some(&detail),
        );
        assert_eq!(refusal.detail.get("exclusions"), detail.get("exclusions"));
        assert_eq!(
            refusal.detail.get("decision_id"),
            Some(&json!("decision-1"))
        );
        assert_eq!(
            refusal.detail.get("code"),
            Some(&json!("account_selection_unavailable"))
        );
        assert_eq!(refusal.detail.get("retry_after_s"), Some(&json!(3)));
        assert_eq!(refusal.detail.get("retry_after_ms"), Some(&json!(3_000)));
        assert!(!refusal.detail.contains_key("unrelated_internal_field"));
    }
}
