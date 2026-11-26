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
use cortexkit_role_routing::errors::{
    AccountUnservableDetail, AccountUnservableReason, NoRouteReason, Refusal, RefusalCode,
};
use cortexkit_role_routing::observe::{
    parse_account_observe_request_serving, AccountObserveAnswer, Observation,
};
use cortexkit_role_routing::service_tier::{check_chosen, ServiceTier};
use serde_json::{json, Value};
use striatum_store::V1Recorded;

use super::{account_ctx, now_ms, provider_refusal, Caller, Deps};
use crate::account_refusal_log::{because, checking};
use crate::account_selection::{self, OpReply};
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
    "fork_of",
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
    "head_turn",
];

const OBSERVE_MEMBERS: &[&str] = &[
    "attempt_outcome",
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
            |scope| scoped_admission_key(&scope.reference, id),
        )
    }

    // Session namespaces feed the salted conversation hash. Retain prefrontal's
    // old spelling through the rename, and even after general aliases retire,
    // until old sessions expire (seven days idle plus one hour). Otherwise heads
    // lose their account pins and prompt caches when the module name changes.
    fn recorded_principal(&self) -> String {
        self.session.map_or_else(
            || self.principal.clone(),
            |scope| {
                format!(
                    "session:{}",
                    serde_json::to_string(&(
                        striatum_core::module_identity::canonical(&scope.owner),
                        &scope.reference
                    ))
                    .unwrap()
                )
            },
        )
    }
}

fn scoped_admission_key(reference: &str, id: &str) -> String {
    serde_json::to_string(&(reference, id)).unwrap()
}

/// The v1 row's principal carries the scope, whereas the allocator includes
/// it in the admission key. Reconstruct that key for the stored explanation.
pub(super) fn allocator_admission_key(
    row: &striatum_store::V1SelectionAdmission,
) -> Option<std::borrow::Cow<'_, str>> {
    match row.principal.strip_prefix("session:") {
        Some(scope) => {
            let (_, reference) = serde_json::from_str::<(String, String)>(scope).ok()?;
            Some(scoped_admission_key(&reference, &row.admission_id).into())
        }
        None => Some(row.admission_id.as_str().into()),
    }
}

/// Session ids are scoped under their owner, not the relay's principal. The
/// allocator hashes the stamped conversation ref within that namespace, so a
/// child can look up its parent's reported use without sharing admission ids.
fn account_caller<'a>(caller: &'a Caller, deps: &Deps<'_>) -> Result<AccountCaller<'a>, Refusal> {
    checking("caller_check");
    let runners = deps
        .broca_credentials
        .lock()
        .map(|credentials| {
            super::runners::enrollment::policies(
                &super::operator_config(deps).v1_runners(),
                &credentials,
            )
        })
        .unwrap_or_else(|_| super::operator_config(deps).v1_runners());
    if caller.module_identity_is_current() {
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
                    serde_json::to_string(&striatum_core::module_identity::canonical(&scope.owner))
                        .unwrap()
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
    let runner = account_caller(caller, deps)
        .ok()
        .filter(|authority| authority.session.is_none());
    // Runner authorization takes precedence, but does not erase the route's
    // session scope: calls in head scope commonly arrive through runner modules.
    let scope_head = caller
        .scope_authority
        .as_ref()
        .is_some_and(|scope| scope.kind == subc_protocol::scope::ScopeKind::Head);
    let head_call_source = if scope_head {
        Some("scope")
    } else if caller.binding_is_current() {
        deps.broca_credentials.lock().ok().and_then(|credentials| {
            account_selection::call_log::AccountCall::head_turn_source(
                params,
                &caller.principal,
                &super::operator_config(deps),
                &credentials,
            )
        })
    } else {
        None
    };
    let call = account_selection::call_log::AccountCall::routing(
        params,
        caller,
        runner.as_ref().map(|authority| authority.module),
        head_call_source,
        deps.store,
    );
    let reply =
        crate::account_refusal_log::record("route.account_for", &caller.principal, params, || {
            account_for_inner(params, caller, deps, &call)
        });
    call.finish();
    reply
}

fn account_for_inner(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    call: &account_selection::call_log::AccountCall,
) -> Result<Value, Refusal> {
    let authority = account_caller(caller, deps)?;
    if params.get("fork_of").is_some_and(|fork| !fork.is_null())
        && [
            "requested_account",
            "warm_credential",
            "parent_conversation_key",
            "candidates",
        ]
        .iter()
        .any(|field| params.get(*field).is_some_and(|value| !value.is_null()))
    {
        return Err(Refusal::invalid_request(
            "fork_of",
            "fork_of cannot be combined with account placement hints",
        ));
    }
    if params.get("fork_of").is_some()
        && params["mode"].as_str().is_some_and(|mode| mode != "admit")
    {
        return Err(Refusal::invalid_request(
            "fork_of",
            "fork_of is valid only in admit mode",
        ));
    }
    if params["mode"] == "continue" {
        if super::consumers::is_media_provider(authority.module) {
            return Err(Refusal::invalid_request(
                "mode",
                "Media engine jobs don't continue an earlier admission. Admit each job with its own engine decision (mode admit).",
            ));
        }
        return continue_account(params, caller, &authority, deps, call);
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
    let mut request = parse_account_for_request_serving(params, &super::describe::capabilities())?;
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
    let hold_wait_max_s = request
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
    #[cfg(test)]
    account_selection::admission::run_after_roster_hook(&format!(
        "v1-replay-checked:{}",
        request.admission_id
    ));
    // Every refusal from here on was decided without the admission record: an
    // identical request may have committed this admission id since the check
    // above. Look again before refusing, and answer as that admission did.
    let attempt = (|| -> Result<Value, Refusal> {
        let request = request.clone();
        let principal = principal.clone();
        let body_json = body_json.clone();
        let mut hold_wait_max_s = hold_wait_max_s;
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
                    && (!engine_decision
                        || super::engines::now(deps) < decision.expires_at_ms as u64)
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
                let message = "A request bound to an earlier routing decision keeps that decision's service tier. Send the same tier, or ask routing for a new decision to change it.";
                // The stored tier, not the caller's request, is invalid.
                if bound_tier.as_deref().and_then(ServiceTier::parse).is_none() {
                    return Err(Refusal::account_unservable(
                        AccountUnservableReason::StoredStateInvalid,
                        vec![],
                        vec![],
                    ));
                }
                return Err(Refusal::with(
                    RefusalCode::InvalidRequest,
                    message,
                    &json!({
                        "field": "service_tier",
                        "decision_id": decision_id,
                        "requested_service_tier": request.service_tier,
                        "bound_service_tier": bound_tier,
                    }),
                ));
            }
            if let Some(input) = deps
                .store
                .get_decision_by_id(decision_id)
                .map_err(store_unavailable)?
            {
                let input: Value =
                    serde_json::from_str(&input.requirements_json).map_err(store_unavailable)?;
                if input["login_not_required"] == true {
                    return Err(login_not_required());
                }
                if let Some(cap) = input["hold_wait_max_s"].as_u64() {
                    hold_wait_max_s = hold_wait_max_s.min(cap);
                }
            }
        } else if !media_caller
            && super::runners::unbound_route_is_keyless(
                deps,
                authority.module,
                &request.provider,
                &request.model,
            )
        {
            return Err(login_not_required());
        }

        let mut parsed = account_selection::admission::AccountForParams::from_v1(
            &request,
            authority.admission_key(&request.admission_id),
            authority.module.to_owned(),
            hold_wait_max_s,
            params,
        )
        .map_err(op_refusal)?;
        parsed.scope_fork_parent(|id| authority.admission_key(id));
        parsed.call = Some(call.clone());
        let flags = config_overrides::live_config(deps.routing_config_path).account_flags();
        let policy = config_overrides::live_config(deps.routing_config_path)
            .allocator_policy(&request.provider);
        // A store failure while building the shared context is reported by
        // `account_for` as transient unavailability, which callers retry,
        // rather than as a `store_error` refusal.
        checking("account_context");
        if flags.enabled
            && !crate::broca_credentials::wait_for_first_listing(deps.broca_credentials)
        {
            return Err(op_refusal(crate::broca_credentials::listing_not_ready(
                params,
            )));
        }
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
            if !credentials.listed_credentials().iter().any(|row| {
                crate::account_selection::class_context::media_credential_serves(
                    row,
                    &request.provider,
                )
            }) {
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
        let record = AdmissionRecord {
            #[cfg(test)]
            answer_mutation: FINISH_ANSWERS.lock().unwrap().remove(&request.admission_id),
            principal,
            request,
            body_json,
            bound_tier,
        };
        match account_selection::admission::account_for_parsed(
            parsed,
            &authority.principal,
            ctx,
            Some(record),
        ) {
            OpReply::Result(result) => Ok(result),
            error => Err(op_refusal(error)),
        }
    })();
    let refusal = match attempt {
        Ok(answer) => return Ok(answer),
        Err(refusal) => refusal,
    };
    match deps
        .store
        .v1_admission(&principal, &request.admission_id)
        .map_err(store_unavailable)?
    {
        Some(first) => replay_admission(first, &body_json, &request, &authority, deps),
        None => Err(refusal),
    }
}

fn op_refusal(reply: OpReply) -> Refusal {
    match reply {
        OpReply::Error {
            code,
            message,
            detail,
        } => account_refusal(&code, message, detail.as_ref()),
        OpReply::Result(_) => internal_unavailable("expected refusal"),
    }
}

fn login_not_required() -> Refusal {
    Refusal::with(
        RefusalCode::AccountUnservable,
        "The selected runner serves this model without a login; there is no account to admit.",
        &json!({"reason": "login_not_required"}),
    )
}

/// Public replay data captured before enqueue; persisted atomically with the
/// allocator's grant so a lost reply cannot leave only half an admission.
pub(crate) struct AdmissionRecord {
    principal: String,
    request: AccountForRequest,
    body_json: String,
    bound_tier: Option<String>,
    #[cfg(test)]
    answer_mutation: Option<fn(&mut Value)>,
}

#[cfg(test)]
static FINISH_ANSWERS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::BTreeMap<String, fn(&mut Value)>>,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
pub(crate) fn inject_finish_answer(admission_id: &str, mutation: fn(&mut Value)) {
    assert!(FINISH_ANSWERS
        .lock()
        .unwrap()
        .insert(admission_id.into(), mutation)
        .is_none());
}

#[derive(Debug)]
pub(crate) enum AdmissionError {
    Store(striatum_core::RouteStateStoreError),
    Refusal(Refusal),
}

impl From<striatum_core::RouteStateStoreError> for AdmissionError {
    fn from(error: striatum_core::RouteStateStoreError) -> Self {
        Self::Store(error)
    }
}

impl striatum_store::AccountCommandError for AdmissionError {
    fn store_error(&self) -> Option<&striatum_core::RouteStateStoreError> {
        match self {
            Self::Store(error) => Some(error),
            Self::Refusal(_) => None,
        }
    }
}

impl AdmissionRecord {
    pub(crate) fn finish(
        &self,
        tx: &striatum_store::AccountTx<'_>,
        reply: OpReply,
        now: i64,
    ) -> Result<OpReply, AdmissionError> {
        let result = match reply {
            OpReply::Result(result) => result,
            error => return Ok(error),
        };
        #[cfg(test)]
        let result = {
            let mut result = result;
            if let Some(mutation) = self.answer_mutation {
                mutation(&mut result);
            }
            result
        };
        let answer = self.answer(&result).map_err(AdmissionError::Refusal)?;
        let standing = tx.v1_record_admission(
            &self.principal,
            &self.request.admission_id,
            &V1Recorded {
                body_json: self.body_json.clone(),
                answer_json: answer.to_string(),
            },
            now,
        )?;
        match standing_answer(standing, &self.body_json) {
            Ok(answer) => Ok(OpReply::Result(answer)),
            Err(refusal) => Ok(OpReply::typed(
                refusal.code.as_str(),
                refusal.message,
                json!(refusal.detail),
            )),
        }
    }

    fn answer(&self, result: &Value) -> Result<Value, Refusal> {
        let request = &self.request;
        let bound_tier = &self.bound_tier;
        let answer = admission_answer(result, request)?;
        let chosen = answer["service_tier_chosen"].as_str();
        let effective_tier = bound_tier
            .as_deref()
            .filter(|tier| *tier != "standard")
            .and_then(ServiceTier::parse)
            .or(request.service_tier);
        // A decision fixes the service tier; the general contract check permits
        // a step-down, so decision-bound admissions must also require equality.
        if !check_chosen(effective_tier, chosen)
            || bound_tier
                .as_deref()
                .is_some_and(|tier| tier != "standard" && chosen != Some(tier))
        {
            checking("chosen_service_tier");
            let mut refusal = Refusal::account_unservable(
                AccountUnservableReason::BoundTierUnavailable,
                vec![],
                vec![],
            );
            refusal.message = "No account can serve this routing decision's service tier any more, and a bound decision never steps down to a lower tier. Ask routing for a new decision.".into();
            return Err(refusal);
        }
        Ok(answer)
    }
}

fn continue_account(
    params: &Value,
    caller: &Caller,
    authority: &AccountCaller<'_>,
    deps: &Deps<'_>,
    call: &account_selection::call_log::AccountCall,
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
            "head_turn",
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
    let mut request =
        cortexkit_role_routing::account_continue::parse_continue_request(params, &describe)?;
    // A head authorized by its session scope had its admission stored under
    // the scope's reference, with the reference as its conversation key (see
    // admit and observe). Continue looks it up the same way, and keys its
    // replay record by the scope too, so two heads of one owner can't read or
    // collide with each other's selection ids.
    if let Some(scope) = authority.session {
        request.admission_id = authority.admission_key(&request.admission_id);
        request.selection_id = authority.admission_key(&request.selection_id);
        request.conversation_key = scope.reference.clone();
    }
    let flags = config_overrides::live_config(deps.routing_config_path).account_flags();
    let policy = config_overrides::live_config(deps.routing_config_path).allocator_policy("");
    checking("account_context");
    let ctx = account_ctx(deps, flags, &policy).map_err(|r| account_unavailable(r.message))?;
    checking("continuation_allocation");
    account_selection::continue_account_with_call(
        &request,
        &authority.principal,
        authority.module,
        caller
            .scope
            .as_ref()
            .and_then(|scope| scope.project_id.as_deref()),
        &ctx,
        Some(call.clone()),
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
    let principal = authority.principal.clone();
    let id = authority.admission_key(&request.admission_id);
    let body_json = body_json.to_owned();
    let request = request.clone();
    crate::account_writer::execute(
        deps.store,
        striatum_store::AccountCommandKind::Replay,
        move |tx| {
            Ok(replay_admission_tx(
                tx,
                first.clone(),
                &body_json,
                &request,
                &principal,
                &id,
            ))
        },
    )
    .map_err(store_unavailable)?
}

fn replay_admission_tx(
    tx: &crate::account_state::AccountCommand<'_>,
    first: V1Recorded,
    body_json: &str,
    request: &AccountForRequest,
    principal: &str,
    id: &str,
) -> Result<Value, Refusal> {
    let original = standing_answer(first, body_json)?;
    if request.mode().as_str() != "admit" {
        return Ok(original);
    }
    let current = (|| {
        // A started admission is open, so the state answers without SQL.
        if !tx
            .open_admission(principal, id)?
            .is_some_and(|row| row.state == "started")
        {
            return Ok(None);
        }
        tx.latest_used_continuation(principal, id)
    })()
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
        let mut refusal = Refusal::account_unservable(
            AccountUnservableReason::AllocatorInvariant,
            vec![],
            vec![],
        );
        refusal.message = format!("the allocator's answer has no {what}");
        refusal
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
/// timed by the wait the allocator states, capped at the next quota recheck
/// when typed exclusions identify quota as the recovery cause. Account-selection unavailability
/// also preserves candidate exclusions and an optional short retry delay.
/// Legacy codes the contract does not declare (`store_error`,
/// `internal_error`, `unscoped_caller`) become the transient
/// `account_selection_unavailable`. Stable `no_eligible_account` becomes
/// `account_unservable`. `account_selection_disabled`
/// is preserved verbatim so it stays distinguishable from quota or
/// authentication refusals; the contract declares it (0.5.2) as untimed
/// with empty detail. A legacy `account_selection_unsupported` that reaches
/// this adapter means no credential of the right kind served the request,
/// so it becomes the untimed `auth_required`, never a transient retry.
/// `concurrency_full` becomes `no_route` with that reason and the allocator's
/// `reset_at_ms`: a wait for a free slot, never an unavailable allocator.
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
        "account_hold" => {
            let mut refusal = Refusal::account_hold(wait_until().unwrap_or_else(now_ms));
            if let Some(reason) = detail
                .and_then(|detail| detail.get("reason"))
                .and_then(Value::as_str)
            {
                refusal.detail.insert("reason".into(), json!(reason));
            }
            refusal
        }
        // The allocator's own instant, unconverted: a time derived from
        // `retry_after_s` would round it to seconds from a later clock read.
        "concurrency_full" => Refusal::no_route(
            NoRouteReason::ConcurrencyFull,
            detail
                .and_then(|detail| detail.get("reset_at_ms"))
                .and_then(Value::as_u64),
        ),
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
        "invalid_request" => {
            let mut refusal = Refusal::invalid_request(
                detail
                    .and_then(|detail| detail.get("field"))
                    .and_then(Value::as_str)
                    .unwrap_or("params"),
                message.clone(),
            );
            if let Some(exclusions) = detail.and_then(|detail| detail.get("exclusions")) {
                refusal
                    .detail
                    .insert("exclusions".into(), exclusions.clone());
            }
            refusal
        }
        // Declared (0.5.2) and untimed: module-wide, use your own credential.
        "account_selection_disabled" => {
            Refusal::new(RefusalCode::AccountSelectionDisabled, message.clone())
        }
        "account_selection_unsupported" => Refusal::new(RefusalCode::AuthRequired, message.clone()),
        "auth_required" if detail.is_some_and(|detail| detail["cause"] == "login_condition") => {
            let detail = detail.unwrap();
            Refusal::with(
                RefusalCode::AuthRequired,
                message.clone(),
                &json!({
                    "cause": "login_condition",
                    "reasons": detail["reasons"],
                    "credential_ids": detail["credential_ids"],
                }),
            )
        }
        "account_selection_unavailable" => {
            let mut refusal = account_unavailable(message.clone());
            if let Some(detail) = detail.and_then(Value::as_object) {
                for key in [
                    "code",
                    "retry_after_s",
                    "decision_id",
                    "exclusions",
                    "retry_after_ms",
                    "cause",
                ] {
                    if let Some(value) = detail.get(key) {
                        refusal.detail.insert(key.into(), value.clone());
                    }
                }
            }
            refusal
        }
        "no_eligible_account" => {
            Refusal::account_unservable(AccountUnservableReason::NoEligibleAccount, vec![], vec![])
        }
        "account_unservable" => {
            // The allocator already used the bounded constructor. Preserve its
            // full counts rather than counting the truncated lists again.
            match detail.and_then(|detail| {
                serde_json::from_value::<AccountUnservableDetail>(detail.clone()).ok()
            }) {
                Some(detail) => {
                    let mut refusal = Refusal::account_unservable(
                        detail.reason,
                        detail.offered_credential_ids.unwrap_or_default(),
                        detail.eligible_credential_ids.unwrap_or_default(),
                    );
                    for (key, count) in [
                        ("offered_credential_count", detail.offered_credential_count),
                        (
                            "eligible_credential_count",
                            detail.eligible_credential_count,
                        ),
                    ] {
                        if refusal.detail.contains_key(key) {
                            if let Some(count) = count {
                                refusal.detail.insert(key.into(), json!(count));
                            }
                        }
                    }
                    refusal
                }
                None => Refusal::account_unservable(
                    AccountUnservableReason::Unknown("unspecified".into()),
                    vec![],
                    vec![],
                ),
            }
        }
        "store_error" | "internal_error" | "unscoped_caller" => {
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
                "eligible_accounts_not_offered",
            ] {
                if let Some(value) = detail.get(key) {
                    refusal.detail.insert(key.into(), value.clone());
                }
            }
        }
        if let Some(reset) = refusal.detail.get("reset_at_ms").and_then(Value::as_u64) {
            if quota_recovery_exclusions(refusal.detail.get("exclusions"), reset) {
                let now = now_ms();
                let retry = super::quota_retry_hint(Some(reset), now).unwrap();
                if retry < reset {
                    refusal.detail.insert("reset_at_ms".into(), json!(retry));
                    if refusal.detail.contains_key("retry_after_s") {
                        refusal.detail.insert(
                            "retry_after_s".into(),
                            json!(retry.saturating_sub(now).div_ceil(1000)),
                        );
                    }
                }
            }
        }
    }
    refusal.message = message;
    refusal
}

/// Credential and route fences share the pool refusal code with quota walls.
/// Only shorten a mixed pool's hint when its dated exclusions identify the
/// recovery being named; undated allocator exclusions must all be quota causes.
fn quota_recovery_exclusions(exclusions: Option<&Value>, reset: u64) -> bool {
    let Some(exclusions) = exclusions
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty())
    else {
        return false;
    };
    let quota = |row: &Value| {
        matches!(
            row["reason"].as_str(),
            Some("below_reserve" | "below_broca_floor" | "scope_exhausted" | "confirmed_fence")
        )
    };
    if exclusions
        .iter()
        .any(|row| row["reset_at_ms"].as_u64().is_some())
    {
        let recovering: Vec<_> = exclusions
            .iter()
            .filter(|row| row["reset_at_ms"].as_u64() == Some(reset))
            .collect();
        !recovering.is_empty() && recovering.into_iter().all(quota)
    } else {
        exclusions.iter().all(quota)
    }
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
    let request = parse_account_observe_request_serving(params, &super::describe::capabilities())?;
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
            let Some(decision) = owner_decision.as_ref().filter(|decision| {
                striatum_core::module_identity::equal(&decision.principal, &caller.principal)
            }) else {
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
        serde_json::from_value::<striatum_core::accounts::evidence::FrameOrigin>(json!(origin))
            .map_err(|_| {
                Refusal::invalid_request(
                    "upstream_origin",
                    format!("{origin:?} is not a frame origin"),
                )
            })?;
    }
    if let Some(source) = &request.resets_at_source {
        checking("observation_reset_source");
        serde_json::from_value::<striatum_core::accounts::evidence::ResetSource>(json!(source))
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
    let reply = account_selection::observations::observe_authorized(
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
    mod quota_retry_tests;
    use super::*;
    mod unservable_tests;

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
