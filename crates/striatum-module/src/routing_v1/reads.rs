//! Public model inventory and reads of immutable selection-time facts.

use super::{provider_refusal, refuse_unknown_members, store_refusal, Caller, Deps};
use cortexkit_role_routing::{budget, errors::Refusal, explain, select::SelectAnswer};
use serde_json::{json, Value};

pub(super) fn consumer_models(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    use cortexkit_role_routing::consumer_models::{self, ConsumerModel, ConsumerModelsAnswer};
    use striatum_core::RouteStateStore;
    refuse_unknown_members(params, &["consumer"])?;
    let request = consumer_models::parse_consumer_models_request(params)?;
    let config = super::operator_config(deps);
    super::consumers::effective(&config).admit(&request.consumer, caller)?;
    let mut facts = deps.store.get_facts().map_err(store_refusal)?;
    let aliases = {
        let credentials = deps.broca_credentials.lock().map_err(store_refusal)?;
        credentials.catalog_targets(&facts)
    };
    striatum_core::score_alias::append_alias_rows(&mut facts, &aliases);
    deps.broca_credentials
        .lock()
        .map_err(store_refusal)?
        .price_facts(&mut facts, config.pricing());
    config.apply(&mut facts);
    let resolved = crate::resolve_routing_policy_with_catalog_targets(
        facts
            .iter()
            .map(|fact| striatum_core::fact_to_route(fact).route_key),
        &config.operator_policy(),
        &Default::default(),
        &crate::provider_auth_types(None),
        &aliases,
    );
    let subscriptions: std::collections::HashSet<_> =
        resolved.subscription_keys.into_iter().collect();
    // Inventory is model-level, like the available-models list: sibling variants
    // contribute their best score without producing duplicate model rows.
    let mut models = std::collections::BTreeMap::new();
    for fact in &facts {
        let key = (fact.provider_id.clone(), fact.model_id.clone());
        let previous: Option<&&striatum_core::ModelFact> = models.get(&key);
        if previous.is_none_or(|old| fact.scores.elo.total_cmp(&old.scores.elo).is_gt()) {
            models.insert(key, fact);
        }
    }
    let mut answer = ConsumerModelsAnswer::new(
        deps.store.catalog_fingerprint().map_err(store_refusal)?,
        &request.consumer,
    );
    for ((provider, model), fact) in models {
        let mut entry = ConsumerModel::new(
            &provider,
            &model,
            !config.route_allowed_on_plane(
                &request.consumer,
                &format!("{provider}/{model}"),
                false,
                fact.billing_plane.as_ref(),
            ),
        );
        entry.elo = fact.scores.elo.is_finite().then_some(fact.scores.elo);
        entry.eq = fact.scores.eq.is_finite().then_some(fact.scores.eq);
        // This read describes standing subscription coverage, not whether an
        // account has quota right now.
        entry.billing = Some(if subscriptions.contains(&format!("{provider}/{model}")) {
            consumer_models::Billing::Subscription
        } else {
            consumer_models::Billing::PerToken
        });
        answer.models.push(entry);
    }
    serde_json::to_value(answer).map_err(store_refusal)
}

fn retained(id: &str, caller: &Caller, deps: &Deps<'_>) -> Result<SelectAnswer, Refusal> {
    let unknown = Refusal::unknown_decision;
    deps.store
        .v1_decision(id)
        .map_err(store_refusal)?
        .filter(|decision| {
            striatum_core::module_identity::equal(&decision.principal, &caller.principal)
                && caller.binding_is_current()
        })
        .ok_or_else(unknown)?;
    let snapshot = deps
        .store
        .v1_selection_json(id)
        .map_err(store_refusal)?
        .ok_or_else(unknown)?;
    serde_json::from_str(&snapshot).map_err(store_refusal)
}

pub(super) fn budget_quote(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    refuse_unknown_members(params, &["decisions", "prompt_tokens"])?;
    let request = budget::parse_budget_quote_request(params)?;
    let seats = request
        .decisions
        .iter()
        .map(|id| {
            retained(id, caller, deps).map(|answer| budget::BudgetSeat::from_select_answer(&answer))
        })
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_value(budget::quote(&seats, request.prompt_tokens)?).map_err(store_refusal)
}

pub(super) fn explain(params: &Value, caller: &Caller, deps: &Deps<'_>) -> Result<Value, Refusal> {
    refuse_unknown_members(params, &["decision_id"])?;
    let request = explain::parse_explain_request(params)?;
    if super::engines::is_decision(deps, &request.decision_id).map_err(store_refusal)? {
        return Err(Refusal::unknown_decision());
    }
    let selection = retained(&request.decision_id, caller, deps)?;
    let mut serving = explain::Serving::new(&selection.route.provider);
    serving.credential_id = selection
        .runner
        .auth
        .as_ref()
        .and_then(|auth| auth.credential_id.clone());
    if let Some(admission) = deps
        .store
        .v1_selection_admission(&selection.decision_id)
        .map_err(store_refusal)?
    {
        let admission: cortexkit_role_routing::account::AccountForAnswer =
            serde_json::from_str(&admission).map_err(store_refusal)?;
        serving.account_id = Some(admission.account.account_id);
        serving.credential_id = Some(admission.account.credential_id);
    }
    let answer = explain::ExplainAnswer::new(
        &request.decision_id,
        selection.route,
        selection.runner,
        serving,
        "routing/v1",
        explain::ReasonCode::parse("selected"),
    );
    let mut answer = serde_json::to_value(answer).map_err(store_refusal)?;
    if let Some(decision) = deps
        .store
        .get_decision_by_id(&request.decision_id)
        .map_err(store_refusal)?
    {
        let inputs: Value =
            serde_json::from_str(&decision.requirements_json).map_err(store_refusal)?;
        for key in [
            "effort_source",
            "workload_source",
            "grade",
            "minimum_unverified",
            "workload_exclusions",
        ] {
            if let Some(value) = inputs.get(key) {
                answer[key] = value.clone();
            }
        }
    }
    Ok(answer)
}

pub(super) fn head_choices(params: &Value, deps: &Deps<'_>) -> Result<Value, Refusal> {
    head_choices_with_config(params, deps, &super::operator_config(deps), false)
}

pub(super) fn head_choices_with_config(
    params: &Value,
    deps: &Deps<'_>,
    config: &crate::config_overrides::LiveConfig,
    effort_detail: bool,
) -> Result<Value, Refusal> {
    refuse_unknown_members(params, &["runner_module", "harness"])?;
    let request = cortexkit_role_routing::head_choices::parse_head_choices_request(params)?;
    let credentials = deps.broca_credentials.lock().map_err(store_refusal)?;
    crate::head_choices::choices_for_harness_with_effort_detail(
        deps.store,
        config,
        &credentials,
        &request.runner_module,
        request.harness.as_deref(),
        effort_detail,
    )
    .map_err(|error| {
        let mut refusal = provider_refusal(&error.code, error.message);
        if let Some(Value::Object(detail)) = error.detail {
            refusal.detail = detail;
        }
        refusal
    })
}

/// The old status request is identical to the public request. Keep its warming
/// diagnostics and add the public limit state, so neither caller is reclassified.
pub(super) fn account_status(
    params: &Value,
    _caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    refuse_unknown_members(params, &["provider", "account_id"])?;
    let request = cortexkit_role_routing::account_status::parse_account_status_request(params)?;
    let config = crate::config_overrides::live_config(deps.routing_config_path);
    let policy = config.allocator_policy(&request.provider);
    let ctx = crate::account_selection::Ctx {
        store: deps.store,
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: config.account_flags(),
        policy: &policy,
        now_ms: super::now_ms() as i64,
        classes: None,
    };
    let crate::account_selection::OpReply::Result(mut answer) =
        crate::account_selection::status::account_status(params, &ctx)
    else {
        return Err(provider_refusal(
            "internal_error",
            "account status could not be read",
        ));
    };
    let limited = answer["reason"]["code"] == "within_reserve";
    answer["limit_state"] = json!(if answer["warm_ok"] == true {
        "available"
    } else if limited {
        "limited"
    } else {
        "unknown"
    });
    if limited {
        if let Some(reset) = answer["windows"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|window| window["kind"] == answer["reason"]["window"])
            .filter_map(|window| window["resets_at"].as_str())
            .filter_map(crate::account_quota::parse_rfc3339_ms)
            .filter(|reset| *reset > ctx.now_ms)
            .max()
        {
            answer["reset_at_ms"] = json!(reset);
        }
    }
    Ok(answer)
}
