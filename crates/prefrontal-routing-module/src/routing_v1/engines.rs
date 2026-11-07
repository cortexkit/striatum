//! Media selection shares the decision ledger, never the language-model scorer.

use cortexkit_role_routing::{
    errors::Refusal,
    select_engine::{self as wire, EngineCandidate, SelectEngineAnswer},
};
use prefrontal_routing_core::{accounts::AllowedAccountClass, route_state_store::DecisionInput};
use prefrontal_routing_store::V1Decision;
use serde_json::Value;

use super::{store_refusal, Caller, Deps};

pub(super) fn now(deps: &Deps<'_>) -> u64 {
    #[cfg(test)]
    let offset = deps
        .broca_credentials
        .lock()
        .unwrap()
        .engine_clock_offset_ms;
    #[cfg(not(test))]
    let offset = {
        let _ = deps;
        0
    };
    super::now_ms().saturating_add(offset)
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct FactNumber(f64);
impl Eq for FactNumber {}
impl PartialOrd for FactNumber {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for FactNumber {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

pub(super) fn is_decision(deps: &Deps<'_>, id: &str) -> Result<bool, String> {
    deps.store
        .v1_selection_json(id)
        .map_err(|error| error.to_string())?
        .map(|snapshot| {
            serde_json::from_str::<Value>(&snapshot)
                .map(|value| value["engine"].is_object())
                .map_err(|error| error.to_string())
        })
        .transpose()
        .map(|value| value.unwrap_or(false))
}

pub(super) fn select(params: &Value, caller: &Caller, deps: &Deps<'_>) -> Result<Value, Refusal> {
    select_inner(params, caller, deps).map_err(|mut refusal| {
        refusal.message = "[TODO(user): Explain media engine selection refusal]".into();
        refusal
    })
}

fn select_inner(params: &Value, caller: &Caller, deps: &Deps<'_>) -> Result<Value, Refusal> {
    let request = wire::parse_select_engine_request(params)?;
    super::consumers::effective(&super::operator_config(deps)).admit(&request.consumer, caller)?;
    let now = now(deps);
    let config = super::operator_config(deps);
    let inputs = crate::prepare_selection_inputs(
        deps.quota_cache,
        deps.store,
        config.pricing(),
        &config.quota_model_pools(),
    )
    .map_err(store_refusal)?;
    let mut candidates: Vec<EngineCandidate<FactNumber, FactNumber>> = Vec::new();
    let catalog = deps
        .broca_credentials
        .lock()
        .map_err(store_refusal)?
        .catalog_snapshot
        .clone();
    #[cfg(test)]
    let media_facts = deps
        .broca_credentials
        .lock()
        .map_err(store_refusal)?
        .engine_test_facts
        .clone();
    for (module, slot) in super::consumers::engine_declarations() {
        candidates.extend(
            slot.engines
                .into_iter()
                .map(|engine| EngineCandidate::new(module.clone(), engine)),
        );
    }
    if let Some(local) = &request.caller_local {
        candidates.extend(
            local
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, engine)| EngineCandidate::caller_local(index, engine)),
        );
    }
    for candidate in &mut candidates {
        // routing/v1 has no engine accuracy or speed measurement report yet.
        // Missing measurements remain unknown; LLM scores cannot stand in for
        // accuracy on a media kind.
        candidate.cost = catalog
            .as_ref()
            .and_then(|catalog| {
                catalog.engine_cost(
                    &request,
                    &candidate.engine.provider,
                    &candidate.engine.model,
                )
            })
            .map(FactNumber);
        let key = prefrontal_routing_core::make_route_key(
            &candidate.engine.provider,
            &candidate.engine.model,
            None,
        );
        if let Some(state) = inputs.snapshot.by_route.get(&key) {
            candidate.fenced = [
                state.cooldown_until,
                state.quota_exhausted_until,
                state.provider_reset_until,
                state.failure_backoff_until,
            ]
            .into_iter()
            .flatten()
            .any(|until| until > now as f64)
                || state.credential_refusal.is_some();
        }
        #[cfg(test)]
        let facts = media_facts.get(&(key, candidate.engine.kind.as_str().to_owned()));
        #[cfg(test)]
        if let Some((accuracy, cost)) = facts {
            candidate.accuracy = accuracy.filter(|value| value.is_finite()).map(FactNumber);
            candidate.cost = cost.filter(|value| value.is_finite()).map(FactNumber);
        }
    }
    let winner = wire::select_engine(&request, &candidates)?;
    let scope = caller.scope.as_ref();
    let decision = DecisionInput {
        decision_kind: "engine".into(), consumer: Some(request.consumer.clone()), target_agent: request.consumer.clone(),
        project_id: scope.and_then(|scope| scope.project_id.clone()),
        workdir: scope.and_then(|scope| scope.workdir.clone()),
        requirements_json: params.to_string(), selected_route_key: Some(prefrontal_routing_core::make_route_key(&winner.engine.provider, &winner.engine.model, None)),
        selected_provider_id: Some(winner.engine.provider.clone()), selected_model_id: Some(winner.engine.model.clone()),
        score_json: serde_json::json!({"accuracy": winner.accuracy.map(|value| value.0), "cost": winner.cost.map(|value| value.0)}).to_string(),
        rejected_json: "[]".into(), allowed_account_class: AllowedAccountClass::PricedPerTokenOnly,
        quota_signal_json: Some(inputs.quota_signal_json), ..Default::default()
    };
    let id = deps
        .store
        .record_decision_with_task_ref(&decision, winner.selected.module.as_deref(), None, None)
        .map_err(store_refusal)?;
    let answer = SelectEngineAnswer::new(&id, now + wire::DECISION_TTL_MS, winner.selected.clone());
    let value = serde_json::to_value(&answer).map_err(store_refusal)?;
    deps.store
        .v1_record_selection(
            &V1Decision {
                decision_id: id,
                principal: caller.principal.clone(),
                consumer: request.consumer,
                allowed_account_class: decision.allowed_account_class,
                project_id: decision.project_id,
                provider: winner.engine.provider.clone(),
                model: winner.engine.model.clone(),
                variant: None,
                runner_module: winner.selected.module.clone().unwrap_or_default(),
                affinity: None,
                created_at_ms: now as i64,
                expires_at_ms: answer.expires_at_ms as i64,
            },
            Some(&value.to_string()),
        )
        .map_err(store_refusal)?;
    Ok(value)
}
