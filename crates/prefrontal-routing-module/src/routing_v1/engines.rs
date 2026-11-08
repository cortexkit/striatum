//! Media selection shares the decision ledger, never the language-model scorer.

use cortexkit_role_routing::{
    engine_measurements::{self as measurements, EngineIdentity, EvalScore, EvaluationSet},
    errors::{Refusal, RefusalCode},
    select_engine::{self as wire, EngineCandidate, SelectEngineAnswer},
};
use prefrontal_routing_core::{accounts::AllowedAccountClass, route_state_store::DecisionInput};
use prefrontal_routing_store::{EngineEvaluation, EngineSpeedSample, V1Decision};
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

/// Refusals keep the message the failing check wrote: a `no_engine` answer
/// names the filter that emptied the candidate set, which a single generic
/// message would hide.
pub(super) fn select(params: &Value, caller: &Caller, deps: &Deps<'_>) -> Result<Value, Refusal> {
    select_inner(params, caller, deps)
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
    let mut candidates: Vec<EngineCandidate<EvalScore, FactNumber>> = Vec::new();
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
        if let Some((_, cost)) = facts {
            candidate.cost = cost.filter(|value| value.is_finite()).map(FactNumber);
        }
    }
    let identities: std::collections::BTreeSet<_> = candidates
        .iter()
        .filter(|candidate| {
            !candidate.selected.caller_local && candidate.engine.kind == request.kind
        })
        .map(|candidate| {
            (
                candidate.engine.provider.as_str(),
                candidate.engine.model.as_str(),
            )
        })
        .collect();
    let metric = request
        .requirements
        .metric
        .as_deref()
        .or(match request.kind {
            wire::Kind::Transcribe => Some("wer"),
            _ => None,
        });
    let (evaluations, samples) = deps
        .store
        .engine_measurements(
            request.kind.as_str(),
            metric,
            &identities.into_iter().collect::<Vec<_>>(),
        )
        .map_err(store_refusal)?;
    if request.requirements.latency_class == Some(wire::LatencyClass::Realtime) {
        for candidate in &mut candidates {
            if candidate.selected.caller_local {
                continue;
            }
            let mut factors: Vec<_> = samples
                .iter()
                .filter(|sample| {
                    sample.kind == candidate.engine.kind.as_str()
                        && sample.provider == candidate.engine.provider
                        && sample.model == candidate.engine.model
                })
                .filter_map(EngineSpeedSample::successful_realtime_factor)
                .collect();
            factors.sort_by(f64::total_cmp);
            if !factors.is_empty() {
                let middle = factors.len() / 2;
                let median = if factors.len() % 2 == 0 {
                    (factors[middle - 1] + factors[middle]) / 2.0
                } else {
                    factors[middle]
                };
                // A measured slower-than-realtime engine fails the requirements
                // stage. With no successful duration samples, its declaration wins.
                if median > 1.0 {
                    candidate.engine.latency_class = wire::LatencyClass::Batch;
                }
            }
        }
    }
    let evaluations: Vec<_> = evaluations
        .into_iter()
        .map(|eval| {
            measurements::EngineEvalRequest::new(
                wire::Kind::new(eval.kind),
                EngineIdentity::new(eval.provider, eval.model),
                eval.metric,
                eval.score,
                EvaluationSet::new(eval.set_id, 0),
                eval.measured_at_ms,
            )
        })
        .collect();
    measurements::apply_evaluations(&request, &mut candidates, &evaluations);
    let winner = wire::select_engine(&request, &candidates)?;
    let scope = caller.scope.as_ref();
    let decision = DecisionInput {
        decision_kind: "engine".into(), consumer: Some(request.consumer.clone()), target_agent: request.consumer.clone(),
        project_id: scope.and_then(|scope| scope.project_id.clone()),
        workdir: scope.and_then(|scope| scope.workdir.clone()),
        requirements_json: params.to_string(), selected_route_key: Some(prefrontal_routing_core::make_route_key(&winner.engine.provider, &winner.engine.model, None)),
        selected_provider_id: Some(winner.engine.provider.clone()), selected_model_id: Some(winner.engine.model.clone()),
        score_json: serde_json::json!({"accuracy": winner.accuracy.map(EvalScore::get), "cost": winner.cost.map(|value| value.0)}).to_string(),
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

fn measurement_refusal(mut refusal: Refusal) -> Refusal {
    refusal.message = "[TODO(user): Explain why the engine measurement was refused.]".into();
    refusal
}

fn measurement_store_refusal(error: impl std::fmt::Display) -> Refusal {
    measurement_refusal(store_refusal(error))
}

fn authorize(caller: &Caller, kind: &wire::Kind, engine: &EngineIdentity) -> Result<(), Refusal> {
    let declarations = super::consumers::engine_declarations();
    let module = caller
        .attested_module()
        .filter(|_| caller.binding_is_current());
    let Some(module) = module else {
        return Err(measurement_refusal(Refusal::new(
            RefusalCode::EngineNotDeclaredByCaller,
            "",
        )));
    };
    measurements::check_engine_authorization(
        module,
        kind,
        engine,
        declarations.iter().flat_map(|(module, slot)| {
            slot.engines
                .iter()
                .map(move |engine| (module.as_str(), engine))
        }),
    )
    .map_err(measurement_refusal)
}

pub(super) fn report(params: &Value, caller: &Caller, deps: &Deps<'_>) -> Result<Value, Refusal> {
    let report = measurements::parse_engine_report_request(params).map_err(measurement_refusal)?;
    authorize(caller, &report.kind, &report.engine)?;
    let binding = deps
        .store
        .v1_decision(&report.decision_id)
        .map_err(measurement_store_refusal)?;
    let ledger = deps
        .store
        .get_decision_by_id(&report.decision_id)
        .map_err(measurement_store_refusal)?;
    if !binding.is_some_and(|decision| {
        caller.attested_module() == Some(decision.runner_module.as_str())
            && decision.provider == report.engine.provider
            && decision.model == report.engine.model
    }) || !ledger.is_some_and(|decision| {
        decision.decision_kind == "engine"
            && serde_json::from_str::<Value>(&decision.requirements_json)
                .ok()
                .is_some_and(|request| request["kind"] == report.kind.as_str())
    }) {
        return Err(measurement_refusal(Refusal::unknown_decision()));
    }
    deps.store
        .record_engine_speed(
            &report.decision_id,
            &EngineSpeedSample {
                kind: report.kind.as_str().into(),
                provider: report.engine.provider,
                model: report.engine.model,
                outcome: report.outcome,
                duration_ms: report.input.duration_ms,
                wall_ms: report.wall_ms,
                first_output_ms: report.first_output_ms,
            },
        )
        .map_err(measurement_store_refusal)?;
    Ok(serde_json::json!({}))
}

pub(super) fn evaluate(params: &Value, caller: &Caller, deps: &Deps<'_>) -> Result<Value, Refusal> {
    let eval = measurements::parse_engine_eval_request(params).map_err(measurement_refusal)?;
    authorize(caller, &eval.kind, &eval.engine)?;
    deps.store
        .record_engine_evaluation(&EngineEvaluation {
            kind: eval.kind.as_str().into(),
            provider: eval.engine.provider,
            model: eval.engine.model,
            metric: eval.metric,
            set_id: eval.set.id,
            score: eval.score,
            measured_at_ms: eval.measured_at_ms,
        })
        .map_err(measurement_store_refusal)?;
    Ok(serde_json::json!({}))
}
