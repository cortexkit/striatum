//! Media selection shares the decision ledger, never the language-model scorer.

use cortexkit_role_routing::{
    engine_measurements::{self as measurements, EngineIdentity, EvalScore, EvaluationSet},
    errors::{NoRouteReason, PinUnavailableReason, Refusal, RefusalCode},
    select_engine::{self as wire, EngineCandidate, SelectEngineAnswer},
    select_media::{self as media, SelectMediaAnswer, SelectMediaRequest},
    select_search::{self as search, SelectSearchRequest},
};
use serde_json::{json, Value};
use striatum_core::{accounts::AllowedAccountClass, route_state_store::DecisionInput};
use striatum_store::{EngineEvaluation, EngineSpeedSample, V1Decision};

use super::{store_refusal, Caller, Deps};

#[path = "media_ranking.rs"]
mod ranking;

#[cfg(test)]
#[path = "media_grade_observer_tests.rs"]
pub(crate) mod grade_observer;

#[path = "media_pricing.rs"]
mod pricing;

#[path = "media_ordering.rs"]
mod ordering;

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

pub(super) const MEDIA_KINDS: &[&str] = &[
    "transcribe",
    "stream_transcribe",
    "synthesize",
    "realtime",
    "image",
    "video",
];

fn caller_local_cost() -> Option<FactNumber> {
    Some(FactNumber(0.0))
}

pub(super) fn select_media(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    let request =
        media::parse_select_media_request(params, &super::describe::capabilities(), MEDIA_KINDS)?;
    select_request(
        params,
        caller,
        deps,
        request.engine_request(),
        Some(&request),
        None,
        None,
    )
}

pub(super) fn select_search(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    let request =
        search::parse_select_search_request(params, &super::describe::capabilities(), &["search"])?;
    // Share engine policy, ranking and decision recording only after the search
    // parser has enforced its narrower wire contract.
    let mut common = SelectMediaRequest::new(
        &request.consumer,
        wire::Kind::new("search"),
        media::MediaRequirements::default(),
        request.privacy.clone(),
    );
    common.requirements.iq = request.requirements.iq;
    common.pin = request.pin.clone();
    common.exclude = request.exclude.clone();
    common.exclude_providers = request.exclude_providers.clone();
    common.eligible_routes = request.eligible_routes.clone();
    select_request(
        params,
        caller,
        deps,
        common.engine_request(),
        Some(&common),
        Some(&request),
        None,
    )
}

/// Media evidence is captured before the first preview row. Account loading
/// may heal evidence only in the private sweep, never in the live `Deps` store.
pub(super) struct MediaEvaluation {
    pub accounts: std::sync::Arc<crate::account_selection::Sweep>,
    pub config: std::sync::Arc<crate::config_overrides::LiveConfig>,
    pub consumers: std::sync::Arc<super::consumers::Registry>,
    pub joined: std::sync::Arc<super::consumers::MediaRoutes>,
    pub inputs: std::sync::Arc<crate::PreparedSelectionInputs>,
    pub account_quota: Option<crate::account_quota::SharedAccountQuota>,
    pub iq: Option<f64>,
    pub evaluations: Vec<EngineEvaluation>,
    pub samples: Vec<EngineSpeedSample>,
    pub evidence: Value,
}

pub(super) fn preview_media(
    request: &SelectMediaRequest,
    caller: &Caller,
    deps: &Deps<'_>,
    view: &mut MediaEvaluation,
) -> Result<Value, Refusal> {
    select_request(
        &Value::Null,
        caller,
        deps,
        request.engine_request(),
        Some(request),
        None,
        Some(view),
    )
}

fn failed_video_requirement(
    request: &SelectMediaRequest,
    facts: Option<&crate::fusiform_catalog::VideoFacts>,
) -> Option<&'static str> {
    // Stated output requirements need catalog evidence. Input duration and a
    // same-named caller_local request entry cannot supply a module's output facts.
    [
        (
            request
                .requirements
                .output_duration_ms
                .is_some_and(|duration| {
                    facts
                        .and_then(|facts| facts.max_output_duration_ms)
                        .is_none_or(|limit| duration > limit)
                }),
            "limit.output_duration_ms",
        ),
        (
            request
                .requirements
                .aspect_ratio
                .as_ref()
                .is_some_and(|ratio| {
                    facts
                        .and_then(|facts| facts.aspect_ratios.as_ref())
                        .is_none_or(|ratios| !ratios.contains(ratio))
                }),
            "capability.aspect_ratios",
        ),
        (
            request
                .requirements
                .audio
                .is_some_and(|audio| facts.and_then(|facts| facts.audio) != Some(audio)),
            "capability.output_audio",
        ),
    ]
    .into_iter()
    .find_map(|(present, fact)| present.then_some(fact))
}

fn select_inner(params: &Value, caller: &Caller, deps: &Deps<'_>) -> Result<Value, Refusal> {
    let request = wire::parse_select_engine_request(params)?;
    select_request(params, caller, deps, request, None, None, None)
}

fn select_request(
    params: &Value,
    caller: &Caller,
    deps: &Deps<'_>,
    request: wire::SelectEngineRequest,
    media: Option<&SelectMediaRequest>,
    search: Option<&SelectSearchRequest>,
    mut evaluation: Option<&mut MediaEvaluation>,
) -> Result<Value, Refusal> {
    let config = evaluation
        .as_ref()
        .map_or_else(|| super::operator_config(deps), |view| view.config.clone());
    let consumers = evaluation.as_ref().map_or_else(
        || std::sync::Arc::new(super::consumers::effective(&config)),
        |view| view.consumers.clone(),
    );
    // Previews require registration in their own handler, but deliberately do
    // not require the operator to own the consumer being inspected.
    if evaluation.is_none() {
        consumers.admit(&request.consumer, caller)?;
    }
    let policy = media.map(|media| {
        // Resolve only media defaults. The consumer registry's general
        // policy view also carries language-model requirements (context, tools)
        // that don't apply to engines, so build a policy holding just the
        // declaration's level and ladder and let operator settings override them.
        let defaults = consumers.defaults_for(&request.consumer);
        let declared = std::collections::BTreeMap::from([(
            request.consumer.clone(),
            crate::config_policy::ConsumerPolicy {
                iq: defaults.iq.map(f64::from),
                mode: defaults
                    .ladder
                    .as_ref()
                    .map(|_| crate::config_policy::ConsumerMode::Ladder),
                ladder: defaults.ladder,
                ..Default::default()
            },
        )]);
        config.consumer_policy(
            &request.consumer,
            &crate::config_policy::ConsumerPolicy {
                iq: evaluation
                    .as_ref()
                    .and_then(|view| view.iq)
                    .or(media.requirements.iq.map(f64::from)),
                ..Default::default()
            },
            &declared,
        )
    });
    if let Some(view) = evaluation.as_mut() {
        view.evidence["iq"] = json!(policy.as_ref().and_then(|policy| policy.iq));
    }
    // The settings resolver folds pins into ladders. Read the pin first so an
    // unavailable explicit choice cannot fall through to another engine.
    let pin = media.and_then(|media| media.pin.clone()).or_else(|| {
        policy.as_ref()?.pin.as_ref().map(|pin| {
            pin.variant.as_ref().map_or_else(
                || pin.model.clone(),
                |variant| format!("{}:{variant}", pin.model),
            )
        })
    });
    let mut pin_refusal = None;
    if let Some(pin) = &pin {
        if !config.media_route_allowed(&request.consumer, pin, true) {
            // The policy diagnostic names the effective rule, including model
            // overrides of provider rules. Only consumer restrictions are an
            // exclusion; a provider's model allowlist is an availability gate.
            let excluded = config
                .route_policy_refusal(&request.consumer, pin)
                .is_some_and(|rule| {
                    rule.split_whitespace().next().is_some_and(|source| {
                        source.ends_with(".only_for") || source.ends_with(".not_for")
                    })
                });
            let refusal = Refusal::pin_unavailable(
                pin,
                if excluded {
                    PinUnavailableReason::Excluded
                } else {
                    PinUnavailableReason::NotAvailable
                },
                None,
            );
            if evaluation.is_none() {
                return Err(refusal);
            }
            pin_refusal = Some(refusal);
        }
    }
    if super::describe::capabilities().contains(&"consumer_declarations") {
        let declared = consumers.defaults_for(&request.consumer);
        super::consumers::kind::resolve(
            declared.kind.as_ref().map(|kind| kind.as_str()),
            Some(request.kind.as_str()),
        )?;
    }
    let now = now(deps);
    let inputs = if let Some(view) = evaluation.as_ref() {
        view.inputs.clone()
    } else {
        std::sync::Arc::new(
            crate::prepare_selection_inputs(
                deps.quota_cache,
                deps.store,
                config.pricing(),
                &config.quota_model_pools(),
            )
            .map_err(store_refusal)?,
        )
    };
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
    let joined = media.map(|_| {
        evaluation.as_ref().map_or_else(
            || super::consumers::media_routes(catalog.as_ref()),
            |view| view.joined.clone(),
        )
    });
    if let Some(joined) = &joined {
        for route in joined
            .by_kind
            .get(request.kind.as_str())
            .into_iter()
            .flat_map(|kind| &kind.routes)
        {
            candidates.push(EngineCandidate::new(
                route.module.clone(),
                route.engine.clone(),
            ));
        }
    } else {
        for (module, slot) in super::consumers::engine_declarations() {
            candidates.extend(
                slot.engines
                    .into_iter()
                    .map(|engine| EngineCandidate::new(module.clone(), engine)),
            );
        }
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
    // Preserve declaration identity before any availability gate removes a
    // module engine: a same-named caller offer must never stand in for it.
    let declared_routes = candidates
        .iter()
        .filter(|candidate| {
            !candidate.selected.caller_local && candidate.engine.kind == request.kind
        })
        .map(|candidate| format!("{}/{}", candidate.engine.provider, candidate.engine.model))
        .collect();
    let mut excluded = std::collections::BTreeMap::new();
    // Withdrawal applies to media and caller-local engines before ranking.
    // Resolved facts also carry it when a credential uses a different provider
    // name from the catalog's provider name.
    candidates.retain(|candidate| {
        let key = format!("{}/{}", candidate.engine.provider, candidate.engine.model);
        let allowed = !catalog
            .as_ref()
            .is_some_and(|catalog| catalog.is_decommissioned(&key))
            && !inputs.facts.iter().any(|fact| {
                fact.decommissioned
                    && fact.provider_id == candidate.engine.provider
                    && fact.model_id == candidate.engine.model
            });
        if !allowed && evaluation.is_some() && candidate.engine.kind == request.kind {
            excluded.insert(key, "decommissioned".to_owned());
        }
        allowed
    });
    let mut accounts = crate::account_selection::ClassContext::read_for_sweep(
        deps.store,
        deps.broca_credentials,
        config.clone(),
        evaluation.as_ref().map_or_else(
            || crate::ACCOUNT_QUOTA.get(),
            |view| view.account_quota.as_ref(),
        ),
        inputs.facts.clone(),
        &inputs.catalog_snapshot,
        now as i64,
        evaluation.as_ref().map(|view| view.accounts.as_ref()),
    )
    .map_err(store_refusal)?;
    let mut top_routes = std::collections::BTreeSet::new();
    let active_media_providers: std::collections::BTreeSet<_> = if media.is_some() {
        let credentials = deps.broca_credentials.lock().map_err(store_refusal)?;
        credentials
            .listed_credentials()
            .iter()
            .flat_map(|row| {
                row.provider_ids
                    .iter()
                    .filter(|provider| {
                        crate::account_selection::class_context::media_credential_serves(
                            row, provider,
                        )
                    })
                    .cloned()
            })
            .collect()
    } else {
        Default::default()
    };
    let mut media_prices = std::collections::BTreeMap::new();
    for candidate in &mut candidates {
        candidate.cost = if media.is_some() {
            // Caller-owned execution has zero routing cost, but never a quoted
            // dollar price: no module account is charged for the offer.
            if candidate.selected.caller_local {
                caller_local_cost()
            } else {
                media_prices
                    .entry(format!(
                        "{}/{}",
                        candidate.engine.provider, candidate.engine.model
                    ))
                    .or_insert_with(|| {
                        catalog.as_ref().and_then(|catalog| {
                            pricing::estimate(
                                catalog,
                                &request,
                                media.and_then(|media| media.requirements.input_characters),
                                &candidate.engine.provider,
                                &candidate.engine.model,
                            )
                        })
                    })
                    .as_ref()
                    .map(|estimate| FactNumber(estimate.cost))
            }
        } else {
            catalog
                .as_ref()
                .filter(|_| media.is_none() || !candidate.selected.caller_local)
                .and_then(|catalog| {
                    catalog.engine_cost(
                        &request,
                        &candidate.engine.provider,
                        &candidate.engine.model,
                    )
                })
                .map(FactNumber)
        };
        let key = striatum_core::make_route_key(
            &candidate.engine.provider,
            &candidate.engine.model,
            None,
        );
        let policy_allowed = if media.is_some() {
            config.media_route_allowed(&request.consumer, &key, pin.as_deref() == Some(&key))
        } else {
            config.route_allowed(&request.consumer, &key, true)
        };
        if policy_allowed
            && (search.is_some()
                || candidate.engine.local
                || active_media_providers.contains(&candidate.engine.provider))
        {
            top_routes.insert(key.clone());
        }
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
        if candidate.fenced && evaluation.is_some() {
            excluded.insert(key.clone(), "route is fenced".to_owned());
        }
        if !candidate.engine.local && candidate.engine.kind == request.kind {
            // route.select_engine ignores every pin-only rule; media route.select
            // enforces model pin-only rules but not chat provider spending rules.
            // Both operations still refuse only_for and not_for exclusions.
            candidate.fenced |= !policy_allowed;
            // Search credentials belong to the declaring module. Its eligible
            // subset, not Broca's media credential inventory, admits the route.
            let reachable = search.is_some()
                || accounts
                    .engine_has_eligible_credential(
                        &candidate.engine.provider,
                        &candidate.engine.model,
                    )
                    .map_err(store_refusal)?;
            candidate.fenced |= !reachable;
            if !reachable && evaluation.is_some() {
                excluded
                    .entry(key.clone())
                    .or_insert_with(|| "no eligible media credential".into());
            }
        }
        #[cfg(test)]
        let facts = media_facts.get(&(key, candidate.engine.kind.as_str().to_owned()));
        #[cfg(test)]
        if let Some((_, cost)) = facts.filter(|_| media.is_none()) {
            candidate.cost = cost.filter(|value| value.is_finite()).map(FactNumber);
        }
        if search.is_some_and(|request| {
            request.allowance_left.as_ref().is_some_and(|routes| {
                routes.iter().any(|route| {
                    route.provider == candidate.engine.provider
                        && route.model == candidate.engine.model
                })
            })
        }) {
            candidate.cost = caller_local_cost();
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
    let (mut evaluations, samples) = if let Some(view) = evaluation.as_ref() {
        (view.evaluations.clone(), view.samples.clone())
    } else {
        deps.store
            .engine_measurements(
                request.kind.as_str(),
                metric,
                &identities.into_iter().collect::<Vec<_>>(),
            )
            .map_err(store_refusal)?
    };
    if let Some(limit) = media
        .and_then(|media| media.requirements.metric_limit.as_ref())
        .filter(|_| evaluation.is_none())
    {
        if Some(limit.metric.as_str()) != metric {
            let identities: Vec<_> = candidates
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
            evaluations.extend(
                deps.store
                    .engine_measurements(request.kind.as_str(), Some(&limit.metric), &identities)
                    .map_err(store_refusal)?
                    .0,
            );
        }
    }
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
    if let Some(media) = media {
        let mut failed_fact = None;
        candidates.retain(|candidate| {
            if candidate.engine.kind != request.kind {
                return true;
            }
            let key = format!("{}/{}", candidate.engine.provider, candidate.engine.model);
            let facts = (!candidate.selected.caller_local)
                .then(|| {
                    catalog
                        .as_ref()
                        .and_then(|catalog| catalog.video_facts(&key))
                })
                .flatten();
            if let Some(fact) = failed_video_requirement(media, facts) {
                failed_fact.get_or_insert(fact);
                if evaluation.is_some() {
                    excluded.insert(
                        key,
                        format!("catalog fact {fact} does not satisfy the request"),
                    );
                }
                return false;
            }
            true
        });
        if let Some(fact) = failed_fact.filter(|_| {
            !candidates
                .iter()
                .any(|candidate| candidate.engine.kind == request.kind)
        }) {
            if let Some(view) = evaluation.as_mut() {
                view.evidence["excluded"] = json!(excluded
                    .into_iter()
                    .map(|(route, reason)| json!({"route":route,"reason":reason}))
                    .collect::<Vec<_>>());
            }
            if let Some(pin) = &pin {
                return Err(Refusal::pin_unavailable(
                    pin,
                    PinUnavailableReason::NotAvailable,
                    None,
                ));
            }
            let mut refusal = Refusal::no_route(NoRouteReason::MissingCapability, None);
            refusal.message = format!("catalog fact {fact} does not satisfy the request");
            return Err(refusal);
        }
        candidates.retain(|candidate| {
            let key = format!("{}/{}", candidate.engine.provider, candidate.engine.model);
            // Local engines bypass the compatibility selector's reachability
            // fence, so apply policy as admission instead of a transient fence.
            let allowed =
                config.media_route_allowed(&request.consumer, &key, pin.as_deref() == Some(&key))
                    && media
                        .exclude
                        .as_ref()
                        .is_none_or(|routes| !routes.contains(&key))
                    && media
                        .exclude_providers
                        .as_ref()
                        .is_none_or(|providers| !providers.contains(&candidate.engine.provider))
                    && media
                        .eligible_routes
                        .as_ref()
                        .is_none_or(|routes| routes.iter().any(|route| route.route_key() == key));
            if !allowed && evaluation.is_some() {
                excluded.insert(
                    key.clone(),
                    config
                        .route_policy_refusal(&request.consumer, &key)
                        .unwrap_or_else(|| {
                            "excluded by media policy or request eligibility".into()
                        }),
                );
            }
            allowed
        });
        if let Some(limit) = &media.requirements.metric_limit {
            let mut floor_request = request.clone();
            floor_request.requirements.metric = Some(limit.metric.clone());
            measurements::apply_evaluations(&floor_request, &mut candidates, &evaluations);
            candidates.retain(|candidate| {
                let allowed = limit.admits(candidate.accuracy.map(EvalScore::get));
                if !allowed && evaluation.is_some() {
                    excluded.insert(
                        format!("{}/{}", candidate.engine.provider, candidate.engine.model),
                        "metric_limit".into(),
                    );
                }
                allowed
            });
        }
    }
    measurements::apply_evaluations(&request, &mut candidates, &evaluations);
    let mut media_grades = joined
        .as_ref()
        .and_then(|joined| joined.by_kind.get(request.kind.as_str()))
        .map(|kind| kind.grades.clone())
        .unwrap_or_default();
    if media.is_some() {
        for candidate in candidates
            .iter()
            .filter(|candidate| candidate.selected.caller_local)
        {
            let key = format!("{}/{}", candidate.engine.provider, candidate.engine.model);
            if let Some(grade) = catalog
                .as_ref()
                .and_then(|catalog| catalog.local_media_grade(request.kind.as_str(), &key))
            {
                media_grades.insert(key, grade);
            }
        }
    }
    let winner = if let Some(policy) = &policy {
        let grades = &media_grades;
        let minimums = media_prices
            .iter()
            .filter_map(|(key, price)| {
                price
                    .as_ref()?
                    .minimum
                    .map(|minimum| (key.clone(), minimum))
            })
            .collect();
        let ranked = ranking::rank(
            &request,
            &candidates,
            grades,
            &top_routes,
            &minimums,
            policy,
        );
        if let Some(view) = evaluation.as_mut() {
            for candidate in &candidates {
                if let Err(refusal) = wire::select_engine(&request, std::slice::from_ref(candidate))
                {
                    excluded
                        .entry(format!(
                            "{}/{}",
                            candidate.engine.provider, candidate.engine.model
                        ))
                        .or_insert_with(|| {
                            format!("{}: {}", refusal.code.as_str(), refusal.message)
                        });
                }
            }
            // A route can have several declarations. A rejected declaration
            // does not exclude an identity that another declaration can serve.
            for candidate in candidates.iter().filter(|candidate| {
                wire::select_engine(&request, std::slice::from_ref(*candidate)).is_ok()
            }) {
                excluded.remove(&format!(
                    "{}/{}",
                    candidate.engine.provider, candidate.engine.model
                ));
            }
            view.evidence["bar"] = ranked
                .bar
                .map_or_else(|| json!({}), |grade| json!({"grade":grade}));
            view.evidence["top_routable"] = ranked
                .top
                .map_or_else(|| json!({}), |grade| json!({"grade":grade}));
            view.evidence["adequate"] = json!(ranked
                .adequate
                .iter()
                .map(|candidate| media_candidate(candidate, grades))
                .collect::<Vec<_>>());
            view.evidence["excluded"] = json!(excluded
                .into_iter()
                .map(|(route, reason)| json!({"route":route,"reason":reason}))
                .collect::<Vec<_>>());
        }
        if let Some(refusal) = pin_refusal {
            return Err(refusal);
        }
        if let Some(pin) = &pin {
            ordering::entry(&request, &candidates, &declared_routes, pin).ok_or_else(|| {
                Refusal::pin_unavailable(pin, PinUnavailableReason::NotAvailable, None)
            })?
        } else if policy.mode == Some(crate::config_policy::ConsumerMode::Ladder) {
            policy
                .ladder
                .iter()
                .flatten()
                .find_map(|entry| ordering::entry(&request, &candidates, &declared_routes, entry))
                .ok_or_else(|| Refusal::no_route(NoRouteReason::NoAvailableModel, None))?
        } else {
            ranked.pick(policy)?
        }
    } else {
        wire::select_engine(&request, &candidates)?
    };
    if evaluation.is_some() {
        let empty = Default::default();
        let grades = joined
            .as_ref()
            .and_then(|joined| joined.by_kind.get(request.kind.as_str()))
            .map(|kind| &kind.grades)
            .unwrap_or(&empty);
        return Ok(media_candidate(winner, grades));
    }
    let pricing = (!winner.selected.caller_local)
        .then(|| {
            media_prices.get(&format!(
                "{}/{}",
                winner.engine.provider, winner.engine.model
            ))
        })
        .flatten()
        .and_then(Option::as_ref);
    let mut score = serde_json::json!({"accuracy": winner.accuracy.map(EvalScore::get), "cost": winner.cost.map(|value| value.0)});
    if let Some(pricing) = pricing {
        score["objective"] = serde_json::json!({"taskPricing": pricing.detail()});
    }
    let scope = caller.scope.as_ref();
    let decision = DecisionInput {
        decision_kind: "engine".into(),
        consumer: Some(request.consumer.clone()),
        target_agent: request.consumer.clone(),
        project_id: scope.and_then(|scope| scope.project_id.clone()),
        workdir: scope.and_then(|scope| scope.workdir.clone()),
        requirements_json: params.to_string(),
        selected_route_key: Some(striatum_core::make_route_key(
            &winner.engine.provider,
            &winner.engine.model,
            None,
        )),
        selected_provider_id: Some(winner.engine.provider.clone()),
        selected_model_id: Some(winner.engine.model.clone()),
        score_json: score.to_string(),
        rejected_json: "[]".into(),
        allowed_account_class: AllowedAccountClass::PricedPerTokenOnly,
        quota_signal_json: Some(inputs.quota_signal_json.clone()),
        ..Default::default()
    };
    let id = deps
        .store
        .record_decision_with_task_ref(&decision, winner.selected.module.as_deref(), None, None)
        .map_err(store_refusal)?;
    let answer = SelectEngineAnswer::new(&id, now + wire::DECISION_TTL_MS, winner.selected.clone());
    let value = if let Some(request) = media {
        let mut answer = SelectMediaAnswer::new(&id, answer.expires_at_ms, winner.selected.clone());
        if !winner.engine.local {
            answer.price = pricing.and_then(|pricing| pricing.price.clone());
        }
        if let Some(search) = search {
            search::check_answer(search, &answer)?;
        } else {
            media::check_answer(request, &answer)?;
        }
        serde_json::to_value(answer)
    } else {
        serde_json::to_value(&answer)
    }
    .map_err(store_refusal)?;
    crate::account_writer::v1_record_selection(
        deps.store,
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

fn media_candidate(
    candidate: &EngineCandidate<EvalScore, FactNumber>,
    grades: &std::collections::BTreeMap<String, f64>,
) -> Value {
    let route = format!("{}/{}", candidate.engine.provider, candidate.engine.model);
    json!({"grade": grades.get(&route), "route":route, "cost_usd":candidate.cost.map(|cost| cost.0)})
}

fn measurement_refusal(mut refusal: Refusal) -> Refusal {
    let message = match refusal.code.as_str() {
        "engine_not_declared_by_caller" => "Only the module that declares this engine for this kind in its role description may report measurements for it, and the calling module doesn't. Nothing was recorded.",
        "unknown_decision" => "The report names no engine decision routing made for this caller, engine and kind, so it can't count as that job's speed. Nothing was recorded.",
        // A request-parsing refusal already names the field it rejected.
        _ if !refusal.message.is_empty() => return refusal,
        _ => "Routing couldn't record this engine measurement. Nothing was recorded.",
    };
    refusal.message = message.into();
    refusal
}

fn measurement_store_refusal(error: impl std::fmt::Display) -> Refusal {
    let mut refusal = store_refusal(error);
    refusal.message = "Routing couldn't store this engine measurement just now. Nothing was recorded; send it again.".into();
    refusal
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
