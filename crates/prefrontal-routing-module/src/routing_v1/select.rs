//! `route.select` for `routing/v1`.
//!
//! The selection itself is routing's existing core: the same facts, quota
//! overlay, configuration overlays, billing-plane prices, credential
//! entitlement fences and operator exclusions every route
//! selection reads ([`crate::prepare_selection_inputs`],
//! [`crate::price_selection_inputs`]), the scorer
//! ([`select_policy::select_candidates`]) and the pin admission
//! ([`crate::work_model_pin::select_with_availability`]). What this module
//! adds is the v1 request: which runners can serve, the requirements the
//! scorer has no input for, the consumer's policy, the project's pin and the
//! affinity preference, and the decision binding.
//!
//! What it answers is exactly what runs: the decision it records names the
//! route and runner of the answer, and nothing selects a second time.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use cortexkit_role_routing::errors::{NoRouteReason, PinUnavailableReason, Refusal};
use cortexkit_role_routing::select::{
    parse_select_request_serving, Modality, RouteRef, RunnerRef, SelectAnswer, ToolsRequirement,
    EXPLANATION_MAX_CHARS,
};
use prefrontal_routing_core::model_router::{
    ModelRouteStateSnapshot, NoRouteError, RequiredCapabilities, RouteFailureReason,
    SelectedModelRoute,
};
use prefrontal_routing_core::slot_saturation::{ConcurrencySlotState, ConcurrencySlots};
use prefrontal_routing_core::{fact_to_route, ModelFact, ModelRequirements, RoutingConfig};
use prefrontal_routing_store::V1Decision;
use serde_json::{json, Value};

use super::{now_ms, operator_config, provider_refusal, store_refusal, Caller, Deps};
use crate::config_policy::RunnerPolicy;
use crate::{
    account_selection, config_overrides, select_policy, work_model_pin, DecisionProvenance,
    SelectionPolicySources,
};

/// The members a v1 select may carry.
const MEMBERS: &[&str] = &[
    "consumer",
    "requirements",
    "pin",
    "exclude",
    "runners",
    "affinity",
    "correlation_id",
    "concurrency_slots",
    "detail",
    "harness",
    "hold_ok",
    "hold_wait_max_s",
];

/// How long an answer stays current routing. A report after this is still
/// accepted; running on the decision then is the caller's risk.
const DECISION_TTL_MS: u64 = 10 * 60_000;

/// How long a select waits for the first billing-plane load before refusing,
/// the same bound the older select uses.
const BILLING_WAIT: Duration = Duration::from_secs(3);

/// Why the answered route was chosen, for the explanation and the detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Basis {
    RequestPin,
    ProjectPin,
    Affinity,
    Automatic,
}

impl Basis {
    fn as_str(self) -> &'static str {
        match self {
            Basis::RequestPin => "the request's pin",
            Basis::ProjectPin => "the project's operator pin",
            Basis::Affinity => "affinity with the previous decision",
            Basis::Automatic => "the lowest-cost route that meets the requirements",
        }
    }
}

pub(super) async fn select(
    params: &serde_json::Value,
    caller: &Caller,
    deps: &Deps<'_>,
) -> Result<Value, Refusal> {
    super::refuse_unknown_members(params, MEMBERS)?;
    let request = parse_select_request_serving(params, super::describe::CAPABILITIES)?;
    let live_config = config_overrides::live_config(deps.routing_config_path);
    let operator_config = operator_config(deps);
    let consumer = request.consumer.clone();
    let consumers = super::consumers::effective(&operator_config);
    consumers.admit(&consumer, caller)?;
    let effective_params = consumers.with_defaults(&consumer, params, &operator_config);
    let request = parse_select_request_serving(&effective_params, super::describe::CAPABILITIES)?;
    if !crate::broca_credentials::wait_for_billing_catalog(deps.broca_credentials, BILLING_WAIT)
        .await
    {
        return Err(provider_refusal(
            "routing_not_ready",
            "routing pricing is awaiting its first billing-plane load; retry shortly",
        ));
    }

    let operator_policy = operator_config.operator_policy();
    // Legacy classes still identify variant/reset groups. Payment class comes
    // only from the resolved consumer policy, not from the consumer's name.
    let legacy_consumer = select_policy::Consumer::from_id(&consumer);
    let consumer_policy = operator_config.consumer_policy(
        &consumer,
        &crate::config_policy::ConsumerPolicy {
            iq: Some(f64::from(request.requirements.iq)),
            eq: request.requirements.eq.map(f64::from),
            ..Default::default()
        },
        &consumers.policies,
    );
    let priced_per_token = select_policy::priced_per_token(&consumer_policy, legacy_consumer);
    let crate::PreparedSelectionInputs {
        now,
        mut facts,
        mut snapshot,
        quota_signal_json,
        quota_note: _,
    } = crate::prepare_selection_inputs(
        deps.quota_cache,
        deps.store,
        live_config.pricing(),
        &live_config.quota_model_pools(),
    )
    .map_err(store_refusal)?;
    // Providers with no catalog rows of their own, each resolved to the
    // catalog provider whose rows it borrows, read once for this selection.
    let catalog_targets = deps
        .broca_credentials
        .lock()
        .map(|credentials| credentials.catalog_targets(&facts))
        .unwrap_or_default();
    let sources = SelectionPolicySources {
        live_config: &live_config,
        operator_config: &operator_config,
        operator_config_path: deps.operator_config_path,
        catalog_targets: &catalog_targets,
    };
    let priced = crate::price_selection_inputs(
        &mut facts,
        quota_signal_json,
        &sources,
        deps.store,
        deps.broca_credentials,
        true,
        priced_per_token,
    )
    .map_err(|error| store_refusal(error.to_string()))?;

    // Runners the caller can send to that reach every tool capability tag the
    // work lists. Tags are matched; a tool provider's module name is not.
    let tags = request
        .requirements
        .tool_capabilities
        .clone()
        .unwrap_or_default();
    let registry = deps
        .broca_credentials
        .lock()
        .map(|credentials| {
            super::runners::Registry::build(
                operator_config.v1_runners(),
                &credentials,
                &facts,
                now as u64,
            )
        })
        .unwrap_or_default();
    let runners: Vec<RunnerPolicy> = registry
        .policies()
        .into_iter()
        .filter(|runner| {
            consumer_policy
                .runner
                .as_ref()
                .is_none_or(|module| module == &runner.module)
        })
        .filter(|runner| {
            request
                .runners
                .as_ref()
                .is_none_or(|listed| listed.contains(&runner.module))
        })
        .collect();
    let runners: Vec<RunnerPolicy> = runners
        .into_iter()
        .filter(|runner| {
            request.harness.as_deref().is_none_or(|harness| {
                registry
                    .get(&runner.module)
                    .is_some_and(|runner| runner.harnesses.iter().any(|hosted| hosted == harness))
            })
        })
        .collect();
    if runners.is_empty() {
        return Err(if let Some(harness) = &request.harness {
            let mut refusal = Refusal::no_route(NoRouteReason::HarnessUnavailable, None);
            refusal.message = format!("no session-starting runner declares harness {harness}");
            refusal
        } else {
            Refusal::no_route(NoRouteReason::NoAvailableModel, None)
        });
    }
    let runners: Vec<RunnerPolicy> = runners
        .into_iter()
        .filter(|runner| {
            tags.iter()
                .all(|tag| runner.tool_capabilities.contains(tag))
        })
        .collect();
    if runners.is_empty() {
        return Err(Refusal::no_route(NoRouteReason::MissingCapability, None));
    }
    let runs = |runner: &RunnerPolicy, route_key: &str, capacity_key: &str| -> bool {
        registry.runs(&runner.module, route_key, capacity_key)
    };

    // The routes some eligible runner runs, then the requirements the scorer
    // has no input for. Routing records no fact for PDF or audio input or for
    // structured output, so it cannot confirm a route serves them and answers
    // none.
    let reachable: Vec<&ModelFact> = facts
        .iter()
        .filter(|fact| {
            let route = fact_to_route(fact);
            runners
                .iter()
                .any(|runner| runs(runner, &route.route_key, &route.capacity_key))
        })
        .collect();
    let requirements = &request.requirements;
    let modalities = requirements.modalities.clone().unwrap_or_default();
    let unconfirmable = modalities
        .iter()
        .any(|modality| matches!(modality, Modality::Pdf | Modality::Audio))
        || requirements.structured_output == Some(true);
    let capable: Vec<&ModelFact> = reachable
        .iter()
        .copied()
        .filter(|_| !unconfirmable)
        .filter(|fact| {
            requirements.tools != Some(ToolsRequirement::Required)
                || fact.supports_tool_use == Some(true)
        })
        .filter(|fact| {
            requirements
                .max_output_tokens
                .is_none_or(|floor| fact.max_output_tokens.is_some_and(|limit| limit >= floor))
        })
        .collect();
    if capable.is_empty() && !reachable.is_empty() {
        return Err(Refusal::no_route(NoRouteReason::MissingCapability, None));
    }

    let available: HashSet<String> = capable
        .iter()
        .map(|fact| fact_to_route(fact).route_key)
        .collect();
    if let Some(classes) = &priced.classes {
        classes.apply_eligibility(&mut snapshot, &facts, priced_per_token, false);
    }
    let candidates = select_policy::prepare_candidates(
        &facts,
        &mut snapshot,
        available,
        HashMap::new(),
        select_policy::CandidateSource::Vault {
            credentials: deps.broca_credentials,
            fences: &priced.entitlement_fences,
            stale: &[],
            defer_pool_pin: false,
        },
        legacy_consumer,
        priced_per_token,
        None,
        priced.subscription_plan_providers.as_ref(),
        &operator_policy,
        &catalog_targets,
        priced.classes.as_ref(),
    );
    let available = candidates.available;
    let mut usable = available.clone();
    let mut account_blocks = HashMap::new();
    if let Some(classes) = &priced.classes {
        for fact in &facts {
            if !matches!(
                classes
                    .probe(
                        &fact.provider_id,
                        &fact.model_id,
                        priced_per_token,
                        false,
                        None,
                        "demand-preflight"
                    )
                    .outcome,
                prefrontal_routing_core::accounts::Outcome::Admit(_)
            ) {
                usable.remove(&prefrontal_routing_core::fact_to_route(fact).route_key);
                if let Some(reset) =
                    classes.blocked_reset(&fact.provider_id, &fact.model_id, priced_per_token)
                {
                    account_blocks.insert(
                        prefrontal_routing_core::fact_to_route(fact).route_key,
                        reset,
                    );
                }
            }
        }
    }
    let subscription_keys = candidates.subscription_keys;
    let rejections = candidates.rejected;

    // Operator score targets allow fractions; keep them in the internal demand
    // rather than sending them through the integer-only request validator.
    let operator_demand =
        operator_config.consumer_policy(&consumer, &Default::default(), &Default::default());
    let demand = ModelRequirements {
        iq: operator_demand
            .iq
            .unwrap_or_else(|| f64::from(requirements.iq)),
        eq: operator_demand
            .eq
            .unwrap_or_else(|| f64::from(requirements.eq.unwrap_or(0))),
    };
    let vision = modalities.contains(&Modality::Image);
    let required_capabilities = match (vision, requirements.context_window) {
        (false, None) if demand.iq > 0.0 || demand.eq > 0.0 => None,
        // The scorer refuses an all-zero demand that names no capability; a
        // zero context floor states "no demand" without adding a constraint.
        (vision, context) => Some(RequiredCapabilities {
            vision: vision.then_some(true),
            min_context: Some(context.map_or(0.0, |floor| floor as f64)),
        }),
    };
    let exclude: Vec<String> = request.exclude.clone().unwrap_or_default();
    let consumer_exclude = operator_config.exclude_groups().named_group(&consumer);
    let slots = request.concurrency_slots.as_ref().map(|slots| {
        ConcurrencySlots::new(slots.iter().map(|slot| ConcurrencySlotState {
            key: slot.key.clone(),
            running: u32::try_from(slot.running).unwrap_or(u32::MAX),
            limit: u32::try_from(slot.limit).unwrap_or(u32::MAX),
            queued: u32::try_from(slot.queued.unwrap_or(0)).unwrap_or(u32::MAX),
        }))
    });
    let selection = Selection {
        consumer: &consumer,
        priced_per_token,
        demand,
        required_capabilities,
        facts: &facts,
        snapshot: &snapshot,
        exclude: &exclude,
        operator_exclude: &operator_policy.exclude,
        consumer_exclude: &consumer_exclude,
        subscription_keys: &subscription_keys,
        rejections: &rejections,
        routing_config: &priced.routing_config,
        slots: slots.as_ref(),
        min_band: live_config.min_band(),
        providers_without_prompt_cache: live_config.providers_without_prompt_cache(),
        deps,
        sources: &sources,
        account_flags: live_config.account_flags(),
        scope: caller.scope.clone(),
        now,
        hold_ok: params
            .get("hold_ok")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        hold_wait_max_s: params.get("hold_wait_max_s").and_then(Value::as_u64),
        conversation_key: request.affinity.as_deref().unwrap_or("select"),
    };

    let project_pin = caller
        .scope
        .as_ref()
        .and_then(|scope| scope.project_id.as_deref())
        .and_then(|project| operator_config.project_pin(project, &consumer))
        .map(str::to_owned);
    let previous = match &request.affinity {
        Some(key) => deps
            .store
            .v1_affinity_decision(&caller.principal, &consumer, key)
            .map_err(store_refusal)?,
        None => None,
    };
    let continuing = previous
        .as_ref()
        .map(|previous| format!("{}/{}", previous.provider, previous.model));
    // Request pin, then project pin, then the continuing model, then the mode.
    // A pin does not fall through. The continuing model does, once it is no
    // longer allowed. Legacy targetAgent bodies never reach this surface.
    let ladder_choice = if request.pin.is_none()
        && project_pin.is_none()
        && continuing.is_none()
        && consumer_policy.mode == Some(crate::config_policy::ConsumerMode::Ladder)
    {
        let mut result = None;
        let mut fallback_hold = None;
        for entry in consumer_policy.ladder.iter().flatten() {
            let Some(route) = RouteRef::parse_pin(entry) else {
                continue;
            };
            let subset = available
                .iter()
                .filter(|key| {
                    *key == &route.route_key()
                        || key.starts_with(&format!("{}/", route.route_key()))
                })
                .cloned()
                .collect();
            if let Ok(selected) = selection.pass().score(&subset) {
                match selection.account_check(&selected) {
                    Ok(()) => {
                        result = Some(Ok((selected, select_policy::OrderBasis::Mode)));
                        break;
                    }
                    Err(refusal) if refusal.code.as_str() == "account_hold" => {
                        if selection.hold_ok {
                            result = Some(Ok((selected, select_policy::OrderBasis::Mode)));
                            break;
                        }
                        if fallback_hold.as_ref().is_none_or(
                            |(_, previous): &(SelectedModelRoute, Refusal)| {
                                previous.detail.get("reset_at_ms").and_then(Value::as_i64)
                                    > refusal.detail.get("reset_at_ms").and_then(Value::as_i64)
                            },
                        ) {
                            fallback_hold = Some((selected, refusal));
                        }
                    }
                    Err(_) => {}
                }
            }
        }
        result.or_else(|| {
            fallback_hold.map(|(selected, _)| Ok((selected, select_policy::OrderBasis::Mode)))
        })
    } else {
        None
    };
    let ordered = ladder_choice.unwrap_or_else(|| {
        selection.pass().select(
            &usable,
            request.pin.as_deref(),
            project_pin.as_deref(),
            continuing.as_deref(),
            |pin| selection.pin(pin, &available),
        )
    });
    let (mut selected, basis, explicit_variant) = match ordered {
        Ok((selected, order)) => {
            let explicit = match order {
                select_policy::OrderBasis::RequestPin => request
                    .pin
                    .as_deref()
                    .and_then(RouteRef::parse_pin)
                    .and_then(|pin| pin.variant),
                select_policy::OrderBasis::ProjectPin => project_pin
                    .as_deref()
                    .and_then(RouteRef::parse_pin)
                    .and_then(|pin| pin.variant),
                select_policy::OrderBasis::Continuing | select_policy::OrderBasis::Mode => None,
            };
            let basis = match order {
                select_policy::OrderBasis::RequestPin => Basis::RequestPin,
                select_policy::OrderBasis::ProjectPin => Basis::ProjectPin,
                select_policy::OrderBasis::Continuing => Basis::Affinity,
                select_policy::OrderBasis::Mode => Basis::Automatic,
            };
            (selected, basis, explicit)
        }
        Err(select_policy::OrderFailure::Pin(refusal)) => return Err(refusal),
        Err(select_policy::OrderFailure::Score(no_route)) => {
            selection.record_refusal(&no_route, &priced.provenance);
            // Account preflight is separate from model scoring. Only name an
            // account block when an otherwise adequate, policy-allowed model
            // would have been selected without that block.
            let mut resets = Vec::new();
            let blocked: HashSet<_> = account_blocks
                .iter()
                .filter_map(|(key, reset)| {
                    if consumer_policy.mode == Some(crate::config_policy::ConsumerMode::Ladder)
                        && !consumer_policy.ladder.iter().flatten().any(|entry| {
                            RouteRef::parse_pin(entry)
                                .is_some_and(|route| route.route_key() == *key)
                        })
                    {
                        return None;
                    }
                    if !available.contains(key)
                        || selection
                            .pass()
                            .score(&HashSet::from([key.clone()]))
                            .is_err()
                    {
                        return None;
                    }
                    resets.extend(*reset);
                    Some(key.clone())
                })
                .collect();
            if !blocked.is_empty() {
                return Err(Refusal::no_route(
                    NoRouteReason::parse("accounts_blocked"),
                    resets.into_iter().min().map(|reset| reset.max(0) as u64),
                ));
            }
            return Err(no_route_refusal(&no_route, now));
        }
    };
    let variant_choice = crate::apply_variant_policy(
        &mut selected,
        &operator_config,
        deps.operator_config_path,
        &catalog_targets,
        legacy_consumer,
        explicit_variant.as_deref(),
    );

    let runner = select_policy::choose_runner(
        &runners,
        consumer_policy.runner.as_deref(),
        &selected.route_key,
        &selected.capacity_key,
        &runs,
    )
    .ok_or_else(|| Refusal::no_route(NoRouteReason::NoAvailableModel, None))?;

    // Record the decision in the ledger every selection writes, then bind it.
    let mut decision = crate::decision_input_for(
        &consumer,
        &demand,
        Some(&selected),
        None,
        &crate::SelectParams {
            scope: caller.scope.clone(),
            ..crate::SelectParams::for_record()
        },
        Some(&priced.provenance),
        Some(&crate::policy_sources_line(&operator_policy)),
    );
    decision.consumer = Some(consumer.clone());
    // Selection chooses the route; account_for enforces its hold. Preserve the
    // effective wait cap so later policy edits cannot widen this decision.
    let max_hold_s = operator_config
        .allocator_policy(&selected.model.provider_id)
        .max_hold_s;
    let hold_cap = request
        .hold_wait_max_s
        .map_or(max_hold_s, |cap| cap.min(max_hold_s));
    let mut input: Value =
        serde_json::from_str(&decision.requirements_json).map_err(store_refusal)?;
    input["hold_wait_max_s"] = json!(hold_cap);
    decision.requirements_json = input.to_string();
    decision.allowed_account_class =
        prefrontal_routing_core::accounts::AllowedAccountClass::from_pay_per_token(
            priced_per_token,
        );
    let decision_id = deps
        .store
        .record_decision_with_selection(&decision, Some(&runner.module), None)
        .map_err(store_refusal)?;
    if registry
        .get(&runner.module)
        .is_some_and(|runner| runner.follows_account_choice)
    {
        // The decision stands without the serving identity: an outcome for
        // it is then recorded as for a route with several credentials.
        if let Err(error) = crate::remember_provider_identity(
            deps.store,
            &decision_id,
            &selected,
            deps.broca_credentials,
        ) {
            crate::warn_operator(format!(
                "[prefrontal-routing] WARN: v1 decision {decision_id} for {} has no remembered serving identity: {error}",
                selected.route_key
            ));
        }
    }
    let now_ms = now_ms();
    let expires_at_ms = now_ms + DECISION_TTL_MS;
    let route = RouteRef {
        provider: selected.model.provider_id.clone(),
        model: selected.model.model_id.clone(),
        variant: selected.prompt_params.variant.clone(),
    };
    deps.store
        .v1_record_decision(&V1Decision {
            decision_id: decision_id.clone(),
            principal: caller.principal.clone(),
            consumer: consumer.clone(),
            allowed_account_class: decision.allowed_account_class,
            project_id: caller
                .scope
                .as_ref()
                .and_then(|scope| scope.project_id.clone()),
            provider: route.provider.clone(),
            model: route.model.clone(),
            variant: route.variant.clone(),
            runner_module: runner.module.clone(),
            affinity: request.affinity.clone(),
            created_at_ms: now_ms as i64,
            expires_at_ms: expires_at_ms as i64,
        })
        .map_err(store_refusal)?;

    let fact = facts
        .iter()
        .filter(|fact| fact.provider_id == route.provider && fact.model_id == route.model)
        .min_by_key(|fact| u8::from(fact.variant != route.variant));
    let explanation = clamp_sentence(format!(
        "{} on {} for consumer {consumer}, chosen by {}.",
        route.pin(),
        runner.module,
        basis.as_str()
    ));
    let mut runner_ref = RunnerRef::new(runner.module.clone(), route.clone());
    if let Some(harness) = registry
        .get(&runner.module)
        .and_then(|runner| runner.selection_harness(request.harness.as_deref()))
    {
        runner_ref = runner_ref.with_harness(harness);
    }
    let answer = SelectAnswer {
        decision_id,
        route: route.clone(),
        runner: runner_ref,
        context_window: fact
            .and_then(|fact| fact.context_window)
            .map_or(0, |window| window as u64),
        max_output_tokens: fact.and_then(|fact| fact.max_output_tokens).unwrap_or(0),
        expires_at_ms,
        explanation,
        detail: (request.detail == Some(true)).then(|| {
            json!({
                "basis": basis.as_str(),
                "explanation": selected.explanation,
                "rejected": selected.rejected,
                "score": selected.score,
                "variant": variant_choice,
            })
        }),
    };
    serde_json::to_value(answer)
        .map_err(|error| provider_refusal("internal_error", error.to_string()))
}

/// Everything one select's automatic and pinned attempts share.
struct Selection<'a> {
    hold_ok: bool,
    hold_wait_max_s: Option<u64>,
    conversation_key: &'a str,
    consumer: &'a str,
    priced_per_token: bool,
    demand: ModelRequirements,
    required_capabilities: Option<RequiredCapabilities>,
    facts: &'a [ModelFact],
    snapshot: &'a ModelRouteStateSnapshot,
    exclude: &'a [String],
    operator_exclude: &'a [String],
    consumer_exclude: &'a [String],
    subscription_keys: &'a HashSet<String>,
    /// Route-local refusals, by route key ([`select_policy::candidate_rejections`]).
    rejections: &'a HashMap<String, String>,
    routing_config: &'a RoutingConfig,
    slots: Option<&'a ConcurrencySlots>,
    min_band: Option<f64>,
    providers_without_prompt_cache: Vec<String>,
    deps: &'a Deps<'a>,
    sources: &'a SelectionPolicySources<'a>,
    account_flags: config_overrides::AccountSelectionFlags,
    /// The scope the selecting route is bound under, recorded with a refusal
    /// the same way a decision records it.
    scope: Option<crate::DecisionScope>,
    now: f64,
}

impl<'a> Selection<'a> {
    fn pass(&self) -> select_policy::SelectionPass<'_> {
        select_policy::SelectionPass {
            consumer: self.consumer,
            target_agent: self.consumer,
            requirements: self.demand,
            facts: self.facts,
            snapshot: self.snapshot,
            exclude: self.exclude,
            operator_exclude: self.operator_exclude,
            required_capabilities: self.required_capabilities,
            subscription_keys: self.subscription_keys,
            priced_per_token: self.priced_per_token,
            min_band: self.min_band,
            prefer_prompt_cache: false,
            providers_without_prompt_cache: &self.providers_without_prompt_cache,
            consumer_exclude: self.consumer_exclude,
            rejections: self.rejections,
            config: self.routing_config,
            slots: self.slots,
            operator_config: self.sources.operator_config,
        }
    }

    /// A named model, through the same pin admission a work run's `model=`
    /// takes: hard admission only, never a quality ranking.
    fn pin(&self, pin: &str, available: &HashSet<String>) -> Result<SelectedModelRoute, Refusal> {
        let parsed = RouteRef::parse_pin(pin)
            .ok_or_else(|| Refusal::pin_unavailable(pin, PinUnavailableReason::Invalid, None))?;
        if self.exclude.contains(&parsed.route_key())
            || !self
                .sources
                .operator_config
                .route_allowed(self.consumer, &parsed.route_key(), true)
        {
            return Err(Refusal::pin_unavailable(
                pin,
                PinUnavailableReason::Excluded,
                None,
            ));
        }
        let variant = work_model_pin::named_variant(pin, self.facts);
        let (pin_key, pin_capacity) =
            work_model_pin::route_keys(pin, variant.as_deref(), self.facts);
        let snapshot = self.snapshot.clone();
        let refused = |reason: &str| {
            let (reason, reset) = pin_reason(reason, &snapshot, &pin_key, &pin_capacity, self.now);
            Refusal::pin_unavailable(pin, reason, reset)
        };
        let catalog = config_overrides::reasoning_catalog_for(self.sources.operator_config_path);
        let selected = work_model_pin::select_with_availability(
            pin,
            self.pass().input(available, snapshot.clone()),
            self.routing_config,
            self.slots,
            catalog.as_ref(),
            self.sources.catalog_targets,
            self.rejections,
        )
        .map_err(|refusal| refused(&refusal.code))?;
        if let Err(refusal) = self.account_check(&selected) {
            if refusal.code.as_str() != "account_hold" {
                return Err(refusal);
            }
        }
        Ok(selected)
    }

    fn account_check(&self, selected: &SelectedModelRoute) -> Result<(), Refusal> {
        let policy = self
            .sources
            .operator_config
            .allocator_policy(&selected.model.provider_id);
        let ctx = super::account_ctx(self.deps, self.account_flags, &policy)?;
        if let Some(classes) = &ctx.classes {
            match classes
                .probe(
                    &selected.model.provider_id,
                    &selected.model.model_id,
                    self.priced_per_token,
                    true,
                    self.hold_wait_max_s,
                    self.conversation_key,
                )
                .outcome
            {
                prefrontal_routing_core::accounts::Outcome::Admit(_) => return Ok(()),
                prefrontal_routing_core::accounts::Outcome::Hold(hold) => {
                    let mut refusal = Refusal::account_hold(hold.reset_at_ms.max(0) as u64);
                    refusal
                        .detail
                        .insert("retry_after_s".into(), json!(hold.retry_after_s));
                    refusal
                        .detail
                        .insert("reset_at_ms".into(), json!(hold.reset_at_ms));
                    return Err(refusal);
                }
                _ => {}
            }
        }
        account_selection::preflight_model_for_class(
            &ctx,
            &selected.model.provider_id,
            &selected.model.model_id,
            self.priced_per_token,
        )
        .map_err(|(code, message)| provider_refusal(&code, message))
    }

    /// Record a refused selection in the ledger, as every selection does.
    fn record_refusal(&self, no_route: &NoRouteError, provenance: &DecisionProvenance) {
        let mut decision = crate::decision_input_for(
            self.consumer,
            &self.demand,
            None,
            Some(no_route),
            &crate::SelectParams {
                scope: self.scope.clone(),
                ..crate::SelectParams::for_record()
            },
            Some(provenance),
            None,
        );
        decision.consumer = Some(self.consumer.to_owned());
        crate::record_failed_selection_decision(self.deps.store.as_ref(), &decision);
    }
}

/// The pin's refusal reason, and when a timed cause ends. A cooldown, a
/// failure backoff and exhausted quota are timed when the route's state
/// names a future deadline for them.
fn pin_reason(
    code: &str,
    snapshot: &ModelRouteStateSnapshot,
    route_key: &str,
    capacity_key: &str,
    now: f64,
) -> (PinUnavailableReason, Option<u64>) {
    let states = [
        snapshot.by_route.get(route_key),
        snapshot.by_capacity.get(capacity_key),
    ];
    let deadline = |pick: fn(&prefrontal_routing_core::RouteRuntimeState) -> Option<f64>| {
        states
            .iter()
            .flatten()
            .filter_map(|state| pick(state))
            .filter(|until| *until > now)
            .max_by(f64::total_cmp)
            .map(|until| until as u64)
    };
    match code {
        "invalid_model_pin" => (PinUnavailableReason::Invalid, None),
        "unknown_model_provider" => (PinUnavailableReason::UnknownProvider, None),
        "unknown_model" => (PinUnavailableReason::UnknownModel, None),
        "invalid_model_variant" => (PinUnavailableReason::InvalidVariant, None),
        "excluded" | "caller_excluded" | "excluded_by_caller" => {
            (PinUnavailableReason::Excluded, None)
        }
        "credential_unentitled" => (PinUnavailableReason::CredentialUnentitled, None),
        "credential_billing_exhausted" => (PinUnavailableReason::CredentialBillingExhausted, None),
        "cooldown_active" => (
            PinUnavailableReason::Cooldown,
            deadline(|state| state.cooldown_until),
        ),
        "backoff_active" => (
            PinUnavailableReason::Backoff,
            deadline(|state| state.failure_backoff_until),
        ),
        "quota_exhausted" | "account_pool_exhausted" => (
            PinUnavailableReason::QuotaExhausted,
            deadline(|state| state.quota_exhausted_until),
        ),
        _ => (PinUnavailableReason::NotAvailable, None),
    }
}

/// The scorer's failure as a v1 `no_route`. Only the routes blocked by
/// exhausted quota, a cooldown or a failure backoff make it timed, by the
/// earliest deadline among them.
fn no_route_refusal(no_route: &NoRouteError, now: f64) -> Refusal {
    let reason = match no_route.reason {
        RouteFailureReason::NoAvailableModel => NoRouteReason::NoAvailableModel,
        RouteFailureReason::MissingRequiredCapability => NoRouteReason::MissingCapability,
        RouteFailureReason::ContextTooSmall => NoRouteReason::ContextTooSmall,
        RouteFailureReason::QuotaOrCooldownExhausted => NoRouteReason::QuotaOrCooldown,
        RouteFailureReason::NoAdequateRoute
        | RouteFailureReason::AllRoutesInadequate
        | RouteFailureReason::InvalidRequirements => NoRouteReason::AllInadequate,
    };
    let reset = (reason == NoRouteReason::QuotaOrCooldown)
        .then_some(no_route.earliest_reset)
        .flatten()
        .filter(|reset| *reset > now)
        .map(|reset| reset as u64);
    let mut refusal = Refusal::no_route(reason, reset);
    refusal.message = format!("{}: {}", refusal.message, no_route.refusal_message());
    refusal
        .detail
        .insert("rejected".into(), json!(no_route.rejected));
    refusal
}

/// One sentence of at most [`EXPLANATION_MAX_CHARS`] characters.
fn clamp_sentence(sentence: String) -> String {
    if sentence.chars().count() <= EXPLANATION_MAX_CHARS {
        return sentence;
    }
    let mut clamped: String = sentence.chars().take(EXPLANATION_MAX_CHARS - 1).collect();
    clamped.push('.');
    clamped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_long_explanation_is_clamped_to_one_bounded_sentence() {
        let long = "x".repeat(EXPLANATION_MAX_CHARS + 50);
        let clamped = clamp_sentence(long);
        assert_eq!(clamped.chars().count(), EXPLANATION_MAX_CHARS);
        assert!(clamped.ends_with('.'));
    }

    #[test]
    fn only_quota_and_cooldown_refusals_carry_a_reset() {
        let refusal = |reason, reset| NoRouteError {
            reason,
            message: "m".into(),
            rejected: Vec::new(),
            requirements: None,
            best_rejected: None,
            earliest_reset: reset,
            earliest_reset_route: None,
        };
        let timed = no_route_refusal(
            &refusal(RouteFailureReason::QuotaOrCooldownExhausted, Some(2_000.0)),
            1_000.0,
        );
        assert_eq!(timed.wait_until_ms(), Some(2_000));
        let past = no_route_refusal(
            &refusal(RouteFailureReason::QuotaOrCooldownExhausted, Some(500.0)),
            1_000.0,
        );
        assert_eq!(past.wait_until_ms(), None);
        let untimed = no_route_refusal(
            &refusal(RouteFailureReason::AllRoutesInadequate, Some(2_000.0)),
            1_000.0,
        );
        assert_eq!(untimed.wait_until_ms(), None);
    }
}
