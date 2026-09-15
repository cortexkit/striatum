//! Post-selection fast-variant upgrade. Ported VERBATIM from the TS
//! `fast-variant-upgrade.ts`.
//!
//! The cost-primary router settles the conservative sibling of a "-fast" model
//! pair under normal quota conditions (quota conservation by default). When the
//! provider's quota PROJECTION is relaxed (lockout-cost pressure near zero), that
//! conservatism buys nothing, so this step swaps the selection to its
//! `<modelID>-fast` sibling when config opts the provider/model in.
//!
//! Self-correcting: burning the fast variant raises the burn rate, the pace
//! projection tightens, pressure rises past the threshold, and selection quietly
//! returns to the normal sibling.

use crate::model_facts::{fact_to_route, ModelFact, ModelRequirements, ModelRoute, FAST_SUFFIX};
use crate::model_router::{
    admit_routes, capability_rejection_reason, compute_objective, latest_deadline,
    ModelRouteStateSnapshot, PromptParams, RequiredCapabilities, RouteRuntimeState, SelectedModel,
    SelectedModelRoute,
};
use crate::provider_model_spec::{parse_spec_list, resolve_spec, ExclusionPolicy, ParsedSpec};
use crate::route_scoring::score_adequacy;
use crate::routing_config::RoutingConfig;

/// Pressure at or below which a quota projection counts as "relaxed" (built-in, no
/// config knob): the projected lockout cost is trivial.
pub const FAST_UPGRADE_MAX_PRESSURE: f64 = 0.25;

/// Merge capacity-shared and route-specific runtime state.
fn effective_runtime(
    snapshot: &ModelRouteStateSnapshot,
    route_key: &str,
    capacity_key: &str,
) -> RouteRuntimeState {
    let by_capacity = snapshot
        .by_capacity
        .get(capacity_key)
        .cloned()
        .unwrap_or_default();
    let by_route = snapshot
        .by_route
        .get(route_key)
        .cloned()
        .unwrap_or_default();
    RouteRuntimeState {
        // Hard blocks compose across scopes; soft route telemetry replaces shared
        // telemetry only when the route has a value.
        cooldown_until: latest_deadline(by_route.cooldown_until, by_capacity.cooldown_until),
        quota_exhausted_until: latest_deadline(
            by_route.quota_exhausted_until,
            by_capacity.quota_exhausted_until,
        ),
        failure_backoff_until: by_route.failure_backoff_until,
        failure_backoff_reason: by_route.failure_backoff_reason,
        success_ema: by_route.success_ema.or(by_capacity.success_ema),
        quota_pressure: by_route.quota_pressure.or(by_capacity.quota_pressure),
        quota_window_model: by_route
            .quota_window_model
            .or(by_capacity.quota_window_model),
        latency_ema_ms: by_route.latency_ema_ms.or(by_capacity.latency_ema_ms),
    }
}

fn is_blocked(state: &RouteRuntimeState, now: f64) -> bool {
    if state.cooldown_until.is_some_and(|v| v > now)
        || state.failure_backoff_until.is_some_and(|until| until > now)
    {
        return true;
    }
    if state.quota_exhausted_until.is_some_and(|v| v > now) {
        return true;
    }
    false
}

pub struct FastVariantUpgradeInput<'a> {
    pub selected: &'a SelectedModelRoute,
    pub facts: &'a [ModelFact],
    pub snapshot: &'a ModelRouteStateSnapshot,
    pub requirements: &'a ModelRequirements,
    pub required_capabilities: Option<RequiredCapabilities>,
    pub available_models: &'a std::collections::HashSet<String>,
    pub prefer_fast_specs: &'a [String],
    pub exclude_route_keys: &'a [String],
    pub is_subscription_route: Option<&'a dyn Fn(&ModelRoute) -> bool>,
    pub now: f64,
    /// True only when this provider returned a live quota entry in the latest
    /// successful refresh and that observation is still recent enough to trust.
    pub quota_signal_fresh: bool,
}

/// Maybe upgrade the selected route to its "-fast" sibling. Returns a new
/// `SelectedModelRoute` when the upgrade applies, else `None` (mutates nothing).
pub fn maybe_upgrade_to_fast_variant(
    input: &FastVariantUpgradeInput,
    config: &RoutingConfig,
) -> Option<SelectedModelRoute> {
    let specs: Vec<ParsedSpec> = parse_spec_list(input.prefer_fast_specs);
    if specs.is_empty() {
        return None;
    }

    let provider_id = &input.selected.model.provider_id;
    let model_id = &input.selected.model.model_id;
    // Already a fast variant: nothing to do.
    if model_id.ends_with(FAST_SUFFIX) {
        return None;
    }
    // Opt-in check on the SELECTED model (base=false: only an explicit allow counts).
    if !resolve_spec(&specs, &format!("{provider_id}/{model_id}"), false) {
        return None;
    }

    let fast_model_id = format!("{model_id}{FAST_SUFFIX}");
    // Prefer the variant-less fast fact, else any fast fact for the model.
    let fast_fact = input
        .facts
        .iter()
        .find(|f| {
            f.provider_id == *provider_id && f.model_id == fast_model_id && f.variant.is_none()
        })
        .or_else(|| {
            input
                .facts
                .iter()
                .find(|f| f.provider_id == *provider_id && f.model_id == fast_model_id)
        })?;

    let fast_route = fact_to_route(fast_fact);
    let fast_route_key = fast_route.route_key.clone();
    let fast_capacity_key = fast_route.capacity_key.clone();

    // The fast sibling must itself be entitled, not excluded, not blocked.
    if input
        .exclude_route_keys
        .iter()
        .any(|k| k == &fast_route_key)
    {
        return None;
    }
    // A negation spec on the fast model itself opts it out.
    if !resolve_spec(&specs, &format!("{provider_id}/{fast_model_id}"), true) {
        return None;
    }
    if !input.available_models.contains(&fast_route_key)
        && !input.available_models.contains(&fast_capacity_key)
    {
        return None;
    }

    let fast_runtime = effective_runtime(input.snapshot, &fast_route_key, &fast_capacity_key);
    if is_blocked(&fast_runtime, input.now) {
        return None;
    }

    // Relaxed-projection gate. The fast-variant upgrade is OPTIMISTIC (it burns the
    // fast sibling's quota faster), so it must require POSITIVE, FRESH evidence that
    // quota is actually relaxed; never the mere ABSENCE of a signal:
    //   - FRESH: this provider returned a live entry in the latest successful poll,
    //     and that provider observation is recent. Retained windows never count.
    //   - PRESENT: the fast sibling must have an actual pressure reading (Some). The
    //     old `unwrap_or(0.0)` treated a MISSING window as "relaxed", so an unmapped
    //     or never-fetched provider would upgrade on no evidence at all.
    //   - LOW: the present reading must be at/below the relaxed threshold.
    // Any of stale / absent / high keeps the conservative normal sibling.
    if !input.quota_signal_fresh {
        return None;
    }
    let pressure = fast_runtime.quota_pressure?;
    if pressure > FAST_UPGRADE_MAX_PRESSURE {
        return None;
    }

    if capability_rejection_reason(fast_fact, input.required_capabilities).is_some() {
        return None;
    }

    // Recompute the static request frontier without quota state. A transient
    // quota signal may permit the upgrade, but it must never redefine the bar.
    // The operator fence reaches this path through `available_models`, which the
    // selection resolved against the operator policy before the upgrade was
    // considered: an operator-excluded fast sibling is not in that set.
    let top_routable = admit_routes(
        input.facts,
        None,
        ExclusionPolicy::caller_only(input.exclude_route_keys),
        input.prefer_fast_specs,
        &[],
        input.required_capabilities,
        input.available_models,
        input.requirements,
        config,
    )
    .top_routable;
    let adequacy = score_adequacy(fast_fact, input.requirements, top_routable, config);
    if !adequacy.clears_adequacy_floor() {
        return None;
    }

    let is_subscription = input
        .is_subscription_route
        .is_some_and(|predicate| predicate(&fast_route));
    let (objective, _) = compute_objective(&fast_route, &fast_runtime, is_subscription);

    let mut upgraded = input.selected.clone();
    upgraded.model = SelectedModel {
        provider_id: fast_fact.provider_id.clone(),
        model_id: fast_fact.model_id.clone(),
        variant: fast_fact.variant.clone(),
    };
    upgraded.route_key = fast_route_key.clone();
    upgraded.capacity_key = fast_capacity_key;
    upgraded.score.selection_cost = objective.selection_cost;
    upgraded.score.objective = objective;
    if !adequacy.capability_only {
        upgraded.score.adequacy = adequacy.adequacy;
        upgraded.score.raw_adequacy = adequacy.raw_adequacy;
        upgraded.score.metrics = adequacy.metrics;
    }
    upgraded.prompt_params = PromptParams {
        provider_id: fast_fact.provider_id.clone(),
        model_id: fast_fact.model_id.clone(),
        variant: fast_fact
            .prompt_defaults
            .as_ref()
            .and_then(|d| d.variant.clone())
            .or_else(|| fast_fact.variant.clone()),
        temperature: fast_fact
            .prompt_defaults
            .as_ref()
            .and_then(|d| d.temperature),
        reasoning_effort: fast_fact
            .prompt_defaults
            .as_ref()
            .and_then(|d| d.reasoning_effort)
            .map(|e| e.as_str().to_string()),
    };
    upgraded.explanation.push(format!(
        "fast upgrade: quota relaxed (pressure {pressure:.2} <= {FAST_UPGRADE_MAX_PRESSURE}) -> {fast_route_key}"
    ));
    Some(upgraded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_facts::{
        make_capacity_key, make_route_key, Confidence, ModelPromptDefaults, ModelScores,
    };
    use crate::model_router::{ObjectiveBreakdown, SelectedRouteScore};
    use crate::routing_config::DEFAULT_ROUTING_CONFIG;
    use std::collections::{BTreeMap, HashMap, HashSet};

    fn fact(provider: &str, model: &str, elo: f64) -> ModelFact {
        ModelFact {
            speed_is_fleet_median: false,
            provider_id: provider.into(),
            model_id: model.into(),
            variant: None,
            family: None,
            scores: ModelScores {
                elo,
                eq: 1700.0,
                speed: 100.0,
            },
            context_window: Some(200_000.0),
            max_output_tokens: None,
            cost_tier: Some(0.0),
            cost_per_m_tok_out: Some(0.0),
            input_rate: None,
            cache_read_rate: None,
            cache_write_rate: None,
            reasoning_rate: None,
            supports_vision: Some(true),
            supports_tool_use: Some(true),
            prompt_defaults: Some(ModelPromptDefaults {
                variant: None,
                temperature: None,
                reasoning_effort: None,
            }),
            confidence: Confidence::Curated,
            notes: None,
            speed_multiplier: None,
            subscription_multiplier: None,
            score_provenance: crate::model_facts::ScoreProvenance::Curated,
            aa_index: None,
            review_by_ms: None,
            eq_source: None,
        }
    }

    // A minimal selected route for the base (non-fast) model.
    fn selected(provider: &str, model: &str) -> SelectedModelRoute {
        SelectedModelRoute {
            model: SelectedModel {
                provider_id: provider.into(),
                model_id: model.into(),
                variant: None,
            },
            route_key: make_route_key(provider, model, None),
            capacity_key: make_capacity_key(provider, model),
            score: SelectedRouteScore {
                adequacy: 1.0,
                raw_adequacy: 1.0,
                selection_cost: 0.0,
                metrics: BTreeMap::new(),
                objective: ObjectiveBreakdown {
                    cost: 0.0,
                    quota: 0.0,
                    speed_score: 0.5,
                    selection_cost: 0.0,
                },
            },
            prompt_params: PromptParams {
                provider_id: provider.into(),
                model_id: model.into(),
                variant: None,
                temperature: None,
                reasoning_effort: None,
            },
            rejected: Vec::new(),
            explanation: vec!["base pick".into()],
            below_floor: None,
        }
    }

    fn avail(keys: &[&str]) -> HashSet<String> {
        keys.iter().map(|s| s.to_string()).collect()
    }

    fn reqs() -> ModelRequirements {
        ModelRequirements { iq: 50.0, eq: 0.0 }
    }

    // A snapshot carrying a present low (relaxed) pressure for the fast sibling, so
    // the freshness/present/low gate is satisfied by default in the upgrade tests.
    fn relaxed_snap(fast_capacity_key: &str) -> ModelRouteStateSnapshot {
        let mut by_capacity = HashMap::new();
        by_capacity.insert(
            fast_capacity_key.to_string(),
            RouteRuntimeState {
                quota_pressure: Some(0.0),
                ..Default::default()
            },
        );
        ModelRouteStateSnapshot {
            now: 1000.0,
            by_route: HashMap::new(),
            by_capacity,
        }
    }

    #[test]
    fn no_specs_means_no_upgrade() {
        let facts = vec![
            fact("openai", "gpt-5.5", 55.0),
            fact("openai", "gpt-5.5-fast", 55.0),
        ];
        let sel = selected("openai", "gpt-5.5");
        let snap = ModelRouteStateSnapshot {
            now: 1000.0,
            by_route: HashMap::new(),
            by_capacity: HashMap::new(),
        };
        let out = maybe_upgrade_to_fast_variant(
            &FastVariantUpgradeInput {
                selected: &sel,
                facts: &facts,
                snapshot: &snap,
                requirements: &reqs(),
                required_capabilities: None,
                available_models: &avail(&["openai/gpt-5.5", "openai/gpt-5.5-fast"]),
                prefer_fast_specs: &[],
                exclude_route_keys: &[],
                is_subscription_route: None,
                now: 1000.0,
                quota_signal_fresh: true,
            },
            &DEFAULT_ROUTING_CONFIG,
        );
        assert!(out.is_none());
    }

    #[test]
    fn upgrades_to_fast_when_opted_in_relaxed_and_adequate() {
        let facts = vec![
            fact("openai", "gpt-5.5", 55.0),
            fact("openai", "gpt-5.5-fast", 55.0),
        ];
        let sel = selected("openai", "gpt-5.5");
        // A PRESENT, fresh, low pressure for the fast sibling (the relaxed evidence).
        let snap = relaxed_snap("openai/gpt-5.5-fast");
        let out = maybe_upgrade_to_fast_variant(
            &FastVariantUpgradeInput {
                selected: &sel,
                facts: &facts,
                snapshot: &snap,
                requirements: &reqs(),
                required_capabilities: None,
                available_models: &avail(&["openai/gpt-5.5", "openai/gpt-5.5-fast"]),
                prefer_fast_specs: &["openai/gpt-5.5".into()],
                exclude_route_keys: &[],
                is_subscription_route: None,
                now: 1000.0,
                quota_signal_fresh: true,
            },
            &DEFAULT_ROUTING_CONFIG,
        )
        .expect("should upgrade");
        assert_eq!(out.route_key, "openai/gpt-5.5-fast");
        assert!(out.explanation.last().unwrap().contains("fast upgrade"));
    }

    #[test]
    fn live_capacity_block_beats_expired_route_deadline() {
        let facts = vec![
            fact("openai", "gpt-5.5", 55.0),
            fact("openai", "gpt-5.5-fast", 55.0),
        ];
        let sel = selected("openai", "gpt-5.5");
        let snap = ModelRouteStateSnapshot {
            now: 1_000.0,
            by_route: HashMap::from([(
                "openai/gpt-5.5-fast".into(),
                RouteRuntimeState {
                    quota_exhausted_until: Some(500.0),
                    ..Default::default()
                },
            )]),
            by_capacity: HashMap::from([(
                "openai/gpt-5.5-fast".into(),
                RouteRuntimeState {
                    quota_exhausted_until: Some(2_000.0),
                    quota_pressure: Some(0.0),
                    ..Default::default()
                },
            )]),
        };

        let out = maybe_upgrade_to_fast_variant(
            &FastVariantUpgradeInput {
                selected: &sel,
                facts: &facts,
                snapshot: &snap,
                requirements: &reqs(),
                required_capabilities: None,
                available_models: &avail(&["openai/gpt-5.5", "openai/gpt-5.5-fast"]),
                prefer_fast_specs: &["openai/gpt-5.5".into()],
                exclude_route_keys: &[],
                is_subscription_route: None,
                now: 1_000.0,
                quota_signal_fresh: true,
            },
            &DEFAULT_ROUTING_CONFIG,
        );

        assert!(out.is_none());
    }

    #[test]
    fn upgraded_route_recomputes_its_objective() {
        let base = fact("openai", "gpt-5.5", 55.0);
        let mut fast = fact("openai", "gpt-5.5-fast", 55.0);
        fast.cost_per_m_tok_out = Some(30.0);
        fast.scores.speed = 180.0;
        let facts = vec![base, fast];
        let sel = selected("openai", "gpt-5.5");
        let mut snap = relaxed_snap("openai/gpt-5.5-fast");
        snap.by_capacity
            .get_mut("openai/gpt-5.5-fast")
            .unwrap()
            .quota_pressure = Some(0.2);

        let out = maybe_upgrade_to_fast_variant(
            &FastVariantUpgradeInput {
                selected: &sel,
                facts: &facts,
                snapshot: &snap,
                requirements: &reqs(),
                required_capabilities: None,
                available_models: &avail(&["openai/gpt-5.5", "openai/gpt-5.5-fast"]),
                prefer_fast_specs: &["openai/gpt-5.5".into()],
                exclude_route_keys: &[],
                is_subscription_route: None,
                now: 1_000.0,
                quota_signal_fresh: true,
            },
            &DEFAULT_ROUTING_CONFIG,
        )
        .expect("should upgrade");
        let fast_route = fact_to_route(&facts[1]);
        let fast_runtime =
            effective_runtime(&snap, &fast_route.route_key, &fast_route.capacity_key);
        let (expected, _) = compute_objective(&fast_route, &fast_runtime, false);

        assert_eq!(out.score.objective, expected);
        assert_eq!(out.score.selection_cost, expected.selection_cost);
        assert_ne!(out.score.objective, sel.score.objective);
    }

    #[test]
    fn does_not_upgrade_on_a_stale_quota_signal_even_if_pressure_reads_low() {
        // A retained stale window still reads low pressure, so a low reading alone
        // is not enough; the provider must have returned a live entry.
        let facts = vec![
            fact("openai", "gpt-5.5", 55.0),
            fact("openai", "gpt-5.5-fast", 55.0),
        ];
        let sel = selected("openai", "gpt-5.5");
        let snap = relaxed_snap("openai/gpt-5.5-fast"); // low pressure present...
        let out = maybe_upgrade_to_fast_variant(
            &FastVariantUpgradeInput {
                selected: &sel,
                facts: &facts,
                snapshot: &snap,
                requirements: &reqs(),
                required_capabilities: None,
                available_models: &avail(&["openai/gpt-5.5", "openai/gpt-5.5-fast"]),
                prefer_fast_specs: &["openai/gpt-5.5".into()],
                exclude_route_keys: &[],
                is_subscription_route: None,
                now: 1000.0,
                quota_signal_fresh: false, // ...but the signal is STALE
            },
            &DEFAULT_ROUTING_CONFIG,
        );
        assert!(
            out.is_none(),
            "a stale low-pressure reading must not trigger the optimistic upgrade"
        );
    }

    #[test]
    fn does_not_upgrade_when_the_fast_sibling_has_no_pressure_reading_at_all() {
        // Absence of a window must NOT read as relaxed (the old unwrap_or(0.0) bug):
        // an unmapped / never-fetched provider has no evidence, so stay conservative.
        let facts = vec![
            fact("openai", "gpt-5.5", 55.0),
            fact("openai", "gpt-5.5-fast", 55.0),
        ];
        let sel = selected("openai", "gpt-5.5");
        let snap = ModelRouteStateSnapshot {
            now: 1000.0,
            by_route: HashMap::new(),
            by_capacity: HashMap::new(),
        };
        let out = maybe_upgrade_to_fast_variant(
            &FastVariantUpgradeInput {
                selected: &sel,
                facts: &facts,
                snapshot: &snap,
                requirements: &reqs(),
                required_capabilities: None,
                available_models: &avail(&["openai/gpt-5.5", "openai/gpt-5.5-fast"]),
                prefer_fast_specs: &["openai/gpt-5.5".into()],
                exclude_route_keys: &[],
                is_subscription_route: None,
                now: 1000.0,
                quota_signal_fresh: true, // fresh cache, but no window for this route
            },
            &DEFAULT_ROUTING_CONFIG,
        );
        assert!(
            out.is_none(),
            "no pressure reading = no evidence of relaxed = no upgrade"
        );
    }

    #[test]
    fn does_not_upgrade_when_fast_sibling_misses_required_vision() {
        let base = fact("openai", "gpt-5.5", 55.0);
        let mut fast = fact("openai", "gpt-5.5-fast", 55.0);
        fast.supports_vision = Some(false);
        let facts = vec![base, fast];
        let sel = selected("openai", "gpt-5.5");
        let snap = relaxed_snap("openai/gpt-5.5-fast");
        let out = maybe_upgrade_to_fast_variant(
            &FastVariantUpgradeInput {
                selected: &sel,
                facts: &facts,
                snapshot: &snap,
                requirements: &reqs(),
                required_capabilities: Some(RequiredCapabilities {
                    vision: Some(true),
                    min_context: None,
                }),
                available_models: &avail(&["openai/gpt-5.5", "openai/gpt-5.5-fast"]),
                prefer_fast_specs: &["openai/gpt-5.5".into()],
                exclude_route_keys: &[],
                is_subscription_route: None,
                now: 1000.0,
                quota_signal_fresh: true,
            },
            &DEFAULT_ROUTING_CONFIG,
        );
        assert!(
            out.is_none(),
            "a required-vision task must stay on the vision-capable base model"
        );
    }

    #[test]
    fn does_not_upgrade_when_fast_sibling_context_is_unknown() {
        let base = fact("openai", "gpt-5.5", 55.0);
        let mut fast = fact("openai", "gpt-5.5-fast", 55.0);
        fast.context_window = None;
        let facts = vec![base, fast];
        let sel = selected("openai", "gpt-5.5");
        let snap = relaxed_snap("openai/gpt-5.5-fast");
        let out = maybe_upgrade_to_fast_variant(
            &FastVariantUpgradeInput {
                selected: &sel,
                facts: &facts,
                snapshot: &snap,
                requirements: &reqs(),
                required_capabilities: Some(RequiredCapabilities {
                    vision: None,
                    min_context: Some(1.0),
                }),
                available_models: &avail(&["openai/gpt-5.5", "openai/gpt-5.5-fast"]),
                prefer_fast_specs: &["openai/gpt-5.5".into()],
                exclude_route_keys: &[],
                is_subscription_route: None,
                now: 1_000.0,
                quota_signal_fresh: true,
            },
            &DEFAULT_ROUTING_CONFIG,
        );

        assert!(out.is_none());
    }

    #[test]
    fn does_not_upgrade_when_fast_sibling_pressure_exceeds_threshold() {
        let facts = vec![
            fact("openai", "gpt-5.5", 55.0),
            fact("openai", "gpt-5.5-fast", 55.0),
        ];
        let sel = selected("openai", "gpt-5.5");
        // Fast sibling's capacity carries high quota pressure → not relaxed.
        let mut by_capacity = HashMap::new();
        by_capacity.insert(
            "openai/gpt-5.5-fast".to_string(),
            RouteRuntimeState {
                quota_pressure: Some(0.9),
                ..Default::default()
            },
        );
        let snap = ModelRouteStateSnapshot {
            now: 1000.0,
            by_route: HashMap::new(),
            by_capacity,
        };
        let out = maybe_upgrade_to_fast_variant(
            &FastVariantUpgradeInput {
                selected: &sel,
                facts: &facts,
                snapshot: &snap,
                requirements: &reqs(),
                required_capabilities: None,
                available_models: &avail(&["openai/gpt-5.5", "openai/gpt-5.5-fast"]),
                prefer_fast_specs: &["openai/gpt-5.5".into()],
                exclude_route_keys: &[],
                is_subscription_route: None,
                now: 1000.0,
                quota_signal_fresh: true,
            },
            &DEFAULT_ROUTING_CONFIG,
        );
        assert!(out.is_none(), "high pressure must block the upgrade");
    }

    #[test]
    fn does_not_upgrade_when_fast_sibling_is_not_available() {
        let facts = vec![
            fact("openai", "gpt-5.5", 55.0),
            fact("openai", "gpt-5.5-fast", 55.0),
        ];
        let sel = selected("openai", "gpt-5.5");
        let snap = ModelRouteStateSnapshot {
            now: 1000.0,
            by_route: HashMap::new(),
            by_capacity: HashMap::new(),
        };
        let out = maybe_upgrade_to_fast_variant(
            &FastVariantUpgradeInput {
                selected: &sel,
                facts: &facts,
                snapshot: &snap,
                requirements: &reqs(),
                required_capabilities: None,
                // fast sibling NOT entitled.
                available_models: &avail(&["openai/gpt-5.5"]),
                prefer_fast_specs: &["openai/gpt-5.5".into()],
                exclude_route_keys: &[],
                is_subscription_route: None,
                now: 1000.0,
                quota_signal_fresh: true,
            },
            &DEFAULT_ROUTING_CONFIG,
        );
        assert!(out.is_none());
    }
}
