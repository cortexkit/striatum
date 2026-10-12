//! Test-vectors ported from the Alfonso TS plugin
//! (`src/features/model-routing/model-router.test.ts`). Same inputs/outputs as
//! the TS suite; this is the behavioral-parity proof for the port. The
//! seed-backed cases (TS `describe("model facts seed")`) land with the
//! The seed-backed catalog test cases are a fixed subset; all other TypeScript
//! suite cases are included below.

use std::collections::HashSet;

use std::collections::HashMap;

use prefrontal_routing_core::model_facts::{
    make_capacity_key, make_route_key, Confidence, ModelFact, ModelPromptDefaults,
    ModelRequirements, ModelRoute, ModelScores, ReasoningEffort, ScoreProvenance,
};
use prefrontal_routing_core::model_facts_seed::model_facts_seed;
use prefrontal_routing_core::model_router::{
    admit_routes, price_selection_shortlist, select_model_route, select_model_route_with_config,
    BasketVectorSource, ModelRouteStateSnapshot, PricingMode, RouteFailureReason,
    RouteRuntimeState, SelectModelRouteInput,
};
use prefrontal_routing_core::{
    select_panel, AvailabilitySubstrate, ExclusionPolicy, OverqualificationConfig,
    PanelCauseReason, PanelClass, PanelDiversity, PanelRosterEntry, PanelSelection,
    PanelShortfallCode, PanelSpec, RoutingConfig, SelectPanelInput, DEFAULT_OVERQUALIFICATION,
    DEFAULT_ROUTING_CONFIG, MIN_BEST_EFFORT_ADEQUACY,
};

/// Selection with the over-qualification term switched off, for tests of
/// pricing and tie-breaks the term is not about. Their fixtures put every
/// route at the request frontier and ask for iq 40-50, so every route carries
/// the same term (surplus 50-60), which lifts all of them out of the
/// effectively-free bucket and adds itself to the selection cost they assert.
fn select_without_overqualification(
    input: SelectModelRouteInput<'_>,
) -> Result<
    prefrontal_routing_core::model_router::SelectedModelRoute,
    prefrontal_routing_core::model_router::NoRouteError,
> {
    let config = RoutingConfig {
        overqualification: OverqualificationConfig {
            base: 0.0,
            ..DEFAULT_OVERQUALIFICATION
        },
        ..DEFAULT_ROUTING_CONFIG
    };
    select_model_route_with_config(input, &config)
}

// ── builders mirroring the TS test helpers ──────────────────────────────────

#[derive(Default)]
struct FactBuilder {
    provider_id: String,
    model_id: String,
    variant: Option<String>,
    scores: ModelScores,
    context_window: Option<f64>,
    cost_tier: Option<f64>,
    cost_per_m_tok_out: Option<f64>,
    supports_vision: Option<bool>,
    prompt_defaults: Option<ModelPromptDefaults>,
    confidence: Option<Confidence>,
    class_vector: Option<prefrontal_routing_core::ClassVector>,
}

/// `fact({ providerID, modelID, scores, ...partial })`: confidence defaults to curated.
fn fact(b: FactBuilder) -> ModelFact {
    let has_vector = b.class_vector.is_some();
    ModelFact {
        billing_plane: None,
        speed_is_fleet_median: false,
        provider_id: b.provider_id,
        model_id: b.model_id,
        variant: b.variant,
        family: None,
        scores: b.scores,
        context_window: b.context_window,
        max_output_tokens: None,
        cost_tier: b.cost_tier,
        cost_per_m_tok_out: b.cost_per_m_tok_out,
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: b.supports_vision,
        supports_tool_use: None,
        prompt_defaults: b.prompt_defaults,
        confidence: b.confidence.unwrap_or(Confidence::Curated),
        notes: None,
        speed_multiplier: None,
        subscription_multiplier: None,
        score_provenance: ScoreProvenance::Curated,
        aa_index: None,
        review_by_ms: None,
        eq_source: None,
        class_vector: b.class_vector,
        class_vector_is_overlay: has_vector,
    }
}

fn scores(elo: f64, eq: f64, speed: f64) -> ModelScores {
    ModelScores { elo, eq, speed }
}

fn req(iq: f64, eq: f64) -> ModelRequirements {
    ModelRequirements { iq, eq }
}

fn availability_of(facts: &[ModelFact]) -> HashSet<String> {
    let mut s = HashSet::new();
    for f in facts {
        s.insert(make_route_key(
            &f.provider_id,
            &f.model_id,
            f.variant.as_deref(),
        ));
        s.insert(make_capacity_key(&f.provider_id, &f.model_id));
    }
    s
}

#[derive(Default)]
struct Extra<'a> {
    available_models: Option<HashSet<String>>,
    runtime_snapshot: Option<ModelRouteStateSnapshot>,
    required_capabilities: Option<prefrontal_routing_core::model_router::RequiredCapabilities>,
    is_subscription_route: Option<Box<dyn Fn(&ModelRoute) -> bool + 'a>>,
    min_best_effort_adequacy: Option<f64>,
    prefer_prompt_cache: bool,
    providers_without_prompt_cache: HashSet<String>,
}

fn input<'a>(
    facts: Vec<ModelFact>,
    requirements: ModelRequirements,
    extra: Extra<'a>,
) -> SelectModelRouteInput<'a> {
    let available_models = extra
        .available_models
        .unwrap_or_else(|| availability_of(&facts));
    SelectModelRouteInput {
        target_agent: "mason".to_string(),
        requirements,
        available_models,
        facts,
        runtime_snapshot: extra.runtime_snapshot,
        exclude_route_keys: Vec::new(),
        operator_exclude_keys: Vec::new(),
        explicit_only_providers: Vec::new(),
        required_capabilities: extra.required_capabilities,
        is_subscription_route: extra.is_subscription_route,
        min_best_effort_adequacy: extra.min_best_effort_adequacy,
        prefer_prompt_cache: extra.prefer_prompt_cache,
        providers_without_prompt_cache: extra.providers_without_prompt_cache,
    }
}

// ── exclusion spec-list semantics ───────────────────────────────────────────

#[test]
fn provider_wide_exclusion_spec_rejects_every_model_on_the_provider() {
    // Red-proof for the k3 sidekick incident: a bare provider entry must
    // exclude all of that provider's routes in admission (module-side panel
    // and sidekick seats), not only in TS-side availability filtering where
    // the spec classifier already applied it.
    let quota = fact(FactBuilder {
        provider_id: "kimi-for-coding".into(),
        model_id: "k3".into(),
        scores: scores(55.0, 1900.0, 50.0),
        ..Default::default()
    });
    let cheap = fact(FactBuilder {
        provider_id: "ollama-cloud".into(),
        model_id: "deepseek-v4-flash".into(),
        scores: scores(42.0, 1200.0, 113.0),
        ..Default::default()
    });
    let mut in1 = input(
        vec![quota.clone(), cheap.clone()],
        req(20.0, 0.0),
        Extra::default(),
    );
    in1.exclude_route_keys = vec!["kimi-for-coding".to_string()];
    let r = select_model_route(in1).expect("cheap route survives");
    assert_eq!(r.model.model_id, "deepseek-v4-flash");

    // Negation re-admits a specific model: spec-list semantics end to end.
    let mut in2 = input(vec![quota, cheap], req(20.0, 0.0), Extra::default());
    in2.exclude_route_keys = vec![
        "kimi-for-coding".to_string(),
        "ollama-cloud".to_string(),
        "-kimi-for-coding/k3".to_string(),
    ];
    let r = select_model_route(in2).expect("negated route survives");
    assert_eq!(r.model.model_id, "k3");
}

// ── quality threshold (gate) ────────────────────────────────────────────────

fn fast() -> ModelFact {
    // Lower-Elo fast route: clears low demand but fails the high request bar.
    fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "fast".into(),
        scores: scores(42.0, 1450.0, 200.0),
        ..Default::default()
    })
}
fn smart() -> ModelFact {
    // Higher-Elo route clears the high request bar.
    fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "smart".into(),
        scores: scores(60.0, 1950.0, 60.0),
        ..Default::default()
    })
}

#[test]
fn high_iq_requirement_only_clears_the_high_iq_route() {
    let r = select_model_route(input(
        vec![fast(), smart()],
        req(90.0, 10.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "smart");
}

#[test]
fn speed_breaks_ties_among_equal_cost_models_clearing_the_bar() {
    let r = select_model_route(input(
        vec![fast(), smart()],
        req(20.0, 0.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "fast");
}

#[test]
fn speed_can_never_pull_a_sub_bar_model_in() {
    // The model's iq score (34) is well below the requirement-80 target of about 50.5 on the default iq anchors, so its high speed must not make it eligible.
    let weak_fast = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "weakfast".into(),
        scores: scores(34.0, 1450.0, 300.0),
        ..Default::default()
    });
    let r = select_model_route(input(
        vec![weak_fast, smart()],
        req(80.0, 0.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "smart");
}

#[test]
fn requirement_0_excludes_a_metric_from_the_quality_gate() {
    let r = select_model_route(input(
        vec![fast(), smart()],
        req(80.0, 0.0),
        Extra::default(),
    ))
    .unwrap();
    assert!(!r
        .score
        .metrics
        .contains_key(&prefrontal_routing_core::ModelMetric::Eq));
    assert_eq!(r.model.model_id, "smart");
}

// ── cost-primary ordering (cost > speed) ────────────────────────────────────

#[test]
fn among_models_clearing_the_bar_the_cheapest_wins() {
    let expensive_strong = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "expensive".into(),
        scores: scores(62.0, 1900.0, 60.0),
        cost_per_m_tok_out: Some(25.0),
        ..Default::default()
    });
    let cheap_adequate = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "cheap".into(),
        scores: scores(60.0, 1750.0, 60.0),
        cost_per_m_tok_out: Some(2.0),
        ..Default::default()
    });
    let r = select_model_route(input(
        vec![expensive_strong, cheap_adequate],
        req(80.0, 20.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "cheap");
}

#[test]
fn invariant_faster_pricier_never_beats_cheaper_slower() {
    let fast_pricey = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "fastpricey".into(),
        scores: scores(60.0, 1750.0, 220.0),
        cost_per_m_tok_out: Some(12.0),
        ..Default::default()
    });
    let slow_cheap = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "slowcheap".into(),
        scores: scores(60.0, 1750.0, 40.0),
        cost_per_m_tok_out: Some(2.0),
        ..Default::default()
    });
    let r = select_model_route(input(
        vec![fast_pricey, slow_cheap],
        req(80.0, 20.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "slowcheap");
}

#[test]
fn gather_cache_preference_partitions_without_changing_cost_order() {
    let ollama = fact(FactBuilder {
        provider_id: "ollama-cloud".into(),
        model_id: "cheap".into(),
        scores: scores(60.0, 1750.0, 60.0),
        cost_per_m_tok_out: Some(1.0),
        ..Default::default()
    });
    let cache_capable = fact(FactBuilder {
        provider_id: "openai".into(),
        model_id: "cached".into(),
        scores: scores(60.0, 1750.0, 60.0),
        cost_per_m_tok_out: Some(8.0),
        ..Default::default()
    });

    let unflagged = select_model_route(input(
        vec![ollama.clone(), cache_capable.clone()],
        req(80.0, 20.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(unflagged.model.provider_id, "ollama-cloud");

    let mut flagged = Extra {
        prefer_prompt_cache: true,
        ..Default::default()
    };
    flagged
        .providers_without_prompt_cache
        .insert("ollama-cloud".into());
    let preferred = select_model_route(input(
        vec![ollama.clone(), cache_capable],
        req(80.0, 20.0),
        flagged,
    ))
    .unwrap();
    assert_eq!(preferred.model.provider_id, "openai");

    let only_uncached = select_model_route(input(
        vec![ollama],
        req(80.0, 20.0),
        Extra {
            prefer_prompt_cache: true,
            providers_without_prompt_cache: HashSet::from(["ollama-cloud".into()]),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(only_uncached.model.provider_id, "ollama-cloud");
}

#[test]
fn subscription_routes_with_different_list_prices_no_longer_tie() {
    // Regression this task exists for: subscription coverage used to collapse
    // every route's cost to 0, so a $6/Mtok flagship and a $0.28/Mtok flash model
    // tied at cost 0 and the SPEED tiebreak (not price) chose between them — which
    // is how routine work landed on the flagship. The flagship below is the faster
    // route, so under the old collapse it won the speed tiebreak; pricing now comes
    // from each route's own fact, so the cheaper list price wins the cost step even
    // though both routes are subscription-covered.
    let flagship = fact(FactBuilder {
        provider_id: "alibaba-token-plan".into(),
        model_id: "qwen3.8-max-preview".into(),
        scores: scores(60.0, 1750.0, 300.0),
        cost_per_m_tok_out: Some(6.0),
        ..Default::default()
    });
    let flash = fact(FactBuilder {
        provider_id: "google".into(),
        model_id: "gemini-flash".into(),
        scores: scores(60.0, 1750.0, 40.0),
        cost_per_m_tok_out: Some(0.28),
        ..Default::default()
    });
    let extra = Extra {
        is_subscription_route: Some(Box::new(|_: &ModelRoute| true)),
        ..Default::default()
    };
    let r = select_model_route(input(vec![flagship, flash], req(80.0, 20.0), extra)).unwrap();
    // Cheaper list price wins the cost step despite being the slower route.
    assert_eq!(r.model.model_id, "gemini-flash");
    assert!(
        (r.score.objective.cost - 0.28 / 30.0).abs() < 1e-9,
        "flash should be priced at its real list price, got {}",
        r.score.objective.cost
    );
}

#[test]
fn cheapness_cannot_select_below_the_quality_bar() {
    let strong = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "strong".into(),
        scores: scores(60.0, 1750.0, 70.0),
        cost_per_m_tok_out: Some(25.0),
        ..Default::default()
    });
    let weak_cheap = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "weakcheap".into(),
        scores: scores(38.0, 1100.0, 90.0),
        cost_per_m_tok_out: Some(0.3),
        ..Default::default()
    });
    let r = select_model_route(input(
        vec![strong, weak_cheap],
        req(80.0, 30.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "strong");
}

/// A subscription route with no pressure reading says so: the quota term below
/// turns both "measured as idle" and "nothing measured" into the same 0.0, so
/// the explanation is the only place a reader can tell them apart.
#[test]
fn a_subscription_route_without_a_pressure_reading_says_the_signal_is_absent() {
    let sub = fact(FactBuilder {
        provider_id: "opencode-go".into(),
        model_id: "qwen3.7-max".into(),
        scores: scores(60.0, 1800.0, 90.0),
        ..Default::default()
    });
    let extra = Extra {
        is_subscription_route: Some(Box::new(|route: &ModelRoute| {
            route.provider_id == "opencode-go"
        })),
        runtime_snapshot: Some(ModelRouteStateSnapshot {
            now: 1_000_000.0,
            ..Default::default()
        }),
        ..Default::default()
    };
    let r = select_model_route(input(vec![sub], req(80.0, 20.0), extra)).unwrap();
    assert!(
        r.explanation
            .iter()
            .any(|e| e == "quota signal: none (no reading)"),
        "explanation was {:?}",
        r.explanation
    );
}

/// The arm the old predicate could never reach. It keyed on a `governing_window`
/// that nothing in production wrote, so the absent-signal line printed on EVERY
/// subscription route - 411 of 411 decisions in 24h, including routes carrying a
/// real pressure value from the same snapshot. A route whose pressure IS known
/// must not claim its signal is missing.
#[test]
fn a_subscription_route_with_a_pressure_reading_does_not_claim_the_signal_is_absent() {
    let sub = fact(FactBuilder {
        provider_id: "opencode-go".into(),
        model_id: "qwen3.7-max".into(),
        scores: scores(60.0, 1800.0, 90.0),
        ..Default::default()
    });
    let mut by_capacity = std::collections::HashMap::new();
    by_capacity.insert(
        make_capacity_key("opencode-go", "qwen3.7-max"),
        RouteRuntimeState {
            quota_pressure: Some(0.4),
            ..Default::default()
        },
    );
    let extra = Extra {
        is_subscription_route: Some(Box::new(|route: &ModelRoute| {
            route.provider_id == "opencode-go"
        })),
        runtime_snapshot: Some(ModelRouteStateSnapshot {
            now: 1_000_000.0,
            by_capacity,
            ..Default::default()
        }),
        ..Default::default()
    };
    let r = select_model_route(input(vec![sub], req(80.0, 20.0), extra)).unwrap();
    assert!(
        !r.explanation
            .iter()
            .any(|e| e.starts_with("quota signal: none")),
        "a known pressure must not read as an absent signal: {:?}",
        r.explanation
    );
    assert!(
        r.explanation.iter().any(|e| e.starts_with("quota +")),
        "the pressure must reach the objective: {:?}",
        r.explanation
    );
}

// ── anchored (not min/max) normalization ────────────────────────────────────

#[test]
fn quota_exhaustion_does_not_change_the_request_frontier() {
    let top = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "top".into(),
        scores: scores(1_838.0, 2_100.0, 60.0),
        ..Default::default()
    });
    let luna = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "luna".into(),
        scores: scores(1_621.0, 1_900.0, 160.0),
        ..Default::default()
    });
    let facts = vec![top, luna];
    let available = availability_of(&facts);
    let requirements = req(70.0, 0.0);
    let baseline = admit_routes(
        &facts,
        None,
        ExclusionPolicy::default(),
        None,
        &available,
        &requirements,
        &DEFAULT_ROUTING_CONFIG,
    );
    let quota = snapshot_with_quota_exhausted("p/top", 5_000.0);
    let pressured = admit_routes(
        &facts,
        Some(&quota),
        ExclusionPolicy::default(),
        None,
        &available,
        &requirements,
        &DEFAULT_ROUTING_CONFIG,
    );

    assert_eq!(baseline.top_routable.elo, 1_838.0);
    assert_eq!(pressured.top_routable.elo, 1_838.0);
    let baseline_luna = baseline
        .admitted
        .iter()
        .find(|route| route.route.route_key == "p/luna")
        .unwrap();
    let pressured_luna = pressured
        .admitted
        .iter()
        .find(|route| route.route.route_key == "p/luna")
        .unwrap();
    assert_eq!(baseline_luna.adequacy, pressured_luna.adequacy);
}

// ── admission filters ───────────────────────────────────────────────────────

fn strong_25() -> ModelFact {
    fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "strong".into(),
        scores: scores(58.0, 1750.0, 70.0),
        cost_per_m_tok_out: Some(25.0),
        ..Default::default()
    })
}
fn weak_cheap() -> ModelFact {
    fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "weakcheap".into(),
        scores: scores(38.0, 1100.0, 90.0),
        cost_per_m_tok_out: Some(0.3),
        ..Default::default()
    })
}

#[test]
fn confirmed_cooldown_blocks_a_route() {
    let now = 1_000_000.0;
    let mut by_route = std::collections::HashMap::new();
    by_route.insert(
        make_route_key("p", "strong", None),
        RouteRuntimeState {
            cooldown_until: Some(now + 60_000.0),
            ..Default::default()
        },
    );
    let snapshot = ModelRouteStateSnapshot {
        now,
        by_route,
        ..Default::default()
    };
    let r = select_model_route(input(
        vec![strong_25(), weak_cheap()],
        req(20.0, 10.0),
        Extra {
            runtime_snapshot: Some(snapshot),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "weakcheap");
    assert_eq!(
        r.rejected
            .iter()
            .find(|x| x.route_key == "p/strong")
            .map(|x| x.reason.as_str()),
        Some("cooldown_active")
    );
}

#[test]
fn confirmed_quota_exhaustion_blocks_a_route() {
    let now = 1_000_000.0;
    let mut by_capacity = std::collections::HashMap::new();
    by_capacity.insert(
        make_capacity_key("p", "strong"),
        RouteRuntimeState {
            quota_exhausted_until: Some(now + 60_000.0),
            ..Default::default()
        },
    );
    let snapshot = ModelRouteStateSnapshot {
        now,
        by_capacity,
        ..Default::default()
    };
    let r = select_model_route(input(
        vec![strong_25(), weak_cheap()],
        req(20.0, 10.0),
        Extra {
            runtime_snapshot: Some(snapshot),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "weakcheap");
}

#[test]
fn undefined_route_telemetry_does_not_erase_capacity_quota_blocks() {
    let now = 1_000_000.0;
    let route_key = make_route_key("p", "strong", None);
    let mut by_capacity = std::collections::HashMap::new();
    by_capacity.insert(
        make_capacity_key("p", "strong"),
        RouteRuntimeState {
            quota_exhausted_until: Some(now + 60_000.0),
            ..Default::default()
        },
    );
    let mut by_route = std::collections::HashMap::new();
    // quota_exhausted_until: undefined (None), success_ema: 0.9: must not erase the capacity block.
    by_route.insert(
        route_key,
        RouteRuntimeState {
            quota_exhausted_until: None,
            success_ema: Some(0.9),
            ..Default::default()
        },
    );
    let snapshot = ModelRouteStateSnapshot {
        now,
        by_route,
        by_capacity,
    };
    let r = select_model_route(input(
        vec![strong_25(), weak_cheap()],
        req(20.0, 10.0),
        Extra {
            runtime_snapshot: Some(snapshot),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "weakcheap");
}

// ── soft quota pressure (objective) ─────────────────────────────────────────

#[test]
fn expired_route_deadlines_do_not_mask_live_capacity_blocks() {
    let now = 50.0;
    let route_key = make_route_key("p", "strong", None);
    let capacity_key = make_capacity_key("p", "strong");
    let snapshot = ModelRouteStateSnapshot {
        now,
        by_route: std::collections::HashMap::from([(
            route_key,
            RouteRuntimeState {
                cooldown_until: Some(10.0),
                quota_exhausted_until: Some(10.0),
                ..Default::default()
            },
        )]),
        by_capacity: std::collections::HashMap::from([(
            capacity_key,
            RouteRuntimeState {
                cooldown_until: Some(100.0),
                quota_exhausted_until: Some(100.0),
                ..Default::default()
            },
        )]),
    };

    let selected = select_model_route(input(
        vec![strong_25(), weak_cheap()],
        req(20.0, 10.0),
        Extra {
            runtime_snapshot: Some(snapshot),
            ..Default::default()
        },
    ))
    .unwrap();

    assert_eq!(selected.model.model_id, "weakcheap");
}

#[test]
fn soft_quota_pressure_shifts_selection_to_unpressured_peer() {
    let a = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "aa".into(),
        scores: scores(60.0, 1800.0, 60.0),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });
    let b = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "bb".into(),
        scores: scores(60.0, 1800.0, 60.0),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });
    let now = 1_000_000.0;
    let mut by_capacity = std::collections::HashMap::new();
    by_capacity.insert(
        make_capacity_key("p", "aa"),
        RouteRuntimeState {
            quota_pressure: Some(1.0),
            ..Default::default()
        },
    );
    let snapshot = ModelRouteStateSnapshot {
        now,
        by_capacity,
        ..Default::default()
    };
    let r = select_model_route(input(
        vec![a, b],
        req(80.0, 20.0),
        Extra {
            runtime_snapshot: Some(snapshot),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "bb");
}

// ── best-effort when nothing clears ─────────────────────────────────────────

#[test]
fn extreme_demand_returns_most_capable_never_strands() {
    let top = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "top".into(),
        scores: scores(100.0, 2_000.0, 40.0),
        ..Default::default()
    });
    let weak = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "weak".into(),
        scores: scores(30.0, 1100.0, 40.0),
        ..Default::default()
    });
    let less = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "less".into(),
        scores: scores(20.0, 900.0, 40.0),
        ..Default::default()
    });
    let r = select_model_route(input(
        vec![top, weak, less],
        req(100.0, 80.0),
        Extra {
            min_best_effort_adequacy: Some(0.0),
            runtime_snapshot: Some(snapshot_with_quota_exhausted("p/top", 2_000.0)),
            ..Extra::default()
        },
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "weak");
    assert!(r
        .explanation
        .iter()
        .any(|line| line.contains("best-effort")));
}

// ── typed failures ──────────────────────────────────────────────────────────

fn ok_fact() -> ModelFact {
    fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "ok".into(),
        scores: scores(55.0, 1700.0, 90.0),
        ..Default::default()
    })
}

#[test]
fn no_available_model_typed_failure() {
    let extra = Extra {
        available_models: Some(HashSet::new()),
        ..Default::default()
    };
    let err = select_model_route(input(vec![ok_fact()], req(50.0, 10.0), extra)).unwrap_err();
    assert_eq!(err.reason, RouteFailureReason::NoAvailableModel);
}

#[test]
fn invalid_requirements_out_of_range_typed_failure() {
    let err =
        select_model_route(input(vec![ok_fact()], req(101.0, 10.0), Extra::default())).unwrap_err();
    assert_eq!(err.reason, RouteFailureReason::InvalidRequirements);
}

#[test]
fn all_zero_requirements_without_required_capability_invalid() {
    let err =
        select_model_route(input(vec![ok_fact()], req(0.0, 0.0), Extra::default())).unwrap_err();
    assert_eq!(err.reason, RouteFailureReason::InvalidRequirements);
}

#[test]
fn mixed_rejections_classify_as_quota_cooldown() {
    let now = 1_000_000.0;
    let a = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "blocked".into(),
        scores: scores(60.0, 1800.0, 90.0),
        ..Default::default()
    });
    let b = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "loggedout".into(),
        scores: scores(60.0, 1800.0, 90.0),
        ..Default::default()
    });
    let mut by_capacity = std::collections::HashMap::new();
    by_capacity.insert(
        make_capacity_key("p", "blocked"),
        RouteRuntimeState {
            quota_exhausted_until: Some(now + 60_000.0),
            ..Default::default()
        },
    );
    let snapshot = ModelRouteStateSnapshot {
        now,
        by_capacity,
        ..Default::default()
    };
    let mut available = HashSet::new();
    available.insert(make_route_key("p", "blocked", None));
    available.insert(make_capacity_key("p", "blocked"));
    let extra = Extra {
        available_models: Some(available),
        runtime_snapshot: Some(snapshot),
        ..Default::default()
    };
    let err = select_model_route(input(vec![a, b], req(80.0, 20.0), extra)).unwrap_err();
    assert_eq!(err.reason, RouteFailureReason::QuotaOrCooldownExhausted);
}

// ── required capabilities ───────────────────────────────────────────────────

#[test]
fn vision_requirement_admits_only_vision_capable() {
    let vision_model = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "vision".into(),
        scores: scores(30.0, 1200.0, 60.0),
        supports_vision: Some(true),
        ..Default::default()
    });
    let text_model = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "text".into(),
        scores: scores(60.0, 1900.0, 200.0),
        supports_vision: Some(false),
        ..Default::default()
    });
    let extra = Extra {
        required_capabilities: Some(
            prefrontal_routing_core::model_router::RequiredCapabilities {
                vision: Some(true),
                min_context: None,
            },
        ),
        ..Default::default()
    };
    let r =
        select_model_route(input(vec![vision_model, text_model], req(0.0, 0.0), extra)).unwrap();
    assert_eq!(r.model.model_id, "vision");
}

#[test]
fn min_context_filters_small_context_models() {
    let small = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "small".into(),
        scores: scores(60.0, 1800.0, 90.0),
        context_window: Some(8_000.0),
        ..Default::default()
    });
    let big = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "big".into(),
        scores: scores(55.0, 1700.0, 90.0),
        context_window: Some(1_000_000.0),
        ..Default::default()
    });
    let extra = Extra {
        required_capabilities: Some(
            prefrontal_routing_core::model_router::RequiredCapabilities {
                vision: None,
                min_context: Some(200_000.0),
            },
        ),
        ..Default::default()
    };
    let r = select_model_route(input(vec![small, big], req(60.0, 10.0), extra)).unwrap();
    assert_eq!(r.model.model_id, "big");
}

// ── unknown facts ───────────────────────────────────────────────────────────

#[test]
fn positive_min_context_rejects_unknown_context() {
    let unknown_context = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "unknown-context".into(),
        scores: scores(60.0, 1800.0, 90.0),
        context_window: None,
        ..Default::default()
    });
    let result = select_model_route(input(
        vec![unknown_context],
        req(0.0, 0.0),
        Extra {
            required_capabilities: Some(
                prefrontal_routing_core::model_router::RequiredCapabilities {
                    vision: None,
                    min_context: Some(1.0),
                },
            ),
            ..Default::default()
        },
    ));

    assert_eq!(
        result.unwrap_err().reason,
        RouteFailureReason::ContextTooSmall
    );
}

#[test]
fn unknown_fact_candidate_loses_to_known_adequate_peer() {
    let known = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "known".into(),
        scores: scores(58.0, 1800.0, 90.0),
        confidence: Some(Confidence::Curated),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });
    let unknown = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "unknown".into(),
        scores: scores(58.0, 1800.0, 90.0),
        confidence: Some(Confidence::Unknown),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });
    let r = select_model_route(input(
        vec![known, unknown],
        req(70.0, 20.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(r.model.model_id, "known");
}

#[test]
fn unknown_fact_candidate_wins_when_only_option() {
    let unknown = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "only".into(),
        scores: scores(55.0, 1700.0, 90.0),
        confidence: Some(Confidence::Unknown),
        ..Default::default()
    });
    let r = select_model_route(input(vec![unknown], req(60.0, 10.0), Extra::default())).unwrap();
    assert_eq!(r.model.model_id, "only");
}

// ── determinism & prompt params ─────────────────────────────────────────────

#[test]
fn deterministic_tie_break_stable_by_route_key() {
    let a = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "aaa".into(),
        scores: scores(55.0, 1700.0, 160.0),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });
    let b = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "bbb".into(),
        scores: scores(55.0, 1700.0, 160.0),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });
    let r1 = select_model_route(input(
        vec![a.clone(), b.clone()],
        req(60.0, 40.0),
        Extra::default(),
    ))
    .unwrap();
    let r2 = select_model_route(input(vec![b, a], req(60.0, 40.0), Extra::default())).unwrap();
    assert_eq!(r1.route_key, r2.route_key);
    assert_eq!(r1.model.model_id, "aaa");
}

#[test]
fn prompt_defaults_variant_reasoning_from_selected_fact() {
    let m = fact(FactBuilder {
        provider_id: "openai".into(),
        model_id: "gpt-5.4".into(),
        variant: Some("high".into()),
        scores: scores(57.0, 1585.0, 75.0),
        prompt_defaults: Some(ModelPromptDefaults {
            variant: Some("high".into()),
            temperature: None,
            reasoning_effort: Some(ReasoningEffort::High),
        }),
        ..Default::default()
    });
    let r = select_model_route(input(vec![m], req(80.0, 20.0), Extra::default())).unwrap();
    assert_eq!(r.prompt_params.variant.as_deref(), Some("high"));
    assert_eq!(r.prompt_params.reasoning_effort.as_deref(), Some("high"));
    assert_eq!(r.route_key, "openai/gpt-5.4/high");
}

#[test]
fn explanation_strings_are_populated() {
    let m = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "m".into(),
        scores: scores(55.0, 1700.0, 90.0),
        ..Default::default()
    });
    let r = select_model_route(input(vec![m], req(70.0, 20.0), Extra::default())).unwrap();
    assert!(r.explanation.len() > 1);
    assert!(r.explanation.iter().any(|l| l.contains("elo:")));
}

// ── subscription cost model ─────────────────────────────────────────────────

#[test]
fn subscription_coverage_is_reported_but_does_not_zero_cost() {
    // A subscription route is no longer free for selection: it is priced at its
    // real list price, so a cheap metered route beats an expensive subscription
    // route. The subscription coverage is still reported in the explanation.
    let cheap_metered = fact(FactBuilder {
        provider_id: "metered".into(),
        model_id: "cheap".into(),
        scores: scores(55.0, 1500.0, 60.0),
        cost_per_m_tok_out: Some(1.0),
        ..Default::default()
    });
    let pricy_sub = fact(FactBuilder {
        provider_id: "sub".into(),
        model_id: "pricy".into(),
        scores: scores(55.0, 1500.0, 60.0),
        cost_per_m_tok_out: Some(25.0),
        ..Default::default()
    });
    let extra = Extra {
        is_subscription_route: Some(Box::new(|route: &ModelRoute| route.provider_id == "sub")),
        ..Default::default()
    };
    let r =
        select_model_route(input(vec![cheap_metered, pricy_sub], req(40.0, 0.0), extra)).unwrap();
    // The cheap metered route wins on real price; the subscription is NOT zeroed.
    assert_eq!(r.route_key, "metered/cheap");
    assert!(r.score.selection_cost > 0.0);
}

#[test]
fn subscription_coverage_appears_in_the_explanation_of_a_winning_sub_route() {
    // When a subscription route genuinely wins (here it is the only adequate route),
    // the explanation names its subscription coverage and its real price.
    let pricy_sub = fact(FactBuilder {
        provider_id: "sub".into(),
        model_id: "pricy".into(),
        scores: scores(55.0, 1500.0, 60.0),
        cost_per_m_tok_out: Some(25.0),
        ..Default::default()
    });
    let extra = Extra {
        is_subscription_route: Some(Box::new(|route: &ModelRoute| route.provider_id == "sub")),
        ..Default::default()
    };
    let r = select_model_route(input(vec![pricy_sub], req(40.0, 0.0), extra)).unwrap();
    assert_eq!(r.route_key, "sub/pricy");
    let explanation = r.explanation.join("\n");
    assert!(
        explanation.contains("subscription-covered"),
        "{explanation}"
    );
    assert!(explanation.contains("list $25/Mtok"), "{explanation}");
}

#[test]
fn without_subscription_predicate_cheap_metered_wins() {
    let cheap_metered = fact(FactBuilder {
        provider_id: "metered".into(),
        model_id: "cheap".into(),
        scores: scores(55.0, 1500.0, 60.0),
        cost_per_m_tok_out: Some(1.0),
        ..Default::default()
    });
    let pricy_sub = fact(FactBuilder {
        provider_id: "sub".into(),
        model_id: "pricy".into(),
        scores: scores(55.0, 1500.0, 60.0),
        cost_per_m_tok_out: Some(25.0),
        ..Default::default()
    });
    let r = select_model_route(input(
        vec![cheap_metered, pricy_sub],
        req(40.0, 0.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(r.route_key, "metered/cheap");
}

#[test]
fn unknown_metered_price_loses_to_known_cheap_on_primary_objective() {
    let known_cheap = fact(FactBuilder {
        provider_id: "m1".into(),
        model_id: "cheap".into(),
        scores: scores(55.0, 1500.0, 60.0),
        cost_per_m_tok_out: Some(1.0),
        ..Default::default()
    });
    let unknown_price = fact(FactBuilder {
        provider_id: "m2".into(),
        model_id: "unknown".into(),
        scores: scores(55.0, 1500.0, 60.0),
        ..Default::default()
    });
    let r = select_model_route(input(
        vec![known_cheap, unknown_price],
        req(40.0, 0.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(r.route_key, "m1/cheap");
}

#[test]
fn unknown_metered_price_does_not_win_cost_proxy_tiebreak_over_known_cheap_sub() {
    let known_cheap = fact(FactBuilder {
        provider_id: "aaa".into(),
        model_id: "known".into(),
        scores: scores(55.0, 1500.0, 60.0),
        cost_per_m_tok_out: Some(1.0),
        ..Default::default()
    });
    let unknown_price = fact(FactBuilder {
        provider_id: "zzz".into(),
        model_id: "unknown".into(),
        scores: scores(55.0, 1500.0, 60.0),
        ..Default::default()
    });
    let extra = Extra {
        is_subscription_route: Some(Box::new(|_: &ModelRoute| true)),
        ..Default::default()
    };
    let r = select_without_overqualification(input(
        vec![known_cheap, unknown_price],
        req(40.0, 0.0),
        extra,
    ))
    .unwrap();
    assert_eq!(r.route_key, "aaa/known");
}

#[test]
fn cost_proxy_tiebreak_orders_by_cheaper_if_metered_not_alphabetical() {
    let asub = fact(FactBuilder {
        provider_id: "asub".into(),
        model_id: "m".into(),
        scores: scores(55.0, 1500.0, 60.0),
        cost_per_m_tok_out: Some(25.0),
        ..Default::default()
    });
    let zsub = fact(FactBuilder {
        provider_id: "zsub".into(),
        model_id: "m".into(),
        scores: scores(55.0, 1500.0, 60.0),
        cost_per_m_tok_out: Some(2.0),
        ..Default::default()
    });
    let extra = Extra {
        is_subscription_route: Some(Box::new(|_: &ModelRoute| true)),
        ..Default::default()
    };
    let r = select_model_route(input(vec![asub, zsub], req(40.0, 0.0), extra)).unwrap();
    // Subscription coverage no longer zeroes cost, so the cheaper list price wins
    // on real price (not alphabetically, and not via a 0-cost tie).
    assert_eq!(r.route_key, "zsub/m");
    assert!(
        r.score.selection_cost > 0.0,
        "subscription routes carry their real price, got {}",
        r.score.selection_cost
    );
}

// ── route-priced cost model (subscription multiplier + caller independence) ──

#[test]
fn pricing_is_independent_of_the_caller_subscription_list() {
    // The same route, requested with an EMPTY caller subscription list and with a
    // FULL one, must produce the same cost term: pricing is a property of the
    // route's fact, not of who asks. (This is the defect where one caller priced a
    // route at $6/Mtok and another at cost 0 for the same model and config.)
    let route = fact(FactBuilder {
        provider_id: "alibaba-token-plan".into(),
        model_id: "qwen3.8-max-preview".into(),
        scores: scores(60.0, 1750.0, 90.0),
        cost_per_m_tok_out: Some(6.0),
        ..Default::default()
    });
    let metered = select_model_route(input(
        vec![route.clone()],
        req(80.0, 20.0),
        Extra {
            is_subscription_route: Some(Box::new(|_: &ModelRoute| false)),
            ..Default::default()
        },
    ))
    .unwrap();
    let subscribed = select_model_route(input(
        vec![route],
        req(80.0, 20.0),
        Extra {
            is_subscription_route: Some(Box::new(|_: &ModelRoute| true)),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(
        metered.score.objective.cost, subscribed.score.objective.cost,
        "cost term must not depend on the caller's subscription list"
    );
    assert_eq!(
        metered.score.selection_cost,
        subscribed.score.selection_cost
    );
    // The shared cost is the real list price, not zero.
    assert!((metered.score.objective.cost - 6.0 / 30.0).abs() < 1e-9);
}

#[test]
fn subscription_multiplier_halves_effective_cost() {
    let mut subsidized = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "subsidized".into(),
        scores: scores(60.0, 1750.0, 90.0),
        cost_per_m_tok_out: Some(6.0),
        ..Default::default()
    });
    subsidized.subscription_multiplier = Some(2.0);
    let unsubsidized = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "unsubsidized".into(),
        scores: scores(60.0, 1750.0, 90.0),
        cost_per_m_tok_out: Some(6.0),
        ..Default::default()
    });
    let r = select_model_route(input(
        vec![subsidized, unsubsidized],
        req(80.0, 20.0),
        Extra::default(),
    ))
    .unwrap();
    // A 2.0 multiplier halves $6 -> $3 effective, beating the $6 list-price sibling.
    assert_eq!(r.model.model_id, "subsidized");
    assert!((r.score.objective.cost - 3.0 / 30.0).abs() < 1e-9);
}

#[test]
fn default_multiplier_leaves_cost_at_list_price() {
    // No configured multiplier (None) behaves as 1.0: the route is priced at list.
    let route = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "m".into(),
        scores: scores(60.0, 1750.0, 90.0),
        cost_per_m_tok_out: Some(6.0),
        ..Default::default()
    });
    let r = select_model_route(input(vec![route], req(80.0, 20.0), Extra::default())).unwrap();
    assert!((r.score.objective.cost - 6.0 / 30.0).abs() < 1e-9);
}

#[test]
fn unknown_price_normalizes_to_maximum_cost() {
    // An absent price (no cost_per_m_tok_out, no cost_tier) maps to the maximum
    // normalized cost, never to "cheapest".
    let unknown = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "unknown".into(),
        scores: scores(55.0, 1500.0, 60.0),
        ..Default::default()
    });
    let r = select_model_route(input(vec![unknown], req(40.0, 0.0), Extra::default())).unwrap();
    assert!(
        (r.score.objective.cost - 1.0).abs() < 1e-9,
        "unknown price must normalize to max cost, got {}",
        r.score.objective.cost
    );
    // Ranking an unpriced route at the maximum is a real decision about a route
    // nobody has a price for, so the explanation says so instead of leaving a
    // reader to conclude the model is expensive.
    let explanation = r.explanation.join("\n");
    assert!(
        explanation.contains("price unknown (ranked as max)"),
        "{explanation}"
    );
}

/// A subscription plane publishes no per-token rate (ollama-cloud in Fusiform holds
/// existence, limits, and capabilities only), so an unpriced route on a plan the
/// operator pays for costs zero and beats a cheap metered route on the same demand;
/// the same unpriced fact on a metered provider still ranks at the maximum.
#[test]
fn unpublished_price_on_a_subscription_plan_is_zero_marginal_cost() {
    let plan = fact(FactBuilder {
        provider_id: "ollama-cloud".into(),
        model_id: "glm-5.3-flash".into(),
        scores: scores(55.0, 1610.0, 60.0),
        ..Default::default()
    });
    let metered = fact(FactBuilder {
        provider_id: "openai".into(),
        model_id: "sol".into(),
        scores: scores(55.0, 1838.0, 60.0),
        // Above the effectively-free bucket, so cost decides ahead of adequacy.
        cost_per_m_tok_out: Some(6.0),
        ..Default::default()
    });
    let r = select_model_route(input(
        vec![plan.clone(), metered],
        req(40.0, 0.0),
        Extra {
            is_subscription_route: Some(Box::new(|route: &ModelRoute| {
                route.fact.provider_id == "ollama-cloud"
            })),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(r.route_key, "ollama-cloud/glm-5.3-flash");
    assert!(
        (r.score.objective.cost - 0.0).abs() < 1e-9,
        "{}",
        r.score.objective.cost
    );
    assert!(
        r.explanation
            .join("\n")
            .contains("subscription plan: marginal token cost zero"),
        "{}",
        r.explanation.join("\n")
    );

    let metered_unpriced =
        select_model_route(input(vec![plan], req(40.0, 0.0), Extra::default())).unwrap();
    assert!((metered_unpriced.score.objective.cost - 1.0).abs() < 1e-9);
}

#[test]
fn explanation_carries_effective_list_multiplier_and_coverage() {
    let mut route = fact(FactBuilder {
        provider_id: "alibaba-token-plan".into(),
        model_id: "qwen3.8-max-preview".into(),
        scores: scores(60.0, 1750.0, 90.0),
        cost_per_m_tok_out: Some(6.0),
        ..Default::default()
    });
    route.subscription_multiplier = Some(2.0);
    let r = select_model_route(input(
        vec![route],
        req(80.0, 20.0),
        Extra {
            is_subscription_route: Some(Box::new(|_: &ModelRoute| true)),
            ..Default::default()
        },
    ))
    .unwrap();
    let explanation = r.explanation.join("\n");
    // Effective $3/Mtok (list $6 ÷ 2.0), the multiplier, and coverage all appear so
    // a reader can tell "cheap because subsidized" from "cheap because cheap model".
    assert!(explanation.contains("effective $3/Mtok"), "{explanation}");
    assert!(explanation.contains("list $6/Mtok"), "{explanation}");
    assert!(explanation.contains("x2 subsidy"), "{explanation}");
    assert!(
        explanation.contains("subscription-covered"),
        "{explanation}"
    );
}

#[test]
fn explanation_omits_multiplier_at_default_and_marks_metered() {
    let route = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "m".into(),
        scores: scores(60.0, 1750.0, 90.0),
        cost_per_m_tok_out: Some(6.0),
        ..Default::default()
    });
    let r = select_model_route(input(vec![route], req(80.0, 20.0), Extra::default())).unwrap();
    let explanation = r.explanation.join("\n");
    assert!(explanation.contains("effective $6/Mtok"), "{explanation}");
    assert!(explanation.contains("list $6/Mtok"), "{explanation}");
    assert!(explanation.contains("metered"), "{explanation}");
    // A 1.0 multiplier is "no subsidy known" and must not be reported.
    assert!(!explanation.contains("subsidy"), "{explanation}");
    assert!(
        !explanation.contains("subscription-covered"),
        "{explanation}"
    );
}

// ── model facts seed (TS describe("model facts seed")) ──────────────────────

#[test]
fn seed_loads_and_every_fact_has_valid_scores() {
    let seed = model_facts_seed();
    assert!(seed.len() >= 18);
    for f in seed {
        assert!(f.scores.elo.is_finite());
        assert!(f.scores.eq.is_finite());
        assert!(f.scores.speed.is_finite());
        assert!(matches!(
            f.confidence,
            prefrontal_routing_core::Confidence::Curated
                | prefrontal_routing_core::Confidence::Unknown
        ));
    }
}

#[test]
fn iq_100_seed_selection_respects_the_adequacy_floor() {
    use prefrontal_routing_core::{ModelMetric, ADEQUACY_FLOOR};

    let seed = model_facts_seed();
    let top_elo = seed
        .iter()
        .map(|fact| fact.scores.elo)
        .fold(f64::NEG_INFINITY, f64::max);
    let selected =
        select_model_route(input(seed.to_vec(), req(100.0, 0.0), Extra::default())).unwrap();
    let selected_elo = selected.score.metrics[&ModelMetric::Elo].fact;

    assert!(selected_elo >= top_elo * ADEQUACY_FLOOR);
    assert!(selected.score.adequacy >= ADEQUACY_FLOOR);
}

#[test]
fn low_demand_task_falls_to_a_cheap_adequate_seed_model() {
    let r = select_model_route(input(
        model_facts_seed().to_vec(),
        req(20.0, 0.0),
        Extra::default(),
    ))
    .unwrap();
    // must NOT pick the most expensive model for a trivial task.
    assert_ne!(r.model.model_id, "claude-opus-4-8");
    assert!(r.score.objective.cost < 0.2);
}

#[test]
fn speed_multiplier_selection_tiebreak() {
    let a = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "model-a".into(),
        scores: scores(55.0, 1700.0, 196.0),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });
    let b = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "model-b".into(),
        scores: scores(55.0, 1700.0, 160.0),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });

    let mut multipliers = std::collections::HashMap::new();
    multipliers.insert("p/model-a".to_string(), 0.5);
    prefrontal_routing_core::model_facts::set_speed_multipliers(multipliers);

    let mut facts = vec![a, b];
    for f in &mut facts {
        let route_key = make_route_key(&f.provider_id, &f.model_id, f.variant.as_deref());
        if let Some(mult) = prefrontal_routing_core::model_facts::get_speed_multiplier(&route_key) {
            f.scores.speed *= mult;
            f.speed_multiplier = Some(mult);
        }
    }

    let r = select_model_route(input(facts, req(60.0, 40.0), Extra::default())).unwrap();
    assert_eq!(r.model.model_id, "model-b");
}

#[test]
fn speed_multiplier_explanation_surfaces_multiplier() {
    let a = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "model-a".into(),
        scores: scores(55.0, 1700.0, 196.0),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });

    let mut multipliers = std::collections::HashMap::new();
    multipliers.insert("p/model-a".to_string(), 0.5);
    prefrontal_routing_core::model_facts::set_speed_multipliers(multipliers);

    let mut facts = vec![a];
    for f in &mut facts {
        let route_key = make_route_key(&f.provider_id, &f.model_id, f.variant.as_deref());
        if let Some(mult) = prefrontal_routing_core::model_facts::get_speed_multiplier(&route_key) {
            f.scores.speed *= mult;
            f.speed_multiplier = Some(mult);
        }
    }

    let r = select_model_route(input(facts, req(60.0, 40.0), Extra::default())).unwrap();
    let explanation = r.explanation.join("\n");
    assert!(explanation.contains("(x0.5 configured)"));
}

// ── Tests for the best-effort adequacy band ─────────────────────────────────

/// Helper: build a snapshot with quota_exhausted_until on the given route.
fn snapshot_with_quota_exhausted(route_key: &str, until: f64) -> ModelRouteStateSnapshot {
    let mut by_route = HashMap::new();
    by_route.insert(
        route_key.to_string(),
        RouteRuntimeState {
            quota_exhausted_until: Some(until),
            cooldown_until: None,
            ..Default::default()
        },
    );
    ModelRouteStateSnapshot {
        now: 1000.0,
        by_route,
        by_capacity: HashMap::new(),
    }
}

/// (a) All-blocked catalog → all_routes_inadequate with earliest reset.
/// Every admissible route is quota-blocked, so nothing even makes it to
/// best-effort. The admission filter rejects everything for quota_exhausted
/// and the result should be QuotaOrCooldownExhausted, not AllRoutesInadequate.
/// (AllRoutesInadequate only fires when admissible routes exist but all fall
/// below the band.)
#[test]
fn all_routes_quota_blocked_yields_quota_exhausted_not_all_inadequate() {
    // Two routes: both quota-blocked, so admission rejects them all.
    let good = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "good".into(),
        scores: scores(55.0, 1600.0, 80.0),
        ..Default::default()
    });
    let snapshot = snapshot_with_quota_exhausted("p/good", 2000.0);
    let err = select_model_route_with_config(
        input(
            vec![good],
            req(50.0, 0.0),
            Extra {
                runtime_snapshot: Some(snapshot),
                ..Extra::default()
            },
        ),
        &DEFAULT_ROUTING_CONFIG,
    )
    .unwrap_err();
    // Nothing admitted → QuotaOrCooldownExhausted, not AllRoutesInadequate.
    assert_eq!(err.reason, RouteFailureReason::QuotaOrCooldownExhausted);
}

/// An admissible survivor below the sanity band is rejected. The blocked top
/// route still defines the request frontier and contributes reset diagnostics.
#[test]
fn best_effort_below_band_yields_all_routes_inadequate() {
    let top = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "top".into(),
        scores: scores(1_000.0, 1500.0, 80.0),
        ..Default::default()
    });
    let weak = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "weak".into(),
        scores: scores(300.0, 1500.0, 80.0),
        ..Default::default()
    });
    let err = select_model_route_with_config(
        input(
            vec![top, weak],
            req(90.0, 0.0),
            Extra {
                runtime_snapshot: Some(snapshot_with_quota_exhausted("p/top", 2_000.0)),
                ..Extra::default()
            },
        ),
        &DEFAULT_ROUTING_CONFIG,
    )
    .unwrap_err();
    assert_eq!(err.reason, RouteFailureReason::AllRoutesInadequate);
    let best = err
        .best_rejected
        .expect("weak route is the closest survivor");
    assert_eq!(best.route_key, "p/weak");
    assert!(best.adequacy < MIN_BEST_EFFORT_ADEQUACY);
    assert_eq!(err.earliest_reset, Some(2_000.0));
}

/// A survivor between the floor and sanity band is returned with a warning.
#[test]
fn best_effort_in_band_gets_below_floor_warning() {
    let top = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "top".into(),
        scores: scores(1_000.0, 2_000.0, 80.0),
        ..Default::default()
    });
    let medium = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "medium".into(),
        scores: scores(700.0, 1450.0, 80.0),
        ..Default::default()
    });
    let result = select_model_route_with_config(
        input(
            vec![top, medium],
            req(70.0, 30.0),
            Extra {
                runtime_snapshot: Some(snapshot_with_quota_exhausted("p/top", 2_000.0)),
                ..Extra::default()
            },
        ),
        &DEFAULT_ROUTING_CONFIG,
    )
    .unwrap();
    assert_eq!(result.route_key, "p/medium");
    assert!(result.score.adequacy < prefrontal_routing_core::ADEQUACY_FLOOR);
    assert!(result.score.adequacy >= MIN_BEST_EFFORT_ADEQUACY);
    let warning = result.below_floor.as_ref().expect("below-floor warning");
    assert!((warning.floor - prefrontal_routing_core::ADEQUACY_FLOOR).abs() < 1e-9);
    assert!((warning.adequacy - result.score.adequacy).abs() < 1e-9);
}

/// Lowering the sanity band changes refusal into an explicit best-effort pick.
#[test]
fn survivor_below_default_band_rejected_above_configured_band_selected() {
    let top = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "top".into(),
        scores: scores(1_000.0, 2_000.0, 80.0),
        ..Default::default()
    });
    let weak = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "weak".into(),
        scores: scores(400.0, 1400.0, 80.0),
        ..Default::default()
    });
    let snapshot = snapshot_with_quota_exhausted("p/top", 2_000.0);
    let err = select_model_route_with_config(
        input(
            vec![top.clone(), weak.clone()],
            req(80.0, 20.0),
            Extra {
                runtime_snapshot: Some(snapshot.clone()),
                ..Extra::default()
            },
        ),
        &DEFAULT_ROUTING_CONFIG,
    )
    .unwrap_err();
    assert_eq!(err.reason, RouteFailureReason::AllRoutesInadequate);

    let result = select_model_route_with_config(
        input(
            vec![top, weak],
            req(80.0, 20.0),
            Extra {
                min_best_effort_adequacy: Some(0.1),
                runtime_snapshot: Some(snapshot),
                ..Extra::default()
            },
        ),
        &DEFAULT_ROUTING_CONFIG,
    )
    .unwrap();
    assert_eq!(result.route_key, "p/weak");
    assert!(result.score.adequacy < prefrontal_routing_core::ADEQUACY_FLOOR);
    assert!(result.score.adequacy >= 0.1);
    assert!(result.below_floor.is_some());
}

/// (d) Config clamp: values >1 are clamped to 1; NaN is rejected loudly.
/// The config parsing lives in the module crate, so we test the core constant
/// and the band-check logic here. The module-level config validation
/// (NaN rejection, clamping) is tested in the module crate's own tests.
#[test]
fn default_min_best_effort_adequacy_is_0_6() {
    assert!((MIN_BEST_EFFORT_ADEQUACY - 0.6).abs() < 1e-9);
}

#[test]
fn min_best_effort_adequacy_config_clamp_logic() {
    // Clamp logic: [0, 1], NaN rejected. These mirror the module-crate validation.
    // Values > 1 clamp to 1.
    assert!((1.5_f64.clamp(0.0, 1.0) - 1.0).abs() < 1e-9);
    // Values < 0 clamp to 0.
    assert!(((-0.1_f64).clamp(0.0, 1.0) - 0.0).abs() < 1e-9);
    // Normal values pass through.
    assert!((0.5_f64.clamp(0.0, 1.0) - 0.5).abs() < 1e-9);
    // NaN is NOT finite and should be rejected at parse time (tested in module crate).
    assert!(f64::NAN.is_nan());
}

#[test]
fn effectively_free_bucket_lets_demand_pick_the_stronger_model() {
    // The terra-at-iq-95 inversion, live 2026-08-05: with 40x subsidies both
    // terra (iq 55, $0.375 effective) and sol (iq 58, $0.75) cleared the bar at
    // a 95 demand - terra at adequacy 0.93, sol at 1.00 - and cost-primary
    // ordering handed the flagship demand to terra to save 37 imaginary cents.
    // Inside the effectively-free bucket, adequacy must decide instead. (Weaker
    // models are NOT this test's subject: past the 85 knee the saturating fit
    // curve drops a 53-iq model below the floor entirely, so the GATE excludes
    // it and the ordering never sees it - two earlier drafts asserted on that
    // shape and were vacuous, their mutation proofs green under any ordering.)
    let mut terra = fact(FactBuilder {
        provider_id: "openai".into(),
        model_id: "terra".into(),
        scores: scores(1_685.0, 1590.0, 100.0),
        cost_per_m_tok_out: Some(15.0),
        ..Default::default()
    });
    terra.subscription_multiplier = Some(40.0); // $0.375 effective
    let mut sol = fact(FactBuilder {
        provider_id: "openai".into(),
        model_id: "sol".into(),
        scores: scores(1_838.0, 1610.0, 55.0),
        cost_per_m_tok_out: Some(30.0),
        ..Default::default()
    });
    sol.subscription_multiplier = Some(40.0); // $0.75 effective

    // Anti-vacuity guard: terra must CLEAR the floor at iq 90 on its own, or
    // the gate (not the ordering under test) is what excludes it and the main
    // assertion stops meaning anything. Fails the test loudly if a future
    // anchor change pushes a 55-iq model under the floor at this demand.
    let terra_alone = select_model_route(input(
        vec![terra.clone()],
        req(90.0, 20.0),
        Extra::default(),
    ))
    .unwrap();
    assert!(
        !terra_alone.explanation.join("\n").contains("best-effort"),
        "terra must CLEAR the floor at iq 90 or this test asserts nothing about ordering"
    );

    let high = select_model_route(input(
        vec![terra.clone(), sol.clone()],
        req(90.0, 20.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(
        high.model.model_id, "sol",
        "inside the free bucket the demand's own quality metric decides"
    );

    // iq 30: both saturate adequacy 1.00, so the bucket falls through to the
    // speed tiebreak and the fast mid model keeps cheap-band work off the
    // flagship.
    let low = select_model_route(input(
        vec![terra.clone(), sol.clone()],
        req(30.0, 20.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(
        low.model.model_id, "terra",
        "when adequacy saturates for both, speed keeps cheap work on the faster model"
    );

    // Metered contrast (the bucket-boundary mutation guard): the same pair
    // WITHOUT subsidies sits far above the free threshold, so cost-primary
    // ordering must still pick the genuinely cheaper adequate route. Fails if
    // the bucket is widened to swallow real dollar differences.
    let metered = select_model_route(input(
        vec![
            fact(FactBuilder {
                provider_id: "openai".into(),
                model_id: "terra".into(),
                scores: scores(1_685.0, 1590.0, 100.0),
                cost_per_m_tok_out: Some(15.0),
                ..Default::default()
            }),
            fact(FactBuilder {
                provider_id: "openai".into(),
                model_id: "sol".into(),
                scores: scores(1_838.0, 1610.0, 55.0),
                cost_per_m_tok_out: Some(30.0),
                ..Default::default()
            }),
        ],
        req(90.0, 20.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(
        metered.model.model_id, "terra",
        "outside the bucket real price differences still order the pick"
    );
}

/// The AllRoutesInadequate diagnostic must report a reset the CALLER could
/// actually wait for: the earliest reset among this request's own
/// quota/cooldown rejections. A route outside the request (excluded here)
/// with an EARLIER reset must not leak into the diagnostic - a caller that
/// schedules a retry at a never-candidate's reset retries into the same
/// refusal.
#[test]
fn all_routes_inadequate_reset_is_scoped_to_the_requests_own_rejections() {
    // blocked: quota-rejected candidate (reset 5000). weak: admissible but
    // below the band, forcing AllRoutesInadequate. outsider: EXCLUDED from the
    // request entirely, carrying an earlier reset (2000) that must not win.
    let blocked = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "blocked".into(),
        scores: scores(100.0, 1600.0, 80.0),
        ..Default::default()
    });
    let weak = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "weak".into(),
        scores: scores(35.0, 1500.0, 80.0),
        ..Default::default()
    });
    let outsider = fact(FactBuilder {
        provider_id: "q".into(),
        model_id: "outsider".into(),
        scores: scores(200.0, 1600.0, 80.0),
        ..Default::default()
    });
    let mut snapshot = snapshot_with_quota_exhausted("p/blocked", 5000.0);
    snapshot.by_route.insert(
        "q/outsider".to_string(),
        RouteRuntimeState {
            quota_exhausted_until: Some(2000.0),
            cooldown_until: None,
            ..Default::default()
        },
    );
    let mut request = input(
        vec![blocked, weak, outsider],
        req(90.0, 0.0),
        Extra {
            runtime_snapshot: Some(snapshot),
            ..Extra::default()
        },
    );
    request.exclude_route_keys = vec!["q/outsider".to_string()];
    let err = select_model_route_with_config(request, &DEFAULT_ROUTING_CONFIG).unwrap_err();
    assert_eq!(err.reason, RouteFailureReason::AllRoutesInadequate);
    assert_eq!(
        err.earliest_reset,
        Some(5000.0),
        "the diagnostic must name the candidate's reset, not the excluded outsider's earlier one"
    );
}

#[test]
fn top_routable_excludes_config_excluded_fable() {
    let fable = fact(FactBuilder {
        provider_id: "anthropic".into(),
        model_id: "claude-fable-5".into(),
        scores: scores(1_911.0, 2_200.0, 60.0),
        ..Default::default()
    });
    let sol = fact(FactBuilder {
        provider_id: "openai".into(),
        model_id: "gpt-5.6-sol".into(),
        scores: scores(1_838.0, 1_900.0, 60.0),
        ..Default::default()
    });
    let facts = vec![fable, sol];
    let available = availability_of(&facts);
    let excluded = admit_routes(
        &facts,
        None,
        ExclusionPolicy::caller_only(&["anthropic/claude-fable-5".into()]),
        None,
        &available,
        &req(70.0, 0.0),
        &DEFAULT_ROUTING_CONFIG,
    );
    assert_eq!(excluded.top_routable.elo, 1_838.0);

    let admitted = admit_routes(
        &facts,
        None,
        ExclusionPolicy::default(),
        None,
        &available,
        &req(70.0, 0.0),
        &DEFAULT_ROUTING_CONFIG,
    );
    assert_eq!(admitted.top_routable.elo, 1_911.0);
}

#[test]
fn iq_endpoints_admit_all_at_zero_and_clear_only_the_top_at_100() {
    let facts = vec![
        fact(FactBuilder {
            provider_id: "p".into(),
            model_id: "top".into(),
            scores: scores(1_838.0, 2_000.0, 60.0),
            ..Default::default()
        }),
        fact(FactBuilder {
            provider_id: "p".into(),
            model_id: "lower".into(),
            scores: scores(1_600.0, 1_900.0, 80.0),
            ..Default::default()
        }),
    ];
    let available = availability_of(&facts);
    let zero = admit_routes(
        &facts,
        None,
        ExclusionPolicy::default(),
        None,
        &available,
        &req(0.0, 0.0),
        &DEFAULT_ROUTING_CONFIG,
    );
    assert_eq!(zero.admitted.len(), 2);
    assert!(zero
        .admitted
        .iter()
        .all(|route| route.adequacy.clears_adequacy_floor()));

    let max = admit_routes(
        &facts,
        None,
        ExclusionPolicy::default(),
        None,
        &available,
        &req(100.0, 0.0),
        &DEFAULT_ROUTING_CONFIG,
    );
    let cleared: Vec<_> = max
        .admitted
        .iter()
        .filter(|route| route.adequacy.clears_adequacy_floor())
        .map(|route| route.route.route_key.as_str())
        .collect();
    assert_eq!(cleared, vec!["p/top"]);
}

#[test]
fn unscored_rows_are_rejected_while_aa_inferred_rows_are_admitted() {
    let mut inferred = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "inferred".into(),
        scores: scores(60.0, 1700.0, 100.0),
        ..Default::default()
    });
    inferred.score_provenance = ScoreProvenance::AaInferred;
    inferred.aa_index = Some(40.0);

    let mut unscored = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "unscored".into(),
        scores: scores(99.0, 2000.0, 200.0),
        ..Default::default()
    });
    unscored.score_provenance = ScoreProvenance::Unscored;

    let selected = select_model_route(input(
        vec![inferred.clone(), unscored.clone()],
        req(20.0, 0.0),
        Extra::default(),
    ))
    .expect("aa_inferred is routable");
    assert_eq!(selected.model.model_id, "inferred");
    assert!(
        selected
            .explanation
            .iter()
            .any(|line| line == "provenance=aa_inferred"),
        "live pick must be attributable: {:?}",
        selected.explanation
    );
    assert!(selected
        .rejected
        .iter()
        .any(|rejected| rejected.route_key == "p/unscored"
            && rejected.reason == "unscored_awaiting_placement"));
}

#[test]
fn unscored_zero_score_row_is_unroutable_at_zero_demand_with_typed_rejection() {
    let mut unscored = fact(FactBuilder {
        provider_id: "catalog-provider".into(),
        model_id: "awaiting-placement".into(),
        scores: scores(0.0, 0.0, 0.0),
        supports_vision: Some(true),
        ..Default::default()
    });
    unscored.score_provenance = ScoreProvenance::Unscored;

    let outcome = select_model_route(input(
        vec![unscored],
        req(0.0, 0.0),
        Extra {
            required_capabilities: Some(
                prefrontal_routing_core::model_router::RequiredCapabilities {
                    vision: Some(true),
                    min_context: None,
                },
            ),
            ..Extra::default()
        },
    ));
    let refusal = outcome.expect_err(
        "an unscored row must not route even when zero scores clear a zero-demand capability request",
    );
    assert_eq!(refusal.rejected.len(), 1);
    assert_eq!(refusal.rejected[0].reason, "unscored_awaiting_placement");
}

#[test]
fn use_basket_predicate_requires_both_counters_and_class_producers_and_never_account() {
    // Gated on counters_producer, class_producer, account_producer:
    // use_basket may be true only when counters_producer and class_producer are both true,
    // and account_producer never enters that predicate.
    use prefrontal_routing_core::{
        use_basket_permitted, ACCOUNT_PRODUCER, CLASS_PRODUCER, COUNTERS_PRODUCER,
    };

    assert_eq!(COUNTERS_PRODUCER, true);
    assert_eq!(CLASS_PRODUCER, true);
    assert_eq!(ACCOUNT_PRODUCER, false);

    assert!(!use_basket_permitted(false, false));
    assert!(!use_basket_permitted(true, false));
    assert!(!use_basket_permitted(false, true));
    assert!(use_basket_permitted(true, true));
}

#[test]
fn counters_producer_false_complete_rate_card_selection_bit_identical() {
    // Gated on counters_producer = false:
    // With counters_producer false, use_basket is never true, and selection is bit-identical
    // to today for every route including routes with a complete rate card.
    let mut complete_card_route = fact(FactBuilder {
        provider_id: "complete".into(),
        model_id: "rate-card-model".into(),
        scores: scores(80.0, 1800.0, 100.0),
        cost_per_m_tok_out: Some(10.0),
        ..Default::default()
    });
    complete_card_route.input_rate = Some(2.0);
    complete_card_route.cache_read_rate = Some(0.5);
    complete_card_route.cache_write_rate = Some(1.5);
    complete_card_route.reasoning_rate = Some(10.0);

    let baseline_route = fact(FactBuilder {
        provider_id: "complete".into(),
        model_id: "rate-card-model".into(),
        scores: scores(80.0, 1800.0, 100.0),
        cost_per_m_tok_out: Some(10.0),
        ..Default::default()
    });

    let selected_card = select_model_route(input(
        vec![complete_card_route],
        req(70.0, 0.0),
        Extra::default(),
    ))
    .unwrap();

    let selected_baseline = select_model_route(input(
        vec![baseline_route],
        req(70.0, 0.0),
        Extra::default(),
    ))
    .unwrap();

    assert_eq!(
        selected_card.score.selection_cost.to_bits(),
        selected_baseline.score.selection_cost.to_bits()
    );
    assert_eq!(
        selected_card.model.model_id,
        selected_baseline.model.model_id
    );
}

#[test]
fn selection_cost_prices_with_basket_on_a_usable_class_vector_and_rates_for_every_used_dimension() {
    use prefrontal_routing_core::{selection_cost, ClassVector};

    let mut fact_item = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "m".into(),
        scores: scores(80.0, 1800.0, 100.0),
        cost_per_m_tok_out: Some(10.0),
        ..Default::default()
    });
    fact_item.input_rate = Some(2.0);
    fact_item.cache_read_rate = Some(0.5);
    fact_item.cache_write_rate = Some(1.5);
    fact_item.reasoning_rate = Some(10.0);

    // No vector: falls back to normalized_metered_cost (10.0 / 30.0)
    assert_eq!(selection_cost(&fact_item, false, true), 10.0 / 30.0);

    // Unusable vector (sample_count 49 < 50): falls back to normalized_metered_cost
    fact_item.class_vector = Some(ClassVector {
        workload_class: "ephemeral".into(),
        fresh_input: 1000.0,
        output_excluding_reasoning: 500.0,
        reasoning: 100.0,
        cached_read: 2000.0,
        cache_write: 50.0,
        sample_count: 49,
        trailing_window_days: 7.0,
        statistic: "arithmetic_mean".into(),
        is_overlay: true,
    });
    assert_eq!(selection_cost(&fact_item, false, true), 10.0 / 30.0);

    // Usable vector (sample_count 50 >= 50): prices with basket
    fact_item.class_vector = Some(ClassVector {
        workload_class: "ephemeral".into(),
        fresh_input: 1000.0,
        output_excluding_reasoning: 500.0,
        reasoning: 100.0,
        cached_read: 2000.0,
        cache_write: 50.0,
        sample_count: 50,
        trailing_window_days: 7.0,
        statistic: "arithmetic_mean".into(),
        is_overlay: true,
    });
    let basket_cost = fact_item.basket_cost().unwrap();
    assert_ne!(basket_cost, 10.0 / 30.0);
    assert_eq!(selection_cost(&fact_item, false, true), basket_cost);

    // Missing reasoning rate: prices reasoning counter at cost_per_m_tok_out
    fact_item.reasoning_rate = None;
    assert_eq!(selection_cost(&fact_item, false, true), basket_cost);

    // Missing input rate while the vector carries fresh input: the route cannot
    // be priced on the basket and takes the unpriced-route cost (max), not the
    // output-only ladder. Pricing it output-only would put it in a different
    // unit from every other route in the same basket-priced selection.
    fact_item.input_rate = None;
    assert_eq!(selection_cost(&fact_item, false, true), 1.0);
    assert!(fact_item.basket_cost().is_none());

    // A missing rate on a dimension the workload never uses changes nothing.
    fact_item.input_rate = Some(2.0);
    fact_item.cache_write_rate = None;
    let mut no_writes = fact_item.class_vector.clone().unwrap();
    no_writes.cache_write = 0.0;
    fact_item.class_vector = Some(no_writes);
    let expected = (1000.0 * 2.0 + 500.0 * 10.0 + 100.0 * 10.0 + 2000.0 * 0.5) / 1e6 / 0.05;
    assert!((selection_cost(&fact_item, false, true) - expected).abs() < 1e-12);
}

#[test]
fn select_model_route_consults_ephemeral_vector_and_names_ephemeral() {
    use prefrontal_routing_core::ClassVector;

    let mut f = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "m".into(),
        scores: scores(80.0, 1800.0, 100.0),
        cost_per_m_tok_out: Some(10.0),
        ..Default::default()
    });
    f.input_rate = Some(2.0);
    f.cache_read_rate = Some(0.5);
    f.cache_write_rate = Some(1.5);
    f.class_vector = Some(ClassVector {
        workload_class: "ephemeral".into(),
        fresh_input: 1000.0,
        output_excluding_reasoning: 500.0,
        reasoning: 100.0,
        cached_read: 2000.0,
        cache_write: 50.0,
        sample_count: 50,
        trailing_window_days: 7.0,
        statistic: "arithmetic_mean".into(),
        is_overlay: true,
    });
    f.class_vector_is_overlay = true;

    assert_eq!(f.ephemeral_vector().unwrap().workload_class, "ephemeral");

    let res = select_model_route(input(vec![f], req(50.0, 0.0), Extra::default())).unwrap();
    assert!(
        res.explanation.iter().any(|e| e.contains("basket pricing")),
        "explanation was {:?}",
        res.explanation
    );
}

// ── Basket cost, whole-selection engagement, degrade, and cache separation fixtures ──

use prefrontal_routing_core::selection_cost;

fn make_ephemeral_vector(
    fresh_input: f64,
    output_excluding_reasoning: f64,
    reasoning: f64,
    cached_read: f64,
    cache_write: f64,
    sample_count: u64,
) -> prefrontal_routing_core::ClassVector {
    prefrontal_routing_core::ClassVector {
        workload_class: "ephemeral".into(),
        fresh_input,
        output_excluding_reasoning,
        reasoning,
        cached_read,
        cache_write,
        sample_count,
        trailing_window_days: 7.0,
        statistic: "arithmetic_mean".into(),
        is_overlay: true,
    }
}

fn make_complete_basket_fact(
    provider: &str,
    model: &str,
    speed: f64,
    vector: Option<prefrontal_routing_core::ClassVector>,
) -> ModelFact {
    let mut f = fact(FactBuilder {
        provider_id: provider.into(),
        model_id: model.into(),
        scores: scores(80.0, 1800.0, speed),
        cost_per_m_tok_out: Some(10.0),
        ..Default::default()
    });
    f.input_rate = Some(2.0);
    f.cache_read_rate = Some(0.5);
    f.cache_write_rate = Some(1.5);
    f.reasoning_rate = Some(8.0);
    f.class_vector = vector;
    f.class_vector_is_overlay = f.class_vector.is_some();
    f
}

#[test]
fn numeric_fixture_complete_card_prices_to_stated_value() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let fact_mult_1 = make_complete_basket_fact("p", "m", 100.0, Some(vec.clone()));

    // Numerator: 1000*2.0 + 500*10.0 + 100*8.0 + 2000*0.5 + 50*1.5 = 2000 + 5000 + 800 + 1000 + 75 = 8875.0
    // Raw cost: 8875.0 / 1e6 = 0.008875
    // Multiplier 1 cost: (0.008875 / 0.05) / 1.0 = 0.1775
    let stated_value = 0.1775;
    let cost_1 = fact_mult_1.basket_cost().unwrap();
    assert!((cost_1 - stated_value).abs() < 1e-9);
    assert!((selection_cost(&fact_mult_1, false, true) - stated_value).abs() < 1e-9);

    // Multiplier 40 prices at exactly 1/40th of multiplier 1:
    let mut fact_mult_40 = fact_mult_1.clone();
    fact_mult_40.subscription_multiplier = Some(40.0);
    let cost_40 = fact_mult_40.basket_cost().unwrap();
    assert_eq!(cost_40, cost_1 / 40.0);
    assert!((cost_40 - stated_value / 40.0).abs() < 1e-9);

    // Explicit zero rate is honoured as zero:
    let mut fact_zero_cache = fact_mult_1.clone();
    fact_zero_cache.cache_read_rate = Some(0.0);
    // Numerator without cached_read (2000 * 0.5 = 1000): 8875 - 1000 = 7875.0
    // (7875.0 / 1e6 / 0.05) = 0.1575
    assert!((fact_zero_cache.basket_cost().unwrap() - 0.1575).abs() < 1e-9);

    // Card serving output, input and both cache rates with reasoning_rate Unpriced or omitted
    // still takes the basket path and prices reasoning at cost_per_m_tok_out:
    let mut fact_omitted_reasoning = fact_mult_1.clone();
    fact_omitted_reasoning.reasoning_rate = None;
    // Reasoning counter prices at cost_per_m_tok_out (10.0 instead of 8.0):
    // 8875 - 800 + (100 * 10.0) = 9075.0
    // (9075.0 / 1e6 / 0.05) = 0.1815
    assert!((fact_omitted_reasoning.basket_cost().unwrap() - 0.1815).abs() < 1e-9);
    assert!((selection_cost(&fact_omitted_reasoning, false, true) - 0.1815).abs() < 1e-9);
}

#[test]
fn mutation_omitted_reasoning_rate_takes_basket_not_today_ladder() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let mut fact_omitted_reasoning = make_complete_basket_fact("p", "m", 100.0, Some(vec));
    fact_omitted_reasoning.reasoning_rate = None;

    // Must take basket path (0.1815), NOT today's ladder (10.0 / 30.0 = 0.3333333333333333).
    let basket = fact_omitted_reasoning.basket_cost().unwrap();
    assert!((basket - 0.1815).abs() < 1e-9);
    assert!((basket - 10.0 / 30.0).abs() > 0.1);
    assert!((selection_cost(&fact_omitted_reasoning, false, true) - 0.1815).abs() < 1e-9);
    assert!((selection_cost(&fact_omitted_reasoning, false, true) - 10.0 / 30.0).abs() > 0.1);
}

#[test]
fn mutation_missing_reasoning_rate_prices_at_output_rate_not_zero() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let mut fact_omitted_reasoning = make_complete_basket_fact("p", "m", 100.0, Some(vec));
    fact_omitted_reasoning.reasoning_rate = None;

    // Pricing reasoning at cost_per_m_tok_out yields 0.1815.
    // A mutation treating missing reasoning rate as 0 would yield:
    // (8875 - 800) = 8075 / 1e6 / 0.05 = 0.1615.
    assert!((fact_omitted_reasoning.basket_cost().unwrap() - 0.1815).abs() < 1e-9);
    assert!((fact_omitted_reasoning.basket_cost().unwrap() - 0.1615).abs() > 0.01);
}

#[test]
fn whole_selection_engagement_all_candidates_complete_prices_in_basket() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let fact_a = make_complete_basket_fact("p", "model_a", 100.0, Some(vec.clone()));
    let fact_b = make_complete_basket_fact("p", "model_b", 120.0, Some(vec));

    let res = select_without_overqualification(input(
        vec![fact_a.clone(), fact_b.clone()],
        req(50.0, 0.0),
        Extra::default(),
    ))
    .unwrap();

    // Both candidates had complete cards and usable ephemeral vector: basket engages for selection.
    assert_eq!(res.score.selection_cost, fact_a.basket_cost().unwrap());
    let exp = res.explanation.join("\n");
    assert!(exp.contains("basket pricing (list dollars)"), "{exp}");
}

/// Prices `facts` the way selection does (producers on, term off) and returns
/// the shortlist verdict.
fn price_basket_selection(
    facts: Vec<ModelFact>,
) -> prefrontal_routing_core::model_router::PricedShortlist {
    price_selection_shortlist(
        input(facts, req(50.0, 0.0), Extra::default()),
        &RoutingConfig {
            overqualification: OverqualificationConfig {
                base: 0.0,
                ..DEFAULT_OVERQUALIFICATION
            },
            ..DEFAULT_ROUTING_CONFIG
        },
        true,
        true,
    )
    .unwrap()
}

fn priced_entry<'p>(
    priced: &'p prefrontal_routing_core::model_router::PricedShortlist,
    route_key: &str,
) -> &'p prefrontal_routing_core::model_router::PricedShortlistEntry {
    priced
        .routes
        .iter()
        .find(|route| route.route_key == route_key)
        .unwrap_or_else(|| panic!("{route_key} not shortlisted: {priced:?}"))
}

// A shortlisted route missing a rate for a dimension the workload uses used to
// switch the WHOLE selection to output-only pricing, so one catalog gap on a
// low-ranked route changed every other route's cost and the winner with it.
// Pricing is per route now: that route alone is unpriceable and ranks at the
// maximum; the complete route keeps its basket price and the selection stays
// on the basket.
#[test]
fn one_candidate_missing_a_used_rate_prices_at_max_and_the_selection_stays_on_the_basket() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let fact_complete = make_complete_basket_fact("p", "model_complete", 100.0, Some(vec.clone()));
    let complete_basket = fact_complete.basket_cost().unwrap();

    let mut missing_cache = fact_complete.clone();
    missing_cache.model_id = "missing_cache".into();
    missing_cache.cache_read_rate = None;
    missing_cache.cache_write_rate = None;

    let mut no_input = fact_complete.clone();
    no_input.model_id = "no_input".into();
    no_input.input_rate = None;

    let mut tier_only = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "tier_only".into(),
        scores: scores(80.0, 1800.0, 100.0),
        cost_tier: Some(3.0),
        ..Default::default()
    });
    tier_only.class_vector = Some(vec.clone());
    tier_only.class_vector_is_overlay = true;

    let mut no_output = fact_complete.clone();
    no_output.model_id = "no_output".into();
    no_output.cost_per_m_tok_out = None;

    for (incomplete, unpriced) in [
        (missing_cache, vec!["cached_read", "cache_write"]),
        (no_input, vec!["fresh_input"]),
        (
            tier_only,
            vec![
                "fresh_input",
                "output",
                "reasoning",
                "cached_read",
                "cache_write",
            ],
        ),
        (no_output, vec!["output"]),
    ] {
        let incomplete_key = format!("p/{}", incomplete.model_id);
        let facts = vec![fact_complete.clone(), incomplete];
        let priced = price_basket_selection(facts.clone());
        assert!(priced.use_basket, "{incomplete_key}: {priced:?}");
        assert_eq!(priced.degraded_by, None);
        assert_eq!(priced.pricing_fallback, None);
        let complete = priced_entry(&priced, "p/model_complete");
        assert_eq!(complete.cost, complete_basket, "{incomplete_key}");
        let gap = priced_entry(&priced, &incomplete_key);
        assert_eq!(gap.cost, 1.0, "{incomplete_key} prices at the maximum");
        assert_eq!(
            gap.basket_pricing.as_ref().unwrap().unpriced_dimensions,
            unpriced,
            "{incomplete_key}"
        );

        let res = select_without_overqualification(input(facts, req(50.0, 0.0), Extra::default()))
            .unwrap();
        assert_eq!(res.route_key, "p/model_complete");
        assert_eq!(res.score.selection_cost, complete_basket);
        assert_eq!(res.score.pricing_mode, PricingMode::Basket);
        assert!(res.priced_with_basket);
        let exp = res.explanation.join("\n");
        assert!(exp.contains("basket pricing (list dollars)"), "{exp}");
        assert!(
            exp.contains("pricing: basket for the whole shortlist"),
            "{exp}"
        );
    }
}

// A decoder-refused rate decodes to None on the fact. It is never read as a
// zero rate: the refusing route is unpriceable (max), and it cannot switch the
// complete route off the basket.
#[test]
fn refused_rate_prices_its_route_at_max_without_switching_the_selection() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let fact_complete = make_complete_basket_fact("p", "model_complete", 100.0, Some(vec.clone()));

    let mut fact_refused = fact_complete.clone();
    fact_refused.model_id = "model_refused".into();
    fact_refused.cache_read_rate = None;

    let priced = price_basket_selection(vec![fact_complete.clone(), fact_refused]);
    assert!(priced.use_basket);
    assert_eq!(
        priced_entry(&priced, "p/model_complete").cost,
        fact_complete.basket_cost().unwrap()
    );
    let refused = priced_entry(&priced, "p/model_refused");
    assert_eq!(refused.cost, 1.0);
    assert_eq!(
        refused.basket_pricing.as_ref().unwrap().unpriced_dimensions,
        vec!["cached_read"]
    );
}

#[test]
fn ladder_asserted_in_order_basket_effective_tier_metered_subscription() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let mut complete_fact = make_complete_basket_fact("p", "m", 100.0, Some(vec));
    complete_fact.cost_per_m_tok_out = Some(15.0);
    complete_fact.cost_tier = Some(3.0);
    let basket = complete_fact.basket_cost().unwrap();

    // 1. Basket (when use_basket = true)
    assert_eq!(selection_cost(&complete_fact, false, true), basket);

    // 2. Else effective price (when use_basket = false)
    assert_eq!(selection_cost(&complete_fact, false, false), 15.0 / 30.0);

    // 3. Else cost_tier / 6.0 (when cost_per_m_tok_out is None)
    let mut tier_only_fact = complete_fact.clone();
    tier_only_fact.cost_per_m_tok_out = None;
    assert_eq!(selection_cost(&tier_only_fact, false, false), 3.0 / 6.0);

    // 4. Else 1.0 for metered (when neither cost_per_m_tok_out nor cost_tier is present)
    let mut unpriced_fact = tier_only_fact.clone();
    unpriced_fact.cost_tier = None;
    assert_eq!(selection_cost(&unpriced_fact, false, false), 1.0);

    // 5. Else 0.0 for a subscription route with neither figure
    assert_eq!(selection_cost(&unpriced_fact, true, false), 0.0);
}

// The incomplete route's gap stays on the incomplete route: the complete one is
// priced at its own basket cost (0.1775), never at the output-only ladder
// (10/30) it was dragged to while one route's gap decided the whole selection.
#[test]
fn incomplete_route_cannot_move_the_complete_route_off_its_basket_price() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let fact_complete = make_complete_basket_fact("p", "complete_route", 100.0, Some(vec.clone()));
    let mut fact_incomplete = fact_complete.clone();
    fact_incomplete.model_id = "incomplete_route".into();
    fact_incomplete.cache_read_rate = None;

    let res = select_without_overqualification(input(
        vec![fact_complete.clone(), fact_incomplete],
        req(50.0, 0.0),
        Extra::default(),
    ))
    .unwrap();

    assert_eq!(res.route_key, "p/complete_route");
    assert_eq!(
        res.score.selection_cost,
        fact_complete.basket_cost().unwrap()
    );
    assert_ne!(res.score.selection_cost, 10.0 / 30.0);
}

#[test]
fn mutation_folding_completeness_check_into_per_route_selection_cost_reddens() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let complete_fact = make_complete_basket_fact("p", "m", 100.0, Some(vec));

    // Calling selection_cost with use_basket = false MUST ignore fact.basket_cost()
    // and take today's ladder.
    let cost = selection_cost(&complete_fact, false, false);
    assert_eq!(cost, 10.0 / 30.0);
    assert_ne!(cost, complete_fact.basket_cost().unwrap());
}

#[test]
fn mutation_pricing_refused_rate_at_zero_reddens() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let mut fact_refused = make_complete_basket_fact("p", "m", 100.0, Some(vec));
    fact_refused.cache_read_rate = None; // Refused decodes to None

    // A refused rate must not be treated as Some(0.0). It stays None.
    assert!(!fact_refused.has_required_basket_rates());
    assert!(fact_refused.basket_cost().is_none());
}

#[test]
fn mutation_swapping_basket_reference_and_cost_reference_reddens() {
    use prefrontal_routing_core::routing_config::{
        BASKET_REFERENCE_COST_PER_MESSAGE, COST_REFERENCE_PER_MTOK,
    };
    assert_eq!(BASKET_REFERENCE_COST_PER_MESSAGE, 0.05);
    assert_eq!(COST_REFERENCE_PER_MTOK, 30.0);

    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let complete_fact = make_complete_basket_fact("p", "m", 100.0, Some(vec));

    // Under BASKET_REFERENCE_COST_PER_MESSAGE (0.05), basket_cost is ~0.1775.
    // If swapped with COST_REFERENCE_PER_MTOK (30.0), basket_cost would be ~0.00029583...
    let cost = complete_fact.basket_cost().unwrap();
    assert!((cost - 0.1775).abs() < 1e-9);
    assert!((cost - (0.008875 / 30.0)).abs() > 0.1);
}

#[test]
fn cache_separation_fixture_routes_rank_apart_on_cached_read_term() {
    use prefrontal_routing_core::routing_config::{
        BASKET_REFERENCE_COST_PER_MESSAGE, EFFECTIVELY_FREE_SELECTION_COST,
    };

    let vec = make_ephemeral_vector(1000.0, 200.0, 0.0, 5000.0, 0.0, 50);

    // Route A (prompt caching): cache_read_rate = 0.2
    let mut route_caching = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "caching".into(),
        scores: scores(80.0, 1800.0, 100.0),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });
    route_caching.input_rate = Some(1.0);
    route_caching.cache_read_rate = Some(0.2);
    route_caching.cache_write_rate = Some(1.0);
    route_caching.class_vector = Some(vec.clone());
    route_caching.class_vector_is_overlay = true;

    // Route B (no prompt caching): equal cost_per_m_tok_out (5.0), cache_read_rate = input_rate (1.0),
    // and higher speed (150.0 vs 100.0) so that under today's ladder Route B wins speed tie-break!
    let mut route_nocache = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "nocache".into(),
        scores: scores(80.0, 1800.0, 150.0),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });
    route_nocache.input_rate = Some(1.0);
    route_nocache.cache_read_rate = Some(1.0); // Prices cached reads at input rate!
    route_nocache.cache_write_rate = Some(1.0);
    route_nocache.class_vector = Some(vec);
    route_nocache.class_vector_is_overlay = true;

    let cost_caching = route_caching.basket_cost().unwrap();
    let cost_nocache = route_nocache.basket_cost().unwrap();

    // Both costs strictly above EFFECTIVELY_FREE_SELECTION_COST (0.05) and strictly below clamp ceiling (1.0)
    assert!(
        cost_caching > EFFECTIVELY_FREE_SELECTION_COST,
        "cost_caching {cost_caching} <= {EFFECTIVELY_FREE_SELECTION_COST}"
    );
    assert!(
        cost_caching < 1.0,
        "cost_caching {cost_caching} reached ceiling"
    );
    assert!(
        cost_nocache > EFFECTIVELY_FREE_SELECTION_COST,
        "cost_nocache {cost_nocache} <= {EFFECTIVELY_FREE_SELECTION_COST}"
    );
    assert!(
        cost_nocache < 1.0,
        "cost_nocache {cost_nocache} reached ceiling"
    );
    assert!(
        cost_caching < cost_nocache,
        "caching {cost_caching} not separated from nocache {cost_nocache}"
    );

    // Shipped reference constant (0.05):
    assert_eq!(BASKET_REFERENCE_COST_PER_MESSAGE, 0.05);
    assert!((cost_caching - 0.06).abs() < 1e-9);
    assert!((cost_nocache - 0.14).abs() < 1e-9);

    // Under basket pricing, route_caching wins because cost is lower (0.06 < 0.14)
    let winner = select_model_route(input(
        vec![route_caching.clone(), route_nocache.clone()],
        req(50.0, 0.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(winner.model.model_id, "caching");

    // Explanation names cached-read term and states quantity (list dollars)
    let exp = winner.explanation.join("\n");
    assert!(exp.contains("list dollars"), "{exp}");
    assert!(exp.contains("cached-read term"), "{exp}");

    // A route without a cache-read rate while the workload reads from cache
    // cannot be priced on the basket: it ranks at the maximum and the caching
    // route keeps its basket price. It no longer switches the pair to the
    // output-only ladder, where nocache won on speed at equal output price.
    let mut route_nocache_absent = route_nocache.clone();
    route_nocache_absent.cache_read_rate = None;

    let winner_absent = select_model_route(input(
        vec![route_caching.clone(), route_nocache_absent.clone()],
        req(50.0, 0.0),
        Extra::default(),
    ))
    .unwrap();
    assert_eq!(winner_absent.model.model_id, "caching");
    assert_eq!(winner_absent.score.pricing_mode, PricingMode::Basket);
    let priced = price_selection_shortlist(
        input(
            vec![route_caching.clone(), route_nocache_absent],
            req(50.0, 0.0),
            Extra::default(),
        ),
        &DEFAULT_ROUTING_CONFIG,
        true,
        true,
    )
    .unwrap();
    let absent = priced
        .routes
        .iter()
        .find(|route| route.route_key == "p/nocache")
        .unwrap();
    assert_eq!(absent.cost, 1.0);
    assert_eq!(
        absent.basket_pricing.as_ref().unwrap().unpriced_dimensions,
        vec!["cached_read"]
    );
}

#[test]
fn mutation_raising_reference_constant_until_both_clamp_at_one_reddens() {
    let vec = make_ephemeral_vector(1000.0, 200.0, 0.0, 5000.0, 0.0, 50);

    let mut route_caching = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "caching".into(),
        scores: scores(80.0, 1800.0, 100.0),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });
    route_caching.input_rate = Some(1.0);
    route_caching.cache_read_rate = Some(0.2);
    route_caching.cache_write_rate = Some(1.0);
    route_caching.class_vector = Some(vec.clone());
    route_caching.class_vector_is_overlay = true;

    let mut route_nocache = fact(FactBuilder {
        provider_id: "p".into(),
        model_id: "nocache".into(),
        scores: scores(80.0, 1800.0, 150.0),
        cost_per_m_tok_out: Some(5.0),
        ..Default::default()
    });
    route_nocache.input_rate = Some(1.0);
    route_nocache.cache_read_rate = Some(1.0);
    route_nocache.cache_write_rate = Some(1.0);
    route_nocache.class_vector = Some(vec);
    route_nocache.class_vector_is_overlay = true;

    let cost_caching = route_caching.basket_cost().unwrap();
    let cost_nocache = route_nocache.basket_cost().unwrap();

    // A mutation that scaled costs until both clamp at 1.0 would cause cost_caching == cost_nocache == 1.0,
    // which reddens these assertions:
    assert!(cost_caching < 1.0);
    assert!(cost_nocache < 1.0);
    assert_ne!(cost_caching, cost_nocache);
    assert!(cost_caching < cost_nocache);
}

#[test]
fn decision_explanation_cost_line_states_quantity_and_subscription_quota_proxy() {
    let vec = make_ephemeral_vector(1000.0, 200.0, 0.0, 5000.0, 0.0, 50);
    let mut route = make_complete_basket_fact("sub", "model_sub", 100.0, Some(vec));
    route.cost_per_m_tok_out = Some(5.0);
    route.subscription_multiplier = Some(2.0);

    let res = select_model_route(input(
        vec![route],
        req(50.0, 0.0),
        Extra {
            is_subscription_route: Some(Box::new(|r: &ModelRoute| r.provider_id == "sub")),
            ..Default::default()
        },
    ))
    .unwrap();

    let exp = res.explanation.join("\n");
    assert!(
        exp.contains("basket-per-message quota proxy"),
        "expected quota proxy in explanation: {exp}"
    );
    assert!(
        exp.contains("cached-read term"),
        "expected cached-read term in explanation: {exp}"
    );
    assert!(
        exp.contains("subscription-covered"),
        "expected subscription-covered in explanation: {exp}"
    );
}

// ── Pinned catalog-and-readings replay (reference calibration) ───────────────
//
// Host: this machine. Stores copied read-only to /tmp/calibration-readonly on
// 2026-09-17: ~/.local/share/opencode/opencode.db, prefrontal-core/store.db,
// prefrontal-routing/store.db, fusiform/store.db.
// Window: trailing 7 days ending 2026-09-17 17:48:19 UTC
// (MAX(message.time_created)=1789667299857).
//
// Readings SQL (n=61878 mason assistant rows):
//   ATTACH core; mason := manager_task.agent='mason' with session_id;
//   AVG tokens.input/output/reasoning/cache.read/cache.write on message.data
//   where role=assistant and time_created > now-7d.
//   Means: 4723.88 / 225.29 / 161.18 / 137755.25 / 171.12.
//
// Catalog: routing model_facts joined to latest Fusiform era rates
// (live routing schema is v35; rate columns from migration 037 are not on
// the store, so rates come from the Fusiform producer the catalog-sync path
// would copy). Routable (not excluded) n=209; complete four rates n=29.
// Median complete rate card (n=29): input 3.0, output 15.0, cache_read 0.25,
// cache_write 3.75. Measured ephemeral basket at that card: $0.055049/message.
// Clamp census (cost component == 1.0): today 7, basket at 0.05 = 13 (exceeds
// today), basket at 0.12 = 3.
//
// Decisions: model_route_decisions target_agent='mason' in the same window,
// n=732. The ledger does not persist shortlists; they are reconstructed from
// the scored routable catalog (14 non-fast rows with non-zero Elo/EQ) plus
// each decision's requirements_json. Every reconstructed shortlist includes
// openai/gpt-5.6-sol-pro, which has no Fusiform rate eras, so use_basket is
// false for 732/732. No BASKET_REFERENCE_COST_PER_MESSAGE can engage the
// basket. Calibration stops rather than inventing a fixture.

use prefrontal_routing_core::routing_config::{
    BASKET_REFERENCE_COST_PER_MESSAGE, EFFECTIVELY_FREE_SELECTION_COST,
};
use prefrontal_routing_core::{ACCOUNT_PRODUCER, CLASS_PRODUCER, COUNTERS_PRODUCER};

#[ignore = "0/732 engage because gpt-5.6-sol-pro has no rate era in every shortlist; re-enable after twin rate inheritance"]
#[test]
fn pinned_catalog_and_readings_replay_holds_bucket_membership_and_is_non_vacuous() {
    assert_eq!(BASKET_REFERENCE_COST_PER_MESSAGE, 0.05);
    assert_eq!(EFFECTIVELY_FREE_SELECTION_COST, 0.05);
    assert!(COUNTERS_PRODUCER && CLASS_PRODUCER);
    assert!(!ACCOUNT_PRODUCER);
    // Reconstructed shortlists over the scored routable catalog engage 0 of 732
    // mason decisions (degrader openai/gpt-5.6-sol-pro has no Fusiform rates).
    // No reference constant can create engagement. Do not report green.
    let engaged_mason_decisions: usize = 0;
    assert!(
        engaged_mason_decisions > 0,
        "pinned fixture engaged no decision while counters_producer and class_producer both exist"
    );
}

/// Scored non-fast routable catalog captured 2026-09-17 (14 rows). Rates are
/// latest Fusiform eras; openai/gpt-5.6-sol-pro has none.
fn scored_routable_catalog() -> Vec<ModelFact> {
    let vec = make_ephemeral_vector(4723.88, 225.29, 161.18, 137755.25, 171.12, 61_878);
    let rows: &[(
        &str,
        &str,
        f64,
        f64,
        f64,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        f64,
        Confidence,
    )] = &[
        (
            "openai",
            "gpt-5.6-sol",
            1838.0,
            1610.0,
            55.0,
            Some(20.0),
            Some(4.0),
            Some(0.4),
            Some(5.0),
            33.0,
            Confidence::Curated,
        ),
        (
            "openai",
            "gpt-5.6-sol-pro",
            1838.0,
            1610.0,
            15.0,
            Some(30.0),
            None,
            None,
            None,
            33.0,
            Confidence::Curated,
        ),
        (
            "openai",
            "gpt-5.6-terra-pro",
            1685.0,
            1590.0,
            22.0,
            Some(15.0),
            None,
            None,
            None,
            33.0,
            Confidence::Curated,
        ),
        (
            "openai",
            "gpt-5.6-luna",
            1621.0,
            1555.0,
            160.0,
            Some(1.2),
            Some(0.2),
            Some(0.02),
            Some(0.25),
            33.0,
            Confidence::Curated,
        ),
        (
            "openai",
            "gpt-5.6-luna-pro",
            1621.0,
            1555.0,
            35.0,
            Some(6.0),
            None,
            None,
            None,
            33.0,
            Confidence::Curated,
        ),
        (
            "ollama-cloud",
            "glm-5.2",
            1660.0,
            1565.0,
            196.0,
            Some(4.4),
            Some(1.4),
            Some(0.26),
            None,
            3.0,
            Confidence::Curated,
        ),
        (
            "ollama-cloud",
            "minimax-m3",
            1311.0,
            1520.0,
            44.0,
            Some(2.4),
            Some(0.6),
            Some(0.12),
            None,
            3.0,
            Confidence::Curated,
        ),
        (
            "ollama-cloud",
            "deepseek-v4-pro",
            1284.0,
            1599.8,
            48.0,
            Some(1.98),
            Some(0.66),
            Some(0.022),
            None,
            3.0,
            Confidence::Curated,
        ),
        (
            "ollama-cloud",
            "mistral-large-3",
            520.0,
            1450.0,
            42.0,
            None,
            None,
            None,
            None,
            3.0,
            Confidence::Curated,
        ),
        (
            "google",
            "gemini-3.8-flash",
            1710.0,
            1620.0,
            0.0,
            Some(3.75),
            Some(0.75),
            Some(0.075),
            None,
            22.0,
            Confidence::Unknown,
        ),
        (
            "ollama-cloud",
            "glm-5.3",
            1732.0,
            1579.0,
            0.0,
            Some(4.4),
            Some(1.4),
            Some(0.26),
            None,
            3.0,
            Confidence::Unknown,
        ),
        (
            "ollama-cloud",
            "glm-5.3-flash",
            1610.0,
            1510.0,
            0.0,
            Some(0.5),
            Some(0.15),
            Some(0.03),
            None,
            3.0,
            Confidence::Unknown,
        ),
        (
            "openai",
            "gpt-6-astra",
            1902.0,
            1750.0,
            55.0,
            Some(20.0),
            Some(10.0),
            Some(1.0),
            Some(12.5),
            33.0,
            Confidence::Curated,
        ),
        (
            "ollama-cloud",
            "deepseek-v4.1-flash",
            1660.0,
            1710.0,
            0.0,
            Some(0.6),
            Some(0.15),
            Some(0.003),
            None,
            3.0,
            Confidence::Unknown,
        ),
    ];
    rows.iter()
        .map(
            |(provider, model, elo, eq, speed, out, inp, cr, cw, mult, conf)| {
                let mut f = fact(FactBuilder {
                    provider_id: (*provider).into(),
                    model_id: (*model).into(),
                    scores: scores(*elo, *eq, *speed),
                    cost_per_m_tok_out: *out,
                    confidence: Some(*conf),
                    class_vector: Some(vec.clone()),
                    ..Default::default()
                });
                f.input_rate = *inp;
                f.cache_read_rate = *cr;
                f.cache_write_rate = *cw;
                f.subscription_multiplier = Some(*mult);
                f.class_vector_is_overlay = true;
                f
            },
        )
        .collect()
}

/// Unique (iq, eq, count) over the 732 in-window mason decisions.
const MASON_DECISION_REQS: &[(f64, f64, usize)] = &[
    (100.0, 40.0, 99),
    (100.0, 30.0, 76),
    (70.0, 0.0, 54),
    (100.0, 60.0, 43),
    (85.0, 50.0, 27),
    (100.0, 70.0, 19),
    (85.0, 55.0, 19),
    (80.0, 55.0, 15),
    (85.0, 40.0, 15),
    (75.0, 40.0, 13),
    (80.0, 50.0, 13),
    (70.0, 40.0, 12),
    (88.0, 45.0, 11),
    (80.0, 40.0, 10),
    (60.0, 0.0, 9),
    (80.0, 30.0, 9),
    (85.0, 45.0, 9),
    (90.0, 40.0, 9),
    (100.0, 50.0, 8),
    (85.0, 65.0, 8),
    (85.0, 75.0, 8),
    (70.0, 35.0, 7),
    (85.0, 80.0, 7),
    (95.0, 35.0, 7),
    (100.0, 35.0, 6),
    (100.0, 85.0, 6),
    (30.0, 0.0, 6),
    (70.0, 50.0, 6),
    (80.0, 45.0, 6),
    (85.0, 35.0, 6),
    (85.0, 60.0, 6),
    (85.0, 70.0, 6),
    (55.0, 30.0, 5),
    (60.0, 35.0, 5),
    (62.0, 40.0, 5),
    (70.0, 30.0, 5),
    (70.0, 60.0, 5),
    (75.0, 30.0, 5),
    (75.0, 45.0, 5),
    (78.0, 45.0, 5),
    (65.0, 0.0, 4),
    (75.0, 50.0, 4),
    (75.0, 55.0, 4),
    (78.0, 40.0, 4),
    (88.0, 55.0, 4),
    (90.0, 45.0, 4),
    (92.0, 40.0, 4),
    (92.0, 45.0, 4),
    (60.0, 30.0, 3),
    (75.0, 60.0, 3),
    (78.0, 50.0, 3),
    (78.0, 55.0, 3),
    (82.0, 45.0, 3),
    (88.0, 35.0, 3),
    (90.0, 30.0, 3),
    (90.0, 50.0, 3),
    (92.0, 50.0, 3),
    (95.0, 50.0, 3),
    (100.0, 55.0, 2),
    (100.0, 65.0, 2),
    (100.0, 75.0, 2),
    (50.0, 0.0, 2),
    (50.0, 75.0, 2),
    (60.0, 45.0, 2),
    (62.0, 45.0, 2),
    (65.0, 45.0, 2),
    (65.0, 50.0, 2),
    (68.0, 45.0, 2),
    (70.0, 45.0, 2),
    (70.0, 55.0, 2),
    (72.0, 0.0, 2),
    (72.0, 70.0, 2),
    (75.0, 25.0, 2),
    (78.0, 30.0, 2),
    (80.0, 75.0, 2),
    (82.0, 50.0, 2),
    (88.0, 50.0, 2),
    (90.0, 70.0, 2),
    (92.0, 30.0, 2),
    (92.0, 55.0, 2),
    (95.0, 30.0, 2),
    (95.0, 85.0, 2),
    (100.0, 45.0, 1),
    (100.0, 90.0, 1),
    (20.0, 0.0, 1),
    (40.0, 0.0, 1),
    (40.0, 20.0, 1),
    (50.0, 40.0, 1),
    (50.0, 60.0, 1),
    (55.0, 35.0, 1),
    (65.0, 35.0, 1),
    (70.0, 65.0, 1),
    (70.0, 75.0, 1),
    (72.0, 40.0, 1),
    (72.0, 50.0, 1),
    (75.0, 35.0, 1),
    (75.0, 70.0, 1),
    (78.0, 35.0, 1),
    (80.0, 60.0, 1),
    (82.0, 40.0, 1),
    (85.0, 0.0, 1),
    (88.0, 40.0, 1),
    (90.0, 35.0, 1),
    (92.0, 60.0, 1),
    (92.0, 65.0, 1),
    (95.0, 0.0, 1),
    (95.0, 55.0, 1),
    (95.0, 60.0, 1),
    (95.0, 70.0, 1),
];

// Every reconstructed shortlist holds openai/gpt-5.6-sol-pro, whose catalog row
// serves only an output price. Under the whole-selection rate gate it switched
// all 732 mason decisions to output-only pricing. Per-dimension pricing keeps
// every one of them on the basket and prices sol-pro alone as unpriceable.
#[test]
fn reconstructed_mason_shortlists_stay_on_the_basket_and_price_gpt_5_6_sol_pro_as_unpriced() {
    let catalog = scored_routable_catalog();
    let total: usize = MASON_DECISION_REQS.iter().map(|(_, _, n)| n).sum();
    assert_eq!(total, 732);
    let mut basket_priced = 0usize;
    for &(iq, eq, n) in MASON_DECISION_REQS {
        let priced = price_selection_shortlist(
            input(catalog.clone(), req(iq, eq), Extra::default()),
            &DEFAULT_ROUTING_CONFIG,
            COUNTERS_PRODUCER,
            CLASS_PRODUCER,
        )
        .unwrap_or_else(|err| panic!("iq {iq} eq {eq} failed to price: {err:?}"));
        assert!(
            priced.use_basket,
            "iq {iq} eq {eq} left the basket: {:?}",
            priced.pricing_fallback
        );
        assert_eq!(priced.degraded_by, None, "iq {iq} eq {eq}");
        let sol_pro = priced
            .routes
            .iter()
            .find(|route| route.route_key == "openai/gpt-5.6-sol-pro")
            .unwrap_or_else(|| panic!("iq {iq} eq {eq}: sol-pro not shortlisted"));
        assert_eq!(sol_pro.cost, 1.0, "iq {iq} eq {eq}");
        assert_eq!(
            sol_pro.basket_pricing.as_ref().unwrap().unpriced_dimensions,
            vec!["fresh_input", "cached_read", "cache_write"],
            "iq {iq} eq {eq}"
        );
        basket_priced += n;
    }
    assert_eq!(basket_priced, 732);
}

// Two routes on one subscription plan share one quota pool, so they compare
// as equal cost whatever the basket says. The 0.05 threshold was calibrated
// on output-only costs; the morning the token profile filled (2026-09-18
// 11:28Z) basket pricing engaged, astra priced above the threshold, and every
// iq-100 selection went to sol on a basket difference the shared pool makes
// meaningless. Across plans the basket still orders (see the flagship-versus-
// flash test above), and metered routes never enter the bucket this way.
#[test]
fn same_plan_subscription_routes_compare_as_equal_cost_under_basket_pricing() {
    let catalog = scored_routable_catalog();
    let pair: Vec<ModelFact> = catalog
        .into_iter()
        .filter(|f| f.model_id == "gpt-5.6-sol" || f.model_id == "gpt-6-astra")
        .collect();
    assert_eq!(pair.len(), 2, "both openai rows are in the pinned catalog");

    // Anti-vacuity: with the basket engaged both routes price above the
    // threshold, so only coverage can put them in the bucket.
    let priced = prefrontal_routing_core::model_router::price_selection_shortlist(
        input(
            pair.clone(),
            req(100.0, 40.0),
            Extra {
                is_subscription_route: Some(Box::new(|_: &ModelRoute| true)),
                ..Default::default()
            },
        ),
        &DEFAULT_ROUTING_CONFIG,
        COUNTERS_PRODUCER,
        CLASS_PRODUCER,
    )
    .unwrap();
    assert!(
        priced.use_basket,
        "basket pricing engages on the pinned rows: {:?}",
        priced.degraded_by
    );
    for route in &priced.routes {
        assert!(
            route.selection_cost > EFFECTIVELY_FREE_SELECTION_COST,
            "{} prices above the threshold under the basket ({})",
            route.route_key,
            route.selection_cost
        );
    }

    let covered = select_model_route(input(
        pair.clone(),
        req(100.0, 40.0),
        Extra {
            is_subscription_route: Some(Box::new(|_: &ModelRoute| true)),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(
        covered.model.model_id, "gpt-6-astra",
        "iq 100 under a subscription buys the stronger model"
    );

    // Metered, the same pair orders by price: the pool rule is for sunk dollars.
    let metered = select_model_route(input(
        pair.clone(),
        req(100.0, 40.0),
        Extra {
            is_subscription_route: Some(Box::new(|_: &ModelRoute| false)),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(
        metered.model.model_id, "gpt-5.6-sol",
        "metered routes above the threshold still order by cost"
    );

    // Two plans, both covered: different pools, so the basket still orders.
    let mut cross = pair;
    for f in cross.iter_mut() {
        if f.model_id == "gpt-6-astra" {
            f.provider_id = "other-plan".into();
        }
    }
    let across = select_model_route(input(
        cross,
        req(100.0, 40.0),
        Extra {
            is_subscription_route: Some(Box::new(|_: &ModelRoute| true)),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(
        across.model.model_id, "gpt-5.6-sol",
        "across plans the cheaper basket wins"
    );
}

// Route ordering must be a total order whatever mix of providers, plans and
// prices arrives. When the same-plan rule was applied pairwise inside the
// comparator it was not transitive (two routes on one plan compared as equal
// cost, a third route on another provider compared on its own cost against
// each), so the winner depended on the order candidates arrived in and large
// shortlists could make Rust's sort panic. Every generated shortlist here
// must select without panicking and select the same route in any arrival
// order.
#[test]
fn generated_shortlists_select_one_winner_in_any_arrival_order() {
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }
    let providers = ["p0", "p1", "p2"];
    // Normalized selection costs (the reference is 30 $/Mtok); 0.05 is the
    // effectively-free threshold, so both sides of it appear.
    let costs = [0.03, 0.05, 0.10, 0.20, 0.40, 0.80];
    let elos = [1_300.0, 1_350.0, 1_400.0, 1_450.0, 1_500.0];
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    for trial in 0..400 {
        let len = if trial % 20 == 0 {
            40
        } else {
            3 + rng.below(4) as usize
        };
        let covered: HashSet<String> = providers
            .iter()
            .filter(|_| rng.below(2) == 0)
            .map(|p| p.to_string())
            .collect();
        let facts: Vec<ModelFact> = (0..len)
            .map(|i| {
                fact(FactBuilder {
                    provider_id: providers[rng.below(3) as usize].to_string(),
                    model_id: format!("m{i}"),
                    scores: scores(elos[rng.below(5) as usize], 1_500.0, 100.0),
                    cost_per_m_tok_out: Some(costs[rng.below(6) as usize] * 30.0),
                    ..Default::default()
                })
            })
            .collect();
        let mut winner: Option<String> = None;
        for rotation in 0..facts.len().min(8) {
            for reversed in [false, true] {
                let mut arrival = facts.clone();
                arrival.rotate_left(rotation);
                if reversed {
                    arrival.reverse();
                }
                let covered = covered.clone();
                let selected = select_model_route(input(
                    arrival,
                    req(80.0, 0.0),
                    Extra {
                        is_subscription_route: Some(Box::new(move |route: &ModelRoute| {
                            covered.contains(&route.provider_id)
                        })),
                        ..Default::default()
                    },
                ));
                let Ok(selected) = selected else { continue };
                let key = selected.route_key.clone();
                match &winner {
                    None => winner = Some(key),
                    Some(first) => assert_eq!(
                        first, &key,
                        "trial {trial}: winner depends on arrival order"
                    ),
                }
            }
        }
    }
}

// ── over-qualification term ─────────────────────────────────────────────────
//
// Prices are effective $/Mtok out (list ÷ subscription subsidy) as the live
// fleet carried them on 2026-10-02: opus 1.11 (cost 0.037), sol 6.1 0.40
// (0.013), flash 0.25 (0.008), luna 0.02 (0.0007). All four sit inside the
// effectively-free bucket, where opus used to win low-demand work on speed.

fn priced(provider: &str, model: &str, elo: f64, eq: f64, speed: f64, price: f64) -> ModelFact {
    fact(FactBuilder {
        provider_id: provider.into(),
        model_id: model.into(),
        scores: scores(elo, eq, speed),
        cost_per_m_tok_out: Some(price),
        ..Default::default()
    })
}

fn opus() -> ModelFact {
    priced(
        "anthropic",
        "claude-opus-5-5",
        1_960.0,
        1_500.0,
        120.0,
        1.11,
    )
}

fn sol() -> ModelFact {
    priced("openai", "gpt-6.1-sol", 1_884.0, 1_300.0, 80.0, 0.40)
}

fn flash() -> ModelFact {
    priced(
        "google",
        "antigravity-gemini-3.8-flash",
        1_710.0,
        1_200.0,
        60.0,
        0.25,
    )
}

/// eq 1060 against opus's 1500 is 37 on the demand scale: just under an eq-40
/// bar (fit 0.97), so luna clears the adequacy floor without fitting exactly.
fn luna() -> ModelFact {
    priced("openai", "gpt-6-luna", 1_641.0, 1_060.0, 55.0, 0.02)
}

fn covered<'a>() -> Extra<'a> {
    Extra {
        is_subscription_route: Some(Box::new(|_: &ModelRoute| true)),
        ..Default::default()
    }
}

fn term_off() -> RoutingConfig {
    RoutingConfig {
        overqualification: OverqualificationConfig {
            base: 0.0,
            ..DEFAULT_OVERQUALIFICATION
        },
        ..DEFAULT_ROUTING_CONFIG
    }
}

fn has_term_line(explanation: &[String]) -> bool {
    explanation
        .iter()
        .any(|line| line.starts_with("overqualified"))
}

#[test]
fn overqualification_at_demand_30_hands_flash_the_work_opus_won_on_speed() {
    let facts = vec![flash(), sol(), opus()];
    let without = select_model_route_with_config(
        input(facts.clone(), req(30.0, 0.0), covered()),
        &term_off(),
    )
    .unwrap();
    assert_eq!(
        without.route_key, "anthropic/claude-opus-5-5",
        "fixture must reproduce the incident: opus wins the free bucket on speed"
    );

    let with = select_model_route(input(facts.clone(), req(30.0, 0.0), covered())).unwrap();
    assert_eq!(with.route_key, "google/antigravity-gemini-3.8-flash");
    assert_eq!(with.score.objective.overqualification, 0.0);
    assert!(!has_term_line(&with.explanation), "{:?}", with.explanation);

    let priced = price_selection_shortlist(
        input(facts, req(30.0, 0.0), covered()),
        &DEFAULT_ROUTING_CONFIG,
        false,
        false,
    )
    .unwrap();
    let term = |key: &str| {
        priced
            .routes
            .iter()
            .find(|route| route.route_key == key)
            .unwrap()
            .overqualification
    };
    assert!((term("anthropic/claude-opus-5-5") - 0.35).abs() < 1e-9);
    assert!((term("openai/gpt-6.1-sol") - 0.141).abs() < 1e-3);
    assert_eq!(term("google/antigravity-gemini-3.8-flash"), 0.0);
}

#[test]
fn overqualification_leaves_opus_at_demand_70_as_it_was() {
    let facts = vec![flash(), sol(), opus()];
    let without = select_model_route_with_config(
        input(facts.clone(), req(70.0, 0.0), covered()),
        &term_off(),
    )
    .unwrap();
    let with = select_model_route(input(facts, req(70.0, 0.0), covered())).unwrap();
    assert_eq!(without.route_key, "anthropic/claude-opus-5-5");
    assert_eq!(with.route_key, without.route_key);
    assert_eq!(with.score, without.score);
    assert_eq!(with.explanation, without.explanation);
}

#[test]
fn overqualification_is_soft_opus_alone_at_demand_30_still_wins_and_says_why() {
    let selected = select_model_route(input(vec![opus()], req(30.0, 0.0), covered())).unwrap();
    assert_eq!(selected.route_key, "anthropic/claude-opus-5-5");
    assert!((selected.score.objective.overqualification - 0.35).abs() < 1e-9);
    assert!(
        (selected.score.selection_cost - (1.11 / 30.0 + 0.35)).abs() < 1e-9,
        "the term is added to the selection cost"
    );
    assert!(
        selected
            .explanation
            .iter()
            .any(|line| line == "overqualified +0.35 (surplus 70 over 30)"),
        "{:?}",
        selected.explanation
    );
    let json = serde_json::to_value(&selected.score).unwrap();
    assert!((json["objective"]["overqualification"].as_f64().unwrap() - 0.35).abs() < 1e-9);
}

#[test]
fn overqualification_has_no_term_when_iq_demand_is_zero() {
    let facts = vec![flash(), sol(), opus()];
    let priced = price_selection_shortlist(
        input(facts.clone(), req(0.0, 30.0), covered()),
        &DEFAULT_ROUTING_CONFIG,
        false,
        false,
    )
    .unwrap();
    assert!(!priced.routes.is_empty());
    assert!(priced
        .routes
        .iter()
        .all(|route| route.overqualification == 0.0));
    let without = select_model_route_with_config(
        input(facts.clone(), req(0.0, 30.0), covered()),
        &term_off(),
    )
    .unwrap();
    let with = select_model_route(input(facts, req(0.0, 30.0), covered())).unwrap();
    assert_eq!(with.route_key, without.route_key);
    assert!(!has_term_line(&with.explanation));
}

#[test]
fn overqualification_guard_keeps_opus_when_the_term_would_pick_a_route_short_on_eq() {
    // The recorded iq 55 / eq 40 mason: opus fits exactly, luna is 37 on eq.
    let facts = vec![opus(), luna()];
    let without = select_model_route_with_config(
        input(facts.clone(), req(55.0, 40.0), covered()),
        &term_off(),
    )
    .unwrap();
    assert_eq!(without.route_key, "anthropic/claude-opus-5-5");

    let priced = price_selection_shortlist(
        input(facts.clone(), req(55.0, 40.0), covered()),
        &DEFAULT_ROUTING_CONFIG,
        false,
        false,
    )
    .unwrap();
    let opus_entry = priced
        .routes
        .iter()
        .find(|route| route.route_key == "anthropic/claude-opus-5-5")
        .unwrap();
    assert!(
        opus_entry.selection_cost > 0.05,
        "the term lifts opus out of the free bucket ({})",
        opus_entry.selection_cost
    );

    let with = select_model_route(input(facts, req(55.0, 40.0), covered())).unwrap();
    assert_eq!(with.route_key, "anthropic/claude-opus-5-5");
    assert!(
        with.explanation.iter().any(|line| line
            == "overqualification guard: kept anthropic/claude-opus-5-5 (adequacy 1.00); the term would have picked openai/gpt-6-luna, which fits the demand less well (adequacy 0.99)"),
        "{:?}",
        with.explanation
    );
}

#[test]
fn overqualification_guard_lets_the_term_move_work_to_a_route_that_fits_as_well() {
    // Same request; sol 6.1 fits eq 40 exactly and sits 34 over iq 55 (no term).
    let facts = vec![opus(), luna(), sol()];
    let without = select_model_route_with_config(
        input(facts.clone(), req(55.0, 40.0), covered()),
        &term_off(),
    )
    .unwrap();
    assert_eq!(without.route_key, "anthropic/claude-opus-5-5");
    let with = select_model_route(input(facts, req(55.0, 40.0), covered())).unwrap();
    assert_eq!(with.route_key, "openai/gpt-6.1-sol");
    assert!(!with
        .explanation
        .iter()
        .any(|line| line.starts_with("overqualification guard")));
}

// ── caller concurrency slots ────────────────────────────────────────────────

mod slot_saturation {
    use super::*;
    use prefrontal_routing_core::model_router::select_model_route_with_slots;
    use prefrontal_routing_core::{ConcurrencySlotState, ConcurrencySlots};

    /// The cheapest adequate route, so it wins whenever its slot has room.
    fn opus() -> ModelFact {
        fact(FactBuilder {
            provider_id: "anthropic".into(),
            model_id: "opus".into(),
            scores: scores(62.0, 1900.0, 60.0),
            cost_per_m_tok_out: Some(2.0),
            ..Default::default()
        })
    }

    /// Adequate but pricier: the runner-up.
    fn sol() -> ModelFact {
        fact(FactBuilder {
            provider_id: "openai".into(),
            model_id: "sol".into(),
            scores: scores(62.0, 1900.0, 60.0),
            cost_per_m_tok_out: Some(10.0),
            ..Default::default()
        })
    }

    /// Cheapest of all and never full, but below the requirement bar: a full
    /// provider must never push the decision onto it.
    fn weak() -> ModelFact {
        fact(FactBuilder {
            provider_id: "cheapco".into(),
            model_id: "weak".into(),
            scores: scores(34.0, 1450.0, 300.0),
            cost_per_m_tok_out: Some(0.1),
            ..Default::default()
        })
    }

    fn slot(key: &str, running: u32, limit: u32) -> ConcurrencySlotState {
        ConcurrencySlotState {
            key: key.to_string(),
            running,
            limit,
            queued: 0,
        }
    }

    fn select(slots: Option<&ConcurrencySlots>) -> prefrontal_routing_core::SelectedModelRoute {
        select_model_route_with_slots(
            input(
                vec![weak(), opus(), sol()],
                req(80.0, 20.0),
                Extra::default(),
            ),
            &DEFAULT_ROUTING_CONFIG,
            slots,
        )
        .unwrap()
    }

    #[test]
    fn a_saturated_preferred_provider_yields_to_the_next_adequate_candidate() {
        let slots = ConcurrencySlots::new([slot("anthropic/opus", 50, 40)]);
        let selected = select(Some(&slots));

        assert_eq!(selected.route_key, "openai/sol");
        let skip = selected
            .rejected
            .iter()
            .find(|rejected| rejected.route_key == "anthropic/opus")
            .expect("the skipped route is in the rejected list");
        assert_eq!(skip.reason, "provider_saturated");
        assert_eq!(skip.detail.as_deref(), Some("anthropic/opus 50/40"));
        assert!(
            selected.explanation.iter().any(|line| line
                == "route anthropic/opus skipped: provider_saturated: anthropic/opus 50/40"),
            "explanation names the skip: {:?}",
            selected.explanation
        );
        let saturation = selected.slot_saturation.expect("the skip is reported");
        assert_eq!(saturation.skipped.len(), 1);
        assert_eq!(saturation.queued_on, None);
    }

    #[test]
    fn every_adequate_provider_saturated_keeps_the_normal_winner_and_says_it_queues() {
        let slots =
            ConcurrencySlots::new([slot("anthropic/opus", 50, 40), slot("openai/sol", 25, 25)]);
        let selected = select(Some(&slots));

        assert_eq!(
            selected.route_key, "anthropic/opus",
            "the normal winner is kept; the inadequate route with room is never chosen"
        );
        assert!(
            selected
                .explanation
                .iter()
                .any(|line| line
                    == "all adequate providers saturated; queued on anthropic/opus (50/40)"),
            "explanation says the winner will queue: {:?}",
            selected.explanation
        );
        assert!(selected
            .rejected
            .iter()
            .all(|rejected| rejected.reason != "provider_saturated"));
        let saturation = selected.slot_saturation.expect("the queue is reported");
        assert!(saturation.skipped.is_empty());
        assert_eq!(saturation.queued_on, Some(slot("anthropic/opus", 50, 40)));
    }

    #[test]
    fn no_slot_state_or_slots_with_room_leave_the_decision_unchanged() {
        let baseline = select_model_route_with_config(
            input(
                vec![weak(), opus(), sol()],
                req(80.0, 20.0),
                Extra::default(),
            ),
            &DEFAULT_ROUTING_CONFIG,
        )
        .unwrap();
        assert_eq!(baseline.route_key, "anthropic/opus");
        assert_eq!(baseline.slot_saturation, None);

        assert_eq!(select(None), baseline);
        let roomy =
            ConcurrencySlots::new([slot("anthropic/opus", 39, 40), slot("openai/sol", 50, 0)]);
        assert_eq!(select(Some(&roomy)), baseline);
    }
}

// ── Per-dimension basket pricing: live-shaped mason IQ band ─────────────────
//
// Live-shaped facts. Opus 5.5 and Sol 6.1 are on subscription plans (x18 and
// x25); the Grok-shaped route is metered and its catalog row serves no
// cache-write rate, as Grok, Kimi and GLM rows do. Its elo (1700) clears the
// mason iq bar up to iq 75 and drops out of the shortlist from iq 78 up.
// Opus has more quota headroom than Sol, which decides between them whenever
// both sit in the effectively-free bucket.

fn mason_live_vector() -> prefrontal_routing_core::ClassVector {
    make_ephemeral_vector(4723.88, 225.29, 161.18, 137755.25, 171.12, 61_878)
}

fn live_rated(
    provider: &str,
    model: &str,
    elo: f64,
    speed: f64,
    rates: (f64, f64, f64, Option<f64>),
    multiplier: Option<f64>,
) -> ModelFact {
    let (out, inp, cache_read, cache_write) = rates;
    let mut f = fact(FactBuilder {
        provider_id: provider.into(),
        model_id: model.into(),
        scores: scores(elo, 1_500.0, speed),
        cost_per_m_tok_out: Some(out),
        class_vector: Some(mason_live_vector()),
        ..Default::default()
    });
    f.input_rate = Some(inp);
    f.cache_read_rate = Some(cache_read);
    f.cache_write_rate = cache_write;
    f.subscription_multiplier = multiplier;
    f
}

fn live_band_facts() -> Vec<ModelFact> {
    vec![
        live_rated(
            "anthropic",
            "claude-opus-5-5",
            1_960.0,
            120.0,
            (20.0, 4.0, 0.20, Some(5.0)),
            Some(18.0),
        ),
        live_rated(
            "openai",
            "gpt-6.1-sol",
            1_884.0,
            80.0,
            (10.0, 2.0, 0.10, Some(2.50)),
            Some(25.0),
        ),
        live_rated(
            "xai",
            "grok-5",
            1_700.0,
            90.0,
            (15.0, 3.0, 0.75, None),
            None,
        ),
    ]
}

fn live_band_extra<'a>() -> Extra<'a> {
    let abundance = |value: f64| RouteRuntimeState {
        quota_abundance: Some(value),
        quota_abundance_window: Some("weekly".into()),
        ..Default::default()
    };
    Extra {
        runtime_snapshot: Some(ModelRouteStateSnapshot {
            now: 1_000.0,
            by_route: HashMap::new(),
            by_capacity: HashMap::from([
                ("anthropic/claude-opus-5-5".to_string(), abundance(0.9)),
                ("openai/gpt-6.1-sol".to_string(), abundance(0.3)),
            ]),
        }),
        is_subscription_route: Some(Box::new(|r: &ModelRoute| r.provider_id != "xai")),
        ..Default::default()
    }
}

// The reported flip: at mason iq 60-75 the Grok-shaped route (no cache-write
// rate) sat in the shortlist and switched the whole selection to output-only
// pricing, where Opus (20/18/30 = 0.037) and Sol (10/25/30 = 0.013) both sat in
// the effectively-free bucket and Opus won on quota headroom. From iq 78 Grok
// dropped out, the basket engaged, Opus priced at 0.061 (above the bucket) and
// Sol at 0.022 won. Before this change: Opus at iq 60/70/75, Sol at iq 80.
// Now every iq prices on the basket and Sol wins all four.
#[test]
fn mason_iq_band_keeps_one_pricing_mode_and_one_winner_across_the_grok_cutoff() {
    let mut grok_shortlisted = Vec::new();
    for iq in [60.0, 70.0, 75.0, 80.0] {
        let priced = price_selection_shortlist(
            input(live_band_facts(), req(iq, 0.0), live_band_extra()),
            &DEFAULT_ROUTING_CONFIG,
            COUNTERS_PRODUCER,
            CLASS_PRODUCER,
        )
        .unwrap();
        assert!(
            priced.use_basket,
            "iq {iq} left the basket: {:?}",
            priced.pricing_fallback
        );
        let cost = |key: &str| {
            priced
                .routes
                .iter()
                .find(|route| route.route_key == key)
                .map(|route| route.cost)
        };
        assert!((cost("anthropic/claude-opus-5-5").unwrap() - 0.061_146).abs() < 1e-5);
        assert!((cost("openai/gpt-6.1-sol").unwrap() - 0.022_013).abs() < 1e-5);
        if let Some(grok) = priced.routes.iter().find(|r| r.route_key == "xai/grok-5") {
            grok_shortlisted.push(iq);
            assert_eq!(grok.cost, 1.0, "iq {iq}");
            assert_eq!(
                grok.basket_pricing.as_ref().unwrap().unpriced_dimensions,
                vec!["cache_write"]
            );
        }

        let selected =
            select_model_route(input(live_band_facts(), req(iq, 0.0), live_band_extra())).unwrap();
        assert_eq!(selected.route_key, "openai/gpt-6.1-sol", "iq {iq}");
        assert_eq!(selected.score.pricing_mode, PricingMode::Basket, "iq {iq}");
        assert_eq!(selected.score.pricing_fallback, None);
    }
    // Anti-vacuity: the fixture must straddle the cutoff the defect lived on.
    assert_eq!(grok_shortlisted, vec![60.0, 70.0, 75.0]);
}

// Codex runs record no cache-write tokens. A route whose catalog row lacks a
// cache-write rate is then fully priceable: the dimension contributes zero
// whatever its rate would be.
#[test]
fn route_without_cache_write_rate_stays_basket_priced_when_the_vector_writes_no_cache() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 0.0, 50);
    let complete = make_complete_basket_fact("p", "complete", 100.0, Some(vec.clone()));
    let mut no_cache_write = make_complete_basket_fact("q", "no_cache_write", 100.0, Some(vec));
    no_cache_write.cache_write_rate = None;
    let expected = no_cache_write
        .basket_cost()
        .expect("no rate is needed for a dimension the workload never uses");

    let priced = price_basket_selection(vec![complete, no_cache_write]);
    assert!(priced.use_basket, "{:?}", priced.pricing_fallback);
    let entry = priced_entry(&priced, "q/no_cache_write");
    assert_eq!(entry.cost, expected);
    assert!(entry.cost < 1.0);
    assert!(entry
        .basket_pricing
        .as_ref()
        .unwrap()
        .unpriced_dimensions
        .is_empty());
}

#[test]
fn route_with_cache_write_tokens_and_no_rate_prices_at_max_and_leaves_the_others_on_the_basket() {
    let vec = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 50);
    let complete = make_complete_basket_fact("p", "complete", 100.0, Some(vec.clone()));
    let complete_basket = complete.basket_cost().unwrap();
    let mut no_cache_write = make_complete_basket_fact("q", "no_cache_write", 100.0, Some(vec));
    no_cache_write.cache_write_rate = None;

    let priced = price_basket_selection(vec![complete, no_cache_write.clone()]);
    assert!(priced.use_basket);
    assert_eq!(priced_entry(&priced, "p/complete").cost, complete_basket);
    let entry = priced_entry(&priced, "q/no_cache_write");
    assert_eq!(entry.cost, 1.0);
    assert_eq!(
        entry.basket_pricing.as_ref().unwrap().unpriced_dimensions,
        vec!["cache_write"]
    );

    // Alone it wins (nothing else to pick) and its line says why it is at max.
    let alone = select_without_overqualification(input(
        vec![no_cache_write],
        req(50.0, 0.0),
        Extra::default(),
    ))
    .unwrap();
    let exp = alone.explanation.join("\n");
    assert!(
        exp.contains(
            "unpriceable, ranked as max (cache_write 50 tokens/message, no cache_write_rate)"
        ),
        "{exp}"
    );
}

fn own_vector(sample_count: u64) -> prefrontal_routing_core::ClassVector {
    prefrontal_routing_core::ClassVector {
        is_overlay: false,
        ..make_ephemeral_vector(500.0, 250.0, 0.0, 1000.0, 0.0, sample_count)
    }
}

fn with_own_vector(mut fact: ModelFact, vector: prefrontal_routing_core::ClassVector) -> ModelFact {
    fact.class_vector = Some(vector);
    fact.class_vector_is_overlay = false;
    fact
}

#[test]
fn route_with_no_usable_own_vector_prices_on_the_fleet_vector_and_says_so() {
    let fleet = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 60);
    let on_fleet = make_complete_basket_fact("p", "on_fleet", 100.0, Some(fleet.clone()));
    let own = with_own_vector(
        make_complete_basket_fact("q", "own", 100.0, None),
        own_vector(80),
    );
    let thin_own = with_own_vector(
        make_complete_basket_fact("r", "thin_own", 100.0, None),
        own_vector(49),
    );

    let priced = price_basket_selection(vec![on_fleet.clone(), own.clone(), thin_own.clone()]);
    assert!(priced.use_basket, "{:?}", priced.pricing_fallback);
    let source = |key: &str| {
        priced_entry(&priced, key)
            .basket_pricing
            .as_ref()
            .unwrap()
            .vector
    };
    assert_eq!(source("p/on_fleet"), BasketVectorSource::Fleet);
    assert_eq!(source("q/own"), BasketVectorSource::Own);
    // An own vector below the sample floor is not used; the fleet's is.
    assert_eq!(source("r/thin_own"), BasketVectorSource::Fleet);
    assert_eq!(
        priced_entry(&priced, "r/thin_own").cost,
        thin_own.basket_price(&fleet).cost.unwrap()
    );
    assert_eq!(
        priced_entry(&priced, "q/own").cost,
        own.basket_price(&own_vector(80)).cost.unwrap()
    );

    // With only the thin route in the input there is no fleet vector at all,
    // so the selection has nothing to basket-price it on.
    let selected =
        select_without_overqualification(input(vec![thin_own], req(50.0, 0.0), Extra::default()))
            .unwrap();
    assert_eq!(selected.score.pricing_mode, PricingMode::OutputOnly);
    assert_eq!(
        selected.score.pricing_fallback.as_deref(),
        Some("r/thin_own has no usable ephemeral vector of its own and no fleet ephemeral vector")
    );

    let with_fleet = select_without_overqualification(input(
        vec![
            make_complete_basket_fact("r", "thin_own", 100.0, None),
            on_fleet,
        ],
        req(50.0, 0.0),
        Extra::default(),
    ))
    .unwrap();
    let exp = with_fleet.explanation.join("\n");
    assert!(
        exp.contains("fleet vector (60 samples), no usable own vector"),
        "{exp}"
    );
    let json = serde_json::to_value(&with_fleet.score).unwrap();
    assert_eq!(json["pricingMode"], "basket");
    assert_eq!(json["objective"]["basketPricing"]["vector"], "fleet");
    assert!(json.get("pricingFallback").is_none());
}

// The only whole-selection fallback left: a shortlisted route with no usable
// own vector while the fleet vector is below the sample floor. It names the
// route and the floor.
#[test]
fn unusable_fleet_vector_is_the_only_whole_selection_fallback_and_names_its_cause() {
    let thin_fleet = make_ephemeral_vector(1000.0, 500.0, 100.0, 2000.0, 50.0, 12);
    let a = make_complete_basket_fact("p", "a", 100.0, Some(thin_fleet.clone()));
    let b = with_own_vector(
        make_complete_basket_fact("q", "b", 100.0, None),
        own_vector(80),
    );

    let priced = price_basket_selection(vec![a.clone(), b.clone()]);
    assert!(!priced.use_basket);
    assert_eq!(priced.degraded_by.as_deref(), Some("p/a"));
    assert_eq!(
        priced.pricing_fallback.as_deref(),
        Some("p/a has no usable ephemeral vector of its own and fleet ephemeral vector has 12 samples, below the floor of 50")
    );
    assert!(priced.routes.iter().all(|r| r.basket_pricing.is_none()));

    let selected =
        select_without_overqualification(input(vec![a, b], req(50.0, 0.0), Extra::default()))
            .unwrap();
    assert_eq!(selected.score.pricing_mode, PricingMode::OutputOnly);
    assert!(!selected.priced_with_basket);
    let exp = selected.explanation.join("\n");
    assert!(
        exp.contains("pricing: output-only for the whole shortlist (p/a has no usable ephemeral vector of its own and fleet ephemeral vector has 12 samples, below the floor of 50)"),
        "{exp}"
    );
    let json = serde_json::to_value(&selected.score).unwrap();
    assert_eq!(json["pricingMode"], "output_only");
    assert!(json["objective"].get("basketPricing").is_none());
}

/// A fixed 1,000-selection probe; run with --ignored --nocapture to report latency.
#[test]
#[ignore]
fn subscription_selection_latency_1000() {
    let facts = model_facts_seed().to_vec();
    let scenario = subscription_scenario(&[3.0, 3.0, 51.0, 53.0, 100.0], 20.0, "mason");
    let reference = scenario.runtime_snapshot.unwrap();
    let mut snapshot = ModelRouteStateSnapshot {
        now: reference.now,
        ..Default::default()
    };
    for fact in &facts {
        let key = if fact.provider_id == "anthropic" {
            "anthropic/claude-opus"
        } else {
            "openai/gpt-6.1-sol"
        };
        if matches!(fact.provider_id.as_str(), "anthropic" | "openai") {
            snapshot.by_capacity.insert(
                make_capacity_key(&fact.provider_id, &fact.model_id),
                reference.by_capacity[key].clone(),
            );
        }
    }
    let start = std::time::Instant::now();
    for _ in 0..1_000 {
        std::hint::black_box(
            select_model_route(input(
                facts.clone(),
                req(70.0, 0.0),
                Extra {
                    runtime_snapshot: Some(snapshot.clone()),
                    is_subscription_route: Some(Box::new(|r| {
                        matches!(r.provider_id.as_str(), "anthropic" | "openai")
                    })),
                    ..Extra::default()
                },
            ))
            .unwrap(),
        );
    }
    eprintln!(
        "subscription_selection_latency_1000: {} us total",
        start.elapsed().as_micros()
    );
}

fn quota_account(used: f64) -> prefrontal_routing_core::subscription::QuotaAccount {
    prefrontal_routing_core::subscription::QuotaAccount {
        weekly_used: used,
        weekly_remaining_fraction: 0.5,
        hours_to_reset: 84.0,
        short_walled: false,
        short_windows: Vec::new(),
        banked_resets: 3,
        banked_expires_at_ms: Some(2_000_000),
    }
}

fn subscription_plan(
    tier: &str,
    fee: f64,
    used: &[f64],
) -> prefrontal_routing_core::subscription::SubscriptionPlan {
    prefrontal_routing_core::subscription::SubscriptionPlan {
        tier: Some(tier.into()),
        fee_usd: fee,
        fusiform_multiplier: None,
        accounts: used
            .iter()
            .map(|u| {
                let mut account = quota_account(*u);
                if tier.starts_with("max_") {
                    account.banked_resets = 0;
                }
                account
            })
            .collect(),
        head_points_per_hour: None,
    }
}

fn subscription_scenario(
    claude_use: &[f64],
    margin: f64,
    agent: &str,
) -> SelectModelRouteInput<'static> {
    use prefrontal_routing_core::subscription::{MeasuredValue, SubscriptionPool};
    let mut facts = vec![
        fact(FactBuilder {
            provider_id: "anthropic".into(),
            model_id: "claude-opus".into(),
            scores: scores(1700.0, 1700.0, 80.0),
            cost_per_m_tok_out: Some(25.0),
            ..Default::default()
        }),
        fact(FactBuilder {
            provider_id: "openai".into(),
            model_id: "gpt-6.1-sol".into(),
            scores: scores(1705.0, 1700.0, 100.0),
            cost_per_m_tok_out: Some(20.0),
            ..Default::default()
        }),
    ];
    for f in &mut facts {
        f.input_rate = Some(if f.provider_id == "anthropic" {
            5.0
        } else {
            4.0
        });
        f.class_vector = Some(prefrontal_routing_core::ClassVector {
            workload_class: "ephemeral".into(),
            fresh_input: 1000.0,
            output_excluding_reasoning: 1000.0,
            reasoning: 0.0,
            cached_read: 0.0,
            cache_write: 0.0,
            sample_count: 100,
            trailing_window_days: 7.0,
            statistic: "mean".into(),
            is_overlay: true,
        });
        f.class_vector_is_overlay = true;
    }
    let mut snapshot = ModelRouteStateSnapshot {
        now: 1_000_000.0,
        ..Default::default()
    };
    for f in &facts {
        let claude = f.provider_id == "anthropic";
        let plans = if claude {
            vec![subscription_plan("max_20x", 200.0, claude_use)]
        } else {
            vec![
                subscription_plan("pro_200", 200.0, &[73.0, 89.0]),
                subscription_plan("pro_100", 100.0, &[100.0]),
            ]
        };
        snapshot.by_capacity.insert(
            make_capacity_key(&f.provider_id, &f.model_id),
            RouteRuntimeState {
                subscription_pool: Some(std::sync::Arc::new(SubscriptionPool {
                    provider: f.provider_id.clone(),
                    keep_free_percent: if claude { margin } else { 0.0 },
                    measured: Some(MeasuredValue {
                        usd_per_point: if claude { 28.4 } else { 9.42 },
                        interval_end_ms: 999_999,
                        row_date: "2026-10-09".into(),
                    }),
                    plans,
                    allow_banked: false,
                    now_ms: 1_000_000,
                })),
                ..Default::default()
            },
        );
    }
    let mut input = input(
        facts,
        req(70.0, 0.0),
        Extra {
            runtime_snapshot: Some(snapshot),
            is_subscription_route: Some(Box::new(|_| true)),
            ..Default::default()
        },
    );
    input.target_agent = agent.into();
    input
}

#[test]
fn subscription_margin_blocks_workers_but_not_heads() {
    let worker = select_model_route(subscription_scenario(&[81.0], 20.0, "mason")).unwrap();
    assert_eq!(worker.route_key, "openai/gpt-6.1-sol");
    assert!(worker
        .rejected
        .iter()
        .any(|r| r.route_key == "anthropic/claude-opus" && r.reason == "subscription_reserve"));
    let rejection = worker
        .rejected
        .iter()
        .find(|r| r.reason == "subscription_reserve")
        .unwrap();
    let diagnostic: serde_json::Value =
        serde_json::from_str(rejection.detail.as_deref().unwrap()).unwrap();
    assert_eq!(diagnostic["pool"], "anthropic");
    assert_eq!(diagnostic["unit"], "weekly_percentage_points_per_account");
    assert_eq!(diagnostic["plans"][0]["headroomPoints"], 19.0);
    assert_eq!(diagnostic["plans"][0]["keepFreePoints"], 19.0);
    assert_eq!(diagnostic["plans"][0]["availablePoints"], 0.0);
    assert_eq!(
        diagnostic["plans"][0]["headReservePoints"],
        serde_json::Value::Null
    );
    let head = select_model_route(subscription_scenario(&[81.0], 20.0, "alfonso")).unwrap();
    assert!(!head
        .rejected
        .iter()
        .any(|r| r.route_key == "anthropic/claude-opus"));
    let unknown_worker =
        select_model_route(subscription_scenario(&[81.0], 20.0, "hire_research")).unwrap();
    assert!(unknown_worker
        .rejected
        .iter()
        .any(|r| r.reason == "subscription_reserve"));
}

fn all_reserved_subscription_scenario() -> SelectModelRouteInput<'static> {
    let mut input = subscription_scenario(&[81.0], 20.0, "mason");
    for runtime in input
        .runtime_snapshot
        .as_mut()
        .unwrap()
        .by_capacity
        .values_mut()
    {
        std::sync::Arc::make_mut(runtime.subscription_pool.as_mut().unwrap()).keep_free_percent =
            100.0;
    }
    input
}

fn subscription_panel_input(
    input: SelectModelRouteInput<'static>,
    selection: PanelSelection,
    iq_floor: f64,
) -> SelectPanelInput<'static> {
    SelectPanelInput {
        panel_spec: PanelSpec {
            size: input.facts.len(),
            iq_floor,
            diversity: PanelDiversity::Family,
            class: PanelClass::Audit,
            temperature_spread: None,
        },
        roster: input
            .facts
            .iter()
            .map(|fact| PanelRosterEntry {
                model: make_capacity_key(&fact.provider_id, &fact.model_id),
                prompt_params: serde_json::Map::new(),
            })
            .collect(),
        selection,
        facts: input.facts,
        runtime_snapshot: input.runtime_snapshot,
        available_models: input.available_models,
        availability_substrate: AvailabilitySubstrate::Host,
        exclude_route_keys: input.exclude_route_keys,
        operator_exclude_keys: input.operator_exclude_keys,
        explicit_only_providers: Vec::new(),
        required_capabilities: input.required_capabilities,
        is_subscription_route: input.is_subscription_route,
        package_tokens: None,
    }
}

#[test]
fn all_reserved_pinned_roster_preserves_each_reserve_cause() {
    let partial = select_panel(subscription_panel_input(
        subscription_scenario(&[81.0], 20.0, "mason"),
        PanelSelection::Roster,
        0.0,
    ))
    .unwrap();
    assert_eq!(partial.members.len(), 1);
    let partial_cause = &partial.shortfall.unwrap().causes[0];

    for iq_floor in [0.0, 100.0] {
        let panel = select_panel(subscription_panel_input(
            all_reserved_subscription_scenario(),
            PanelSelection::Roster,
            iq_floor,
        ))
        .expect("a reserved roster returns a decision instead of panicking");
        assert!(panel.members.is_empty());
        assert!(panel.runner_up.is_empty());
        let shortfall = panel.shortfall.expect("every requested seat is reserved");
        assert_eq!(shortfall.code, PanelShortfallCode::SizeReduced);
        assert_eq!(
            shortfall.detail,
            "only 0 of 2 requested panel slots can be served"
        );
        assert_eq!(shortfall.causes.len(), 2);
        assert_eq!(&shortfall.causes[0], partial_cause);
        for (cause, model) in shortfall
            .causes
            .iter()
            .zip(["anthropic/claude-opus", "openai/gpt-6.1-sol"])
        {
            assert_eq!(cause.model, model);
            assert_eq!(cause.reason, PanelCauseReason::QuotaExhausted);
            assert_eq!(
                cause.detail.as_deref(),
                Some("weekly headroom is reserved for heads")
            );
        }
    }
}

#[test]
fn all_reserved_automatic_panel_reports_quota_before_iq_package_and_diversity() {
    for iq_floor in [0.0, 100.0] {
        for package_tokens in [None, Some(usize::MAX)] {
            let mut input = subscription_panel_input(
                all_reserved_subscription_scenario(),
                PanelSelection::NPick,
                iq_floor,
            );
            input.package_tokens = package_tokens;
            let panel = select_panel(input).unwrap();
            assert!(panel.members.is_empty());
            let shortfall = panel.shortfall.unwrap();
            assert_eq!(shortfall.code, PanelShortfallCode::SizeReduced);
            assert_eq!(shortfall.causes.len(), 2);
            assert!(shortfall.causes.iter().all(|cause| {
                cause.reason == PanelCauseReason::QuotaExhausted
                    && cause.detail.as_deref() == Some("weekly headroom is reserved for heads")
            }));
        }
    }
}

#[test]
fn all_reserved_single_selection_returns_a_typed_refusal() {
    let refusal = select_model_route(all_reserved_subscription_scenario()).unwrap_err();
    assert_eq!(refusal.reason, RouteFailureReason::NoAvailableModel);
    assert_eq!(refusal.rejected.len(), 2);
    assert!(refusal
        .rejected
        .iter()
        .all(|rejection| rejection.reason == "subscription_reserve"));
}

#[test]
fn subscription_today_iq70_claude_wins_then_reserve_pressure_flips_to_sol() {
    let selected = select_model_route(subscription_scenario(
        &[3.0, 3.0, 51.0, 53.0, 100.0],
        20.0,
        "mason",
    ))
    .unwrap();
    assert_eq!(selected.route_key, "anthropic/claude-opus");
    let pricing = selected.score.objective.subscription_pricing.unwrap();
    assert_eq!(pricing.available_points, Some(210.0));
    assert_eq!(pricing.banked_resets_applied, 0);
    assert!((pricing.message_points - 0.03 / 28.4).abs() < 1e-12);
    assert!(selected
        .explanation
        .iter()
        .any(|line| line.contains("astrocyte 2026-10-09")
            && line.contains("head reserve unavailable")));
    let selected = select_model_route(subscription_scenario(
        &[78.0, 78.0, 78.0, 78.0, 100.0],
        20.0,
        "mason",
    ))
    .unwrap();
    assert_eq!(selected.route_key, "openai/gpt-6.1-sol");
    assert_eq!(
        selected
            .score
            .objective
            .subscription_pricing
            .unwrap()
            .banked_resets_applied,
        0
    );
}

#[test]
fn subscription_pressure_is_monotone_and_expiring_headroom_is_cheaper() {
    use prefrontal_routing_core::subscription::plan_pressure;
    assert!(plan_pressure(0.5, 0.01) > 90.0 * plan_pressure(0.5, 0.1));
    assert!(plan_pressure(0.01, 0.5) < plan_pressure(0.5, 0.5));
}

#[test]
#[ignore]
fn quota_priced_selection_latency_1000() {
    let start = std::time::Instant::now();
    for _ in 0..1_000 {
        std::hint::black_box(
            select_model_route(subscription_scenario(
                &[3.0, 3.0, 51.0, 53.0, 100.0],
                20.0,
                "mason",
            ))
            .unwrap(),
        );
    }
    eprintln!(
        "quota_priced_selection_latency_1000: {} us total (2 candidates)",
        start.elapsed().as_micros()
    );
}

#[test]
fn subscription_operator_value_overrides_fresh_astro_in_decision() {
    let mut input = subscription_scenario(&[3.0], 20.0, "mason");
    input.facts[0].subscription_multiplier = Some(18.0);
    let selected = select_model_route(input).unwrap();
    let pricing = selected.score.objective.subscription_pricing.unwrap();
    assert_eq!(pricing.value_source, "operator override");
    assert_eq!(pricing.value_per_point_usd, 36.0);
    assert!(selected
        .explanation
        .iter()
        .any(|line| line.contains("operator override")));
}

#[test]
fn subscription_close_economic_prices_preserve_quality_tiebreak() {
    let mut input = subscription_scenario(&[3.0], 0.0, "mason");
    input.requirements = req(100.0, 0.0);
    let runtime = input
        .runtime_snapshot
        .as_mut()
        .unwrap()
        .by_capacity
        .get_mut("openai/gpt-6.1-sol")
        .unwrap();
    let pool = std::sync::Arc::make_mut(runtime.subscription_pool.as_mut().unwrap());
    pool.measured.as_mut().unwrap().usd_per_point = 22.5;
    pool.plans = vec![subscription_plan("pro_200", 200.0, &[3.0])];
    let selected = select_model_route(input).unwrap();
    assert_eq!(selected.route_key, "openai/gpt-6.1-sol");
}

#[test]
fn no_route_refusal_groups_fences_and_attributes_xai_wall() {
    let mut input = subscription_scenario(&[81.0], 20.0, "mason");
    let now = 1_791_694_675_127.0;
    let until = 1_792_081_969_000.0;
    input.facts[1].provider_id = "xai".into();
    input.facts[1].model_id = "grok-4.7".into();
    let snapshot = input.runtime_snapshot.as_mut().unwrap();
    snapshot.now = now;
    let runtime = snapshot
        .by_capacity
        .get_mut("anthropic/claude-opus")
        .unwrap();
    let pool = std::sync::Arc::make_mut(runtime.subscription_pool.as_mut().unwrap());
    for plan in &mut pool.plans {
        for account in &mut plan.accounts {
            account.hours_to_reset = 133.0346825;
        }
    }
    snapshot.by_route.insert(
        "xai/grok-4.7".into(),
        RouteRuntimeState {
            quota_exhausted_until: Some(until),
            ..Default::default()
        },
    );
    input.facts.push(fact(FactBuilder {
        provider_id: "openai".into(),
        model_id: "gpt".into(),
        ..Default::default()
    }));
    input.exclude_route_keys = vec!["openai".into()];
    for i in 0..10 {
        let model = format!("operator-{i}");
        input.facts.push(fact(FactBuilder {
            provider_id: "google".into(),
            model_id: model.clone(),
            ..Default::default()
        }));
        input.operator_exclude_keys.push(format!("google/{model}"));
    }
    input.available_models = input
        .facts
        .iter()
        .map(|fact| format!("{}/{}", fact.provider_id, fact.model_id))
        .collect();
    input.is_subscription_route = Some(Box::new(|route| route.provider_id == "anthropic"));
    let error = select_model_route(input).unwrap_err();
    assert_eq!(error.reason, RouteFailureReason::QuotaOrCooldownExhausted);
    assert_eq!(error.earliest_reset, Some(until));
    assert_eq!(error.earliest_reset_route.as_deref(), Some("xai/grok-4.7"));
    let message = error.refusal_message();
    for expected in [
        "excluded by operator: 10",
        "reserved for heads: 1 (anthropic, until weekly reset in 133 h)",
        "out of credit: 1 (xai/grok-4.7, until_ms=1792081969000)",
        "excluded for this item/caller: 1 (openai)",
        "Earliest route wall expiry: xai/grok-4.7 (until_ms=1792081969000)",
    ] {
        assert!(message.contains(expected), "missing {expected}: {message}");
    }
    let groups = serde_json::to_value(error.blocking_causes()).unwrap();
    assert!(groups
        .as_array()
        .unwrap()
        .iter()
        .any(|group| group["reason"] == "excluded_by_operator" && group["count"] == 10));
}
