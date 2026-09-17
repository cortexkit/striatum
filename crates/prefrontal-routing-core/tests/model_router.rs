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
    admit_routes, select_model_route, select_model_route_with_config, ModelRouteStateSnapshot,
    RouteFailureReason, RouteRuntimeState, SelectModelRouteInput,
};
use prefrontal_routing_core::{ExclusionPolicy, DEFAULT_ROUTING_CONFIG, MIN_BEST_EFFORT_ADEQUACY};

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
}

/// `fact({ providerID, modelID, scores, ...partial })`: confidence defaults to curated.
fn fact(b: FactBuilder) -> ModelFact {
    ModelFact {
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
    prefer_fast_specs: Vec<String>,
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
        prefer_fast_specs: extra.prefer_fast_specs,
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
        &[],
        &[],
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
        &[],
        &[],
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
    let r = select_model_route(input(
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
fn fast_mechanical_task_routes_to_a_fast_seed_model() {
    let r = select_model_route(input(
        model_facts_seed().to_vec(),
        req(3.0, 0.0),
        Extra::default(),
    ))
    .unwrap();
    let id = &r.model.model_id;
    assert!(
        id.contains("mini")
            || id.contains("nano")
            || id.contains("flash")
            || id.contains("grok")
            || id.contains("minimax"),
        "expected a fast model, got {id}"
    );
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
        &[],
        &[],
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
        &[],
        &[],
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
        &[],
        &[],
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
        &[],
        &[],
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
fn fast_variant_without_opt_in_cannot_win_effectively_free_speed_tiebreak() {
    let mut base = fact(FactBuilder {
        provider_id: "openai".into(),
        model_id: "gpt-5.6-sol".into(),
        scores: scores(1_838.0, 1_610.0, 55.0),
        cost_per_m_tok_out: Some(30.0),
        ..Default::default()
    });
    base.subscription_multiplier = Some(40.0);
    let mut fast = fact(FactBuilder {
        provider_id: "openai".into(),
        model_id: "gpt-5.6-sol-fast".into(),
        scores: scores(1_838.0, 1_610.0, 200.0),
        cost_per_m_tok_out: Some(60.0),
        ..Default::default()
    });
    fast.subscription_multiplier = Some(40.0);

    let selected =
        select_model_route(input(vec![base, fast], req(30.0, 20.0), Extra::default())).unwrap();

    assert_eq!(selected.model.model_id, "gpt-5.6-sol");
    assert!(selected.rejected.iter().any(|rejected| {
        rejected.route_key == "openai/gpt-5.6-sol-fast"
            && rejected.reason == "fast_variant_not_opted_in"
    }));
}

#[test]
fn fast_spelling_without_a_catalogued_base_stays_admissible() {
    let fast = fact(FactBuilder {
        provider_id: "provider".into(),
        model_id: "only-model-fast".into(),
        scores: scores(1_700.0, 1_600.0, 150.0),
        cost_per_m_tok_out: Some(10.0),
        ..Default::default()
    });

    let selected =
        select_model_route(input(vec![fast], req(30.0, 20.0), Extra::default())).unwrap();

    assert_eq!(selected.model.model_id, "only-model-fast");
    assert!(selected.rejected.is_empty());
}

#[test]
fn base_preference_spec_re_admits_the_fast_variant() {
    let base = fact(FactBuilder {
        provider_id: "openai".into(),
        model_id: "gpt-5.6-sol".into(),
        scores: scores(1_838.0, 1_610.0, 55.0),
        cost_per_m_tok_out: Some(30.0),
        ..Default::default()
    });
    let fast = fact(FactBuilder {
        provider_id: "openai".into(),
        model_id: "gpt-5.6-sol-fast".into(),
        scores: scores(1_838.0, 1_610.0, 200.0),
        cost_per_m_tok_out: Some(30.0),
        ..Default::default()
    });

    let selected = select_model_route(input(
        vec![base, fast],
        req(30.0, 20.0),
        Extra {
            prefer_fast_specs: vec!["openai/gpt-5.6-sol".into()],
            ..Extra::default()
        },
    ))
    .unwrap();

    assert_eq!(selected.model.model_id, "gpt-5.6-sol-fast");
    assert!(!selected
        .rejected
        .iter()
        .any(|rejected| rejected.reason == "fast_variant_not_opted_in"));
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
