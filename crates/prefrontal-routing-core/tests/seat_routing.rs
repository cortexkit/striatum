//! Contract fixtures for persona seat routing (persona substrate v1, slice
//! S2): canonical grand-family facts, exact invocation pins, adequacy
//! filtering, ordered typed recommendations, known-family diversity
//! preference, unknown-family fallback, and deterministic tie-breaks.
//!
//! Covers acceptance criterion 6 and the routing portions of criterion 12.
//!
//! Seat routing is a pure function: every refusal is an `Err` returned BEFORE
//! any caller-side invitation record or create-session effect can exist. The
//! "no invitation or session effect" proofs therefore assert that failure
//! paths yield a typed error and no selection, which is the only channel
//! through which an effect could be requested.

use std::collections::{BTreeSet, HashSet};

use prefrontal_routing_core::model_facts::{
    is_valid_grand_family_id, make_capacity_key, make_route_key, Confidence, ModelFact, ModelScores,
};
use prefrontal_routing_core::model_facts_seed::model_facts_seed;
use prefrontal_routing_core::model_router::{ModelRouteStateSnapshot, RouteRuntimeState};
use prefrontal_routing_core::seat_routing::{
    incumbent_grand_families, select_seat_route, SeatRecommendationTier, SeatRecommendations,
    SeatRouteError, SeatRouteInput,
};
use prefrontal_routing_core::ModelRequirements;

// ── fixture helpers ─────────────────────────────────────────────────────────

/// A curated fact with an explicit optional grand family. Families are always
/// set HERE, as data; never derived from the provider or model name (several
/// fixtures deliberately contradict the name to prove no inference happens).
fn fact(provider: &str, model: &str, family: Option<&str>, iq: f64, cost: f64) -> ModelFact {
    ModelFact {
        provider_id: provider.to_string(),
        model_id: model.to_string(),
        variant: None,
        family: family.map(str::to_string),
        scores: ModelScores {
            iq,
            eq: 1600.0,
            speed: 100.0,
        },
        context_window: Some(200_000.0),
        max_output_tokens: None,
        cost_tier: None,
        cost_per_m_tok_out: Some(cost),
        supports_vision: Some(true),
        supports_tool_use: Some(true),
        prompt_defaults: None,
        confidence: Confidence::Curated,
        notes: None,
        speed_multiplier: None,
        subscription_multiplier: None,
    }
}

fn availability_of(facts: &[ModelFact]) -> HashSet<String> {
    let mut set = HashSet::new();
    for f in facts {
        set.insert(make_route_key(
            &f.provider_id,
            &f.model_id,
            f.variant.as_deref(),
        ));
        set.insert(make_capacity_key(&f.provider_id, &f.model_id));
    }
    set
}

fn families(entries: &[&str]) -> BTreeSet<String> {
    entries.iter().map(|s| s.to_string()).collect()
}

/// Demand iq 50 → adequacy target ≈40.3 on the default anchors: iq 50 clears
/// the gate comfortably, iq 30 lands far below it.
fn req() -> ModelRequirements {
    ModelRequirements { iq: 50.0, eq: 0.0 }
}

struct InputBuilder {
    facts: Vec<ModelFact>,
    model_pin: Option<String>,
    recommendations: SeatRecommendations,
    incumbent_families: BTreeSet<String>,
    available_models: Option<HashSet<String>>,
    runtime_snapshot: Option<ModelRouteStateSnapshot>,
}

impl InputBuilder {
    fn new(facts: Vec<ModelFact>) -> Self {
        Self {
            facts,
            model_pin: None,
            recommendations: SeatRecommendations::default(),
            incumbent_families: BTreeSet::new(),
            available_models: None,
            runtime_snapshot: None,
        }
    }

    fn pin(mut self, pin: &str) -> Self {
        self.model_pin = Some(pin.to_string());
        self
    }

    fn recommendations(mut self, recommendations: SeatRecommendations) -> Self {
        self.recommendations = recommendations;
        self
    }

    fn incumbents(mut self, incumbents: BTreeSet<String>) -> Self {
        self.incumbent_families = incumbents;
        self
    }

    fn available(mut self, available: HashSet<String>) -> Self {
        self.available_models = Some(available);
        self
    }

    fn snapshot(mut self, snapshot: ModelRouteStateSnapshot) -> Self {
        self.runtime_snapshot = Some(snapshot);
        self
    }

    fn build(self) -> SeatRouteInput<'static> {
        let available = self
            .available_models
            .unwrap_or_else(|| availability_of(&self.facts));
        SeatRouteInput {
            requirements: req(),
            model_pin: self.model_pin,
            recommendations: self.recommendations,
            incumbent_families: self.incumbent_families,
            facts: self.facts,
            runtime_snapshot: self.runtime_snapshot,
            exclude_route_keys: Vec::new(),
            required_capabilities: None,
            available_models: available,
            is_subscription_route: None,
        }
    }
}

// ── canonical grand-family facts (no inference from names) ──────────────────

#[test]
fn grand_family_grammar_accepts_canonical_lowercase_ids() {
    for id in ["openai-gpt", "anthropic", "deepseek", "a", "x1", "g.l_m-5"] {
        assert!(is_valid_grand_family_id(id), "{id} should be canonical");
    }
    for id in ["", "GPT", "openai gpt", "-gpt", ".gpt", "família", "a/b"] {
        assert!(!is_valid_grand_family_id(id), "{id} should be rejected");
    }
    let too_long = "a".repeat(65);
    assert!(!is_valid_grand_family_id(&too_long));
    assert!(is_valid_grand_family_id(&"a".repeat(64)));
}

#[test]
fn grand_family_fact_is_data_never_inferred_from_names() {
    // A model literally named like a GPT carries NO family fact → None.
    let unlabeled = fact("openai", "gpt-9", None, 50.0, 5.0);
    assert_eq!(unlabeled.grand_family_id(), None);

    // A family fact contradicting the model name wins: the curated datum is
    // authoritative and nothing looks at the name.
    let cross_labeled = fact("acme", "gpt-clone", Some("deepseek"), 50.0, 5.0);
    assert_eq!(cross_labeled.grand_family_id(), Some("deepseek"));

    // A curated value outside the canonical grammar reads as absent, not as a
    // silently normalized family.
    let malformed = fact("acme", "m1", Some("Not Canonical"), 50.0, 5.0);
    assert_eq!(malformed.grand_family_id(), None);
}

#[test]
fn seed_family_vocabulary_is_canonical() {
    // Every family label shipped in the curated seed must already satisfy the
    // shared canonical grammar consumed by prompt selectors and routing.
    for fact in model_facts_seed() {
        if let Some(family) = &fact.family {
            assert!(
                is_valid_grand_family_id(family),
                "seed family `{family}` for {}/{} is not canonical",
                fact.provider_id,
                fact.model_id
            );
            assert_eq!(fact.grand_family_id(), Some(family.as_str()));
        }
    }
}

#[test]
fn incumbent_families_resolve_from_facts_only() {
    let facts = vec![
        fact("anthropic", "opus", Some("anthropic"), 50.0, 5.0),
        fact("openai", "gpt", Some("openai-gpt"), 50.0, 5.0),
        fact("mystery", "m1", None, 50.0, 5.0),
    ];
    let incumbents = incumbent_grand_families(
        &facts,
        &[
            "anthropic/opus".to_string(),
            "mystery/m1".to_string(),
            "unknown/route".to_string(),
        ],
    );
    // Only the labeled, matching incumbent contributes; unknown-family and
    // unmatched incumbents contribute nothing (and never fail resolution).
    assert_eq!(incumbents, families(&["anthropic"]));
}

// ── criterion 6: explicit pins ──────────────────────────────────────────────

#[test]
fn adequate_pin_wins_over_recommendations_and_cheaper_routes() {
    let facts = vec![
        fact("cheap", "adequate", Some("fam-a"), 50.0, 1.0),
        fact("pinned", "target", Some("fam-b"), 50.0, 20.0),
    ];
    let selection = select_seat_route(
        InputBuilder::new(facts)
            .pin("pinned/target")
            .recommendations(SeatRecommendations {
                models: vec!["cheap/adequate".to_string()],
                ..Default::default()
            })
            .build(),
    )
    .expect("adequate pin must select");
    assert_eq!(selection.route_key, "pinned/target");
    assert!(selection.pinned);
    assert_eq!(selection.recommendation, None);
    assert_eq!(selection.grand_family_id.as_deref(), Some("fam-b"));
}

#[test]
fn unavailable_pin_fails_with_no_selection() {
    // Pin names a model absent from the catalog entirely.
    let facts = vec![fact("other", "adequate", Some("fam-a"), 50.0, 1.0)];
    let err = select_seat_route(InputBuilder::new(facts).pin("ghost/model").build())
        .expect_err("unavailable pin must fail before any effect");
    assert!(matches!(err, SeatRouteError::ModelPinUnavailable { .. }));
    assert_eq!(err.code(), "model_pin_unavailable");
}

#[test]
fn known_but_logged_out_pin_fails_as_unavailable() {
    let facts = vec![
        fact("pinned", "target", Some("fam-b"), 50.0, 5.0),
        fact("other", "adequate", Some("fam-a"), 50.0, 1.0),
    ];
    // Available set excludes the pinned route: caller is not logged into it.
    let available: HashSet<String> = ["other/adequate".to_string()].into_iter().collect();
    let err = select_seat_route(
        InputBuilder::new(facts)
            .pin("pinned/target")
            .available(available)
            .build(),
    )
    .expect_err("logged-out pin must fail");
    assert!(matches!(err, SeatRouteError::ModelPinUnavailable { .. }));
}

#[test]
fn quota_exhausted_pin_fails_as_unavailable() {
    let facts = vec![
        fact("pinned", "target", Some("fam-b"), 50.0, 5.0),
        fact("other", "adequate", Some("fam-a"), 50.0, 1.0),
    ];
    let mut snapshot = ModelRouteStateSnapshot {
        now: 1_000.0,
        ..Default::default()
    };
    snapshot.by_capacity.insert(
        "pinned/target".to_string(),
        RouteRuntimeState {
            quota_exhausted_until: Some(2_000.0),
            ..Default::default()
        },
    );
    let err = select_seat_route(
        InputBuilder::new(facts)
            .pin("pinned/target")
            .snapshot(snapshot)
            .build(),
    )
    .expect_err("exhausted pin must fail");
    match err {
        SeatRouteError::ModelPinUnavailable { detail, .. } => {
            assert!(detail.contains("quota_exhausted"), "detail: {detail}");
        }
        other => panic!("expected ModelPinUnavailable, got {other:?}"),
    }
}

#[test]
fn inadequate_pin_fails_with_named_adequacy_error() {
    let facts = vec![
        fact("pinned", "weak", Some("fam-b"), 30.0, 5.0),
        fact("other", "adequate", Some("fam-a"), 50.0, 1.0),
    ];
    let err = select_seat_route(InputBuilder::new(facts).pin("pinned/weak").build())
        .expect_err("inadequate pin must fail, not silently reroute");
    match err {
        SeatRouteError::ModelPinInadequate {
            pin,
            adequacy,
            floor,
        } => {
            assert_eq!(pin, "pinned/weak");
            assert!(adequacy < floor);
        }
        other => panic!("expected ModelPinInadequate, got {other:?}"),
    }
}

#[test]
fn malformed_pin_is_rejected() {
    let facts = vec![fact("other", "adequate", Some("fam-a"), 50.0, 1.0)];
    for pin in ["no-slash", "a/b/c", "/model", "provider/"] {
        let err = select_seat_route(InputBuilder::new(facts.clone()).pin(pin).build())
            .expect_err("malformed pin must fail");
        assert!(matches!(err, SeatRouteError::ModelPinUnavailable { .. }));
    }
}

// ── criterion 6: recommendations ────────────────────────────────────────────

#[test]
fn recommendation_tiers_apply_in_model_provider_family_order() {
    let facts = vec![
        fact("prov-a", "model-a", Some("fam-a"), 50.0, 1.0),
        fact("prov-b", "model-b", Some("fam-b"), 50.0, 2.0),
        fact("prov-c", "model-c", Some("fam-c"), 50.0, 3.0),
    ];
    // All three tiers name an available adequate route; the model tier wins.
    let selection = select_seat_route(
        InputBuilder::new(facts.clone())
            .recommendations(SeatRecommendations {
                models: vec!["prov-c/model-c".to_string()],
                providers: vec!["prov-b".to_string()],
                families: vec!["fam-a".to_string()],
            })
            .build(),
    )
    .expect("route must select");
    assert_eq!(selection.route_key, "prov-c/model-c");
    let matched = selection.recommendation.expect("a recommendation matched");
    assert_eq!(matched.tier, SeatRecommendationTier::Model);
    assert_eq!(matched.entry, "prov-c/model-c");

    // With no model-tier match, the provider tier wins over the family tier.
    let selection = select_seat_route(
        InputBuilder::new(facts)
            .recommendations(SeatRecommendations {
                models: vec!["ghost/none".to_string()],
                providers: vec!["prov-b".to_string()],
                families: vec!["fam-a".to_string()],
            })
            .build(),
    )
    .expect("route must select");
    assert_eq!(selection.route_key, "prov-b/model-b");
    assert_eq!(
        selection.recommendation.expect("matched").tier,
        SeatRecommendationTier::Provider
    );
}

#[test]
fn recommendation_entries_are_ordered_preferences_within_a_tier() {
    let facts = vec![
        fact("prov-a", "model-a", Some("fam-a"), 50.0, 1.0),
        fact("prov-b", "model-b", Some("fam-b"), 50.0, 2.0),
    ];
    // First declared entry is unavailable/inadequate → the second wins the
    // tier; entry order, not catalog order, decides.
    let selection = select_seat_route(
        InputBuilder::new(facts)
            .recommendations(SeatRecommendations {
                models: vec!["ghost/none".to_string(), "prov-b/model-b".to_string()],
                ..Default::default()
            })
            .build(),
    )
    .expect("route must select");
    assert_eq!(selection.route_key, "prov-b/model-b");
    assert_eq!(
        selection.recommendation.expect("matched").entry,
        "prov-b/model-b"
    );
}

#[test]
fn inadequate_recommendation_falls_through_to_any_adequate_route() {
    let facts = vec![
        fact("prov-a", "weak", Some("fam-a"), 30.0, 1.0),
        fact("prov-b", "strong", Some("fam-b"), 50.0, 2.0),
    ];
    // The recommended model exists and is available but is inadequate: a
    // recommendation is a preference, so routing continues over all adequate
    // routes instead of failing.
    let selection = select_seat_route(
        InputBuilder::new(facts)
            .recommendations(SeatRecommendations {
                models: vec!["prov-a/weak".to_string()],
                ..Default::default()
            })
            .build(),
    )
    .expect("recommendation fallback must select");
    assert_eq!(selection.route_key, "prov-b/strong");
    assert_eq!(selection.recommendation, None);
}

// ── criterion 6: family diversity ───────────────────────────────────────────

#[test]
fn diversity_prefers_family_absent_from_incumbents() {
    // Families A and B adequate; incumbents all map to A → seat routes to B,
    // even though the A route is cheaper.
    let facts = vec![
        fact("prov-a", "model-a", Some("fam-a"), 50.0, 1.0),
        fact("prov-b", "model-b", Some("fam-b"), 50.0, 10.0),
    ];
    let selection = select_seat_route(
        InputBuilder::new(facts)
            .incumbents(families(&["fam-a"]))
            .build(),
    )
    .expect("route must select");
    assert_eq!(selection.route_key, "prov-b/model-b");
    assert!(selection.diversity_satisfied);
}

#[test]
fn diversity_is_preference_not_gate_when_only_incumbent_family_is_adequate() {
    // Only family A adequate; incumbents are A → seat A anyway.
    let facts = vec![
        fact("prov-a", "model-a", Some("fam-a"), 50.0, 1.0),
        fact("prov-b", "weak-b", Some("fam-b"), 30.0, 1.0),
    ];
    let selection = select_seat_route(
        InputBuilder::new(facts)
            .incumbents(families(&["fam-a"]))
            .build(),
    )
    .expect("fallback to incumbent family must select");
    assert_eq!(selection.route_key, "prov-a/model-a");
    assert!(!selection.diversity_satisfied);
}

#[test]
fn unknown_family_never_satisfies_the_diversity_preference() {
    // The unlabeled route is cheaper, but an unknown family cannot count as
    // "different from the incumbents"; the labeled fam-b route is preferred.
    let facts = vec![
        fact("prov-x", "unlabeled", None, 50.0, 1.0),
        fact("prov-b", "model-b", Some("fam-b"), 50.0, 10.0),
    ];
    let selection = select_seat_route(
        InputBuilder::new(facts)
            .incumbents(families(&["fam-a"]))
            .build(),
    )
    .expect("route must select");
    assert_eq!(selection.route_key, "prov-b/model-b");
    assert!(selection.diversity_satisfied);
}

#[test]
fn unknown_family_route_remains_adequate_and_selectable() {
    // Missing family data never makes a route inadequate: with no labeled
    // alternative satisfying the preference, the unlabeled route still seats
    // via the deterministic fallback.
    let facts = vec![
        fact("prov-x", "unlabeled", None, 50.0, 1.0),
        fact("prov-a", "model-a", Some("fam-a"), 50.0, 10.0),
    ];
    let selection = select_seat_route(
        InputBuilder::new(facts)
            .incumbents(families(&["fam-a"]))
            .build(),
    )
    .expect("route must select");
    // Neither candidate satisfies diversity (unlabeled cannot; fam-a matches
    // the incumbents), so the cheapest adequate route wins the fallback.
    assert_eq!(selection.route_key, "prov-x/unlabeled");
    assert!(!selection.diversity_satisfied);
    assert_eq!(selection.grand_family_id, None);
}

#[test]
fn all_family_facts_absent_is_deterministic_cost_order() {
    // With no family facts anywhere, diversity is inert and the existing
    // cost-primary deterministic order decides: repeatably.
    let facts = vec![
        fact("prov-x", "m-x", None, 50.0, 5.0),
        fact("prov-y", "m-y", None, 50.0, 1.0),
        fact("prov-z", "m-z", None, 50.0, 5.0),
    ];
    for _ in 0..3 {
        let selection = select_seat_route(
            InputBuilder::new(facts.clone())
                .incumbents(families(&["fam-a"]))
                .build(),
        )
        .expect("route must select");
        assert_eq!(selection.route_key, "prov-y/m-y");
    }
}

// ── criterion 6: adequacy filtering and no-effect proofs ────────────────────

#[test]
fn no_adequate_route_fails_with_no_selection() {
    // Available routes exist but none clears the gate → typed refusal, no
    // best-effort fallback, and therefore no invitation or session effect.
    let facts = vec![
        fact("prov-a", "weak-a", Some("fam-a"), 30.0, 1.0),
        fact("prov-b", "weak-b", Some("fam-b"), 30.0, 2.0),
    ];
    let err = select_seat_route(InputBuilder::new(facts).build())
        .expect_err("inadequate pool must refuse");
    assert!(matches!(err, SeatRouteError::NoAdequateRoute { .. }));
    assert_eq!(err.code(), "no_adequate_route");
}

#[test]
fn empty_availability_fails_with_no_selection() {
    let facts = vec![fact("prov-a", "model-a", Some("fam-a"), 50.0, 1.0)];
    let err = select_seat_route(InputBuilder::new(facts).available(HashSet::new()).build())
        .expect_err("nothing available must refuse");
    assert!(matches!(err, SeatRouteError::NoAdequateRoute { .. }));
}

#[test]
fn deterministic_tie_breaks_follow_existing_cost_speed_route_key_chain() {
    // Equal cost and speed everywhere → the route-key lexicographic tail
    // decides, deterministically across repeated runs.
    let facts = vec![
        fact("prov-b", "model", Some("fam-b"), 50.0, 5.0),
        fact("prov-a", "model", Some("fam-a"), 50.0, 5.0),
    ];
    for _ in 0..3 {
        let selection =
            select_seat_route(InputBuilder::new(facts.clone()).build()).expect("route must select");
        assert_eq!(selection.route_key, "prov-a/model");
    }
}

// ── routing portions of criterion 12 ────────────────────────────────────────

#[test]
fn recommendation_lists_preserve_declared_order_end_to_end() {
    // Two adequate family-tier entries: the FIRST declared family wins even
    // though the second's route is cheaper: order is preference, not score.
    let facts = vec![
        fact("prov-a", "model-a", Some("fam-a"), 50.0, 10.0),
        fact("prov-b", "model-b", Some("fam-b"), 50.0, 1.0),
    ];
    let selection = select_seat_route(
        InputBuilder::new(facts)
            .recommendations(SeatRecommendations {
                families: vec!["fam-a".to_string(), "fam-b".to_string()],
                ..Default::default()
            })
            .build(),
    )
    .expect("route must select");
    assert_eq!(selection.route_key, "prov-a/model-a");
    let matched = selection.recommendation.expect("family tier matched");
    assert_eq!(matched.tier, SeatRecommendationTier::Family);
    assert_eq!(matched.entry, "fam-a");
}

#[test]
fn family_recommendations_use_the_canonical_fact_vocabulary() {
    // A family recommendation only matches the curated canonical family fact;
    // a malformed curated label (treated as absent) can never match.
    let facts = vec![
        fact("prov-a", "model-a", Some("Not Canonical"), 50.0, 1.0),
        fact("prov-b", "model-b", Some("fam-b"), 50.0, 10.0),
    ];
    let selection = select_seat_route(
        InputBuilder::new(facts)
            .recommendations(SeatRecommendations {
                families: vec!["fam-b".to_string()],
                ..Default::default()
            })
            .build(),
    )
    .expect("route must select");
    assert_eq!(selection.route_key, "prov-b/model-b");
}

#[test]
fn selection_reports_grand_family_for_prompt_variant_consumers() {
    // The routing result carries the same canonical family id the prompt
    // family selectors consume, so both surfaces share one vocabulary.
    let facts = vec![fact("prov-b", "model-b", Some("fam-b"), 50.0, 1.0)];
    let selection = select_seat_route(InputBuilder::new(facts).build()).expect("route must select");
    assert_eq!(selection.grand_family_id.as_deref(), Some("fam-b"));
    assert!(is_valid_grand_family_id(
        selection.grand_family_id.as_deref().unwrap()
    ));
}
