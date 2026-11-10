use std::collections::{HashMap, HashSet};

use prefrontal_routing_core::{
    demand_bar, make_capacity_key, make_route_key, model_facts_seed, select_model_route,
    select_panel, select_panel_with_policy, AvailabilitySubstrate, ClassPanelPolicy, Confidence,
    InvalidPanelRequest, ModelFact, ModelPromptDefaults, ModelRequirements,
    ModelRouteStateSnapshot, ModelScores, PanelCauseReason, PanelClass, PanelDiversity,
    PanelExtraOrder, PanelOrder, PanelRosterEntry, PanelSelection, PanelShortfallCode, PanelSpec,
    RequiredCapabilities, RouteRuntimeState, ScoreProvenance, SelectModelRouteInput,
    SelectPanelInput, DEFAULT_DEMAND_EXPONENT, DEFAULT_ROUTING_CONFIG,
};
use serde_json::{json, Map};

mod policy_pipeline;
mod policy_records;
mod shortfall_diagnostics;

fn fact(model: &str, elo: f64, family: Option<&str>) -> ModelFact {
    ModelFact {
        billing_plane: None,
        auth_methods: None,
        speed_is_fleet_median: false,
        provider_id: "p".into(),
        model_id: model.into(),
        variant: None,
        family: family.map(str::to_string),
        scores: ModelScores {
            elo,
            eq: 1_600.0,
            speed: 100.0,
        },
        context_window: Some(100_000.0),
        max_output_tokens: None,
        cost_tier: Some(1.0),
        cost_per_m_tok_out: Some(3.0),
        input_rate: None,
        cache_read_rate: None,
        cache_write_rate: None,
        reasoning_rate: None,
        supports_vision: Some(true),
        supports_tool_use: Some(true),
        prompt_defaults: None,
        confidence: Confidence::Curated,
        notes: None,
        speed_multiplier: None,
        subscription_multiplier: None,
        score_provenance: ScoreProvenance::Curated,
        aa_index: None,
        review_by_ms: None,
        eq_source: None,
        class_vector: None,
        class_vector_is_overlay: false,
        upstream_status: None,
    }
}

fn roster(facts: &[ModelFact]) -> Vec<PanelRosterEntry> {
    facts
        .iter()
        .map(|fact| PanelRosterEntry {
            model: make_capacity_key(&fact.provider_id, &fact.model_id),
            prompt_params: Map::new(),
        })
        .collect()
}

fn availability(facts: &[ModelFact]) -> HashSet<String> {
    facts
        .iter()
        .map(|fact| make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()))
        .collect()
}

fn spec(size: usize, iq_floor: f64, diversity: PanelDiversity) -> PanelSpec {
    PanelSpec {
        size,
        iq_floor,
        diversity,
        class: PanelClass::Audit,
        temperature_spread: None,
    }
}

fn panel_input(facts: Vec<ModelFact>, panel_spec: PanelSpec) -> SelectPanelInput<'static> {
    SelectPanelInput {
        package_tokens: None,
        selection: PanelSelection::Roster,
        roster: roster(&facts),
        available_models: availability(&facts),
        availability_substrate: AvailabilitySubstrate::Host,
        availability_label: "host".to_string(),
        panel_spec,
        facts,
        runtime_snapshot: None,
        exclude_route_keys: Vec::new(),
        operator_exclude_keys: Vec::new(),
        required_capabilities: None,
        is_subscription_route: None,
    }
}

fn n_pick_input(facts: Vec<ModelFact>, panel_spec: PanelSpec) -> SelectPanelInput<'static> {
    SelectPanelInput {
        package_tokens: None,
        selection: PanelSelection::NPick,
        roster: Vec::new(),
        available_models: availability(&facts),
        availability_substrate: AvailabilitySubstrate::Host,
        availability_label: "host".to_string(),
        panel_spec,
        facts,
        runtime_snapshot: None,
        exclude_route_keys: Vec::new(),
        operator_exclude_keys: Vec::new(),
        required_capabilities: None,
        is_subscription_route: None,
    }
}

fn package_facts() -> Vec<ModelFact> {
    [
        ("small", 1900.0, "small-lineage", 500_000.0),
        ("kept", 1880.0, "kept-lineage", 1_000_000.0),
        ("duplicate", 1870.0, "kept-lineage", 1_000_000.0),
        ("replacement", 1860.0, "new-lineage", 1_000_000.0),
    ]
    .into_iter()
    .map(|(model, elo, family, window)| {
        let mut row = fact(model, elo, Some(family));
        row.context_window = Some(window);
        row.max_output_tokens = Some(64_000);
        row
    })
    .collect()
}

#[test]
fn oversized_roster_pin_is_replaced_using_automatic_lineage_rules() {
    let facts = package_facts();
    let mut input = panel_input(facts.clone(), spec(2, 80.0, PanelDiversity::Family));
    input.roster = roster(&facts[..2]);
    input.package_tokens = Some(227_000);
    let panel = select_panel(input).unwrap();
    assert!(panel.shortfall.is_none());
    assert_eq!(
        panel
            .members
            .iter()
            .map(|member| member.route_key.as_str())
            .collect::<Vec<_>>(),
        ["p/replacement", "p/kept"]
    );
    assert_eq!(panel.members[0].context_window, Some(1_000_000));
    let policy = panel.policy.as_ref().unwrap();
    assert_eq!(policy.replacements.len(), 1);
    assert_eq!(policy.replacements[0].replaced_route_key, "p/small");
    assert_eq!(
        policy.replacements[0].replacement_route_key,
        "p/replacement"
    );
    assert_eq!(
        policy.replacements[0].reason,
        "package 227000 tokens exceeds p/small ceiling 210000 tokens"
    );
}

#[test]
fn roster_package_refusal_names_binding_seat_limits_and_zero_fitting_width() {
    let facts = package_facts();
    let mut input = panel_input(facts.clone(), spec(2, 80.0, PanelDiversity::Family));
    input.roster = roster(&facts[..2]);
    input.package_tokens = Some(500_000);
    let panel = select_panel(input).unwrap();
    let refusal = panel.shortfall.unwrap();
    assert_eq!(refusal.code, PanelShortfallCode::PackageUnmeetable);
    assert_eq!(refusal.detail, "The evidence package is 500,000 tokens; none of the 2 panel seats can read that much. p/small can read up to 210,000 tokens. p/kept can read up to 460,000 tokens. A seat gives evidence half of its context window after reserving room for its answer and prompt; the other half is kept for the panel's discussion. Narrow the evidence (line ranges or fewer includes) or set a smaller roster in the project's athena.members setting. Nothing was sent to a model and no evidence was cut.");
    assert!(panel.members.is_empty());
    for (model, detail) in [
        (
            "p/small",
            "package 500000 tokens exceeds p/small ceiling 210000 tokens",
        ),
        (
            "p/kept",
            "package 500000 tokens exceeds p/kept ceiling 460000 tokens",
        ),
    ] {
        assert!(refusal.causes.iter().any(|cause| {
            cause.model == model
                && cause.reason == PanelCauseReason::PackageExceedsCeiling
                && cause
                    .detail
                    .as_deref()
                    .is_some_and(|value| value.starts_with(detail))
        }));
    }
}

#[test]
fn automatic_package_refusal_names_binding_seat_limits_and_fitting_width() {
    let facts = package_facts();
    let mut panel_spec = spec(4, 80.0, PanelDiversity::Family);
    panel_spec.class = PanelClass::Spec;
    let mut input = n_pick_input(facts, panel_spec);
    input.package_tokens = Some(227_000);
    let panel = select_panel(input).unwrap();
    assert_eq!(panel.members.len(), 2);
    let refusal = panel.shortfall.unwrap();
    assert_eq!(refusal.code, PanelShortfallCode::PackageUnmeetable);
    assert_eq!(refusal.detail, "The evidence package is 227,000 tokens; only 2 of the 4 panel seats can read that much. p/small can read up to 210,000 tokens. A seat gives evidence half of its context window after reserving room for its answer and prompt; the other half is kept for the panel's discussion. Narrow the evidence (line ranges or fewer includes) or set a smaller roster in the project's athena.members setting. Nothing was sent to a model and no evidence was cut.");
}

#[test]
fn package_refusal_bounds_the_rejected_seat_listing_in_selection_order() {
    let facts = (1..=6)
        .map(|index| {
            fact(
                &format!("small-{index}"),
                1900.0 - f64::from(index),
                Some(&format!("family-{index}")),
            )
        })
        .collect();
    let mut input = panel_input(facts, spec(6, 80.0, PanelDiversity::Family));
    input.package_tokens = Some(1_234_567);
    let panel = select_panel(input).unwrap();
    let refusal = panel.shortfall.unwrap();
    assert_eq!(refusal.code, PanelShortfallCode::PackageUnmeetable);
    assert_eq!(refusal.detail, "The evidence package is 1,234,567 tokens; none of the 6 panel seats can read that much. p/small-1 can read up to 26,000 tokens. p/small-2 can read up to 26,000 tokens. p/small-3 can read up to 26,000 tokens. p/small-4 can read up to 26,000 tokens. 2 more seats are too small. A seat gives evidence half of its context window after reserving room for its answer and prompt; the other half is kept for the panel's discussion. Narrow the evidence (line ranges or fewer includes) or set a smaller roster in the project's athena.members setting. Nothing was sent to a model and no evidence was cut.");
}

#[test]
fn package_substitution_obeys_exclusion_quota_and_iq_admission() {
    for blocker in ["caller", "operator", "quota", "iq"] {
        let mut facts = package_facts();
        if blocker == "iq" {
            facts[3].scores.elo = 100.0;
        }
        let mut input = panel_input(facts.clone(), spec(2, 80.0, PanelDiversity::Family));
        input.roster = roster(&facts[..2]);
        input.package_tokens = Some(227_000);
        match blocker {
            "caller" => input.exclude_route_keys = vec!["p/replacement".into()],
            "operator" => input.operator_exclude_keys = vec!["p/replacement".into()],
            "quota" => {
                input.runtime_snapshot = Some(ModelRouteStateSnapshot {
                    now: 1.0,
                    by_route: HashMap::from([(
                        "p/replacement".into(),
                        RouteRuntimeState {
                            quota_exhausted_until: Some(20.0),
                            ..Default::default()
                        },
                    )]),
                    ..Default::default()
                })
            }
            _ => {}
        }
        assert_eq!(
            select_panel_with_policy(
                input,
                &DEFAULT_ROUTING_CONFIG,
                &ClassPanelPolicy {
                    top_k: 2,
                    by: PanelOrder::Eq,
                    extra: 0,
                    extra_by: None,
                    floor: Some(80),
                }
            )
            .unwrap()
            .shortfall
            .unwrap()
            .code,
            PanelShortfallCode::PackageUnmeetable,
            "{blocker}"
        );
    }
}

#[test]
fn a_fitting_package_preserves_pins_and_automatic_runner_ups_fit() {
    let facts = package_facts();
    let mut input = panel_input(facts.clone(), spec(2, 80.0, PanelDiversity::Family));
    input.roster = roster(&facts[..2]);
    input.package_tokens = Some(210_000);
    assert_eq!(select_panel(input).unwrap().members[0].route_key, "p/small");
    let mut automatic = n_pick_input(facts, spec(1, 80.0, PanelDiversity::Family));
    automatic.package_tokens = Some(227_000);
    let panel = select_panel(automatic).unwrap();
    assert!(panel
        .members
        .iter()
        .all(|member| member.route_key != "p/small"));
    assert!(panel
        .runner_up
        .iter()
        .all(|member| member.route_key != "p/small"));
}

#[test]
fn operator_exclusion_fences_automatic_seats_and_names_its_own_cause() {
    let facts = vec![
        fact("fenced", 1_800.0, Some("a")),
        fact("kept", 1_700.0, Some("b")),
        fact("rerouted", 1_650.0, Some("c")),
    ];
    let mut input = n_pick_input(facts, spec(3, 0.0, PanelDiversity::None));
    input.operator_exclude_keys = vec!["p/fenced".into()];
    // The caller's own exclusion of a DIFFERENT route must stay distinguishable
    // from the operator's: one is fixed by editing a config, the other by asking
    // for something else.
    input.exclude_route_keys = vec!["p/rerouted".into()];
    let panel = select_panel(input).unwrap();

    assert_eq!(
        panel
            .members
            .iter()
            .map(|member| member.route_key.as_str())
            .collect::<Vec<_>>(),
        vec!["p/kept"]
    );
    let causes = &panel
        .shortfall
        .expect("a fenced panel reports its shortfall")
        .causes;
    let cause_for = |model: &str| {
        causes
            .iter()
            .find(|cause| cause.model == model)
            .map(|cause| cause.reason)
    };
    assert_eq!(
        cause_for("p/fenced"),
        Some(PanelCauseReason::ExcludedByOperator)
    );
    assert_eq!(cause_for("p/rerouted"), Some(PanelCauseReason::Excluded));
}

#[test]
fn iq_floor_uses_the_request_elo_frontier() {
    let top = 1_838.0;
    let target = demand_bar(top, 70.0, DEFAULT_DEMAND_EXPONENT);
    let facts = vec![
        fact("frontier", top, Some("frontier")),
        fact("at-boundary", target, Some("a")),
        fact("just-below", target - 0.01, Some("b")),
    ];
    let selected = select_panel_with_policy(
        n_pick_input(facts, spec(3, 70.0, PanelDiversity::None)),
        &DEFAULT_ROUTING_CONFIG,
        &ClassPanelPolicy {
            top_k: 3,
            by: PanelOrder::Eq,
            extra: 0,
            extra_by: None,
            floor: Some(70),
        },
    )
    .unwrap();
    let routes: HashSet<_> = selected
        .members
        .iter()
        .map(|member| member.route_key.as_str())
        .collect();
    assert_eq!(routes, HashSet::from(["p/frontier", "p/at-boundary"]));
    assert_eq!(
        selected.shortfall.unwrap().code,
        PanelShortfallCode::IqFloorUnmeetable
    );

    let selected = select_panel_with_policy(
        n_pick_input(
            vec![
                fact("frontier", top, Some("a")),
                fact("too-low", top - 0.1, Some("b")),
            ],
            spec(2, 100.0, PanelDiversity::None),
        ),
        &DEFAULT_ROUTING_CONFIG,
        &ClassPanelPolicy {
            top_k: 2,
            by: PanelOrder::Eq,
            extra: 0,
            extra_by: None,
            floor: Some(100),
        },
    )
    .unwrap();
    assert_eq!(selected.members.len(), 1);
    assert_eq!(selected.members[0].route_key, "p/frontier");

    let no_floor = select_panel(panel_input(
        vec![fact("low-elo", 15.0, Some("a"))],
        spec(1, 0.0, PanelDiversity::None),
    ))
    .unwrap();
    assert_eq!(no_floor.members[0].route_key, "p/low-elo");
    assert!(no_floor.shortfall.is_none());
}

#[test]
fn admission_is_shared_not_reimplemented() {
    let mut excluded = fact("excluded", 60.0, Some("a"));
    excluded.supports_vision = Some(true);
    let unavailable = fact("unavailable", 60.0, Some("b"));
    let quota = fact("quota", 60.0, Some("c"));
    let mut incapable = fact("incapable", 60.0, Some("d"));
    incapable.supports_vision = Some(false);
    let winner = fact("winner", 60.0, Some("e"));
    let facts = vec![excluded, unavailable, quota, incapable, winner];
    let available: HashSet<String> = ["p/excluded", "p/quota", "p/incapable", "p/winner"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let snapshot = ModelRouteStateSnapshot {
        now: 10.0,
        by_route: HashMap::from([(
            "p/quota".into(),
            RouteRuntimeState {
                quota_exhausted_until: Some(20.0),
                ..Default::default()
            },
        )]),
        ..Default::default()
    };
    let capabilities = Some(RequiredCapabilities {
        vision: Some(true),
        min_context: None,
    });
    let singular = select_model_route(SelectModelRouteInput {
        target_agent: "mason".into(),
        target_is_head: false,
        requirements: ModelRequirements { iq: 50.0, eq: 10.0 },
        available_models: available.clone(),
        facts: facts.clone(),
        runtime_snapshot: Some(snapshot.clone()),
        exclude_route_keys: vec!["p/excluded".into()],
        operator_exclude_keys: Vec::new(),
        required_capabilities: capabilities,
        is_subscription_route: None,
        min_best_effort_adequacy: None,
        prefer_prompt_cache: false,
        providers_without_prompt_cache: HashSet::new(),
        providers_without_cache_write_reports: HashSet::new(),
    })
    .unwrap();
    assert_eq!(singular.route_key, "p/winner");
    let singular_rejections: HashMap<_, _> = singular
        .rejected
        .iter()
        .map(|rejected| (rejected.route_key.as_str(), rejected.reason.as_str()))
        .collect();
    assert_eq!(singular_rejections["p/excluded"], "excluded");
    assert_eq!(singular_rejections["p/unavailable"], "not_available");
    assert_eq!(singular_rejections["p/quota"], "quota_exhausted");
    assert_eq!(singular_rejections["p/incapable"], "missing_vision");

    let mut panel_roster = roster(&facts);
    panel_roster.push(PanelRosterEntry {
        model: "p/missing".into(),
        prompt_params: Map::new(),
    });
    let panel = select_panel(SelectPanelInput {
        package_tokens: None,
        selection: PanelSelection::Roster,
        panel_spec: spec(5, 0.0, PanelDiversity::None),
        roster: panel_roster,
        facts,
        runtime_snapshot: Some(snapshot),
        exclude_route_keys: vec!["p/excluded".into()],
        // Provider-wide operator fence: if the standing policy leaked into a pinned
        // roster it would empty this panel outright, not merely drop one seat.
        operator_exclude_keys: vec!["p".into()],
        required_capabilities: capabilities,
        available_models: available,
        availability_substrate: AvailabilitySubstrate::Host,
        availability_label: "host".to_string(),
        is_subscription_route: None,
    })
    .unwrap();
    let causes = &panel.shortfall.unwrap().causes;
    // Roster pins BYPASS the exclusion list by design (pins are the deliberate
    // way to seat reserve-listed models), so `p/excluded` must NOT appear as a
    // cause; the other admission gates still apply to pins.
    for (model, reason) in [
        ("p/unavailable", PanelCauseReason::Unavailable),
        ("p/quota", PanelCauseReason::QuotaExhausted),
        ("p/incapable", PanelCauseReason::Capability),
        ("p/missing", PanelCauseReason::MissingFact),
    ] {
        assert!(causes
            .iter()
            .any(|cause| cause.model == model && cause.reason == reason));
    }
    assert!(
        !causes.iter().any(|cause| matches!(
            cause.reason,
            PanelCauseReason::Excluded | PanelCauseReason::ExcludedByOperator
        )),
        "roster pins must bypass both the caller and operator exclusion lists"
    );
    assert!(
        panel.members.iter().any(|m| m.route_key == "p/excluded"),
        "the excluded-but-pinned model must be seated"
    );
    assert!(
        panel.members.iter().any(|m| m.route_key == "p/winner"),
        "an operator-excluded provider stays seatable through a deliberate pin"
    );

    let exhausted = vec![fact("only", 60.0, Some("a"))];
    let panel = select_panel(SelectPanelInput {
        package_tokens: None,
        selection: PanelSelection::Roster,
        panel_spec: spec(1, 0.0, PanelDiversity::None),
        roster: roster(&exhausted),
        available_models: availability(&exhausted),
        facts: exhausted,
        runtime_snapshot: Some(ModelRouteStateSnapshot {
            now: 10.0,
            by_route: HashMap::from([(
                "p/only".into(),
                RouteRuntimeState {
                    quota_exhausted_until: Some(20.0),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }),
        exclude_route_keys: Vec::new(),
        operator_exclude_keys: Vec::new(),
        required_capabilities: None,
        availability_substrate: AvailabilitySubstrate::Host,
        availability_label: "host".to_string(),
        is_subscription_route: None,
    })
    .unwrap();
    assert!(
        panel.members.is_empty(),
        "panel selection must not break glass"
    );
}

/// A rejection that says only "unavailable" cannot be acted on: whether the route
/// is unreachable for the sender or merely missing from the caller's host list
/// have different fixes. The cause names the list that was consulted.
#[test]
fn an_availability_rejection_names_the_substrate_whose_list_was_consulted() {
    // The caller names the list it consulted; core carries that label on every
    // availability rejection verbatim.
    let detail_for = |substrate: AvailabilitySubstrate, label: &str| {
        let facts = vec![fact("absent", 1_700.0, Some("a"))];
        let mut input = panel_input(facts, spec(1, 0.0, PanelDiversity::None));
        input.available_models = HashSet::new();
        input.availability_substrate = substrate;
        input.availability_label = label.to_string();
        let causes = select_panel(input)
            .unwrap()
            .shortfall
            .expect("an unavailable pin leaves the panel short")
            .causes;
        let cause = causes
            .into_iter()
            .find(|cause| cause.model == "p/absent")
            .expect("the unavailable pin is reported");
        assert_eq!(cause.reason, PanelCauseReason::Unavailable);
        cause.detail
    };

    assert_eq!(
        detail_for(AvailabilitySubstrate::Host, "host"),
        Some("host".to_string())
    );
    assert_eq!(
        detail_for(AvailabilitySubstrate::Broca, "runner reach"),
        Some("runner reach".to_string())
    );
    assert_eq!(
        detail_for(
            AvailabilitySubstrate::BrocaServedCatalog,
            "reachability unknown: judged against served catalog"
        ),
        Some("reachability unknown: judged against served catalog".to_string())
    );
}

/// A pinned seat whose send failed on quota in one round is asked about again
/// in the next with the wall recorded on its route. It must read as a quota
/// wall carrying the reset it is waiting for, on either substrate's list: a
/// seat reported `unavailable` sends the reader looking for a missing provider
/// when the fix is waiting for the window to reset.
#[test]
fn a_pinned_seat_walled_by_quota_reads_as_quota_with_its_reset() {
    for substrate in [
        AvailabilitySubstrate::Host,
        AvailabilitySubstrate::BrocaServedCatalog,
    ] {
        for (cooldown_until, quota_exhausted_until) in
            [(Some(61_000.0), None), (None, Some(61_000.0))]
        {
            let walled = vec![fact("walled", 1_700.0, Some("a"))];
            let mut input = panel_input(walled, spec(1, 0.0, PanelDiversity::None));
            input.availability_substrate = substrate;
            input.runtime_snapshot = Some(ModelRouteStateSnapshot {
                now: 1_000.0,
                by_route: HashMap::from([(
                    "p/walled".into(),
                    RouteRuntimeState {
                        cooldown_until,
                        quota_exhausted_until,
                        ..Default::default()
                    },
                )]),
                by_capacity: HashMap::from([(
                    "p/walled".into(),
                    RouteRuntimeState {
                        cooldown_until: cooldown_until.map(|_| 91_000.0),
                        quota_exhausted_until: quota_exhausted_until.map(|_| 91_000.0),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            });
            let panel = select_panel(input).unwrap();
            assert!(panel.members.is_empty(), "a walled pin does not sit");
            let cause = panel
                .shortfall
                .expect("the walled pin leaves the panel short")
                .causes
                .into_iter()
                .find(|cause| cause.model == "p/walled")
                .expect("the walled pin is reported");
            assert_eq!(
                cause.reason,
                PanelCauseReason::QuotaExhausted,
                "{substrate:?}"
            );
            assert_eq!(cause.reset_at_ms, Some(91_000), "{substrate:?}");
            assert_eq!(serde_json::to_value(&cause).unwrap()["reset_at_ms"], 91_000);
            let detail = cause.detail.expect("a wall names its reset");
            assert!(detail.contains("until_ms=91000"), "{substrate:?}: {detail}");
        }
    }
}

#[test]
fn reserved_headroom_panel_cause_publishes_structured_reset_without_prose_timestamp() {
    use prefrontal_routing_core::subscription::{QuotaAccount, SubscriptionPlan, SubscriptionPool};
    let account = |hours_to_reset| QuotaAccount {
        weekly_used: 99.0,
        weekly_remaining_fraction: 0.01,
        hours_to_reset,
        short_walled: false,
        short_windows: vec![],
        banked_resets: 0,
        banked_expires_at_ms: None,
        prepaid_credits: false,
    };
    let mut input = panel_input(
        vec![
            fact("sub", 1_700.0, Some("a")),
            fact("plain", 1_700.0, Some("b")),
        ],
        spec(2, 0.0, PanelDiversity::None),
    );
    input.is_subscription_route = Some(Box::new(|route| route.model_id == "sub"));
    input.runtime_snapshot = Some(ModelRouteStateSnapshot {
        now: 1791682778812.0,
        by_capacity: HashMap::from([(
            "p/sub".into(),
            RouteRuntimeState {
                subscription_pool: Some(std::sync::Arc::new(SubscriptionPool {
                    keep_free_percent: 20.0,
                    now_ms: 1791682778812,
                    plans: vec![SubscriptionPlan {
                        tier: None,
                        plan: None,
                        measured: None,
                        fee_usd: 0.0,
                        accounts: vec![account(2.0), account(1.0)],
                    }],
                    ..Default::default()
                })),
                ..Default::default()
            },
        )]),
        ..Default::default()
    });
    let panel = select_panel(input).unwrap();
    assert_eq!(panel.members.len(), 1);
    assert_eq!(panel.members[0].route_key, "p/plain");
    let cause = panel
        .shortfall
        .unwrap()
        .causes
        .into_iter()
        .find(|cause| cause.model == "p/sub")
        .unwrap();
    assert_eq!(cause.reason, PanelCauseReason::QuotaExhausted);
    assert_eq!(cause.reset_at_ms, Some(1791686378812));
    let wire = serde_json::to_value(cause).unwrap();
    assert_eq!(wire["reset_at_ms"], 1791686378812i64);
    assert_eq!(
        wire["detail"],
        "no live account has weekly quota above its keep-free margin"
    );
}

#[test]
fn family_diversity_groups_absent_lineage_with_other() {
    let facts = vec![
        fact("a1", 60.0, Some("a")),
        fact("a2", 59.0, Some("a")),
        fact("unlabeled", 58.0, None),
        fact("also-unlabeled", 57.0, None),
        fact("explicit-other", 56.0, Some("other")),
    ];
    let family = select_panel(n_pick_input(
        facts.clone(),
        spec(3, 0.0, PanelDiversity::Family),
    ))
    .unwrap();
    assert_eq!(family.members.len(), 2);
    assert_eq!(family.members[1].route_key, "p/unlabeled");
    assert_eq!(family.members[1].family.as_deref(), Some("other"));
    let shortfall = family.shortfall.unwrap();
    assert_eq!(shortfall.code, PanelShortfallCode::DiversityUnmeetable);
    assert!(shortfall
        .causes
        .iter()
        .any(|cause| cause.reason == PanelCauseReason::FamilyDuplicate));
    assert!(!shortfall
        .causes
        .iter()
        .any(|cause| cause.reason == PanelCauseReason::FamilyUnlabeled));
    let roster = select_panel(panel_input(facts, spec(5, 0.0, PanelDiversity::None))).unwrap();
    assert!(roster.members.iter().any(
        |member| member.route_key == "p/unlabeled" && member.family.as_deref() == Some("other")
    ));
}

#[test]
fn seated_members_and_runner_ups_keep_fact_output_caps() {
    let mut capped = fact("capped", 61.0, Some("a"));
    capped.max_output_tokens = Some(8_000);
    capped.context_window = Some(500_000.0);
    let mut uncapped = fact("uncapped", 60.0, Some("b"));
    uncapped.max_output_tokens = None;
    uncapped.context_window = None;

    let panel = select_panel(panel_input(
        vec![capped, uncapped],
        spec(1, 0.0, PanelDiversity::None),
    ))
    .unwrap();

    assert_eq!(panel.members[0].max_output_tokens, Some(8_000));
    assert_eq!(panel.runner_up[0].max_output_tokens, None);
    let serialized = serde_json::to_value(&panel).unwrap();
    assert_eq!(serialized["members"][0]["maxOutputTokens"], json!(8_000));
    assert!(serialized["runnerUp"][0].get("maxOutputTokens").is_none());
    // The context window rides with the seat so the consult can size its
    // package from the seated roster without a second catalog lookup.
    assert_eq!(panel.members[0].context_window, Some(500_000));
    assert_eq!(serialized["members"][0]["contextWindow"], json!(500_000));
    assert!(serialized["runnerUp"][0].get("contextWindow").is_none());
}

#[test]
fn ordering_temperature_and_promotion() {
    let mut high = fact("high", 61.0, Some("a"));
    high.cost_per_m_tok_out = Some(30.0);
    let mut fast_sub = fact("fast-sub", 60.0, Some("b"));
    fast_sub.scores.speed = 180.0;
    let mut slow_sub = fact("slow-sub", 60.0, Some("c"));
    slow_sub.scores.speed = 60.0;
    let facts = vec![slow_sub, high, fast_sub];
    let mut panel_spec = spec(2, 0.0, PanelDiversity::Family);
    panel_spec.temperature_spread = Some(vec![0.2]);
    let mut input = panel_input(facts, panel_spec);
    input.is_subscription_route = Some(Box::new(|route| route.model_id.ends_with("sub")));
    let panel = select_panel(input).unwrap();
    assert_eq!(panel.members[0].route_key, "p/high", "iq orders first");
    assert_eq!(panel.members[1].route_key, "p/fast-sub");
    assert_eq!(panel.members[0].temperature, Some(0.2));
    assert_eq!(
        panel.members[1].temperature, None,
        "short spreads right-pad absence"
    );
    assert_eq!(panel.runner_up[0].route_key, "p/slow-sub");

    let failed_temperature = panel.members[0].temperature;
    let surviving_families: HashSet<_> = panel.members[1..]
        .iter()
        .filter_map(|member| member.family.as_deref())
        .collect();
    let promoted = panel
        .runner_up
        .iter()
        .find(|spare| {
            spare
                .family
                .as_deref()
                .is_some_and(|family| !surviving_families.contains(family))
        })
        .unwrap();
    assert_eq!(promoted.route_key, "p/slow-sub");
    assert_eq!(
        failed_temperature,
        Some(0.2),
        "the promoted spare inherits the failed slot temperature"
    );

    let facts = vec![fact("one", 60.0, Some("a"))];
    let mut panel_spec = spec(1, 0.0, PanelDiversity::None);
    panel_spec.temperature_spread = Some(vec![0.1, 0.9]);
    let panel = select_panel(panel_input(facts, panel_spec)).unwrap();
    assert_eq!(panel.members[0].temperature, Some(0.1));
}

#[test]
fn shortfall_precedence_and_empty_success() {
    let unavailable = fact("unavailable", 60.0, Some("a"));
    let zero_admission = select_panel(SelectPanelInput {
        package_tokens: None,
        selection: PanelSelection::Roster,
        panel_spec: spec(1, 100.0, PanelDiversity::Family),
        roster: roster(std::slice::from_ref(&unavailable)),
        facts: vec![unavailable],
        available_models: HashSet::new(),
        availability_substrate: AvailabilitySubstrate::Host,
        availability_label: "host".to_string(),
        runtime_snapshot: None,
        exclude_route_keys: Vec::new(),
        operator_exclude_keys: Vec::new(),
        required_capabilities: None,
        is_subscription_route: None,
    })
    .unwrap();
    assert_eq!(
        zero_admission.shortfall.unwrap().code,
        PanelShortfallCode::SizeReduced,
        "zero admission outranks iq and diversity"
    );

    let mut below_input = n_pick_input(
        vec![fact("top", 100.0, Some("top")), fact("below", 40.0, None)],
        spec(2, 100.0, PanelDiversity::Family),
    );
    below_input.runtime_snapshot = Some(ModelRouteStateSnapshot {
        now: 1_000.0,
        by_route: HashMap::from([(
            "p/top".to_string(),
            RouteRuntimeState {
                quota_exhausted_until: Some(2_000.0),
                ..Default::default()
            },
        )]),
        ..Default::default()
    });
    let below = select_panel(below_input).unwrap();
    assert_eq!(
        below.shortfall.unwrap().code,
        PanelShortfallCode::DiversityUnmeetable,
        "the below-floor IQ top seat is admitted; lowering the floor adds no family"
    );
    let diversity = select_panel(n_pick_input(
        vec![fact("one", 60.0, Some("a"))],
        spec(2, 0.0, PanelDiversity::Family),
    ))
    .unwrap();
    assert_eq!(
        diversity.shortfall.unwrap().code,
        PanelShortfallCode::DiversityUnmeetable
    );
    // The same one-family single fact through a pinned ROSTER is not a diversity
    // shortfall: the caller pinned one model and gets one seat (size_reduced).
    let roster_single = select_panel(panel_input(
        vec![fact("one", 60.0, Some("a"))],
        spec(2, 0.0, PanelDiversity::Family),
    ))
    .unwrap();
    assert_eq!(
        roster_single.shortfall.unwrap().code,
        PanelShortfallCode::SizeReduced
    );
    let size = select_panel(panel_input(
        vec![fact("one", 60.0, Some("a"))],
        spec(2, 0.0, PanelDiversity::None),
    ))
    .unwrap();
    assert_eq!(
        size.shortfall.unwrap().code,
        PanelShortfallCode::SizeReduced
    );

    for invalid in [
        spec(0, 0.0, PanelDiversity::None),
        spec(9, 0.0, PanelDiversity::None),
        spec(1, 101.0, PanelDiversity::None),
    ] {
        assert!(matches!(
            select_panel(panel_input(Vec::new(), invalid)),
            Err(InvalidPanelRequest(_))
        ));
    }
    let mut invalid_temperature = spec(1, 0.0, PanelDiversity::None);
    invalid_temperature.temperature_spread = Some(vec![2.1]);
    assert!(select_panel(panel_input(Vec::new(), invalid_temperature)).is_err());

    let serialized = serde_json::to_value(
        select_panel(panel_input(Vec::new(), spec(1, 0.0, PanelDiversity::None))).unwrap(),
    )
    .unwrap();
    assert_eq!(serialized["members"], json!([]));
    assert!(!serialized["shortfall"]["causes"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn policy_eq_reuses_elo_floor_and_fact_prompt_defaults() {
    let top = 1_838.0;
    let target = demand_bar(top, 70.0, DEFAULT_DEMAND_EXPONENT);
    let mut at_boundary = fact("at-boundary", target, Some("a"));
    at_boundary.prompt_defaults = Some(ModelPromptDefaults {
        variant: Some("high".into()),
        temperature: Some(0.4),
        reasoning_effort: None,
    });
    let selected = select_panel_with_policy(
        n_pick_input(
            vec![
                fact("frontier", top, Some("frontier")),
                at_boundary,
                fact("below", target - 0.01, Some("b")),
            ],
            spec(3, 70.0, PanelDiversity::None),
        ),
        &DEFAULT_ROUTING_CONFIG,
        &ClassPanelPolicy {
            top_k: 3,
            by: PanelOrder::Eq,
            extra: 0,
            extra_by: None,
            floor: Some(70),
        },
    )
    .unwrap();

    assert_eq!(selected.members.len(), 2);
    let boundary = selected
        .members
        .iter()
        .find(|member| member.route_key == "p/at-boundary")
        .unwrap();
    assert_eq!(boundary.prompt_params["variant"], "high");
    assert_eq!(boundary.prompt_params["temperature"], 0.4);
    assert_eq!(
        selected.shortfall.unwrap().code,
        PanelShortfallCode::IqFloorUnmeetable
    );
}

#[test]
fn routes_with_the_same_capacity_key_collapse_without_static_metadata() {
    let base = fact("shared", 60.0, Some("family"));
    let mut variant = base.clone();
    variant.variant = Some("high".into());
    let mut panel_spec = spec(2, 0.0, PanelDiversity::None);
    panel_spec.class = PanelClass::Diagnose;

    let selected = select_panel(n_pick_input(vec![variant, base], panel_spec)).unwrap();

    assert_eq!(selected.members.len(), 1);
    assert_eq!(selected.members[0].route_key, "p/shared");
    assert_eq!(selected.policy.unwrap().sibling_collapses.len(), 1);
}

#[test]
fn curated_seed_contains_no_fast_model_ids() {
    assert!(model_facts_seed()
        .iter()
        .all(|fact| !fact.model_id.ends_with("-fast")));
}

#[test]
fn n_pick_needs_no_package_size_inputs() {
    // The package no longer steers seating: the consult sizes its package from
    // the seated roster instead, so an n_pick request carries no estimate.
    let facts = vec![fact("one", 60.0, Some("a"))];
    let selected = select_panel(n_pick_input(facts, spec(1, 0.0, PanelDiversity::None))).unwrap();
    assert_eq!(selected.members.len(), 1);
    let serialized = serde_json::to_value(selected.policy.unwrap()).unwrap();
    assert!(serialized.get("proDowngradedForSize").is_none());
}

#[test]
fn n_pick_applies_shared_availability_exclusion_and_capability_admission() {
    let excluded = fact("excluded", 60.0, Some("a"));
    let unavailable = fact("unavailable", 60.0, Some("b"));
    let mut incapable = fact("incapable", 60.0, Some("c"));
    incapable.supports_vision = Some(false);
    let winner = fact("winner", 60.0, Some("d"));
    let facts = vec![excluded, unavailable, incapable, winner];
    let mut panel_spec = spec(4, 0.0, PanelDiversity::None);
    panel_spec.class = PanelClass::Diagnose;
    let mut input = n_pick_input(facts, panel_spec);
    input.exclude_route_keys = vec!["p/excluded".into()];
    input.available_models.remove("p/unavailable");
    input.required_capabilities = Some(RequiredCapabilities {
        vision: Some(true),
        min_context: None,
    });

    let selected = select_panel(input).unwrap();
    assert_eq!(selected.members[0].route_key, "p/winner");
    let causes = selected.shortfall.unwrap().causes;
    for (model, reason) in [
        ("p/excluded", PanelCauseReason::Excluded),
        ("p/unavailable", PanelCauseReason::Unavailable),
        ("p/incapable", PanelCauseReason::Capability),
    ] {
        assert!(causes
            .iter()
            .any(|cause| cause.model == model && cause.reason == reason));
    }
}

#[test]
fn roster_mode_is_never_inferred_from_roster_contents() {
    let facts = vec![fact("available", 60.0, Some("a"))];
    let mut input = panel_input(facts, spec(1, 0.0, PanelDiversity::None));
    input.roster.clear();
    let selected = select_panel(input).unwrap();

    assert!(selected.members.is_empty());
    assert!(selected.policy.is_none());
    assert_eq!(
        selected.shortfall.unwrap().code,
        PanelShortfallCode::SizeReduced
    );
}

#[test]
fn roster_mode_seats_pins_below_the_iq_floor() {
    // The floor governs n_pick admission only. A pinned roster is the operator's
    // statement of who sits: the live incident (2026-08-06, MC audit consult
    // ct_...3ab81b4661a0) had a pinned 4-seat audit silently halved to sol+opus
    // because the floor bar (85 -> ~55.05) sat above kimi/qwen's 55.0 facts.
    let facts = vec![
        fact("strong-a", 75.0, Some("fam-a")),
        fact("strong-b", 74.0, Some("fam-b")),
        fact("pinned-low-c", 40.0, Some("fam-c")),
        fact("pinned-low-d", 39.0, Some("fam-d")),
    ];
    let output = select_panel(panel_input(facts, spec(4, 99.0, PanelDiversity::Family))).unwrap();
    assert_eq!(
        output.members.len(),
        4,
        "a pinned roster seats every pin regardless of the iq floor: {:?}",
        output.shortfall
    );
    assert!(
        output.shortfall.is_none(),
        "full seating means no shortfall record (a floor-rejection would surface here)"
    );
}

#[test]
fn roster_mode_seats_same_family_pins_without_diversity_ejection() {
    let facts = vec![
        fact("gpt-a", 60.0, Some("openai-gpt")),
        fact("claude-x", 60.0, Some("anthropic")),
        fact("claude-y", 60.0, Some("anthropic")),
        fact("grok-z", 60.0, Some("xai-grok")),
    ];
    let output = select_panel(panel_input(facts, spec(4, 70.0, PanelDiversity::Family))).unwrap();
    assert_eq!(
        output.members.len(),
        4,
        "an explicit 4-model roster seats all 4 pins even with two anthropic-family members: {:?}",
        output.shortfall
    );
    assert!(
        output.shortfall.is_none(),
        "no diversity shortfall for pins"
    );
}
