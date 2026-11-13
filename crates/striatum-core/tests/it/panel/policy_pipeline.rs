use super::*;

fn policy(top_k: usize, by: PanelOrder, floor: Option<u8>) -> ClassPanelPolicy {
    ClassPanelPolicy {
        top_k,
        by,
        floor,
        extra: 0,
        extra_by: None,
    }
}

fn choose(
    facts: Vec<ModelFact>,
    class: PanelClass,
    policy: ClassPanelPolicy,
) -> striatum_core::SelectedPanel {
    let mut input = n_pick_input(facts, spec(8, 100.0, PanelDiversity::None));
    input.panel_spec.class = class;
    select_panel_with_policy(input, &DEFAULT_ROUTING_CONFIG, &policy).unwrap()
}

fn keys(panel: &striatum_core::SelectedPanel) -> Vec<&str> {
    panel
        .members
        .iter()
        .map(|member| member.route_key.as_str())
        .collect()
}

#[test]
fn panel_shortfall_reports_candidate_funnel_not_catalog_noise() {
    let facts = vec![
        fact("first", 1900.0, Some("first")),
        fact("second", 1880.0, Some("second")),
        fact("third", 1870.0, Some("third")),
    ];
    let select = |facts: Vec<ModelFact>| {
        let mut input = n_pick_input(facts, spec(3, 80.0, PanelDiversity::Family));
        input.available_models.remove("p/third");
        input.required_capabilities = Some(RequiredCapabilities {
            vision: Some(true),
            ..Default::default()
        });
        input
            .available_models
            .retain(|key| !key.contains("unreachable") && !key.contains("unscored"));
        select_panel_with_policy(
            input,
            &DEFAULT_ROUTING_CONFIG,
            &ClassPanelPolicy {
                extra: 1,
                extra_by: Some(PanelExtraOrder::Iq),
                ..policy(2, PanelOrder::Iq, Some(80))
            },
        )
        .unwrap()
    };
    let baseline = select(facts.clone());
    let mut noisy = facts;
    for index in 0..150 {
        let mut unscored = fact(&format!("unscored-{index}"), 0.0, None);
        unscored.score_provenance = ScoreProvenance::Unscored;
        noisy.push(unscored);
        let mut non_text = fact(&format!("non-text-{index}"), 100.0, None);
        non_text.supports_vision = Some(false);
        noisy.push(non_text);
        noisy.push(fact(&format!("unreachable-{index}"), 100.0, None));
        noisy.push(fact(&format!("low-iq-{index}"), 100.0, Some("noise")));
    }
    let noisy = select(noisy);
    assert_eq!(keys(&noisy), keys(&baseline));
    assert_eq!(
        baseline.shortfall.as_ref().unwrap().code,
        PanelShortfallCode::DiversityUnmeetable
    );
    // The low-IQ routes add another reachable family, but only if the IQ floor
    // is relaxed. Their rejection evidence changes the cause, not the seats.
    assert_eq!(
        noisy.shortfall.as_ref().unwrap().code,
        PanelShortfallCode::IqFloorUnmeetable
    );
    let base = serde_json::to_value(&baseline).unwrap();
    let value = serde_json::to_value(&noisy).unwrap();
    let funnel = &value["shortfall"]["funnel"];
    assert_eq!(base["shortfall"]["funnel"]["catalog"], 3);
    assert_eq!(funnel["catalog"], 603);
    assert_eq!(funnel["scored"], 453);
    assert_eq!(funnel["operatorAdmitted"], 453);
    assert_eq!(funnel["callerAdmitted"], 453);
    assert_eq!(funnel["reachable"], 302);
    assert_eq!(funnel["capabilityEligible"], 152);
    assert_eq!(funnel["quotaEligible"], 152);
    assert_eq!(funnel["packageFit"], 152);
    assert_eq!(funnel["adequacyEligible"], 2);
    assert_eq!(funnel["distinctFamilies"], 2);
    assert_eq!(funnel["initiallyAdmitted"], 2);
    assert_eq!(value["policy"]["funnel"], *funnel);
    let mut reasons = std::collections::BTreeMap::<String, usize>::new();
    for cause in value["shortfall"]["causes"].as_array().unwrap() {
        *reasons
            .entry(cause["reason"].as_str().unwrap().to_string())
            .or_default() += 1;
    }
    assert_eq!(
        reasons,
        std::collections::BTreeMap::from([
            ("unscored_awaiting_placement".into(), 150),
            ("unavailable".into(), 151),
            ("capability".into(), 150),
            ("below_iq_floor".into(), 150),
        ])
    );
}

#[test]
fn small_window_route_is_package_rejected_not_unavailable() {
    let mut small = fact("small-window", 1900.0, Some("third"));
    small.context_window = Some(256_000.0);
    small.max_output_tokens = Some(64_000);
    let mut input = n_pick_input(vec![small], spec(1, 80.0, PanelDiversity::Family));
    input.package_tokens = Some(179_283);
    let panel = select_panel_with_policy(
        input,
        &DEFAULT_ROUTING_CONFIG,
        &policy(1, PanelOrder::Iq, Some(80)),
    )
    .unwrap();
    let shortfall = panel.shortfall.unwrap();
    assert_eq!(shortfall.code, PanelShortfallCode::PackageUnmeetable);
    assert_eq!(shortfall.causes.len(), 1);
    assert_eq!(
        shortfall.causes[0].reason,
        PanelCauseReason::PackageTooLarge
    );
    let detail = shortfall.causes[0].detail.as_deref().unwrap();
    for expected in [
        "package 179283 tokens",
        "window 256000",
        "output reserve 64000",
        "prompt reserve 16000",
        "ceiling 88000",
    ] {
        assert!(detail.contains(expected), "missing {expected}: {detail}");
    }
}

#[test]
fn builtin_defaults_and_lab_wire_class() {
    use PanelClass::*;
    use PanelExtraOrder as Extra;
    use PanelOrder as By;
    assert_eq!(serde_json::from_str::<PanelClass>("\"lab\"").unwrap(), Lab);
    for (class, top_k, by, extra, extra_by, floor) in [
        (Audit, 2, By::Iq, 1, Some(Extra::Random), Some(80)),
        (Spec, 3, By::Iq, 1, Some(Extra::Random), Some(80)),
        (Diagnose, 3, By::Iq, 0, None, Some(80)),
        (DesignReview, 3, By::Eq, 0, None, None),
        (Evaluate, 2, By::Iq, 1, Some(Extra::Eq), None),
        (Explain, 1, By::Cost, 0, None, Some(50)),
        (Lab, 4, By::Iq, 0, None, None),
    ] {
        assert_eq!(
            ClassPanelPolicy::default_for_class(class),
            ClassPanelPolicy {
                top_k,
                by,
                extra,
                extra_by,
                floor
            }
        );
        let mut input = n_pick_input(
            (0..8)
                .map(|i| fact(&format!("m{i}"), 1800.0, Some(&format!("f{i}"))))
                .collect(),
            spec(1, 0.0, PanelDiversity::None),
        );
        input.panel_spec.class = class;
        let panel = select_panel(input).unwrap();
        assert_eq!(panel.members.len(), top_k + extra, "{class:?}");
        assert_eq!(panel.policy.unwrap().selection, "class_policy");
    }
}

#[test]
fn audit_override_is_whole_and_seats_exactly_one() {
    let panel = choose(
        vec![fact("top", 1800.0, Some("a")), fact("low", 10.0, Some("b"))],
        PanelClass::Audit,
        policy(1, PanelOrder::Iq, None),
    );
    assert_eq!(keys(&panel), ["p/top"]);
    assert!(panel.shortfall.is_none());
    assert_eq!(panel.runner_up[0].route_key, "p/low", "no inherited floor");
}

#[test]
fn explain_cost_seat_clears_inclusive_demand_bar() {
    let top = 2000.0;
    let config = striatum_core::RoutingConfig {
        demand_exponent: 0.7,
        ..DEFAULT_ROUTING_CONFIG
    };
    let bar = demand_bar(top, 50.0, config.demand_exponent);
    let mut at = fact("at", bar, Some("at"));
    at.cost_per_m_tok_out = Some(10.0);
    let mut below = fact("below", bar - 0.001, Some("below"));
    below.cost_per_m_tok_out = Some(0.01);
    let mut frontier = fact("frontier", top, Some("frontier"));
    frontier.cost_per_m_tok_out = Some(100.0);
    let mut input = n_pick_input(
        vec![below, at, frontier],
        spec(8, 0.0, PanelDiversity::None),
    );
    input.panel_spec.class = PanelClass::Explain;
    let panel = striatum_core::select_panel_with_config(input, &config).unwrap();
    assert_eq!(keys(&panel), ["p/at"]);
    assert_eq!(
        panel
            .runner_up
            .iter()
            .map(|spare| spare.route_key.as_str())
            .collect::<Vec<_>>(),
        ["p/frontier"]
    );
}

#[test]
fn iq_order_uses_economics_then_lineage_and_skips_duplicate_families() {
    let mut expensive = fact("expensive", 1800.0, Some("a"));
    expensive.cost_per_m_tok_out = Some(100.0);
    let panel = choose(
        vec![
            expensive,
            fact("z-name", 1800.0, Some("a")),
            fact("a-name", 1800.0, Some("z")),
            fact("lower", 1700.0, Some("b")),
        ],
        PanelClass::Audit,
        policy(3, PanelOrder::Iq, None),
    );
    assert_eq!(keys(&panel), ["p/z-name", "p/a-name", "p/lower"]);
    assert_eq!(panel.runner_up[0].route_key, "p/expensive");
}

#[test]
fn eq_order_descends_then_uses_iq_without_mixed_ordering() {
    let mut high_eq = fact("high-eq", 100.0, Some("a"));
    high_eq.scores.eq = 2000.0;
    let mut tied_lower_iq = fact("tied", 1500.0, Some("b"));
    tied_lower_iq.scores.eq = 1900.0;
    let mut tied_higher_iq = fact("tied-top", 1700.0, Some("c"));
    tied_higher_iq.scores.eq = 1900.0;
    let panel = choose(
        vec![
            fact("iq-top", 2000.0, Some("d")),
            tied_lower_iq,
            tied_higher_iq,
            high_eq,
        ],
        PanelClass::DesignReview,
        policy(3, PanelOrder::Eq, None),
    );
    assert_eq!(keys(&panel), ["p/high-eq", "p/tied-top", "p/tied"]);
}

#[test]
fn eq_topk_filters_by_policy_bar() {
    let mut low = fact("low", 100.0, Some("low"));
    low.scores.eq = 3000.0;
    let panel = choose(
        vec![low, fact("top", 2000.0, Some("top"))],
        PanelClass::DesignReview,
        policy(1, PanelOrder::Eq, Some(80)),
    );
    assert_eq!(keys(&panel), ["p/top"]);
}

#[test]
fn iq_topk_floor_rejects_a_below_bar_preferred_pro_sibling() {
    for class in [PanelClass::Audit, PanelClass::Diagnose, PanelClass::Spec] {
        let panel = choose(
            vec![
                fact("frontier", 2000.0, Some("a")),
                fact("base", 1900.0, Some("b")),
                fact("base-pro", 100.0, Some("b")),
            ],
            class,
            policy(2, PanelOrder::Iq, Some(80)),
        );
        assert_eq!(keys(&panel), ["p/frontier"]);
        let funnel = panel.policy.as_ref().unwrap().funnel.as_ref().unwrap();
        assert_eq!(funnel.adequacy_eligible, 1);
        assert_eq!(funnel.distinct_families, 1);
        assert_eq!(
            panel.shortfall.unwrap().code,
            PanelShortfallCode::IqFloorUnmeetable
        );
        assert!(100.0 < demand_bar(2000.0, 80.0, DEFAULT_DEMAND_EXPONENT));
    }
}

#[test]
fn iq_ranked_top_seats_obey_the_class_floor() {
    let panel = choose(
        vec![
            fact("frontier", 2000.0, Some("a")),
            fact("just-below", 1999.99, Some("b")),
        ],
        PanelClass::Diagnose,
        policy(2, PanelOrder::Iq, Some(100)),
    );
    assert_eq!(keys(&panel), ["p/frontier"]);
    assert!(panel.runner_up.is_empty());
    let shortfall = panel.shortfall.unwrap();
    assert_eq!(shortfall.code, PanelShortfallCode::IqFloorUnmeetable);
    assert!(shortfall.causes.iter().any(|cause| {
        cause.model == "p/just-below" && cause.reason == PanelCauseReason::BelowIqFloor
    }));
}

#[test]
fn walled_strong_family_leaves_a_top_seat_empty_instead_of_seating_a_weak_model() {
    for wall in ["quota", "excluded", "unavailable", "package"] {
        let mut facts = vec![
            fact("frontier", 2000.0, Some("a")),
            fact("strong", 1950.0, Some("b")),
            fact("weak", 593.0, Some("c")),
        ];
        for row in &mut facts {
            if row.model_id != "strong" {
                row.context_window = Some(1_000_000.0);
            }
        }
        let mut input = n_pick_input(facts, spec(2, 0.0, PanelDiversity::Family));
        match wall {
            "quota" => {
                input.runtime_snapshot = Some(ModelRouteStateSnapshot {
                    now: 1000.0,
                    by_route: HashMap::from([(
                        "p/strong".into(),
                        RouteRuntimeState {
                            quota_exhausted_until: Some(2000.0),
                            ..Default::default()
                        },
                    )]),
                    ..Default::default()
                })
            }
            "excluded" => input.operator_exclude_keys.push("p/strong".into()),
            "unavailable" => {
                input.available_models.remove("p/strong");
            }
            "package" => input.package_tokens = Some(100_000),
            _ => unreachable!(),
        }
        let panel = select_panel_with_policy(
            input,
            &DEFAULT_ROUTING_CONFIG,
            &policy(2, PanelOrder::Iq, Some(80)),
        )
        .unwrap();
        assert_eq!(keys(&panel), ["p/frontier"], "{wall}");
        assert!(panel.runner_up.iter().all(|row| row.route_key != "p/weak"));
        let shortfall = panel.shortfall.unwrap();
        assert!(
            shortfall.causes.iter().any(|cause| {
                cause.model == "p/weak" && cause.reason == PanelCauseReason::BelowIqFloor
            }),
            "{wall}: {shortfall:?}"
        );
    }
}

#[test]
fn iq_ranked_top_seats_without_a_floor_keep_weak_distinct_families() {
    let panel = choose(
        vec![
            fact("frontier", 2000.0, Some("a")),
            fact("weak", 593.0, Some("b")),
        ],
        PanelClass::Evaluate,
        policy(2, PanelOrder::Iq, None),
    );
    assert_eq!(keys(&panel), ["p/frontier", "p/weak"]);
    assert!(panel.shortfall.is_none());
}

#[test]
fn sibling_pro_preference_requires_class_and_policy_floor() {
    for (class, floor) in [
        (PanelClass::Audit, Some(79)),
        (PanelClass::Diagnose, None),
        (PanelClass::Spec, Some(0)),
        (PanelClass::Evaluate, Some(80)),
    ] {
        let panel = choose(
            vec![
                fact("base", 1800.0, Some("a")),
                fact("base-pro", 1900.0, Some("a")),
            ],
            class,
            policy(1, PanelOrder::Iq, floor),
        );
        assert_eq!(keys(&panel), ["p/base"]);
    }
}

#[test]
fn absent_lineage_shares_one_other_seat_without_classifier_diversity() {
    let panel = choose(
        vec![
            fact("unlabeled", 2000.0, None),
            fact("also-unlabeled", 1900.0, None),
            fact("labeled", 100.0, Some("a")),
        ],
        PanelClass::Lab,
        policy(2, PanelOrder::Iq, None),
    );
    assert_eq!(keys(&panel), ["p/unlabeled", "p/labeled"]);
    assert_eq!(panel.members[0].family.as_deref(), Some("other"));
    assert!(panel.shortfall.is_none());
}

#[test]
fn package_ceiling_precedes_sibling_collapse() {
    let mut base = fact("base", 1800.0, Some("a"));
    base.context_window = Some(1_000_000.0);
    let mut pro = fact("base-pro", 1900.0, Some("a"));
    pro.context_window = Some(50_000.0);
    let mut input = n_pick_input(vec![base, pro], spec(1, 0.0, PanelDiversity::None));
    input.package_tokens = Some(227_000);
    let panel = select_panel_with_policy(
        input,
        &DEFAULT_ROUTING_CONFIG,
        &policy(1, PanelOrder::Iq, Some(80)),
    )
    .unwrap();
    assert_eq!(keys(&panel), ["p/base"]);
    assert!(panel.policy.unwrap().sibling_collapses.is_empty());
}

#[test]
fn extra_floor_applies_to_retained_pro_not_its_base() {
    let mut p = policy(1, PanelOrder::Iq, Some(80));
    p.extra = 1;
    p.extra_by = Some(PanelExtraOrder::Iq);
    let panel = choose(
        vec![
            fact("frontier", 2000.0, Some("a")),
            fact("base", 1900.0, Some("b")),
            fact("base-pro", 100.0, Some("b")),
        ],
        PanelClass::Audit,
        p,
    );
    assert_eq!(keys(&panel), ["p/frontier"]);
    assert!(
        panel
            .shortfall
            .unwrap()
            .causes
            .iter()
            .any(|cause| cause.model == "p/base-pro"
                && cause.reason == PanelCauseReason::BelowIqFloor)
    );
}

#[test]
fn policy_width_ignores_classifier_fields_and_assigns_slot_temperatures() {
    let mut p = policy(1, PanelOrder::Iq, None);
    p.extra = 1;
    p.extra_by = Some(PanelExtraOrder::Eq);
    let mut eq = fact("eq", 100.0, Some("b"));
    eq.scores.eq = 3000.0;
    let mut input = n_pick_input(
        vec![
            fact("iq", 2000.0, Some("a")),
            eq,
            fact("other", 1900.0, Some("c")),
        ],
        spec(0, f64::NAN, PanelDiversity::None),
    );
    input.panel_spec.temperature_spread = Some(vec![0.1, 0.9]);
    let panel = select_panel_with_policy(input, &DEFAULT_ROUTING_CONFIG, &p).unwrap();
    assert_eq!(keys(&panel), ["p/iq", "p/eq"]);
    assert_eq!(
        panel
            .members
            .iter()
            .map(|seat| seat.temperature.unwrap())
            .collect::<Vec<_>>(),
        [0.1, 0.9]
    );
    assert!(panel.shortfall.is_none());
}

#[test]
fn short_panel_has_a_cause_for_each_missing_policy_seat() {
    let panel = choose(
        vec![fact("only", 1800.0, Some("a"))],
        PanelClass::Lab,
        policy(4, PanelOrder::Iq, None),
    );
    assert_eq!(keys(&panel), ["p/only"]);
    let shortfall = panel.shortfall.unwrap();
    assert_eq!(shortfall.code, PanelShortfallCode::DiversityUnmeetable);
    assert_eq!(shortfall.causes.len(), 3);
    assert!(shortfall
        .causes
        .iter()
        .all(|cause| cause.reason == PanelCauseReason::Unavailable));
}
