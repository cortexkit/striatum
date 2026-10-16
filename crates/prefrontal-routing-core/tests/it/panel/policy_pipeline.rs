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
) -> prefrontal_routing_core::SelectedPanel {
    let mut input = n_pick_input(facts, spec(8, 100.0, PanelDiversity::None));
    input.panel_spec.class = class;
    select_panel_with_policy(input, &DEFAULT_ROUTING_CONFIG, &policy).unwrap()
}

fn keys(panel: &prefrontal_routing_core::SelectedPanel) -> Vec<&str> {
    panel
        .members
        .iter()
        .map(|member| member.route_key.as_str())
        .collect()
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
    let config = prefrontal_routing_core::RoutingConfig {
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
    let panel = prefrontal_routing_core::select_panel_with_config(input, &config).unwrap();
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
fn diagnose_iq_topk_can_sit_below_floor_bar_and_prefers_pro() {
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
        assert_eq!(keys(&panel), ["p/frontier", "p/base-pro"]);
        assert!(
            panel.shortfall.is_none(),
            "the bar must not refuse a full IQ panel"
        );
        assert!(100.0 < demand_bar(2000.0, 80.0, DEFAULT_DEMAND_EXPONENT));
    }
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
fn unlabeled_families_are_dropped_even_without_classifier_diversity() {
    let panel = choose(
        vec![
            fact("unlabeled", 2000.0, None),
            fact("labeled", 100.0, Some("a")),
        ],
        PanelClass::Lab,
        policy(2, PanelOrder::Iq, None),
    );
    assert_eq!(keys(&panel), ["p/labeled"]);
    let shortfall = panel.shortfall.unwrap();
    assert!(shortfall
        .causes
        .iter()
        .any(|cause| cause.reason == PanelCauseReason::FamilyUnlabeled));
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
