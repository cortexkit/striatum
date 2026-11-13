use super::*;

#[test]
fn package_message_lists_closest_fits_first_and_omits_below_floor_routes() {
    let facts = [
        ("smallest", 2000.0, 176_000.0),
        ("small", 1990.0, 180_000.0),
        ("medium", 1980.0, 184_000.0),
        ("larger", 1970.0, 188_000.0),
        ("nearly-fits", 1960.0, 572_000.0),
        ("weak", 593.0, 1_000_000.0),
    ]
    .into_iter()
    .map(|(model, elo, window)| {
        let mut row = fact(model, elo, Some(model));
        row.context_window = Some(window);
        row.max_output_tokens = Some(64_000);
        row
    })
    .collect();
    let mut input = n_pick_input(facts, spec(2, 0.0, PanelDiversity::Family));
    input.package_tokens = Some(500_000);
    let panel = select_panel_with_policy(
        input,
        &DEFAULT_ROUTING_CONFIG,
        &ClassPanelPolicy {
            top_k: 2,
            by: PanelOrder::Iq,
            extra: 0,
            extra_by: None,
            floor: Some(80),
        },
    )
    .unwrap();
    let shortfall = panel.shortfall.unwrap();
    assert_eq!(shortfall.code, PanelShortfallCode::PackageUnmeetable);
    assert!(shortfall.detail.starts_with("The evidence package is 500,000 tokens; none of the 2 panel seats can read that much. p/nearly-fits can read up to 246,000 tokens. p/larger can read up to 54,000 tokens. p/medium can read up to 52,000 tokens. p/small can read up to 50,000 tokens. 1 more seats are too small."), "{}", shortfall.detail);
    assert!(!shortfall.detail.contains("p/weak"), "{}", shortfall.detail);
    // Typed causes retain the package rejection even when the prose omits a
    // route that would still fail the floor after shrinking the evidence.
    assert!(shortfall.causes.iter().any(|cause| {
        cause.model == "p/weak"
            && cause.reason == PanelCauseReason::PackageTooLarge
            && cause
                .detail
                .as_ref()
                .unwrap()
                .contains("ceiling 460000 tokens")
    }));
}

fn incident_facts() -> Vec<ModelFact> {
    [
        ("opus", 2315.0, "claude", 1_000_000.0, 128_000),
        ("astra", 2067.0, "gpt", 572_000.0, 64_000),
        ("argon", 1852.0, "gemini", 1_048_576.0, 65_536),
    ]
    .into_iter()
    .map(|(model, iq, family, window, output)| {
        let mut row = fact(model, iq, Some(family));
        row.context_window = Some(window);
        row.max_output_tokens = Some(output);
        row
    })
    .collect()
}

fn select_spec(facts: Vec<ModelFact>, tokens: usize) -> striatum_core::SelectedPanel {
    let mut panel_spec = spec(4, 80.0, PanelDiversity::Family);
    panel_spec.class = PanelClass::Spec;
    let mut input = n_pick_input(facts, panel_spec);
    input.package_tokens = Some(tokens);
    select_panel(input).unwrap()
}

#[test]
fn spec_extra_floor_shortfall_is_not_mislabeled_by_an_irrelevant_package_rejection() {
    for tokens in [85_068, 80_982, 46_960] {
        let mut facts = incident_facts();
        let mut fourth = fact("fourth", 1703.0, Some("kimi"));
        fourth.context_window = Some(1_048_576.0);
        facts.push(fourth);
        facts.push(fact("too-small-and-below-floor", 1000.0, Some("small")));
        let panel = select_spec(facts, tokens);
        assert_eq!(panel.members.len(), 1);
        let shortfall = panel.shortfall.unwrap();
        assert_eq!(shortfall.code, PanelShortfallCode::IqFloorUnmeetable);
        assert!(shortfall.causes.iter().any(|cause| {
            cause.model == "p/fourth" && cause.reason == PanelCauseReason::BelowIqFloor
        }));
        assert!(shortfall.causes.iter().any(|cause| {
            cause.model == "p/too-small-and-below-floor"
                && cause.reason == PanelCauseReason::PackageTooLarge
                && cause
                    .detail
                    .as_ref()
                    .unwrap()
                    .contains("ceiling 26000 tokens")
        }));
    }
}

#[test]
fn spec_floor_shortfall_is_not_mislabeled_by_a_small_duplicate_family() {
    for tokens in [85_068, 80_982, 46_960] {
        let mut facts = incident_facts();
        facts.push(fact("too-small-sibling", 2300.0, Some("claude")));
        let panel = select_spec(facts, tokens);
        assert_eq!(panel.members.len(), 1);
        assert_eq!(
            panel.shortfall.unwrap().code,
            PanelShortfallCode::IqFloorUnmeetable
        );
    }
}

#[test]
fn spec_package_shortfall_still_names_a_small_eligible_fourth_family() {
    for tokens in [85_068, 80_982, 46_960] {
        let mut facts = incident_facts();
        facts[1].scores.elo = 2267.0;
        facts[2].scores.elo = 2252.0;
        facts.push(fact("small-fourth", 2200.0, Some("fourth")));
        let panel = select_spec(facts, tokens);
        assert_eq!(panel.members.len(), 3);
        assert_eq!(
            panel.shortfall.unwrap().code,
            PanelShortfallCode::PackageUnmeetable
        );
    }
}
