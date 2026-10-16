use super::*;
use prefrontal_routing_core::{
    select_panel_with_policy_context, PanelPhase, PanelPins, PanelRunnerUp, PanelSelectionContext,
    PanelSynthesizerPin, SelectedPanel,
};

fn choose(
    facts: Vec<ModelFact>,
    class: PanelClass,
    policy: ClassPanelPolicy,
    context: &PanelSelectionContext,
) -> SelectedPanel {
    let mut input = n_pick_input(facts, spec(1, 0.0, PanelDiversity::None));
    input.panel_spec.class = class;
    select_panel_with_policy_context(input, &DEFAULT_ROUTING_CONFIG, &policy, context).unwrap()
}

fn random_policy(extra: usize) -> ClassPanelPolicy {
    ClassPanelPolicy {
        top_k: 1,
        by: PanelOrder::Iq,
        extra,
        extra_by: Some(PanelExtraOrder::Random),
        floor: None,
    }
}

fn golden_facts() -> Vec<ModelFact> {
    vec![
        fact("top", 2100.0, Some("top")),
        fact("e", 1800.0, Some("e")),
        fact("c", 1800.0, Some("c")),
        fact("a", 1800.0, Some("a")),
        fact("d", 1800.0, Some("d")),
        fact("b", 1800.0, Some("b")),
    ]
}

fn extras(panel: &SelectedPanel) -> Vec<&str> {
    panel.members[1..]
        .iter()
        .map(|member| member.route_key.as_str())
        .collect()
}

fn spares(panel: &SelectedPanel, phase: PanelPhase) -> Vec<&str> {
    panel
        .runner_up
        .iter()
        .filter(|spare| spare.phase == phase)
        .map(|spare| spare.route_key.as_str())
        .collect()
}

#[test]
fn random_extra_golden_vectors_and_replay() {
    // Preset expected routes catch changes to the hash bytes, index, or pool order
    // instead of deriving expectations from the same random-extra implementation.
    for (seed, extra, winners, continuation) in [
        (
            "consult-42",
            1,
            vec!["p/a"],
            vec!["p/b", "p/d", "p/c", "p/e"],
        ),
        (
            "consult-42",
            2,
            vec!["p/a", "p/b"],
            vec!["p/d", "p/c", "p/e"],
        ),
        ("lab-7#1", 2, vec!["p/b", "p/e"], vec!["p/d", "p/c", "p/a"]),
        ("lab-7#2", 2, vec!["p/b", "p/a"], vec!["p/c", "p/e", "p/d"]),
    ] {
        let context = PanelSelectionContext {
            seed: seed.into(),
            ..Default::default()
        };
        let panel = choose(
            golden_facts(),
            PanelClass::Lab,
            random_policy(extra),
            &context,
        );
        assert_eq!(extras(&panel), winners, "seed {seed}");
        assert_eq!(
            spares(&panel, PanelPhase::Extra),
            continuation,
            "seed {seed}"
        );
        let mut reversed = golden_facts();
        reversed.reverse();
        let replay = choose(reversed, PanelClass::Lab, random_policy(extra), &context);
        assert_eq!(panel, replay);
        let record = serde_json::to_value(panel.policy.unwrap()).unwrap();
        assert_eq!(record["seed"], seed);
        assert_eq!(record["seats"][1]["reason"], "extra");
        assert_eq!(record["seats"][1]["rank"], serde_json::Value::Null);
    }
}

#[test]
fn random_draw_removes_the_winners_entire_family() {
    let mut facts = golden_facts();
    facts
        .iter_mut()
        .find(|fact| fact.model_id == "c")
        .unwrap()
        .family = Some("b".into());
    let panel = choose(
        facts,
        PanelClass::Audit,
        random_policy(2),
        &PanelSelectionContext {
            seed: "consult-42".into(),
            ..Default::default()
        },
    );
    assert_eq!(extras(&panel), ["p/a", "p/b"]);
    assert_eq!(spares(&panel, PanelPhase::Extra), ["p/d", "p/e"]);
    assert!(!spares(&panel, PanelPhase::Extra).contains(&"p/c"));
}

#[test]
fn ranks_count_family_skips_but_not_package_admission_removals() {
    let mut facts = vec![
        fact("A", 2000.0, Some("X")),
        fact("B", 1900.0, Some("X")),
        fact("C", 1850.0, Some("Z")),
        fact("D", 1800.0, Some("Y")),
    ];
    for fact in &mut facts {
        fact.context_window = Some(1_000_000.0);
    }
    facts[2].context_window = Some(50_000.0);
    let mut input = n_pick_input(facts, spec(8, 100.0, PanelDiversity::None));
    input.package_tokens = Some(227_000);
    input.panel_spec.class = PanelClass::Diagnose;
    input.panel_spec.temperature_spread = Some(vec![0.2, 0.8]);
    let policy = ClassPanelPolicy {
        top_k: 2,
        ..ClassPanelPolicy::default_for_class(PanelClass::Diagnose)
    };
    let panel = select_panel_with_policy_context(
        input,
        &DEFAULT_ROUTING_CONFIG,
        &policy,
        &PanelSelectionContext::default(),
    )
    .unwrap();
    assert_eq!(
        panel
            .members
            .iter()
            .map(|member| (member.route_key.as_str(), member.temperature))
            .collect::<Vec<_>>(),
        [("p/A", Some(0.2)), ("p/D", Some(0.8))]
    );
    let record = serde_json::to_value(panel.policy.unwrap()).unwrap();
    assert_eq!(
        record["seats"],
        json!([
            {"slot":0,"routeKey":"p/A","reason":"topK","rank":1},
            {"slot":1,"routeKey":"p/D","reason":"topK","rank":3}
        ])
    );
}

#[test]
fn design_review_and_diagnose_policy_readback_fixtures() {
    let context = PanelSelectionContext {
        seed: "unused".into(),
        pins: PanelPins {
            synthesizer: Some(PanelSynthesizerPin {
                model: "p/synth".into(),
                variant: Some("high".into()),
            }),
        },
    };
    for (class, class_name, by, floor) in [
        (PanelClass::DesignReview, "design_review", "eq", json!(null)),
        (PanelClass::Diagnose, "diagnose", "iq", json!(80)),
    ] {
        let policy = ClassPanelPolicy::default_for_class(class);
        let panel = choose(
            vec![
                fact("A", 2000.0, Some("X")),
                fact("D", 1800.0, Some("Y")),
                fact("E", 1700.0, Some("Z")),
            ],
            class,
            policy,
            &context,
        );
        let stored = serde_json::to_string(&panel.policy.unwrap()).unwrap();
        let readback: serde_json::Value = serde_json::from_str(&stored).unwrap();
        assert_eq!(
            readback,
            json!({
                "selection":"class_policy", "class":class_name, "topK":3, "by":by,
                "extra":0, "extraBy":null, "floor":floor, "width":3, "seed":null,
                "pins":{"synthesizer":{"model":"p/synth","variant":"high"}},
                "seats":[
                    {"slot":0,"routeKey":"p/A","reason":"topK","rank":1},
                    {"slot":1,"routeKey":"p/D","reason":"topK","rank":2},
                    {"slot":2,"routeKey":"p/E","reason":"topK","rank":3}
                ],
                "seatedVariants":[
                    {"slot":0,"routeKey":"p/A","variant":"base"},
                    {"slot":1,"routeKey":"p/D","variant":"base"},
                    {"slot":2,"routeKey":"p/E","variant":"base"}
                ], "siblingCollapses":[]
            })
        );
    }
}

#[test]
fn ranked_extras_and_phase_spares_use_their_own_total_orders() {
    let mut facts = vec![
        fact("top", 2100.0, Some("top")),
        fact("iq", 2000.0, Some("iq")),
        fact("eq", 1900.0, Some("eq")),
        fact("cost", 1800.0, Some("cost")),
        fact("last", 1700.0, Some("last")),
    ];
    facts[2].scores.eq = 2200.0;
    facts[3].cost_per_m_tok_out = Some(0.01);
    facts[4].scores.eq = 2100.0;
    for (order, winner, top_spares, extra_spares) in [
        (
            PanelExtraOrder::Iq,
            "p/iq",
            vec!["p/eq", "p/cost", "p/last"],
            vec!["p/eq", "p/cost", "p/last"],
        ),
        (
            PanelExtraOrder::Eq,
            "p/eq",
            vec!["p/iq", "p/cost", "p/last"],
            vec!["p/last", "p/iq", "p/cost"],
        ),
        (
            PanelExtraOrder::Cost,
            "p/cost",
            vec!["p/iq", "p/eq", "p/last"],
            vec!["p/eq", "p/iq", "p/last"],
        ),
    ] {
        let policy = ClassPanelPolicy {
            extra_by: Some(order),
            ..random_policy(1)
        };
        let panel = choose(
            facts.clone(),
            PanelClass::Evaluate,
            policy,
            &PanelSelectionContext::default(),
        );
        assert_eq!(extras(&panel), [winner]);
        assert_eq!(spares(&panel, PanelPhase::TopK), top_spares);
        assert_eq!(spares(&panel, PanelPhase::Extra), extra_spares);
        let record = serde_json::to_value(&panel.policy.unwrap()).unwrap();
        assert_eq!(record["seed"], json!(null));
        assert_eq!(record["seats"][1]["rank"], 1);
        assert_eq!(record["pins"], json!({"synthesizer":null}));
    }
}

#[test]
fn extra_ranks_count_family_skips_and_omit_below_bar_candidates() {
    let mut facts = vec![
        fact("top", 2000.0, Some("top")),
        fact("first", 1950.0, Some("X")),
        fact("duplicate", 1940.0, Some("X")),
        fact("below", 10.0, Some("low")),
        fact("third", 1930.0, Some("Y")),
    ];
    for (index, fact) in facts.iter_mut().enumerate() {
        fact.scores.eq = 3000.0 - index as f64;
    }
    let policy = ClassPanelPolicy {
        extra_by: Some(PanelExtraOrder::Eq),
        floor: Some(80),
        ..random_policy(2)
    };
    let panel = choose(
        facts,
        PanelClass::Audit,
        policy,
        &PanelSelectionContext::default(),
    );
    assert_eq!(extras(&panel), ["p/first", "p/third"]);
    assert_eq!(spares(&panel, PanelPhase::TopK), ["p/duplicate", "p/below"]);
    assert_eq!(spares(&panel, PanelPhase::Extra), ["p/duplicate"]);
    let record = serde_json::to_value(panel.policy.unwrap()).unwrap();
    assert_eq!(record["seats"][1]["rank"], 1);
    assert_eq!(record["seats"][2]["rank"], 3);
}

#[test]
fn retained_below_bar_pro_has_no_extra_seat_or_spare() {
    let panel = choose(
        vec![
            fact("top", 2000.0, Some("top")),
            fact("base", 1900.0, Some("X")),
            fact("base-pro", 10.0, Some("X")),
        ],
        PanelClass::Audit,
        ClassPanelPolicy {
            floor: Some(80),
            ..random_policy(1)
        },
        &PanelSelectionContext {
            seed: "unused".into(),
            ..Default::default()
        },
    );
    assert!(extras(&panel).is_empty());
    assert!(spares(&panel, PanelPhase::Extra).is_empty());
    assert_eq!(spares(&panel, PanelPhase::TopK), ["p/base-pro"]);
    let record = serde_json::to_value(panel.policy.unwrap()).unwrap();
    assert_eq!(record["seed"], json!(null), "no random seat was drawn");
}

#[test]
fn legacy_runner_up_defaults_to_topk_and_new_entries_serialize_phase() {
    let legacy = json!({"routeKey":"p/a","promptParams":{},"family":"a"});
    let spare: PanelRunnerUp = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(spare.phase, PanelPhase::TopK);
    let mut expected = legacy;
    expected["phase"] = json!("topK");
    assert_eq!(serde_json::to_value(spare).unwrap(), expected);
}
