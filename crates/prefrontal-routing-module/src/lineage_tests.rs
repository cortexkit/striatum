fn lineage_catalog(rig: &Rig) -> Vec<prefrontal_routing_core::ModelFact> {
    use prefrontal_routing_core::{FactSource, RouteStateStore};
    let store = rig.store();
    let snapshot = decode_catalog_get(include_bytes!("../fixtures/catalog-lineage.json")).unwrap();
    apply_mapped_catalog(&store, &snapshot, None, Some("lineage-fixture")).unwrap();
    let mut facts = store.get_facts().unwrap();
    facts.retain(|fact| fact.provider_id == "lineage-fixture");
    assert_eq!(facts.len(), 6);
    for fact in &mut facts {
        fact.scores = prefrontal_routing_core::ModelScores {
            elo: 1800.0,
            eq: 1700.0,
            speed: 100.0,
        };
        fact.confidence = prefrontal_routing_core::Confidence::Curated;
        fact.score_provenance = prefrontal_routing_core::ScoreProvenance::Curated;
        store.upsert_fact(fact, FactSource::Override).unwrap();
    }
    facts
}

#[test]
fn catalog_lineage_drives_panel_diversity_and_pricing_fallback() {
    use prefrontal_routing_core::task_profile::{
        compute_profile, ClosedTask, ProfileLevel, TokenVector, MIN_TASKS,
    };
    use prefrontal_routing_core::{
        select_panel_with_policy, AvailabilitySubstrate, ClassPanelPolicy, PanelClass,
        PanelDiversity, PanelOrder, PanelSelection, PanelSpec, SelectPanelInput,
    };
    let rig = Rig::new("lineage-consumers");
    let facts = lineage_catalog(&rig);
    let row = |id: &str| facts.iter().find(|fact| fact.model_id == id).unwrap();
    assert_eq!(row("verbatim").grand_family_id(), Some(" QWEN "));
    assert_eq!(row("unknown-a").grand_family_id(), Some("other"));
    assert_eq!(row("unknown-b").grand_family_id(), Some("other"));
    let selected = select_panel_with_policy(
        SelectPanelInput {
            selection: PanelSelection::NPick,
            roster: vec![],
            available_models: facts
                .iter()
                .map(|fact| format!("{}/{}", fact.provider_id, fact.model_id))
                .collect(),
            availability_substrate: AvailabilitySubstrate::Host,
            availability_label: "fixture".into(),
            panel_spec: PanelSpec {
                size: 6,
                iq_floor: 0.0,
                diversity: PanelDiversity::Family,
                class: PanelClass::Audit,
                temperature_spread: None,
            },
            facts: facts.clone(),
            runtime_snapshot: None,
            exclude_route_keys: vec![],
            operator_exclude_keys: vec![],
            required_capabilities: None,
            is_subscription_route: None,
            package_tokens: None,
        },
        &Default::default(),
        &ClassPanelPolicy {
            top_k: 6,
            by: PanelOrder::Iq,
            extra: 0,
            extra_by: None,
            floor: Some(0),
        },
    )
    .unwrap();
    let families: BTreeSet<_> = selected
        .members
        .iter()
        .map(|member| member.family.as_deref().unwrap())
        .collect();
    assert_eq!(selected.members.len(), 4, "{:?}", selected.shortfall);
    assert_eq!(families, BTreeSet::from(["qwen", "gpt", "other", " QWEN "]));

    let tasks: Vec<_> = (0..MIN_TASKS)
        .map(|_| ClosedTask {
            workload: "mason",
            model: row("gpt-impostor-b"),
            iq_bucket: 7,
            tokens: TokenVector {
                output_excluding_reasoning: 123.0,
                ..Default::default()
            },
        })
        .chain((0..MIN_TASKS).map(|_| ClosedTask {
            workload: "mason",
            model: row("claude-impostor-c"),
            iq_bucket: 7,
            tokens: TokenVector {
                output_excluding_reasoning: 999.0,
                ..Default::default()
            },
        }))
        .collect();
    let profile = compute_profile(&tasks, row("claude-impostor-a"), "mason", 7).unwrap();
    assert_eq!(profile.level, ProfileLevel::SameFamily);
    assert_eq!(profile.tokens.output_excluding_reasoning, 123.0);

    let unknown_tasks: Vec<_> = (0..MIN_TASKS)
        .map(|_| ClosedTask {
            workload: "mason",
            model: row("unknown-a"),
            iq_bucket: 7,
            tokens: TokenVector {
                output_excluding_reasoning: 321.0,
                ..Default::default()
            },
        })
        .collect();
    assert_eq!(
        compute_profile(&unknown_tasks, row("unknown-b"), "mason", 7)
            .unwrap()
            .level,
        ProfileLevel::SameFamily
    );
    assert_eq!(
        compute_profile(&tasks, row("verbatim"), "mason", 7)
            .unwrap()
            .level,
        ProfileLevel::FleetBucket
    );
}

#[test]
fn catalog_lineage_replaces_stale_rules_on_variants_and_clears_on_absence() {
    use prefrontal_routing_core::{FactSource, FamilySource, GrandFamily, RouteStateStore};
    let rig = Rig::new("lineage-refresh");
    let facts = lineage_catalog(&rig);
    let store = rig.store();
    let mut variant = facts
        .iter()
        .find(|fact| fact.model_id == "claude-impostor-a")
        .unwrap()
        .clone();
    variant.variant = Some("high".into());
    variant.family = Some("claude".into());
    store.upsert_fact(&variant, FactSource::Override).unwrap();
    assert_eq!(
        store
            .get_fact_by_route_key("lineage-fixture/claude-impostor-a/high")
            .unwrap()
            .unwrap()
            .family
            .as_deref(),
        Some("qwen")
    );
    for key in [
        "lineage-fixture/claude-impostor-a",
        "lineage-fixture/claude-impostor-a/high",
    ] {
        store
            .set_model_family(key, Some(GrandFamily::Claude), FamilySource::Rule)
            .unwrap();
    }
    let config_path = rig.dir.join("routing.jsonc");
    std::fs::write(
        &config_path,
        r#"{"models":{"lineage-fixture/claude-impostor-a":{"elo":1900}}}"#,
    )
    .unwrap();
    let mut snapshot =
        decode_catalog_get(include_bytes!("../fixtures/catalog-lineage.json")).unwrap();
    apply_catalog_snapshot(
        &store,
        &snapshot,
        Some(&config_path),
        None,
        None,
        "lineage test",
    )
    .unwrap();
    crate::config_overrides::apply_overrides_from_config(&store, &config_path).unwrap();
    let assert_lineage = |expected: Option<&str>| {
        for key in [
            "lineage-fixture/claude-impostor-a",
            "lineage-fixture/claude-impostor-a/high",
        ] {
            assert_eq!(
                store
                    .get_fact_by_route_key(key)
                    .unwrap()
                    .unwrap()
                    .family
                    .as_deref(),
                expected
            );
        }
    };
    assert_lineage(Some("qwen"));
    snapshot
        .facts
        .iter_mut()
        .find(|fact| fact.model_id == "claude-impostor-a")
        .unwrap()
        .lineage = None;
    apply_catalog_snapshot(
        &store,
        &snapshot,
        Some(&config_path),
        None,
        None,
        "lineage withdrawal",
    )
    .unwrap();
    crate::config_overrides::apply_overrides_from_config(&store, &config_path).unwrap();
    assert_lineage(None);
    drop(store);
    let reopened = rig.store();
    assert_eq!(
        reopened
            .get_fact_by_route_key("lineage-fixture/verbatim")
            .unwrap()
            .unwrap()
            .family
            .as_deref(),
        Some(" QWEN ")
    );
}

#[test]
fn routing_has_no_config_glob_family_readers() {
    let defaults: Value =
        serde_json::from_str(include_str!("../data/config_convert_defaults.json")).unwrap();
    assert!(defaults.get("model_families").is_none());
    let schema: Value =
        serde_json::from_str(&crate::config_overrides::routing_config_schema()).unwrap();
    assert!(schema["properties"].get("model_families").is_none());
    // Guard the deleted reader interfaces as well as the config shape. Quota-pool
    // mappings under accounts remain independent of catalog lineage.
    for source in [
        include_str!("config_overrides.rs"),
        include_str!("config_convert.rs"),
        include_str!("fusiform_catalog.rs"),
    ] {
        for reader in [
            concat!("Model", "FamilyRules"),
            concat!("matching_", "family"),
            concat!("apply_family_", "rules"),
            concat!("Ordered", "Families"),
            concat!("config.", "model_families"),
        ] {
            assert!(
                !source.contains(reader),
                "removed family reader remains: {reader}"
            );
        }
    }
}
