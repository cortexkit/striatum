use striatum_core::{FactSource, ModelFact, ScoreProvenance};

fn quality_fact(provider: &str, model: &str) -> ModelFact {
    serde_json::from_value(json!({
        "providerID": provider, "modelID": model,
        "scores": {"elo": 1000, "eq": 1100, "speed": 0},
        "confidence": "curated"
    })).unwrap()
}

#[test]
fn pinned_fusiform_operator_quality_payload_decodes() {
    // Response of "catalog.get — synthetic operator quality scores" in the
    // fusiform repository's crates/fusiform-module/fixtures/served-payloads.json
    // at commit 0eee4710e06b4f3fbb8cee15019d5e7d5dc3d991.
    let snapshot = decode_catalog_get(include_bytes!("../fixtures/operator-quality-scores.json")).unwrap();
    assert_eq!(snapshot.quality_scores, Some(QualityScores {
        graded_by: "operator".into(), graded_at_ms: 123,
    }));
    let grade = &snapshot.grades["synthetic-quality-reseller/synthetic-model"];
    assert_eq!(grade.model, "synthetic-quality-reseller/synthetic-model");
    assert_eq!((grade.elo, grade.eq), (1284.0, 1599.8));
}

#[test]
fn operator_quality_precedence_and_anthropic_platform_mapping() {
    quality_precedence_for_mapping("anthropic", "anthropic-platform");
}

#[test]
fn operator_quality_mapping_follows_credentials_for_arbitrary_providers() {
    quality_precedence_for_mapping("synthetic-quality-vendor", "synthetic-quality-account");
}

fn quality_precedence_for_mapping(catalog_provider: &str, mapped_provider: &str) {
    let rig = Rig::new(&format!("operator-quality-precedence-{mapped_provider}"));
    let store = rig.store();
    let catalog_route = format!("{catalog_provider}/quality-test");
    let mapped_route = format!("{mapped_provider}/quality-test");
    store.upsert_fact(&quality_fact(catalog_provider, "quality-test"), FactSource::Override).unwrap();
    store.upsert_fact(&quality_fact("synthetic", "ungraded"), FactSource::Override).unwrap();
    let credentials = Arc::new(Mutex::new(crate::broca_credentials::BrocaCredentials::default()));
    credentials.lock().unwrap().apply_listing(&cortexkit_claustrum_client::ScopedCredentialListing {
        view: "quality-mapping".into(),
        credentials: vec![cortexkit_claustrum_client::ListedCredential {
            id: "quality-credential".into(), state: "active".into(),
            categories: vec!["llm-provider".into()],
            provider_ids: vec![mapped_provider.into()], serves: vec![catalog_provider.into()],
            ..Default::default()
        }],
        ..Default::default()
    }, 123);
    let snapshot = decode_catalog_get(&serde_json::to_vec(&json!({
        "catalog_version": 9, "resolved_at_ms": 123,
        "models": {(catalog_route.clone()): {
            "existence": "present", "model.canonical_id": "lab/quality-test",
            "quality.elo": 1284, "quality.eq": 1599.8
        }},
        "quality_scores": {"graded_by": "operator", "graded_at_ms": 123}
    })).unwrap()).unwrap();
    apply_mapped_catalog(&store, &snapshot, Some(&credentials), None).unwrap();
    assert_eq!(store.operator_quality().unwrap()[&mapped_route], snapshot.grades[&catalog_route]);
    let config_path = rig.dir.join("routing.jsonc");
    crate::test_config::write(&config_path, &json!({"model_routing": {
        "models": {
            (catalog_route.clone()): {"elo": 900, "eq": 950},
            (mapped_route.clone()): {"elo": 1400}
        }
    }}).to_string()).unwrap();
    let config = crate::config_overrides::settle_live_config(&config_path);
    let mut shadowed = config.shadowed_operator_grades(&store.operator_quality().unwrap());
    shadowed.sort();
    let mut expected_shadowed = vec![catalog_route.clone(), mapped_route.clone()];
    expected_shadowed.sort();
    assert_eq!(shadowed, expected_shadowed);
    let mut facts = store.get_facts().unwrap();
    let targets = credentials.lock().unwrap().catalog_targets(&facts);
    striatum_core::score_alias::append_alias_rows(&mut facts, &targets);
    config.apply(&mut facts);
    config.apply(&mut facts);
    for provider in [catalog_provider, mapped_provider] {
        let fact = facts.iter().find(|f| f.provider_id == provider && f.model_id == "quality-test").unwrap();
        if provider == catalog_provider {
            assert_eq!(fact.scores.elo, 900.0);
            assert_eq!(fact.scores.eq, 950.0);
            assert_eq!(fact.eq_source.as_deref(), Some("override"));
        } else {
            assert_eq!(fact.scores.elo, 1400.0);
            assert_eq!(fact.scores.eq, 1599.8);
            assert_eq!(fact.eq_source.as_deref(), Some("operator_grade"));
        }
        assert_eq!(fact.score_provenance, ScoreProvenance::EloOverride);
    }
    facts.retain(|f| f.model_id == "quality-test" || f.provider_id == "synthetic");
    let requirements = striatum_core::ModelRequirements { iq: 1.0, eq: 1.0 };
    let selected = striatum_core::select_model_route(
        striatum_core::SelectModelRouteInput {
            target_agent: "quality-test".into(), target_is_head: false, requirements,
            available_models: facts.iter().map(|f| format!("{}/{}", f.provider_id, f.model_id)).collect(),
            facts, runtime_snapshot: None, exclude_route_keys: Vec::new(),
            operator_exclude_keys: Vec::new(), required_capabilities: None,
            is_subscription_route: None, min_best_effort_adequacy: None,
            prefer_prompt_cache: false, providers_without_prompt_cache: Default::default(),
            providers_without_cache_write_reports: Default::default(),
        },
    ).unwrap();
    let candidates = serde_json::to_value(&selected.score).unwrap()["candidates"].clone();
    let candidates = candidates.as_array().unwrap();
    for (route, elo_source, eq_source) in [
        (catalog_route.as_str(), "override", "override"),
        (mapped_route.as_str(), "override", "operator_grade"),
        ("synthetic/ungraded", "curated", "curated"),
    ] {
        let candidate = candidates.iter().find(|c| c["routeKey"] == route).unwrap();
        assert_eq!(candidate["eloSource"], elo_source);
        assert_eq!(candidate["eqSource"], eq_source);
        assert_eq!(candidate["eligible"], true);
    }
    let decision = crate::decision_input_for("quality-test", &requirements, Some(&selected), None,
        &crate::SelectParams::for_record(), None, None);
    store.record_decision(&decision).unwrap();
    let rows = store.unscored_admissible_models(0, i64::MAX).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].model, "synthetic/ungraded");
    store.replace_operator_quality(false, &BTreeMap::new()).unwrap();
    let facts = store.get_facts().unwrap();
    let fact = facts.iter().find(|f| f.provider_id == catalog_provider && f.model_id == "quality-test").unwrap();
    assert_eq!((fact.scores.elo, fact.scores.eq), (1000.0, 1100.0));
    assert_eq!(store.get_fact_by_route_key(&catalog_route).unwrap().unwrap().scores.elo, 1000.0);
}
