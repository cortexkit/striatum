use striatum_core::{FactSource, ModelFact, ScoreProvenance};

fn decommissioned_poll(store: &SqliteRouteStateStore, flag: Option<bool>) {
    let mut retired = json!({"existence":"present", "model.canonical_id":"lab/retired",
        "quality.future_member":{"ignored":true}, "capability.tool_call":true});
    if let Some(flag) = flag {
        retired["quality.decommissioned"] = json!(flag);
    }
    let snapshot = decode_catalog_get(&serde_json::to_vec(&json!({
        "catalog_version":11, "resolved_at_ms":123, "future_catalog_member":true,
        "quality_scores":{"graded_by":"operator", "graded_at_ms":123, "future_member":true},
        "models":{
            "vendor/retired":retired,
            "reseller/retired":{"existence":"present", "model.canonical_id":"lab/retired", "capability.tool_call":true}
        }
    })).unwrap()).unwrap();
    apply_mapped_catalog(store, &snapshot, None, Some("vendor")).unwrap();
}

fn decommissioned_select(facts: Vec<ModelFact>) -> Result<striatum_core::SelectedModelRoute, striatum_core::NoRouteError> {
    striatum_core::select_model_route(striatum_core::SelectModelRouteInput {
        target_agent:"withdrawal-test".into(), target_is_head:false,
        requirements:striatum_core::ModelRequirements {iq:1.0,eq:1.0},
        available_models:facts.iter().map(|fact|format!("{}/{}",fact.provider_id,fact.model_id)).collect(),
        facts, runtime_snapshot:None, exclude_route_keys:Vec::new(), operator_exclude_keys:Vec::new(),
        required_capabilities:None, is_subscription_route:None, min_best_effort_adequacy:None,
        prefer_prompt_cache:false, providers_without_prompt_cache:Default::default(),
        providers_without_cache_write_reports:Default::default(),
    })
}

#[test]
fn decommissioned_served_payload_excludes_select_and_canonical_resellers() {
    let rig = Rig::new("decommissioned-canonical");
    let store = rig.store();
    for provider in ["vendor", "reseller"] {
        store.upsert_fact(&quality_fact(provider,"retired"), FactSource::Override).unwrap();
    }
    decommissioned_poll(&store, Some(true));
    let facts:Vec<_> = store.get_facts().unwrap().into_iter().filter(|f|f.model_id=="retired").collect();
    assert_eq!(facts.len(),2);
    let error = decommissioned_select(facts).expect_err("operator withdrawal must gate both routes");
    for provider in ["vendor", "reseller"] {
        assert!(error.rejected.iter().any(|r|r.route_key==format!("{provider}/retired") && r.reason=="decommissioned"), "{error:?}");
    }
}

#[test]
fn decommissioned_later_poll_without_flag_or_with_false_restores_routing() {
    let rig = Rig::new("decommissioned-current-state");
    let store = rig.store();
    store.upsert_fact(&quality_fact("vendor","retired"), FactSource::Override).unwrap();
    let facts = || store.get_facts().unwrap().into_iter().filter(|f|f.provider_id=="vendor" && f.model_id=="retired").collect();
    for restored in [None, Some(false)] {
        decommissioned_poll(&store, Some(true));
        assert!(decommissioned_select(facts()).is_err(), "flag must withdraw route before restoration");
        let withdrawn_fingerprint = store.catalog_fingerprint().unwrap();
        decommissioned_poll(&store, restored);
        assert!(decommissioned_select(facts()).is_ok(), "missing or false flag must restore routing");
        assert_ne!(store.catalog_fingerprint().unwrap(), withdrawn_fingerprint);
    }
}

#[test]
fn decommissioned_core_panel_gate_counts_withdrawals_before_scoring_and_rosters() {
    use striatum_core::panel::*;
    let rig = Rig::new("decommissioned-panel-funnel");
    let store = rig.store();
    for (provider,model) in [("vendor","retired"),("reseller","retired"),("vendor","active")] {
        let mut fact = quality_fact(provider,model);
        fact.family=Some(format!("{provider}-{model}"));
        store.upsert_fact(&fact,FactSource::Override).unwrap();
    }
    decommissioned_poll(&store,Some(true));
    for selection in [PanelSelection::NPick,PanelSelection::Roster] {
        let facts:Vec<_> = store.get_facts().unwrap().into_iter().filter(|f|["vendor","reseller"].contains(&f.provider_id.as_str())).collect();
        let automatic = selection==PanelSelection::NPick;
        let panel = select_panel_with_policy(SelectPanelInput {
            package_tokens:None,
            panel_spec:PanelSpec {class:PanelClass::Audit,size:3,iq_floor:0.0,diversity:PanelDiversity::Family,temperature_spread:None},
            selection, roster:if automatic {Vec::new()} else {vec![PanelRosterEntry {model:"vendor/retired".into(),prompt_params:Default::default()}]},
            available_models:facts.iter().map(|f|format!("{}/{}",f.provider_id,f.model_id)).collect(),
            facts,runtime_snapshot:None,exclude_route_keys:Vec::new(),operator_exclude_keys:Vec::new(),
            required_capabilities:None,availability_substrate:AvailabilitySubstrate::Broca,
            availability_label:"test".into(),is_subscription_route:None,
        }, &striatum_core::RoutingConfig::default(), &ClassPanelPolicy {top_k:3,by:PanelOrder::Iq,extra:0,extra_by:None,floor:None}).unwrap();
        assert_eq!(panel.members.len(),usize::from(automatic));
        assert!(panel.members.iter().all(|m|!m.route_key.contains("retired")));
        assert!(panel.candidates.iter().all(|c|!c.model.contains("retired")));
        assert!(panel.rejected_causes.iter().any(|c|c.model=="vendor/retired" && serde_json::to_value(c.reason).unwrap()=="decommissioned"));
        assert!(!serde_json::to_string(&panel.shortfall).unwrap().contains("retired"));
        if automatic {
            let funnel=panel.policy.unwrap().funnel.unwrap();
            assert_eq!((funnel.catalog,funnel.scored),(3,1));
        }
    }
}

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
