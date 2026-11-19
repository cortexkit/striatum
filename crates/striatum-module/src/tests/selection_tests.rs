//! Legacy selection and panel tests exercise the wire choices against quota and policy fixtures.

use super::{quota_cache_tests::health_report_for, *};

#[test]
fn ignored_caller_policy_warning_is_claimed_once_per_boot() {
    let warned = Once::new();
    assert!(!claim_ignored_caller_policy_warning(false, &warned));
    assert!(claim_ignored_caller_policy_warning(true, &warned));
    assert!(!claim_ignored_caller_policy_warning(true, &warned));
}

#[test]
fn quota_provenance_records_the_pacing_window_model() {
    let snapshot = ModelRouteStateSnapshot {
        now: 0.0,
        by_route: HashMap::new(),
        by_capacity: HashMap::from([(
            "openai/gpt-5.6-sol".to_string(),
            striatum_core::RouteRuntimeState {
                quota_pressure: Some(0.7),
                quota_window_model: Some(striatum_core::PaceWindowModel::UtilizationFallback),
                ..Default::default()
            },
        )]),
    };
    let windows: Vec<ProviderUsage> = serde_json::from_value(json!([{
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 70.0 } }
    }]))
    .unwrap();

    let value: serde_json::Value = serde_json::from_str(&quota_signal_json(
        &snapshot,
        &QuotaSignalView {
            windows: &windows,
            available: true,
            refreshed_at_ms: 1,
            retained_degraded_count: 0,
            last_error: None,
            restored_from_ms: None,
            stale_age_ms: None,
        },
    ))
    .unwrap();

    assert_eq!(
        value["signals"][0]["windowModel"],
        serde_json::Value::String("utilization_fallback".to_string())
    );
}

#[test]
fn an_auth_json_oauth_claim_is_not_a_non_host_auth_fact() {
    // Routing takes no auth file path at all: host auth kinds come only
    // from the request.
    assert!(provider_auth_types(None).is_empty());
    assert_eq!(
        provider_auth_types(Some(&HashMap::from([("example".into(), "apikey".into())])))["example"],
        "apikey"
    );
}

/// A selection prices and reserves every candidate with the routing
/// configuration it started with. Publishing another configuration while it
/// runs changes neither one candidate's speed multiplier nor the keep-free
/// reserve on the other candidate's subscription pool.
#[test]
fn prepared_inputs_price_and_reserve_every_candidate_on_the_held_configuration() {
    let rig = Rig::new("selection-input-held-pricing");
    let store = Arc::new(rig.open_store());
    store
        .upsert_fact(&fact("held-a", "model"), FactSource::Override)
        .unwrap();
    store
        .upsert_fact(&fact("held-b", "model"), FactSource::Override)
        .unwrap();
    let path = rig.dir.join("routing.jsonc");
    crate::test_config::write(
            &path,
            r#"{"model_routing":{"models":{"held-a/model":{"speed_multiplier":0.5}},"keep_free_percent":{"held-b":20}}}"#,
        )
        .unwrap();
    let held = config_overrides::settle_live_config(&path);
    crate::test_config::write(
            &path,
            r#"{"model_routing":{"models":{"held-a/model":{"speed_multiplier":2.0}},"keep_free_percent":{"held-b":0}}}"#,
        )
        .unwrap();
    let published = config_overrides::settle_live_config(&path);
    assert!(!Arc::ptr_eq(&held, &published));

    let quota = Arc::new(Mutex::new(QuotaCache::default()));
    let judged = |pricing: &RoutePricing| {
        let prepared = prepare_selection_inputs(&quota, &store, pricing, &[]).unwrap();
        let a = prepared
            .facts
            .iter()
            .find(|fact| fact.provider_id == "held-a")
            .unwrap()
            .clone();
        let reserve = prepared.snapshot.by_capacity["held-b/model"]
            .subscription_pool
            .as_ref()
            .unwrap()
            .keep_free_percent;
        (a.speed_multiplier, a.scores.speed, reserve)
    };
    assert_eq!(judged(held.pricing()), (Some(0.5), 45.0, 20.0));
    assert_eq!(judged(published.pricing()), (Some(2.0), 180.0, 0.0));
}

#[test]
fn prepare_selection_inputs_preserves_the_catalog_observed_speed_overlay() {
    let rig = Rig::new("selection-input-observed-speed");
    let store = Arc::new(rig.open_store());
    let base = fact("observed-caller", "fixture");
    let mut sibling = base.clone();
    sibling.variant = Some("low".into());
    store.upsert_fact(&base, FactSource::Override).unwrap();
    store.upsert_fact(&sibling, FactSource::Override).unwrap();
    crate::routing_v1::effort_preview::seed_for_test(
        rig.dir.join("store.db"),
        "observed-caller/fixture",
        crate::routing_v1::effort_preview::test_graded("high", &["low", "high"]),
    );

    let observed_at = now_ms().saturating_sub(10_000) as f64;
    for index in 0..25 {
        store
            .record_usage_observation(&UsageObservation {
                id: format!("selection-input-observed-{index}"),
                route_key: "observed-caller/fixture/high".into(),
                capacity_key: "observed-caller/fixture".into(),
                provider_id: "observed-caller".into(),
                input_tokens: None,
                output_tokens: 1_000.0 + index as f64,
                reasoning_tokens: None,
                cached_read_tokens: None,
                cache_write_tokens: None,
                duration_ms: 1_000.0,
                workload_class: None,
                account_key: None,
                observed_at: observed_at + index as f64,
            })
            .unwrap();
    }

    let expected = store
        .get_facts_with_observed_speed(now_ms() as f64, &RoutePricing::default())
        .unwrap()
        .into_iter()
        .filter(|fact| fact.provider_id == "observed-caller")
        .collect::<Vec<_>>();
    let prepared = prepare_selection_inputs(
        &Arc::new(Mutex::new(QuotaCache::default())),
        &store,
        &RoutePricing::default(),
        &[],
    )
    .unwrap();
    let actual = prepared
        .facts
        .into_iter()
        .filter(|fact| fact.provider_id == "observed-caller")
        .collect::<Vec<_>>();

    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 2);
    assert!(actual.iter().all(|fact| fact.scores.speed > 1_000.0));
    assert_eq!(actual[0].scores.speed, actual[1].scores.speed);
}

#[tokio::test]
async fn legacy_roster_wire_is_refused_before_recording_a_decision() {
    let vectors = select_panel_wire_vectors();
    let request = vectors["legacyRoster"]["request"].as_str().unwrap();
    let request_value: serde_json::Value = serde_json::from_str(request).unwrap();
    assert!(request_value["params"].get("selection").is_none());
    assert!(request_value["params"]
        .get("packageTokensEstimate")
        .is_none());
    assert!(request_value["params"]
        .get("proMemberMaxPackageTokens")
        .is_none());

    let (response, _rig, _store) = run_select_panel_wire_request("legacy-wire", request).await;
    assert_eq!(response.header.ty, FrameType::Error);
    let response_value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(response_value["code"], "roster_removed");
    assert!(_store
        .get_decisions_for_task("panel-wire-legacy")
        .unwrap()
        .is_empty());
}

/// Availability follows the substrate that sends, not the host that requested
/// the panel. Broca can seat a catalog route missing from the host's list.
#[tokio::test]
async fn a_panel_route_absent_from_the_host_list_seats_on_broca_and_is_rejected_on_the_host() {
    let rig = Rig::new("panel-substrate");
    let store = Arc::new(rig.open_store());
    let mut kimi = fact("kimi-code-plan-global", "k3");
    kimi.family = Some("kimi".into());
    kimi.scores.elo = 1700.0;
    store.upsert_fact(&kimi, FactSource::Override).unwrap();
    let broca_credentials = mapped_broca_credentials("kimi-code-plan-global");
    let request = |substrate: &str| {
        json!({
            "method": ROUTE_SELECT_PANEL_OP,
            "params": {
                // The caller's host list lacks Kimi's plan entirely.
                "availableModels": ["anthropic/claude-fable-5-1"],
                "excludeRouteKeys": ["anthropic", "openai", "google", "xai"],
                "panelSpec": {"class": "EXPLAIN", "diversity": "none", "iqFloor": 0, "size": 1},
                "substrate": substrate,
                "taskId": format!("panel-substrate-{substrate}"),
            }
        })
    };
    let run = |substrate: &'static str| {
        let store = Arc::clone(&store);
        let broca_credentials = Arc::clone(&broca_credentials);
        async move {
            let response = run_route_wire_request_with_paths(
                &store,
                request(substrate),
                &test_operator_config(),
                &broca_credentials,
            )
            .await;
            assert_eq!(response.header.ty, FrameType::Response);
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["result"].clone()
        }
    };

    let broca = run("broca").await;
    assert_eq!(
        broca["members"][0]["routeKey"], "kimi-code-plan-global/k3",
        "the catalog serves the route, so it sits on Broca whatever the host lists"
    );
    assert!(broca.get("shortfall").is_none());

    let host = run("opencode").await;
    assert_eq!(host["members"], json!([]));
    let cause = host["shortfall"]["causes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|cause| cause["model"] == "kimi-code-plan-global/k3")
        .unwrap();
    assert_eq!(cause["model"], "kimi-code-plan-global/k3");
    assert_eq!(cause["reason"], "unavailable");
    assert_eq!(
        cause["detail"], "harness broca does not serve this route for this request",
        "the rejection names the route's mapped harness"
    );
    // The decision also records the sending substrate, even when its
    // per-model rejection list is compacted by the store.
    let recorded = store
        .get_decision_by_id(host["decisionID"].as_str().unwrap())
        .unwrap()
        .expect("the panel decision is recorded");
    let explanation: serde_json::Value =
        serde_json::from_str(recorded.explanation_json.as_ref().unwrap()).unwrap();
    assert_eq!(explanation["availability"], "host");
}

/// Before a credential listing arrives, routing knows nothing about which
/// providers Broca can authenticate to. A panel seats from the served catalog, never from the
/// caller's host list (here empty, which would refuse every seat), and the
/// decision row says the reach was unknown even though nothing was refused.
/// The gap also turns `health.check` degraded, so it cannot sit unseen.
#[tokio::test]
async fn a_broca_panel_with_no_reachability_source_seats_a_catalog_route_and_says_so() {
    let rig = Rig::new("panel-substrate-unknown-reach");
    let store = Arc::new(rig.open_store());
    let mut live = fact("livebroca", "m-new");
    live.family = Some("qwen".into());
    live.scores.elo = 10_000.0;
    store.upsert_fact(&live, FactSource::Override).unwrap();
    let broca_credentials = empty_broca_credentials();
    let response = run_route_wire_request_with_paths(
        &store,
        json!({
            "method": ROUTE_SELECT_PANEL_OP,
            "params": {
                "availableModels": [],
                "excludeRouteKeys": ["anthropic", "openai", "google", "xai"],
                "panelSpec": {"class": "lab"},
                "substrate": "broca",
                "taskId": "panel-substrate-unknown-reach",
            }
        }),
        &test_operator_config(),
        &broca_credentials,
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Response);
    let result =
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["result"].clone();
    assert_eq!(result["members"][0]["routeKey"], "livebroca/m-new");
    assert!(result.get("shortfall").is_none(), "{result}");
    let explanation: serde_json::Value =
        serde_json::from_str(&decision_explanation(&store, &result)).unwrap();
    assert_eq!(
        explanation["availability"],
        "broca reachability unknown: judged against served catalog"
    );
    assert_eq!(
        broca_credentials
            .lock()
            .unwrap()
            .health_report(now_ms())
            .detail
            .as_deref(),
        Some("broca reachability unknown: credential listing unavailable")
    );
}

/// Each health check uses the latest credential listing. Completing the
/// provider mapping clears the "broca reachability unknown" detail, even if
/// an earlier Broca send fell back to the whole served catalog.
#[tokio::test]
async fn unknown_broca_reach_degrades_health_until_listing_maps_it() {
    let ModuleControlResponse::HealthCheck { status, detail, .. } =
        health_report_for_with_credentials(QuotaCache::default(), &empty_broca_credentials()).await
    else {
        panic!("health.check answers with a health report");
    };
    assert_eq!(status, HealthStatus::Degraded);
    assert_eq!(
        detail.as_deref(),
        Some("broca reachability unknown: credential listing unavailable")
    );
    let ModuleControlResponse::HealthCheck { status, detail, .. } =
        health_report_for(QuotaCache::default()).await
    else {
        panic!("health.check answers with a health report");
    };
    assert_eq!(status, HealthStatus::Ok);
    assert_eq!(detail, None);
}

/// Even with every credential mapped to a provider, a provider that no active
/// credential serves must not get a panel seat.
#[tokio::test]
async fn a_broca_panel_never_seats_a_provider_broca_holds_no_credential_for() {
    let rig = Rig::new("panel-substrate-credentials");
    let store = Arc::new(rig.open_store());
    store
        .upsert_fact(&fact("uncredentialed", "m1"), FactSource::Override)
        .unwrap();
    let response = run_route_wire_request_with_paths(
        &store,
        json!({
            "method": ROUTE_SELECT_PANEL_OP,
            "params": {
                "availableModels": ["uncredentialed/m1"],
                "panelSpec": {"class": "SPEC", "diversity": "none", "iqFloor": 0, "size": 1},
                "substrate": "broca",
                "taskId": "panel-substrate-uncredentialed"
            }
        }),
        &test_operator_config(),
        &mapped_broca_credentials("kimi-code-plan-global"),
    )
    .await;
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["result"]["members"], json!([]));
    assert_eq!(
        body["result"]["shortfall"]["causes"][0]["reason"],
        "unavailable"
    );
    assert_eq!(
        body["result"]["shortfall"]["causes"][0]["detail"],
        "harness broca does not serve this route for this request"
    );
}

fn decision_explanation(store: &SharedStore, result: &serde_json::Value) -> String {
    store
        .get_decision_by_id(result["decisionID"].as_str().unwrap())
        .unwrap()
        .expect("the selection is recorded")
        .explanation_json
        .unwrap_or_default()
}

#[tokio::test]
async fn n_pick_wire_matches_policy_fixture_and_decision_explanation() {
    let vectors = select_panel_wire_vectors();
    let request = vectors["nPick"]["request"].as_str().unwrap();
    let (response, _rig, store) = run_select_panel_wire_request("n-pick-wire", request).await;
    assert_eq!(response.header.ty, FrameType::Response);
    let response_value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    let decision_id = response_value["result"]["decisionID"].as_str().unwrap();
    let expected = vectors["nPick"]["reply"]
        .as_str()
        .unwrap()
        .replace("$decisionID", decision_id);
    let mut expected: serde_json::Value = serde_json::from_str(&expected).unwrap();
    // The archived Sol Pro row has no catalog effort facts, so the xhigh
    // default routing once stored with the model must no longer appear in the
    // legacy panel reply.
    let removed_effort_delta = [(
        "/result/members/0/promptParams",
        "openai/gpt-5.6-sol-pro",
        "xhigh",
    )];
    for (path, route, old_variant) in removed_effort_delta {
        assert_eq!(expected["result"]["members"][0]["routeKey"], route);
        assert_eq!(
            expected
                .pointer_mut(path)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove("variant"),
            Some(json!(old_variant))
        );
    }
    for member in expected["result"]["members"].as_array_mut().unwrap() {
        member["harness"] = json!("broca");
    }
    for cause in expected["result"]["shortfall"]["causes"]
        .as_array_mut()
        .unwrap()
    {
        if cause["reason"] == "unavailable" {
            cause["detail"] = json!("harness broca does not serve this route for this request");
        }
    }
    let funnel = json!({
        "catalog": 46, "scored": 46, "operatorAdmitted": 46, "callerAdmitted": 46,
        "reachable": 2, "capabilityEligible": 2, "quotaEligible": 2, "packageFit": 2,
        "adequacyEligible": 1, "distinctFamilies": 1, "initiallyAdmitted": 1,
        "gateOrder": ["deprecated_upstream", "unscored_awaiting_placement", "excluded_by_operator",
            "excluded", "unavailable", "capability", "quota_or_backoff", "package_fit",
            "sibling_collapse", "family_labeled", "phase_adequacy", "distinct_families", "initially_admitted"]
    });
    expected["result"]["policy"]["funnel"] = funnel.clone();
    expected["result"]["shortfall"]["funnel"] = funnel;
    assert_eq!(response_value, expected);

    let decisions = store.get_decisions_for_task("panel-wire-n-pick").unwrap();
    assert_eq!(decisions.len(), 1);
}

#[tokio::test]
async fn automatic_panel_requests_obey_caller_exclusions_with_or_without_a_package_size() {
    let vectors = select_panel_wire_vectors();
    let original: serde_json::Value =
        serde_json::from_str(vectors["nPick"]["request"].as_str().unwrap()).unwrap();
    for package_tokens in [None, Some(227_000)] {
        let mut request = original.clone();
        request["params"]["excludeRouteKeys"] = json!(["openai"]);
        if let Some(tokens) = package_tokens {
            request["params"]["packageTokens"] = json!(tokens);
        }
        let (response, _rig, _store) =
            run_select_panel_wire_request("caller-exclusion", &request.to_string()).await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["result"]["members"], json!([]));
        assert!(
            body["result"]["shortfall"]["causes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|cause| cause["reason"] == "excluded"),
            "{body}"
        );
    }
}

#[test]
fn n_pick_decision_explanation_includes_selection_and_policy() {
    let vectors = select_panel_wire_vectors();
    let request = vectors["nPick"]["request"].as_str().unwrap();
    let params = serde_json::from_str::<SelectPanelBody>(request)
        .unwrap()
        .params;
    let policy = striatum_core::PanelPolicy {
        package_tokens: None,
        largest_package_ceiling: None,
        replacements: Vec::new(),
        selection: "class_policy".into(),
        seated_variants: vec![striatum_core::SeatedVariant {
            slot: 0,
            route_key: "openai/gpt-5.6-sol-pro".into(),
            variant: striatum_core::SeatedSiblingVariant::Pro,
            reasoning_variant: None,
            reasoning_variant_source: None,
            reasoning_variant_fallbacks: Vec::new(),
        }],
        sibling_collapses: Vec::new(),
        ..Default::default()
    };
    let panel = SelectedPanel {
        candidates: Vec::new(),
        availability_rejections: Vec::new(),
        rejected_causes: Vec::new(),
        members: Vec::new(),
        runner_up: Vec::new(),
        shortfall: None,
        policy: Some(policy),
    };
    let provenance = DecisionProvenance {
        quota_signal_json: r#"{"state":"none","reason":"quota signal: none"}"#.into(),
        policy_revision: "test-policy".into(),
        anchor_version: ANCHOR_VERSION,
    };
    let decision = panel_decision_input(
        &params,
        &panel,
        AvailabilitySubstrate::Host,
        &provenance,
        "policy sources: test",
    );
    let explanation: serde_json::Value =
        serde_json::from_str(decision.explanation_json.as_deref().unwrap()).unwrap();
    assert_eq!(explanation["selection"], "class_policy");
    assert_eq!(explanation["policy"]["selection"], "class_policy");
}

#[test]
fn package_tokens_are_typed_and_decisions_retain_policy_metadata() {
    let mut body: serde_json::Value = serde_json::from_str(
        select_panel_wire_vectors()["nPick"]["request"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    body["params"]["selection"] = json!("n_pick");
    body["params"]["packageTokens"] = json!(227_000);
    let params = serde_json::from_value::<SelectPanelBody>(body.clone())
        .unwrap()
        .params;
    assert_eq!(params.package_tokens, Some(227_000));
    body["params"]["packageTokens"] = json!("227000");
    assert!(serde_json::from_value::<SelectPanelBody>(body).is_err());
    let panel = SelectedPanel {
        candidates: Vec::new(),
        availability_rejections: Vec::new(),
        rejected_causes: Vec::new(),
        members: Vec::new(),
        runner_up: Vec::new(),
        shortfall: None,
        policy: Some(striatum_core::PanelPolicy {
            selection: "class_policy".into(),
            seated_variants: Vec::new(),
            sibling_collapses: Vec::new(),
            package_tokens: Some(227_000),
            largest_package_ceiling: Some(460_000),
            replacements: vec![striatum_core::panel::PackageSeatReplacement {
                slot: 0,
                replaced_route_key: "p/small".into(),
                replacement_route_key: "p/large".into(),
                reason: "package 227000 tokens exceeds p/small ceiling 210000 tokens".into(),
            }],
            ..Default::default()
        }),
    };
    let decision = panel_decision_input(
        &params,
        &panel,
        AvailabilitySubstrate::Broca,
        &DecisionProvenance {
            quota_signal_json: "{}".into(),
            policy_revision: "test-policy".into(),
            anchor_version: ANCHOR_VERSION,
        },
        "policy sources: test",
    );
    let explanation: serde_json::Value =
        serde_json::from_str(decision.explanation_json.as_deref().unwrap()).unwrap();
    assert_eq!(explanation["selection"], "class_policy");
    assert_eq!(
        explanation["policy"]["replacements"][0]["replacementRouteKey"],
        "p/large"
    );
}

/// The package no longer steers seating, so an n_pick request needs no package
/// size fields; a caller still sending the retired ones is served, not refused.
#[tokio::test]
async fn n_pick_needs_no_package_size_fields_and_tolerates_retired_ones() {
    let vectors = select_panel_wire_vectors();
    let request = vectors["nPick"]["request"].as_str().unwrap();
    let mut with_retired: serde_json::Value = serde_json::from_str(request).unwrap();
    assert!(with_retired["params"]
        .get("packageTokensEstimate")
        .is_none());
    with_retired["params"]["packageTokensEstimate"] = json!(100);
    with_retired["params"]["proMemberMaxPackageTokens"] = json!(8_000);
    let (response, _rig, _store) =
        run_select_panel_wire_request("n-pick-retired-fields", &with_retired.to_string()).await;
    assert_eq!(response.header.ty, FrameType::Response);
}

#[tokio::test]
async fn select_panel_serves_full_contract_shape() {
    let advertised = manifest(DEFAULT_MODULE_ID)
        .provides
        .into_iter()
        .find_map(|role| match role {
            ProviderRole::ManagementSurface { operations, .. } => Some(operations),
            _ => None,
        })
        .unwrap();
    assert!(advertised
        .iter()
        .any(|operation| operation.name == ROUTE_SELECT_PANEL_OP));

    let rig = Rig::new("select-panel-contract");
    let store = Arc::new(rig.open_store());
    let quota_cache = Arc::new(Mutex::new(QuotaCache::default()));
    let request = json!({
        "method": ROUTE_SELECT_PANEL_OP,
        "params": {
            "panelSpec": {
                "size": 1,
                "iqFloor": 0,
                "diversity": "none",
                "class": "AUDIT",
                "temperatureSpread": [0.2]
            },
            "selection": "n_pick",
            "availableModels": ["openai/gpt-5.6-sol", "openai/gpt-5.5"],
            "subscriptionKeys": ["openai/gpt-5.6-sol", "openai/gpt-5.5"],
            "taskId": "panel-contract-task"
        }
    });
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        7,
        1,
        99,
        serde_json::to_vec(&request).unwrap(),
    )
    .unwrap();
    let (tx, mut rx) = mpsc::channel(1);
    handle_select_request(
        frame,
        1,
        &tx,
        &quota_cache,
        &store,
        &Arc::new(Mutex::new(None)),
        &test_operator_config(),
        &test_operator_config(),
        &empty_broca_credentials(),
    )
    .await
    .unwrap();
    let response = rx.recv().await.unwrap();
    assert_eq!(response.header.ty, FrameType::Response);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    let result = &body["result"];
    assert_eq!(result["members"].as_array().unwrap().len(), 1);
    assert_eq!(result["runnerUp"].as_array().unwrap().len(), 0);
    assert!(
        result.get("runner_up").is_none(),
        "wire keys stay camelCase"
    );
    assert_eq!(result["members"][0]["temperature"], 0.2);
    // This panel fixture has no catalog effort facts for its seat, so the seat
    // runs with no named effort.
    let removed_effort_delta = ["/members/0/promptParams/variant"];
    for path in removed_effort_delta {
        assert!(result.pointer(path).is_none(), "{path}: {result}");
    }
    assert!(result["decisionID"].as_str().is_some());
    assert_eq!(result["policy"]["selection"], "class_policy");
    assert_eq!(result["policy"]["width"], 3);
    assert!(
        result.get("shortfall").is_some(),
        "one family cannot fill width three"
    );

    let decisions = store.get_decisions_for_task("panel-contract-task").unwrap();
    assert_eq!(decisions.len(), 1);
    assert_eq!(decisions[0].decision_kind, "panel");
    assert_eq!(decisions[0].selected_route_key, None);
}

/// Legacy slot metadata cannot make a removed roster executable.
#[tokio::test]
async fn select_panel_refuses_rosters_even_with_legacy_slot_metadata() {
    let rig = Rig::new("select-panel-pinned-saturated");
    let store = Arc::new(rig.open_store());
    let response = run_route_wire_request(
        &store,
        json!({
            "method": ROUTE_SELECT_PANEL_OP,
            "params": {
                "panelSpec": {
                    "size": 1,
                    "iqFloor": 0,
                    "diversity": "none",
                    "class": "AUDIT",
                    "temperatureSpread": [0.2]
                },
                "roster": [
                    {"model": "openai/gpt-5.6-sol", "promptParams": {"variant": "xhigh"}},
                    {"model": "openai/gpt-5.5", "promptParams": {"variant": "high"}}
                ],
                "availableModels": ["openai/gpt-5.6-sol", "openai/gpt-5.5"],
                "concurrencySlots": [
                    {"key": "openai/gpt-5.6-sol", "running": 50, "limit": 40}
                ],
                "taskId": "panel-pinned-saturated"
            }
        }),
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Error);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["code"], "roster_removed");
}

/// The routing file for the paid Anthropic API key: a provider with no
/// catalog rows, which borrows `anthropic`'s rows through its keys' `serves`
/// vendor and which `exclude.all` keeps out of automatic choice. Naming one
/// of its models still admits it.
fn platform_operator_config(rig: &Rig) -> PathBuf {
    let path = rig.dir.join("striatum.jsonc");
    crate::test_config::write(
        &path,
        r#"{ "model_routing": {
                "exclude": { "all": ["anthropic-platform"] }
            } }"#,
    )
    .unwrap();
    path
}

/// A vault listing whose rows map `provider_ids` as given. Each row serves
/// the vendor its id names (`apikey:anthropic:work` serves `anthropic`), as
/// the vault lists them. `account_id` is the key's workspace, deliberately
/// different from the id's label.
fn platform_listing(rows: &[(&str, &[&str])]) -> SharedBrocaCredentials {
    let credentials = empty_broca_credentials();
    credentials.lock().unwrap().apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            credentials: rows
                .iter()
                .map(
                    |(id, providers)| cortexkit_claustrum_client::ListedCredential {
                        id: (*id).into(),
                        categories: vec!["llm-provider".into()],
                        auth_method: Some(
                            if id.starts_with("apikey:") {
                                "apikey"
                            } else if id.starts_with("chatgpt:") {
                                "chatgpt"
                            } else {
                                "oauth"
                            }
                            .into(),
                        ),
                        credential_type: if id.starts_with("apikey:") {
                            "apikey"
                        } else {
                            "oauth"
                        }
                        .into(),
                        state: "active".into(),
                        provider_ids: providers.iter().map(|p| (*p).into()).collect(),
                        serves: id
                            .split(':')
                            .nth(1)
                            .map(str::to_string)
                            .into_iter()
                            .collect(),
                        account_id: Some("workspace-7".into()),
                        ..Default::default()
                    },
                )
                .collect(),
            view: format!("platform-view-{}", rows.len()),
            ..Default::default()
        },
        now_ms(),
    );
    credentials
}

#[tokio::test]
async fn panel_replaces_an_exhausted_claude_pool_and_records_blocked_keys() {
    let rig = Rig::new("panel-exhausted-pool");
    let store: SharedStore = Arc::new(rig.open_store());
    rusqlite::Connection::open(rig.dir.join("store.db"))
        .unwrap()
        .execute("DELETE FROM model_facts", [])
        .unwrap();
    for (provider, model, family, elo) in [
        ("anthropic", "claude-opus-5-5", "claude", 2200.0),
        ("openai", "gpt-fallback", "gpt", 2100.0),
        ("anthropic", "claude-sonnet-fallback", "claude", 2000.0),
    ] {
        let mut row = fact(provider, model);
        row.family = Some(family.into());
        row.scores.elo = elo;
        store.upsert_fact(&row, FactSource::Override).unwrap();
    }
    let config = rig.dir.join("striatum.jsonc");
    crate::test_config::write(&config, json!({"model_routing": {
            "exclude": {"all": ["anthropic/claude-opus-5-5", "anthropic-platform/claude-sonnet-fallback"]},
            "panels": {"audit": {"topK": 2, "extra": 0, "by": "iq"}}
        }}).to_string()).unwrap();
    let credentials = platform_listing(&[
        ("apikey:anthropic:empty", &["anthropic-platform"]),
        ("apikey:anthropic:limited", &["anthropic-platform"]),
        ("oauth:anthropic:subscription", &["anthropic"]),
        ("apikey:openai", &["openai"]),
    ]);
    let call = |method: &'static str, params: serde_json::Value| {
        let store = Arc::clone(&store);
        let credentials = credentials.clone();
        let config = config.clone();
        async move {
            let frame = Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Interactive, false),
                7,
                1,
                99,
                serde_json::to_vec(&json!({"method": method, "params": params})).unwrap(),
            )
            .unwrap();
            let (tx, mut rx) = mpsc::channel(1);
            handle_select_request(
                frame,
                1,
                &tx,
                &Arc::new(Mutex::new(QuotaCache::default())),
                &store,
                &Arc::new(Mutex::new(None)),
                &config,
                &config,
                &credentials,
            )
            .await
            .unwrap();
            serde_json::from_slice::<serde_json::Value>(&rx.recv().await.unwrap().body).unwrap()
        }
    };
    let panel_params = json!({"substrate": "broca", "panelSpec": {"class": "AUDIT"},
            "availableModels": [], "taskId": "pool-panel"});
    let healthy = call(ROUTE_SELECT_PANEL_OP, panel_params.clone()).await;
    assert_eq!(
        healthy["result"]["members"][0]["routeKey"], "anthropic-platform/claude-opus-5-5",
        "{healthy}"
    );
    for (key, kind) in [("empty", "billing_exhausted"), ("limited", "rate_limited")] {
        let wall = call(
            "route.pool_wall",
            json!({"provider": "anthropic-platform",
                "credentialId": format!("apikey:anthropic:{key}"), "kind": kind,
                "untilMs": now_ms() as i64 + 3_600_000}),
        )
        .await;
        assert_eq!(wall["result"]["ok"], true, "{wall}");
    }
    let body = call(ROUTE_SELECT_PANEL_OP, panel_params).await;
    let members = body["result"]["members"]
        .as_array()
        .unwrap_or_else(|| panic!("{body}"));
    assert_eq!(members.len(), 2, "{body}");
    assert_eq!(members[0]["routeKey"], "openai/gpt-fallback", "{body}");
    assert_eq!(
        members[1]["routeKey"], "anthropic/claude-sonnet-fallback",
        "{body}"
    );
    assert!(body["result"].get("shortfall").is_none(), "{body}");
    let decision = store
        .get_decision_by_id(body["result"]["decisionID"].as_str().unwrap())
        .unwrap()
        .unwrap();
    let rejected: serde_json::Value = serde_json::from_str(&decision.rejected_json).unwrap();
    let cause = rejected
        .as_array()
        .unwrap()
        .iter()
        .find(|cause| cause["model"] == "anthropic-platform/claude-opus-5-5")
        .unwrap_or_else(|| panic!("missing pooled-route rejection: {rejected}; panel: {body}"));
    assert_eq!(cause["reason"], "unavailable", "{rejected}");
    let detail = cause["detail"].as_str().unwrap();
    let pool_rejection: serde_json::Value = serde_json::from_str(detail).unwrap();
    assert_eq!(pool_rejection["code"], "explicit_provider_pool_exhausted");
    assert_eq!(pool_rejection["pool"]["keys"].as_array().unwrap().len(), 2);
    for expected in [
        "explicit_provider_pool_exhausted",
        "apikey:anthropic:empty",
        "billing_exhausted",
        "apikey:anthropic:limited",
        "rate_limited",
    ] {
        assert!(detail.contains(expected), "{cause}");
    }
    let unfilled = call(
        ROUTE_SELECT_PANEL_OP,
        json!({
            "substrate": "broca", "panelSpec": {"class": "AUDIT"},
            "taskId": "pool-panel-unfilled",
            "excludeRouteKeys": ["openai/gpt-fallback", "anthropic/claude-sonnet-fallback"],
        }),
    )
    .await;
    assert_eq!(unfilled["result"]["members"], json!([]), "{unfilled}");
    assert!(unfilled["result"]["shortfall"].is_object(), "{unfilled}");
    assert!(
        store
            .get_decision_by_id(unfilled["result"]["decisionID"].as_str().unwrap())
            .unwrap()
            .is_some(),
        "{unfilled}"
    );
}

/// The four API keys the live vault maps to `anthropic-platform`.
const LIVE_PLATFORM_KEYS: [&str; 4] = [
    "apikey:anthropic:wsampleyahoo",
    "apikey:anthropic:usersamplegmail",
    "apikey:anthropic:wsamplegmail",
    "apikey:anthropic:ysample",
];

/// Broca's run-end error for the low-credit key, as core stored it in the
/// provider cooldown's `last_error` (serde_json's sorted key order).
const LIVE_LOW_CREDIT_ERROR: &str = r#"{"class":"permanent","credential_id":"apikey:anthropic:wsampleyahoo","message":"400: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.\"},\"request_id\":\"req_011CgK8eU8fTeQoECLazghi7\"}","provider_code":"billing_exhausted","status":400}"#;

/// A pool of the four live keys with `anthropic/claude-opus-5-5` in the catalog.
fn live_platform_pool(name: &str) -> (Rig, SharedStore, SharedBrocaCredentials, PathBuf) {
    let platform: &[&str] = &["anthropic-platform"];
    let credentials = platform_listing(&LIVE_PLATFORM_KEYS.map(|key| (key, platform)));
    let rig = Rig::new(name);
    let store: SharedStore = Arc::new(rig.open_store());
    store
        .upsert_fact(&fact("anthropic", "claude-opus-5-5"), FactSource::Override)
        .unwrap();
    let config = platform_operator_config(&rig);
    (rig, store, credentials, config)
}

async fn live_platform_call(
    store: &SharedStore,
    credentials: &SharedBrocaCredentials,
    config: &Path,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let frame = run_route_wire_request_with_paths(
        store,
        json!({"method": method, "params": params}),
        config,
        credentials,
    )
    .await;
    serde_json::from_slice(&frame.body).unwrap()
}

/// Core's outcome report for a run one key ended with `billing_exhausted`.
async fn report_live_billing_refusal(
    store: &SharedStore,
    credentials: &SharedBrocaCredentials,
    credential: &str,
) {
    let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            7,
            1,
            99,
            serde_json::to_vec(&json!({"method": ROUTE_SET_DECISION_OUTCOME_OP, "params": {
                "decisionID": format!("decision-{credential}"), "outcome": "error",
                "lastError": LIVE_LOW_CREDIT_ERROR.replace(LIVE_PLATFORM_KEYS[0], credential),
                "providerOutcome": {
                    "accountRef": credential, "credentialId": credential,
                    "credentialRefusal": {"providerId": "anthropic-platform", "credentialId": credential, "code": "billing_exhausted"}
                }}}))
            .unwrap(),
        )
        .unwrap();
    let (tx, mut rx) = mpsc::channel(1);
    handle_set_decision_outcome(
        frame,
        &tx,
        store,
        Path::new("/nonexistent/live-platform-pool.jsonc"),
        credentials,
    )
    .await
    .unwrap();
    let ack: serde_json::Value = serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap();
    assert_eq!(ack["result"]["ok"], true, "{ack}");
}

/// A vault listing with explicit rows: (id, provider_ids, account_id).
pub(super) fn vault_listing(rows: &[(&str, &[&str], Option<&str>)]) -> SharedBrocaCredentials {
    let credentials = empty_broca_credentials();
    credentials.lock().unwrap().apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            credentials: rows
                .iter()
                .map(
                    |(id, providers, account)| cortexkit_claustrum_client::ListedCredential {
                        id: (*id).into(),
                        categories: vec!["llm-provider".into()],
                        auth_method: Some(
                            if id.starts_with("apikey:") {
                                "apikey"
                            } else if id.starts_with("chatgpt:") {
                                "chatgpt"
                            } else {
                                "oauth"
                            }
                            .into(),
                        ),
                        credential_type: if id.starts_with("apikey:") {
                            "apikey"
                        } else {
                            "oauth"
                        }
                        .into(),
                        state: "active".into(),
                        provider_ids: providers.iter().map(|p| (*p).into()).collect(),
                        serves: id
                            .split(':')
                            .nth(1)
                            .map(str::to_string)
                            .into_iter()
                            .collect(),
                        account_id: account.map(str::to_string),
                        ..Default::default()
                    },
                )
                .collect(),
            view: format!("vault-view-{}", rows.len()),
            ..Default::default()
        },
        now_ms(),
    );
    credentials
}

#[tokio::test]
async fn panels_never_seat_a_provider_excluded_in_all_and_refuse_removed_roster_pins() {
    let credentials = platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])]);
    let panel = |label: &'static str, roster: serde_json::Value, selection: &'static str| {
        let credentials = credentials.clone();
        async move {
            let rig = Rig::new(label);
            let store: SharedStore = Arc::new(rig.open_store());
            store
                .upsert_fact(&fact("anthropic", "claude-opus-4-1"), FactSource::Override)
                .unwrap();
            let config = platform_operator_config(&rig);
            let frame = run_route_wire_request_with_paths(
                &store,
                json!({"method": ROUTE_SELECT_PANEL_OP, "params": {
                    "panelSpec": {"class": "SPEC", "diversity": "none", "iqFloor": 0, "size": 1},
                    "selection": selection, "roster": roster, "substrate": "broca",
                    "taskId": label,
                }}),
                &config,
                &credentials,
            )
            .await;
            serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()
        }
    };
    let automatic = panel("platform-panel-npick", json!([]), "n_pick").await;
    assert_eq!(automatic["result"]["members"], json!([]), "{automatic}");
    assert!(
        automatic.to_string().contains("excluded_by_operator"),
        "an automatic seat names why the paid key was skipped: {automatic}"
    );
    let pinned = panel(
        "platform-panel-roster",
        json!([{"model": "anthropic-platform/claude-opus-4-1"}]),
        "roster",
    )
    .await;
    assert_eq!(pinned["code"], "roster_removed", "{pinned}");
}

#[test]
fn pool_membership_comes_from_the_listing_and_never_guesses() {
    let listing = platform_listing(&[("apikey:anthropic:work", &["anthropic-platform"])]);
    let credentials = listing.lock().unwrap();
    let keys = credentials
        .pool_keys("anthropic-platform", now_ms())
        .unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].credential_id, "apikey:anthropic:work");
    assert_eq!(keys[0].workspace, "workspace-7");
    assert_eq!(
        credentials
            .pool_keys("anthropic-platform-work", now_ms())
            .unwrap_err()
            .0,
        "explicit_provider_credential_unresolved"
    );
    assert_eq!(
        BrocaCredentials::default()
            .pool_keys("anthropic-platform", now_ms())
            .unwrap_err()
            .0,
        "explicit_provider_credential_unresolved",
        "an unread listing is unresolved, not a default key"
    );
}
