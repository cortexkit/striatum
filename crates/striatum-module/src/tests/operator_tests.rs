//! Operator-operation tests cover recorded decision inspection and targeted recovery.

use super::*;

#[tokio::test]
async fn clear_cooldown_accepts_stored_and_work_variant_keys_and_clears_base_variants() {
    const BASE: &str = "alibaba-token-plan/qwen3.8-max-preview";
    let rig = Rig::new("clear-variant-cooldown");
    let store = Arc::new(rig.open_store());
    let variant = format!("{BASE}/xhigh");
    let until = now_ms() as f64 + 1_800_000.0;
    for key in [variant.clone(), format!("{BASE}:xhigh"), BASE.into()] {
        for route in [BASE, variant.as_str()] {
            store
                .update_route_state(
                    route,
                    BASE,
                    &striatum_core::RouteStateUpdate {
                        cooldown_until: Some(until),
                        wall_class: Some(RouteWallClass::Quota),
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        let response = run_route_wire_request(
            &store,
            json!({
                "method": ROUTE_CLEAR_COOLDOWN_OP, "params": {"routeKey": key}
            }),
        )
        .await;
        assert_eq!(
            response.header.ty,
            FrameType::Response,
            "{:?}",
            response.body
        );
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let rows = body["result"]["clearedRoutes"].as_array().unwrap();
        assert!(rows
            .iter()
            .any(|row| row["routeKey"] == variant && row["clearedCooldownUntil"] == until));
        let snapshot = store.build_snapshot(now_ms() as f64).unwrap();
        assert_eq!(snapshot.by_route[&variant].cooldown_until, None);
        if key == BASE {
            assert_eq!(rows.len(), 2);
            assert_eq!(snapshot.by_route[BASE].cooldown_until, None);
        } else {
            assert_eq!(rows.len(), 1);
            assert_eq!(snapshot.by_route[BASE].cooldown_until, Some(until));
        }
    }
}

#[tokio::test]
async fn route_clear_cooldown_refuses_unknown_route_key() {
    let rig = Rig::new("clear-cooldown-unknown");
    let store = Arc::new(rig.open_store());
    let response = run_route_wire_request(
        &store,
        json!({
            "method": ROUTE_CLEAR_COOLDOWN_OP,
            "params": { "routeKey": "unknown/model" },
        }),
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Error);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["code"], "unknown_route");
    assert!(body["message"].as_str().unwrap().contains("unknown/model"));
}

#[tokio::test]
async fn route_explain_by_send_id_returns_recorded_rejections_verbatim() {
    let rig = Rig::new("route-explain-send-id");
    let store = Arc::new(rig.open_store());
    let rejections = json!([
        { "routeKey": "p/too-slow", "reason": "too_slow", "detail": "recorded" },
        { "routeKey": "p/too-costly", "reason": "too_expensive" },
    ]);
    let score = json!({
        "adequacy": 0.97,
        "selectionCost": 12.5,
        "metrics": { "speed": { "actual": 73.0 } },
    });
    store
        .record_decision(&DecisionInput {
            decision_id: Some("dec-explain-send".into()),
            decision_kind: "single".into(),
            task_id: Some("task-explain".into()),
            parent_session_id: Some("ses-explain".into()),
            send_id: Some("send-explain".into()),
            target_agent: "mason".into(),
            requirements_json: r#"{"iq":80,"eq":20}"#.into(),
            selected_route_key: Some("p/selected".into()),
            selected_provider_id: Some("p".into()),
            selected_model_id: Some("selected".into()),
            score_json: serde_json::to_string(&score).unwrap(),
            rejected_json: serde_json::to_string(&rejections).unwrap(),
            explanation_json: Some(r#"["cost-primary"]"#.into()),
            quota_signal_json: Some(r#"{"state":"present","signals":[]}"#.into()),
            policy_revision: Some("policy-explain".into()),
            anchor_version: ANCHOR_VERSION,
            ..Default::default()
        })
        .unwrap();

    let identity = striatum_core::route_state_store::ProviderOutcome {
        account_ref: "frozen-account".into(),
        credential_id: Some("oauth:p:frozen-account".into()),
        resets_at_ms: None,
        credential_refusal: None,
        ..Default::default()
    };
    store
        .set_decision_provider_identity("dec-explain-send", &identity)
        .unwrap();

    let response = run_route_wire_request(
        &store,
        json!({
            "method": ROUTE_EXPLAIN_OP,
            "params": { "sendID": "send-explain" },
        }),
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Response);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["result"]["decisionID"], "dec-explain-send");
    assert_eq!(body["result"]["providerIdentity"], json!(identity));
    assert_eq!(body["result"]["why"]["rejections"], rejections);
    assert_eq!(body["result"]["why"]["score"], score);
    assert_eq!(body["result"]["provenance"]["sendID"], "send-explain");
    assert_eq!(body["result"]["provenance"]["taskID"], "task-explain");
    assert_eq!(
        body["result"]["provenance"]["parentSessionID"],
        "ses-explain"
    );
    assert_eq!(body["result"]["provenance"]["targetAgent"], "mason");
}

#[tokio::test]
async fn route_explain_serves_core_fields_from_a_compacted_decision() {
    let rig = Rig::new("route-explain-compacted");
    let store = Arc::new(rig.open_store());
    let candidate = |route: &str| {
        json!({"routeKey": route, "model": route, "elo": 1300.0, "eq": 1200.0,
                "eloSource": "curated", "eqSource": "curated", "eligible": true})
    };
    let mut candidates: Vec<serde_json::Value> = (0..200)
        .map(|index| candidate(&format!("p/other-{index:03}")))
        .collect();
    candidates.extend([candidate("p/selected"), candidate("q/adequate")]);
    let objective = json!({
        "cost": 0.25, "quota": 0.0, "speedScore": 0.5, "selectionCost": 0.25,
        "taskPricing": {"profile": null, "tokens": null, "priceUnit": "mtok_output",
            "listCostUsd": 2.0, "valueMultiplier": 6.0, "valueSource": "operator",
            "pressure": 0.1, "finalCostUsd": 12.0, "normalizedCost": 0.25,
            "omittedDimensions": [], "unpricedDimensions": []},
    });
    let metrics = json!({"elo": {"fact": 1300.0, "target": 1250.0}});
    let score = json!({
        "candidates": candidates,
        "adequacy": 1.0,
        "rawAdequacy": 1.04,
        "selectionCost": 0.25,
        "metrics": metrics,
        "objective": objective,
        "alsoAdequate": ["q/adequate"],
        "pricingMode": "output_only",
    });
    let rejections = json!([{ "routeKey": "p/walled", "reason": "quota_exhausted" }]);
    store
        .record_decision(&DecisionInput {
            decision_id: Some("dec-compacted".into()),
            decision_kind: "single".into(),
            target_agent: "mason".into(),
            requirements_json: r#"{"iq":80,"eq":20}"#.into(),
            selected_route_key: Some("p/selected".into()),
            selected_provider_id: Some("p".into()),
            selected_model_id: Some("selected".into()),
            score_json: serde_json::to_string(&score).unwrap(),
            rejected_json: serde_json::to_string(&rejections).unwrap(),
            explanation_json: Some(r#"["cost-primary"]"#.into()),
            quota_signal_json: Some(
                r#"{"state":"present","signals":[{"capacityKey":"p","pressure":0.1}]}"#.into(),
            ),
            policy_revision: Some("policy-compacted".into()),
            anchor_version: ANCHOR_VERSION,
            ..Default::default()
        })
        .unwrap();
    async fn explain(store: &SharedStore) -> serde_json::Value {
        let response = run_route_wire_request(
            store,
            json!({
                "method": ROUTE_EXPLAIN_OP,
                "params": { "decisionID": "dec-compacted" },
            }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Response);
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["result"].clone()
    }
    let full = explain(&store).await;
    assert_eq!(
        full["detailRetention"],
        json!({"detail": "full", "strippedAtMs": null})
    );
    assert_eq!(full["why"]["score"], score);

    // The full-detail window has passed and the decision is not sampled.
    let batch = store.strip_decision_detail(i64::MAX, 0, 16, 1_234).unwrap();
    assert_eq!(batch.stripped, 1);
    let compact = explain(&store).await;
    assert_eq!(
        compact["detailRetention"],
        json!({"detail": "compact", "strippedAtMs": 1_234})
    );
    // What core reads: the adequate set and policy revision for its work
    // view, and the selected route's value multiplier for its prices.
    assert_eq!(
        compact.pointer("/why/score/alsoAdequate"),
        Some(&json!(["q/adequate"]))
    );
    assert_eq!(compact["policyRevision"], "policy-compacted");
    assert_eq!(
        compact.pointer("/why/score/objective/taskPricing/valueMultiplier"),
        Some(&json!(6.0))
    );
    assert_eq!(compact["why"]["score"]["objective"], objective);
    assert_eq!(compact["why"]["score"]["metrics"], metrics);
    assert_eq!(
        compact["why"]["score"]["candidates"],
        json!([candidate("p/selected"), candidate("q/adequate")])
    );
    assert_eq!(compact["why"]["score"]["candidatesOmitted"], 200);
    assert_eq!(compact["why"]["rejections"], rejections);
    assert_eq!(compact["why"]["explanation"], json!(["cost-primary"]));
    assert_eq!(
        compact["quotaSignalSnapshot"],
        json!({"state": "present", "signalsOmitted": 1})
    );
    assert_eq!(compact["selectedRoute"]["routeKey"], "p/selected");
}

#[tokio::test]
async fn route_decisions_list_composes_target_agent_and_since_filters() {
    let rig = Rig::new("route-decisions-list-filters");
    let store = Arc::new(rig.open_store());
    for (id, target_agent, route_key) in [
        ("dec-old-mason", "mason", "p/old"),
        ("dec-new-mason", "mason", "p/new"),
        ("dec-new-explore", "explore", "p/other"),
    ] {
        store
            .record_decision(&DecisionInput {
                decision_id: Some(id.into()),
                target_agent: target_agent.into(),
                requirements_json: "{}".into(),
                selected_route_key: Some(route_key.into()),
                score_json: "null".into(),
                rejected_json: "[]".into(),
                anchor_version: ANCHOR_VERSION,
                ..Default::default()
            })
            .unwrap();
    }
    let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    for (id, created_at) in [
        ("dec-old-mason", 1_000_i64),
        ("dec-new-mason", 6_000_i64),
        ("dec-new-explore", 7_000_i64),
    ] {
        connection
            .execute(
                "UPDATE model_route_decisions SET created_at=?1 WHERE id=?2",
                rusqlite::params![created_at, id],
            )
            .unwrap();
    }

    let response = run_route_wire_request(
        &store,
        json!({
            "method": ROUTE_DECISIONS_LIST_OP,
            "params": { "target_agent": "mason", "since_ms": 5_000 },
        }),
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Response);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(
        body["result"]["decisions"],
        json!([{
            "decisionID": "dec-new-mason",
            "createdAtMs": 6_000,
            "selectedRouteKey": "p/new",
            "decisionKind": "single",
            "topRejectionReason": null,
            "topRejectionCount": 0,
        }])
    );
}

#[tokio::test]
async fn route_explain_unknown_id_names_key_and_retention() {
    let rig = Rig::new("route-explain-missing");
    let store = Arc::new(rig.open_store());
    let response = run_route_wire_request(
        &store,
        json!({
            "method": ROUTE_EXPLAIN_OP,
            "params": { "decisionID": "dec-pruned" },
        }),
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Error);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    let message = body["message"].as_str().unwrap();
    let retention_days =
        retention::retention_windows(&config_overrides::config_path()).model_route_decisions_days;
    assert_eq!(body["code"], "not_found");
    assert!(message.contains("decisionID 'dec-pruned'"));
    assert!(message.contains(&format!(
        "not found or pruned; retention is {retention_days} days"
    )));
}

#[tokio::test]
async fn route_explain_exactly_one_key_validation_does_not_mutate_decisions() {
    let rig = Rig::new("route-explain-one-key");
    let store = Arc::new(rig.open_store());
    for params in [json!({}), json!({ "decisionID": "dec", "sendID": "send" })] {
        let response = run_route_wire_request(
            &store,
            json!({ "method": ROUTE_EXPLAIN_OP, "params": params }),
        )
        .await;
        assert_eq!(response.header.ty, FrameType::Error);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["code"], "invalid_request");
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("exactly one of decisionID or sendID"));
    }
    assert!(store
        .list_decisions(&DecisionListFilter {
            limit: 20,
            ..Default::default()
        })
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn route_resolve_policy_dispatches_as_a_read_only_wire_query() {
    let request = json!({
        "method": ROUTE_RESOLVE_POLICY_OP,
        "params": {
            "availableModels": ["fixture/a", "fixture/b"],
            "routingPolicy": {
                "exclude": ["fixture/a"],
                "subscription": ["fixture"],
            },
            "providerAuthTypes": { "fixture": "api" }
        }
    });
    let (response, _rig, _store) = run_select_panel_wire_request(
        "resolve-policy-wire",
        &serde_json::to_string(&request).unwrap(),
    )
    .await;
    assert_eq!(response.header.ty, FrameType::Response);
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(
        body["result"],
        json!({
            "availableModels": ["fixture/a", "fixture/b"],
            "subscriptionKeys": [],
            "connectedModelCount": 2,
        })
    );
}
