use super::*;

#[tokio::test]
async fn typed_panel_shortfall_retains_quota_and_backoff_reset_times() {
    use striatum_core::RouteStateUpdate;
    let (h, route, mut request) = configured().await;
    let routes = request["available_routes"].as_array().unwrap().clone();
    request["available_routes"] = json!([routes[0], routes[1]]);
    let quota_route = routes[0].as_str().unwrap();
    let backoff_route = routes[1].as_str().unwrap();
    let quota_reset = crate::now_ms() + 600_000;
    let backoff_reset = quota_reset + 60_000;
    h.store()
        .update_route_state(
            quota_route,
            quota_route,
            &RouteStateUpdate {
                quota_exhausted_until: Some(quota_reset as f64),
                ..Default::default()
            },
        )
        .unwrap();
    h.store()
        .update_route_state(backoff_route, backoff_route, &RouteStateUpdate::default())
        .unwrap();
    crate::routing_v1::native_fixture_sources::failure_backoff(
        h.store(),
        backoff_route,
        backoff_reset as f64,
        Some("three terminal errors"),
    );
    let answer = panel(&route, &request).await;
    assert!(answer.seats.is_empty());
    let encoded = serde_json::to_value(answer).unwrap();
    let decoded: SelectPanelAnswer = serde_json::from_value(encoded).unwrap();
    let detail = decoded.shortfall.unwrap().typed_detail().unwrap().unwrap();
    for (model, reason, reset) in [
        (quota_route, "quota_exhausted", quota_reset),
        (backoff_route, "backoff_active", backoff_reset),
    ] {
        let cause = detail
            .causes
            .iter()
            .find(|cause| cause.model == model)
            .unwrap();
        assert_eq!(cause.reason, reason);
        assert_eq!(cause.reset_at_ms, Some(reset));
    }
}

#[tokio::test]
async fn panel_shortfall_detail_retains_package_exclusion_and_availability_evidence() {
    let (h, route, mut request) = configured().await;
    let routes = request["available_routes"].as_array().unwrap().clone();
    request["available_routes"] = json!([routes[0], routes[1], routes[3]]);
    request["exclude"] = json!([routes[1]]);
    request["package_tokens"] = json!(70_000);
    let mut small = h
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|f| f.model_id == "panel-0")
        .unwrap();
    small.context_window = Some(50_000.0);
    h.store()
        .upsert_fact(
            &small,
            striatum_core::route_state_store::FactSource::Override,
        )
        .unwrap();

    let answer = panel(&route, &request).await;
    assert_eq!(answer.seats.len(), 1);
    let shortfall = answer.shortfall.as_ref().unwrap();
    assert_eq!(shortfall.code, ShortfallCode::PackageUnmeetable);
    let detail = shortfall.detail.as_ref().unwrap();
    assert_eq!(detail["package_tokens"], 70_000);
    assert!(detail["message"]
        .as_str()
        .unwrap()
        .contains("70,000 tokens"));
    let causes = detail["causes"].as_array().unwrap();
    let small = causes.iter().find(|c| c["model"] == routes[0]).unwrap();
    assert_eq!(small["reason"], "package_too_large");
    let typed = shortfall.typed_detail().unwrap().unwrap();
    let small = typed
        .causes
        .iter()
        .find(|cause| cause.model == routes[0].as_str().unwrap())
        .unwrap();
    let budget = small.typed_detail().unwrap().unwrap();
    assert_eq!(budget.package_tokens, 70_000);
    assert_eq!(budget.budget_tokens, 1_000);
    assert_eq!(budget.context_window, 50_000);
    assert!(budget.context_window_known);
    assert_eq!(budget.catalog_max_output_tokens, Some(32_000));
    assert_eq!(budget.output_reserve_tokens, 32_000);
    assert_eq!(budget.prompt_reserve_tokens, 16_000);
    assert!(causes
        .iter()
        .any(|c| c["model"] == routes[1] && c["reason"] == "excluded"));
    assert!(causes
        .iter()
        .any(|c| c["model"] == routes[2] && c["reason"] == "unavailable"));
    let stored: Value = serde_json::from_str(
        &h.store()
            .v1_panel(&answer.panel_id, "reserved:prefrontal-core")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(stored["answer"], serde_json::to_value(&answer).unwrap());
}

#[tokio::test]
async fn spec_extra_floor_shortfalls_keep_incident_package_sizes_and_rejected_routes() {
    let (h, route, mut request) = configured().await;
    request["class"] = json!("spec");
    h.edit_config(|c| {
        c["panels"]["spec"] = json!({"topK":3,"by":"iq","extra":1,"extraBy":"random","floor":80});
    });
    for mut fact in h
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .filter(|f| f.model_id.starts_with("panel-"))
    {
        let index = fact
            .model_id
            .trim_start_matches("panel-")
            .parse::<usize>()
            .unwrap();
        fact.scores.elo = [2315.0, 2067.0, 1852.0, 1703.0, 1000.0][index];
        fact.context_window =
            Some([1_000_000.0, 572_000.0, 1_048_576.0, 1_000_000.0, 100_000.0][index]);
        fact.max_output_tokens = Some([128_000, 64_000, 65_536, 64_000, 32_000][index]);
        h.store()
            .upsert_fact(
                &fact,
                striatum_core::route_state_store::FactSource::Override,
            )
            .unwrap();
    }
    for tokens in [85_068, 80_982, 46_960] {
        request["package_tokens"] = json!(tokens);
        let answer = panel(&route, &request).await;
        assert_eq!(answer.seats.len(), 1);
        let shortfall = answer.shortfall.unwrap();
        assert_eq!(shortfall.code, ShortfallCode::FloorUnmeetable);
        let detail = shortfall.detail.unwrap();
        assert_eq!(detail["package_tokens"], tokens);
        let causes = detail["causes"].as_array().unwrap();
        assert!(causes
            .iter()
            .any(|c| c["model"].as_str().unwrap().ends_with("/panel-3")
                && c["reason"] == "below_iq_floor"));
        assert!(causes
            .iter()
            .any(|c| c["model"].as_str().unwrap().ends_with("/panel-4")
                && c["reason"] == "package_too_large"));
    }
}
