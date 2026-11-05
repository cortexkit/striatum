#[tokio::test]
async fn replay_model_facts() {
    let _clock = crate::InjectedClock::set(1_800_000_000_000);
    let _inputs = prefrontal_routing_store::replay::Inputs::new(1_800_000_000_000).install();
    let rig = Rig::new("replay-model-facts");
    let store = Arc::new(rig.open_store());
    let mut rows = Vec::new();
    for (model, output) in [("ordinary", Some(32_000)), ("zero", Some(0)),
        ("placeholder", Some(200_000)), ("unknown", None)] {
        let mut model_fact = fact("replay", model);
        model_fact.context_window = Some(200_000.0);
        model_fact.max_output_tokens = output;
        model_fact.family = Some("claude".into());
        store.upsert_fact(&model_fact, FactSource::Override).unwrap();
        let request = json!({"method":"route.model_fact", "params":{"providerID":"replay", "modelID":model}});
        let reply = run_route_wire_request(&store, request.clone()).await;
        assert_eq!(reply.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        rows.push(json!({"catalog_fact":model_fact, "request":request, "reply":body}));
    }
    crate::routing_v1::replay_tests::assert_or_record("model-facts", json!({
        "body_source":"main.rs::route_model_fact_reads_a_seed_absent_anthropic_fact_from_the_live_store",
        "rows":rows,
    }));
}
