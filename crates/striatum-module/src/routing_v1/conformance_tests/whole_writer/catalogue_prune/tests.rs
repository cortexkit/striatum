use super::*;

#[tokio::test]
async fn catalogue_prune_ends_a_route_wall_before_sibling_select_and_rediscovery() {
    let _clock = crate::InjectedClock::set(START);
    let world = World::new("catalogue-prune-block-lifetime").await;
    let store = world.harness.store();
    let route = format!("{}/model-c", fixture::SUBSCRIPTION_PROVIDER);
    let original = store.get_fact_by_route_key(&route).unwrap().unwrap();
    let mut sibling = original.clone();
    sibling.variant = Some("still-listed".into());
    store.upsert_fact(&sibling, FactSource::Override).unwrap();
    let sibling_key = format!("{route}/still-listed");
    store
        .update_route_state(
            &route,
            &route,
            &RouteStateUpdate {
                observed_at_ms: Some(START as i64),
                unscoped_wall: true,
                cooldown_until: Some((START + 3_600_000) as f64),
                last_error: Some("route capacity".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(store
        .fence_rechecks()
        .unwrap()
        .iter()
        .any(|origin| origin.kind == "route"
            && origin.origin_key == serde_json::json!([route]).to_string()));
    // Only catalogue eligibility is seeded directly. The actual production
    // prune must remove its route source and clock in the same command.
    store
        .with_writer_for_test(|conn| {
            conn.execute(
                "UPDATE model_facts SET fusiform_present=0,source='fusiform' WHERE route_key=?1",
                [&route],
            )
        })
        .unwrap();
    assert!(store
        .prune_unserved_unpicked_facts()
        .unwrap()
        .contains(&route));
    let key = serde_json::json!([route]).to_string();
    assert!(
        store
            .fence_rechecks()
            .unwrap()
            .iter()
            .all(|origin| !(origin.kind == "route" && origin.origin_key == key)),
        "a pruned route must not keep a model-scoped origin that blocks its sibling"
    );
    let (_, sibling_reply) = world
        .owner
        .call(
            "route.select",
            &json!({"consumer":world.consumer,"requirements":{"iq":30},"pin":sibling_key}),
        )
        .await;
    assert!(
        sibling_reply.get("result").is_some(),
        "a still-listed variant must match main after the source route is pruned: {sibling_reply}"
    );
    store.upsert_fact(&original, FactSource::Override).unwrap();
    let (frame, reply) = world
        .owner
        .call(
            "route.select",
            &json!({"consumer":world.consumer,"requirements":{"iq":30},"pin":route}),
        )
        .await;
    assert_eq!(
        frame,
        FrameType::Response,
        "rediscovery must not restore a ghost wall: {reply}"
    );
    let blocks = crate::account_state::snapshot(store).unwrap();
    assert!(blocks.blocks.clock("route", &key).is_none());
    assert!(blocks
        .blocks
        .view_origins()
        .iter()
        .all(|origin| !(origin.kind == "route" && origin.origin_key == key)));
}
