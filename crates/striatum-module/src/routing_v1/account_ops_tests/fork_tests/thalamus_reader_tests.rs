use super::*;
use sha2::{Digest, Sha256};

const REQUEST: &[u8] =
    include_bytes!("../../../../fixtures/thalamus-route-fork-of-v1/request.json");
const HOLD: &[u8] =
    include_bytes!("../../../../fixtures/thalamus-route-fork-of-v1/hold-answer.json");

async fn relay_parent() -> (Arc<Harness>, Route) {
    assert_eq!(
        format!("{:x}", Sha256::digest(REQUEST)),
        "a96e194685b92151f7ceceed5e0dafc580ed3b0f5836b43d6d14fe5299151a36"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(HOLD)),
        "94b1c12960b67c9841db267888af00a66fe847bf9a2911ea0c85c7ff395c54f8"
    );
    let (harness, mut credentials) = fixture();
    let mut fact = super::super::super::account_classes_tests::account_fact("anthropic");
    fact.model_id = "synthetic-model".into();
    harness.install_facts(&[fact.clone()]);
    credentials.refresh_serving_methods(&Default::default(), &[fact.clone()], NOW as i64, &[]);
    fact.billing_plane.as_mut().unwrap().auth_method = "oauth".into();
    fact.billing_plane.as_mut().unwrap().kind = "subscription".into();
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact]),
    );
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"thalamus-harness","via_vault":true}]);
        config["concurrency"] =
            json!({"enforcement_enabled":true,"providers":{"anthropic":{"limit":1}}});
    });
    let relay = harness.bind(reserved("thalamus-harness"), None).await;
    let parent = relay
        .call(
            "route.account_for",
            &json!({
                "provider":"anthropic", "model":"synthetic-model", "conversation_key":"relay-head",
                "admission_id":"new-parent", "head_turn":true, "warm_credential":PARENT,
            }),
        )
        .await;
    assert_eq!(response(&parent)["account"]["account_id"], PARENT);
    response(
        &relay
            .call(
                "route.account_observe",
                &json!({"event_id":"head-start", "kind":"admission_started",
        "admission_id":"new-parent", "run_id":"relay-head-run"}),
            )
            .await,
    );
    (harness, relay)
}

#[tokio::test]
async fn thalamus_fork_request_exact_bytes_admit_with_head_turn() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, relay) = relay_parent().await;
    let (kind, bytes) = relay.call_raw_params("route.account_for", REQUEST).await;
    assert_eq!(
        kind,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let reply: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(reply["result"]["account"]["account_id"], PARENT);
    assert_eq!(reply["result"]["account"]["credential_id"], PARENT);
    let state = crate::account_state::snapshot(harness.store()).unwrap();
    assert_eq!(
        state.inflight.count_for_model(
            &Default::default(),
            "anthropic",
            PARENT,
            "synthetic-model",
            NOW as i64
        ),
        2
    );
    crate::account_state::assert_published_equals_fresh_load(
        harness.store(),
        "Thalamus exact-byte fork admitted",
    );
}

#[tokio::test]
async fn thalamus_fork_hold_matches_recorded_reader_shape() {
    let _clock = crate::InjectedClock::set(NOW);
    // Use a fresh parent so the exact same fork id has not already committed:
    // a committed admission must replay its success even after a later block.
    let (harness, relay) = relay_parent().await;
    response(&relay.call("route.account_observe", &json!({
        "event_id":"head-limit", "kind":"account_limit_exhausted", "admission_id":"new-parent",
        "limit_window":"five_hour", "observed_at_ms":NOW, "resets_at_ms":NOW+3_600_000, "resets_at_source":"header",
    })).await);
    let (kind, bytes) = relay.call_raw_params("route.account_for", REQUEST).await;
    assert_eq!(
        kind,
        FrameType::Error,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let reply: Value = serde_json::from_slice(&bytes).unwrap();
    let fixture: Value = serde_json::from_slice(HOLD).unwrap();
    // The frame type carries the error envelope; compare its body to the
    // recorded envelope's error without reserializing routing's answer bytes.
    assert_eq!(reply["code"], fixture["error"]["code"]);
    assert_eq!(
        reply["detail"]["reason"],
        fixture["error"]["detail"]["reason"]
    );
    assert!(fixture["error"]["detail"]["until_ms"].is_u64());
    assert_eq!(reply["detail"]["until_ms"], NOW + 900_000);
    eprintln!(
        "ROUTING_FORK_HOLD_ANSWER_BASE64={}",
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes)
    );
    crate::account_state::assert_published_equals_fresh_load(
        harness.store(),
        "Thalamus exact-byte fork held",
    );
}
