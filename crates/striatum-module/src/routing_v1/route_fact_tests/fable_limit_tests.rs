use super::*;
use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};

const RUNNER: &str = "thalamus-harness";
const SUBJECT: &str = "ck.box_test.event.thalamus-harness.route_fact.v1";
const FABLE: &str = "claude-fable-5-2";
const OPUS: &str = "claude-opus-5-5";
const A: &str = "00000000-0000-4000-8000-00000000000a";
const B: &str = "00000000-0000-4000-8000-00000000000b";
const A_CREDENTIAL: &str = "oauth:anthropic";
const B_CREDENTIAL: &str = "oauth:anthropic:second";
const AT: i64 = 1_795_700_000_000;
const READING_RESET: i64 = 1_795_789_000_000;
const START: &[u8] =
    include_bytes!("../../../fixtures/route-limit-fable-v1/admission_started.json");
const LIMIT: &[u8] =
    include_bytes!("../../../fixtures/route-limit-fable-v1/account_limit_exhausted.json");

async fn setup() -> (Arc<Harness>, Route) {
    let harness = Harness::new("relayed-fable-limit");
    let facts = [FABLE, OPUS].map(|model| {
        let mut fact = super::super::account_classes_tests::account_fact("anthropic");
        fact.model_id = model.into();
        fact.auth_methods = Some(vec!["oauth".into()]);
        let plane = fact.billing_plane.as_mut().unwrap();
        plane.auth_method = "oauth".into();
        plane.kind = "subscription".into();
        fact
    });
    harness.install_facts(&facts);
    for fact in &facts {
        super::super::account_classes_tests::catalog_fact(&harness, fact);
    }
    let mut credentials = crate::broca_credentials::BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "claude-sign-ins".into(),
            credentials: [(A, A_CREDENTIAL), (B, B_CREDENTIAL)]
                .into_iter()
                .map(|(account, credential)| ListedCredential {
                    id: credential.into(),
                    account_id: Some(account.into()),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec!["anthropic".into()],
                    serves: vec!["anthropic".into()],
                    credential_type: "oauth".into(),
                    auth_method: Some("oauth".into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        AT as u64,
    );
    credentials.refresh_serving_methods(&Default::default(), &facts, AT, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts),
    );
    credentials.publish_plan_prices(&json!({"prices": []}));
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["providers"]["anthropic"]["subscription"] = json!(true);
        config["runners"] = json!([{"module":RUNNER,"via_vault":true}]);
    });
    let runner = harness.bind(reserved(RUNNER), None).await;
    (harness, runner)
}

fn readings(fetched_at: i64, a_fable_used: f64) {
    let entries = [(A, 64.0, a_fable_used), (B, 25.0, 15.0)].map(|(account, weekly, fable)| {
        json!({
            "provider":"claude", "apiProvider":"anthropic", "account":account,
            "source":"vault", "fetchedAt":crate::account_selection::rfc3339(fetched_at),
            "usage": {
                "primary":{"usedPercent":10.0,"windowMinutes":300},
                "secondary":{"usedPercent":weekly,"windowMinutes":10080},
                "extraRateWindows":[{"id":"Fable","title":"7 Day (Fable)","window":{
                    "usedPercent":fable,"windowKind":"weekly","windowMinutes":10080,
                    "resetsAt":crate::account_selection::rfc3339(READING_RESET)
                }}]
            }
        })
    });
    crate::account_quota::apply_provider_reply(
        &mut crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap(),
        "claude",
        &serde_json::to_vec(&json!({"result": entries, "completeProviders": ["claude"]})).unwrap(),
        fetched_at,
    )
    .unwrap();
}

async fn admit(
    runner: &Route,
    id: &str,
    session: &str,
    model: &str,
    only_a: bool,
) -> (FrameType, Value) {
    let mut body = json!({"provider":"anthropic", "model":model, "conversation_key":session,
        "admission_id":id, "hold_wait_max_s":0});
    if only_a {
        body["candidates"] = json!({"view_digest":"a-only",
            "accounts":[{"account_id":A,"credential_id":A_CREDENTIAL}]});
    }
    runner.call("route.account_for", &body).await
}

async fn exercise_relayed_fable_limit(with_reset: bool) {
    let _clock = crate::InjectedClock::set(AT as u64);
    let (harness, runner) = setup().await;
    readings(AT, 45.0);
    let first = result(admit(&runner, "pin-a", "sticky", FABLE, true).await);
    assert_eq!(first["account"]["account_id"], A);

    let receipt_at = AT + 60_000;
    let _receipt = crate::InjectedClock::set(receipt_at as u64);
    readings(receipt_at, 100.0);
    let start: Value = serde_json::from_slice(START).unwrap();
    // Use the sample's synthetic ID as the real admission ID to keep both
    // captures unchanged on the wire.
    let admission = start["params"]["admission_id"].as_str().unwrap();
    let sticky = result(admit(&runner, admission, "sticky", FABLE, false).await);
    assert_eq!(sticky["account"]["account_id"], A);
    assert_eq!(sticky["pin"]["id"], first["pin"]["id"]);
    assert_eq!(sticky["pin"]["generation"], first["pin"]["generation"]);
    assert_eq!(sticky["reason"]["code"], "sticky_hit");

    let mut limit: Value = serde_json::from_slice(LIMIT).unwrap();
    let no_reset;
    let limit_body = if with_reset {
        LIMIT
    } else {
        let params = limit["params"].as_object_mut().unwrap();
        assert!(params.remove("resets_at_ms").is_some());
        assert!(params.remove("resets_at_source").is_some());
        no_reset = serde_json::to_vec(&limit).unwrap();
        no_reset.as_slice()
    };
    let handler = harness.fact_handler();
    let (bus, mut cursor) = bus(&handler).await;
    cursor.expect_finished = false;
    for (fact, bytes) in [(&start, START), (&limit, limit_body)] {
        super::digest_tests::consume_notice(
            &handler,
            &bus,
            &mut cursor,
            SUBJECT,
            fact["params"]["event_id"].as_str().unwrap(),
            digest_headers(ContentDigest::of_bytes(bytes)),
            bytes,
        )
        .await;
    }
    assert_eq!((cursor.acks, cursor.terms), (2, 0));
    assert_publisher(&handler, RUNNER, [2, 0, 0, 0], Some(receipt_at as u64));

    let moved = result(admit(&runner, "move-to-b", "sticky", FABLE, false).await);
    assert_eq!(moved["account"]["account_id"], B);
    assert_ne!(moved["pin"]["id"], sticky["pin"]["id"]);
    let fresh = result(admit(&runner, "cold-fable", "cold-fable", FABLE, false).await);
    assert_eq!(fresh["account"]["account_id"], B);
    let opus = result(admit(&runner, "opus-on-a", "opus", OPUS, true).await);
    assert_eq!(opus["account"]["account_id"], A);

    let fences = || {
        harness
            .store()
            .account_read(|tx| tx.active_pool_fences("anthropic"))
            .unwrap()
    };
    let still_full_at = receipt_at + 60_000;
    let _still_full = crate::InjectedClock::set(still_full_at as u64);
    readings(still_full_at, 100.0);
    let cold = result(admit(&runner, "still-full", "still-full", FABLE, false).await);
    assert_eq!(cold["account"]["account_id"], B);
    assert_eq!(
        fences().len(),
        1,
        "another 100% reading must not clear the fence"
    );

    let recovered_at = still_full_at + 60_000;
    let _recovered = crate::InjectedClock::set(recovered_at as u64);
    readings(recovered_at, 64.0);
    let recovered = admit(&runner, "recovered", "recovered", FABLE, true).await;
    assert_eq!(
        recovered.0,
        FrameType::Response,
        "fresh Insula Fable headroom must clear A's fence: {}",
        recovered.1
    );
    assert_eq!(recovered.1["result"]["account"]["account_id"], A);
    assert!(fences().is_empty(), "the recovered fence must be retired");
}

#[tokio::test]
async fn relayed_fable_limit_with_header_clears_on_fresh_insula_reading() {
    exercise_relayed_fable_limit(true).await;
}

#[tokio::test]
async fn relayed_fable_limit_without_header_clears_on_fresh_insula_reading() {
    exercise_relayed_fable_limit(false).await;
}
