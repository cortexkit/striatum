use super::*;

mod tests;

const ACCOUNT: &str = "only-chatgpt-account";
const CREDENTIAL: &str = "oauth:only-chatgpt-login";
const MODELS: [&str; 3] = ["gpt-6-luna", "gpt-6.1-sol", "gpt-6.1-terra"];

fn one_account() -> Arc<Harness> {
    let harness = Harness::new("transient-backoff-select");
    let mut credentials = BrocaCredentials::default();
    // Only the three OpenAI routes adequate for this demand exist here, so a
    // refusal can come only from the account test held on them.
    rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .execute("DELETE FROM model_facts", [])
        .unwrap();
    let facts: Vec<_> = MODELS
        .iter()
        .map(|model| {
            let mut fact = crate::tests::fact("openai", model);
            fact.auth_methods = Some(vec!["oauth".into()]);
            fact.billing_plane = Some(BillingPlane {
                provider_id: "openai".into(),
                auth_method: "oauth".into(),
                kind: "subscription".into(),
                selection_reason: "single subscription account fixture".into(),
                rates: [("rate.output".into(), BillingRate::Priced { rate: 1.0 })]
                    .into_iter()
                    .collect(),
            });
            fact
        })
        .collect();
    harness.install_facts(&facts);
    for fact in &facts {
        catalog_fact(&harness, fact);
    }
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "one-account".into(),
            credentials: vec![ListedCredential {
                id: CREDENTIAL.into(),
                account_id: Some(ACCOUNT.into()),
                state: "active".into(),
                categories: vec!["llm-provider".into()],
                provider_ids: vec!["openai".into()],
                serves: vec!["openai".into()],
                credential_type: "oauth".into(),
                auth_method: Some("oauth".into()),
                ..Default::default()
            }],
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &facts, NOW as i64, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts),
    );
    credentials.publish_plan_prices(&json!({"prices":[]}));
    publish_fixture_providers(&mut credentials, "broca", &["openai"], true);
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"broca", "via_vault":true}]);
        config["consumers"] = json!({
            "prefrontal.mason":{"owner":"prefrontal", "mode":"demand"},
            "prefrontal.sidekick.map":{"owner":"prefrontal", "mode":"demand"}
        });
    });
    harness
}

fn admission(id: &str) -> Value {
    json!({"provider":"openai", "model":MODELS[0], "admission_id":id,
        "conversation_key":id, "candidates":{"view_digest":"one-account",
        "accounts":[{"account_id":ACCOUNT, "credential_id":CREDENTIAL}]}})
}

fn served(id: &str, event: &str) -> Value {
    json!({"event_id":event, "kind":"attempt", "admission_id":id,
        "served_account_id":ACCOUNT, "credential_id":CREDENTIAL,
        "attempt_outcome":{"status":"served"}})
}

#[tokio::test]
async fn expired_transient_backoff_with_inflight_probe_refuses_fresh_sidekick_select() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = one_account();
    let runner = harness.bind(reserved("broca"), None).await;
    let owner = harness
        .bind(reserved("prefrontal"), Some("pj-backoff-reproduction"))
        .await;
    let select = json!({"consumer":"prefrontal.sidekick.map",
        "requirements":{"iq":45,"eq":0}});
    let before = owner.call("route.select", &select).await;
    assert_eq!(before.0, FrameType::Response, "{}", before.1);
    assert_credential(
        &runner
            .call("route.account_for", &admission("older-run"))
            .await,
        CREDENTIAL,
    );
    let backoff_at = NOW + 1_000;
    let retry_at = backoff_at + 5_000;
    let _backoff = crate::InjectedClock::set(backoff_at);
    let observed = runner
        .call(
            "route.account_observe",
            &json!({"event_id":"five-second-backoff",
            "kind":"transient_backoff", "admission_id":"older-run",
            "served_account_id":ACCOUNT, "retry_at_ms":retry_at}),
        )
        .await;
    assert_eq!(observed.0, FrameType::Response, "{}", observed.1);

    let probe_at = backoff_at + 51_650;
    let _probe = crate::InjectedClock::set(probe_at);
    let mut mason_select = select.clone();
    mason_select["consumer"] = json!("prefrontal.mason");
    let selected = owner.call("route.select", &mason_select).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let mut probe = admission("probe-run");
    probe["model"] = selected.1["result"]["route"]["model"].clone();
    probe["decision_id"] = selected.1["result"]["decision_id"].clone();
    assert_credential(&runner.call("route.account_for", &probe).await, CREDENTIAL);
    let started = runner
        .call(
            "route.account_observe",
            &json!({"event_id":"probe-started",
        "kind":"admission_started", "admission_id":"probe-run", "run_id":"streaming-run"}),
        )
        .await;
    assert_eq!(started.0, FrameType::Response, "{}", started.1);
    let origin = harness
        .store()
        .fence_rechecks()
        .unwrap()
        .into_iter()
        .find(|origin| origin.kind == "observation")
        .unwrap();
    assert_eq!(
        origin.claim_owner, None,
        "admission consumes the decision's reservation"
    );
    assert_eq!(origin.last_claimed_at, Some(probe_at as i64));
    assert_eq!(
        origin.next_recheck_at,
        probe_at as i64 + striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS
    );

    let select_at = backoff_at + 58_128;
    let _during = crate::InjectedClock::set(select_at);
    assert!(select_at > retry_at);
    let ignored = runner
        .call(
            "route.account_observe",
            &served("older-run", "late-old-success"),
        )
        .await;
    assert_eq!(ignored.0, FrameType::Response, "{}", ignored.1);
    assert!(
        !ignored.1.to_string().contains("block_cleared"),
        "{}",
        ignored.1
    );
    let trace = harness.store().trace_block_reads_for_test().unwrap();
    let owners = harness
        .store()
        .account_read(|tx| tx.probe_authorization_owners())
        .unwrap();
    assert!(!owners.is_empty());
    let oracle_reads = trace.take();
    assert!(
        oracle_reads
            .reader
            .iter()
            .any(|sql| sql.contains("probe_authorization")),
        "the counter must see the SQL oracle: {oracle_reads:?}"
    );
    let (frame, bytes) = owner.call_raw("route.select", &select).await;
    let reads = trace.take();
    assert!(
        reads.reader.is_empty() && reads.writer.is_empty(),
        "live select must not read probe authorizations from SQL: {reads:?}"
    );
    println!("HELD_PROBE_REFUSAL={}", String::from_utf8_lossy(&bytes));
    assert_eq!(frame, FrameType::Error);
    let refusal: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(refusal["code"], "no_route", "{refusal}");
    assert_eq!(bytes, br#"{"code":"no_route","message":"An account that can serve this request is being tested. Check again shortly.","detail":{"reason":"accounts_blocked","reset_at_ms":1800000089128}}"#);
    let during = harness
        .store()
        .fence_rechecks()
        .unwrap()
        .into_iter()
        .find(|origin| origin.kind == "observation")
        .unwrap();
    assert_eq!(
        during, origin,
        "a check-again hint must not move the test clock"
    );
    let account_wait = runner
        .call("route.account_for", &admission("another-run"))
        .await;
    assert_eq!(account_wait.0, FrameType::Error, "{}", account_wait.1);
    assert_eq!(
        account_wait.1["code"], "account_pool_exhausted",
        "{}",
        account_wait.1
    );
    assert_eq!(
        account_wait.1["detail"]["reset_at_ms"],
        select_at + 30_000,
        "account_for gives the same check-again hint: {}",
        account_wait.1
    );
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let rejected: String = conn
        .query_row(
            "SELECT rejected_json FROM model_route_decisions ORDER BY rowid DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let rejected: Vec<Value> = serde_json::from_str(&rejected).unwrap();
    assert_eq!(rejected.len(), 3, "{rejected:?}");
    for model in MODELS {
        assert!(
            rejected
                .iter()
                .any(|route| route["routeKey"] == format!("openai/{model}")
                    && route["reason"] == "backoff_active"),
            "{rejected:?}"
        );
    }

    let _served = crate::InjectedClock::set(backoff_at + 80_434);
    let success = runner
        .call(
            "route.account_observe",
            &served("probe-run", "probe-first-step-served"),
        )
        .await;
    assert_eq!(success.0, FrameType::Response, "{}", success.1);
    assert!(
        success.1.to_string().contains("block_cleared"),
        "{}",
        success.1
    );
    assert!(harness
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .all(|origin| origin.kind != "observation"));
    let active: i64 = conn
        .query_row(
            "SELECT count(*) FROM block WHERE kind='observation' AND source_present=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let retired: i64 = conn
        .query_row(
            "SELECT count(*) FROM retirement_tombstone WHERE kind='observation'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!((active, retired), (0, 1));
    let after = owner.call("route.select", &select).await;
    assert_eq!(after.0, FrameType::Response, "{}", after.1);
}
