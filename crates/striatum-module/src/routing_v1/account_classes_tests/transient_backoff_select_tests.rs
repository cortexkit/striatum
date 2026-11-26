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
async fn expired_transient_backoff_reopens_fresh_sidekick_select_without_sql_reads() {
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

    let reopened_at = retry_at + 1;
    let _reopened = crate::InjectedClock::set(reopened_at);
    let first_request = select.clone();
    let second_request = select.clone();
    let (first, second) = tokio::join!(
        owner.call("route.select", &first_request),
        owner.call("route.select", &second_request)
    );
    assert_eq!(first.0, FrameType::Response, "{}", first.1);
    assert_eq!(second.0, FrameType::Response, "{}", second.1);
    assert!(
        crate::account_state::published(harness.store(), reopened_at as i64)
            .unwrap()
            .fence_rechecks()
            .unwrap()
            .iter()
            .all(|origin| origin.kind != "observation")
    );

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
    trace.take();
    let owners = harness
        .store()
        .account_read(|tx| tx.probe_authorization_owners())
        .unwrap();
    assert!(owners.is_empty(), "expiry creates no recovery-test claim");
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
    assert_eq!(
        frame,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
}
