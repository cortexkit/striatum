use super::*;
use cortexkit_claustrum_client::ListedCredentialCondition;

const FIRST: &str = "oauth:synthetic";
const SECOND: &str = "oauth:synthetic:second";

fn login(id: &str, account_id: &str, reason: Option<&str>) -> ListedCredential {
    ListedCredential {
        id: id.into(),
        account_id: Some(account_id.into()),
        credential_type: "oauth".into(),
        categories: vec!["llm-provider".into()],
        provider_ids: vec!["synthetic".into()],
        auth_method: Some("oauth".into()),
        state: "active".into(),
        condition: reason.map(|reason| ListedCredentialCondition {
            reason: reason.into(),
            since_ms: 7,
        }),
        ..Default::default()
    }
}

fn listing(view: &str, rows: Vec<ListedCredential>) -> ScopedCredentialListing {
    ScopedCredentialListing {
        view: view.into(),
        credentials: rows,
        ..empty_listing()
    }
}

fn apply(harness: &Harness, view: &str, rows: Vec<ListedCredential>) {
    harness
        .credentials()
        .lock()
        .unwrap()
        .apply_listing(&listing(view, rows), NOW);
}

fn admission(id: &str) -> Value {
    let mut value = request(id, false);
    value["provider"] = json!("synthetic");
    value
}

fn assert_admitted(reply: (FrameType, Value), id: &str) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(
        reply.1["result"]["account"]["credential_id"], id,
        "{}",
        reply.1
    );
    reply.1["result"].clone()
}

fn assert_login_required(reply: (FrameType, Value), reasons: &[&str], ids: &[&str]) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "auth_required", "{}", reply.1);
    assert_eq!(
        reply.1["detail"],
        json!({
            "cause":"login_condition", "reasons":reasons, "credential_ids":ids
        })
    );
}

#[tokio::test]
async fn login_condition_admission_uses_only_unmarked_logins() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    apply(
        &harness,
        "mixed",
        vec![
            login(FIRST, "account-a", Some("provider_ineligible")),
            login(SECOND, "account-b", None),
        ],
    );
    let runner = harness.bind(reserved("class-runner"), None).await;
    let mut request = admission("offered-only-marked");
    request["candidates"] = json!({"view_digest":"mixed", "accounts":[
        {"account_id":"account-a", "credential_id":FIRST}
    ]});
    assert_login_required(
        runner.call("route.account_for", &request).await,
        &["provider_ineligible"],
        &[FIRST],
    );
    assert_admitted(
        runner
            .call("route.account_for", &admission("unmarked-spare"))
            .await,
        SECOND,
    );
}

#[tokio::test]
async fn login_condition_moves_an_existing_session() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    apply(
        &harness,
        "before-condition",
        vec![login(FIRST, "account-a", None)],
    );
    let runner = harness.bind(reserved("class-runner"), None).await;
    let mut request = admission("already-placed");
    let first = assert_admitted(runner.call("route.account_for", &request).await, FIRST);
    let started = runner.call("route.account_observe", &json!({
        "event_id":"condition-session-started", "kind":"admission_started", "admission_id":"already-placed"
    })).await;
    assert_eq!(started.0, FrameType::Response, "{}", started.1);
    apply(
        &harness,
        "during-session",
        vec![
            login(FIRST, "account-a", Some("provider_verification_required")),
            login(SECOND, "account-b", None),
        ],
    );
    request["admission_id"] = json!("next-account-request");
    let second = assert_admitted(runner.call("route.account_for", &request).await, SECOND);
    assert_ne!(first["pin"]["id"], second["pin"]["id"]);
}

#[tokio::test]
async fn login_condition_all_marked_requires_auth_with_sorted_detail() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    apply(
        &harness,
        "all-marked",
        vec![
            login(SECOND, "account-b", Some("future_provider_reason")),
            login(FIRST, "account-a", Some("provider_verification_required")),
        ],
    );
    let runner = harness.bind(reserved("class-runner"), None).await;
    for legacy in [false, true] {
        let mut request = admission(&format!("all-marked-{legacy}"));
        if legacy {
            request["caller"] = json!({"runner":"class-runner"});
        }
        assert_login_required(
            runner.call("route.account_for", &request).await,
            &["future_provider_reason", "provider_verification_required"],
            &[FIRST, SECOND],
        );
    }
}

#[tokio::test]
async fn login_condition_clear_restores_the_next_admission() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let runner = harness.bind(reserved("class-runner"), None).await;
    apply(
        &harness,
        "marked",
        vec![login(FIRST, "account-a", Some("provider_ineligible"))],
    );
    let request = admission("retry-after-clear");
    assert_login_required(
        runner.call("route.account_for", &request).await,
        &["provider_ineligible"],
        &[FIRST],
    );
    apply(&harness, "cleared", vec![login(FIRST, "account-a", None)]);
    assert_admitted(runner.call("route.account_for", &request).await, FIRST);
}

#[tokio::test]
async fn login_condition_restores_before_the_first_live_listing() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let snapshot = listing(
        "saved-mark",
        vec![login(FIRST, "account-a", Some("provider_ineligible"))],
    );
    harness.set_credentials(restarted_credentials(&snapshot));
    let runner = harness.bind(reserved("class-runner"), None).await;
    assert_login_required(
        runner
            .call("route.account_for", &admission("from-restart"))
            .await,
        &["provider_ineligible"],
        &[FIRST],
    );
}

#[tokio::test]
async fn login_condition_status_is_sorted_and_preserves_unmarked_bytes() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let request = json!({"provider":"synthetic", "account_id":"shared-account"});
    let old = json!({"result":{
        "provider":"synthetic", "account_id":"shared-account", "observed_at":null,
        "age_band":null, "windows":[], "warm_ok":false,
        "reason":{"code":"account_unknown"}, "limit_state":"unknown"
    }});
    apply(
        &harness,
        "status-unmarked",
        vec![login(FIRST, "shared-account", None)],
    );
    let unmarked = runner.call("route.account_status", &request).await;
    assert_eq!(unmarked.0, FrameType::Response, "{}", unmarked.1);
    assert_eq!(
        serde_json::to_vec(&unmarked.1).unwrap(),
        serde_json::to_vec(&old).unwrap()
    );

    apply(
        &harness,
        "status-marked",
        vec![
            login(SECOND, "shared-account", Some("provider_ineligible")),
            login(
                FIRST,
                "shared-account",
                Some("provider_verification_required"),
            ),
            login("oauth:synthetic:unmarked", "shared-account", None),
            login(
                "oauth:synthetic:unrelated",
                "other-account",
                Some("provider_ineligible"),
            ),
        ],
    );
    let mut expected = old;
    expected["result"]["credentials"] = json!([
        {"credential_id":FIRST, "condition":{"reason":"provider_verification_required", "since_ms":7}},
        {"credential_id":SECOND, "condition":{"reason":"provider_ineligible", "since_ms":7}},
    ]);
    assert_eq!(
        runner.call("route.account_status", &request).await,
        (FrameType::Response, expected)
    );
}
