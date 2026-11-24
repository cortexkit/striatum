//! A head authorized by its session scope alone (not an enrolled runner)
//! continues its own admission by the public id the admit answer gave it.

use super::*;
use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};

const SPARE: &str = "spare-account";

fn two_oauth_accounts(
    harness: &Harness,
    mut credentials: crate::broca_credentials::BrocaCredentials,
) {
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "two-oauth".into(),
            credentials: [("oauth:test", "oauth-account"), ("oauth:spare", SPARE)]
                .into_iter()
                .map(|(id, account)| ListedCredential {
                    id: id.into(),
                    account_id: Some(account.into()),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec!["synthetic".into()],
                    serves: vec!["synthetic".into()],
                    credential_type: "oauth".into(),
                    auth_method: Some("oauth".into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    let mut state = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_provider_reply(&mut state, "test-source", &serde_json::to_vec(&json!({
        "result":(["oauth-account", SPARE].into_iter().map(|account| json!({"provider":"test-source","apiProvider":"synthetic","account":account,"source":"vault",
            "fetchedAt":crate::account_selection::rfc3339(NOW as i64),
            "usage":{"primary":{"usedPercent":10.0,"windowMinutes":300,
                "resetsAt":crate::account_selection::rfc3339(NOW as i64 + 3_600_000)}}})).collect::<Vec<_>>()),
        "completeProviders":["test-source"]
    })).unwrap(), NOW as i64).unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
}

fn continue_body(admission: &str, previous: &Value, selection: &str) -> Value {
    json!({"mode":"continue","run_id":"head-run","admission_id":admission,
        "conversation_key":"body-key","previous_decision_id":previous,"selection_id":selection,
        "exhausted":{"credential_id":"oauth:test","account":"oauth-account","limit_window":"five_hour",
            "observed_at_ms":NOW,"resets_at_ms":NOW+300_000}})
}

/// Admit and observe key the allocator's admission by the session reference
/// and the public id; continue must look it up the same way, and two heads of
/// one owner reusing an id or a selection id must not see each other's runs.
#[tokio::test]
async fn a_session_head_continues_its_admission_by_public_id() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    hosts(&[("host-one", true, true)]);
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["subscription"] = json!(true);
        config["providers"]["synthetic"]["accounts"] =
            json!({"strategy":"ordered","order":["oauth-account", SPARE]});
    });
    two_oauth_accounts(&harness, credentials);
    let head = session(&harness, "host-one", "head-a", "head").await;
    let admitted = response(&head.call("route.account_for", &request("id1")).await).clone();
    assert_eq!(admitted["account"]["account_id"], "oauth-account");
    response(
        &head
            .call(
                "route.account_observe",
                &json!({"event_id":"id1-start",
        "kind":"admission_started","admission_id":"id1","run_id":"head-run"}),
            )
            .await,
    );
    let previous = admitted["reason"]["decision_id"].clone();

    // Another head of the same owner, with the same ids, has no run to continue.
    let other = session(&harness, "host-one", "head-b", "head").await;
    let refused = other
        .call(
            "route.account_for",
            &continue_body("id1", &previous, "move"),
        )
        .await;
    assert_ne!(refused.1["result"]["kind"], "offer", "{}", refused.1);

    let offer = head
        .call(
            "route.account_for",
            &continue_body("id1", &previous, "move"),
        )
        .await;
    let offer = response(&offer).clone();
    assert_eq!(offer["kind"], "offer", "{offer}");
    assert_eq!(offer["account"], SPARE, "{offer}");
    // The same selection replays its answer.
    assert_eq!(
        response(
            &head
                .call(
                    "route.account_for",
                    &continue_body("id1", &previous, "move")
                )
                .await
        ),
        &offer
    );
    crate::account_state::assert_published_equals_fresh_load(harness.store(), "session continue");
}
