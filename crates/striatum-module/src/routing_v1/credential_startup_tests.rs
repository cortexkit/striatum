//! Startup distinguishes a missing vault view from a loaded, empty view.

use std::sync::Arc;

use cortexkit_claustrum_client::{ListedCredential, ListedGrant, ScopedCredentialListing};
use serde_json::{json, Value};
use striatum_store::SqliteRouteStateStore;
use subc_protocol::FrameType;

use super::account_classes_tests::setup;
use super::conformance_tests::{reserved, Harness};
use crate::{account_quota, BrocaCredentials};

const NOW: u64 = 1_800_000_000_000;

fn request(id: &str, legacy: bool) -> Value {
    let mut request = json!({"provider":"deepseek", "model":"model",
        "conversation_key": format!("conversation-{id}"), "admission_id":id});
    if legacy {
        request["caller"] = json!({"runner":"class-runner"});
        request["mode"] = json!("admit");
    }
    request
}

fn empty_listing() -> ScopedCredentialListing {
    ScopedCredentialListing {
        view: "empty-vault".into(),
        grant_tuples: ["llm-provider", "llm-gateway", "media-provider"]
            .into_iter()
            .map(|category| ListedGrant {
                selector_kind: "category".into(),
                selector: category.into(),
                operation: "list".into(),
            })
            .collect(),
        ..Default::default()
    }
}

async fn cold_start(legacy: bool) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("deepseek", false);
    harness.set_credentials(BrocaCredentials::default());
    let runner = harness.bind(reserved("class-runner"), None).await;
    let started = std::time::Instant::now();
    let missing = runner
        .call("route.account_for", &request("before-listing", legacy))
        .await;
    assert_eq!(missing.0, FrameType::Error, "{}", missing.1);
    assert_eq!(
        missing.1["code"], "account_selection_unavailable",
        "{}",
        missing.1
    );
    assert_eq!(
        missing.1["detail"]["cause"], "credential_listing_not_loaded",
        "{}",
        missing.1
    );
    assert_eq!(missing.1["detail"]["retry_after_s"], 1, "{}", missing.1);
    assert!(started.elapsed() >= std::time::Duration::from_millis(1400));
    assert!(started.elapsed() < std::time::Duration::from_secs(3));

    // Loading even an empty view permanently ends the startup refusal. A later
    // outage or stale timestamp must not send the caller into unlimited retries.
    {
        let credentials = harness.credentials();
        let mut guard = credentials.lock().unwrap();
        guard.apply_listing(&empty_listing(), 1);
        guard.note_failure("vault unreachable after the first listing");
    }
    let loaded = runner
        .call("route.account_for", &request("after-listing", legacy))
        .await;
    assert_eq!(loaded.0, FrameType::Error, "{}", loaded.1);
    assert_eq!(loaded.1["code"], "auth_required", "{}", loaded.1);
    assert!(loaded.1["detail"].get("cause").is_none(), "{}", loaded.1);
}

#[tokio::test]
async fn cold_start_v1_never_claims_auth_is_required_before_a_listing() {
    cold_start(false).await;
}

#[tokio::test]
async fn cold_start_legacy_never_claims_auth_is_required_before_a_listing() {
    cold_start(true).await;
}

#[tokio::test]
async fn cold_start_first_live_listing_wakes_the_request_without_blocking_tokio() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("deepseek", false);
    harness.set_credentials(BrocaCredentials::default());
    let runner = harness.bind(reserved("class-runner"), None).await;
    let credentials = harness.credentials();
    let published = Arc::new(AtomicBool::new(false));
    let publisher_done = Arc::clone(&published);
    let publisher = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        credentials
            .lock()
            .unwrap()
            .apply_listing(&empty_listing(), NOW);
        publisher_done.store(true, Ordering::SeqCst);
    });
    let started = std::time::Instant::now();
    let reply = runner
        .call("route.account_for", &request("first-live-listing", false))
        .await;
    assert!(
        published.load(Ordering::SeqCst),
        "request must wait for the view"
    );
    assert_eq!(reply.1["code"], "auth_required", "{}", reply.1);
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
    publisher.await.unwrap();
}

#[tokio::test]
async fn cold_start_media_waits_for_a_view_but_loaded_empty_means_auth_required() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("media-credential-startup");
    super::consumers::replace_catalog(&json!([{
        "module_id":"colliculus", "nonce_verified":true,
        "role_declarations":{"media-provider/v1":{"engines":[{
            "kind":"transcribe", "provider":"media-vendor", "model":"transcriber", "local":false,
            "latency_class":"interactive", "formats":["audio/wav"], "languages":["en"]
        }]}}
    }]));
    harness.set_credentials(BrocaCredentials::default());
    let route = harness.bind(reserved("colliculus"), None).await;
    let mut request = json!({"provider":"media-vendor", "model":"transcriber",
        "conversation_key":"media-startup", "admission_id":"before-media-listing"});
    let missing = route.call("route.account_for", &request).await;
    assert_eq!(missing.0, FrameType::Error, "{}", missing.1);
    assert_eq!(
        missing.1["code"], "account_selection_unavailable",
        "{}",
        missing.1
    );
    assert_eq!(
        missing.1["detail"]["cause"], "credential_listing_not_loaded",
        "{}",
        missing.1
    );
    assert_eq!(missing.1["detail"]["retry_after_s"], 1, "{}", missing.1);
    harness
        .credentials()
        .lock()
        .unwrap()
        .apply_listing(&empty_listing(), NOW);
    request["admission_id"] = json!("after-media-listing");
    let loaded = route.call("route.account_for", &request).await;
    assert_eq!(loaded.0, FrameType::Error, "{}", loaded.1);
    assert_eq!(loaded.1["code"], "auth_required", "{}", loaded.1);
}

fn restarted_credentials(listing: &ScopedCredentialListing) -> BrocaCredentials {
    let scratch = cortexkit_test_support::ScratchDir::new("credential-listing-restart");
    let path = scratch.path().join("store.db");
    let descriptor = striatum_store::dev_descriptor("striatum", path.to_string_lossy());
    {
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let row = account_quota::apply_roster_listing(&mut Default::default(), listing, NOW as i64)
            .unwrap();
        crate::account_writer::put_account_vault_roster(&store, &row).unwrap();
    }
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    let mut credentials = BrocaCredentials::default();
    account_quota::restore_credential_listing(&store, &mut credentials);
    credentials
}

async fn assert_empty_on_both_wires(harness: &Arc<Harness>) {
    let runner = harness.bind(reserved("class-runner"), None).await;
    for legacy in [false, true] {
        let reply = runner
            .call(
                "route.account_for",
                &request(&format!("empty-{legacy}"), legacy),
            )
            .await;
        assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
        assert_eq!(reply.1["code"], "auth_required", "{}", reply.1);
    }
}

#[tokio::test]
async fn cold_start_restored_empty_listing_is_authoritatively_empty() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("deepseek", false);
    let credentials = restarted_credentials(&empty_listing());
    harness.set_credentials(credentials);
    assert_empty_on_both_wires(&harness).await;
}

#[tokio::test]
async fn cold_start_persisted_api_key_serves_immediately_after_restart() {
    use striatum_core::{FactSource, RouteStateStore};
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("deepseek", false);
    let mut fact = harness
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|fact| fact.provider_id == "deepseek")
        .unwrap();
    fact.auth_methods = Some(vec!["apikey".into()]);
    harness
        .store()
        .upsert_fact(&fact, FactSource::Override)
        .unwrap();
    let mut listing = empty_listing();
    listing.view = "deepseek-key".into();
    listing.credentials.push(ListedCredential {
        id: "apikey:deepseek".into(),
        credential_type: "apikey".into(),
        auth_method: Some("apikey".into()),
        categories: vec!["llm-provider".into()],
        provider_ids: vec!["deepseek".into()],
        serves: vec!["deepseek".into()],
        state: "active".into(),
        ..Default::default()
    });
    let credentials = restarted_credentials(&listing);
    assert_eq!(
        credentials.listed_credentials(),
        listing.credentials.as_slice()
    );
    harness.set_credentials(credentials);
    let runner = harness.bind(reserved("class-runner"), None).await;
    for legacy in [false, true] {
        let started = std::time::Instant::now();
        let reply = runner
            .call(
                "route.account_for",
                &request(&format!("restored-{legacy}"), legacy),
            )
            .await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(
            reply.1["result"]["account"]["credential_id"], "apikey:deepseek",
            "{}",
            reply.1
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }
}
