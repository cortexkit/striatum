use super::*;
use crate::account_quota::{apply_live_listing, AccountQuotaState};
use cortexkit_claustrum_client::decode_list_scoped_reply;
use serde_json::json;
use std::sync::Mutex;
use tokio::sync::{mpsc, oneshot};

fn notice(epoch: &str, seq: u64, view: &str) -> WatchNotice {
    WatchNotice::decode(
        &serde_json::to_vec(&json!({
            "kind": "changed", "epoch": epoch, "seq": seq, "view": view, "keys": "a2V5cw=="
        }))
        .unwrap(),
    )
    .unwrap()
}

fn listing(view: &str, state: &str) -> ScopedCredentialListing {
    decode_list_scoped_reply(json!({"result": {
        "view": view,
        "credentials": [{"id": "oauth:anthropic:login", "categories": ["llm-provider"],
            "type": "oauth", "serves": ["anthropic"], "provider_ids": ["anthropic"],
            "auth_method": "claude-code", "refresh_adapter": "claude",
            "state": state, "record_version": 1, "operations": ["get"], "account_id": "a"}],
        "grant_tuples": []
    }}))
    .unwrap()
}

#[test]
fn unchanged_digest_does_not_schedule_a_read() {
    let mut inventory = Inventory::default();
    inventory.tracker.opened();
    inventory.notice(&notice("epoch-a", 1, "view-a"));
    assert!(inventory.take_pending());
    inventory.applied("view-a", true);
    inventory.notice(&notice("epoch-a", 2, "view-a"));
    assert!(!inventory.take_pending());
    assert!(inventory.confirmed);
}

#[test]
fn a_read_ahead_of_the_coalesced_notice_becomes_confirmed_without_another_read() {
    let mut inventory = Inventory::default();
    inventory.tracker.opened();
    inventory.notice(&notice("epoch-a", 1, "view-a"));
    assert!(inventory.take_pending());
    inventory.applied("view-b", true);
    assert!(!inventory.confirmed);
    inventory.notice(&notice("epoch-a", 2, "view-b"));
    assert!(!inventory.take_pending());
    assert!(inventory.confirmed);
}

#[test]
fn epoch_change_rereads_even_an_unchanged_digest() {
    let mut inventory = Inventory::default();
    inventory.tracker.opened();
    inventory.notice(&notice("epoch-a", 1, "view-a"));
    assert!(inventory.take_pending());
    inventory.applied("view-a", true);
    inventory.notice(&notice("epoch-b", 1, "view-a"));
    assert!(inventory.take_pending());
    assert!(!inventory.confirmed);
}

#[test]
fn burst_coalesces_to_one_read_plus_one_during_that_read() {
    let mut inventory = Inventory::default();
    inventory.tracker.opened();
    for seq in 1..=20 {
        inventory.notice(&notice("epoch-a", seq, &format!("view-{seq}")));
    }
    assert!(inventory.take_pending());
    assert!(!inventory.take_pending(), "only one read may be started");
    // The first read is held open while more notices arrive.
    for seq in 21..=40 {
        inventory.notice(&notice("epoch-a", seq, &format!("view-{seq}")));
    }
    inventory.applied("view-20", true);
    assert!(
        !inventory.confirmed,
        "an older read cannot acknowledge newer notices"
    );
    assert!(inventory.take_pending());
    assert!(
        !inventory.take_pending(),
        "the burst adds only one follow-up"
    );
    inventory.applied("view-40", true);
    assert!(inventory.confirmed);
    inventory.notice(&notice("epoch-a", 41, "view-40"));
    assert!(!inventory.take_pending());
}

#[test]
fn unknown_method_falls_back_and_live_cadence_switches() {
    let mut inventory = Inventory::default();
    inventory.tracker.opened();
    assert_eq!(cadence(false), Duration::from_secs(60));
    inventory.notice(&notice("epoch-a", 1, "view-a"));
    inventory.take_pending();
    inventory.applied("view-a", true);
    assert_eq!(cadence(true), Duration::from_secs(900));
    assert_eq!(
        inventory.closed(ScopedWatchOutcome::Unsupported),
        WatchAction::Fallback
    );
    assert!(!inventory.confirmed);
    assert_eq!(cadence(false), Duration::from_secs(60));
    assert!(inventory.take_pending());
}

#[test]
fn pending_digest_and_dropped_old_watch_clear_confirmation_and_read_immediately() {
    let mut inventory = Inventory::default();
    inventory.tracker.opened();
    inventory.notice(&notice("epoch-a", 1, "view-a"));
    inventory.take_pending();
    inventory.applied("view-a", true);
    let mut credentials = crate::BrocaCredentials::default();
    credentials.apply_listing(&listing("view-a", "active"), 1);
    credentials.set_watch_confirmed(
        inventory
            .latest_view
            .as_deref()
            .filter(|_| inventory.confirmed),
    );
    assert!(credentials.listing_is_fresh(900_002));
    inventory.notice(&notice("epoch-a", 2, "view-b"));
    credentials.set_watch_confirmed(
        inventory
            .latest_view
            .as_deref()
            .filter(|_| inventory.confirmed),
    );
    assert!(!credentials.listing_is_fresh(900_002));
    assert!(inventory.take_pending());
    inventory.applied("view-b", true);
    credentials.apply_listing(&listing("view-b", "active"), 1);
    credentials.set_watch_confirmed(
        inventory
            .latest_view
            .as_deref()
            .filter(|_| inventory.confirmed),
    );
    assert!(credentials.listing_is_fresh(900_002));
    assert_eq!(
        inventory.closed(ScopedWatchOutcome::Ended),
        WatchAction::ResubscribeAndReread
    );
    credentials.set_watch_confirmed(
        inventory
            .latest_view
            .as_deref()
            .filter(|_| inventory.confirmed),
    );
    assert!(!credentials.listing_is_fresh(900_002));
    assert!(
        inventory.take_pending(),
        "loss is due now, without a poll tick"
    );
}

type Reply = oneshot::Sender<ScopedCredentialListing>;
struct FakeRoute {
    stream: Mutex<Option<mpsc::UnboundedReceiver<Result<WatchNotice, ScopedWatchOutcome>>>>,
    reads: mpsc::UnboundedSender<Reply>,
}
struct FakeStream(mpsc::UnboundedReceiver<Result<WatchNotice, ScopedWatchOutcome>>);

#[async_trait::async_trait]
impl NoticeStream for FakeStream {
    async fn event(&mut self) -> Result<WatchNotice, ScopedWatchOutcome> {
        self.0
            .recv()
            .await
            .unwrap_or(Err(ScopedWatchOutcome::Ended))
    }
}

#[async_trait::async_trait]
impl Route for FakeRoute {
    async fn open(&self) -> Result<Box<dyn NoticeStream>, CredentialResolverError> {
        Ok(Box::new(FakeStream(
            self.stream.lock().unwrap().take().unwrap(),
        )))
    }
    async fn list(&self) -> Result<ScopedCredentialListing, CredentialResolverError> {
        let (reply, receive) = oneshot::channel();
        self.reads.send(reply).unwrap();
        Ok(receive.await.unwrap())
    }
}

#[tokio::test]
async fn needs_reauth_notice_reloads_and_removes_login_without_polling() {
    let scratch = cortexkit_test_support::ScratchDir::new("vault-watch-listing");
    let store = Arc::new(
        striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
            "striatum",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap(),
    );
    let state = Arc::new(Mutex::new(AccountQuotaState::default()));
    let credentials = Arc::new(Mutex::new(crate::BrocaCredentials::default()));
    // Seed the saved startup view; the watch must invalidate this active login.
    credentials
        .lock()
        .unwrap()
        .apply_listing(&listing("view-a", "active"), crate::now_ms());
    let fact = crate::tests::fact("anthropic", "sonnet");
    assert_eq!(
        credentials.lock().unwrap().serving_identities(&fact).len(),
        1
    );
    let (events, stream) = mpsc::unbounded_channel();
    let (reads, mut requests) = mpsc::unbounded_channel();
    let route = Arc::new(FakeRoute {
        stream: Mutex::new(Some(stream)),
        reads,
    });
    let persisted = Arc::new(tokio::sync::Mutex::new(None));
    let latest = Arc::new(Mutex::new(None));
    let task = tokio::spawn(run(route, Arc::clone(&state), Arc::clone(&credentials), {
        let (state, credentials) = (Arc::clone(&state), Arc::clone(&credentials));
        move |reply| {
            let (state, credentials, store, persisted, latest) = (
                Arc::clone(&state),
                Arc::clone(&credentials),
                Arc::clone(&store),
                Arc::clone(&persisted),
                Arc::clone(&latest),
            );
            async move {
                apply_live_listing(reply, &state, &store, &credentials, &persisted, &latest).await
            }
        }
    }));
    // No timer advances: every list request is explicitly released by the fake vault.
    let first = requests.recv().await.unwrap();
    events.send(Ok(notice("epoch-a", 1, "view-a"))).unwrap();
    while state.lock().unwrap().vault_watch.last_seq != Some(1) {
        tokio::task::yield_now().await;
    }
    first.send(listing("view-a", "active")).unwrap();
    requests
        .recv()
        .await
        .unwrap()
        .send(listing("view-a", "active"))
        .unwrap();
    while !credentials.lock().unwrap().listing_is_fresh(u64::MAX) {
        tokio::task::yield_now().await;
    }
    events.send(Ok(notice("epoch-a", 2, "view-b"))).unwrap();
    requests
        .recv()
        .await
        .unwrap()
        .send(listing("view-b", "needs_reauth"))
        .unwrap();
    while state
        .lock()
        .unwrap()
        .roster
        .as_ref()
        .is_none_or(|roster| roster.view != "view-b")
    {
        tokio::task::yield_now().await;
    }
    assert!(
        credentials
            .lock()
            .unwrap()
            .serving_identities(&fact)
            .is_empty(),
        "the login must stop being a routing candidate when the watched listing marks needs_reauth"
    );
    assert_eq!(state.lock().unwrap().vault_watch.last_seq, Some(2));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}
