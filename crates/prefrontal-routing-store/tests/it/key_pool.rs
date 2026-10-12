//! The key-pool store: leases count as runs in flight only while renewed, a
//! session keeps its workspace across runs, walls skip a key, and a pool with
//! no admissible key refuses instead of guessing.

use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_core::key_pool::PoolKey;
use prefrontal_routing_store::{
    dev_descriptor, PoolAcquire, PoolAcquireOutcome, PoolLease, PoolWallKind, SqliteRouteStateStore,
};

static NEXT_RIG: AtomicU64 = AtomicU64::new(0);
const PROVIDER: &str = "anthropic-platform";
const LEASE_MS: i64 = 600_000;
const IDLE_MS: i64 = 7 * 24 * 3_600_000;

struct Rig {
    dir: std::path::PathBuf,
}

impl Rig {
    fn new(label: &str) -> Self {
        let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
        let dir = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-key-pool-{label}-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn open(&self) -> SqliteRouteStateStore {
        let path = self.dir.join("store.db").to_string_lossy().into_owned();
        SqliteRouteStateStore::open(&dev_descriptor("prefrontal-routing", path))
            .expect("open route-state store")
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn acquire(
    store: &SqliteRouteStateStore,
    keys: &[PoolKey],
    session: &str,
    now: i64,
) -> PoolAcquireOutcome {
    store
        .pool_acquire(&PoolAcquire {
            provider: PROVIDER,
            session_id: session,
            keys,
            now_ms: now,
            lease_ms: LEASE_MS,
            affinity_idle_ms: IDLE_MS,
        })
        .unwrap()
}

fn leased(outcome: PoolAcquireOutcome) -> PoolLease {
    match outcome {
        PoolAcquireOutcome::Leased(lease) => lease,
        PoolAcquireOutcome::Exhausted(blocked) => panic!("no key: {blocked:?}"),
    }
}

#[test]
fn concurrent_sessions_spread_across_keys_by_runs_in_flight() {
    let rig = Rig::new("spread");
    let store = rig.open();
    let keys = [
        PoolKey::new("apikey:anthropic", None, "active"),
        PoolKey::new("apikey:anthropic:b", None, "active"),
    ];
    let first = leased(acquire(&store, &keys, "ses-1", 1_000));
    let second = leased(acquire(&store, &keys, "ses-2", 1_001));
    assert_ne!(
        first.credential_id, second.credential_id,
        "the second run takes the idle key"
    );
    // A third session finds one run on each key and takes the least recently used.
    let third = leased(acquire(&store, &keys, "ses-3", 1_002));
    assert_eq!(third.credential_id, first.credential_id);
}

#[test]
fn a_lease_from_a_core_that_died_stops_counting_after_expiry() {
    let rig = Rig::new("expiry");
    let store = rig.open();
    let keys = [
        PoolKey::new("apikey:anthropic:a", None, "active"),
        PoolKey::new("apikey:anthropic:b", None, "active"),
    ];
    let dead = leased(acquire(&store, &keys, "ses-dead", 1_000));
    assert_eq!(
        store
            .pool_key_states(PROVIDER, 1_000 + LEASE_MS - 1)
            .unwrap()[&dead.credential_id]
            .in_flight,
        1
    );
    // Nobody renews it: past its validity it no longer counts.
    assert_eq!(
        store
            .pool_key_states(PROVIDER, 1_000 + LEASE_MS + 1)
            .unwrap()[&dead.credential_id]
            .in_flight,
        0
    );
    // A renewed lease keeps counting, and an expired one whose run is still live
    // is reinstated on the same key.
    let live = leased(acquire(&store, &keys, "ses-live", 2_000));
    let later = 2_000 + 2 * LEASE_MS;
    assert!(store
        .pool_renew(
            PROVIDER,
            "ses-live",
            &live.lease_id,
            Some(&live.credential_id),
            later,
            LEASE_MS
        )
        .unwrap()
        .is_some());
    assert_eq!(
        store.pool_key_states(PROVIDER, later + 1).unwrap()[&live.credential_id].in_flight,
        1
    );
    // Release is idempotent and stops counting at once.
    assert_eq!(
        store
            .pool_release(PROVIDER, "ses-live", None, false, later + 2)
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .pool_release(PROVIDER, "ses-live", None, false, later + 3)
            .unwrap(),
        0
    );
    assert_eq!(
        store.pool_key_states(PROVIDER, later + 4).unwrap()[&live.credential_id].in_flight,
        0
    );
    assert!(
        store
            .pool_renew(
                PROVIDER,
                "ses-live",
                &live.lease_id,
                Some(&live.credential_id),
                later + 5,
                LEASE_MS
            )
            .unwrap()
            .is_none(),
        "a released lease is not revived by a late renewal"
    );
}

#[test]
fn a_session_keeps_its_workspace_across_a_revival_and_moves_only_when_it_is_walled() {
    let rig = Rig::new("affinity");
    let store = rig.open();
    let keys = [
        PoolKey::new("apikey:anthropic:w1a", Some("ws-1"), "active"),
        PoolKey::new("apikey:anthropic:w1b", Some("ws-1"), "active"),
        PoolKey::new("apikey:anthropic:w2a", Some("ws-2"), "active"),
    ];
    let launch = leased(acquire(&store, &keys, "ses-1", 1_000));
    let bound = launch.workspace.clone();
    // The run ends; other sessions load the bound workspace.
    store
        .pool_release(PROVIDER, "ses-1", None, false, 1_100)
        .unwrap();
    for session in ["ses-x", "ses-y"] {
        let other = leased(acquire(&store, &keys, session, 1_200));
        let _ = other;
    }
    let revival = leased(acquire(&store, &keys, "ses-1", 1_300));
    assert_eq!(
        revival.workspace, bound,
        "a revival stays in its prompt-cache workspace"
    );
    assert_eq!(revival.moved_from, None);
    // Every key of the bound workspace is walled: the next run moves and says why.
    store
        .pool_release(PROVIDER, "ses-1", None, false, 1_400)
        .unwrap();
    for key in keys.iter().filter(|key| key.workspace == bound) {
        store
            .pool_wall(
                PROVIDER,
                &key.credential_id,
                PoolWallKind::Billing,
                10_000,
                "billing_exhausted",
                1_400,
            )
            .unwrap();
    }
    let moved = leased(acquire(&store, &keys, "ses-1", 1_500));
    assert_ne!(moved.workspace, bound);
    assert_eq!(moved.moved_from.as_deref(), Some(bound.as_str()));
    assert!(moved.move_reason.unwrap().contains("billing_exhausted"));
    // Ending the session forgets the workspace.
    store
        .pool_release(PROVIDER, "ses-1", None, true, 1_600)
        .unwrap();
}

#[test]
fn a_walled_key_is_skipped_and_an_all_walled_pool_refuses_with_every_reason() {
    let rig = Rig::new("walls");
    let store = rig.open();
    let keys = [
        PoolKey::new("apikey:anthropic", None, "active"),
        PoolKey::new("apikey:anthropic:b", None, "active"),
        PoolKey::new("apikey:anthropic:c", None, "needs_reauth"),
    ];
    store
        .pool_wall(
            PROVIDER,
            "apikey:anthropic",
            PoolWallKind::Billing,
            50_000,
            "billing_exhausted",
            1_000,
        )
        .unwrap();
    let lease = leased(acquire(&store, &keys, "ses-1", 1_000));
    assert_eq!(lease.credential_id, "apikey:anthropic:b");
    store
        .pool_wall(
            PROVIDER,
            "apikey:anthropic:b",
            PoolWallKind::RateLimited,
            31_000,
            "rate_limited",
            1_000,
        )
        .unwrap();
    // The session's live lease is on a key that just became inadmissible, so it
    // is released and nothing else is admissible.
    let PoolAcquireOutcome::Exhausted(blocked) = acquire(&store, &keys, "ses-1", 1_001) else {
        panic!("an all-walled pool must refuse");
    };
    let reasons: Vec<(String, String, Option<i64>)> = blocked
        .into_iter()
        .map(|block| (block.credential_id, block.reason, block.reset_at_ms))
        .collect();
    assert_eq!(
        reasons,
        vec![
            (
                "apikey:anthropic".into(),
                "billing_exhausted".into(),
                Some(50_000)
            ),
            (
                "apikey:anthropic:b".into(),
                "rate_limited".into(),
                Some(31_000)
            ),
            (
                "apikey:anthropic:c".into(),
                "vault_state:needs_reauth".into(),
                None
            ),
        ]
    );
    // The cooldown passes and the key is admissible again.
    assert_eq!(
        leased(acquire(&store, &keys, "ses-1", 31_001)).credential_id,
        "apikey:anthropic:b"
    );
}
