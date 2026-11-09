use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_store::{
    dev_descriptor, PersistedAccountReading, PersistedProviderCompleteness, PersistedVaultRoster,
    SqliteRouteStateStore,
};

static NEXT_RIG: AtomicU64 = AtomicU64::new(0);

struct Rig {
    dir: std::path::PathBuf,
}

impl Rig {
    fn new(label: &str) -> Self {
        let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "prefrontal-account-quota-{label}-{}-{id}",
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

fn reading(account: &str, fetched_at: &str, received_at: i64) -> PersistedAccountReading {
    PersistedAccountReading {
        provider: "claude".into(),
        account_id: account.into(),
        entry_json: format!(
            r#"{{"provider":"claude","account":"{account}","fetchedAt":"{fetched_at}"}}"#
        ),
        fetched_at: Some(fetched_at.into()),
        received_at,
        tombstoned_at: None,
    }
}

#[test]
fn a_poll_that_names_one_account_keeps_the_others_and_survives_reopen() {
    let rig = Rig::new("merge");
    {
        let store = rig.open();
        store
            .put_account_quota_poll(
                &[
                    reading("acct-a", "2026-09-30T00:00:00.123456789+00:00", 1_000),
                    reading("acct-b", "2026-09-30T00:00:01Z", 1_000),
                ],
                &[PersistedProviderCompleteness {
                    provider: "claude".into(),
                    last_polled_at: 1_000,
                    last_poll_complete: true,
                    last_complete_at: Some(1_000),
                }],
            )
            .unwrap();
        // A later poll that only names acct-a must not remove acct-b's row.
        store
            .put_account_quota_poll(
                &[reading("acct-a", "2026-09-30T00:01:00Z", 2_000)],
                &[PersistedProviderCompleteness {
                    provider: "claude".into(),
                    last_polled_at: 2_000,
                    last_poll_complete: false,
                    last_complete_at: Some(1_000),
                }],
            )
            .unwrap();
        store
            .put_account_vault_roster(&PersistedVaultRoster {
                view: "view-1".into(),
                roster_json: "[]".into(),
                fetched_at: 2_000,
            })
            .unwrap();
    }

    let store = rig.open();
    let readings = store.account_quota_readings().unwrap();
    assert_eq!(readings.len(), 2);
    assert_eq!(readings[0].account_id, "acct-a");
    assert_eq!(
        readings[0].fetched_at.as_deref(),
        Some("2026-09-30T00:01:00Z")
    );
    assert_eq!(readings[0].received_at, 2_000);
    assert_eq!(readings[1].account_id, "acct-b");
    assert_eq!(
        readings[1].fetched_at.as_deref(),
        Some("2026-09-30T00:00:01Z"),
        "the untouched sibling keeps its original source date across the reopen"
    );
    let completeness = store.account_quota_completeness().unwrap();
    assert_eq!(
        completeness,
        vec![PersistedProviderCompleteness {
            provider: "claude".into(),
            last_polled_at: 2_000,
            last_poll_complete: false,
            last_complete_at: Some(1_000),
        }]
    );
    let roster = store
        .account_vault_roster()
        .unwrap()
        .expect("roster persisted");
    assert_eq!(roster.view, "view-1");
    assert_eq!(roster.fetched_at, 2_000);
}
