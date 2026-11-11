use std::sync::atomic::{AtomicU64, Ordering};

use striatum_core::quota::quota_poll::{
    admit_reading_intervals, reading_log_retention_cutoff, record_reading_log_poll,
    render_account_key,
};
use striatum_core::quota::ProviderUsage;
use striatum_core::route_state_store::QuotaReadingLogRow;
use striatum_core::{RouteStateStore, UsageObservation};
use striatum_store::{dev_descriptor, SqliteRouteStateStore};

const EPOCH_MS: i64 = 1_893_456_000_000;
static NEXT_RIG: AtomicU64 = AtomicU64::new(0);

struct Rig {
    dir: std::path::PathBuf,
}

impl Rig {
    fn new(label: &str) -> Self {
        let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "prefrontal-quota-reading-{label}-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn db_path(&self) -> String {
        self.dir.join("store.db").to_string_lossy().into_owned()
    }

    fn open(&self) -> SqliteRouteStateStore {
        SqliteRouteStateStore::open(&dev_descriptor("striatum", self.db_path()))
            .expect("open route-state store")
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn reading(window_key: &str, observed_at: i64, fetched_at: Option<i64>) -> QuotaReadingLogRow {
    QuotaReadingLogRow {
        account_key: render_account_key("openai", Some("team-a")),
        window_key: window_key.into(),
        observed_at,
        used_percent: 20.0,
        raw_used_percent: Some(25.0),
        resets_at: Some(EPOCH_MS + 24 * 60 * 60_000),
        window_minutes: Some(300),
        banked_credit_count: Some(2),
        fetched_at,
        stale: None,
    }
}

fn poll_entry(with_secondary: bool) -> ProviderUsage {
    let secondary = with_secondary.then(|| {
        serde_json::json!({
            "usedPercent": 40.0,
            "rawUsedPercent": 45.0,
            "resetsAt": "2030-01-08T00:00:00Z",
            "windowMinutes": 10080
        })
    });
    serde_json::from_value(serde_json::json!({
        "provider": "codex",
        "apiProvider": "openai",
        "account": "team-a",
        "fetchedAt": "2030-01-01T00:00:00Z",
        "savedResets": { "availableCount": 2 },
        "usage": {
            "primary": {
                "usedPercent": 20.0,
                "rawUsedPercent": 25.0,
                "resetsAt": "2030-01-01T05:00:00Z",
                "windowMinutes": 300
            },
            "secondary": secondary
        }
    }))
    .unwrap()
}

fn usage_row(account_key: String) -> UsageObservation {
    UsageObservation {
        id: "same-account-usage".into(),
        route_key: "openai/gpt-5.4/high".into(),
        capacity_key: "openai/gpt-5.4".into(),
        provider_id: "openai".into(),
        input_tokens: Some(1_000.0),
        output_tokens: 200.0,
        reasoning_tokens: Some(20.0),
        cached_read_tokens: Some(10_000.0),
        cache_write_tokens: Some(0.0),
        duration_ms: 2_000.0,
        workload_class: Some("ephemeral".into()),
        account_key: Some(account_key),
        observed_at: EPOCH_MS as f64,
    }
}

#[test]
fn migration_038_creates_the_append_only_reading_log() {
    let rig = Rig::new("migration");
    let store = rig.open();
    let connection = rusqlite::Connection::open(rig.db_path()).unwrap();
    let columns = connection
        .prepare("PRAGMA table_info(quota_reading_log)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        columns,
        vec![
            "account_key",
            "window_key",
            "observed_at",
            "used_percent",
            "raw_used_percent",
            "resets_at",
            "window_minutes",
            "banked_credit_count",
            "fetched_at",
            "is_stale",
            "stale_since",
            "stale_class",
        ]
    );
    let index_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'index' AND name = 'quota_reading_log_observed_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(index_count, 1);
    drop(connection);
    drop(store);
}

#[test]
fn usage_and_reading_rows_share_a_byte_equal_account_key_and_join() {
    let rig = Rig::new("account-key-join");
    let store = rig.open();
    let account_key = render_account_key("openai", Some("team-a"));
    store
        .record_usage_delivery(&usage_row(account_key.clone()))
        .unwrap();
    assert_eq!(
        record_reading_log_poll(&store, &poll_entry(false), "openai", &[], EPOCH_MS).unwrap(),
        1
    );

    let connection = rusqlite::Connection::open(rig.db_path()).unwrap();
    let (usage_key, reading_key, joins): (Vec<u8>, Vec<u8>, i64) = connection
        .query_row(
            "SELECT CAST(u.account_key AS BLOB), CAST(l.account_key AS BLOB),
                    (SELECT COUNT(*) FROM model_route_usage u2
                     JOIN quota_reading_log l2 ON l2.account_key = u2.account_key)
             FROM model_route_usage u
             JOIN quota_reading_log l ON l.account_key = u.account_key",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(usage_key, reading_key);
    assert_eq!(usage_key, account_key.as_bytes());
    assert_eq!(joins, 1);
}

#[test]
fn primary_and_weekly_windows_at_one_poll_remain_distinct_and_do_not_cross() {
    let rig = Rig::new("window-key");
    let store = rig.open();
    assert_eq!(
        record_reading_log_poll(&store, &poll_entry(true), "openai", &[], EPOCH_MS).unwrap(),
        2
    );
    let stored = store.get_quota_readings("openai/team-a").unwrap();
    assert_eq!(stored.len(), 2);
    assert_ne!(stored[0].window_key, stored[1].window_key);
    assert!(admit_reading_intervals(&stored).is_empty());
}

#[test]
fn five_polls_of_one_preserved_source_reading_keep_five_poll_clock_rows() {
    let rig = Rig::new("two-clocks");
    let store = rig.open();
    let entry = poll_entry(false);
    for minute in 0..5 {
        assert_eq!(
            record_reading_log_poll(&store, &entry, "openai", &[], EPOCH_MS + minute * 60_000,)
                .unwrap(),
            1
        );
    }
    let stored = store.get_quota_readings("openai/team-a").unwrap();
    assert_eq!(stored.len(), 5);
    assert_eq!(
        stored
            .iter()
            .map(|row| row.observed_at)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        5
    );
    assert_eq!(
        stored
            .iter()
            .map(|row| row.fetched_at)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        1
    );
    assert!(admit_reading_intervals(&stored).is_empty());
}

#[test]
fn duplicate_composite_key_never_rewrites_the_original_reading() {
    let rig = Rig::new("append-only");
    let store = rig.open();
    let original = reading("codexbar:codex:primary", EPOCH_MS, Some(EPOCH_MS));
    assert_eq!(store.append_quota_readings(&[original.clone()]).unwrap(), 1);

    let mut replacement = original.clone();
    replacement.used_percent = 99.0;
    replacement.raw_used_percent = Some(99.0);
    assert_eq!(store.append_quota_readings(&[replacement]).unwrap(), 0);
    assert_eq!(
        store.get_quota_readings("openai/team-a").unwrap(),
        vec![original]
    );

    let connection = rusqlite::Connection::open(rig.db_path()).unwrap();
    let update = connection.execute(
        "UPDATE quota_reading_log SET used_percent = 50 WHERE account_key = ?1",
        ["openai/team-a"],
    );
    assert!(
        update.is_err(),
        "the schema must reject updates, not merely avoid them in Rust"
    );
}

#[test]
fn retention_prunes_only_rows_older_than_fourteen_days() {
    let rig = Rig::new("retention");
    let store = rig.open();
    let now = EPOCH_MS;
    let cutoff = reading_log_retention_cutoff(now);
    let rows = vec![
        reading("old", cutoff - 1, Some(cutoff - 1)),
        reading("boundary", cutoff, Some(cutoff)),
        reading("new", now, Some(now)),
    ];
    assert_eq!(store.append_quota_readings(&rows).unwrap(), 3);

    assert_eq!(store.prune_quota_readings(now).unwrap(), 1);
    let remaining = store.get_quota_readings("openai/team-a").unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().any(|row| row.window_key == "boundary"));
    assert!(remaining.iter().any(|row| row.window_key == "new"));
}

/// The routing module persists its merged quota cache so a restart can select
/// with the last accepted reading. The snapshot must survive closing and
/// reopening the database with its observed time, a later write must replace
/// the earlier one, and every field routing decodes must come back unchanged
/// after serializing through the store.
#[test]
fn quota_snapshot_round_trips_across_reopen_with_its_observed_time() {
    let rig = Rig::new("quota-snapshot");
    let windows: Vec<ProviderUsage> = serde_json::from_value(serde_json::json!([
        {
            "provider": "codex",
            "apiProvider": "openai",
            "account": "team-a",
            "source": "oauth",
            "usage": {
                "primary": {
                    "usedPercent": 12.5,
                    "rawUsedPercent": 20.0,
                    "resetsAt": "2030-01-01T05:00:00Z",
                    "windowMinutes": 300,
                    "windowModel": "fixed"
                },
                "extraRateWindows": [
                    { "title": "Gemini X", "id": "x", "window": { "usedPercent": 90.0, "windowType": "rolling" } }
                ]
            },
            "spend": [{
                "id": "seat",
                "label": "Seat",
                "funding": "subscription",
                "remaining": { "minor": 20, "exponent": 2, "unit": "USD" },
                "total": { "minor": 100, "exponent": 2, "unit": "USD" },
                "basis": "reported",
                "spendable": null
            }],
            "savedResets": { "availableCount": 2 },
            "stale": { "since": "2029-12-31T23:00:00Z", "class": "upstream_failed" },
            "fetchedAt": "2029-12-31T23:59:00Z"
        },
        {
            "provider": "claude",
            "error": "credential rejected",
            "errorClass": "credential_rejected"
        }
    ]))
    .unwrap();

    {
        let store = rig.open();
        assert_eq!(store.quota_snapshot().unwrap(), None);
        store.put_quota_snapshot("[]", EPOCH_MS).unwrap();
        store
            .put_quota_snapshot(&serde_json::to_string(&windows).unwrap(), EPOCH_MS + 60_000)
            .unwrap();
    }

    let reopened = rig.open();
    let persisted = reopened
        .quota_snapshot()
        .unwrap()
        .expect("snapshot survives reopen");
    assert_eq!(
        persisted.observed_at,
        EPOCH_MS + 60_000,
        "the later write replaces the earlier one, observed time included"
    );
    let restored: Vec<ProviderUsage> = serde_json::from_str(&persisted.windows_json).unwrap();
    assert_eq!(restored, windows);
}
