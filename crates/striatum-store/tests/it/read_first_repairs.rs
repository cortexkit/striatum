use striatum_store::{dev_descriptor, AccountCommandKind, PoolWallKind, SqliteRouteStateStore};

fn store(label: &str) -> (cortexkit_test_support::ScratchDir, SqliteRouteStateStore) {
    let dir = cortexkit_test_support::ScratchDir::new(label);
    let store = SqliteRouteStateStore::open(&dev_descriptor(
        label,
        dir.join("store.db").to_string_lossy(),
    ))
    .unwrap();
    (dir, store)
}

#[test]
fn pool_states_unchanged_do_not_acquire_writer() {
    let (_dir, store) = store("read-first-pool");
    store
        .pool_wall(
            "p",
            "c",
            PoolWallKind::RateLimited,
            i64::MAX,
            "refusal",
            100,
        )
        .unwrap();
    let acquisitions = store.writer_acquisition_count_for_test();
    let commits = store.writer_commit_count_for_test();
    let states = store.pool_key_states("p", 200).unwrap();
    assert_eq!(states["c"].cooldown_until_ms, Some(3_600_100));
    assert_eq!(store.writer_acquisition_count_for_test(), acquisitions);
    assert_eq!(store.writer_commit_count_for_test(), commits);
}

#[test]
fn pool_repair_rechecks_newer_writer_without_repeating_row_changes() {
    let (_dir, store) = store("read-first-pool-race");
    // The inserted pool_key_state row needs a durable retry clock.
    store
        .with_writer_for_test(|conn| {
            conn.execute_batch(
                "INSERT INTO pool_key_state(provider,credential_id,cooldown_until,updated_at)
         VALUES ('p','c',9223372036854775807,100)",
            )
        })
        .unwrap();
    let before = store.account_command_count_for_test(AccountCommandKind::Maintenance);
    let rows_before = store
        .account_command_row_changes_for_test(AccountCommandKind::Maintenance)
        .len();
    let states = store
        .pool_key_states_after_read_for_test("p", 200, || {
            // This runs after the reader has released its snapshot, on a separate
            // connection, exactly where a concurrent reporter can win the writer.
            store
                .pool_wall("p", "c", PoolWallKind::RateLimited, i64::MAX, "newer", 500)
                .unwrap();
        })
        .unwrap();
    assert_eq!(states["c"].cooldown_until_ms, Some(3_600_500));
    let clocks = store.fence_rechecks().unwrap();
    assert_eq!(clocks.len(), 1);
    assert_eq!(clocks[0].evidence_at, 500);
    assert_eq!(clocks[0].due_at(), 3_600_500);
    assert_eq!(
        store.account_command_count_for_test(AccountCommandKind::Maintenance) - before,
        2
    );
    let changes = store.account_command_row_changes_for_test(AccountCommandKind::Maintenance);
    let changes = &changes[rows_before..];
    assert_eq!(changes.len(), 2);
    assert!(changes[0] > 0);
    assert_eq!(
        changes[1], 0,
        "the stale repair must not repeat the competing writer's row changes"
    );
    assert_eq!(
        changes.iter().filter(|rows| **rows > 0).count(),
        1,
        "only the competing writer changed durable rows"
    );
    let acquisitions = store.writer_acquisition_count_for_test();
    store.pool_key_states("p", 200).unwrap();
    assert_eq!(store.writer_acquisition_count_for_test(), acquisitions);
}

#[test]
fn pool_sync_repairs_expired_unmanaged_wall_once() {
    let (_dir, store) = store("read-first-retire");
    store
        .with_writer_for_test(|conn| {
            conn.execute_batch(
                "INSERT INTO pool_key_state(provider,credential_id,cooldown_until,updated_at)
         VALUES ('p','c',1,0)",
            )
        })
        .unwrap();
    let before = store.writer_commit_count_for_test();
    assert_eq!(
        store.pool_key_states("p", 200).unwrap()["c"].cooldown_until_ms,
        None
    );
    assert_eq!(store.writer_commit_count_for_test() - before, 1);
    let acquisitions = store.writer_acquisition_count_for_test();
    store.pool_key_states("p", 200).unwrap();
    assert_eq!(store.writer_commit_count_for_test() - before, 1);
    assert_eq!(store.writer_acquisition_count_for_test(), acquisitions);
}
