use striatum_core::key_pool::PoolKey;
use striatum_store::{
    dev_descriptor, AccountCommandKind, PoolAcquire, PoolAcquireOutcome, SqliteRouteStateStore,
};

#[test]
fn writer_counters_count_pool_acquire_as_one_account_batch_and_ignore_deferred_read_commit() {
    let dir = cortexkit_test_support::ScratchDir::new("writer-counters");
    let store = SqliteRouteStateStore::open(&dev_descriptor(
        "striatum",
        dir.join("store.db").to_string_lossy().into_owned(),
    ))
    .unwrap();
    let commits = store.writer_commit_count_for_test();
    let acquisitions = store.writer_acquisition_count_for_test();
    let account_writes = store.account_write_transaction_count_for_test();
    let commands = store.account_command_count_for_test(AccountCommandKind::Maintenance);
    let outside = store.outside_account_writer_count_for_test();
    let keys = [PoolKey::new("apikey:test", None, "active")];
    assert!(matches!(
        store
            .pool_acquire(&PoolAcquire {
                provider: "test",
                session_id: "session",
                keys: &keys,
                now_ms: 1_000,
                lease_ms: 60_000,
                affinity_idle_ms: 60_000,
            })
            .unwrap(),
        PoolAcquireOutcome::Leased(_)
    ));
    assert_eq!(
        store.outside_account_writer_count_for_test(),
        outside,
        "pool acquire must write only inside the account writer"
    );
    assert_eq!(
        store.account_command_count_for_test(AccountCommandKind::Maintenance) - commands,
        1
    );
    assert_eq!(store.writer_commit_count_for_test() - commits, 1);
    assert_eq!(store.writer_acquisition_count_for_test() - acquisitions, 1);
    assert_eq!(
        store.account_write_transaction_count_for_test(),
        account_writes + 1
    );

    store.put_quota_snapshot("{}", 1_000).unwrap();
    assert_eq!(store.writer_commit_count_for_test() - commits, 2);
    assert_eq!(store.writer_acquisition_count_for_test() - acquisitions, 2);

    store
        .with_writer_for_test(|conn| {
            let tx = conn.unchecked_transaction()?;
            let count: i64 = tx.query_row("SELECT COUNT(*) FROM account_admission", [], |row| {
                row.get(0)
            })?;
            assert_eq!(count, 0);
            tx.commit()
        })
        .unwrap();
    assert_eq!(store.writer_commit_count_for_test() - commits, 2);
    assert_eq!(store.writer_acquisition_count_for_test() - acquisitions, 3);
}
