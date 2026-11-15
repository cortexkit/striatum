use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};
use std::sync::atomic::{AtomicU64, Ordering};
use striatum_store::{
    dev_descriptor, SqliteRouteStateStore, READER_CACHE_SIZE_KIB, WAL_SIZE_CAP_BYTES,
    WAL_TRUNCATE_WAIT_BOUND,
};

fn open(label: &str) -> (cortexkit_test_support::ScratchDir, SqliteRouteStateStore) {
    let dir = cortexkit_test_support::ScratchDir::new(label);
    let store = SqliteRouteStateStore::open(&dev_descriptor(
        "wal-checkpoints",
        dir.join("store.db").to_string_lossy().into_owned(),
    ))
    .unwrap();
    (dir, store)
}

fn all_readers(store: &SqliteRouteStateStore, remaining: usize) {
    if remaining == 0 {
        return;
    }
    store
        .with_reader_for_test(|conn| {
            let cache: i64 = conn.pragma_query_value(None, "cache_size", |r| r.get(0))?;
            assert_eq!(cache, READER_CACHE_SIZE_KIB);
            all_readers(store, remaining - 1);
            Ok(())
        })
        .unwrap();
}

#[test]
fn writer_uses_memory_temp_store() {
    let (_dir, store) = open("writer-temp-store");
    let (temp_store, tracking): (i64, bool) = store
        .with_writer_for_test(|conn| {
            Ok((
                conn.pragma_query_value(None, "temp_store", |row| row.get(0))?,
                conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_temp_schema WHERE name='catalog_facts_writes')",
                    [],
                    |row| row.get(0),
                )?,
            ))
        })
        .unwrap();
    assert_eq!(temp_store, 2, "SQLite MEMORY is 2");
    assert!(
        tracking,
        "configuring temp_store must preserve catalog tracking"
    );
}

#[test]
fn open_configures_writer_and_every_reader_including_later_checkouts() {
    let (_dir, store) = open("wal-pragmas");
    store
        .with_writer_for_test(|conn| {
            let auto: i64 = conn.pragma_query_value(None, "wal_autocheckpoint", |r| r.get(0))?;
            let sync: i64 = conn.pragma_query_value(None, "synchronous", |r| r.get(0))?;
            assert_eq!(auto, 0);
            assert_eq!(sync, 1, "SQLite NORMAL is 1");
            Ok(())
        })
        .unwrap();
    let slots = cortexkit_store::SqliteOpenOptions::default().read_pool_size;
    all_readers(&store, slots);
    all_readers(&store, slots);
    assert!(WAL_TRUNCATE_WAIT_BOUND <= std::time::Duration::from_millis(10));
}

#[test]
fn passive_tick_does_not_acquire_the_writer_or_queue_concurrent_commits() {
    let (_dir, store) = open("wal-independent-maintenance");
    let store = std::sync::Arc::new(store);
    let before = store.writer_acquisition_count_for_test();
    // Holding the writer mutex cannot hold up a passive checkpoint on the
    // maintenance connection. The receive timeout is only a deadlock watchdog.
    let (result, tick) = store
        .with_writer_for_test(|_| {
            let (sent, received) = std::sync::mpsc::channel();
            let checkpoint_store = std::sync::Arc::clone(&store);
            let tick = std::thread::spawn(move || {
                let _ = sent.send(checkpoint_store.checkpoint_wal());
            });
            let result = received.recv_timeout(std::time::Duration::from_secs(5));
            Ok((result, tick))
        })
        .unwrap();
    tick.join().unwrap();
    result.unwrap().unwrap();
    assert_eq!(store.writer_acquisition_count_for_test() - before, 1);

    let ready = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            ready.wait();
            let before = SqliteRouteStateStore::thread_stage_timing();
            for n in 0..100 {
                store.put_quota_snapshot("{}", n).unwrap();
            }
            SqliteRouteStateStore::thread_stage_timing().writer_queue_ns - before.writer_queue_ns
        });
        ready.wait();
        store.checkpoint_wal().unwrap();
        let queue_ns = writer.join().unwrap();
        // Average queue wait stays below 1 ms with no competing writer; the
        // checkpoint must not make commits wait for its database copy or fsync.
        assert!(
            queue_ns / 100 < 1_000_000,
            "writer queue ns/commit={}",
            queue_ns / 100
        );
    });
}

#[test]
fn replacement_reader_after_failed_rollback_still_reads_correctly() {
    let (_dir, store) = open("wal-reader-replacement");
    // Keep the other pool slots checked out so the next innermost read must
    // replace the discarded connection, rather than reuse another idle reader.
    fn replace(store: &SqliteRouteStateStore, remaining: usize) {
        store
            .with_reader_for_test(|conn| {
                if remaining > 1 {
                    replace(store, remaining - 1);
                } else {
                    let discarded = store.with_reader_for_test(|bad| {
                        bad.authorizer(Some(|ctx: AuthContext<'_>| match ctx.action {
                            AuthAction::Transaction {
                                operation: TransactionOperation::Rollback,
                            } => Authorization::Deny,
                            AuthAction::Select => Authorization::Deny,
                            _ => Authorization::Allow,
                        }));
                        // Returning an error drops the transaction. Denying its
                        // rollback leaves it open, so cortexkit-store discards it.
                        Err::<(), _>(rusqlite::Error::InvalidQuery)
                    });
                    assert!(discarded.is_err());
                    store
                        .with_reader_for_test(|fresh| {
                            assert_eq!(fresh.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))?, 1);
                            let cache: i64 =
                                fresh.pragma_query_value(None, "cache_size", |r| r.get(0))?;
                            assert_ne!(
                                cache, READER_CACHE_SIZE_KIB,
                                "replacement uses SQLite's default cache"
                            );
                            Ok(())
                        })
                        .unwrap();
                }
                let _: i64 = conn.query_row("SELECT 1", [], |r| r.get(0))?;
                Ok(())
            })
            .unwrap();
    }
    replace(
        &store,
        cortexkit_store::SqliteOpenOptions::default().read_pool_size - 1,
    );
}

fn grow_wal(store: &SqliteRouteStateStore) -> u64 {
    let commits = 40;
    let before = store.writer_commit_count_for_test();
    for i in 0..commits {
        // Change every overflow page; repeating identical bytes would let SQLite
        // elide almost all page writes after the first commit.
        let payload = char::from(b'a' + (i % 26) as u8)
            .to_string()
            .repeat((WAL_SIZE_CAP_BYTES / 20) as usize);
        store.put_quota_snapshot(&payload, i).unwrap();
    }
    assert_eq!(
        store.writer_commit_count_for_test() - before,
        commits as u64
    );
    commits as u64
}

#[test]
fn request_commits_do_not_checkpoint_until_the_tick_and_tick_caps_the_wal() {
    let (dir, store) = open("wal-no-request-checkpoints");
    store.checkpoint_wal().unwrap();
    // Start with a completely backfilled database, then compare its actual
    // bytes. An automatic checkpoint would copy the changed snapshot page here.
    store
        .with_writer_for_test(|conn| conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)"))
        .unwrap();
    let before = std::fs::read(dir.join("store.db")).unwrap();
    grow_wal(&store);
    assert_eq!(std::fs::read(dir.join("store.db")).unwrap(), before);
    assert!(std::fs::metadata(dir.join("store.db-wal")).unwrap().len() > WAL_SIZE_CAP_BYTES);
    let timeout = || {
        store
            .with_writer_for_test(|conn| {
                conn.pragma_query_value(None, "busy_timeout", |r| r.get::<_, i64>(0))
            })
            .unwrap()
    };
    let before_timeout = timeout();
    store.checkpoint_wal().unwrap();
    assert_eq!(timeout(), before_timeout);
    assert!(std::fs::metadata(dir.join("store.db-wal")).unwrap().len() <= WAL_SIZE_CAP_BYTES);
}

static BUSY_CALLS: AtomicU64 = AtomicU64::new(0);

fn bounded_busy_wait(attempt: i32) -> bool {
    BUSY_CALLS.fetch_add(1, Ordering::SeqCst);
    // Each callback advances a virtual millisecond. No wall-clock sleep or
    // latency assertion is involved in exercising the truncate wait budget.
    u128::try_from(attempt).unwrap() < WAL_TRUNCATE_WAIT_BOUND.as_millis()
}

#[test]
fn reader_snapshot_exhausts_tick_wait_budget_and_next_tick_truncates() {
    let (dir, store) = open("wal-busy-reader");
    store.put_quota_snapshot("old", 0).unwrap();
    let reader = rusqlite::Connection::open(dir.join("store.db")).unwrap();
    reader
        .execute_batch("BEGIN; SELECT * FROM quota_snapshot")
        .unwrap();
    grow_wal(&store);
    let bytes = std::fs::metadata(dir.join("store.db-wal")).unwrap().len();
    assert!(bytes > WAL_SIZE_CAP_BYTES);
    BUSY_CALLS.store(0, Ordering::SeqCst);
    store
        .set_busy_handler_for_test(Some(bounded_busy_wait))
        .unwrap();
    store.checkpoint_wal().unwrap();
    assert_eq!(
        BUSY_CALLS.load(Ordering::SeqCst),
        WAL_TRUNCATE_WAIT_BOUND.as_millis() as u64 + 1
    );
    assert_eq!(
        std::fs::metadata(dir.join("store.db-wal")).unwrap().len(),
        bytes
    );
    reader.execute_batch("COMMIT").unwrap();
    store.checkpoint_wal().unwrap();
    assert!(std::fs::metadata(dir.join("store.db-wal")).unwrap().len() <= WAL_SIZE_CAP_BYTES);
}
