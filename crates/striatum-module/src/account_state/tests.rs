use super::*;
use std::sync::atomic::AtomicU64;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use striatum_store::AccountCommandKind;

fn fixture(
    name: &str,
) -> (
    cortexkit_test_support::ScratchDir,
    Arc<SqliteRouteStateStore>,
) {
    let scratch = cortexkit_test_support::ScratchDir::new(name);
    let store = SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
        name,
        scratch.path().join("state.db").to_string_lossy(),
    ))
    .unwrap();
    (scratch, Arc::new(store))
}

fn install(store: &SqliteRouteStateStore) -> Arc<StateOwner> {
    let owner = Arc::new(StateOwner::default());
    store.install_account_state_bridge(owner.clone()).unwrap();
    owner
}

fn snapshot(owner: &StateOwner) -> AccountState {
    (**owner.published.read().unwrap().as_ref().unwrap()).clone()
}

#[test]
fn initialization_load_is_read_only_and_publishes_once() {
    let (_scratch, store) = fixture("state-initial-read");
    let before = store.account_write_transaction_count_for_test();
    let owner = install(&store);
    assert_eq!(snapshot(&owner).generation, 1);
    assert_eq!(store.account_write_transaction_count_for_test(), before);
    assert_eq!(
        store.account_command_count_for_test(AccountCommandKind::Maintenance),
        0
    );
}

#[test]
fn salt_is_authoritative_and_its_sql_read_negative_control_moves_the_counter() {
    let (_scratch, store) = fixture("state-salt-reads");
    let _owner = install(&store);
    let reads = Arc::new(AtomicU64::new(0));
    let counter = reads.clone();
    store
        .with_writer_for_test(|conn| {
            conn.authorizer(Some(move |context: rusqlite::hooks::AuthContext<'_>| {
                if matches!(
                    context.action,
                    rusqlite::hooks::AuthAction::Read {
                        table_name: "account_selection_salt",
                        ..
                    }
                ) {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                rusqlite::hooks::Authorization::Allow
            }));
            Ok(())
        })
        .unwrap();
    for i in 0..8 {
        let value = store
            .enqueue_account_state_command(AccountCommandKind::Admit, move |tx, state| {
                AccountCommand::new(tx, state).salt(i, || format!("salt-{i}"))
            })
            .unwrap()
            .wait()
            .unwrap();
        assert_eq!(value, "salt-0");
        assert_eq!(
            super::snapshot(&store)
                .unwrap()
                .selection_salt
                .as_deref()
                .map(String::as_str),
            Some("salt-0")
        );
    }
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    store.account_transaction(|tx| tx.existing_salt()).unwrap();
    assert!(
        reads.load(Ordering::Relaxed) > 0,
        "the SQL oracle must exercise the read counter"
    );
}

#[test]
fn startup_load_holds_early_account_work_until_publication() {
    let (_scratch, store) = fixture("state-startup-hold");
    let owner = Arc::new(StateOwner::default());
    let (entered, loading) = mpsc::channel();
    let (resume, paused) = mpsc::channel();
    *owner.load_hook.lock().unwrap() = Some(Box::new(move || {
        entered.send(()).unwrap();
        paused.recv().unwrap();
        Ok(())
    }));
    let installing = store.clone();
    let driver = owner.clone();
    let (ready, readiness) = mpsc::channel();
    let loader = std::thread::spawn(move || {
        let result = installing.install_account_state_bridge(driver);
        ready.send(result).unwrap();
    });
    loading.recv_timeout(Duration::from_secs(2)).unwrap();
    let requesting = store.clone();
    let (replied, reply) = mpsc::channel();
    let request = std::thread::spawn(move || {
        replied
            .send(crate::account_writer::execute(
                &requesting,
                AccountCommandKind::Admit,
                |tx| tx.salt(10, || "early-request".into()),
            ))
            .unwrap();
    });
    assert!(owner.published.read().unwrap().is_none());
    assert!(matches!(
        readiness.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert!(matches!(
        reply.recv_timeout(Duration::from_millis(30)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    resume.send(()).unwrap();
    readiness
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    assert_eq!(
        reply.recv_timeout(Duration::from_secs(2)).unwrap().unwrap(),
        "early-request"
    );
    loader.join().unwrap();
    request.join().unwrap();
}

#[test]
fn failed_initial_load_never_publishes_or_allows_serving() {
    let (_scratch, store) = fixture("state-startup-failure");
    let owner = Arc::new(StateOwner::default());
    *owner.load_hook.lock().unwrap() = Some(Box::new(|| {
        Err(RouteStateStoreError("forced load failure".into()))
    }));
    assert!(store.install_account_state_bridge(owner.clone()).is_err());
    assert!(owner.published.read().unwrap().is_none());
    assert!(owner
        .snapshot()
        .err()
        .unwrap()
        .to_string()
        .contains("forced load failure"));
    assert!(crate::account_writer::execute(&store, AccountCommandKind::Admit, |_| Ok(())).is_err());
}

#[test]
fn a_writer_that_stops_during_the_first_load_wakes_waiting_readers() {
    let (_scratch, store) = fixture("state-startup-writer-stops");
    let owner = Arc::new(StateOwner::default());
    let (entered, loading) = mpsc::channel();
    let (resume, paused) = mpsc::channel::<()>();
    *owner.load_hook.lock().unwrap() = Some(Box::new(move || {
        entered.send(()).unwrap();
        let _ = paused.recv();
        panic!("forced writer stop during the first load");
    }));
    let installing = store.clone();
    let driver = owner.clone();
    let loader = std::thread::spawn(move || installing.install_account_state_bridge(driver));
    loading.recv_timeout(Duration::from_secs(2)).unwrap();
    // A reader arrives while the first load runs and waits for it.
    let waiting = owner.clone();
    let (answered, answer) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        answered.send(waiting.snapshot().map(|_| ())).unwrap();
    });
    resume.send(()).unwrap();
    assert!(loader.join().unwrap().is_err());
    let answer = answer
        .recv_timeout(Duration::from_secs(5))
        .expect("the waiting reader is woken when the writer stops");
    assert!(answer.is_err(), "{answer:?}");
    reader.join().unwrap();
}

/// A first load that takes longer than the readers' bound answers the
/// timeout to a request that arrives meanwhile, within the bound. The
/// request does not queue a second load behind the first, which it would
/// then wait for without a bound.
#[test]
fn a_stalled_first_load_answers_a_request_within_the_bound() {
    let (_scratch, store) = fixture("state-startup-stalled-load");
    super::FIRST_LOAD_WAIT_FOR_TEST.store(200, Ordering::Relaxed);
    let owner = Arc::new(StateOwner::default());
    let (entered, loading) = mpsc::channel();
    let (resume, paused) = mpsc::channel::<()>();
    *owner.load_hook.lock().unwrap() = Some(Box::new(move || {
        entered.send(()).unwrap();
        let _ = paused.recv();
        Ok(())
    }));
    let installing = store.clone();
    let driver = owner.clone();
    let loader = std::thread::spawn(move || installing.install_account_state_bridge(driver));
    loading.recv_timeout(Duration::from_secs(2)).unwrap();
    let requesting = store.clone();
    let (answered, answer) = mpsc::channel();
    let request = std::thread::spawn(move || {
        answered.send(super::initialize(&requesting)).unwrap();
    });
    let answer = answer
        .recv_timeout(Duration::from_secs(5))
        .expect("the request is answered while the first load still runs");
    let error = answer.unwrap_err().to_string();
    assert!(error.contains(super::FIRST_LOAD_PENDING), "{error}");
    resume.send(()).unwrap();
    // The installer waits within the same bound: it may have answered that
    // the load is still running before the load was resumed.
    if let Err(error) = loader.join().unwrap() {
        assert!(
            error.to_string().contains(super::FIRST_LOAD_PENDING),
            "{error}"
        );
    }
    request.join().unwrap();
    super::FIRST_LOAD_WAIT_FOR_TEST.store(5_000, Ordering::Relaxed);
    super::initialize(&store).unwrap();
    assert_eq!(
        owner.loads.load(Ordering::Relaxed),
        1,
        "the request queued no second load"
    );
}

/// The installing caller and a retry after a failed first load wait on the
/// load within the same bound as a reader; the load keeps running and
/// publishes when it ends, without a second load.
#[test]
fn a_stalled_load_answers_its_installer_and_its_retry_within_the_bound() {
    let (_scratch, store) = fixture("state-startup-stalled-installer");
    super::FIRST_LOAD_WAIT_FOR_TEST.store(200, Ordering::Relaxed);
    let owner = Arc::new(StateOwner::default());
    let (resume, paused) = mpsc::channel::<()>();
    let paused = std::sync::Mutex::new(paused);
    let first = std::sync::atomic::AtomicBool::new(true);
    *owner.load_hook.lock().unwrap() = Some(Box::new(move || {
        if first.swap(false, Ordering::Relaxed) {
            return Err(RouteStateStoreError("first load failed".into()));
        }
        let _ = paused.lock().unwrap().recv();
        Ok(())
    }));
    let bounded = |what: &str, call: Box<dyn FnOnce() -> RouteStateResult<()> + Send>| {
        let (answered, answer) = mpsc::channel();
        std::thread::spawn(move || answered.send(call()).unwrap());
        answer
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("{what} waited past the bound"))
    };
    let installing = store.clone();
    let driver = owner.clone();
    let installed = bounded(
        "the installer",
        Box::new(move || installing.install_account_state_bridge(driver)),
    );
    assert!(installed
        .unwrap_err()
        .to_string()
        .contains("first load failed"));
    let retrying = store.clone();
    let retried = bounded("the retry", Box::new(move || super::initialize(&retrying)));
    let error = retried.unwrap_err().to_string();
    assert!(error.contains(super::FIRST_LOAD_PENDING), "{error}");
    let asking = store.clone();
    let asked = bounded(
        "a reader during the retry",
        Box::new(move || super::initialize(&asking)),
    );
    let error = asked.unwrap_err().to_string();
    assert!(error.contains(super::FIRST_LOAD_PENDING), "{error}");
    resume.send(()).unwrap();
    let ready = store.clone();
    bounded(
        "the reader after the load",
        Box::new(move || {
            super::FIRST_LOAD_WAIT_FOR_TEST.store(5_000, Ordering::Relaxed);
            super::initialize(&ready)
        }),
    )
    .unwrap();
    assert_eq!(
        owner.loads.load(Ordering::Relaxed),
        2,
        "one failed load, one retry"
    );
}

#[test]
fn account_for_frame_sent_during_load_is_answered_after_publication() {
    let _clock = crate::InjectedClock::set(crate::routing_v1::request_cpu_bench::FIXTURE_NOW);
    let fixture = crate::routing_v1::request_cpu_bench::production_fixture();
    let store = fixture.store().clone();
    let owner = Arc::new(StateOwner::default());
    let (entered, loading) = mpsc::channel();
    let (resume, paused) = mpsc::channel();
    *owner.load_hook.lock().unwrap() = Some(Box::new(move || {
        entered.send(()).unwrap();
        paused.recv().unwrap();
        Ok(())
    }));
    let installing = store.clone();
    let driver = owner.clone();
    let loader = std::thread::spawn(move || installing.install_account_state_bridge(driver));
    loading.recv_timeout(Duration::from_secs(2)).unwrap();
    let inputs = crate::TestThreadState::capture();
    let (replied, reply) = mpsc::channel();
    let request = std::thread::spawn(move || {
        let _inputs = inputs.install();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        replied
            .send(runtime.block_on(fixture.account_request("during-load")))
            .unwrap();
    });
    assert!(matches!(
        reply.recv_timeout(Duration::from_millis(30)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(owner.published.read().unwrap().is_none());
    resume.send(()).unwrap();
    let (kind, body) = reply.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(kind, subc_protocol::FrameType::Response, "{body}");
    assert!(
        body["result"]["account"]["credential_id"].is_string(),
        "{body}"
    );
    assert!(!body.to_string().contains("account_selection_unavailable"));
    loader.join().unwrap().unwrap();
    request.join().unwrap();
}

#[test]
fn load_time_on_production_sized_and_representative_smoke_fixtures() {
    use crate::routing_v1::request_cpu_bench::{
        cpu_us, production_fixture, representative_fixture, FixtureCounts,
    };
    for (name, fixture) in [
        ("production-sized", production_fixture()),
        (
            "representative SMOKE, NOT A BASELINE",
            representative_fixture(FixtureCounts {
                subscription_rules: 7,
                exclusion_rules: 5,
                runner_route_rules: 3,
                catalog_facts: 12,
                listed_credentials: 6,
            }),
        ),
    ] {
        let cpu = cpu_us();
        let start = Instant::now();
        initialize(fixture.store()).unwrap();
        eprintln!(
            "account_state salt-only startup {name}: wall={}us cpu={}us",
            start.elapsed().as_micros(),
            cpu_us() - cpu
        );
    }
}

#[test]
fn command_rollback_restores_the_working_copy_for_its_batch_sibling() {
    let (_scratch, store) = fixture("state-savepoint");
    let owner = install(&store);
    let (started, running) = mpsc::channel();
    let (resume, paused) = mpsc::channel();
    let held = store
        .enqueue_account_command(AccountCommandKind::Maintenance, move |_| {
            started.send(()).unwrap();
            paused.recv().unwrap();
            Ok(())
        })
        .unwrap();
    running.recv_timeout(Duration::from_secs(2)).unwrap();
    let failed = store
        .enqueue_account_state_command::<()>(AccountCommandKind::Admit, |tx, state| {
            let mut tx = AccountCommand::new(tx, state);
            tx.salt(1, || "rolled-back".into())?;
            Err(RouteStateStoreError("forced command failure".into()))
        })
        .unwrap();
    let sibling = store
        .enqueue_account_state_command(AccountCommandKind::Admit, |tx, state| {
            AccountCommand::new(tx, state).salt(2, || "committed".into())
        })
        .unwrap();
    resume.send(()).unwrap();
    held.wait().unwrap();
    assert!(failed.wait().is_err());
    assert_eq!(sibling.wait().unwrap(), "committed");
    assert_eq!(
        snapshot(&owner)
            .selection_salt
            .as_deref()
            .map(String::as_str),
        Some("committed")
    );
    assert_eq!(
        store
            .account_transaction(|tx| tx.existing_salt())
            .unwrap()
            .as_deref(),
        Some("committed")
    );
}

#[test]
fn commit_publishes_exactly_once_before_reply_and_never_while_commit_is_held() {
    let (_scratch, store) = fixture("state-publication-order");
    let owner = install(&store);
    let before = snapshot(&owner);
    let (entered, committing) = mpsc::channel();
    let (resume, held) = mpsc::channel();
    store
        .with_writer_for_test(|conn| {
            conn.commit_hook(Some(move || {
                entered.send(()).unwrap();
                held.recv().unwrap();
                false
            }));
            Ok(())
        })
        .unwrap();
    let queued = store
        .enqueue_account_state_command(AccountCommandKind::Admit, |tx, state| {
            AccountCommand::new(tx, state).salt(1, || "durable".into())
        })
        .unwrap();
    let (replied, reply) = mpsc::channel();
    let waiter = std::thread::spawn(move || replied.send(queued.wait()).unwrap());
    committing.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(snapshot(&owner), before);
    assert!(matches!(reply.try_recv(), Err(mpsc::TryRecvError::Empty)));
    resume.send(()).unwrap();
    assert_eq!(
        reply.recv_timeout(Duration::from_secs(2)).unwrap().unwrap(),
        "durable"
    );
    let published = snapshot(&owner);
    assert_eq!(published.generation, before.generation + 1);
    assert_eq!(
        published.selection_salt.as_deref().map(String::as_str),
        Some("durable")
    );
    waiter.join().unwrap();
    store
        .with_writer_for_test(|conn| {
            conn.commit_hook(None::<fn() -> bool>);
            Ok(())
        })
        .unwrap();
}

#[test]
fn commit_failure_keeps_publication_and_reloads_before_next_batch() {
    let (_scratch, store) = fixture("state-commit-failure");
    let owner = install(&store);
    let before = snapshot(&owner);
    store.with_writer_for_test(|conn| conn.execute_batch(
        "CREATE TABLE state_parent(id PRIMARY KEY); \
         CREATE TABLE state_child(parent REFERENCES state_parent(id) DEFERRABLE INITIALLY DEFERRED); \
         CREATE TRIGGER state_fail AFTER INSERT ON account_selection_salt BEGIN \
           INSERT INTO state_child VALUES (1); END;"
    )).unwrap();
    let failed = crate::account_writer::execute(&store, AccountCommandKind::Admit, |tx| {
        tx.salt(1, || "uncommitted".into())
    });
    assert!(failed.unwrap_err().to_string().contains("store_error"));
    assert_eq!(snapshot(&owner), before);
    store
        .with_writer_for_test(|conn| conn.execute_batch("DROP TRIGGER state_fail"))
        .unwrap();
    assert_eq!(
        crate::account_writer::execute(&store, AccountCommandKind::Admit, |tx| tx
            .salt(2, || "fresh".into()))
        .unwrap(),
        "fresh"
    );
    assert_eq!(owner.loads.load(Ordering::Relaxed), 2);
}

#[test]
fn external_commit_is_seen_on_tick_but_internal_commits_and_passive_checkpoint_do_not_reload() {
    let (scratch, store) = fixture("state-data-version");
    let owner = install(&store);
    let other = rusqlite::Connection::open(scratch.path().join("state.db")).unwrap();
    let initial_loads = owner.loads.load(Ordering::Relaxed);
    other
        .execute(
            "INSERT INTO account_selection_salt VALUES (1,'external',1)",
            [],
        )
        .unwrap();
    crate::account_writer::tick(&store, 1).unwrap();
    assert_eq!(owner.loads.load(Ordering::Relaxed), initial_loads + 1);
    assert_eq!(
        snapshot(&owner)
            .selection_salt
            .as_deref()
            .map(String::as_str),
        Some("external")
    );
    let loads = owner.loads.load(Ordering::Relaxed);
    for i in 0..100 {
        store.with_writer_for_test(|conn| conn.execute_batch(&format!("CREATE TABLE IF NOT EXISTS non_account(id); INSERT INTO non_account VALUES ({i});"))).unwrap();
        crate::account_writer::tick(&store, 1).unwrap();
    }
    other
        .execute_batch("PRAGMA wal_checkpoint(PASSIVE)")
        .unwrap();
    crate::account_writer::tick(&store, 1).unwrap();
    assert_eq!(owner.loads.load(Ordering::Relaxed), loads);
    let before = snapshot(&owner);
    other
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    crate::account_writer::tick(&store, 1).unwrap();
    assert!(owner.loads.load(Ordering::Relaxed) <= loads + 1);
    let mut after = snapshot(&owner);
    after.generation = before.generation;
    assert_eq!(after, before);
}

#[test]
fn checkpoint_cycles_do_not_reload_unchanged_account_state() {
    let (_scratch, store) = fixture("state-passive-checkpoints");
    let owner = install(&store);
    let loads = owner.loads.load(Ordering::Relaxed);
    for cycle in 0..3 {
        for write in 0..20 {
            let payload = char::from(b'a' + write as u8)
                .to_string()
                .repeat(1024 * 1024);
            store
                .put_quota_snapshot(&payload, cycle * 20 + write)
                .unwrap();
        }
        store.checkpoint_wal().unwrap();
        for kind in [AccountCommandKind::Observe, AccountCommandKind::QuotaDelta] {
            store.account_command(kind, |_| Ok(())).unwrap();
        }
        assert_eq!(owner.loads.load(Ordering::Relaxed), loads, "cycle {cycle}");
        assert_eq!(snapshot(&owner).selection_salt, None);
    }
}

#[test]
fn generated_salt_sequences_match_sql_replies_and_round_trip() {
    let mut coverage = [false; 5];
    let sequences: Vec<_> = (1..=12u64)
        .map(|seed| {
            let mut rng = seed;
            let operations: Vec<_> = (0..32)
                .map(|_| {
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let op = ((rng >> 32) % 5) as usize;
                    coverage[op] = true;
                    op
                })
                .collect();
            (seed, operations)
        })
        .collect();
    assert!(
        coverage.into_iter().all(|reached| reached),
        "generator must cover read, rollback, write, Tick and reload before comparing"
    );
    for (seed, operations) in sequences {
        let (_scratch, store) = fixture(&format!("state-property-{seed}"));
        let (_oracle_scratch, oracle) = fixture(&format!("state-oracle-{seed}"));
        let owner = install(&store);
        for (step, op) in operations.into_iter().enumerate() {
            let step = step as i64;
            let run = move |tx: &mut AccountCommand<'_>| {
                if op == 0 {
                    return tx.existing_salt();
                }
                let salt = tx.salt(step, || format!("salt-{seed}-{step}"))?;
                if op == 1 {
                    return Err(RouteStateStoreError("generated rollback".into()));
                }
                Ok(Some(salt))
            };
            if op == 4 {
                owner.reload.store(true, Ordering::Release);
            }
            let actual = crate::account_writer::execute(&store, AccountCommandKind::Admit, run)
                .map_err(|e| e.to_string());
            let expected = oracle
                .account_command(AccountCommandKind::Admit, move |tx| {
                    run(&mut AccountCommand::new(tx, None))
                })
                .map_err(|e| e.to_string());
            assert_eq!(actual, expected, "seed={seed}, step={step}");
            if op == 3 {
                crate::account_writer::tick(&store, step).unwrap();
            }
            let loaded = store.account_transaction(AccountState::load).unwrap();
            let mut memory = snapshot(&owner);
            memory.generation = 0;
            assert_eq!(loaded, memory, "seed={seed}, step={step}");
        }
    }
}

#[test]
fn working_copy_shares_immutable_salt_and_reports_clone_and_load_cpu() {
    let (_scratch, store) = fixture("state-copy-cost");
    let owner = install(&store);
    crate::account_writer::execute(&store, AccountCommandKind::Admit, |tx| {
        tx.salt(1, || "shared".into())
    })
    .unwrap();
    let original = owner.published_snapshot().unwrap();
    assert!(
        Arc::ptr_eq(&original, &owner.published_snapshot().unwrap()),
        "readers must receive the publication, not a cloned working copy"
    );
    let copy = original.fork();
    let copy = copy.as_any().downcast_ref::<AccountState>().unwrap();
    assert!(Arc::ptr_eq(
        original.selection_salt.as_ref().unwrap(),
        copy.selection_salt.as_ref().unwrap()
    ));
    let start = Instant::now();
    for _ in 0..100_000 {
        std::hint::black_box(owner.published_snapshot().unwrap().fork());
    }
    let clone_ns = start.elapsed().as_nanos() / 100_000;
    let start = Instant::now();
    for _ in 0..100 {
        store.account_transaction(AccountState::load).unwrap();
    }
    eprintln!(
        "account_state salt-only snapshot-and-fork: {clone_ns}ns/op; SQL load: {}us/op (100 loads)",
        start.elapsed().as_micros() / 100
    );
}

/// A failed state load fails its batch, not the process: the writer stays up
/// and the next account command loads again.
#[test]
fn a_failed_load_is_retried_by_the_next_command_instead_of_stopping_the_writer() {
    let (_scratch, store) = fixture("state-load-retry");
    let owner = Arc::new(StateOwner::default());
    let failures = Arc::new(AtomicU64::new(1));
    let remaining = failures.clone();
    *owner.load_hook.lock().unwrap() = Some(Box::new(move || {
        if remaining.load(Ordering::Relaxed) > 0 {
            remaining.fetch_sub(1, Ordering::Relaxed);
            return Err(RouteStateStoreError("transient load failure".into()));
        }
        Ok(())
    }));
    assert!(store.install_account_state_bridge(owner.clone()).is_err());
    assert_eq!(
        crate::account_writer::execute(&store, AccountCommandKind::Admit, |tx| tx
            .salt(1, || "after-retry".into()))
        .unwrap(),
        "after-retry"
    );
    assert!(owner.published.read().unwrap().is_some());
}

/// The routing writer records an offer's decision, reservation and deadline
/// together or not at all, and migration 088 cleared every other shape. A row
/// with only some of them stops the load with an error naming the row, rather
/// than loading as something SQL would read differently.
#[test]
fn a_continuation_with_a_partial_offer_is_refused_at_load_naming_the_row() {
    let (_scratch, store) = fixture("state-partial-offer");
    store
        .with_writer_for_test(|conn| {
            conn.execute(
                "INSERT INTO account_continue (principal, selection_id, request_json, \
                   answer_json, run_id, admission_id, previous_decision_id, decision_id) \
                 VALUES ('p', 'partial', '{}', '{}', 'run', 'original', 'root', 'decision')",
                [],
            )
        })
        .unwrap();
    let error = store
        .install_account_state_bridge(Arc::new(StateOwner::default()))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("principal p, selection partial") && error.contains("refuses to load"),
        "{error}"
    );
}

fn state_blind_source_offenders(
    path: &std::path::Path,
    text: &str,
    forbidden: &[&str],
    command_only: &[&str],
) -> Vec<String> {
    // Only this exact first-line inner gate excludes the whole file from a
    // production build. Mixed, feature-only and outer gates still get scanned.
    let tests = path.ends_with("account_state/tests.rs")
        || path.ends_with("account_state/admission_tests.rs")
        || text.lines().next() == Some("#![cfg(test)]");
    let mut offenders = Vec::new();
    for &name in forbidden {
        if text.contains(name) && !tests {
            offenders.push(format!("{}: {name}", path.display()));
        }
    }
    for &name in command_only {
        if text.contains(name) && !tests && !path.ends_with("src/account_state.rs") {
            offenders.push(format!("{}: {name}", path.display()));
        }
    }
    offenders
}

#[test]
fn state_blind_source_scan_honors_only_the_exact_whole_file_test_gate() {
    let path = std::path::Path::new("src/production_plant.rs");
    let calls = "tx.v1_commit_report(); tx.set_admission_lifecycle(); tx.admission_in_states();";
    let forbidden = [".v1_commit_report("];
    let command_only = [".set_admission_lifecycle(", ".admission_in_states("];
    for prefix in [
        "",
        "#![cfg(any(test, feature = \"x\"))]\n",
        "#![cfg(feature = \"x\")]\n",
        "#[cfg(test)]\n",
    ] {
        let source = format!("{prefix}{calls}");
        assert_eq!(
            state_blind_source_offenders(path, &source, &forbidden, &command_only).len(),
            3,
            "runtime-capable or ungated source is still scanned: {prefix:?}"
        );
    }
    assert!(state_blind_source_offenders(
        path,
        &format!("#![cfg(test)]\n{calls}"),
        &forbidden,
        &command_only,
    )
    .is_empty());
}

/// Once the module's state is installed, the store-level writers that change
/// admission-group rows without it refuse, and the module never calls them.
#[test]
fn state_blind_admission_writers_refuse_once_state_is_installed() {
    let (_scratch, store) = fixture("state-blind-writers");
    let _owner = install(&store);
    assert!(store.prune_account_admissions(1, 1, 1).is_err());
    assert!(store.prune_account_affinities(1, 1).is_err());
    assert!(store.prune_account_continuations(1, 1).is_err());
    assert!(store
        .v1_commit_report(
            "decision",
            "outcome",
            "report",
            &V1Recorded {
                body_json: "{}".into(),
                answer_json: "{}".into(),
            },
            striatum_core::RouteOutcome::Completed,
            None,
            false,
            &Default::default(),
            None,
            |_| Ok(()),
            1,
        )
        .is_err());

    // Source check: no module code reaches them by name. The raw SQL writers
    // that persist the state's own decisions are called only from the module
    // command type in account_state.rs, which keeps the state in step.
    let forbidden = [
        "store.prune_account_admissions(",
        "store.prune_account_affinities(",
        "store.prune_account_continuations(",
        ".v1_commit_report(",
    ];
    let command_only = [
        ".release_admission_probes(",
        ".set_admission_lifecycle(",
        ".set_affinity_lease_end(",
        ".admission_in_states(",
        ".settle_continuation_offer(",
        "_returning(",
    ];
    let mut pending = vec![std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")];
    let mut offenders = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                let text = std::fs::read_to_string(&path).unwrap();
                offenders.extend(state_blind_source_offenders(
                    &path,
                    &text,
                    &forbidden,
                    &command_only,
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "state-blind writers called: {offenders:?}"
    );
}

/// After a failed load, requests that arrive together retry it once and share
/// that attempt's result, instead of each reloading the whole state in turn.
#[test]
fn concurrent_retries_after_a_failed_load_share_one_reload() {
    let (_scratch, store) = fixture("state-single-flight");
    let store = Arc::new(store);
    let owner = Arc::new(StateOwner::default());
    let loads = Arc::new(AtomicU64::new(0));
    let counted = loads.clone();
    *owner.load_hook.lock().unwrap() = Some(Box::new(move || {
        counted.fetch_add(1, Ordering::Relaxed);
        // Long enough for every caller below to arrive while it runs.
        std::thread::sleep(Duration::from_millis(300));
        Err(RouteStateStoreError("load keeps failing".into()))
    }));
    assert!(store.install_account_state_bridge(owner).is_err());
    let initial = loads.load(Ordering::Relaxed);
    let callers: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            std::thread::spawn(move || crate::account_state::initialize(&store))
        })
        .collect();
    for caller in callers {
        assert!(caller.join().unwrap().is_err());
    }
    assert_eq!(loads.load(Ordering::Relaxed) - initial, 1);
}

#[test]
#[should_panic(expected = "block tables written outside an account command")]
fn a_block_write_that_bypasses_the_account_writer_fails_the_next_state_read() {
    let (_scratch, store) = fixture("state-block-bypass");
    install(&store);
    // Not a seeding seam: this stands for production code writing a block
    // table without a writer command.
    store
        .bypass_account_writer_for_test(
            "INSERT INTO block(kind,origin_key,provider,account_id,evidence,evidence_at,quota,deadline,next_recheck_at,source_present,managed) VALUES ('observation',json_array('p','a'),'p','a',json_array('k','[]'),1,0,1,1,1,0)",
        )
        .unwrap();
    let _ = super::snapshot(&store);
}

#[test]
fn a_seeded_block_write_is_reloaded_before_the_state_is_read() {
    let (_scratch, store) = fixture("state-block-seed");
    let owner = install(&store);
    let loads = owner.loads.load(Ordering::Relaxed);
    store
        .with_writer_for_test(|conn| {
            conn.execute_batch(
                "INSERT INTO block(kind,origin_key,provider,account_id,evidence,evidence_at,quota,deadline,next_recheck_at,source_present,managed) VALUES ('observation',json_array('p','a'),'p','a',json_array('k','[]'),1,0,1,1,1,0)",
            )
        })
        .unwrap();
    assert!(store.block_seed_writes_for_test() > 0);
    assert_eq!(
        store.block_writes_outside_command(),
        0,
        "seeding is no violation"
    );
    let state = super::snapshot(&store).unwrap();
    assert_eq!(owner.loads.load(Ordering::Relaxed), loads + 1);
    let fresh = store.account_transaction(AccountState::load).unwrap();
    assert_eq!(state.blocks, fresh.blocks);
    assert_eq!(state.blocks.table_sizes()[0], ("block", 1));
}

/// A command that writes a block row and then reads the block tables sees
/// its own write, though the writer copies the row into the working state
/// only when the command ends. Its batch sibling sees it too, and both match
/// SQL.
#[test]
fn a_command_reads_its_own_block_writes_from_the_working_state() {
    let (_scratch, store) = fixture("state-own-block-writes");
    install(&store);
    let fence = striatum_store::AccountPoolFenceRow {
        evidence: striatum_store::AccountFenceRow {
            fence_id: "own-write".into(),
            provider: "p".into(),
            account_id: "a".into(),
            scope_json: "{}".into(),
            fence_json: "{}".into(),
            source: "broca_account_limit".into(),
            observed_at: 1_000,
            principal: "x".into(),
            admission_id: None,
            observation_id: None,
            created_at: 1_000,
            cleared_at: None,
            cleared_reason: None,
        },
        pool_key: None,
        recovery_set: Vec::new(),
    };
    let (own, sql) = crate::account_writer::repair(&store, move |tx| {
        assert!(tx.active_fences("p")?.is_empty());
        tx.insert_pool_fence(&fence)?;
        let own = tx.active_fences("p")?;
        Ok((own, tx.sql.active_fences("p")?))
    })
    .unwrap();
    assert_eq!(own.len(), 1, "the command must see its own fence");
    assert_eq!(own, sql);
    let (later, sql) = crate::account_writer::repair(&store, |tx| {
        tx.clear_fence("own-write", 2_000, "test")?;
        Ok((tx.active_fences("p")?, tx.sql.active_fences("p")?))
    })
    .unwrap();
    assert!(
        later.is_empty(),
        "a cleared fence must leave the command's view"
    );
    assert_eq!(later, sql);
}
