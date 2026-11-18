use super::*;
use std::collections::BTreeSet;
use std::sync::mpsc;
use std::time::Duration;
use striatum_store::{AccountCommandKind, AccountPinRow};

#[path = "offer_tests.rs"]
mod offers;

#[path = "retention_tests.rs"]
mod retention;

#[path = "cap_tests.rs"]
mod caps;

#[path = "reset_tests.rs"]
mod reset;

const T: i64 = 1_790_683_200_000;
const MINUTE: i64 = 60_000;

fn fixture(
    name: &str,
) -> (
    cortexkit_test_support::ScratchDir,
    Arc<SqliteRouteStateStore>,
    Arc<StateOwner>,
) {
    let scratch = cortexkit_test_support::ScratchDir::new(name);
    let store = open(&scratch, name);
    let owner = install(&store, T);
    (scratch, store, owner)
}

fn open(scratch: &cortexkit_test_support::ScratchDir, name: &str) -> Arc<SqliteRouteStateStore> {
    Arc::new(
        SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
            name,
            scratch.path().join("state.db").to_string_lossy(),
        ))
        .unwrap(),
    )
}

fn install(store: &SqliteRouteStateStore, now: i64) -> Arc<StateOwner> {
    let owner = Arc::new(StateOwner {
        load_now_ms: Some(now),
        ..StateOwner::default()
    });
    store.install_account_state_bridge(owner.clone()).unwrap();
    owner
}

fn row(id: &str, now: i64) -> AccountAdmissionRow {
    AccountAdmissionRow {
        principal: "p".into(),
        admission_id: id.into(),
        request_fingerprint: "fp".into(),
        request_mode: "admit".into(),
        decision_id: format!("decision-{id}"),
        conversation_hash: format!("conversation-{id}"),
        provider: "provider".into(),
        model: "m1".into(),
        account_id: "a".into(),
        credential_id: "credential-a".into(),
        pin_id: format!("pin-{id}"),
        pin_generation: 1,
        state: "reserved".into(),
        charged: true,
        request_bytes: 1,
        request_bytes_estimated: false,
        committed_at: now,
        admission_valid_until: now + MINUTE,
        lease_valid_until: now + 2 * MINUTE,
        reply_json: "{}".into(),
        run_id: None,
        started_at: None,
        started_late: false,
        released_at: None,
        finished_at: None,
        last_observed_at: None,
    }
}

fn insert(tx: &mut AccountCommand<'_>, row: &AccountAdmissionRow) -> RouteStateResult<()> {
    tx.insert_pin(&AccountPinRow {
        pin_id: row.pin_id.clone(),
        principal: row.principal.clone(),
        conversation_hash: row.conversation_hash.clone(),
        provider: row.provider.clone(),
        pin_generation: row.pin_generation,
        affinity_model: row.model.clone(),
        account_id: row.account_id.clone(),
        credential_id: row.credential_id.clone(),
        pin_mode: "conversation_lease".into(),
        previous_pin_id: None,
        decision_id: row.decision_id.clone(),
        created_at: row.committed_at,
    })?;
    tx.insert_admission(row)
}

fn run<R: Send + 'static>(
    store: &SqliteRouteStateStore,
    now: i64,
    command: impl FnMut(&mut AccountCommand<'_>) -> RouteStateResult<R> + Send + 'static,
) -> RouteStateResult<R> {
    let _clock = crate::InjectedClock::set(now as u64);
    crate::account_writer::execute(store, AccountCommandKind::Repair, command)
}

fn count(owner: &StateOwner, account: &str, scope: InflightScope<'_>, now: i64) -> usize {
    owner
        .published_snapshot()
        .unwrap()
        .inflight
        .count("provider", account, scope, now)
}

fn reserve(store: &SqliteRouteStateStore, row: AccountAdmissionRow, now: i64) {
    run(store, now, move |tx| insert(tx, &row)).unwrap();
}

fn start(store: &SqliteRouteStateStore, principal: &str, id: &str, run_id: &str, now: i64) {
    let (principal, id, run_id) = (principal.to_owned(), id.to_owned(), run_id.to_owned());
    assert!(run(store, now, move |tx| tx.mark_admission_started(
        &principal,
        &id,
        Some(&run_id),
        now,
        true
    ))
    .unwrap());
}

fn loaded(store: &SqliteRouteStateStore, now: i64) -> AccountState {
    store
        .account_transaction(|tx| AccountState::load_at(tx, now))
        .unwrap()
}

fn assert_restart_count(
    store: &SqliteRouteStateStore,
    owner: &StateOwner,
    now: i64,
    expected: usize,
) {
    assert_eq!(count(owner, "a", InflightScope::Account, now), expected);
    assert_eq!(
        loaded(store, now)
            .inflight
            .count("provider", "a", InflightScope::Account, now),
        expected
    );
}

#[test]
fn inflight_counts_reservations_runs_principals_and_stored_models() {
    let (_scratch, store, owner) = fixture("inflight-grouping");
    reserve(&store, row("one", T), T);
    let mut second = row("two", T);
    second.model = "m2".into();
    reserve(&store, second, T);
    assert_eq!(count(&owner, "a", InflightScope::Account, T), 2);
    start(&store, "p", "one", "run", T);
    start(&store, "p", "two", "run", T);
    assert_restart_count(&store, &owner, T, 1);
    for model in ["m1", "m2"] {
        assert_eq!(count(&owner, "a", InflightScope::AccountModel(model), T), 1);
        assert_eq!(
            loaded(&store, T).inflight.count(
                "provider",
                "a",
                InflightScope::AccountModel(model),
                T
            ),
            1
        );
    }
    let mut other = row("three", T);
    other.principal = "q".into();
    other.request_mode = "reassign".into();
    reserve(&store, other, T);
    start(&store, "q", "three", "run", T);
    assert_restart_count(&store, &owner, T, 2);
    run(&store, T, |tx| {
        tx.mark_admission_finished("p", "one", T)?;
        tx.mark_admission_finished("p", "two", T)?;
        tx.mark_admission_finished("q", "three", T)
    })
    .unwrap();
    assert_restart_count(&store, &owner, T, 0);
}

#[test]
fn inflight_renewals_group_by_conversation_and_cap_count_until_the_last_closes() {
    let (_scratch, store, owner) = fixture("inflight-renewal");
    let mut original = row("original", T);
    original.conversation_hash = "conversation".into();
    reserve(&store, original, T);
    start(&store, "p", "original", "run", T);
    for (id, seconds) in [("renew-one", 10), ("renew-two", 20)] {
        let mut renewal = row(id, T);
        renewal.request_mode = "renew".into();
        renewal.charged = false;
        renewal.conversation_hash = "conversation".into();
        renewal.admission_valid_until = T + seconds * 1_000;
        renewal.pin_generation = if id == "renew-one" { 2 } else { 3 };
        reserve(&store, renewal, T);
    }
    assert_restart_count(&store, &owner, T, 1);
    // A different model cannot suppress renewals in this model's cap count.
    let mut other_model = row("other-model", T);
    other_model.conversation_hash = "conversation".into();
    other_model.model = "m2".into();
    other_model.pin_generation = 4;
    reserve(&store, other_model, T);
    run(&store, T + 1, |tx| {
        tx.mark_admission_finished("p", "original", T + 1)
    })
    .unwrap();
    for (now, expected) in [(T + 1, 1), (T + 10_000, 1), (T + 20_000, 0)] {
        assert_eq!(
            count(&owner, "a", InflightScope::AccountModel("m1"), now),
            expected
        );
        assert_eq!(
            loaded(&store, now).inflight.count(
                "provider",
                "a",
                InflightScope::AccountModel("m1"),
                now
            ),
            expected
        );
        assert_restart_count(&store, &owner, now, 1);
    }
    let mut first = row("started-renew-one", T);
    first.request_mode = "renew".into();
    let mut second = row("started-renew-two", T);
    second.request_mode = "renew".into();
    reserve(&store, first, T);
    reserve(&store, second, T);
    start(&store, "p", "started-renew-one", "shared-run", T);
    start(&store, "p", "started-renew-two", "shared-run", T);
    assert_eq!(
        count(&owner, "a", InflightScope::AccountModel("m1"), T + 20_000),
        2
    );
}

#[test]
fn inflight_lapses_without_changing_rows_and_observations_restore_pruned_entries() {
    let (_scratch, store, owner) = fixture("inflight-lapse");
    reserve(&store, row("started", T), T);
    reserve(&store, row("unstarted", T), T);
    start(&store, "p", "started", "run", T);
    assert_restart_count(&store, &owner, T + MINUTE - 1, 2);
    assert_restart_count(&store, &owner, T + MINUTE, 1);
    assert_eq!(
        owner
            .published_snapshot()
            .unwrap()
            .admissions
            .open_admission("p", "unstarted")
            .unwrap()
            .row
            .state,
        "reserved"
    );
    assert_restart_count(&store, &owner, T + 30 * MINUTE - 1_000, 1);
    assert_restart_count(&store, &owner, T + 30 * MINUTE, 0);
    assert_eq!(
        owner
            .published_snapshot()
            .unwrap()
            .admissions
            .open_admission("p", "started")
            .unwrap()
            .row
            .state,
        "started"
    );
    // Refilling drops old ledger entries, not the durable rows observations need.
    let later = T + 30 * MINUTE;
    run(&store, later, |tx| tx.demand("provider", "a", None)).unwrap();
    assert_eq!(
        owner
            .published_snapshot()
            .unwrap()
            .inflight
            .entry_count("provider", "a"),
        0
    );
    reserve(&store, row("refill", later), later);
    assert_eq!(
        owner
            .published_snapshot()
            .unwrap()
            .inflight
            .entry_count("provider", "a"),
        1
    );
    run(&store, later, move |tx| {
        let row = tx.admission("p", "started")?.unwrap();
        tx.observe_admission(&row, later)
    })
    .unwrap();
    assert_restart_count(&store, &owner, later, 2);
    run(&store, later, |tx| {
        tx.mark_admission_expired("p", "unstarted")
    })
    .unwrap();
    start(&store, "p", "unstarted", "late-run", later);
    assert_restart_count(&store, &owner, later, 3);
    run(&store, later, move |tx| {
        tx.mark_admission_released("p", "refill", later)
    })
    .unwrap();
    assert_restart_count(&store, &owner, later, 2);
    assert_restart_count(&store, &owner, later + 30 * MINUTE, 0);
}

#[test]
fn inflight_loader_is_bounded_and_counts_do_not_visit_admission_history() {
    let (scratch, store, owner) = fixture("inflight-account-local");
    run(&store, T, |tx| {
        for i in 0..500 {
            let mut historical = row(&format!("historical-{i}"), T - 120 * MINUTE);
            historical.state = "started".into();
            historical.started_at = Some(T - 120 * MINUTE);
            historical.last_observed_at = historical.started_at;
            historical.run_id = Some(format!("historical-run-{i}"));
            insert(tx, &historical)?;
        }
        let live = row("live", T);
        insert(tx, &live)?;
        for i in 0..50 {
            let mut other = row(&format!("other-{i}"), T);
            other.account_id = "b".into();
            insert(tx, &other)?;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(
        owner
            .published_snapshot()
            .unwrap()
            .inflight
            .entry_count("provider", "a"),
        1
    );
    assert_eq!(
        owner.published_snapshot().unwrap().admissions.open.len(),
        551
    );
    let mut detached = loaded(&store, T);
    detached.admissions = AdmissionState::default();
    assert_eq!(
        detached
            .inflight
            .count("provider", "a", InflightScope::Account, T),
        1
    );
    assert_eq!(
        detached
            .inflight
            .count("provider", "b", InflightScope::Account, T),
        50
    );
    drop(owner);
    drop(store);
    let restarted = open(&scratch, "inflight-account-local");
    let after = install(&restarted, T);
    assert_eq!(
        after
            .published_snapshot()
            .unwrap()
            .inflight
            .entry_count("provider", "a"),
        1
    );
    assert_eq!(count(&after, "a", InflightScope::Account, T), 1);
    assert_eq!(count(&after, "a", InflightScope::Account, T + MINUTE), 0);
}

/// Reference partitions are formed by comparing whole durable rows rather
/// than using the ledger's sets or its stored window deadlines.
fn oracle_count(
    rows: &[AccountAdmissionRow],
    account: &str,
    model: Option<&str>,
    now: i64,
) -> usize {
    let active: Vec<_> = rows
        .iter()
        .filter(|row| {
            row.account_id == account
                && model.is_none_or(|model| row.model == model)
                && match row.state.as_str() {
                    "reserved" => row.admission_valid_until > now,
                    "started" => row
                        .started_at
                        .into_iter()
                        .chain(row.last_observed_at)
                        .max()
                        .is_some_and(|last| now - last < 30 * MINUTE),
                    _ => false,
                }
        })
        .collect();
    active
        .iter()
        .enumerate()
        .filter(|(index, row)| {
            if row.request_mode == "renew" {
                !active.iter().any(|other| {
                    other.request_mode != "renew"
                        && other.principal == row.principal
                        && other.conversation_hash == row.conversation_hash
                }) && !active[..*index].iter().any(|other| {
                    other.request_mode == "renew"
                        && other.principal == row.principal
                        && other.conversation_hash == row.conversation_hash
                })
            } else {
                row.run_id.is_none()
                    || !active[..*index].iter().any(|other| {
                        other.request_mode != "renew"
                            && other.principal == row.principal
                            && other.run_id == row.run_id
                    })
            }
        })
        .count()
}

#[test]
fn inflight_randomized_writer_sequences_equal_durable_rows_at_each_restart_instant() {
    for seed in [7_u64, 91] {
        let (_scratch, store, owner) = fixture(&format!("inflight-sequence-{seed}"));
        let mut random = seed;
        let mut next = || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };
        let mut now = T;
        let mut ids = Vec::new();
        let mut seen_operations = BTreeSet::new();
        for step in 0..160 {
            now += [1, 1_000, 30_000, 30 * MINUTE][next() as usize % 4];
            let operation = if ids.is_empty() {
                0
            } else {
                next() as usize % 7
            };
            seen_operations.insert(operation);
            if operation == 0 {
                let mut admission = row(&format!("admission-{step}"), now);
                admission.principal = format!("principal-{}", next() % 2);
                admission.pin_generation = step + 1;
                admission.conversation_hash = format!("conversation-{}", next() % 3);
                admission.model = format!("m{}", 1 + next() % 2);
                admission.account_id = if next() % 2 == 0 { "a" } else { "b" }.into();
                admission.request_mode = ["admit", "renew", "reassign"][next() as usize % 3].into();
                admission.charged = admission.request_mode != "renew";
                ids.push((admission.principal.clone(), admission.admission_id.clone()));
                reserve(&store, admission, now);
            } else {
                let (principal, id) = ids[next() as usize % ids.len()].clone();
                let run_id = format!("run-{}", next() % 3);
                run(&store, now, move |tx| {
                    match operation {
                        1 => {
                            tx.mark_admission_started(&principal, &id, Some(&run_id), now, true)?;
                        }
                        2 => {
                            let row = tx.admission(&principal, &id)?.unwrap();
                            tx.observe_admission(&row, now)?;
                        }
                        3 => {
                            tx.mark_admission_released(&principal, &id, now)?;
                        }
                        4 => {
                            tx.mark_admission_finished(&principal, &id, now)?;
                        }
                        5 => {
                            tx.mark_admission_expired(&principal, &id)?;
                        }
                        _ => {}
                    }
                    Ok(())
                })
                .unwrap();
            }
            let rows = store
                .account_transaction(|tx| tx.load_admission_group())
                .unwrap()
                .open
                .into_iter()
                .map(|open| open.row)
                .collect::<Vec<_>>();
            let restarted = loaded(&store, now);
            for account in ["a", "b"] {
                for (scope, model) in [
                    (InflightScope::Account, None),
                    (InflightScope::AccountModel("m1"), Some("m1")),
                    (InflightScope::AccountModel("m2"), Some("m2")),
                ] {
                    let expected = oracle_count(&rows, account, model, now);
                    assert_eq!(
                        count(&owner, account, scope, now),
                        expected,
                        "seed={seed} step={step}"
                    );
                    assert_eq!(
                        restarted.inflight.count("provider", account, scope, now),
                        expected,
                        "restart seed={seed} step={step}"
                    );
                }
            }
        }
        assert_eq!(seen_operations.len(), 7);
    }
}

#[test]
fn inflight_batch_sees_prior_commands_and_publishes_only_after_commit() {
    let (_scratch, store, owner) = fixture("inflight-batch-publication");
    let (committing, committed) = mpsc::channel();
    let (release, held) = mpsc::channel();
    let armed = Arc::new(AtomicBool::new(false));
    let hook_armed = armed.clone();
    store
        .with_writer_for_test(|conn| {
            conn.commit_hook(Some(move || {
                if hook_armed.load(Ordering::Relaxed) {
                    committing.send(()).unwrap();
                    held.recv().unwrap();
                }
                false
            }));
            Ok(())
        })
        .unwrap();
    let (entered, running) = mpsc::channel();
    let (resume, paused) = mpsc::channel();
    let barrier = store
        .enqueue_account_command(AccountCommandKind::Maintenance, move |_| {
            entered.send(()).unwrap();
            paused.recv().unwrap();
            Ok(())
        })
        .unwrap();
    running.recv_timeout(Duration::from_secs(5)).unwrap();
    let failed = store
        .enqueue_account_state_command::<()>(AccountCommandKind::Admit, |sql, state| {
            let _clock = crate::InjectedClock::set(T as u64);
            let mut tx = AccountCommand::new(sql, state);
            insert(&mut tx, &row("rolled-back", T))?;
            Err(RouteStateStoreError("forced savepoint failure".into()))
        })
        .unwrap();
    let mut commands = Vec::new();
    for i in 0..2 {
        let armed = armed.clone();
        commands.push(
            store
                .enqueue_account_state_command(AccountCommandKind::Admit, move |sql, state| {
                    let _clock = crate::InjectedClock::set(T as u64);
                    let mut tx = AccountCommand::new(sql, state);
                    let before = tx.inflight_count("provider", "a", InflightScope::Account, T);
                    insert(&mut tx, &row(&format!("committed-{i}"), T))?;
                    let after = tx.inflight_count("provider", "a", InflightScope::Account, T);
                    if i == 1 {
                        armed.store(true, Ordering::Relaxed);
                    }
                    Ok((before, after))
                })
                .unwrap(),
        );
    }
    resume.send(()).unwrap();
    barrier.wait().unwrap();
    committed.recv_timeout(Duration::from_secs(5)).unwrap();
    let unpublished = count(&owner, "a", InflightScope::Account, T);
    release.send(()).unwrap();
    assert!(failed.wait().is_err());
    assert_eq!(unpublished, 0);
    for (i, command) in commands.into_iter().enumerate() {
        assert_eq!(command.wait().unwrap(), (i, i + 1));
    }
    store
        .with_writer_for_test(|conn| {
            conn.commit_hook(None::<fn() -> bool>);
            Ok(())
        })
        .unwrap();
    assert_restart_count(&store, &owner, T, 2);
}

#[test]
fn inflight_failed_batch_commit_discards_every_command_and_its_ledger() {
    let (_scratch, store, owner) = fixture("inflight-failed-commit");
    let armed = Arc::new(AtomicBool::new(false));
    let hook_armed = armed.clone();
    store
        .with_writer_for_test(|conn| {
            conn.commit_hook(Some(move || hook_armed.load(Ordering::Relaxed)));
            Ok(())
        })
        .unwrap();
    let (entered, running) = mpsc::channel();
    let (resume, paused) = mpsc::channel();
    let barrier = store
        .enqueue_account_command(AccountCommandKind::Maintenance, move |_| {
            entered.send(()).unwrap();
            paused.recv().unwrap();
            Ok(())
        })
        .unwrap();
    running.recv_timeout(Duration::from_secs(5)).unwrap();
    let (working, counts) = mpsc::channel();
    let mut commands = Vec::new();
    for i in 0..2 {
        let (armed, working) = (armed.clone(), working.clone());
        commands.push(
            store
                .enqueue_account_state_command(AccountCommandKind::Admit, move |sql, state| {
                    let _clock = crate::InjectedClock::set(T as u64);
                    let mut tx = AccountCommand::new(sql, state);
                    insert(&mut tx, &row(&format!("failed-{i}"), T))?;
                    working
                        .send(tx.inflight_count("provider", "a", InflightScope::Account, T))
                        .unwrap();
                    if i == 1 {
                        armed.store(true, Ordering::Relaxed);
                    }
                    Ok(())
                })
                .unwrap(),
        );
    }
    resume.send(()).unwrap();
    barrier.wait().unwrap();
    for command in commands {
        let error = command.wait().unwrap_err().to_string();
        assert!(
            error.contains("store_error") && error.contains("retry"),
            "{error}"
        );
    }
    store
        .with_writer_for_test(|conn| {
            conn.commit_hook(None::<fn() -> bool>);
            Ok(())
        })
        .unwrap();
    assert_eq!(counts.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
    assert_eq!(counts.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
    assert_restart_count(&store, &owner, T, 0);
    assert!(store
        .account_transaction(|tx| tx.admission("p", "failed-0"))
        .unwrap()
        .is_none());
}

#[test]
fn inflight_divergence_uses_the_writer_reload_instead_of_publishing_a_different_ledger() {
    let (_scratch, store, owner) = fixture("inflight-divergence");
    reserve(&store, row("original", T), T);
    assert_restart_count(&store, &owner, T, 1);
    let mut stale = loaded(&store, T);
    store
        .with_writer_for_test(|conn| {
            conn.execute(
                "UPDATE account_admission SET state='released' WHERE admission_id='original'",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    let error = store
        .account_transaction(|sql| {
            let _clock = crate::InjectedClock::set(T as u64);
            AccountCommand::new(sql, Some(&mut stale)).mark_admission_started(
                "p",
                "original",
                Some("run"),
                T,
                false,
            )
        })
        .unwrap_err()
        .to_string();
    assert!(error.contains(striatum_store::STATE_DIVERGED), "{error}");
    let loads = owner.loads.load(Ordering::Relaxed);
    let changed = run(&store, T, |tx| {
        tx.mark_admission_started("p", "original", Some("run"), T, false)
    })
    .unwrap();
    assert!(!changed);
    assert!(owner.loads.load(Ordering::Relaxed) > loads);
    assert_restart_count(&store, &owner, T, 0);
}

#[test]
fn inflight_attempt_readds_each_lapsed_started_reservation_it_reaches() {
    let (_scratch, store, owner) = fixture("inflight-observation-reach");
    run(&store, T, |tx| {
        insert(tx, &row("original", T))?;
        tx.mark_admission_started("p", "original", Some("run"), T, false)?;
        let mut reservation = row("reservation", T);
        reservation.account_id = "b".into();
        insert(tx, &reservation)?;
        let record = V1Recorded {
            body_json: "{}".into(),
            answer_json: "{}".into(),
        };
        tx.record_continuation(
            "p",
            "offer",
            &record,
            "run",
            "original",
            "decision-original",
            Some(("decision-reservation", "reservation", T + MINUTE)),
        )?;
        tx.use_continuation("decision-reservation", T)
    })
    .unwrap();
    assert_eq!(count(&owner, "b", InflightScope::Account, T), 1);
    let later = T + 30 * MINUTE;
    let mut refill = row("b-refill", later);
    refill.account_id = "b".into();
    reserve(&store, refill, later);
    reserve(&store, row("a-refill", later), later);
    assert_eq!(
        owner
            .published_snapshot()
            .unwrap()
            .inflight
            .entry_count("provider", "b"),
        1
    );
    run(&store, later, move |tx| {
        let original = tx.admission("p", "original")?.unwrap();
        tx.observe_admission(&original, later)
    })
    .unwrap();
    assert_eq!(count(&owner, "b", InflightScope::Account, later), 2);
    assert_eq!(
        loaded(&store, later)
            .inflight
            .count("provider", "b", InflightScope::Account, later),
        2
    );
    assert_eq!(
        count(&owner, "b", InflightScope::Account, later + MINUTE),
        1
    );
    run(&store, later + MINUTE, move |tx| {
        tx.finish_continuations("p", "original", "run", later + MINUTE)
    })
    .unwrap();
    assert_eq!(
        count(&owner, "b", InflightScope::Account, later + MINUTE),
        0
    );
}

#[test]
fn inflight_upgrade_preserves_recent_attempts_without_resetting_the_lapse() {
    let scratch = cortexkit_test_support::ScratchDir::new("inflight-upgrade");
    let descriptor = striatum_store::dev_descriptor(
        "inflight-upgrade",
        scratch.path().join("state.db").to_string_lossy(),
    );
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 88).unwrap();
    old.with_writer_for_test(|conn| {
        conn.execute_batch("INSERT INTO account_pin (pin_id, principal, conversation_hash, provider,
            pin_generation, affinity_model, account_id, credential_id, pin_mode, decision_id, created_at)
            VALUES ('pin', 'p', 'conversation', 'provider', 1, 'm1', 'a', 'credential', 'run_frozen', 'd', 0);")?;
        conn.execute("INSERT INTO account_admission (principal, admission_id, request_fingerprint, request_mode,
            decision_id, conversation_hash, provider, model, account_id, credential_id, pin_id,
            pin_generation, state, charged, request_bytes, request_bytes_estimated, committed_at,
            admission_valid_until, lease_valid_until, reply_json, run_id, started_at)
            VALUES ('p', 'original', 'fp', 'admit', 'd', 'conversation', 'provider', 'm1', 'a', 'credential',
            'pin', 1, 'started', 1, 1, 0, ?1, ?1+60000, ?1+120000, '{}', 'run', ?1)", [T - 120 * MINUTE])?;
        conn.execute("INSERT INTO account_observation (observation_id, principal, runner, event_id, kind,
            admission_id, bound, observation_json, effects_json, received_at)
            VALUES ('observation', 'p', 'runner', 'event', 'attempt', 'original', 1, '{}', '[]', ?1)", [T - MINUTE])?;
        Ok(())
    }).unwrap();
    drop(old);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    let owner = install(&store, T);
    assert_restart_count(&store, &owner, T, 1);
    assert_restart_count(&store, &owner, T + 29 * MINUTE - 1, 1);
    assert_restart_count(&store, &owner, T + 29 * MINUTE, 0);
    drop(owner);
    drop(store);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    let owner = install(&store, T);
    assert_restart_count(&store, &owner, T, 1);
    assert_restart_count(&store, &owner, T + 29 * MINUTE, 0);
}

#[test]
fn tick_expires_untouched_inflight_accounts_without_changing_durable_rows() {
    let (_scratch, store, owner) = fixture("inflight-tick-expiry");
    reserve(&store, row("first", T), T);
    let mut other = row("other", T);
    other.account_id = "b".into();
    other.credential_id = "credential-b".into();
    reserve(&store, other, T);
    let mut started = row("started", T);
    started.account_id = "c".into();
    started.credential_id = "credential-c".into();
    reserve(&store, started, T);
    start(&store, "p", "started", "run", T);
    let deadline = T + MINUTE;
    assert_eq!(count(&owner, "a", InflightScope::Account, deadline - 1), 1);
    assert_eq!(count(&owner, "b", InflightScope::Account, deadline - 1), 1);
    assert_eq!(count(&owner, "a", InflightScope::Account, deadline), 0);
    assert_eq!(count(&owner, "b", InflightScope::Account, deadline), 0);
    assert_eq!(
        owner
            .published_snapshot()
            .unwrap()
            .inflight
            .entry_count("provider", "a"),
        1
    );
    assert_eq!(
        owner
            .published_snapshot()
            .unwrap()
            .inflight
            .entry_count("provider", "b"),
        1
    );
    let loads = owner.loads.load(Ordering::Relaxed);
    let trace = striatum_store::writer_entry_trace::Trace::default();
    let _trace = trace.install();
    run(&store, deadline, move |tx| tx.tick(deadline)).unwrap();
    let state = owner.published_snapshot().unwrap();
    assert_eq!(state.inflight.entry_count("provider", "a"), 0);
    assert_eq!(state.inflight.entry_count("provider", "b"), 0);
    assert_eq!(state.inflight.entry_count("provider", "c"), 1);
    assert_eq!(
        state
            .admissions
            .open_admission("p", "first")
            .unwrap()
            .row
            .state,
        "reserved"
    );
    assert_eq!(
        state
            .admissions
            .open_admission("p", "other")
            .unwrap()
            .row
            .state,
        "reserved"
    );
    crate::account_state::assert_fresh_load_for_test(
        &store,
        deadline,
        "Tick expired both untouched accounts",
    );
    run(&store, deadline, move |tx| tx.tick(deadline)).unwrap();
    let started_deadline = T + 30 * MINUTE;
    assert_eq!(
        count(&owner, "c", InflightScope::Account, started_deadline),
        0
    );
    run(&store, started_deadline, move |tx| {
        tx.tick(started_deadline)
    })
    .unwrap();
    assert_eq!(
        owner
            .published_snapshot()
            .unwrap()
            .inflight
            .entry_count("provider", "c"),
        0
    );
    assert_eq!(
        owner
            .published_snapshot()
            .unwrap()
            .admissions
            .open_admission("p", "started")
            .unwrap()
            .row
            .state,
        "started"
    );
    crate::account_state::assert_fresh_load_for_test(
        &store,
        started_deadline,
        "Tick expired the unobserved started run",
    );
    assert_eq!(
        owner.loads.load(Ordering::Relaxed),
        loads,
        "a reload hid an expiry mirror"
    );
    assert!(
        !trace.entries().is_empty(),
        "the writer trace did not observe the ticks"
    );
    assert!(
        trace
            .entries()
            .iter()
            .all(|entry| entry.account_rows_written == Some(0)),
        "cache expiry wrote durable account rows: {:?}",
        trace.entries()
    );
}
