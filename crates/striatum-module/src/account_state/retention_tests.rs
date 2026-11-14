use super::*;
use offers::{counts, offer, original};

const DAY: i64 = 24 * 60 * MINUTE;
const NOW: i64 = T + 40 * DAY;

fn admissions(store: &SqliteRouteStateStore, now: i64, limit: usize) -> usize {
    run(store, now, move |tx| {
        tx.prune_account_admissions(now - 30 * DAY, limit, now)
    })
    .unwrap()
}

fn continuations(store: &SqliteRouteStateStore, now: i64, limit: usize) -> usize {
    run(store, now, move |tx| {
        tx.prune_account_continuations(now - 7 * DAY, limit)
    })
    .unwrap()
}

fn exists(store: &SqliteRouteStateStore, id: &str) -> bool {
    store
        .account_transaction(|tx| tx.admission("p", id))
        .unwrap()
        .is_some()
}

fn observe(store: &SqliteRouteStateStore, id: &str, now: i64) {
    let id = id.to_owned();
    run(store, now, move |tx| {
        let row = tx.admission("p", &id)?.expect("remaining admission");
        tx.observe_admission(&row, now)
    })
    .unwrap();
}

fn restart(
    scratch: &cortexkit_test_support::ScratchDir,
    store: Arc<SqliteRouteStateStore>,
    owner: Arc<StateOwner>,
    now: i64,
) -> (Arc<SqliteRouteStateStore>, Arc<StateOwner>) {
    drop(owner);
    drop(store);
    let store = open(scratch, "retention-restart");
    let owner = install(&store, now);
    (store, owner)
}

#[test]
fn retention_prunes_legacy_rows_and_lapsed_runs_but_not_recent_observations() {
    let (_scratch, store, owner) = fixture("retention-observation");
    for (id, state) in [
        ("released", "released"),
        ("unstarted", "reserved"),
        ("pre-migration-finished", "finished"),
        ("lapsed", "started"),
        ("observed", "started"),
    ] {
        let mut row = row(id, T);
        row.state = state.into();
        if state != "reserved" {
            row.started_at = Some(T);
            row.run_id = Some(id.into());
        }
        if state == "released" {
            row.released_at = Some(T);
        }
        if state == "finished" {
            row.finished_at = Some(T);
        }
        if state == "started" {
            row.last_observed_at = Some(if id == "observed" { NOW - MINUTE } else { T });
        }
        reserve(&store, row, T);
    }
    counts(&store, &owner, NOW, [1, 0, 0]);
    assert_eq!(admissions(&store, NOW, 100), 4);
    for id in ["released", "unstarted", "pre-migration-finished", "lapsed"] {
        assert!(!exists(&store, id), "{id} should be pruned");
    }
    assert!(exists(&store, "observed"));
    counts(&store, &owner, NOW, [1, 0, 0]);
}

#[test]
fn retention_checks_open_windows_independently_of_the_cutoff() {
    let (_scratch, store, owner) = fixture("retention-open-window");
    original(&store);
    reserve(&store, row("reserved", T), T);
    let cutoff = T + 4 * MINUTE;
    assert_eq!(
        run(&store, T + 30_000, move |tx| {
            tx.prune_account_admissions(cutoff, 100, T + 30_000)
        })
        .unwrap(),
        0
    );
    counts(&store, &owner, T + 30_000, [2, 0, 0]);
    assert_eq!(
        run(&store, T + 5 * MINUTE, move |tx| {
            tx.prune_account_admissions(cutoff, 100, T + 5 * MINUTE)
        })
        .unwrap(),
        1
    );
    assert!(exists(&store, "original"));
    assert_eq!(
        run(&store, T + 30 * MINUTE, move |tx| {
            tx.prune_account_admissions(cutoff, 100, T + 30 * MINUTE)
        })
        .unwrap(),
        1
    );
    counts(&store, &owner, T + 30 * MINUTE, [0, 0, 0]);
}

fn finished_move_with_sibling(store: &SqliteRouteStateStore, fresh: bool) {
    original(store);
    reserve(store, row("sibling", T), T);
    start(store, "p", "sibling", "run", T);
    run(store, T + 1, |tx| {
        offer(tx, "b", "original", "decision-original", "b", T + 1)?;
        tx.use_continuation("decision-b", T + 1)?;
        tx.mark_admission_finished("p", "original", T + 2)?;
        tx.finish_continuations("p", "original", "run", T + 2)
    })
    .unwrap();
    if fresh {
        observe(store, "sibling", NOW - MINUTE);
    }
}

#[test]
fn retention_keeps_closed_source_evidence_for_a_fresh_succeeded_sibling() {
    let (scratch, store, owner) = fixture("retention-fresh-sibling");
    finished_move_with_sibling(&store, true);
    counts(&store, &owner, NOW, [0, 0, 0]);
    admissions(&store, NOW, 100);
    assert!(exists(&store, "original"));
    assert_eq!(continuations(&store, NOW, 100), 0);
    counts(&store, &owner, NOW, [0, 0, 0]);
    let (store, owner) = restart(&scratch, store, owner, NOW);
    observe(&store, "sibling", NOW);
    counts(&store, &owner, NOW, [0, 0, 0]);
}

#[test]
fn retention_removes_due_succeeded_sibling_before_its_closed_source() {
    let (_scratch, store, owner) = fixture("retention-due-sibling");
    finished_move_with_sibling(&store, false);
    counts(&store, &owner, NOW, [0, 0, 0]);
    admissions(&store, NOW, 100);
    assert!(!exists(&store, "sibling"));
    assert!(exists(&store, "original"));
    admissions(&store, NOW, 100);
    assert!(!exists(&store, "original"));
    assert_eq!(continuations(&store, NOW, 100), 1);
    counts(&store, &owner, NOW, [0, 0, 0]);
}

#[test]
fn retention_one_row_batches_keep_source_until_all_other_succeeded_rows_go() {
    let (scratch, mut store, mut owner) = fixture("retention-batch-source");
    original(&store);
    for id in ["sibling-1", "sibling-2"] {
        reserve(&store, row(id, T), T);
        start(&store, "p", id, "run", T);
    }
    run(&store, T + 1, |tx| {
        offer(tx, "b", "original", "decision-original", "b", T + 1)?;
        tx.use_continuation("decision-b", T + 1)?;
        tx.mark_admission_finished("p", "b", T + 2)
    })
    .unwrap();
    for pass in 0..5 {
        // Each late attempt is allowed to renew observation time. Advance the
        // next pass so that the remaining row is genuinely due again.
        let now = NOW + pass * 40 * DAY;
        admissions(&store, now, 1);
        continuations(&store, now, 1);
        let remaining = ["sibling-1", "sibling-2"]
            .into_iter()
            .find(|id| exists(&store, id));
        if let Some(id) = remaining {
            assert!(exists(&store, "original"));
            observe(&store, id, now);
            counts(&store, &owner, now, [0, 0, 0]);
            (store, owner) = restart(&scratch, store, owner, now);
            observe(&store, id, now + 1);
        }
        counts(&store, &owner, now + 1, [0, 0, 0]);
    }
    assert!(!exists(&store, "original"));
    assert!(!exists(&store, "b"));
    assert_eq!(continuations(&store, NOW + 200 * DAY, 1), 0);
}

#[test]
fn retention_reservation_liveness_uses_values_not_deletion_dependencies() {
    for limit in [1, 100] {
        let (_scratch, store, owner) = fixture("retention-lapsed-move");
        original(&store);
        run(&store, T + 1, |tx| {
            offer(tx, "b", "original", "decision-original", "b", T + 1)?;
            tx.use_continuation("decision-b", T + 1)
        })
        .unwrap();
        counts(&store, &owner, NOW, [0, 0, 0]);
        let mut deleted = [0, 0];
        for _ in 0..3 {
            deleted[0] += admissions(&store, NOW, limit);
            deleted[1] += continuations(&store, NOW, limit);
            counts(&store, &owner, NOW, [0, 0, 0]);
        }
        assert_eq!(deleted, [2, 1]);
    }
    let (_scratch, store, owner) = fixture("retention-live-move");
    original(&store);
    run(&store, NOW - MINUTE, |tx| {
        offer(tx, "b", "original", "decision-original", "b", NOW - MINUTE)?;
        tx.use_continuation("decision-b", NOW - MINUTE)
    })
    .unwrap();
    counts(&store, &owner, NOW, [0, 1, 0]);
    assert_eq!(admissions(&store, NOW, 100), 0);
    counts(&store, &owner, NOW, [0, 1, 0]);
}

#[test]
fn retention_keeps_a_predecessor_reservation_and_offer_for_its_succeeded_sibling() {
    let (scratch, store, owner) = fixture("retention-reservation-source");
    original(&store);
    run(&store, T + 1, |tx| {
        offer(tx, "b", "original", "decision-original", "b", T + 1)?;
        tx.use_continuation("decision-b", T + 1)
    })
    .unwrap();
    let mut sibling = row("b-sibling", T + 2);
    sibling.account_id = "b".into();
    reserve(&store, sibling, T + 2);
    start(&store, "p", "b-sibling", "run", T + 2);
    run(&store, T + 3, |tx| {
        offer(tx, "c", "original", "decision-b", "c", T + 3)?;
        tx.use_continuation("decision-c", T + 3)?;
        tx.mark_admission_finished("p", "original", T + 4)?;
        tx.finish_continuations("p", "original", "run", T + 4)
    })
    .unwrap();
    observe(&store, "b-sibling", NOW - MINUTE);
    counts(&store, &owner, NOW, [0, 0, 0]);
    admissions(&store, NOW, 100);
    assert!(exists(&store, "b"));
    assert_eq!(continuations(&store, NOW, 100), 0);
    let (store, owner) = restart(&scratch, store, owner, NOW);
    observe(&store, "b-sibling", NOW);
    counts(&store, &owner, NOW, [0, 0, 0]);
}

#[test]
fn retention_keeps_the_second_used_offer_while_its_predecessor_is_started() {
    let (_scratch, store, owner) = fixture("retention-started-predecessor");
    reserve(&store, row("original", T), T);
    run(&store, T, |tx| {
        tx.mark_admission_expired("p", "original")?;
        offer(tx, "b", "original", "decision-original", "b", T)?;
        tx.use_continuation("decision-b", T)?;
        offer(tx, "c", "original", "decision-b", "c", T + 1)?;
        tx.use_continuation("decision-c", T + 1)?;
        tx.mark_admission_finished("p", "c", T + 2)
    })
    .unwrap();
    assert_eq!(continuations(&store, NOW, 100), 0);
    assert!(exists(&store, "b"));
    counts(&store, &owner, NOW, [0, 0, 0]);
}

#[test]
fn retention_prunes_offer_chains_last_first_without_changing_a_later_hold() {
    let (scratch, mut store, mut owner) = fixture("retention-offer-chain");
    original(&store);
    run(&store, T + 1, |tx| {
        offer(tx, "b", "original", "decision-original", "b", T + 1)?;
        tx.use_continuation("decision-b", T + 1)
    })
    .unwrap();
    reserve(&store, row("later", T + 2), T + 2);
    start(&store, "p", "later", "run", T + 2);
    run(&store, T + 3, |tx| {
        offer(tx, "c", "original", "decision-b", "c", T + 3)?;
        tx.use_continuation("decision-c", T + 3)?;
        tx.mark_admission_finished("p", "original", NOW - 8 * DAY)?;
        tx.finish_continuations("p", "original", "run", NOW - 8 * DAY)
    })
    .unwrap();
    observe(&store, "later", NOW - MINUTE);
    counts(&store, &owner, NOW, [1, 0, 0]);
    for expected in [1, 1, 0] {
        assert_eq!(continuations(&store, NOW, 1), expected);
        // Without the first offer, reload resolves the second offer's source
        // to the original admission instead of the first reservation. That
        // would incorrectly succeed the new hold on the original account.
        (store, owner) = restart(&scratch, store, owner, NOW);
        counts(&store, &owner, NOW, [1, 0, 0]);
    }
}
