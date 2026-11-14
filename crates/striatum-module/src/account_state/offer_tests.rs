use super::*;

pub(super) fn offer(
    tx: &mut AccountCommand<'_>,
    id: &str,
    original: &str,
    previous: &str,
    account: &str,
    now: i64,
) -> RouteStateResult<()> {
    let mut reservation = row(id, now);
    reservation.account_id = account.into();
    reservation.credential_id = format!("credential-{account}");
    insert(tx, &reservation)?;
    tx.record_continuation(
        "p",
        id,
        &V1Recorded {
            body_json: "{}".into(),
            answer_json: "{}".into(),
        },
        "run",
        original,
        previous,
        Some((&reservation.decision_id, id, now + MINUTE)),
    )
}

pub(super) fn counts(
    store: &SqliteRouteStateStore,
    owner: &StateOwner,
    now: i64,
    expected: [usize; 3],
) {
    let restarted = loaded(store, now);
    for (account, expected) in ["a", "b", "c"].into_iter().zip(expected) {
        assert_eq!(
            count(owner, account, InflightScope::Account, now),
            expected,
            "live {account} at {now}"
        );
        assert_eq!(
            restarted
                .inflight
                .count("provider", account, InflightScope::Account, now),
            expected,
            "restart {account} at {now}"
        );
    }
}

pub(super) fn original(store: &SqliteRouteStateStore) {
    reserve(store, row("original", T), T);
    start(store, "p", "original", "run", T);
}

#[test]
fn used_offer_transfers_one_of_six_and_sibling_reservations_are_distinct() {
    let (_scratch, store, owner) = fixture("offer-transfer-six");
    original(&store);
    for i in 0..5 {
        let id = format!("other-{i}");
        reserve(&store, row(&id, T), T);
        start(&store, "p", &id, &id, T);
    }
    run(&store, T + 1, |tx| {
        offer(tx, "b1", "original", "decision-original", "b", T + 1)?;
        offer(tx, "b2", "original", "decision-original", "b", T + 1)
    })
    .unwrap();
    counts(&store, &owner, T + 1, [6, 2, 0]);
    run(&store, T + 2, |tx| {
        tx.use_continuation("decision-b1", T + 2)
    })
    .unwrap();
    counts(&store, &owner, T + 2, [5, 1, 0]);
    run(&store, T + 3, |tx| {
        let row = tx.admission("p", "original")?.unwrap();
        tx.observe_admission(&row, T + 3)
    })
    .unwrap();
    counts(&store, &owner, T + 3, [5, 1, 0]);
}

#[test]
fn unused_offer_expiry_late_use_and_run_finish_leave_no_phantom_windows() {
    let (_scratch, store, owner) = fixture("offer-expiry-finish");
    original(&store);
    run(&store, T + 1, |tx| {
        offer(tx, "expired", "original", "decision-original", "b", T + 1)
    })
    .unwrap();
    counts(&store, &owner, T + 1, [1, 1, 0]);
    let deadline = T + 1 + MINUTE;
    counts(&store, &owner, deadline, [1, 0, 0]);
    run(&store, deadline, move |tx| {
        tx.use_continuation("decision-expired", deadline)
    })
    .unwrap();
    counts(&store, &owner, deadline, [1, 0, 0]);
    run(&store, deadline + 1, move |tx| {
        offer(
            tx,
            "released",
            "original",
            "decision-original",
            "b",
            deadline + 1,
        )?;
        tx.mark_admission_finished("p", "original", deadline + 1)?;
        tx.finish_continuations("p", "original", "run", deadline + 1)
    })
    .unwrap();
    counts(&store, &owner, deadline + 1, [0, 0, 0]);
    reserve(&store, row("next", deadline + 2), deadline + 2);
    start(&store, "p", "next", "run", deadline + 2);
    run(&store, deadline + 3, move |tx| {
        offer(tx, "used", "next", "decision-next", "c", deadline + 3)?;
        tx.use_continuation("decision-used", deadline + 3)
    })
    .unwrap();
    counts(&store, &owner, deadline + 3, [0, 0, 1]);
}

#[test]
fn observed_offer_use_and_late_attempt_revive_only_the_current_account() {
    let (_scratch, store, owner) = fixture("offer-observed-revival");
    original(&store);
    run(&store, T + 1, |tx| {
        offer(tx, "b", "original", "decision-original", "b", T + 1)?;
        assert!(tx.use_observed_continuation(
            "p",
            "original",
            "run",
            "b",
            Some("credential-b"),
            T + 1
        )?);
        Ok(())
    })
    .unwrap();
    counts(&store, &owner, T + 1, [0, 1, 0]);
    let later = T + 31 * MINUTE;
    reserve(&store, row("a-refill", later), later);
    let mut b = row("b-refill", later);
    b.account_id = "b".into();
    reserve(&store, b, later);
    counts(&store, &owner, later, [1, 1, 0]);
    run(&store, later, move |tx| {
        let row = tx.admission("p", "original")?.unwrap();
        tx.observe_admission(&row, later)
    })
    .unwrap();
    counts(&store, &owner, later, [1, 2, 0]);
    assert_eq!(
        owner
            .published_snapshot()
            .unwrap()
            .inflight
            .entry_count("provider", "a"),
        1
    );
}

#[test]
fn same_millisecond_chain_succeeds_explicit_predecessors_but_not_new_rows() {
    let (_scratch, store, owner) = fixture("offer-same-millisecond");
    original(&store);
    run(&store, T, |tx| {
        offer(tx, "b", "original", "decision-original", "b", T)?;
        tx.use_continuation("decision-b", T)?;
        offer(tx, "c", "original", "decision-b", "c", T)?;
        tx.use_continuation("decision-c", T)
    })
    .unwrap();
    counts(&store, &owner, T, [0, 0, 1]);
    reserve(&store, row("same-time", T), T);
    start(&store, "p", "same-time", "run", T);
    counts(&store, &owner, T, [1, 0, 1]);
    run(&store, T + 1, |tx| {
        let row = tx.admission("p", "original")?.unwrap();
        tx.observe_admission(&row, T + 1)
    })
    .unwrap();
    counts(&store, &owner, T + 1, [1, 0, 1]);
}

#[test]
fn succession_covers_other_admissions_but_finish_reaches_only_its_own_chain() {
    let (_scratch, store, owner) = fixture("offer-two-admissions");
    original(&store);
    reserve(&store, row("other", T), T);
    start(&store, "p", "other", "run", T);
    counts(&store, &owner, T, [1, 0, 0]);
    run(&store, T + 1, |tx| {
        offer(tx, "b", "original", "decision-original", "b", T + 1)
    })
    .unwrap();
    counts(&store, &owner, T + 1, [1, 1, 0]);
    run(&store, T + 2, |tx| tx.use_continuation("decision-b", T + 2)).unwrap();
    counts(&store, &owner, T + 2, [0, 1, 0]);
    for id in ["other", "original"] {
        run(&store, T + 3, move |tx| {
            let row = tx.admission("p", id)?.unwrap();
            tx.observe_admission(&row, T + 3)
        })
        .unwrap();
        counts(&store, &owner, T + 3, [0, 1, 0]);
    }
    run(&store, T + 4, |tx| {
        tx.mark_admission_finished("p", "other", T + 4)?;
        tx.finish_continuations("p", "other", "run", T + 4)
    })
    .unwrap();
    counts(&store, &owner, T + 4, [0, 1, 0]);
    run(&store, T + 5, |tx| {
        tx.mark_admission_finished("p", "original", T + 5)?;
        tx.finish_continuations("p", "original", "run", T + 5)
    })
    .unwrap();
    counts(&store, &owner, T + 5, [0, 0, 0]);
    reserve(&store, row("later", T + 6), T + 6);
    start(&store, "p", "later", "run", T + 6);
    counts(&store, &owner, T + 6, [1, 0, 0]);
}

#[test]
fn renewal_stays_on_departed_account_until_its_own_window_closes() {
    let (_scratch, store, owner) = fixture("offer-renewal");
    original(&store);
    let mut renewal = row("renewal", T);
    renewal.request_mode = "renew".into();
    renewal.conversation_hash = "conversation-original".into();
    renewal.pin_generation = 2;
    reserve(&store, renewal, T);
    start(&store, "p", "renewal", "run", T);
    run(&store, T + 1, |tx| {
        offer(tx, "b", "original", "decision-original", "b", T + 1)?;
        tx.use_continuation("decision-b", T + 1)
    })
    .unwrap();
    counts(&store, &owner, T + 1, [1, 1, 0]);
    run(&store, T + 2, |tx| {
        tx.mark_admission_finished("p", "renewal", T + 2)
    })
    .unwrap();
    counts(&store, &owner, T + 2, [0, 1, 0]);
}

#[test]
fn loader_uses_closed_left_rows_and_never_needs_routing_admission_records() {
    let (scratch, store, owner) = fixture("offer-loader-closed-row");
    original(&store);
    reserve(&store, row("other", T), T);
    start(&store, "p", "other", "run", T);
    run(&store, T + 1, |tx| {
        offer(tx, "b", "original", "decision-original", "b", T + 1)?;
        tx.use_continuation("decision-b", T + 1)?;
        // Close only the original row, leaving its used reservation and the
        // other admission open; the loader still needs its account identity.
        tx.mark_admission_finished("p", "original", T + 1)?;
        Ok(())
    })
    .unwrap();
    let conn = rusqlite::Connection::open(scratch.path().join("state.db")).unwrap();
    conn.execute("DELETE FROM routing_v1_admission", [])
        .unwrap();
    counts(&store, &owner, T + 1, [0, 1, 0]);
    let loaded = loaded(&store, T + 1);
    assert_eq!(loaded.inflight.entry_count("provider", "a"), 0);
    assert!(!loaded
        .admissions
        .open
        .contains_key(&("p".into(), "original".into())));
}

#[test]
fn offer_use_rollback_publishes_neither_succession_nor_started_reservation() {
    let (_scratch, store, owner) = fixture("offer-use-rollback");
    original(&store);
    run(&store, T + 1, |tx| {
        offer(tx, "b", "original", "decision-original", "b", T + 1)
    })
    .unwrap();
    let failed: RouteStateResult<()> = run(&store, T + 2, |tx| {
        tx.use_continuation("decision-b", T + 2)?;
        Err(RouteStateStoreError("abort after use".into()))
    });
    assert!(failed.unwrap_err().0.contains("abort after use"));
    counts(&store, &owner, T + 2, [1, 1, 0]);
    run(&store, T + 3, |tx| tx.use_continuation("decision-b", T + 3)).unwrap();
    counts(&store, &owner, T + 3, [0, 1, 0]);
}

#[derive(Clone)]
struct DurableOffer {
    original: String,
    previous: String,
    decision: String,
    reservation: String,
    used: Option<i64>,
}

fn assert_durable_oracle(
    store: &SqliteRouteStateStore,
    owner: &StateOwner,
    path: &std::path::Path,
    ids: &[String],
    now: i64,
) {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut statement = conn.prepare("SELECT admission_id, previous_decision_id, decision_id, reservation_id, used_at_ms FROM account_continue WHERE principal='p'").unwrap();
    let offers = statement
        .query_map([], |r| {
            Ok(DurableOffer {
                original: r.get(0)?,
                previous: r.get(1)?,
                decision: r.get(2)?,
                reservation: r.get(3)?,
                used: r.get(4)?,
            })
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let rows = store
        .account_transaction(|tx| {
            ids.iter()
                .map(|id| tx.admission("p", id).map(Option::unwrap))
                .collect::<RouteStateResult<Vec<_>>>()
        })
        .unwrap();
    let active = rows
        .iter()
        .filter(|row| {
            !offers
                .iter()
                .filter(|offer| offer.used.is_some())
                .any(|offer| {
                    let left_id = offers
                        .iter()
                        .find(|previous| {
                            previous.original == offer.original
                                && previous.decision == offer.previous
                        })
                        .map(|previous| &previous.reservation)
                        .unwrap_or(&offer.original);
                    let left = rows
                        .iter()
                        .find(|row| &row.admission_id == left_id)
                        .unwrap();
                    row.state == "started"
                        && row.run_id.as_deref() == Some("run")
                        && left.provider == row.provider
                        && left.account_id == row.account_id
                        && (&row.admission_id == left_id
                            || (row.request_mode != "renew"
                                && row.started_at.unwrap() < offer.used.unwrap()))
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    let restarted = loaded(store, now);
    for account in ["a", "b", "c"] {
        for (scope, model) in [
            (InflightScope::Account, None),
            (InflightScope::AccountModel("m1"), Some("m1")),
            (InflightScope::AccountModel("m2"), Some("m2")),
        ] {
            let expected = oracle_count(&active, account, model, now);
            assert_eq!(
                count(owner, account, scope, now),
                expected,
                "live {account} at {now}"
            );
            assert_eq!(
                restarted.inflight.count("provider", account, scope, now),
                expected,
                "restart {account} at {now}"
            );
        }
    }
}

#[test]
fn randomized_offers_uses_expiries_releases_and_renewals_match_durable_oracle() {
    for seed in [17_u64, 93] {
        let (scratch, store, owner) = fixture(&format!("offer-random-{seed}"));
        let mut rng = seed;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut now = T;
        let mut ids = Vec::new();
        let mut seen = BTreeSet::new();
        for step in 0..48 {
            now += [0, 1, MINUTE, 31 * MINUTE][next() as usize % 4];
            let id = format!("original-{step}");
            let mut admission = row(&id, now);
            admission.model = if next() % 2 == 0 { "m1" } else { "m2" }.into();
            reserve(&store, admission, now);
            start(&store, "p", &id, "run", now);
            ids.push(id.clone());
            let renewal_id = format!("renew-{step}");
            let mut renewal = row(&renewal_id, now);
            renewal.request_mode = "renew".into();
            renewal.conversation_hash = format!("conversation-{id}");
            renewal.pin_generation = 2;
            reserve(&store, renewal, now);
            start(&store, "p", &renewal_id, "run", now);
            ids.push(renewal_id);
            let target = if next() % 2 == 0 { "b" } else { "c" };
            let offer_id = format!("offer-{step}");
            let previous = format!("decision-{id}");
            let mode = (next() % 4) as usize;
            seen.insert(mode);
            let original = id.clone();
            let reservation = offer_id.clone();
            run(&store, now, move |tx| {
                offer(tx, &reservation, &original, &previous, target, now)?;
                match mode {
                    0 | 1 => tx.use_continuation(&format!("decision-{reservation}"), now)?,
                    2 => tx.use_continuation(&format!("decision-{reservation}"), now + MINUTE)?,
                    _ => {
                        tx.mark_admission_finished("p", &original, now)?;
                        tx.finish_continuations("p", &original, "run", now)?;
                    }
                }
                Ok(())
            })
            .unwrap();
            ids.push(offer_id);
            if mode == 2 {
                now += MINUTE;
            }
            if mode == 1 {
                let original = id.clone();
                run(&store, now, move |tx| {
                    let row = tx.admission("p", &original)?.unwrap();
                    tx.observe_admission(&row, now)
                })
                .unwrap();
            }
            let conn = rusqlite::Connection::open(scratch.path().join("state.db")).unwrap();
            conn.execute("DELETE FROM routing_v1_admission", [])
                .unwrap();
            assert_durable_oracle(&store, &owner, &scratch.path().join("state.db"), &ids, now);
        }
        assert_eq!(seen.len(), 4);
    }
}

#[test]
fn generated_late_uses_after_closed_originals_match_each_fresh_load() {
    let (scratch, store, owner) = fixture("offer-synthetic-late-uses");
    let mut ids = Vec::new();
    let mut rng = 317_u64;
    for step in 0..24 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let now = T + step * 100;
        let id = format!("closed-{step}");
        reserve(&store, row(&id, now), now);
        start(&store, "p", &id, "run", now);
        ids.push(id.clone());
        let other = format!("still-started-{step}");
        reserve(&store, row(&other, now), now);
        start(&store, "p", &other, "run", now);
        ids.push(other.clone());
        let reservation = format!("late-{step}");
        let original = id.clone();
        let offered = reservation.clone();
        let make_after_close = rng % 2 == 0;
        run(&store, now + 1, move |tx| {
            // Low-level writers can construct sequences the handler rejects:
            // real run_finished also releases its unused offers.
            if make_after_close {
                tx.mark_admission_finished("p", &original, now + 1)?;
            }
            offer(
                tx,
                &offered,
                &original,
                &format!("decision-{original}"),
                "b",
                now + 1,
            )?;
            if !make_after_close {
                tx.mark_admission_finished("p", &original, now + 1)?;
            }
            Ok(())
        })
        .unwrap();
        ids.push(reservation.clone());
        assert_durable_oracle(
            &store,
            &owner,
            &scratch.path().join("state.db"),
            &ids,
            now + 1,
        );
        run(&store, now + 2, move |tx| {
            tx.use_continuation(&format!("decision-{reservation}"), now + 2)
        })
        .unwrap();
        assert_durable_oracle(
            &store,
            &owner,
            &scratch.path().join("state.db"),
            &ids,
            now + 2,
        );
        run(&store, now + 3, move |tx| {
            let row = tx.admission("p", &other)?.unwrap();
            tx.observe_admission(&row, now + 3)
        })
        .unwrap();
        assert_durable_oracle(
            &store,
            &owner,
            &scratch.path().join("state.db"),
            &ids,
            now + 3,
        );
    }
    assert!(
        continuation_source_reads() > 0,
        "closed synthetic sources must use the counted fallback"
    );
}

#[test]
fn migration_keeps_recent_moved_attempt_on_destination_not_original_account() {
    for bound in [true, false] {
        let scratch = cortexkit_test_support::ScratchDir::new("offer-migration");
        let descriptor = striatum_store::dev_descriptor(
            "offer-migration",
            scratch.path().join("state.db").to_string_lossy(),
        );
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 88).unwrap();
        old.with_writer_for_test(|conn| {
            for (id, account) in [("original", "a"), ("destination", "b")] {
                conn.execute("INSERT INTO account_pin (pin_id, principal, conversation_hash, provider, pin_generation, affinity_model, account_id, credential_id, pin_mode, decision_id, created_at)
                    VALUES (?1, 'p', ?1, 'provider', 1, 'm1', ?2, 'credential', 'run_frozen', 'd', ?3)", rusqlite::params![id, account, T - 120 * MINUTE])?;
                conn.execute("INSERT INTO account_admission (principal, admission_id, request_fingerprint, request_mode, decision_id, conversation_hash, provider, model, account_id, credential_id, pin_id, pin_generation, state, charged, request_bytes, request_bytes_estimated, committed_at, admission_valid_until, lease_valid_until, reply_json, run_id, started_at)
                    VALUES ('p', ?1, 'fp', 'admit', 'd', ?1, 'provider', 'm1', ?2, 'credential', ?1, 1, 'started', 1, 1, 0, ?3, ?3+60000, ?3+120000, '{}', 'run', ?3)", rusqlite::params![id, account, T - 120 * MINUTE])?;
            }
            conn.execute("INSERT INTO account_continue (principal, selection_id, request_json, answer_json, run_id, admission_id, previous_decision_id, decision_id, reservation_id, valid_until_ms, used_at_ms)
                VALUES ('p', 'move', '{}', '{}', 'run', 'original', 'd', 'moved', 'destination', ?1, ?2)", rusqlite::params![T - 119 * MINUTE, T - 120 * MINUTE])?;
            conn.execute("INSERT INTO account_observation (observation_id, principal, runner, event_id, kind, admission_id, bound, observation_json, effects_json, received_at)
                VALUES ('attempt', 'p', 'runner', 'attempt', 'attempt', 'original', ?1, '{}', ?2, ?3)", rusqlite::params![bound, if bound { "[]" } else { r#"[{"effect":"served_account_mismatch"}]"# }, T - MINUTE])?;
            Ok(())
        }).unwrap();
        drop(old);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let owner = install(&store, T);
        counts(&store, &owner, T, [0, 1, 0]);
        counts(&store, &owner, T + 29 * MINUTE, [0, 0, 0]);
    }
}

/// An offer that lapses before its reservation's own deadline stops counting
/// toward concurrency at the offer's deadline: through the readers' view
/// before any command, and in the stored state after the Tick releases it,
/// stamped at that deadline.
#[test]
fn a_lapsed_offer_stops_counting_at_its_deadline_before_and_after_any_command() {
    let (_scratch, store, _owner) = fixture("offer-lapse-view");
    original(&store);
    run(&store, T + 1, |tx| {
        let mut reservation = row("lapsing", T + 1);
        reservation.account_id = "b".into();
        reservation.credential_id = "credential-b".into();
        // The reservation outlives its offer, so only the offer's lapse can
        // free its slot.
        reservation.admission_valid_until = T + 1 + 10 * MINUTE;
        insert(tx, &reservation)?;
        tx.record_continuation(
            "p",
            "lapsing",
            &V1Recorded {
                body_json: "{}".into(),
                answer_json: "{}".into(),
            },
            "run",
            "original",
            "decision-original",
            Some((&reservation.decision_id, "lapsing", T + 1 + MINUTE)),
        )
    })
    .unwrap();
    let count_b = |state: &AccountState, now: i64| {
        state.inflight.count("provider", "b", InflightScope::Account, now)
    };
    let deadline = T + 1 + MINUTE;
    let before = published_at(&store, deadline - 1).unwrap();
    assert_eq!(count_b(&before, deadline - 1), 1, "the live offer holds its slot");
    let commits = store.writer_commit_count_for_test();
    let read = published_at(&store, deadline).unwrap();
    assert_eq!(count_b(&read, deadline), 0, "lapsed offer, before any command");
    assert_eq!(
        read.admissions.open_admission("p", "lapsing").map(|open| open.row.state.clone()),
        None,
        "the view releases the reservation"
    );
    assert_eq!(store.writer_commit_count_for_test(), commits, "reading sends no command");
    crate::account_writer::tick(&store, deadline + 30_000).unwrap();
    let stored = snapshot(&store).unwrap();
    assert_eq!(count_b(&stored, deadline + 30_000), 0, "after the Tick releases it");
    let row = store
        .account_read(|tx| tx.admission("p", "lapsing"))
        .unwrap()
        .unwrap();
    assert_eq!((row.state.as_str(), row.released_at), ("released", Some(deadline)));
    assert_eq!(
        count_b(&loaded(&store, deadline + 30_000), deadline + 30_000),
        0,
        "after a restart"
    );
}
