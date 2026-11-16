//! Load-equivalence of the in-memory admission state: after any generated
//! sequence of writer commands, pruning included, the published state must
//! equal a fresh load of the same SQL rows.
//!
//! Requests now read the state instead of SQL, and the continuation
//! transitions are decided on the state and persisted by key. So the
//! generated sequence also runs, command for command, against an oracle store
//! that has no state and takes the original SQL paths: both must reply alike,
//! hold the same rows, and every switched read of the state must answer what
//! the original SQL query answers on the oracle.

use super::*;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use striatum_store::{AccountCommandKind, AccountPinRow, LatestAdmissionRow};

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

fn published(owner: &StateOwner) -> AdmissionState {
    owner.published_snapshot().unwrap().admissions.clone()
}

fn loaded(store: &SqliteRouteStateStore) -> AdmissionState {
    store
        .account_transaction(AccountState::load)
        .unwrap()
        .admissions
}

fn assert_equivalent(store: &SqliteRouteStateStore, owner: &StateOwner, context: &str) {
    let memory = published(owner);
    assert!(memory.closing.is_empty(), "{context}: unsettled chains");
    assert_eq!(loaded(store), memory, "{context}");
}

fn run<T: Send + 'static>(
    store: &SqliteRouteStateStore,
    command: impl FnMut(&mut AccountCommand<'_>) -> RouteStateResult<T> + Send + 'static,
) -> RouteStateResult<T> {
    crate::account_writer::execute(store, AccountCommandKind::Repair, command)
}

/// The oracle runs a command as one SQL transaction with no state, which is
/// how every `AccountCommand` method behaved before reads moved to memory.
fn run_oracle<T>(
    oracle: &SqliteRouteStateStore,
    command: impl FnOnce(&mut AccountCommand<'_>) -> RouteStateResult<T>,
) -> RouteStateResult<T> {
    oracle.account_transaction(|tx| command(&mut AccountCommand::new(tx, None)))
}

/// Run one command on the oracle and through the writer; both must reply alike.
fn both<T: Send + PartialEq + std::fmt::Debug + 'static>(
    store: &SqliteRouteStateStore,
    oracle: &SqliteRouteStateStore,
    command: impl Fn(&mut AccountCommand<'_>) -> RouteStateResult<T> + Clone + Send + 'static,
) -> RouteStateResult<T> {
    let expected = run_oracle(oracle, command.clone());
    let actual = run(store, command);
    match (&expected, &actual) {
        (Ok(expected), Ok(actual)) => assert_eq!(actual, expected, "command replies differ"),
        (Err(_), Err(_)) => {}
        _ => panic!("command outcome differs: oracle {expected:?}, state {actual:?}"),
    }
    actual
}

/// Every row of the admission group's tables, in rowid order.
fn group_rows(store: &SqliteRouteStateStore) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
    store
        .with_reader_for_test(|conn| {
            [
                "account_admission",
                "account_affinity",
                "account_continue",
                "account_pin",
            ]
            .into_iter()
            .map(|table| {
                let mut statement =
                    conn.prepare(&format!("SELECT rowid, * FROM {table} ORDER BY rowid"))?;
                let width = statement.column_count();
                let rows = statement
                    .query_map([], |row| (0..width).map(|i| row.get(i)).collect())?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok((table.to_owned(), rows))
            })
            .collect()
        })
        .unwrap()
}

/// Each read a request now answers from the state must equal the SQL query it
/// replaced, run on the oracle's rows.
fn assert_same_answers(
    state: &AdmissionState,
    oracle: &SqliteRouteStateStore,
    world: &World,
    now: i64,
    context: &str,
) {
    const OPEN: [&str; 3] = ["reserved", "started", "expired"];
    oracle
        .account_transaction(|tx| {
            for account in ["a0", "a1"] {
                for floor in [None, Some(now - 5), Some(now - 20)] {
                    assert_eq!(
                        state.demand(PROVIDER, account, floor),
                        tx.demand(PROVIDER, account, floor)?,
                        "{context}: demand of {account} after {floor:?}"
                    );
                }
            }
            for principal in ["p0", "p1"] {
                for hash in ["h0", "h1", "h2"] {
                    let affinity = tx.affinity(principal, hash, PROVIDER)?;
                    assert_eq!(
                        state.affinity(principal, hash, PROVIDER),
                        affinity.as_ref(),
                        "{context}: affinity {principal}/{hash}"
                    );
                    if let Some(affinity) = &affinity {
                        assert_eq!(
                            state.pin_has_started(affinity),
                            tx.pin_has_started(affinity)?,
                            "{context}: pin_has_started {principal}/{hash}"
                        );
                    }
                    for at in [now - 10, now, now + 10, now + 50] {
                        assert_eq!(
                            state.parent_used_account(principal, hash, PROVIDER, at),
                            tx.parent_used_account(principal, hash, PROVIDER, at)?,
                            "{context}: parent_used_account {principal}/{hash} at {at}"
                        );
                    }
                }
            }
            for at in [now, now + 10, now + 30] {
                assert_eq!(
                    state.has_expired_offers(at),
                    tx.has_expired_continuations(at)?,
                    "{context}: has_expired_continuations at {at}"
                );
            }
            for admission in &world.admissions {
                let (principal, id) = (&admission.principal, &admission.id);
                let open = tx
                    .admission(principal, id)?
                    .filter(|row| OPEN.contains(&row.state.as_str()));
                let memory = state.open_admission(principal, id);
                assert_eq!(
                    memory.map(|open| &open.row),
                    open.as_ref(),
                    "{context}: admission {id}"
                );
                if open.is_some() {
                    assert_eq!(
                        memory.and_then(|open| open.input_size_class),
                        tx.admission_size_class(principal, id)?,
                        "{context}: admission_size_class {id}"
                    );
                }
            }
            // Chains are held for open originals only; a request reads them
            // only after finding its original open.
            for (principal, original, run) in &world.runs {
                if state.open_admission(principal, original).is_none() {
                    continue;
                }
                for decision in world.decisions.iter().map(String::as_str).chain(["root"]) {
                    assert_eq!(
                        state.continued_reservation(principal, original, run, decision),
                        tx.continued_reservation(principal, original, run, decision)?,
                        "{context}: continued_reservation {original}/{run}/{decision}"
                    );
                    assert_eq!(
                        state.has_used_successor(principal, original, run, decision),
                        tx.continuation_has_used_successor(principal, original, run, decision)?,
                        "{context}: continuation_has_used_successor {original}/{run}/{decision}"
                    );
                }
                let reservation = match state.latest_used_reservation(principal, original) {
                    Some(id) => tx.admission(principal, &id)?,
                    None => None,
                };
                assert_eq!(
                    reservation,
                    tx.latest_used_continuation(principal, original)?,
                    "{context}: latest_used_continuation {original}"
                );
            }
            Ok(())
        })
        .unwrap();
}

const PROVIDER: &str = "prov";

#[derive(Clone)]
struct Admission {
    principal: String,
    id: String,
    hash: String,
    account: String,
    credential: String,
    pin: String,
    generation: u64,
    charged: bool,
    bytes: u64,
    committed_at: i64,
    valid_for: i64,
    lease_for: i64,
}

impl Admission {
    fn row(&self) -> AccountAdmissionRow {
        AccountAdmissionRow {
            principal: self.principal.clone(),
            admission_id: self.id.clone(),
            request_fingerprint: "fp".into(),
            request_mode: "admit".into(),
            decision_id: format!("decision-{}", self.id),
            conversation_hash: self.hash.clone(),
            provider: PROVIDER.into(),
            model: "model".into(),
            account_id: self.account.clone(),
            credential_id: self.credential.clone(),
            pin_id: self.pin.clone(),
            pin_generation: self.generation,
            state: "reserved".into(),
            charged: self.charged,
            request_bytes: if self.charged { self.bytes } else { 0 },
            request_bytes_estimated: false,
            committed_at: self.committed_at,
            admission_valid_until: self.committed_at + self.valid_for,
            lease_valid_until: self.committed_at + self.lease_for,
            reply_json: "{}".into(),
            run_id: None,
            started_at: None,
            started_late: false,
            released_at: None,
            finished_at: None,
            last_observed_at: None,
        }
    }

    fn pin_row(&self) -> AccountPinRow {
        AccountPinRow {
            pin_id: self.pin.clone(),
            principal: self.principal.clone(),
            conversation_hash: self.hash.clone(),
            provider: PROVIDER.into(),
            pin_generation: self.generation,
            affinity_model: "model".into(),
            account_id: self.account.clone(),
            credential_id: self.credential.clone(),
            pin_mode: "conversation_lease".into(),
            previous_pin_id: None,
            decision_id: format!("decision-{}", self.id),
            created_at: self.committed_at,
        }
    }

    fn affinity(&self, now: i64) -> AccountAffinityRow {
        AccountAffinityRow {
            principal: self.principal.clone(),
            conversation_hash: self.hash.clone(),
            provider: PROVIDER.into(),
            affinity_model: "model".into(),
            pin_id: self.pin.clone(),
            pin_generation: self.generation,
            account_id: self.account.clone(),
            credential_id: self.credential.clone(),
            credential_binding_revision: 1,
            policy_version: 1,
            initial_demand_bytes: self.bytes,
            created_at: now,
            last_used_at: now,
            expires_at: now + self.lease_for,
            lease_valid_until: Some(now + self.lease_for),
        }
    }
}

/// Insert a pinned admission and, when it is the conversation's first or a
/// reassignment, install its pin as the affinity. Pins are always fresh, as
/// serving issues them.
fn admit(
    tx: &mut AccountCommand<'_>,
    admission: &Admission,
    install: bool,
) -> RouteStateResult<()> {
    let current = tx.affinity(&admission.principal, &admission.hash, PROVIDER)?;
    tx.insert_pin(&admission.pin_row())?;
    tx.insert_admission(&admission.row())?;
    tx.record_admission_probe_scope(
        &admission.principal,
        &admission.id,
        admission.bytes as u32 % 7,
        Some(&format!("model-decision-{}", admission.id)),
    )?;
    if install {
        let expected = current
            .as_ref()
            .map(|row| (row.pin_id.as_str(), row.pin_generation));
        tx.put_affinity(&admission.affinity(admission.committed_at), expected)?;
    }
    Ok(())
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        (!items.is_empty()).then(|| &items[self.below(items.len() as u64) as usize])
    }
}

const OPERATIONS: usize = 20;
const OPERATION_NAMES: [&str; OPERATIONS] = [
    "admit",
    "reassign_or_stale_affinity",
    "touch_affinity",
    "end_lease",
    "expire",
    "start",
    "release",
    "finish_with_continuations",
    "offer_continuation",
    "use_continuation",
    "use_observed_continuation",
    "release_expired_continuations",
    "tick",
    "prune_admissions",
    "prune_affinities",
    "prune_continuations",
    "rolled_back_command",
    "reload",
    "admit_without_affinity",
    "prune_pins",
];

/// What the generator has created so far, for choosing targets.
#[derive(Default)]
struct World {
    admissions: Vec<Admission>,
    runs: Vec<(String, String, String)>,
    decisions: Vec<String>,
    serial: u64,
    pruned_affinities: usize,
}

impl World {
    fn fresh(&mut self, prefix: &str) -> String {
        self.serial += 1;
        format!("{prefix}-{}", self.serial)
    }

    fn new_admission(&mut self, rng: &mut Rng, now: i64) -> Admission {
        let principal = format!("p{}", rng.below(2));
        let hash = format!("h{}", rng.below(3));
        let account = format!("a{}", rng.below(2));
        // Pin generations are unique per conversation; a global counter keeps
        // them unique even when a caller moves the admission to another one.
        self.serial += 1;
        let generation = self.serial;
        Admission {
            principal,
            id: self.fresh("adm"),
            hash,
            credential: format!("{account}-key"),
            account,
            pin: self.fresh("pin"),
            generation,
            charged: rng.below(4) != 0,
            bytes: 1 + rng.below(1000),
            // Equal and out-of-order commit times exercise the rowid tie-break.
            committed_at: now - rng.below(3) as i64,
            valid_for: 1 + rng.below(40) as i64,
            lease_for: 1 + rng.below(80) as i64,
        }
    }
}

/// Apply one generated operation through the writer.
fn apply(
    store: &Arc<SqliteRouteStateStore>,
    oracle: &SqliteRouteStateStore,
    owner: &StateOwner,
    world: &mut World,
    rng: &mut Rng,
    op: usize,
    now: i64,
) {
    let any = |rng: &mut Rng, world: &World| rng.pick(&world.admissions).cloned();
    let result = match op {
        0 | 18 => {
            let admission = world.new_admission(rng, now);
            world.admissions.push(admission.clone());
            let install = op == 0;
            both(store, oracle, move |tx| admit(tx, &admission, install))
        }
        1 => {
            let Some(target) = any(rng, world) else {
                return;
            };
            let stale = rng.below(3) == 0;
            let mut fresh = world.new_admission(rng, now);
            fresh.principal = target.principal.clone();
            fresh.hash = target.hash.clone();
            both(store, oracle, move |tx| {
                tx.insert_pin(&fresh.pin_row())?;
                let current = tx.affinity(&fresh.principal, &fresh.hash, PROVIDER)?;
                let expected = if stale {
                    Some((target.pin.clone(), target.generation + 100))
                } else {
                    current.map(|row| (row.pin_id, row.pin_generation))
                };
                tx.put_affinity(
                    &fresh.affinity(now),
                    expected
                        .as_ref()
                        .map(|(pin, generation)| (pin.as_str(), *generation)),
                )?;
                Ok(())
            })
        }
        2 => {
            let Some(target) = any(rng, world) else {
                return;
            };
            let lease = (rng.below(2) == 0).then_some(now + rng.below(50) as i64);
            both(store, oracle, move |tx| {
                tx.touch_affinity(
                    &target.principal,
                    &target.hash,
                    PROVIDER,
                    (&target.pin, target.generation),
                    now,
                    now + 30,
                    lease,
                )
                .map(|_| ())
            })
        }
        3 => {
            let Some(target) = any(rng, world) else {
                return;
            };
            let at = now - rng.below(20) as i64;
            both(store, oracle, move |tx| {
                tx.end_lease(&target.pin, at).map(|_| ())
            })
        }
        4 => {
            let Some(target) = any(rng, world) else {
                return;
            };
            both(store, oracle, move |tx| {
                tx.mark_admission_expired(&target.principal, &target.id)
                    .map(|_| ())
            })
        }
        5 => {
            let Some(target) = any(rng, world) else {
                return;
            };
            let run_id = world.fresh("run");
            let late = rng.below(2) == 0;
            let started = (target.principal.clone(), target.id.clone(), run_id.clone());
            let result = both(store, oracle, move |tx| {
                tx.mark_admission_started(&target.principal, &target.id, &run_id, now, late)
            });
            if matches!(result, Ok(true)) {
                world.runs.push(started);
            }
            result.map(|_| ())
        }
        6 => {
            let Some(target) = any(rng, world) else {
                return;
            };
            both(store, oracle, move |tx| {
                tx.mark_admission_released(&target.principal, &target.id, now)
                    .map(|_| ())
            })
        }
        7 => {
            // As observation does: finishing an original finishes its run's
            // continuations in the same command.
            let Some((principal, id, run_id)) = rng.pick(&world.runs).cloned() else {
                return;
            };
            both(store, oracle, move |tx| {
                if tx.mark_admission_finished(&principal, &id, now)? {
                    tx.finish_continuations(&principal, &id, &run_id, now)?;
                }
                Ok(())
            })
        }
        8 => {
            // Most offers continue a started run; some name an original that
            // never started, so retention can reach its chain.
            let (principal, original, run_id) = if rng.below(4) != 0 {
                let Some(started) = rng.pick(&world.runs).cloned() else {
                    return;
                };
                started
            } else {
                let Some(target) = any(rng, world) else {
                    return;
                };
                (target.principal, target.id, world.fresh("run"))
            };
            let mut reservation = world.new_admission(rng, now);
            reservation.principal = principal.clone();
            // As serving does, a reservation can become its conversation's
            // affinity pin (`install`); releasing or finishing the reservation
            // then ends that affinity row's lease, a write the state decides.
            let install = rng.below(2) == 0;
            world.admissions.push(reservation.clone());
            let previous = if rng.below(2) == 0 {
                rng.pick(&world.decisions)
                    .cloned()
                    .unwrap_or_else(|| "root".into())
            } else {
                "root".into()
            };
            let decision = world.fresh("decision");
            world.decisions.push(decision.clone());
            let selection = world.fresh("selection");
            let valid_until = now + rng.below(30) as i64;
            both(store, oracle, move |tx| {
                admit(tx, &reservation, install)?;
                tx.record_continuation(
                    &principal,
                    &selection,
                    &V1Recorded {
                        body_json: "{}".into(),
                        answer_json: "{}".into(),
                    },
                    &run_id,
                    &original,
                    &previous,
                    Some((&decision, &reservation.id, valid_until)),
                )
            })
        }
        9 => {
            let Some(decision) = rng.pick(&world.decisions).cloned() else {
                return;
            };
            both(store, oracle, move |tx| tx.use_continuation(&decision, now))
        }
        10 => {
            let Some((principal, id, run_id)) = rng.pick(&world.runs).cloned() else {
                return;
            };
            let account = format!("a{}", rng.below(2));
            let credential = (rng.below(2) == 0).then(|| format!("{account}-key"));
            both(store, oracle, move |tx| {
                tx.use_observed_continuation(
                    &principal,
                    &id,
                    &run_id,
                    &account,
                    credential.as_deref(),
                    now,
                )
                .map(|_| ())
            })
        }
        11 => both(store, oracle, move |tx| {
            tx.release_expired_continuations(now)
        }),
        12 => {
            run_oracle(oracle, |tx| tx.tick(now)).unwrap();
            crate::account_writer::tick(store, now)
        }
        13 => {
            let (before, limit) = (now - rng.below(60) as i64, 1 + rng.below(6) as usize);
            let expected = run_oracle(oracle, |tx| {
                tx.prune_account_admissions(before, limit, now + 30 * 60_000)
            });
            crate::account_writer::prune_account_admissions(store, before, limit, now + 30 * 60_000)
                .map(|pruned| assert_eq!(pruned, expected.unwrap()))
        }
        14 => {
            let (before, limit) = (now - rng.below(60) as i64, 1 + rng.below(6) as usize);
            let keys = store
                .plan_account_affinities(before, now, None, limit)
                .unwrap()
                .keys;
            let expected = run_oracle(oracle, |tx| tx.prune_account_affinities(&keys, before, now));
            crate::account_writer::prune_account_affinities(store, keys, before, now).map(
                |pruned| {
                    world.pruned_affinities += pruned.affinities;
                    assert_eq!(pruned, expected.unwrap())
                },
            )
        }
        15 => {
            let (before, limit) = (now - rng.below(60) as i64, 1 + rng.below(6) as usize);
            let expected = run_oracle(oracle, |tx| tx.prune_account_continuations(before, limit));
            crate::account_writer::prune_account_continuations(store, before, limit)
                .map(|pruned| assert_eq!(pruned, expected.unwrap()))
        }
        16 => {
            let admission = world.new_admission(rng, now);
            let failed = both(store, oracle, move |tx| {
                admit(tx, &admission, true)?;
                tx.release_expired_continuations(now)?;
                Err::<(), _>(RouteStateStoreError("generated rollback".into()))
            });
            assert!(failed.is_err());
            Ok(())
        }
        17 => {
            run_oracle(oracle, |tx| tx.tick(now)).unwrap();
            owner.reload.store(true, Ordering::Release);
            crate::account_writer::tick(store, now)
        }
        19 => {
            let (before, limit) = (now - rng.below(60) as i64, 1 + rng.below(6) as usize);
            let expected = run_oracle(oracle, |tx| tx.prune_account_pins(before, limit));
            crate::account_writer::prune_account_pins(store, before, limit)
                .map(|pruned| assert_eq!(pruned, expected.unwrap()))
        }
        _ => unreachable!(),
    };
    result.unwrap_or_else(|error| panic!("{}: {error}", OPERATION_NAMES[op]));
}

fn generated(seed: u64, steps: usize) -> Vec<(usize, i64)> {
    let mut rng = Rng(seed);
    let mut now = 1_000;
    (0..steps)
        .map(|_| {
            now += rng.below(8) as i64;
            (rng.below(OPERATIONS as u64) as usize, now)
        })
        .collect()
}

#[test]
fn pin_retention_agrees_with_a_fresh_state_load() {
    let (_scratch, store) = fixture("pin-retention-load");
    let owner = install(&store);
    let mut world = World::default();
    let mut rng = Rng(13);
    let current = world.new_admission(&mut rng, 10);
    run(&store, move |tx| admit(tx, &current, true)).unwrap();
    for at in [1, 2] {
        let mut orphan = world.new_admission(&mut rng, at);
        orphan.hash = "orphan".into();
        run(&store, move |tx| tx.insert_pin(&orphan.pin_row())).unwrap();
    }
    assert_eq!(
        crate::account_writer::prune_account_pins(&store, 100, 256).unwrap(),
        2
    );
    assert_equivalent(&store, &owner, "after deleting unneeded pins");
    owner.reload.store(true, Ordering::Release);
    crate::account_writer::tick(&store, 10).unwrap();
    assert_equivalent(&store, &owner, "after restart load");
}

#[test]
fn expired_affinity_retention_agrees_with_a_fresh_state_load() {
    let (_scratch, store) = fixture("expired-affinity-retention-load");
    let owner = install(&store);
    let mut world = World::default();
    let mut rng = Rng(17);
    let expired = world.new_admission(&mut rng, 10);
    run(&store, move |tx| {
        tx.insert_pin(&expired.pin_row())?;
        tx.put_affinity(&expired.affinity(10), None)?;
        Ok(())
    })
    .unwrap();
    let report = crate::retention::run_retention_pass(
        &store,
        &crate::retention::RetentionWindows::default(),
        4_000_000,
        &crate::retention::PassBudget {
            pause: std::time::Duration::ZERO,
            ..Default::default()
        },
    );
    assert_eq!(report.account_affinities, 1);
    assert_eq!(report.account_pins, 1);
    assert_equivalent(&store, &owner, "after deleting an expired affinity");
    owner.reload.store(true, Ordering::Release);
    crate::account_writer::tick(&store, 4_000_000).unwrap();
    assert_equivalent(&store, &owner, "after restart load");
}

/// Live-shaped retention measurement, in separate processes for independent
/// resident-memory deltas. Of 165,000 expired affinities, retained admissions
/// protect 109,931, matching the live store's roughly 55,000 prunable rows.
fn measure_affinity_retention_load(prune: bool) {
    const NOW: i64 = 1_800_000_000_000;
    const DAY: i64 = 86_400_000;
    let (scratch, store) = fixture(if prune {
        "retention-load-after"
    } else {
        "retention-load-before"
    });
    store.with_writer_for_test(|conn| conn.execute_batch(&format!(
        "BEGIN;
         WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<279999)
         INSERT INTO account_decision (decision_id,mode,trigger,principal,provider,model,family,outcome,code,policy_version,decision_json,created_at)
         SELECT 'd'||i,'serve','request','p','prov','model','model','admit','cold_scored',1,'{{}}',
           CASE WHEN i<55000 THEN {NOW}-31*{DAY}+i ELSE {NOW}-{DAY}+i END FROM n;
         WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<191709)
         INSERT INTO account_pin (pin_id,principal,conversation_hash,provider,pin_generation,affinity_model,account_id,credential_id,pin_mode,decision_id,created_at)
         SELECT 'pin'||i,'p','h'||i,'prov',1,'model','a'||(i%40),'cred'||(i%40),'conversation_lease','d'||i,
           CASE WHEN i<55000 THEN {NOW}-31*{DAY}+i ELSE {NOW}-8*{DAY}+i END FROM n;
         INSERT INTO account_affinity (principal,conversation_hash,provider,affinity_model,pin_id,pin_generation,account_id,credential_id,credential_binding_revision,policy_version,initial_demand_bytes,created_at,last_used_at,expires_at,lease_valid_until)
         SELECT principal,conversation_hash,provider,affinity_model,pin_id,pin_generation,account_id,credential_id,1,1,10,created_at,created_at,
           CASE WHEN rowid<=165000 THEN {NOW}-7200000-165000+rowid ELSE {NOW}+{DAY} END,{NOW}-1 FROM account_pin;
         INSERT INTO account_admission (principal,admission_id,request_fingerprint,request_mode,
           decision_id,conversation_hash,provider,model,account_id,credential_id,pin_id,pin_generation,
           state,charged,request_bytes,request_bytes_estimated,committed_at,admission_valid_until,
           lease_valid_until,reply_json,started_at,finished_at)
         SELECT principal,'adm'||rowid,'fp','admit',decision_id,conversation_hash,provider,'model',
           account_id,credential_id,pin_id,pin_generation,'finished',1,100,0,{NOW}-8*{DAY},
           {NOW}-8*{DAY}+60000,{NOW}-8*{DAY}+120000,'{{}}',{NOW}-8*{DAY},{NOW}-8*{DAY}+1000
         FROM account_pin WHERE rowid>55069 AND rowid<=165000;
         COMMIT;"
    ))).unwrap();
    if prune {
        let (mut total, mut after, mut passes) = (0, None, 0);
        let (mut max_batch, mut max_plan) = (std::time::Duration::ZERO, std::time::Duration::ZERO);
        let start = std::time::Instant::now();
        loop {
            let plan_start = std::time::Instant::now();
            let plan = store
                .plan_account_affinities(NOW - 3_600_000, NOW, after, 4096)
                .unwrap();
            let plan_time = plan_start.elapsed();
            max_plan = max_plan.max(plan_time);
            passes += 1;
            println!(
                "affinity plan pass={passes}: examined={} candidates={} wall_ms={}",
                plan.examined,
                plan.keys.len(),
                plan_time.as_secs_f64() * 1000.0
            );
            for keys in plan
                .keys
                .chunks(crate::retention::AFFINITY_DELETE_BATCH_ROWS)
            {
                let keys = keys.to_vec();
                let batch_start = std::time::Instant::now();
                let batch = store
                    .account_command(AccountCommandKind::Maintenance, move |tx| {
                        tx.delete_account_affinities_returning(&keys, NOW - 3_600_000, NOW)
                            .map(|(batch, _)| batch)
                    })
                    .unwrap();
                max_batch = max_batch.max(batch_start.elapsed());
                total += batch.affinities;
            }
            after = plan.last.or(after);
            if plan.examined < 4096 {
                break;
            }
        }
        assert_eq!(total, 55_069);
        println!("affinity backlog: deleted={total} passes={passes} wall_ms={} max_batch_ms={} max_plan_ms={}", start.elapsed().as_secs_f64()*1000.0, max_batch.as_secs_f64()*1000.0, max_plan.as_secs_f64()*1000.0);
    }
    let start = std::time::Instant::now();
    let mut deleted = 0;
    let mut batches = 0;
    let mut max_batch = std::time::Duration::ZERO;
    for _ in 0..16 {
        let batch_start = std::time::Instant::now();
        let n = store.prune_serve_decisions(NOW - 30 * DAY, 256).unwrap();
        max_batch = max_batch.max(batch_start.elapsed());
        deleted += n;
        batches += 1;
        if n < 256 {
            break;
        }
    }
    println!("decision pass prune={prune}: deleted={deleted} batches={batches} wall_ms={} max_batch_ms={}", start.elapsed().as_secs_f64()*1000.0, max_batch.as_secs_f64()*1000.0);
    drop(store);
    let store = SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
        "retention-load",
        scratch.path().join("state.db").to_string_lossy(),
    ))
    .unwrap();
    let resident = || -> u64 {
        std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
            .unwrap_or(0)
            * 4096
    };
    let before = resident();
    let start = std::time::Instant::now();
    let state = store.account_transaction(AccountState::load).unwrap();
    let elapsed = start.elapsed();
    let after = resident();
    assert_eq!(
        state.admissions.affinity.len(),
        if prune { 136_641 } else { 191_710 }
    );
    println!("account-state load prune={prune}: affinities={} wall_ms={} rss_before_bytes={before} rss_after_bytes={after} rss_delta_bytes={}",
        state.admissions.affinity.len(), elapsed.as_secs_f64()*1000.0, after.saturating_sub(before));
    drop(std::hint::black_box(state));
    if !prune {
        let owner = install(&store);
        let plan_start = std::time::Instant::now();
        let plan = store
            .plan_account_affinities(NOW - 3_600_000, NOW, None, 4096)
            .unwrap();
        let plan_time = plan_start.elapsed();
        let mut max_command = std::time::Duration::ZERO;
        for keys in plan
            .keys
            .chunks(crate::retention::AFFINITY_DELETE_BATCH_ROWS)
        {
            let start = std::time::Instant::now();
            crate::account_writer::prune_account_affinities(
                &store,
                keys.to_vec(),
                NOW - 3_600_000,
                NOW,
            )
            .unwrap();
            max_command = max_command.max(start.elapsed());
        }
        println!(
            "stateful first pass: plan_ms={} max_command_ms={}",
            plan_time.as_secs_f64() * 1000.0,
            max_command.as_secs_f64() * 1000.0
        );
        assert_equivalent(&store, &owner, "measured planned affinity delete");
    }
}

#[test]
#[ignore]
fn measure_affinity_retention_load_before() {
    measure_affinity_retention_load(false);
}

#[test]
#[ignore]
fn measure_affinity_retention_load_after() {
    measure_affinity_retention_load(true);
}

#[test]
fn generated_admission_sequences_with_pruning_match_a_fresh_load() {
    let seeds = 1..=16u64;
    let mut coverage = [false; OPERATIONS];
    for seed in seeds.clone() {
        for (op, _) in generated(seed, 160) {
            coverage[op] = true;
        }
    }
    for (op, reached) in coverage.iter().enumerate() {
        assert!(reached, "generator never reaches {}", OPERATION_NAMES[op]);
    }
    let mut reached = BTreeMap::<&str, usize>::new();
    let mut pruned_affinities = 0;
    for seed in seeds {
        let (_scratch, store) = fixture(&format!("admission-state-property-{seed}"));
        let (_oracle_scratch, oracle) = fixture(&format!("admission-state-oracle-{seed}"));
        let owner = install(&store);
        let mut world = World::default();
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9));
        for (step, (op, now)) in generated(seed, 160).into_iter().enumerate() {
            apply(&store, &oracle, &owner, &mut world, &mut rng, op, now);
            let context = format!("seed={seed} step={step} op={}", OPERATION_NAMES[op]);
            assert_equivalent(&store, &owner, &context);
            assert_eq!(
                group_rows(&store),
                group_rows(&oracle),
                "{context}: rows differ from the oracle"
            );
            assert_same_answers(&published(&owner), &oracle, &world, now, &context);
        }
        let state = published(&owner);
        pruned_affinities += world.pruned_affinities;
        for (name, size) in [
            ("open", state.open.len()),
            ("latest", state.latest.len()),
            ("demand", state.demand.len()),
            ("affinity", state.affinity.len()),
            ("pin_started", state.pin_started.len()),
            ("offers", state.offers.len()),
            ("chains", state.chains.len()),
        ] {
            *reached.entry(name).or_default() += size;
        }
    }
    // Every index must have held entries at some sequence end, or the
    // comparison above proved nothing about it.
    for (name, total) in &reached {
        assert!(
            *total > 0,
            "no sequence ended with a non-empty {name} index"
        );
    }
    assert!(
        pruned_affinities > 0,
        "the generated sequence must delete affinity rows"
    );
}

fn admission(
    principal: &str,
    id: &str,
    pin: &str,
    generation: u64,
    at: i64,
    lasts: i64,
) -> Admission {
    Admission {
        principal: principal.into(),
        id: id.into(),
        hash: "h".into(),
        account: "a".into(),
        credential: "a-key".into(),
        pin: pin.into(),
        generation,
        charged: true,
        bytes: 10,
        committed_at: at,
        valid_for: lasts,
        lease_for: lasts,
    }
}

/// Borrow the indexed-admission fixture for a real catalogue route. Keep the
/// byte count fixed when a caller varies `charged`, so the charge is one bit.
pub(crate) fn pinned_admission_for_route(
    principal: &str,
    id: &str,
    provider: &str,
    model: &str,
    account: &str,
    credential: &str,
    at: i64,
) -> (AccountPinRow, AccountAdmissionRow) {
    let mut input = admission(principal, id, &format!("pin-{id}"), 1, at, 120_000);
    input.hash = format!("history-{id}");
    input.account = account.into();
    input.credential = credential.into();
    input.bytes = 1_000_000;
    let mut row = input.row();
    row.provider = provider.into();
    row.model = model.into();
    let mut pin = input.pin_row();
    pin.provider = provider.into();
    pin.affinity_model = model.into();
    (pin, row)
}

fn prune_everything_before(store: &SqliteRouteStateStore, before: i64) {
    crate::account_writer::prune_account_admissions(store, before, 100, before + 30 * 60_000)
        .unwrap();
}

#[test]
fn pruning_the_newest_finished_admission_reveals_the_older_open_one() {
    let (_scratch, store) = fixture("admission-state-prune-latest");
    let owner = install(&store);
    let older = admission("p", "older", "pin-1", 1, 10, 1_000);
    let newer = admission("p", "newer", "pin-2", 2, 20, 1);
    run(&store, move |tx| {
        admit(tx, &older, true)?;
        admit(tx, &newer, true)?;
        tx.mark_admission_started("p", "newer", "run", 20, false)?;
        tx.mark_admission_finished("p", "newer", 21)?;
        Ok(())
    })
    .unwrap();
    let conversation = ("p".to_string(), "h".to_string(), PROVIDER.to_string());
    let latest = |owner: &StateOwner| {
        published(owner)
            .latest
            .get(&conversation)
            .map(|row| LatestAdmissionRow::clone(row).admission_id)
    };
    assert_eq!(latest(&owner).as_deref(), Some("newer"));
    let reads = maintenance_reads();
    prune_everything_before(&store, 100);
    assert_eq!(latest(&owner).as_deref(), Some("older"));
    assert!(
        maintenance_reads() > reads,
        "the recheck is a counted maintenance read"
    );
    assert_equivalent(&store, &owner, "after pruning the newest admission");
}

#[test]
fn pruning_a_pins_last_started_admission_clears_its_started_count() {
    let (_scratch, store) = fixture("admission-state-prune-pin-started");
    let owner = install(&store);
    let only = admission("p", "only", "pin-1", 1, 10, 1);
    run(&store, move |tx| {
        admit(tx, &only, true)?;
        tx.mark_admission_started("p", "only", "run", 10, false)?;
        tx.mark_admission_finished("p", "only", 11)?;
        Ok(())
    })
    .unwrap();
    let conversation = ("p".to_string(), "h".to_string(), PROVIDER.to_string());
    assert_eq!(published(&owner).pin_started.get(&conversation), Some(&1));
    prune_everything_before(&store, 100);
    assert_eq!(published(&owner).pin_started.get(&conversation), None);
    assert_equivalent(&store, &owner, "after pruning the pin's started admission");
}

#[test]
fn pruning_the_newest_used_offer_reveals_the_older_used_one() {
    let (_scratch, store) = fixture("admission-state-prune-used-offer");
    let owner = install(&store);
    // An original that expired unstarted is open but not live, so retention
    // may delete its continuation rows while the chain still exists.
    let original = admission("p", "original", "pin-1", 1, 10, 1_000);
    let first = admission("p", "first", "pin-2", 2, 10, 1);
    let second = admission("p", "second", "pin-3", 3, 13, 1);
    let record = V1Recorded {
        body_json: "{}".into(),
        answer_json: "{}".into(),
    };
    run(&store, move |tx| {
        admit(tx, &original, true)?;
        tx.mark_admission_expired("p", "original")?;
        admit(tx, &first, false)?;
        tx.record_continuation(
            "p",
            "s1",
            &record,
            "run",
            "original",
            "root",
            Some(("d1", "first", 12)),
        )?;
        tx.use_continuation("d1", 11)?;
        admit(tx, &second, false)?;
        tx.record_continuation(
            "p",
            "s2",
            &record,
            "run",
            "original",
            "d1",
            Some(("d2", "second", 14)),
        )?;
        tx.use_continuation("d2", 13)?;
        // Both reservations finish. The successor keeps its predecessor
        // offer until the successor is pruned, preserving chain resolution.
        tx.mark_admission_finished("p", "first", 14)?;
        tx.mark_admission_finished("p", "second", 14)?;
        Ok(())
    })
    .unwrap();
    let original = ("p".to_string(), "original".to_string());
    let latest_used = |owner: &StateOwner| {
        published(owner)
            .chains
            .get(&original)
            .and_then(|chain| chain.latest_used.clone())
            .map(|(_, reservation)| reservation)
    };
    assert_eq!(latest_used(&owner).as_deref(), Some("second"));
    let reads = maintenance_reads();
    let pruned = crate::account_writer::prune_account_continuations(&store, 100, 100).unwrap();
    assert_eq!(pruned, 1, "only the newest used offer is unprotected");
    assert_eq!(latest_used(&owner).as_deref(), Some("first"));
    assert!(
        maintenance_reads() > reads,
        "the recheck is a counted maintenance read"
    );
    assert_equivalent(&store, &owner, "after pruning the newest used offer");
}

#[test]
fn working_copy_fork_shares_admission_shards_and_reports_cost() {
    let (_scratch, store) = fixture("admission-state-fork");
    let owner = install(&store);
    let rows: Vec<_> = (0..2_000)
        .map(|i| {
            let mut row = admission(
                &format!("p{}", i % 7),
                &format!("a{i}"),
                &format!("pin-{i}"),
                1,
                10 + i,
                1_000,
            );
            row.hash = format!("h{i}");
            row
        })
        .collect();
    run(&store, move |tx| {
        for row in &rows {
            admit(tx, row, true)?;
        }
        Ok(())
    })
    .unwrap();
    let snapshot = owner.published_snapshot().unwrap();
    let fork = snapshot.fork();
    let fork = fork.as_any().downcast_ref::<AccountState>().unwrap();
    assert!(fork
        .admissions
        .open
        .shares_storage_with(&snapshot.admissions.open));
    assert!(fork
        .admissions
        .demand
        .shares_storage_with(&snapshot.admissions.demand));
    let start = std::time::Instant::now();
    for _ in 0..100_000 {
        std::hint::black_box(owner.published_snapshot().unwrap().fork());
    }
    let fork_ns = start.elapsed().as_nanos() / 100_000;
    let start = std::time::Instant::now();
    for _ in 0..20 {
        loaded(&store);
    }
    eprintln!(
        "admission state with {} open admissions: snapshot-and-fork {fork_ns}ns/op; SQL load {}us/op (20 loads)",
        snapshot.admissions.open.len(),
        start.elapsed().as_micros() / 20
    );
}

/// Cost of the admission state at the live installation's size: 223,437
/// admissions (charged and finished, so all of them are demand) over 191,244
/// conversations with an affinity each, across 40 accounts and 30 days.
/// Prints the startup load, one admission command, and 256-row retention
/// batches as writer wall time. Run with `--ignored --nocapture` in release.
#[test]
#[ignore]
fn live_sized_admission_state_costs() {
    const CONVERSATIONS: i64 = 191_244;
    const ADMISSIONS: i64 = 223_437;
    const BASE: i64 = 1_000_000_000_000;
    const STEP: i64 = 11_600;
    let (_scratch, store) = fixture("admission-state-live-size");
    store
        .with_writer_for_test(|conn| {
            conn.execute_batch(&format!(
                "BEGIN;
                 WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < {last_conversation})
                 INSERT INTO account_pin (pin_id, principal, conversation_hash, provider, pin_generation,
                   affinity_model, account_id, credential_id, pin_mode, previous_pin_id, decision_id, created_at)
                 SELECT 'pin-' || i, 'p' || (i % 7), 'h' || i, 'prov', 1, 'model', 'a' || (i % 40),
                   'a' || (i % 40) || '-key', 'conversation_lease', NULL, 'd' || i, {BASE} + i * {STEP} FROM n;
                 WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < {last_conversation})
                 INSERT INTO account_affinity (principal, conversation_hash, provider, affinity_model, pin_id,
                   pin_generation, account_id, credential_id, credential_binding_revision, policy_version,
                   initial_demand_bytes, created_at, last_used_at, expires_at, lease_valid_until)
                 SELECT 'p' || (i % 7), 'h' || i, 'prov', 'model', 'pin-' || i, 1, 'a' || (i % 40),
                   'a' || (i % 40) || '-key', 1, 1, 10, {BASE} + i * {STEP}, {BASE} + i * {STEP},
                   {BASE} + i * {STEP} + 60000, {BASE} + i * {STEP} + 60000 FROM n;
                 WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < {last_admission})
                 INSERT INTO account_admission (principal, admission_id, request_fingerprint, request_mode,
                   decision_id, conversation_hash, provider, model, account_id, credential_id, pin_id,
                   pin_generation, state, charged, request_bytes, request_bytes_estimated, committed_at,
                   admission_valid_until, lease_valid_until, reply_json, run_id, started_at, started_late,
                   released_at, finished_at)
                 SELECT 'p' || ((i % {CONVERSATIONS}) % 7), 'adm-' || i, 'fp', 'admit', 'd' || i,
                   'h' || (i % {CONVERSATIONS}), 'prov', 'model', 'a' || ((i % {CONVERSATIONS}) % 40),
                   'a' || ((i % {CONVERSATIONS}) % 40) || '-key', 'pin-' || (i % {CONVERSATIONS}), 1,
                   CASE WHEN i >= {last_admission} - 200 THEN 'started' ELSE 'finished' END, 1, 100, 0,
                   {BASE} + i * {STEP}, {BASE} + i * {STEP} + 60000, {BASE} + i * {STEP} + 60000, '{{}}',
                   'run-' || i, {BASE} + i * {STEP} + 10, 0, NULL,
                   CASE WHEN i >= {last_admission} - 200 THEN NULL ELSE {BASE} + i * {STEP} + 1000 END
                 FROM n;
                 COMMIT;",
                last_conversation = CONVERSATIONS - 1,
                last_admission = ADMISSIONS - 1,
            ))
        })
        .unwrap();
    // Three loads in the writer's way (one read transaction, then the index
    // build), each with its wall and process CPU time; the host is shared, so
    // the least of the three is the cost.
    for attempt in 1..=3 {
        let cpu = crate::routing_v1::request_cpu_bench::cpu_us();
        let start = std::time::Instant::now();
        let rows = store
            .account_transaction(|tx| tx.load_admission_group())
            .unwrap();
        let read = start.elapsed();
        let state = AdmissionState::from_rows(rows);
        let built = start.elapsed();
        let cpu = crate::routing_v1::request_cpu_bench::cpu_us() - cpu;
        drop(state);
        println!(
            "live-size load {attempt}: {built:?} wall (SQL read {read:?}), {} ms process CPU",
            cpu / 1000
        );
    }
    let start = std::time::Instant::now();
    let owner = install(&store);
    let load = start.elapsed();
    let state = published(&owner);
    let demand: usize = state.demand.iter().map(|(_, rows)| rows.len()).sum();
    println!(
        "live-size startup load: {:?} (open={} latest={} affinity={} demand_accounts={} demand_entries={demand})",
        load,
        state.open.len(),
        state.latest.len(),
        state.affinity.len(),
        state.demand.len(),
    );
    let now = BASE + ADMISSIONS * STEP;
    let mut admits = Vec::new();
    for i in 0..20 {
        let mut fresh = admission(
            "p0",
            &format!("fresh-{i}"),
            &format!("fresh-pin-{i}"),
            1,
            now,
            60_000,
        );
        fresh.hash = format!("fresh-{i}");
        fresh.account = format!("a{}", i % 40);
        fresh.credential = format!("a{}-key", i % 40);
        let start = std::time::Instant::now();
        run(&store, move |tx| admit(tx, &fresh, true)).unwrap();
        admits.push(start.elapsed());
    }
    admits.sort();
    println!(
        "live-size admission command: median {:?} max {:?} (20 commands, writer wall time)",
        admits[10], admits[19]
    );
    for (name, prune) in [("admissions", 0), ("affinities", 1), ("continuations", 2)] {
        let before = BASE + 2_000 * STEP;
        let start = std::time::Instant::now();
        let pruned = match prune {
            0 => crate::account_writer::prune_account_admissions(
                &store,
                before,
                256,
                now + 30 * 60_000,
            )
            .unwrap(),
            1 => {
                crate::account_writer::prune_account_affinities(
                    &store,
                    store
                        .plan_account_affinities(before, now, None, 256)
                        .unwrap()
                        .keys,
                    before,
                    now,
                )
                .unwrap()
                .affinities
            }
            _ => crate::account_writer::prune_account_continuations(&store, before, 256).unwrap(),
        };
        println!(
            "live-size prune {name}: {pruned} rows in {:?} (one retention batch, writer wall time)",
            start.elapsed()
        );
    }
    assert_equivalent(&store, &owner, "after live-size commands");
}

/// One command in a batch finding the state disagreeing with SQL rolls the
/// whole batch back; the writer reloads the state and runs the batch again.
/// A command earlier in the batch that computed from the stale state must not
/// have its stale reply published, and the detecting command must succeed on
/// the fresh state instead of failing.
#[test]
fn a_batch_with_a_diverged_command_runs_again_on_fresh_state() {
    let (_scratch, store) = fixture("batch-divergence");
    let _owner = install(&store);
    let first = admission("p", "first", "pin-1", 1, 1_000, 60_000);
    let hash = first.hash.clone();
    run(&store, move |tx| admit(tx, &first, true)).unwrap();
    // A write the state never saw (the writer's own connection does not
    // report it as a change): the lease moves from 61,000 to 66,000.
    store
        .with_writer_for_test(|conn| {
            conn.execute(
                "UPDATE account_affinity SET lease_valid_until = lease_valid_until + 5000",
                [],
            )
        })
        .unwrap();
    // Hold the writer on a command until A and B are both queued, so the two
    // run in one batch.
    let (started, running) = std::sync::mpsc::channel::<()>();
    let (release, held) = std::sync::mpsc::channel::<()>();
    let held = std::sync::Mutex::new(held);
    let blocker = store
        .enqueue_account_state_command(AccountCommandKind::Repair, move |_, _| {
            let _ = started.send(());
            let _ = held.lock().unwrap().recv();
            Ok(())
        })
        .unwrap();
    running.recv().unwrap();
    // A only reads the lease.
    let lease = {
        let hash = hash.clone();
        store
            .enqueue_account_state_command(AccountCommandKind::Repair, move |tx, state| {
                let tx = AccountCommand::new(tx, state);
                Ok(tx
                    .affinity("p", &hash, PROVIDER)?
                    .unwrap()
                    .lease_valid_until)
            })
            .unwrap()
    };
    // B ends the pin's lease at 62,000: the state, still at 61,000, sees
    // nothing to end, while SQL, at 66,000, ends it.
    let ended = store
        .enqueue_account_state_command(AccountCommandKind::Repair, move |tx, state| {
            AccountCommand::new(tx, state).end_lease("pin-1", 62_000)
        })
        .unwrap();
    release.send(()).unwrap();
    blocker.wait().unwrap();
    assert_eq!(
        lease.wait().unwrap(),
        Some(66_000),
        "A replied from the stale state"
    );
    assert!(
        ended.wait().unwrap(),
        "B must succeed on the reloaded state"
    );
    assert_eq!(
        store.account_writer_batch_sizes_for_test().last(),
        Some(&2),
        "A and B ran in one batch"
    );
}

/// The lease-end write a continuation decides must name the pin, generation
/// and lease end the state saw. If SQL has moved on, it is a divergence, not
/// an overwrite of the newer lease.
#[test]
fn a_decided_lease_end_does_not_overwrite_a_lease_the_state_did_not_see() {
    let (_scratch, store) = fixture("lease-end-cas");
    let _owner = install(&store);
    let first = admission("p", "first", "pin-1", 1, 1_000, 60_000);
    run(&store, move |tx| admit(tx, &first, true)).unwrap();
    let mut state = store.account_transaction(AccountState::load).unwrap();
    // SQL's lease moves on after the state read 61,000.
    store
        .with_writer_for_test(|conn| {
            conn.execute("UPDATE account_affinity SET lease_valid_until = 70000", [])
        })
        .unwrap();
    let result = store.account_transaction(move |tx| {
        let mut command = AccountCommand::new(tx, Some(&mut state as &mut dyn AccountWorkingState));
        command.decide_for_test(|admissions| {
            admissions.end_lease("pin-1", 50_000);
        })
    });
    let error = result.unwrap_err().to_string();
    assert!(error.contains(striatum_store::STATE_DIVERGED), "{error}");
    let lease: Option<i64> = store
        .with_writer_for_test(|conn| {
            conn.query_row(
                "SELECT lease_valid_until FROM account_affinity",
                [],
                |row| row.get(0),
            )
        })
        .unwrap();
    assert_eq!(lease, Some(70_000));
}

/// A transition the state finds not applicable (the admission is not open
/// in memory) must not be skipped silently when SQL still holds the row in a
/// state the transition applies to.
#[test]
fn a_decided_transition_the_state_skips_is_confirmed_against_sql() {
    let (_scratch, store) = fixture("unchanged-guard");
    let _owner = install(&store);
    let first = admission("p", "first", "pin-1", 1, 1_000, 60_000);
    run(&store, move |tx| admit(tx, &first, true)).unwrap();
    let mut state = store.account_transaction(AccountState::load).unwrap();
    // The state forgets the open admission, as a missed update would.
    state.admissions.open = Default::default();
    let result = store.account_transaction(move |tx| {
        let mut command = AccountCommand::new(tx, Some(&mut state as &mut dyn AccountWorkingState));
        command.decide_for_test(|admissions| {
            admissions.finish("p", "first", 2_000);
        })
    });
    let error = result.unwrap_err().to_string();
    assert!(error.contains(striatum_store::STATE_DIVERGED), "{error}");
}

/// Releasing an expired admission's probe checks the admission first. One
/// that started meanwhile keeps its probe authorization and is left as it is.
#[test]
fn a_probe_release_leaves_a_started_admission_and_its_authorization_alone() {
    let (_scratch, store) = fixture("probe-release-started");
    let _owner = install(&store);
    let first = admission("p", "first", "pin-1", 1, 1_000, 60_000);
    run(&store, move |tx| {
        admit(tx, &first, true)?;
        tx.mark_admission_started("p", "first", "run", 2_000, false)
            .map(|_| ())
    })
    .unwrap();
    let owner = serde_json::json!(["account_admission", "p", "first"]).to_string();
    store
        .with_writer_for_test(|conn| {
            conn.execute(
                "INSERT INTO fence_probe_authorization VALUES (?1, 'kind', 'origin', 'evidence', 1, 1, 1)",
                [&owner],
            )
        })
        .unwrap();
    store
        .account_transaction(move |tx| {
            let _ = tx.release_admission_probes(
                vec![("p".into(), "first".into(), owner.clone())],
                100_000,
            )?;
            Ok(())
        })
        .unwrap();
    let (authorizations, state): (i64, String) = store
        .with_writer_for_test(|conn| {
            conn.query_row(
                "SELECT (SELECT count(*) FROM fence_probe_authorization), \
                   (SELECT state FROM account_admission WHERE admission_id = 'first')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
        })
        .unwrap();
    assert_eq!((authorizations, state.as_str()), (1, "started"));
}
