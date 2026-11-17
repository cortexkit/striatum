//! Writer CPU comparison, run without a profiler, on the heap-measurement rows
//! from `memory_tests::rows()`: about 31k open admissions and 125k pinned
//! conversations, the size of the live store's account state in November 2026.

use super::*;
use rusqlite::params;
use striatum_store::{AccountAdmissionRow, AccountAffinityRow, AccountPinRow, AdmissionGroupRows};

const NOW: i64 = 1_800_000_000_000;

fn seed(store: &SqliteRouteStateStore, rows: &AdmissionGroupRows, now: i64) {
    let shift = now - NOW;
    store.with_writer_for_test(|conn| {
        conn.execute_batch("BEGIN")?;
        // The heap fixture projects historical admissions without a pin history.
        // Seed unique pin identities for its foreign keys; the measured commands
        // read the original admission/affinity conversations, not these pin keys.
        let mut pin = conn.prepare_cached("INSERT OR IGNORE INTO account_pin
            (pin_id,principal,conversation_hash,provider,pin_generation,affinity_model,account_id,
             credential_id,pin_mode,previous_pin_id,decision_id,created_at)
             VALUES (?1,?2,?3,?4,1,?5,?6,?7,'conversation_lease',NULL,?8,?9)")?;
        let mut admission = conn.prepare_cached("INSERT INTO account_admission
            (principal,admission_id,request_fingerprint,request_mode,decision_id,conversation_hash,
             provider,model,account_id,credential_id,pin_id,pin_generation,state,charged,request_bytes,
             request_bytes_estimated,committed_at,admission_valid_until,lease_valid_until,reply_json,
             run_id,started_at,started_late,released_at,finished_at,last_observed_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26)")?;
        for (i, scan) in rows.admissions.iter().enumerate() {
            let mut row = rows.open.first().unwrap().row.clone();
            if let Some(open) = rows.open.get(i) {
                row = open.row.clone();
            } else {
                let latest = &scan.latest;
                row.principal = latest.principal.clone();
                row.admission_id = latest.admission_id.clone();
                row.conversation_hash = latest.conversation_hash.clone();
                row.provider = latest.provider.clone();
                row.account_id = latest.account_id.clone();
                row.pin_id = latest.pin_id.clone();
                row.state = latest.state.clone();
                row.committed_at = latest.committed_at;
                row.admission_valid_until = latest.admission_valid_until;
                row.lease_valid_until = latest.lease_valid_until;
                row.charged = scan.charged;
                row.request_bytes = scan.request_bytes;
                row.started_at = scan.started.then_some(latest.committed_at + 1);
            }
            row.committed_at += shift;
            row.admission_valid_until += shift;
            row.lease_valid_until += shift;
            row.started_at = row.started_at.map(|at| at + shift);
            row.last_observed_at = row.last_observed_at.map(|at| at + shift);
            pin.execute(params![row.pin_id,row.principal,row.pin_id,row.provider,
                row.model,row.account_id,row.credential_id,row.decision_id,row.committed_at])?;
            admission.execute(params![row.principal,row.admission_id,row.request_fingerprint,
                row.request_mode,row.decision_id,row.conversation_hash,row.provider,row.model,
                row.account_id,row.credential_id,row.pin_id,row.pin_generation,row.state,row.charged,
                row.request_bytes,row.request_bytes_estimated,row.committed_at,row.admission_valid_until,
                row.lease_valid_until,row.reply_json,row.run_id,row.started_at,row.started_late,
                row.released_at,row.finished_at,row.last_observed_at])?;
        }
        let mut affinity = conn.prepare_cached("INSERT INTO account_affinity
            (principal,conversation_hash,provider,affinity_model,pin_id,pin_generation,account_id,
             credential_id,credential_binding_revision,policy_version,initial_demand_bytes,created_at,
             last_used_at,expires_at,lease_valid_until)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)")?;
        for row in &rows.affinities {
            pin.execute(params![row.pin_id,row.principal,row.pin_id,row.provider,
                row.affinity_model,row.account_id,row.credential_id,"cpu-seed",row.created_at])?;
            affinity.execute(params![row.principal,row.conversation_hash,row.provider,row.affinity_model,
                row.pin_id,row.pin_generation,row.account_id,row.credential_id,row.credential_binding_revision,
                row.policy_version,row.initial_demand_bytes,row.created_at+shift,row.last_used_at+shift,row.expires_at+shift,
                row.lease_valid_until.map(|at| at+shift)])?;
        }
        conn.execute_batch("COMMIT")
    }).unwrap();
}

fn command<T: Send + 'static>(
    store: &SqliteRouteStateStore,
    command: impl FnMut(&mut AccountCommand<'_>) -> RouteStateResult<T> + Send + 'static,
) -> T {
    crate::account_writer::execute(store, striatum_store::AccountCommandKind::Repair, command)
        .unwrap()
}

fn sample(
    store: &SqliteRouteStateStore,
    label: &str,
    rows: &[AccountAdmissionRow],
    change: impl Fn(&mut AccountCommand<'_>, &AccountAdmissionRow) -> RouteStateResult<()>
        + Copy
        + Send
        + 'static,
) {
    let cpu = crate::routing_v1::request_cpu_bench::cpu_us();
    let mut wall = Vec::with_capacity(rows.len());
    for row in rows {
        let row = row.clone();
        let before = std::time::Instant::now();
        command(store, move |tx| change(tx, &row));
        wall.push(before.elapsed().as_micros());
    }
    let cpu = crate::routing_v1::request_cpu_bench::cpu_us() - cpu;
    wall.sort_unstable();
    println!(
        "live_state_{label} n={} cpu_us/op={:.3} p50_us={} p99_us={}",
        rows.len(),
        cpu as f64 / rows.len() as f64,
        wall[wall.len() / 2],
        wall[wall.len() * 99 / 100]
    );
}

fn pin(row: &AccountAdmissionRow) -> AccountPinRow {
    AccountPinRow {
        pin_id: row.pin_id.clone(),
        principal: row.principal.clone(),
        conversation_hash: row.conversation_hash.clone(),
        provider: row.provider.clone(),
        pin_generation: 1,
        affinity_model: row.model.clone(),
        account_id: row.account_id.clone(),
        credential_id: row.credential_id.clone(),
        pin_mode: "conversation_lease".into(),
        previous_pin_id: None,
        decision_id: row.decision_id.clone(),
        created_at: row.committed_at,
    }
}

fn affinity(row: &AccountAdmissionRow) -> AccountAffinityRow {
    AccountAffinityRow {
        principal: row.principal.clone(),
        conversation_hash: row.conversation_hash.clone(),
        provider: row.provider.clone(),
        affinity_model: row.model.clone(),
        pin_id: row.pin_id.clone(),
        pin_generation: 1,
        account_id: row.account_id.clone(),
        credential_id: row.credential_id.clone(),
        credential_binding_revision: 1,
        policy_version: 1,
        initial_demand_bytes: row.request_bytes,
        created_at: row.committed_at,
        last_used_at: row.committed_at,
        expires_at: row.lease_valid_until,
        lease_valid_until: Some(row.lease_valid_until),
    }
}

#[test]
#[ignore = "alternating profiler-off live-shaped writer CPU comparison"]
fn live_shaped_writer_cpu() {
    // The writer has its own thread-local clock, so seed relative to host time.
    let now = crate::now_ms() as i64;
    let rows = super::memory_tests::rows();
    let scratch = cortexkit_test_support::ScratchDir::new("compact-writer-cpu");
    let store = SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
        "compact-writer-cpu",
        scratch.path().join("state.db").to_string_lossy(),
    ))
    .unwrap();
    seed(&store, &rows, now);
    let owner = Arc::new(StateOwner::default());
    store.install_account_state_bridge(owner.clone()).unwrap();
    let snapshot = owner.published_snapshot().unwrap();
    assert_eq!(snapshot.admissions.open.len(), 30_981);
    assert_eq!(snapshot.admissions.latest.len(), 124_291);
    assert_eq!(snapshot.admissions.affinity.len(), 125_873);
    drop(snapshot);
    let mut existing = Vec::new();
    let mut fresh = Vec::new();
    for (i, binding) in rows.affinities.iter().take(1_000).enumerate() {
        let mut row = rows.open[0].row.clone();
        row.admission_id = format!("cpu-existing-{i}");
        row.decision_id = format!("cpu-existing-decision-{i}");
        row.principal = binding.principal.clone();
        row.conversation_hash = binding.conversation_hash.clone();
        row.provider = binding.provider.clone();
        row.account_id = binding.account_id.clone();
        row.credential_id = binding.credential_id.clone();
        row.pin_id = binding.pin_id.clone();
        row.model = binding.affinity_model.clone();
        row.state = "reserved".into();
        row.charged = true;
        row.committed_at = now;
        row.admission_valid_until = now + 60_000;
        row.lease_valid_until = now + 120_000;
        existing.push(row.clone());
        row.admission_id = format!("cpu-new-{i}");
        row.decision_id = format!("cpu-new-decision-{i}");
        row.conversation_hash = format!("{:064x}", 1_000_000 + i);
        row.pin_id = format!("cpu-new-pin-{i}");
        fresh.push(row);
    }
    drop(rows);
    sample(&store, "existing_admit", &existing, move |tx, row| {
        let binding = tx
            .affinity(&row.principal, &row.conversation_hash, &row.provider)?
            .unwrap();
        std::hint::black_box(tx.parent_used_account(
            &row.principal,
            &row.conversation_hash,
            &row.provider,
            now,
        )?);
        std::hint::black_box(tx.pin_has_started(&binding)?);
        tx.insert_admission(row)
    });
    sample(&store, "new_admit", &fresh, move |tx, row| {
        assert!(tx
            .affinity(&row.principal, &row.conversation_hash, &row.provider)?
            .is_none());
        tx.insert_pin(&pin(row))?;
        assert!(tx.put_affinity(&affinity(row), None)?);
        tx.insert_admission(row)
    });
    sample(&store, "report_start", &existing, move |tx, row| {
        std::hint::black_box(
            tx.open_admission(&row.principal, &row.admission_id)?
                .unwrap(),
        );
        assert!(tx.mark_admission_started(
            &row.principal,
            &row.admission_id,
            &format!("cpu-run-{}", row.admission_id),
            now + 1,
            false
        )?);
        Ok(())
    });
    sample(&store, "report_attempt", &existing, move |tx, row| {
        let row = tx
            .open_admission(&row.principal, &row.admission_id)?
            .unwrap();
        tx.observe_admission(&row, now + 2)
    });
    sample(&store, "report_finish", &existing, move |tx, row| {
        std::hint::black_box(
            tx.open_admission(&row.principal, &row.admission_id)?
                .unwrap(),
        );
        assert!(tx.mark_admission_finished(&row.principal, &row.admission_id, now + 3)?);
        Ok(())
    });
    let published = owner.published_snapshot().unwrap();
    let loaded = store
        .account_read(|tx| AccountState::load_at(tx, now + 3))
        .unwrap();
    assert!(
        published.admissions.open == loaded.admissions.open,
        "open rows differ"
    );
    assert!(
        published.admissions.latest == loaded.admissions.latest,
        "latest rows differ"
    );
    assert!(
        published.admissions.demand == loaded.admissions.demand,
        "demand rows differ"
    );
    assert!(
        published.admissions.affinity == loaded.admissions.affinity,
        "affinity rows differ"
    );
    assert!(
        published.admissions == loaded.admissions,
        "admission indexes differ"
    );
    assert!(
        published.inflight == loaded.inflight,
        "inflight ledger differs"
    );
    assert!(published.blocks == loaded.blocks, "block state differs");
}
