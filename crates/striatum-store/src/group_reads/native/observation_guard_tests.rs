use super::native_rig;
use crate::{AccountCommandKind, AccountObservationRow};

fn receipt(id: &str, at: i64) -> AccountObservationRow {
    AccountObservationRow {
        observation_id: id.into(),
        principal: "retirement-guard".into(),
        runner: "runner".into(),
        event_id: id.into(),
        kind: "transient_backoff".into(),
        admission_id: None,
        bound: true,
        observation_json: "{}".into(),
        effects_json: serde_json::json!([{"effect":"transient_backoff","provider":"guard-provider",
            "account_id":"guard-account","retry_at_ms":at+60_000}])
        .to_string(),
        received_at: at,
    }
}

#[test]
fn observation_effect_insert_makes_exactly_one_guard_lookup_and_other_inserts_make_none() {
    let rig = native_rig("observation-guard-counter");
    let trace = rig.store.trace_block_reads_for_test().unwrap();
    let at = crate::now_ms();
    let row = receipt("guard-first", at);
    rig.store
        .account_command(AccountCommandKind::Observe, move |tx| {
            tx.insert_observation(&row)
        })
        .unwrap();
    let reads = trace.take();
    assert!(
        reads.reader.is_empty() && reads.writer.is_empty(),
        "unexpected request reads: {reads:#?}"
    );
    assert_eq!(
        reads.observation_retirement_guards.len(),
        1,
        "one lookup per covered source insert: {reads:#?}"
    );
    assert_eq!(reads.writer_bookkeeping.len(), 1);
    assert_eq!(rig.published(), rig.fresh());
    let key = serde_json::json!(["guard-provider", "guard-account"]).to_string();
    let origin = rig.published().source_origin("observation", &key).unwrap();
    rig.store
        .account_command(AccountCommandKind::Observe, move |tx| {
            tx.command_sql(|conn| {
                conn.execute(
                    "INSERT INTO retirement_tombstone VALUES (?1,?2,?3,?4,?5)",
                    rusqlite::params![
                        origin.kind,
                        origin.origin_key,
                        origin.evidence,
                        origin.evidence_at,
                        at + 1
                    ],
                )
            })?;
            Ok(())
        })
        .unwrap();
    trace.take();
    let row = receipt("guard-older-receipt", at - 1);
    rig.store
        .account_command(AccountCommandKind::Observe, move |tx| {
            tx.insert_observation(&row)
        })
        .unwrap();
    let reads = trace.take();
    assert_eq!(
        reads.observation_retirement_guards.len(),
        1,
        "a rejected insert still performs one lookup"
    );
    assert!(
        reads.reader.is_empty() && reads.writer.is_empty(),
        "unexpected request reads: {reads:#?}"
    );
    assert!(rig.published().source_origin("observation", &key).is_none());
    assert_eq!(rig.published(), rig.fresh());
    let refusal = striatum_core::route_state_store::CredentialRefusal {
        provider_id: "uncovered".into(),
        credential_id: "credential".into(),
        code: "ENTITLEMENT_ERROR".into(),
    };
    rig.store
        .record_credential_entitlement_fence(&refusal, None, at + 2)
        .unwrap();
    let reads = trace.take();
    assert_eq!(
        reads.observation_retirement_guards.len(),
        0,
        "an uncovered source insert has no retirement lookup: {reads:#?}"
    );
    assert!(
        reads.reader.is_empty() && reads.writer.is_empty(),
        "the ordinary insert is still write-only: {reads:#?}"
    );
    assert_eq!(rig.published(), rig.fresh());
}

#[test]
fn observation_retirement_guard_is_not_exempt_on_non_writer_connections_or_by_marker() {
    let rig = native_rig("observation-guard-boundary");
    let trace = rig.store.trace_block_reads_for_test().unwrap();
    let marker = crate::block_storage::OBSERVATION_RETIREMENT_GUARD_MARKER;
    rig.store
        .with_reader_for_test(|conn| {
            conn.prepare(&format!(
                "{marker} {}",
                crate::block_storage::OBSERVATION_RETIREMENT_LOOKUP
            ))?
            .query_map(
                rusqlite::named_params! {":key":"[\"outside\",\"account\"]",":at":0},
                |row| row.get::<_, i64>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap();
    let reads = trace.take();
    assert_eq!(
        reads.reader.len(),
        1,
        "a request cannot borrow the insert's marker: {reads:#?}"
    );
    assert!(reads.observation_retirement_guards.is_empty());
    let row = crate::block_storage::StoredBlock::from_origin(crate::fence_rechecks::ViewOrigin {
        kind: "observation".into(),
        origin_key: "[\"outside\",\"account\"]".into(),
        provider: "outside".into(),
        account_id: Some("account".into()),
        evidence: "[\"outside-receipt\",0]".into(),
        evidence_at: crate::now_ms(),
        quota: Some(false),
        deadline: Some((crate::now_ms() + 60_000) as f64),
        ..Default::default()
    })
    .unwrap();
    // An out-of-command write is intentional here: using the writer's SQLite
    // connection is not permission to use a writer-command read exemption.
    rig.store
        .store
        .with_conn(|conn| crate::block_storage::write_source(conn, &row))
        .unwrap();
    let reads = trace.take();
    assert!(
        reads
            .writer
            .iter()
            .any(|sql| crate::block_storage::is_observation_retirement_insert(sql)),
        "the exact guarded statement must remain an ordinary read outside a command: {reads:#?}"
    );
    assert!(reads.observation_retirement_guards.is_empty());
}

#[test]
fn observation_retirement_lookup_is_a_covering_seek_without_scan_and_returns_at_most_one_row() {
    let rig = native_rig("observation-guard-plan");
    let key = "[\"plan-provider\",\"plan-account\"]";
    rig.store
        .account_command(AccountCommandKind::Maintenance, move |tx| {
            tx.command_sql(|conn| {
                for at in 0..100 {
                    conn.execute(
                        "INSERT INTO retirement_tombstone VALUES ('observation',?1,?2,?3,?3)",
                        rusqlite::params![key, format!("receipt-{at}"), at],
                    )?;
                }
                Ok(())
            })
        })
        .unwrap();
    rig.store
        .with_reader_for_test(|conn| {
            let sql = crate::block_storage::OBSERVATION_RETIREMENT_LOOKUP;
            let plan = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
                .query_map(rusqlite::named_params! {":key":key,":at":50}, |row| {
                    row.get::<_, String>(3)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            assert!(
                plan.iter().any(|line| line.contains("SEARCH")
                    && line.contains("COVERING INDEX observation_retirement_guard")
                    && line.contains("kind=?")
                    && line.contains("origin_key=?")
                    && line.contains("evidence_at>")),
                "{plan:?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|line| line.contains("SCAN") || line.contains("TEMP B-TREE")),
                "{plan:?}"
            );
            let mut query = conn.prepare(sql)?;
            let matches = query
                .query_map(rusqlite::named_params! {":key":key,":at":50}, |row| {
                    row.get::<_, i64>(0)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            assert_eq!(
                matches,
                vec![1],
                "fifty matching generations must still visit at most one result"
            );
            assert_eq!(query.get_status(rusqlite::StatementStatus::FullscanStep), 0);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        rig.published()
            .table_sizes()
            .into_iter()
            .find(|(name, _)| *name == "retirement_tombstone")
            .unwrap()
            .1,
        0,
        "retirement history must remain outside the loaded state"
    );
}
