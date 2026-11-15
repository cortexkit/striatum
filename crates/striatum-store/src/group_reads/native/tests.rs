fn native_rig(name: &str) -> crate::block_state::tests::Rig {
    let rig = crate::block_state::tests::rig_before_block_migration(name);
    rig.store
        .with_writer_for_test(|conn| {
            crate::block_storage::install_migration_function(conn)?;
            conn.execute_batch(crate::block_storage::MIGRATION)
        })
        .unwrap();
    rig.reload();
    rig
}

#[test]
fn new_source_commands_read_only_classified_writer_bookkeeping() {
    let rig = native_rig("source-order-writer-reads");
    let trace = rig.store.trace_block_reads_for_test().unwrap();
    let refusal = striatum_core::route_state_store::CredentialRefusal {
        provider_id: "bookkeeping".into(),
        credential_id: "credential".into(),
        code: "ENTITLEMENT_ERROR".into(),
    };
    rig.store
        .record_credential_entitlement_fence(&refusal, None, crate::now_ms())
        .unwrap();
    assert!(rig
        .published()
        .entitlement_fence("bookkeeping", "credential")
        .is_some());
    let reads = trace.take();
    assert!(
        reads.reader.is_empty() && reads.writer.is_empty(),
        "a published-state command must not read block data to decide its action: {reads:#?}"
    );
    assert_eq!(reads.writer_bookkeeping.len(), 1);
    assert_eq!(
        reads.writer_bookkeeping[0],
        crate::block_storage::NEXT_SOURCE_ORDER
    );
    assert_eq!(rig.published(), rig.fresh());
}

#[test]
fn source_order_exemption_cannot_be_reused_outside_a_writer_command() {
    let rig = native_rig("source-order-exemption-boundary");
    let trace = rig.store.trace_block_reads_for_test().unwrap();
    let query = crate::block_storage::NEXT_SOURCE_ORDER;
    rig.store
        .with_reader_for_test(|conn| {
            conn.query_row(query, ["credential"], |row| row.get::<_, i64>(0))
        })
        .unwrap();
    let read = trace.take();
    assert_eq!(
        read.reader.len(),
        1,
        "the exact exempt statement is an ordinary request read outside a command: {read:#?}"
    );
    assert!(read.writer_bookkeeping.is_empty());
    rig.store
        .store
        .with_conn(|conn| conn.query_row(query, ["credential"], |row| row.get::<_, i64>(0)))
        .unwrap();
    let read = trace.take();
    assert_eq!(
        read.writer.len(),
        1,
        "a writer connection alone does not make the read writer bookkeeping: {read:#?}"
    );
    assert!(read.writer_bookkeeping.is_empty());
    rig.store
        .with_reader_for_test(|conn| {
            conn.query_row(
                "/* writer bookkeeping: next source order per kind */ SELECT count(*) FROM block",
                [],
                |row| row.get::<_, i64>(0),
            )
        })
        .unwrap();
    let read = trace.take();
    assert_eq!(read.reader.len(), 1);
    assert!(
        read.writer_bookkeeping.is_empty(),
        "copying only the marker exempts nothing"
    );
}

#[test]
fn new_block_request_reads_and_outside_command_writes_move_the_counters() {
    let rig = native_rig("physical-block-negative-counters");
    let trace = rig.store.trace_block_reads_for_test().unwrap();
    rig.store
        .with_reader_for_test(|conn| {
            conn.query_row("SELECT count(*) FROM block", [], |row| row.get::<_, i64>(0))
        })
        .unwrap();
    let read = trace.take();
    assert_eq!(read.reader.len(), 1);
    assert!(read.writer_bookkeeping.is_empty());
    let before = rig.store.block_writes_outside_command();
    rig.store.store.with_conn(|conn|conn.execute("INSERT INTO block(kind,origin_key,provider,evidence,evidence_at,quota,next_recheck_at)
        VALUES ('provider','[\"outside\"]','outside','evidence',1,0,2)",[])).unwrap();
    assert!(
        rig.store.block_writes_outside_command() > before,
        "an unbatched write to the new table must be counted"
    );
}

#[test]
fn next_source_order_is_a_covering_index_seek_not_a_row_scan() {
    let rig = native_rig("source-order-query-plan");
    rig.store
        .with_reader_for_test(|conn| {
            let query = crate::block_storage::NEXT_SOURCE_ORDER;
            for kind in ["credential", "non_quota_route", "transient_route"] {
                let plan = conn
                    .prepare(&format!("EXPLAIN QUERY PLAN {query}"))?
                    .query_map([kind], |row| row.get::<_, String>(3))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                assert!(
                    plan.iter()
                        .all(|step| !step.contains("SCAN block") && !step.contains("TEMP B-TREE")),
                    "{kind}: {plan:?}"
                );
                let seeks = plan
                    .iter()
                    .filter(|step| {
                        step.contains(
                            "SEARCH block USING COVERING INDEX block_source_order (kind=?)",
                        )
                    })
                    .count();
                assert_eq!(
                    seeks, 3,
                    "each of the three conditional scalar MAX queries must seek its kind: {plan:?}"
                );
                // A SEARCH plan alone is not sufficient: MAX over an expression
                // could still visit every row in that kind's index range. SQLite's
                // min/max optimization skips the loop after taking the first row.
                let bytecode = conn
                    .prepare(&format!("EXPLAIN {query}"))?
                    .query_map([kind], |row| {
                        Ok((row.get::<_, String>(1)?, row.get::<_, Option<String>>(5)?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let steps = bytecode
                    .iter()
                    .enumerate()
                    .filter(|(_, row)| row.0 == "AggStep" && row.1.as_deref() == Some("max(1)"))
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                assert_eq!(steps.len(), 3, "{bytecode:?}");
                assert!(
                    steps
                        .iter()
                        .all(|index| bytecode.get(index + 1).is_some_and(|next| next.0 == "Goto")),
                    "each MAX must exit after its first matching index row: {bytecode:?}"
                );
            }
            Ok(())
        })
        .unwrap();
}
