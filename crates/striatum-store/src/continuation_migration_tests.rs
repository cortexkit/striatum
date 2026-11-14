//! Migration 088 normalizes continuation rows that carry only part of an
//! offer.

use super::*;

fn collect<T>(
    conn: &Connection,
    sql: &str,
    map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<Vec<T>> {
    let mut statement = conn.prepare(sql)?;
    let rows = statement.query_map([], map)?;
    rows.collect()
}

type ContinueRow = (String, Option<String>, Option<String>, Option<i64>, String);

/// Migration 088 turns a continuation row that has only some of its offer
/// columns into a plain record that keeps its replay body, and leaves
/// whole offers and plain records alone. The partial shapes are the ones
/// the schema allows and the routing writer never writes.
#[test]
fn migration_clears_partial_continuation_offers_and_keeps_their_bodies() {
    let scratch = cortexkit_test_support::ScratchDir::new("partial-continuation-migration");
    let path = scratch.path().join("store.db");
    let descriptor = crate::dev_descriptor("partial-continuation", path.to_string_lossy());
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 87).unwrap();
    old.with_writer_for_test(|conn| {
        conn.execute_batch(
            "INSERT INTO account_continue (principal, selection_id, request_json, \
               answer_json, run_id, admission_id, previous_decision_id, decision_id, \
               reservation_id, valid_until_ms) VALUES \
             ('p', 'a-no-reservation', '{}', '\"a\"', 'run', 'original', 'root', 'd1', NULL, NULL), \
             ('p', 'b-no-deadline', '{}', '\"b\"', 'run', 'original', 'root', 'd2', 'r2', NULL), \
             ('p', 'c-no-decision', '{}', '\"c\"', 'run', 'original', 'root', NULL, 'r3', 5), \
             ('p', 'd-whole', '{}', '\"d\"', 'run', 'original', 'root', 'd4', 'r4', 5), \
             ('p', 'e-record', '{}', '\"e\"', 'run', 'original', 'root', NULL, NULL, NULL)",
        )
    })
    .unwrap();
    drop(old);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    let rows: Vec<ContinueRow> = store
        .with_writer_for_test(|conn| {
            collect(
                conn,
                "SELECT selection_id, decision_id, reservation_id, valid_until_ms, \
                   answer_json FROM account_continue ORDER BY selection_id",
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
        })
        .unwrap();
    let record = |selection: &str, answer: &str| -> ContinueRow {
        (selection.into(), None, None, None, answer.into())
    };
    assert_eq!(
        rows,
        vec![
            record("a-no-reservation", "\"a\""),
            record("b-no-deadline", "\"b\""),
            record("c-no-decision", "\"c\""),
            (
                "d-whole".into(),
                Some("d4".into()),
                Some("r4".into()),
                Some(5),
                "\"d\"".into()
            ),
            record("e-record", "\"e\""),
        ]
    );
}
