use super::*;

const T: i64 = 1_790_683_200_000;
const START: i64 = T - 2 * 3_600_000;
const RECENT: i64 = T - 60_000;
const LEFT_OUT: &str = include_str!("../queries/admission_last_observed_left_out.sql");

fn admission(conn: &Connection, principal: &str, id: &str, state: &str) {
    let pin = format!("{principal}/{id}");
    conn.execute(
        "INSERT INTO account_pin (pin_id, principal, conversation_hash, provider,
         pin_generation, affinity_model, account_id, credential_id, pin_mode, decision_id, created_at)
         VALUES (?1, ?2, ?1, 'provider', 1, 'model', 'A', 'key', 'run_frozen', 'decision', ?3)",
        params![pin, principal, START],
    ).unwrap();
    conn.execute(
        "INSERT INTO account_admission (principal, admission_id, request_fingerprint, request_mode,
         decision_id, conversation_hash, provider, model, account_id, credential_id, pin_id,
         pin_generation, state, charged, request_bytes, request_bytes_estimated, committed_at,
         admission_valid_until, lease_valid_until, reply_json, run_id, started_at, model_decision_id)
         VALUES (?1, ?2, 'fp', 'admit', 'decision', ?3, 'provider', 'model', 'A', 'key', ?3,
         1, ?4, 1, 123, 0, ?5, ?5 + 60000, ?5 + 120000, '{\"kept\":true}', 'run', ?6, 'owner-decision')",
        params![principal, id, pin, state, START, if state == "reserved" { None } else { Some(START) }],
    ).unwrap();
}

#[allow(clippy::too_many_arguments)]
fn attempt(
    conn: &Connection,
    principal: &str,
    runner: &str,
    id: &str,
    decision: bool,
    bound: bool,
    effects: &str,
    at: i64,
) {
    let raw = if decision {
        r#"{"decision_id":"owner-decision"}"#
    } else {
        "{}"
    };
    conn.execute(
        "INSERT INTO account_observation (observation_id, principal, runner, event_id, kind,
         admission_id, bound, observation_json, effects_json, received_at)
         VALUES (lower(hex(randomblob(16))), ?1, ?2, lower(hex(randomblob(16))), 'attempt', ?3, ?4, ?5, ?6, ?7)",
        params![principal, runner, id, bound, raw, effects, at],
    ).unwrap();
}

fn rows(conn: &Connection, migrated: bool) -> Vec<Vec<rusqlite::types::Value>> {
    let mut statement = conn
        .prepare("SELECT * FROM account_admission ORDER BY principal, admission_id")
        .unwrap();
    let columns = statement.column_count() - usize::from(migrated);
    statement
        .query_map([], |row| (0..columns).map(|i| row.get(i)).collect())
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

#[test]
fn migration_fills_only_provable_attempt_reach_without_granting_time() {
    let scratch = cortexkit_test_support::ScratchDir::new("last-observation-migration");
    let descriptor = dev_descriptor(
        "last-observation",
        scratch.path().join("state.db").to_string_lossy(),
    );
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 88).unwrap();
    let before = old.with_writer_for_test(|conn| {
        for (principal, id) in [
            ("reserved:broca", "shared"), ("reserved:other", "shared"),
            ("legacy", "own"), ("owner", "shared"), ("reserved:broca", "owner-only"),
            ("reserved:broca", "move"), ("reserved:broca", "reservation"),
            ("reserved:broca", "unrelated"), ("reserved:broca", "future-start"),
        ] { admission(conn, principal, id, "started"); }
        for state in ["reserved", "released", "expired", "finished"] {
            admission(conn, "reserved:broca", state, state);
            attempt(conn, "reserved:broca", "broca", state, false, true, "[]", RECENT);
        }
        conn.execute("UPDATE account_admission SET started_at=?1 WHERE admission_id='future-start'", [T]).unwrap();
        attempt(conn, "reserved:broca", "broca", "future-start", false, true, "[]", RECENT);
        attempt(conn, "reserved:broca", "broca", "shared", true, true, "[]", RECENT);
        attempt(conn, "legacy", "broca", "own", false, true, "[]", RECENT);
        // A matching decision cannot distinguish an owner report from a legacy
        // caller that supplied the decision id without an owner lookup.
        attempt(conn, "owner", "broca", "shared", true, true, "[]", T);
        attempt(conn, "owner", "broca", "owner-only", true, true, "[]", T);
        attempt(conn, "reserved:broca", "broca", "shared", false, false, "[]", T);
        attempt(conn, "reserved:broca", "broca", "move", false, false,
            r#"[{"effect":"served_account_mismatch"}]"#, RECENT);
        conn.execute("UPDATE account_admission SET account_id='B' WHERE admission_id='reservation'", []).unwrap();
        conn.execute("INSERT INTO account_continue (principal, selection_id, request_json, answer_json,
            run_id, admission_id, previous_decision_id, decision_id, reservation_id, valid_until_ms, used_at_ms)
            VALUES ('reserved:broca', 'offer', '{}', '{}', 'run', 'move', 'old', 'new', 'reservation', ?1, ?2)",
            params![START + 60000, START]).unwrap();
        assert_eq!(conn.query_row(LEFT_OUT, [], |r| r.get::<_, i64>(0)).unwrap(), 2);
        Ok(rows(conn, false))
    }).unwrap();
    drop(old);
    let store = SqliteRouteStateStore::open(&descriptor).unwrap();
    store
        .with_writer_for_test(|conn| {
            assert_eq!(
                conn.query_row(LEFT_OUT, [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                2
            );
            assert_eq!(
                rows(conn, true),
                before,
                "all pre-existing columns stay byte-identical"
            );
            let get = |principal: &str, id: &str| {
                conn.query_row(
            "SELECT last_observed_at FROM account_admission WHERE principal=?1 AND admission_id=?2",
            params![principal, id], |r| r.get::<_, Option<i64>>(0)).unwrap()
            };
            for (principal, id) in [
                ("reserved:broca", "shared"),
                ("legacy", "own"),
                ("reserved:broca", "move"),
                ("reserved:broca", "reservation"),
            ] {
                assert_eq!(get(principal, id), Some(RECENT), "{principal}/{id}");
            }
            for (principal, id) in [
                ("reserved:other", "shared"),
                ("owner", "shared"),
                ("reserved:broca", "owner-only"),
                ("reserved:broca", "unrelated"),
            ] {
                assert_eq!(get(principal, id), Some(START), "{principal}/{id}");
            }
            assert_eq!(get("reserved:broca", "future-start"), Some(T));
            for state in ["reserved", "released", "expired", "finished"] {
                assert_eq!(get("reserved:broca", state), None, "{state}");
            }
            Ok(())
        })
        .unwrap();
}
