use super::*;
use std::time::Duration;
mod typed_error_tests;

fn fixture(name: &str) -> (cortexkit_test_support::ScratchDir, SqliteRouteStateStore) {
    let scratch = cortexkit_test_support::ScratchDir::new(name);
    let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
        name,
        scratch.path().join("state.db").to_string_lossy(),
    ))
    .unwrap();
    store
        .store
        .with_conn(|conn| conn.execute_batch("CREATE TABLE writer_test(id TEXT PRIMARY KEY)"))
        .unwrap();
    (scratch, store)
}

fn queued<T: Send + 'static>(
    mut f: impl FnMut(&AccountTx<'_>) -> RouteStateResult<T> + Send + 'static,
) -> (Queued, mpsc::Receiver<RouteStateResult<T>>) {
    let (reply, receiver) = mpsc::channel();
    (
        Queued {
            enqueued: Instant::now(),
            initialize_only: false,
            command:
                Box::new(
                    TypedCommand {
                        run: move |tx: &AccountTx<'_>,
                                   _: Option<&mut (dyn crate::AccountWorkingState + 'static)>| {
                            f(tx)
                        },
                        result: None,
                        reply,
                        timing: Arc::new(Mutex::new(StoreStageTiming::default())),
                    },
                ),
        },
        receiver,
    )
}

fn insert(tx: &AccountTx<'_>, id: &str) -> RouteStateResult<String> {
    tx.conn
        .execute("INSERT INTO writer_test VALUES (?1)", [id])
        .map_err(err)?;
    tx.conn
        .query_row(
            "SELECT group_concat(id, ',') FROM (SELECT id FROM writer_test ORDER BY id)",
            [],
            |row| row.get(0),
        )
        .map_err(err)
}

#[test]
fn outside_writer_counter_detects_each_account_table_bypass() {
    let (_scratch, store) = fixture("account-writer-bypass");
    // Disable constraints and trigger fan-out only on this disposable database:
    // each insert must exercise the hook for its own real schema table, rather
    // than accidentally being counted through another table's trigger.
    store
        .store
        .with_conn(|conn| {
            conn.execute_batch("PRAGMA foreign_keys=OFF; PRAGMA ignore_check_constraints=ON;")?;
            conn.set_db_config(
                rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER,
                false,
            )?;
            Ok(())
        })
        .unwrap();
    for table in connection_store::ACCOUNT_TABLES {
        let before = store.outside_account_writer_count_for_test();
        store
            .store
            .with_conn(|conn| {
                let mut statement = conn.prepare(&format!("PRAGMA table_info({table})"))?;
                let columns = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                assert!(!columns.is_empty(), "missing account table {table}");
                let names = columns
                    .iter()
                    .map(|(name, _)| format!("\"{name}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                let values = columns
                    .iter()
                    .map(|(_, ty)| {
                        if ty.contains("INT") || ty.contains("REAL") {
                            "1"
                        } else {
                            "'bypass'"
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                conn.execute(
                    &format!("INSERT OR REPLACE INTO {table} ({names}) VALUES ({values})"),
                    [],
                )?;
                Ok(())
            })
            .unwrap_or_else(|error| panic!("bypass {table}: {error}"));
        assert!(
            store.outside_account_writer_count_for_test() > before,
            "uncounted bypass of {table}"
        );
        let before_delete = store.outside_account_writer_count_for_test();
        store
            .store
            .with_conn(|conn| conn.execute(&format!("DELETE FROM {table}"), []))
            .unwrap();
        assert!(
            store.outside_account_writer_count_for_test() > before_delete,
            "uncounted whole-table delete of {table}"
        );
    }
}

#[test]
fn batch_scope_is_not_counted_and_projection_never_starts_a_writer() {
    let (_scratch, store) = fixture("account-writer-scope");
    let before = store.outside_account_writer_count_for_test();
    store.account_command(AccountCommandKind::Maintenance, |tx| {
        tx.conn.execute("INSERT INTO account_selection_salt(singleton,salt,created_at) VALUES (1,'batch',1)", []).map_err(err)?;
        Ok(())
    }).unwrap();
    assert_eq!(store.outside_account_writer_count_for_test(), before);
    let projection = store.private_account_copy_for_test(1).unwrap();
    projection
        .account_command(AccountCommandKind::Tick, |tx| tx.tick(1))
        .unwrap();
    assert!(projection.account_writer.get().is_none());
    assert_eq!(
        projection.account_command_count_for_test(AccountCommandKind::Tick),
        0
    );
}

#[test]
fn held_commit_groups_64_admits_into_bounded_batches_and_idle_is_immediate() {
    let (_scratch, store) = fixture("account-writer-burst");
    let before = store.writer_commit_count_for_test();
    let acquisitions = store.writer_acquisition_count_for_test();
    let (entered, waiting) = mpsc::channel();
    let (release, held) = mpsc::channel();
    let first = store
        .enqueue_account_command(AccountCommandKind::Admit, move |tx| {
            insert(tx, "held")?;
            entered.send(()).unwrap();
            held.recv().unwrap();
            Ok(())
        })
        .unwrap();
    waiting
        .recv_timeout(Duration::from_secs(1))
        .expect("idle command starts without waiting for a batch");
    let replies: Vec<_> = (0..64)
        .map(|id| {
            store
                .enqueue_account_command(AccountCommandKind::Admit, move |tx| {
                    insert(tx, &id.to_string())
                })
                .unwrap()
        })
        .collect();
    assert!(
        matches!(first.0.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "no reply before COMMIT"
    );
    release.send(()).unwrap();
    first.wait().unwrap();
    for reply in replies {
        reply.wait().unwrap();
    }
    let commits = store.writer_commit_count_for_test() - before;
    assert_eq!(commits, 3);
    assert!(commits as f64 / 65.0 < 1.0);
    assert_eq!(
        store.writer_acquisition_count_for_test() - acquisitions,
        3,
        "each batch acquires the existing connection once"
    );
    assert_eq!(store.account_writer_batch_sizes_for_test(), [1, 32, 32]);
    assert!(store.account_command_queue_ns() > 0);
}

fn isolated_batch(fail: bool) -> (Vec<String>, Vec<String>) {
    let (_scratch, store) = fixture(if fail {
        "account-writer-savepoint-fail"
    } else {
        "account-writer-savepoint-control"
    });
    let mut batch = Vec::new();
    let mut replies = Vec::new();
    for id in ["a", "bad", "c", "d"] {
        if id == "bad" && !fail {
            continue;
        }
        let (command, receiver) = queued(move |tx| {
            let result = insert(tx, id)?;
            if id == "bad" {
                return Err(err("forced mid-write failure"));
            }
            Ok(result)
        });
        batch.push(command);
        replies.push((id, receiver));
    }
    store
        .store
        .with_conn(|conn| {
            write_batch(
                conn,
                &mut batch,
                &mut stateless(),
                &store.account_write_transactions,
            )
        })
        .unwrap();
    for command in batch {
        command.command.publish(None, StoreStageTiming::default());
    }
    let answers = replies
        .into_iter()
        .filter_map(|(id, reply)| {
            let reply = reply.recv().unwrap();
            if id == "bad" {
                assert!(reply.unwrap_err().to_string().contains("forced mid-write"));
                None
            } else {
                Some(reply.unwrap())
            }
        })
        .collect();
    let rows = store
        .store
        .with_read(|conn| {
            conn.prepare("SELECT id FROM writer_test ORDER BY id")?
                .query_map([], |row| row.get(0))?
                .collect()
        })
        .unwrap();
    (answers, rows)
}

#[test]
fn mid_write_failure_leaves_sibling_replies_equal_to_batch_without_failed_command() {
    let failed = isolated_batch(true);
    let control = isolated_batch(false);
    assert_eq!(failed, control);
    assert_eq!(failed.1, ["a", "c", "d"]);
}

#[test]
fn begin_commit_and_transaction_aborting_statement_fail_every_command() {
    for fault in ["begin", "commit", "statement"] {
        let (_scratch, store) = fixture(&format!("account-writer-{fault}-failure"));
        let mut batch = Vec::new();
        let mut replies = Vec::new();
        for id in ["a", "b", "c"] {
            let (command, reply) = queued(move |tx| {
                let result = insert(tx, id)?;
                if id == "b" && fault == "statement" {
                    tx.conn
                        .execute_batch("INSERT INTO abort_test VALUES (1)")
                        .map_err(err)?;
                }
                if id == "b" && fault == "commit" {
                    tx.conn
                        .execute_batch("INSERT INTO deferred_child VALUES (1)")
                        .map_err(err)?;
                }
                Ok(result)
            });
            batch.push(command);
            replies.push(reply);
        }
        let error = store.store.with_conn(|conn| {
            conn.execute_batch("CREATE TABLE abort_test(id); CREATE TRIGGER abort_write BEFORE INSERT ON abort_test BEGIN SELECT RAISE(ROLLBACK, 'forced abort'); END;
                CREATE TABLE deferred_parent(id PRIMARY KEY);
                CREATE TABLE deferred_child(parent REFERENCES deferred_parent(id) DEFERRABLE INITIALLY DEFERRED);")?;
            if fault == "begin" { conn.execute_batch("BEGIN IMMEDIATE")?; }
            let result = write_batch(conn, &mut batch, &mut stateless(), &store.account_write_transactions);
            if fault == "begin" { conn.execute_batch("ROLLBACK")?; }
            result
        }).unwrap_err().to_string();
        for command in batch {
            command
                .command
                .publish(Some(&error), StoreStageTiming::default());
        }
        for reply in replies {
            let error = reply
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("store_error") && error.contains("retry"),
                "{fault}: {error}"
            );
        }
        let count: i64 = store
            .store
            .with_read(|conn| {
                conn.query_row("SELECT count(*) FROM writer_test", [], |row| row.get(0))
            })
            .unwrap();
        assert_eq!(count, 0, "{fault} publishes no durable rows");
    }
}

#[test]
fn writer_death_wakes_pending_and_later_callers_and_fires_exit_hook() {
    let (_scratch, store) = fixture("account-writer-death");
    let (exit, exited) = mpsc::channel();
    let writer = AccountWriter::start(
        store.store.clone(),
        store.account_state.clone(),
        store.account_write_transactions.clone(),
        Arc::new(move || {
            exit.send(()).unwrap();
        }),
    );
    assert!(store.account_writer.set(writer).is_ok());
    let (entered, waiting) = mpsc::channel();
    let (kill, killed) = mpsc::channel();
    let dying = store
        .enqueue_account_command::<()>(AccountCommandKind::Admit, move |tx| {
            insert(tx, "uncommitted")?;
            entered.send(()).unwrap();
            killed.recv().unwrap();
            panic!("forced writer death");
        })
        .unwrap();
    waiting.recv_timeout(Duration::from_secs(1)).unwrap();
    let pending = store
        .enqueue_account_command(AccountCommandKind::Admit, |tx| insert(tx, "pending"))
        .unwrap();
    kill.send(()).unwrap();
    for reply in [
        dying
            .0
            .recv_timeout(Duration::from_secs(1))
            .map(|r| r.map(|_| String::new())),
        pending.0.recv_timeout(Duration::from_secs(1)),
    ] {
        assert!(matches!(reply, Err(mpsc::RecvTimeoutError::Disconnected)));
    }
    exited.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(store
        .account_command(AccountCommandKind::Admit, |tx| insert(tx, "later"))
        .unwrap_err()
        .to_string()
        .contains("retry"));
}

#[test]
fn dropped_committed_reply_replays_and_shutdown_drains_without_uncommitted_answers() {
    let (scratch, store) = fixture("account-writer-reply-loss");
    let reply = store
        .enqueue_account_command(AccountCommandKind::Admit, |tx| insert(tx, "once"))
        .unwrap();
    drop(reply);
    let replay = store
        .account_command(AccountCommandKind::Replay, |tx| {
            tx.conn
                .query_row("SELECT id FROM writer_test WHERE id='once'", [], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(err)
        })
        .unwrap();
    assert_eq!(replay, "once");
    let committed: i64 = store
        .store
        .with_read(|conn| {
            conn.query_row(
                "SELECT count(*) FROM writer_test WHERE id='once'",
                [],
                |row| row.get(0),
            )
        })
        .unwrap();
    assert_eq!(
        committed, 1,
        "the reply describes a row visible outside the batch"
    );
    let pending = store
        .enqueue_account_command(AccountCommandKind::Admit, |tx| insert(tx, "shutdown"))
        .unwrap();
    drop(store);
    assert!(pending.wait().unwrap().contains("shutdown"));
    let restarted = SqliteRouteStateStore::open(&crate::dev_descriptor(
        "account-writer-reply-loss",
        scratch.path().join("state.db").to_string_lossy(),
    ))
    .unwrap();
    let committed: i64 = restarted
        .store
        .with_read(|conn| {
            conn.query_row(
                "SELECT count(*) FROM writer_test WHERE id IN ('once','shutdown')",
                [],
                |row| row.get(0),
            )
        })
        .unwrap();
    assert_eq!(committed, 2, "shutdown acknowledged only committed rows");
}

fn retry_grant(tx: &AccountTx<'_>) -> RouteStateResult<String> {
    let standing: Option<String> = tx.conn.query_row(
        "SELECT answer_json FROM routing_v1_admission WHERE principal='owner' AND admission_id='once'",
        [], |row| row.get(0),
    ).optional().map_err(err)?;
    if let Some(standing) = standing {
        return Ok(standing);
    }
    insert(tx, "probe-claim")?;
    let recorded = V1Recorded {
        body_json: "request".into(),
        answer_json: "committed-reply".into(),
    };
    tx.v1_record_admission("owner", "once", &recorded, 1)?;
    Ok(recorded.answer_json)
}

#[test]
fn crash_child() {
    let Ok(path) = std::env::var("STRIATUM_TEST_ACCOUNT_WRITER_DB") else {
        return;
    };
    let store =
        SqliteRouteStateStore::open(&crate::dev_descriptor("account-writer-crash", path)).unwrap();
    store
        .account_command(AccountCommandKind::Admit, retry_grant)
        .unwrap();
    panic!("crash injection was not reached");
}

#[test]
fn crash_before_commit_retries_fresh_and_after_commit_replays_without_second_claim() {
    for point in ["before_commit", "after_commit"] {
        let (scratch, store) = fixture("account-writer-crash");
        drop(store);
        let path = scratch.path().join("state.db");
        let output = std::process::Command::new(crate::test_exe::current_test_exe().unwrap())
            .args([
                "--exact",
                "account_writer::tests::crash_child",
                "--nocapture",
            ])
            .env("STRIATUM_TEST_ACCOUNT_WRITER_DB", &path)
            .env("STRIATUM_TEST_ACCOUNT_WRITER_CRASH", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "account-writer-crash",
            path.to_string_lossy(),
        ))
        .unwrap();
        let existing = store.v1_admission("owner", "once").unwrap();
        assert_eq!(existing.is_some(), point == "after_commit");
        let rows: i64 = store
            .store
            .with_read(|conn| {
                conn.query_row("SELECT count(*) FROM writer_test", [], |row| row.get(0))
            })
            .unwrap();
        assert_eq!(rows, i64::from(point == "after_commit"));
        let reply = store
            .account_command(
                if existing.is_some() {
                    AccountCommandKind::Replay
                } else {
                    AccountCommandKind::Admit
                },
                retry_grant,
            )
            .unwrap();
        assert_eq!(reply, "committed-reply");
        let rows: i64 = store
            .store
            .with_read(|conn| {
                conn.query_row("SELECT count(*) FROM writer_test", [], |row| row.get(0))
            })
            .unwrap();
        assert_eq!(rows, 1, "retry claims exactly once");
        assert_eq!(
            store
                .v1_admission("owner", "once")
                .unwrap()
                .unwrap()
                .answer_json,
            reply
        );
    }
}

/// How long one background Tick holds the account writer when the admission
/// history is as large as the live store's (about 221k rows). Every routing
/// write queues behind this batch, and before Tick moved off the async runtime
/// the runtime itself waited for it too. Also prints SQLite's plan for each
/// Tick statement so a full scan of the history shows up by name.
#[test]
#[ignore = "live-sized Tick writer-hold measurement; run in release with --ignored --nocapture"]
fn live_sized_tick_writer_hold() {
    let rows: i64 = std::env::var("STRIATUM_TICK_ADMISSIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(221_642);
    let (_scratch, store) = fixture("account-writer-tick-live-sized");
    let now = 1_900_000_000_000_i64;
    store
        .store
        .with_conn(|conn| {
            // Synthetic history names no real decisions or pins; only its size
            // and shape matter for the timing.
            conn.execute_batch("PRAGMA foreign_keys=OFF")?;
            conn.execute(
                "INSERT INTO account_admission (principal, admission_id, request_fingerprint,
                   request_mode, decision_id, conversation_hash, provider, model, account_id,
                   credential_id, pin_id, pin_generation, state, charged, request_bytes,
                   request_bytes_estimated, committed_at, admission_valid_until,
                   lease_valid_until, reply_json, run_id, started_at, finished_at, released_at,
                   model_decision_id)
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?1)
                 SELECT 'reserved:runner-'||(i%8), 'adm-'||i, 'fp', 'admit', 'dec-'||i, 'conv',
                   'anthropic', 'pick', 'acct-'||(i%20), 'cred-'||(i%40), 'pin', 1,
                   CASE WHEN i%1000=0 THEN 'reserved' ELSE 'finished' END, 0, 0, 0,
                   ?2-i*1000, ?2-i*1000, ?2-i*1000,
                   json_object('pad', hex(randomblob(400))), 'run-'||i, ?2-i*1000, ?2-i*1000,
                   NULL, 'mdec-'||i
                 FROM n",
                rusqlite::params![rows, now],
            )?;
            conn.execute(
                "INSERT INTO account_continue (principal, selection_id, request_json, answer_json,
                   run_id, admission_id, previous_decision_id, decision_id, reservation_id,
                   valid_until_ms) VALUES ('reserved:runner-1', 'sel', '{}', '{}', 'run-1',
                   'adm-1', 'dec-0', 'dec-cont', 'adm-1001', ?1)",
                [now + 60_000],
            )?;
            // Live stores hold about one probe authorization at a time. This
            // one belongs to an admission that already started, so Tick finds
            // nothing to release and every round does the same work.
            conn.execute(
                "INSERT INTO fence_probe_authorization (owner, kind, origin_key, evidence,
                   evidence_at, claimed_at, claim_generation)
                 VALUES (json_array('account_admission','reserved:runner-1','adm-1'),
                   'account', '[\"fence\"]', '{}', ?1, ?1, 1)",
                [now],
            )?;
            conn.execute_batch("PRAGMA foreign_keys=ON")?;
            Ok(())
        })
        .unwrap();
    store
        .store
        .with_conn(|conn| {
            for sql in [
                crate::fence_rechecks::EXPIRED_ADMISSION_PROBES.to_owned(),
                format!("SELECT EXISTS({})", crate::fence_rechecks::EXPIRED_ADMISSION_PROBES),
                "UPDATE account_admission SET state='released', released_at=?1 WHERE state='reserved' AND (principal,admission_id) IN (SELECT principal,reservation_id FROM account_continue WHERE used_at_ms IS NULL AND released_at_ms IS NULL AND valid_until_ms<=?1)".to_owned(),
                "UPDATE account_continue SET released_at_ms=?1 WHERE used_at_ms IS NULL AND released_at_ms IS NULL AND valid_until_ms<=?1".to_owned(),
            ] {
                let mut plan = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
                let steps = plan
                    .query_map([now], |row| row.get::<_, String>(3))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                eprintln!("plan {:?}: {steps:?}", &sql[..60]);
            }
            Ok(())
        })
        .unwrap();
    let mut held = Vec::new();
    for round in 0..5 {
        let started = Instant::now();
        store
            .account_command(AccountCommandKind::Tick, move |tx| tx.tick(now + round))
            .unwrap();
        held.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    eprintln!("tick writer hold over {rows} admissions, ms per round: {held:.1?}");
    // Every selection's ClassContext::load previews the same lookup on a reader.
    let mut preview = Vec::new();
    for _ in 0..5 {
        let started = Instant::now();
        store
            .account_read(|tx| tx.has_expired_admission_probes(now))
            .unwrap();
        preview.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    eprintln!("selection preview over {rows} admissions, ms per round: {preview:.1?}");
}

/// A writer state with no module state installed, as a store without the
/// account state bridge runs.
fn stateless() -> crate::account_state::WriterAccountState {
    crate::account_state::WriterAccountState::new(
        Arc::new(std::sync::OnceLock::new()),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
}
