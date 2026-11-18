//! The admission group (admissions, affinities, demand, continuations) is
//! served from the in-memory account state. These tests drive the real
//! `route.account_for` (admit and continue) and `route.account_observe` frames
//! while every statement SQLite runs on the store's writer and pooled readers
//! is traced, and assert that none of them reads those tables.
//!
//! One kind of read is sanctioned, the keyed exception: a lookup by key of an
//! admission or continuation the memory state does not hold, because it is
//! closed or absent (the state keeps open rows only) or because it is a stored
//! body. Each admission or observation may do at most one, and none when its
//! admission is open. A completed report may look its decision's admissions up
//! by model decision id.

use super::*;

/// Run `call` and return the group reads it caused.
async fn traced<T>(
    reads: &striatum_store::group_reads::GroupReads,
    call: impl std::future::Future<Output = T>,
) -> (T, Vec<String>, Vec<String>) {
    reads.take();
    let value = call.await;
    let (ordinary, keyed) = reads.take();
    (value, ordinary, keyed)
}

fn assert_reads(op: &str, ordinary: &[String], keyed: &[String], max_keyed: usize) {
    assert!(
        ordinary.is_empty(),
        "{op} read the admission group from SQL: {ordinary:#?}"
    );
    assert!(
        keyed.len() <= max_keyed,
        "{op} made {} keyed-exception reads, at most {max_keyed} allowed: {keyed:#?}",
        keyed.len()
    );
}

#[tokio::test]
async fn account_for_continue_and_observe_read_no_admission_group_sql() {
    let (subject, consumer, runner, request) = paused().await;
    let reads = subject
        .harness()
        .store()
        .trace_admission_group_reads_for_test()
        .unwrap();

    // The instrument sees an ordinary read on a pooled reader and on the writer.
    subject
        .harness()
        .store()
        .account_read(|tx| tx.affinity("p", "h", "x"))
        .unwrap();
    subject
        .harness()
        .store()
        .account_transaction(|tx| tx.affinity("p", "h", "x"))
        .unwrap();
    let (ordinary, _) = reads.take();
    assert_eq!(ordinary.len(), 2, "{ordinary:#?}");

    let admit = json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
        "admission_id":"second","conversation_key":"second-conversation",
        "parent_conversation_key":"conversation","initial_input_bytes":1024});
    let (reply, ordinary, keyed) = traced(reads, runner.call("route.account_for", &admit)).await;
    let admitted = result(reply);
    assert_reads("fresh admission", &ordinary, &keyed, 1);

    let (reply, ordinary, keyed) = traced(reads, runner.call("route.account_for", &admit)).await;
    assert_eq!(result(reply), admitted);
    assert_reads("replay of an open admission", &ordinary, &keyed, 0);

    let (reply, ordinary, keyed) = traced(
        reads,
        runner.call(
            "route.account_observe",
            &json!({"event_id":"second-started","kind":"admission_started",
                "admission_id":"second","run_id":"second-run"}),
        ),
    )
    .await;
    result(reply);
    assert_reads("in-run observation", &ordinary, &keyed, 0);

    let (reply, ordinary, keyed) = traced(
        reads,
        runner.call(
            "route.account_observe",
            &json!({"event_id":"second-served","kind":"attempt","admission_id":"second",
            "served_account_id":admitted["account"]["account_id"],
            "credential_id":admitted["account"]["credential_id"],
            "attempt_outcome":{"status":"served"}}),
        ),
    )
    .await;
    result(reply);
    assert_reads("in-run typed success", &ordinary, &keyed, 0);

    let (reply, ordinary, keyed) = traced(reads, runner.call("route.account_for", &request)).await;
    let offer = result(reply);
    assert_eq!(offer["kind"], "offer", "{offer}");
    assert_reads("continuation offer", &ordinary, &keyed, 1);

    let (reply, ordinary, keyed) = traced(reads, runner.call("route.account_for", &request)).await;
    assert_eq!(result(reply), offer);
    assert_reads("continuation replay", &ordinary, &keyed, 1);

    let (reply, ordinary, keyed) = traced(
        reads,
        runner.call(
            "route.account_observe",
            &offer_observation(&offer, "admission_started", "offer-used"),
        ),
    )
    .await;
    result(reply);
    assert_reads("observation that uses an offer", &ordinary, &keyed, 0);

    let mut finished = offer_observation(&offer, "run_finished", "run-finished");
    finished["run_id"] = json!("run");
    let (reply, ordinary, keyed) =
        traced(reads, runner.call("route.account_observe", &finished)).await;
    result(reply);
    assert_reads("observation that finishes the run", &ordinary, &keyed, 0);

    // The run's original is finished now, so it is no longer in memory.
    let (reply, ordinary, keyed) = traced(
        reads,
        runner.call(
            "route.account_observe",
            &json!({"event_id":"after-finish","kind":"attempt",
                "admission_id":"admission","run_id":"run",
                "served_account_id":request["exhausted"]["account"],
                "credential_id":request["exhausted"]["credential_id"],
                "attempt_outcome":{"status":"served_empty"}}),
        ),
    )
    .await;
    result(reply);
    assert_reads(
        "observation naming a closed admission",
        &ordinary,
        &keyed,
        1,
    );

    // A completed report heals the fences (temporary bans on routing to a
    // credential after it failed) that its success disproves. It finds the
    // admission its run used, which has finished by now, by decision and run.
    let (reply, ordinary, keyed) = traced(
        reads,
        consumer.call(
            "route.report",
            &json!({"decision_id":request["previous_decision_id"],"outcome":"completed",
                "usage":{"input_tokens":100,"output_tokens":20}}),
        ),
    )
    .await;
    result(reply);
    assert_reads("completed report", &ordinary, &keyed, 2);
    assert!(
        keyed
            .iter()
            .any(|sql| sql.contains("model_decision_id = ?1 AND run_id = ?2")),
        "the report did not reach its run's admission lookup: {keyed:#?}"
    );
}

/// Run one `route.account_for` the way the frame handler does, outside the
/// async harness, so a test hook can run a competing request inside it.
fn admit_directly(
    store: &crate::SharedStore,
    credentials: &crate::SharedBrocaCredentials,
    config: std::sync::Arc<crate::config_overrides::LiveConfig>,
    principal: &str,
    request: &Value,
    now: i64,
) -> crate::account_selection::OpReply {
    let policy = config.allocator_policy(request["provider"].as_str().unwrap());
    let ctx = crate::account_selection::Ctx {
        store,
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: crate::config_overrides::AccountSelectionFlags { enabled: true },
        policy: &policy,
        now_ms: now,
        classes: Some(
            crate::account_selection::ClassContext::load(
                store,
                credentials,
                config,
                crate::ACCOUNT_QUOTA.get(),
                now,
            )
            .unwrap(),
        ),
    };
    crate::account_selection::handle(
        crate::account_selection::ACCOUNT_FOR_OP,
        &serde_json::to_vec(
            &json!({"method":crate::account_selection::ACCOUNT_FOR_OP,"params":request}),
        )
        .unwrap(),
        principal,
        &ctx,
    )
}

struct Direct {
    fixture: crate::routing_v1::request_cpu_bench::RequestFixture,
    principal: String,
    module: String,
    now: i64,
}

impl Direct {
    fn new() -> Self {
        use crate::routing_v1::request_cpu_bench as bench;
        let fixture = bench::representative_fixture(bench::FixtureCounts {
            subscription_rules: 7,
            exclusion_rules: 5,
            runner_route_rules: 3,
            catalog_facts: 12,
            listed_credentials: 6,
        });
        let module = fixture.catalog.runners[0]
            .module
            .trim_start_matches("reserved:")
            .to_owned();
        Self {
            principal: format!("reserved:{module}"),
            module,
            fixture,
            now: bench::FIXTURE_NOW as i64,
        }
    }

    fn request(&self, admission: &str, conversation: &str) -> Value {
        json!({"provider":"cpu-provider-0","model":"model-0","conversation_key":conversation,
            "admission_id":admission,"caller":{"runner":self.module}})
    }

    /// A closure that admits `request` as this principal, for test hooks.
    fn admitter(
        &self,
        request: Value,
    ) -> impl Fn() -> crate::account_selection::OpReply + Clone + Send + 'static {
        let (store, credentials, config) = (
            self.fixture.store().clone(),
            self.fixture.credentials(),
            self.fixture.operator_config(),
        );
        let (principal, now) = (self.principal.clone(), self.now);
        move || {
            admit_directly(
                &store,
                &credentials,
                config.clone(),
                &principal,
                &request,
                now,
            )
        }
    }
}

fn admitted(reply: crate::account_selection::OpReply) -> Value {
    match reply {
        crate::account_selection::OpReply::Result(result) => result,
        other => panic!("expected an admission, got {other:?}"),
    }
}

/// The writer command checks an admission id that is not open in memory
/// against SQL by key before allocating. If an identical request admits and
/// closes the same admission id after this one passed its pre-writer checks,
/// this one replays that closed admission instead of admitting again.
#[test]
fn a_late_duplicate_replays_an_admission_closed_meanwhile() {
    let _clock = crate::InjectedClock::set(crate::routing_v1::request_cpu_bench::FIXTURE_NOW);
    let direct = Direct::new();
    let admit = direct.admitter(direct.request("raced", "raced"));
    let competing = std::sync::Arc::new(std::sync::Mutex::new(None));
    {
        let (admit, competing) = (admit.clone(), competing.clone());
        let (store, principal, now) = (
            direct.fixture.store().clone(),
            direct.principal.clone(),
            direct.now,
        );
        crate::account_selection::admission::after_roster_prepared("raced", move || {
            let reply = admit();
            crate::account_writer::execute(
                &store,
                striatum_store::AccountCommandKind::Release,
                move |tx| tx.mark_admission_released(&principal, "raced", now),
            )
            .unwrap();
            *competing.lock().unwrap() = Some(reply);
        });
    }
    let late = admitted(admit());
    let mut competing = admitted(competing.lock().unwrap().take().expect("the hook ran"));
    competing["admission"]["state"] = json!("released");
    assert_eq!(late, competing);
}

/// The same race on a reassignment, where the conversation's pin also moved
/// after the competing admission: without the keyed check before the pin
/// check, the late request answers `pin_generation_conflict` and never
/// reaches an INSERT, instead of replaying.
#[test]
fn a_late_duplicate_reassignment_replays_instead_of_reporting_a_pin_conflict() {
    let _clock = crate::InjectedClock::set(crate::routing_v1::request_cpu_bench::FIXTURE_NOW);
    let direct = Direct::new();
    let base = admitted(direct.admitter(direct.request("base", "moved"))());
    let mut request = direct.request("reassigned", "moved");
    request["mode"] = json!("reassign");
    request["expected_pin"] = base["pin"].clone();
    let admit = direct.admitter(request);
    let competing = std::sync::Arc::new(std::sync::Mutex::new(None));
    {
        let (admit, competing) = (admit.clone(), competing.clone());
        let (store, principal, now) = (
            direct.fixture.store().clone(),
            direct.principal.clone(),
            direct.now,
        );
        crate::account_selection::admission::after_roster_prepared("reassigned", move || {
            let reply = admit();
            crate::account_writer::execute(
                &store,
                striatum_store::AccountCommandKind::Observe,
                move |tx| {
                    tx.mark_admission_finished(&principal, "reassigned", now)?;
                    // A later reassignment moves the conversation's pin on.
                    let admission = tx.admission(&principal, "reassigned")?.unwrap();
                    let mut moved = tx
                        .affinity(
                            &principal,
                            &admission.conversation_hash,
                            &admission.provider,
                        )?
                        .unwrap();
                    let current = (moved.pin_id.clone(), moved.pin_generation);
                    moved.pin_generation += 1;
                    assert!(tx.put_affinity(&moved, Some((&current.0, current.1)))?);
                    Ok(())
                },
            )
            .unwrap();
            *competing.lock().unwrap() = Some(reply);
        });
    }
    let late = admitted(admit());
    let mut competing = admitted(competing.lock().unwrap().take().expect("the hook ran"));
    competing["admission"]["state"] = json!("finished");
    assert_eq!(late, competing);
}

/// A request refused before its writer command (here every credential is
/// route-fenced) must still replay a closed admission of the same id. The
/// published state holds only open admissions, so this refusal path checks
/// the id by key before answering.
#[test]
fn a_request_refused_before_the_writer_replays_its_closed_admission() {
    use striatum_store::AccountCommandKind::Admit;
    let _clock = crate::InjectedClock::set(crate::routing_v1::request_cpu_bench::FIXTURE_NOW);
    let direct = Direct::new();
    let store = direct.fixture.store().clone();
    let mut first = admitted(direct.admitter(direct.request("closed", "closed"))());
    let (principal, now) = (direct.principal.clone(), direct.now);
    crate::account_writer::execute(
        &store,
        striatum_store::AccountCommandKind::Release,
        move |tx| tx.mark_admission_released(&principal, "closed", now),
    )
    .unwrap();
    for i in 0..6 {
        store
            .record_account_route_fence(&striatum_store::AccountRouteFence {
                provider: "cpu-provider-0".into(),
                credential_id: format!("oauth:cpu-{i}"),
                model: "model-0".into(),
                until_ms: now + 3_600_000,
                recorded_at: now,
                decision_id: format!("fence-{i}"),
            })
            .unwrap();
    }
    // A new id is refused without reaching the writer.
    let admits = store.account_command_count_for_test(Admit);
    assert!(matches!(
        direct.admitter(direct.request("unclaimed", "unclaimed"))(),
        crate::account_selection::OpReply::Error { .. }
    ));
    assert_eq!(store.account_command_count_for_test(Admit), admits);
    let replayed = admitted(direct.admitter(direct.request("closed", "closed"))());
    first["admission"]["state"] = json!("released");
    assert_eq!(replayed, first);
}

/// A command whose working state disagrees with SQL fails, rolls back and
/// makes the writer reload, so the next request on that conversation is
/// served instead of failing on the same mismatch forever.
#[test]
fn a_state_and_sql_mismatch_fails_one_command_and_the_next_one_is_served() {
    let _clock = crate::InjectedClock::set(crate::routing_v1::request_cpu_bench::FIXTURE_NOW);
    let direct = Direct::new();
    admitted(direct.admitter(direct.request("first", "diverged"))());
    // A write the state never saw, as a bug or an outside writer would make.
    direct
        .fixture
        .store()
        .with_writer_for_test(|conn| {
            conn.execute(
                "UPDATE account_affinity SET pin_generation = pin_generation + 1000",
                [],
            )?;
            conn.execute(
                "UPDATE account_pin SET pin_generation = pin_generation + 1000",
                [],
            )
        })
        .unwrap();
    let _ = direct.admitter(direct.request("second", "diverged"))();
    admitted(direct.admitter(direct.request("third", "diverged"))());
}

/// Two identical continuation requests that both pass their pre-writer
/// checks before either is recorded: the second must replay the first's
/// stored answer even when that answer made no offer (here a refusal), which
/// the memory state never holds.
#[tokio::test]
async fn a_duplicate_continuation_replays_a_recorded_refusal() {
    let (subject, _consumer, runner, mut request) = paused().await;
    request["selection_id"] = json!("duplicate");
    request["previous_decision_id"] = json!("no-such-decision");
    let harness = subject.harness();
    let store = harness.store().clone();
    let queued = store.account_command_count_for_test(striatum_store::AccountCommandKind::Repair);
    // Hold the writer until both requests have queued their commands, so both
    // finish every check before the first answer is recorded.
    let (locked, holding) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        store
            .with_writer_for_test(|_| {
                locked.send(()).unwrap();
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                while store
                    .account_command_count_for_test(striatum_store::AccountCommandKind::Repair)
                    < queued + 2
                    && std::time::Instant::now() < deadline
                {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Ok(())
            })
            .unwrap();
    });
    holding.recv().unwrap();
    let mut pending = tokio::task::JoinSet::new();
    for _ in 0..2 {
        let (harness, frame) = (
            harness.clone(),
            runner.request_frame("route.account_for", &request),
        );
        pending.spawn(async move {
            let reply = harness.handle(frame).await;
            (
                reply.header.ty,
                serde_json::from_slice::<Value>(&reply.body).unwrap(),
            )
        });
    }
    let mut replies = Vec::new();
    while let Some(reply) = pending.join_next().await {
        replies.push(reply.unwrap());
    }
    holder.join().unwrap();
    for (ty, body) in &replies {
        assert_eq!(*ty, FrameType::Response, "{body}");
    }
    assert_eq!(replies[0].1, replies[1].1);
    assert_eq!(replies[0].1["result"]["kind"], "refuse", "{}", replies[0].1);
}
