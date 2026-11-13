//! The admission group (admissions, affinities, demand, continuations) is
//! served from the in-memory account state. These tests drive the real
//! `route.account_for` (admit and continue) and `route.account_observe` frames
//! while every statement SQLite runs on the store's writer and pooled readers
//! is traced, and assert that none of them reads those tables.
//!
//! One read is sanctioned: a primary-key lookup of an admission or
//! continuation that the memory state does not hold because it is closed or
//! absent (the state keeps open rows only). Each operation may do at most one
//! such keyed-exception read, and none when its admission is open.

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
    let (subject, _consumer, runner, request) = paused().await;
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
                "admission_id":"admission","run_id":"run"}),
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
}
