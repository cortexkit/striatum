use super::*;

#[tokio::test]
async fn report_delivery_succeeds_left_account_and_unbound_attempt_refreshes_moved_run() {
    const NOW: u64 = 1_790_683_200_000;
    let _clock = crate::InjectedClock::set(NOW);
    let (subject, consumer, runner, request) = paused().await;
    let source_reads = crate::account_state::continuation_source_reads();
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer");
    let assert_counts = |a, b, now| {
        let state = crate::account_state::snapshot(subject.harness().store()).unwrap();
        let scope = crate::account_state::InflightScope::Account;
        assert_eq!(
            state
                .inflight
                .count(fixture::SUBSCRIPTION_PROVIDER, "account-1", scope, now),
            a
        );
        assert_eq!(
            state.inflight.count(
                fixture::SUBSCRIPTION_PROVIDER,
                offer["account"].as_str().unwrap(),
                scope,
                now
            ),
            b
        );
    };
    assert_counts(1, 1, NOW as i64);
    result(consumer.call("route.report", &json!({"decision_id":offer["decision_id"],"outcome":"completed","usage":{"input_tokens":100,"output_tokens":20}})).await);
    assert_counts(0, 1, NOW as i64);
    assert_eq!(
        crate::account_state::continuation_source_reads() - source_reads,
        0,
        "the real continue and report handlers must resolve the source without a keyed read"
    );
    let _later = crate::InjectedClock::set(NOW + 31 * 60_000);
    assert_counts(0, 0, (NOW + 31 * 60_000) as i64);
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(&offer, "attempt", "late-moved-attempt"),
            )
            .await,
    );
    assert_counts(0, 1, (NOW + 31 * 60_000) as i64);
    let harness = subject.harness();
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let (bound, effect): (bool, String) = conn.query_row("SELECT bound, effects_json FROM account_observation WHERE event_id='late-moved-attempt'", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    assert!(!bound);
    assert!(effect.contains("served_account_mismatch"), "{effect}");
}
