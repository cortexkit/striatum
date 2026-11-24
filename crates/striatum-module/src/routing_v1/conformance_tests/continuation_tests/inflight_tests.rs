use super::*;

#[tokio::test]
async fn retention_after_recent_report_use_keeps_the_original_observation_path() {
    const START: u64 = 1_790_683_200_000;
    const NOW: u64 = START + 40 * 24 * 60 * 60_000;
    let _clock = crate::InjectedClock::set(START);
    let (subject, consumer, runner, mut request) = paused().await;
    let _use_clock = crate::InjectedClock::set(NOW - 60_000);
    subject
        .configure_continue_credentials(&fixture::continue_credentials())
        .await
        .unwrap();
    request["exhausted"]["observed_at_ms"] = json!(NOW - 60_000);
    request["exhausted"]["resets_at_ms"] = json!(NOW + 240_000);
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
    result(consumer.call("route.report", &json!({"decision_id":offer["decision_id"],"outcome":"completed","usage":{"input_tokens":100,"output_tokens":20}})).await);
    let assert_counts = |harness: &Harness, now, expected, stage| {
        let state = crate::account_state::snapshot(harness.store()).unwrap();
        let scope = crate::account_state::InflightScope::Account;
        assert_eq!(
            state
                .inflight
                .count(fixture::SUBSCRIPTION_PROVIDER, "account-1", scope, now),
            0
        );
        assert_eq!(
            state.inflight.count(
                fixture::SUBSCRIPTION_PROVIDER,
                offer["account"].as_str().unwrap(),
                scope,
                now
            ),
            expected,
            "{stage} at {now}"
        );
    };
    let _pass_clock = crate::InjectedClock::set(NOW);
    assert_counts(&subject.harness(), NOW as i64, 1, "before retention");
    let harness = subject.harness();
    let report = tokio::task::spawn_blocking(move || {
        let _clock = crate::InjectedClock::set(NOW);
        crate::retention::run_retention_pass(
            harness.store(),
            &crate::retention::RetentionWindows::default(),
            NOW as i64,
            &crate::retention::PassBudget::default(),
        )
    })
    .await
    .unwrap();
    assert_eq!(
        report.account_admissions, 0,
        "a moved run still reports under its old admission"
    );
    assert_counts(&subject.harness(), NOW as i64, 1, "after retention");
    drop(consumer);
    drop(runner);
    let harness = subject.harness.into_inner().unwrap();
    let previous = request["previous_decision_id"].as_str().unwrap().to_owned();
    let (harness, runner_module) = tokio::task::spawn_blocking(move || {
        let _clock = crate::InjectedClock::set(NOW);
        let harness = upgrade_harness(harness);
        crate::account_state::initialize_at(harness.store(), NOW as i64).unwrap();
        let runner_module = harness
            .store()
            .v1_decision(&previous)
            .unwrap()
            .unwrap()
            .runner_module;
        (harness, runner_module)
    })
    .await
    .unwrap();
    assert_counts(&harness, NOW as i64, 1, "after restart");
    let runner = harness.bind(reserved(&runner_module), None).await;
    let _later = crate::InjectedClock::set(NOW + 31 * 60_000);
    let later = (NOW + 31 * 60_000) as i64;
    assert_counts(&harness, later, 0, "lapsed");
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(&offer, "attempt", "retained-attempt"),
            )
            .await,
    );
    assert_counts(&harness, later, 1, "late attempt");
    result(
        runner
            .call(
                "route.account_observe",
                &offer_observation(&offer, "run_finished", "retained-finish"),
            )
            .await,
    );
    assert_counts(&harness, later, 0, "finished");
}

#[tokio::test]
async fn report_delivery_succeeds_left_account_and_bound_attempt_refreshes_moved_run() {
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
    assert!(bound);
    assert_eq!(effect, "[]");
}
