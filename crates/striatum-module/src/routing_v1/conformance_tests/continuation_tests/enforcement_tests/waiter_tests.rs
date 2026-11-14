//! Waiters on continue mode. A continue request ranks by the class of the
//! consumer on the decision it continues, and a run held for concurrency
//! waits on its replacement accounts' counts, never on the account it is
//! leaving. Every scenario runs under an injected clock.

use cortexkit_role_routing::declarations::ConsumerPriority;

use super::*;

/// The runs' selections are for `SUMMARY`, here a `blocking` consumer.
fn blocking_runs(harness: &Harness) {
    harness
        .edit_config(|config| config["consumer_priorities"] = json!({fixture::SUMMARY:"blocking"}));
}

/// The published live waiters at `now` as (class, accounts waited on), in
/// order.
fn waiters(harness: &Harness, now: u64) -> Vec<(ConsumerPriority, Vec<String>)> {
    let state = crate::account_state::snapshot(harness.store()).unwrap();
    let mut waiters: Vec<_> = state
        .waiters
        .live(now as i64)
        .map(|(_, waiter)| {
            (
                waiter.order,
                waiter.class,
                waiter
                    .counts
                    .iter()
                    .map(|count| count.account.clone())
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    waiters.sort_by_key(|(order, _, _)| *order);
    waiters
        .into_iter()
        .map(|(_, class, accounts)| (class, accounts))
        .collect()
}

/// A plain admission with no decision and no affinity, so it ranks as
/// `worker` and may take any eligible account: A, full with the runs, or B.
fn plain(id: &str) -> Value {
    json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
        "admission_id":id,"conversation_key":"plain"})
}

fn assert_refused(reply: &(FrameType, Value)) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(
        reply.1["detail"]["reason"], "concurrency_full",
        "{}",
        reply.1
    );
}

async fn finish(runner: &Route, id: &str) {
    result(
        runner
            .call(
                "route.account_observe",
                &json!({"event_id":format!("{id}-finished"),"kind":"run_finished",
                    "admission_id":id,"run_id":format!("{id}-run")}),
            )
            .await,
    );
}

#[tokio::test]
async fn a_held_blocking_run_waits_on_the_replacement_and_its_next_selection_gets_the_offer_first()
{
    let _clock = crate::InjectedClock::set(T);
    let (subject, consumer, runner, runs) = runs_on_a(2, None).await;
    let harness = subject.harness();
    caps(&harness, 1, true);
    blocking_runs(&harness);
    fill(&runner, B, 1, true).await;
    let held = result(runner.call("route.account_for", &runs[0]).await);
    assert_concurrency_hold(&held, T + RESERVED_CEILING_MS);
    // The run waits on B, its one replacement, not on A, which it is leaving.
    assert_eq!(
        waiters(&harness, T),
        [(ConsumerPriority::Blocking, vec![B.to_owned()])]
    );
    // A slot on B frees inside the wait: a lower class may not take it.
    finish(&runner, "fill-account-2-0").await;
    assert_eq!(count(&harness, B), 0);
    assert_refused(&runner.call("route.account_for", &plain("plain-1")).await);
    // A second run held later waits behind the first.
    let second = result(runner.call("route.account_for", &runs[1]).await);
    assert_concurrency_hold(&second, T + RESERVED_CEILING_MS);
    // A new selection_id keeps the first run's order and is offered B ahead
    // of the later blocking run and the worker.
    let mut again = runs[0].clone();
    again["selection_id"] = json!("selection-0-again");
    let offer = result(runner.call("route.account_for", &again).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
    assert_eq!(offer["account"], B, "{offer}");
    assert_eq!(count(&harness, B), 1);
    // The offer's reservation took the slot and ended the run's wait.
    assert_eq!(
        waiters(&harness, T),
        [
            (ConsumerPriority::Worker, vec![A.to_owned(), B.to_owned()]),
            (ConsumerPriority::Blocking, vec![B.to_owned()])
        ]
    );
    assert_refused(&runner.call("route.account_for", &plain("plain-2")).await);
    result(
        consumer
            .call(
                "route.report",
                &json!({"decision_id":offer["decision_id"],
                    "outcome":"completed","usage":{"input_tokens":100,"output_tokens":20}}),
            )
            .await,
    );
    assert_eq!(count(&harness, B), 1);
    assert_refused(&runner.call("route.account_for", &plain("plain-3")).await);
}

#[tokio::test]
async fn replaying_a_continue_hold_after_its_wait_ends_records_no_waiter() {
    let clock = crate::InjectedClock::set(T);
    let (subject, _, runner, runs) = runs_on_a(1, None).await;
    let harness = subject.harness();
    caps(&harness, 1, true);
    blocking_runs(&harness);
    fill(&runner, B, 1, true).await;
    let held = result(runner.call("route.account_for", &runs[0]).await);
    assert_concurrency_hold(&held, T + RESERVED_CEILING_MS);
    finish(&runner, "fill-account-2-0").await;
    drop(clock);
    // The wait lasts until 30 s after the hold's until_ms.
    let ends = T + RESERVED_CEILING_MS + 30_000;
    let before = crate::InjectedClock::set(ends - 1);
    assert_refused(&runner.call("route.account_for", &plain("plain-1")).await);
    drop(before);
    let _after = crate::InjectedClock::set(ends);
    let replayed = result(runner.call("route.account_for", &runs[0]).await);
    assert_eq!(replayed, held);
    // The stored answer is not a refusal: it brings back no waiter, so the
    // free slot goes to the next request.
    assert_eq!(
        waiters(&harness, ends),
        [(ConsumerPriority::Worker, vec![A.to_owned(), B.to_owned()])]
    );
    let admitted = result(runner.call("route.account_for", &plain("plain-2")).await);
    assert_eq!(admitted["account"]["account_id"], B, "{admitted}");
}

#[tokio::test]
async fn a_second_continuation_ranks_by_the_consumer_of_the_run_like_the_first() {
    let _clock = crate::InjectedClock::set(T);
    let (subject, consumer, runner, runs) = runs_on_a(1, None).await;
    let harness = subject.harness();
    caps(&harness, 1, true);
    blocking_runs(&harness);
    const C: &str = "account-6";
    let credentials: Vec<_> = fixture::continue_credentials()
        .into_iter()
        .filter(|credential| [A, B, C].contains(&credential.account.account_id.as_str()))
        .collect();
    subject
        .configure_continue_credentials(&credentials)
        .await
        .unwrap();
    // C is full with a started run of its own.
    let id = "fill-c";
    let filled = result(runner.call("route.account_for", &json!({"provider":fixture::SUBSCRIPTION_PROVIDER,
        "model":"model-c","admission_id":id,"conversation_key":id,
        "candidates":{"view_digest":"c-only","accounts":[{"account_id":C,"credential_id":"credential-6"}]}})).await);
    assert_eq!(filled["account"]["account_id"], C, "{filled}");
    result(
        runner
            .call(
                "route.account_observe",
                &json!({"event_id":"fill-c-started",
        "kind":"admission_started","admission_id":id,"run_id":"fill-c-run"}),
            )
            .await,
    );
    // The first continuation moves the run to B, the one replacement with room.
    let offer = result(runner.call("route.account_for", &runs[0]).await);
    assert_eq!(offer["account"], B, "{offer}");
    result(
        consumer
            .call(
                "route.report",
                &json!({"decision_id":offer["decision_id"],
                    "outcome":"completed","usage":{"input_tokens":100,"output_tokens":20}}),
            )
            .await,
    );
    // The second continues the offer's decision, a copy of the run's own:
    // it ranks as the run's consumer when held.
    let b_credential = credentials
        .iter()
        .find(|credential| credential.account.account_id == B)
        .unwrap()
        .account
        .credential_id
        .clone();
    let mut second = runs[0].clone();
    second["selection_id"] = json!("selection-second");
    second["previous_decision_id"] = offer["decision_id"].clone();
    second["exhausted"]["credential_id"] = json!(b_credential);
    second["exhausted"]["account"] = json!(B);
    let held = result(runner.call("route.account_for", &second).await);
    assert_eq!(held["kind"], "hold", "{held}");
    assert_eq!(held["routing_code"], "concurrency_full", "{held}");
    assert_eq!(
        waiters(&harness, T),
        [(ConsumerPriority::Blocking, vec![C.to_owned()])]
    );
}
