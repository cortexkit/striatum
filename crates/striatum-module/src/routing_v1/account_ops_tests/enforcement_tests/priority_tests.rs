//! Waiters on `route.account_for`: a conversation refused for concurrency
//! keeps a free slot from requests ranked below it until it comes back, or
//! until 30 s after the retry time it was given. Every scenario runs under an
//! injected clock.

use cortexkit_role_routing::declarations::ConsumerPriority::{self, *};

use super::*;

/// Declare consumers of three classes. A request bound to a decision for one
/// of them ranks by that class; an unbound request ranks as `worker`.
fn classes(harness: &Harness) {
    crate::routing_v1::consumers::replace_catalog(&json!([
        {"module_id":"class-owner","nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[{"name":"class-owner.urgent"},{"name":"class-owner.bulk"}]}}}
    ]));
    harness.edit_config(|config| {
        config["consumer_priorities"] =
            json!({"class-owner.urgent":"blocking","class-owner.bulk":"background"})
    });
}

/// A harness with A and B capped at 1, enforcement on, and the class-owner
/// module that selects for the declared consumers.
async fn enforced() -> (Arc<Harness>, Route, Route) {
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    // A's quota reading is fresh at the scenario's clock, not only at NOW.
    reading(crate::account_selection::now_ms() as u64);
    classes(&harness);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let owner = harness.bind(reserved("class-owner"), None).await;
    (harness, runner, owner)
}

/// `body` bound to a fresh decision selected for `consumer`.
async fn bound(owner: &Route, consumer: &str, mut body: Value) -> Value {
    let selected = owner
        .call(
            "route.select",
            &json!({"consumer":consumer,"requirements":{"iq":30,"eq":0}}),
        )
        .await;
    body["decision_id"] = response(&selected)["decision_id"].clone();
    body
}

/// `body` asked again by the same conversation under a new admission id.
fn again(conversation: &str, id: &str, mut body: Value) -> Value {
    body["admission_id"] = json!(id);
    body["conversation_key"] = json!(conversation);
    body
}

/// A run admitted on `account` and started: a hold that outlives the ceiling.
async fn running(runner: &Route, account: &str, id: &str) {
    admitted_on(runner, &on(account, id)).await;
    observe(
        runner,
        json!({"event_id":format!("{id}-start"),"kind":"admission_started",
            "admission_id":id,"run_id":format!("{id}-run")}),
    )
    .await;
}

async fn finish(runner: &Route, id: &str) {
    observe(
        runner,
        json!({"event_id":format!("{id}-finish"),"kind":"run_finished",
            "admission_id":id,"run_id":format!("{id}-run")}),
    )
    .await;
}

/// The published live waiters at `now` as (class, order, accounts waited
/// on), in order.
fn waiters(harness: &Harness, now: u64) -> Vec<(ConsumerPriority, u64, Vec<String>)> {
    let state = crate::account_state::snapshot(harness.store()).unwrap();
    let mut waiters: Vec<_> = state
        .waiters
        .live(now as i64)
        .map(|(_, waiter)| {
            (
                waiter.class,
                waiter.order,
                waiter
                    .counts
                    .iter()
                    .map(|count| count.account.clone())
                    .collect(),
            )
        })
        .collect();
    waiters.sort_by_key(|(_, order, _)| *order);
    waiters
}

fn classes_waiting(harness: &Harness, now: u64) -> Vec<ConsumerPriority> {
    waiters(harness, now)
        .into_iter()
        .map(|(class, _, _)| class)
        .collect()
}

#[tokio::test]
async fn a_blocking_waiter_takes_the_freed_slot_before_a_later_background_request() {
    // Refused first, X is admitted before Y, although Y arrived while the
    // slot was free.
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, runner, owner) = enforced().await;
    running(&runner, A, "h").await;
    let x = bound(&owner, "class-owner.urgent", on(A, "x")).await;
    assert_concurrency_full(&runner.call("route.account_for", &x).await, NOW + 30_000);
    assert_eq!(waiters(&harness, NOW), [(Blocking, 0, vec![A.to_owned()])]);
    finish(&runner, "h").await;
    assert_eq!(count(&harness, A, NOW), 0);
    lines();
    let y = bound(&owner, "class-owner.bulk", on(A, "y")).await;
    assert_concurrency_full(&runner.call("route.account_for", &y).await, NOW + 30_000);
    assert_eq!(count(&harness, A, NOW), 0);
    // Refused though a slot is free, Y logs its refusal line like any other.
    let (_, pressure) = lines();
    assert_eq!(
        pressure,
        [
            json!({"surface":"routing/v1","consumer":"class-owner.bulk","class":"background",
            "model":"model","refused":true,"accounts":[{"account_id":A,"count":0,"cap":1}]})
        ]
    );
    let retried = bound(
        &owner,
        "class-owner.urgent",
        again("x", "x-again", on(A, "x")),
    )
    .await;
    assert_eq!(
        admitted_on(&runner, &retried).await["account"]["account_id"],
        A
    );
    // Taking the slot it waited on ends X's wait; Y still waits.
    assert_eq!(classes_waiting(&harness, NOW), [Background]);

    // In the other order nothing is waiting when Y arrives: the lower class
    // that comes first takes the slot, since routing does not predict arrivals.
    let (harness, runner, owner) = enforced().await;
    running(&runner, A, "h").await;
    finish(&runner, "h").await;
    let y = bound(&owner, "class-owner.bulk", on(A, "y")).await;
    assert_eq!(admitted_on(&runner, &y).await["account"]["account_id"], A);
    let x = bound(&owner, "class-owner.urgent", on(A, "x")).await;
    assert_concurrency_full(&runner.call("route.account_for", &x).await, NOW + 30_000);
    assert_eq!(classes_waiting(&harness, NOW), [Blocking]);
}

#[tokio::test]
async fn a_waiter_that_does_not_come_back_frees_the_slot_30_s_after_its_retry_time() {
    let clock = crate::InjectedClock::set(NOW);
    let (harness, runner, owner) = enforced().await;
    running(&runner, A, "h").await;
    let x = bound(&owner, "class-owner.urgent", on(A, "x")).await;
    assert_concurrency_full(&runner.call("route.account_for", &x).await, NOW + 30_000);
    finish(&runner, "h").await;
    drop(clock);
    // X's wait ends at its retry time + 30 s, the first instant Y could
    // take the slot.
    let ends = NOW + 60_000;
    let before = at(ends - 1);
    let y = bound(&owner, "class-owner.bulk", on(A, "y")).await;
    assert_concurrency_full(&runner.call("route.account_for", &y).await, ends);
    drop(before);
    let _after = at(ends);
    let y = bound(
        &owner,
        "class-owner.bulk",
        again("y", "y-again", on(A, "y")),
    )
    .await;
    assert_eq!(admitted_on(&runner, &y).await["account"]["account_id"], A);
    assert!(waiters(&harness, ends).is_empty());
}

#[tokio::test]
async fn reset_at_ms_counts_only_the_waiters_still_live_at_the_instant() {
    // X is refused while A is held by a started run, so its retry time is
    // the ceiling, `refused_at` + 30 s. The run then finishes, and at T the
    // slot is free.
    let t = NOW + 60_000;
    for (refused_at, reset) in [(t - 40_000, t + 20_000), (t - 25_000, t + 30_000)] {
        let clock = crate::InjectedClock::set(refused_at);
        let (_harness, runner, owner) = enforced().await;
        running(&runner, A, "h").await;
        let x = bound(&owner, "class-owner.urgent", on(A, "x")).await;
        assert_concurrency_full(
            &runner.call("route.account_for", &x).await,
            refused_at + 30_000,
        );
        finish(&runner, "h").await;
        drop(clock);
        let _t = at(t);
        let y = bound(&owner, "class-owner.bulk", on(A, "y")).await;
        assert_concurrency_full(&runner.call("route.account_for", &y).await, reset);
    }
}

#[tokio::test]
async fn worker_waiters_go_in_order_of_first_refusal_and_a_waiter_never_holds_a_slot_against_itself(
) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, runner, _owner) = enforced().await;
    running(&runner, A, "h").await;
    // Two unbound requests rank as `worker`, refused at one clock instant:
    // the earlier command has the earlier order.
    for id in ["p", "q"] {
        assert_concurrency_full(
            &runner.call("route.account_for", &on(A, id)).await,
            NOW + 30_000,
        );
    }
    assert_eq!(
        waiters(&harness, NOW),
        [
            (Worker, 0, vec![A.to_owned()]),
            (Worker, 1, vec![A.to_owned()])
        ]
    );
    finish(&runner, "h").await;
    // Q retrying first is refused: P was refused before it.
    assert_concurrency_full(
        &runner
            .call("route.account_for", &again("q", "q-again", on(A, "q")))
            .await,
        NOW + 30_000,
    );
    let p = admitted_on(&runner, &again("p", "p-again", on(A, "p"))).await;
    assert_eq!(p["account"]["account_id"], A);
    assert_eq!(waiters(&harness, NOW), [(Worker, 1, vec![A.to_owned()])]);

    // X waits as `blocking`, then comes back bound to a `background`
    // consumer's decision: its own wait is never ahead of it.
    let (harness, runner, owner) = enforced().await;
    running(&runner, A, "h").await;
    let x = bound(&owner, "class-owner.urgent", on(A, "x")).await;
    assert_concurrency_full(&runner.call("route.account_for", &x).await, NOW + 30_000);
    finish(&runner, "h").await;
    let x = bound(
        &owner,
        "class-owner.bulk",
        again("x", "x-again", on(A, "x")),
    )
    .await;
    assert_eq!(admitted_on(&runner, &x).await["account"]["account_id"], A);
    assert!(waiters(&harness, NOW).is_empty());
}

#[tokio::test]
async fn a_waiter_kept_on_its_affinity_account_waits_on_that_count_alone() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, runner, owner) = enforced().await;
    // B takes two, so it has room after X places a run there.
    harness.edit_config(|config| {
        config["concurrency"]["accounts"] = json!({format!("synthetic/{B}"):{"limit":2}})
    });
    running(&runner, A, "x-first").await;
    finish(&runner, "x-first").await;
    running(&runner, A, "holder").await;
    // X keeps its affinity to A, where the other run holds the only slot.
    let mut x = fresh("x");
    x["conversation_key"] = json!("x-first");
    assert_concurrency_full(&runner.call("route.account_for", &x).await, NOW + 30_000);
    assert_eq!(waiters(&harness, NOW), [(Worker, 0, vec![A.to_owned()])]);
    // A free slot on B is not one X could take, so a background request
    // with no affinity gets it.
    let y = bound(&owner, "class-owner.bulk", fresh("y")).await;
    assert_eq!(admitted_on(&runner, &y).await["account"]["account_id"], B);
    // A later refusal keeps X's order and takes its class and retry time.
    let later = at(NOW + 5_000);
    let x = bound(
        &owner,
        "class-owner.urgent",
        again("x-first", "x-later", fresh("x")),
    )
    .await;
    assert_concurrency_full(&runner.call("route.account_for", &x).await, NOW + 35_000);
    assert_eq!(
        waiters(&harness, NOW + 5_000),
        [(Blocking, 0, vec![A.to_owned()])]
    );
    let state = crate::account_state::snapshot(harness.store()).unwrap();
    let (_, waiter) = state.waiters.live(NOW as i64 + 5_000).next().unwrap();
    assert_eq!(waiter.retry_at_ms, NOW as i64 + 35_000);
    // An admission of X on a count it does not wait on leaves it waiting.
    let elsewhere = admitted_on(&runner, &again("x-first", "x-on-b", on(B, "x-on-b"))).await;
    assert_eq!(elsewhere["account"]["account_id"], B);
    assert_eq!(
        waiters(&harness, NOW + 5_000),
        [(Blocking, 0, vec![A.to_owned()])]
    );
    // A slot on the count it waits on ends the wait.
    finish(&runner, "holder").await;
    let back = admitted_on(&runner, &again("x-first", "x-back", on(A, "x-back"))).await;
    assert_eq!(back["account"]["account_id"], A);
    assert!(waiters(&harness, NOW + 5_000).is_empty());
    drop(later);

    // A renewal by a waiting conversation that already holds the count adds
    // no hold, so it leaves the conversation waiting.
    let (harness, runner, held) = waiting_holder().await;
    let renewed = admitted_on(&runner, &renew("x", "x-renew", &held)).await;
    assert_eq!(renewed["account"]["account_id"], A);
    assert_eq!(count(&harness, A, NOW), 1);
    assert_eq!(waiters(&harness, NOW), [(Worker, 0, vec![A.to_owned()])]);
}

async fn waiting_holder() -> (Arc<Harness>, Route, Value) {
    let (harness, runner, _owner) = enforced().await;
    running(&runner, A, "holder").await;
    let x = on(A, "x");
    assert_concurrency_full(&runner.call("route.account_for", &x).await, NOW + 30_000);
    finish(&runner, "holder").await;

    // X waited on the account count, so admitting it under a per-model cap
    // leaves the old wait intact. Switching back puts the hold and wait on
    // the same count.
    harness.edit_config(|config| {
        config["concurrency"]["providers"]["synthetic"]["scope"] = json!("account_model");
    });
    let held = admitted_on(&runner, &x).await;
    start_admission(&runner, "x", "x-run").await;
    harness.edit_config(|config| {
        config["concurrency"]["providers"]["synthetic"]["scope"] = json!("account");
    });
    assert_eq!(waiters(&harness, NOW), [(Worker, 0, vec![A.to_owned()])]);
    (harness, runner, held)
}

#[tokio::test]
async fn an_admit_using_its_open_runs_slot_does_not_clear_a_waiter() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, runner, _) = waiting_holder().await;
    let next = again("x", "x-next", on(A, "x"));
    assert_eq!(
        admitted_on(&runner, &next).await["account"]["account_id"],
        A
    );
    start_admission(&runner, "x-next", "x-run").await;
    assert_eq!(count(&harness, A, NOW), 1);
    assert_eq!(waiters(&harness, NOW), [(Worker, 0, vec![A.to_owned()])]);
}

#[tokio::test]
async fn a_waiter_holds_back_one_slot_in_total_whatever_it_waits_on() {
    // X is refused at T - 40 s with both accounts held by started runs, so
    // its retry time is T - 10 s and its wait ends at T + 20 s.
    let t = NOW + 40_000;
    let clock = crate::InjectedClock::set(NOW);
    let (harness, runner, owner) = enforced().await;
    let x = bound(&owner, "class-owner.urgent", fresh("x")).await;
    running(&runner, A, "ha").await;
    running(&runner, B, "hb").await;
    assert_concurrency_full(&runner.call("route.account_for", &x).await, t - 10_000);
    assert_eq!(
        waiters(&harness, NOW),
        [(Blocking, 0, vec![B.to_owned(), A.to_owned()])]
    );
    finish(&runner, "ha").await;
    finish(&runner, "hb").await;
    drop(clock);
    let _t = at(t);
    // With A and B free, X needs only one of them.
    let y = admitted_on(&runner, &fresh("y")).await;
    assert!([A, B].contains(&y["account"]["account_id"].as_str().unwrap()));
    assert_concurrency_full(
        &runner.call("route.account_for", &fresh("z")).await,
        t + 20_000,
    );
}

#[tokio::test]
async fn the_waiters_ahead_are_counted_together_not_first_fit() {
    let t = NOW + 40_000;
    // X waits on both accounts and W on A alone. Together they need both
    // free slots, whichever was refused first.
    for x_first in [true, false] {
        let clock = crate::InjectedClock::set(NOW);
        let (harness, runner, owner) = enforced().await;
        let x = bound(&owner, "class-owner.urgent", fresh("x")).await;
        let w = bound(&owner, "class-owner.urgent", on(A, "w")).await;
        running(&runner, A, "ha").await;
        running(&runner, B, "hb").await;
        let bodies = if x_first { [x, w] } else { [w, x] };
        for body in bodies {
            assert_concurrency_full(&runner.call("route.account_for", &body).await, t - 10_000);
        }
        assert_eq!(waiters(&harness, NOW).len(), 2);
        finish(&runner, "ha").await;
        finish(&runner, "hb").await;
        drop(clock);
        let _t = at(t);
        assert_concurrency_full(
            &runner.call("route.account_for", &fresh("y")).await,
            t + 20_000,
        );
        let (_, pressure) = lines();
        assert_eq!(
            pressure.last().unwrap()["accounts"],
            json!([{"account_id":B,"count":0,"cap":1},{"account_id":A,"count":0,"cap":1}])
        );
    }
    // With W waiting on B alone, V that can take only A is refused, though
    // W waits on none of V's counts: X and W need both slots.
    for w_waits in [true, false] {
        let clock = crate::InjectedClock::set(NOW);
        let (_harness, runner, owner) = enforced().await;
        let x = bound(&owner, "class-owner.urgent", fresh("x")).await;
        let w = if w_waits {
            Some(bound(&owner, "class-owner.urgent", on(B, "w")).await)
        } else {
            None
        };
        running(&runner, A, "ha").await;
        running(&runner, B, "hb").await;
        assert_concurrency_full(&runner.call("route.account_for", &x).await, t - 10_000);
        if w_waits {
            let w = w.unwrap();
            assert_concurrency_full(&runner.call("route.account_for", &w).await, t - 10_000);
        }
        finish(&runner, "ha").await;
        finish(&runner, "hb").await;
        drop(clock);
        let _t = at(t);
        let v = runner.call("route.account_for", &on(A, "v")).await;
        if w_waits {
            assert_concurrency_full(&v, t + 20_000);
        } else {
            assert_eq!(response(&v)["account"]["account_id"], A);
        }
    }
}

#[tokio::test]
async fn head_calls_and_unenforced_caps_record_no_waiter() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, false);
    let runner = harness.bind(reserved("class-runner"), None).await;
    running(&runner, A, "h").await;
    // With enforcement off the request is admitted over the cap.
    assert_eq!(
        admitted_on(&runner, &on(A, "x")).await["account"]["account_id"],
        A
    );
    assert!(waiters(&harness, NOW).is_empty());

    // A head call is never refused, so never a waiter, and no waiter holds
    // a slot against it.
    let (harness, runner, _owner) = enforced().await;
    let head = harness.bind(reserved("class-runner"), Some("head")).await;
    running(&runner, A, "h").await;
    assert_eq!(
        admitted_on(&head, &on(A, "head-over")).await["account"]["account_id"],
        A
    );
    assert_eq!(count(&harness, A, NOW), 2);
    assert!(waiters(&harness, NOW).is_empty());
    // The head call's reservation is started and finished like any run.
    observe(
        &runner,
        json!({"event_id":"head-over-start","kind":"admission_started",
            "admission_id":"head-over","run_id":"head-over-run"}),
    )
    .await;
    assert_concurrency_full(
        &runner.call("route.account_for", &on(A, "x")).await,
        NOW + 30_000,
    );
    finish(&runner, "h").await;
    finish(&runner, "head-over").await;
    assert_eq!(count(&harness, A, NOW), 0);
    // X waits for the free slot, yet the head call takes it.
    assert_eq!(
        admitted_on(&head, &on(A, "head-free")).await["account"]["account_id"],
        A
    );
    assert_eq!(waiters(&harness, NOW), [(Worker, 0, vec![A.to_owned()])]);
}

/// The models of the counts each live waiter waits on, in order.
fn waited_models(harness: &Harness, now: u64) -> Vec<(u64, Vec<Option<String>>)> {
    let state = crate::account_state::snapshot(harness.store()).unwrap();
    let mut waiters: Vec<_> = state
        .waiters
        .live(now as i64)
        .map(|(_, waiter)| {
            (
                waiter.order,
                waiter
                    .counts
                    .iter()
                    .map(|count| count.model.clone())
                    .collect(),
            )
        })
        .collect();
    waiters.sort();
    waiters
}

fn for_model(model: &str, mut body: Value) -> Value {
    body["model"] = json!(model);
    body
}

#[tokio::test]
async fn under_account_model_scope_a_waiter_holds_no_slot_against_another_model() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    // A second model on the same accounts, so each has a count of its own.
    let first = crate::tests::fact("synthetic", "model");
    let mut other = crate::tests::fact("synthetic", "model-2");
    other.auth_methods = Some(vec!["oauth".into(), "apikey".into()]);
    harness.install_facts(&[first.clone(), other.clone()]);
    crate::routing_v1::account_classes_tests::catalog_fact(&harness, &other);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[first, other]),
    );
    harness.set_credentials(credentials);
    caps(&harness, 1, true);
    classes(&harness);
    harness.edit_config(|config| {
        config["concurrency"]["providers"]["synthetic"]["scope"] = json!("account_model")
    });
    let runner = harness.bind(reserved("class-runner"), None).await;
    let owner = harness.bind(reserved("class-owner"), None).await;
    running(&runner, A, "h").await;
    let x = bound(&owner, "class-owner.urgent", on(A, "x")).await;
    assert_concurrency_full(&runner.call("route.account_for", &x).await, NOW + 30_000);
    assert_eq!(
        waited_models(&harness, NOW),
        [(0, vec![Some("model".to_owned())])]
    );
    finish(&runner, "h").await;
    // X waits on A's count for `model`: another model's count on A is free
    // for anyone, while `model`'s free slot is X's.
    let other = admitted_on(&runner, &for_model("model-2", on(A, "other"))).await;
    assert_eq!(other["account"]["account_id"], A);
    assert_concurrency_full(
        &runner.call("route.account_for", &on(A, "same")).await,
        NOW + 30_000,
    );
    // Refused later for the other model, X waits on that model's count
    // instead and keeps its order; `model`'s slot is free for anyone again.
    let x = for_model("model-2", again("x", "x-other", on(A, "x")));
    assert_concurrency_full(&runner.call("route.account_for", &x).await, NOW + 30_000);
    assert_eq!(
        waited_models(&harness, NOW),
        [
            (0, vec![Some("model-2".to_owned())]),
            (1, vec![Some("model".to_owned())])
        ]
    );
    let next = admitted_on(&runner, &again("same", "same-again", on(A, "same"))).await;
    assert_eq!(next["account"]["account_id"], A);
}

#[tokio::test]
async fn consumer_cut_migrated_old_decision_admissions_use_canonical_priority_on_both_surfaces() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        migrated_priority_case(legacy, false).await;
    }
}

#[tokio::test]
async fn consumer_cut_legacy_admission_keeps_priority_after_binding_retention() {
    let _clock = crate::InjectedClock::set(NOW);
    migrated_priority_case(true, true).await;
}

async fn migrated_priority_case(legacy: bool, prune_binding: bool) {
    let (harness, credentials) =
        crate::routing_v1::account_classes_tests::setup_at_version("synthetic", false, Some(93));
    let decision = striatum_store::V1Decision {
        decision_id: "old-mason".into(),
        principal: "reserved:prefrontal-core".into(),
        consumer: "mason".into(),
        allowed_account_class: Default::default(),
        project_id: None,
        provider: "synthetic".into(),
        model: "model".into(),
        variant: None,
        runner_module: "class-runner".into(),
        affinity: None,
        created_at_ms: NOW as i64 - (if prune_binding { 10 } else { 3 }) * 86_400_000,
        expires_at_ms: NOW as i64 - (if prune_binding { 9 } else { 2 }) * 86_400_000,
    };
    harness
        .store()
        .seed_preupgrade_decision_for_test(&decision, true)
        .unwrap();
    let harness = crate::routing_v1::conformance_tests::upgrade_harness(harness);
    let fact = crate::routing_v1::account_classes_tests::account_fact("synthetic");
    harness.install_facts(&[fact.clone()]);
    crate::routing_v1::account_classes_tests::catalog_fact(&harness, &fact);
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["consumers"] = json!({"prefrontal-core.mason":{"owner":"prefrontal-core"}});
        config["consumer_priorities"] = json!({"prefrontal-core.mason":"blocking"});
    });
    if prune_binding {
        let pruned = harness
            .store()
            .prune_v1_decisions(NOW as i64 - 7 * 86_400_000, 10)
            .unwrap();
        assert_eq!(pruned.decisions, 1);
        assert!(harness.store().v1_decision("old-mason").unwrap().is_none());
    }
    caps(&harness, 1, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    running(&runner, A, "occupying").await;
    let mut body = on(A, "migrated-waiter");
    body[if legacy {
        "model_decision_id"
    } else {
        "decision_id"
    }] = json!("old-mason");
    if legacy {
        body["caller"] = json!({"runner":"class-runner"});
    }
    let reply = runner.call("route.account_for", &body).await;
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    assert_eq!(
        classes_waiting(&harness, NOW),
        [Blocking],
        "legacy={legacy}, binding_pruned={prune_binding}: {reply:?}"
    );
    if prune_binding {
        assert_eq!(
            harness
                .store()
                .get_decision_by_id("old-mason")
                .unwrap()
                .unwrap()
                .target_agent,
            "prefrontal-core.mason"
        );
    } else {
        assert_eq!(
            harness
                .store()
                .v1_decision("old-mason")
                .unwrap()
                .unwrap()
                .consumer,
            "prefrontal-core.mason"
        );
    }
}
