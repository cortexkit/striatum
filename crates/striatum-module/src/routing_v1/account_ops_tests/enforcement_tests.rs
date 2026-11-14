//! Enforced concurrency caps on `route.account_for`. Every scenario runs under
//! an injected clock; counts are read from the published in-memory ledger.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use cortexkit_role_routing::errors::RefusalCode;
use rusqlite::Connection;
use striatum_store::AccountCommandKind;

use super::*;
use crate::account_state::InflightScope;

mod head_turn_tests;
mod priority_tests;
mod select_ledger_tests;

const A: &str = "oauth-account";
const B: &str = "apikey-account";
const MINUTE: u64 = 60_000;

fn credential(account: &str) -> &'static str {
    if account == A {
        "oauth:test"
    } else {
        "apikey:test"
    }
}

/// A fresh quota reading for A at `now`, so a scenario that moves its clock
/// is judged on headroom and not on a stale reading.
fn reading(now: u64) {
    let mut state = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_provider_reply(&mut state, "test-source", &serde_json::to_vec(&json!({
        "result":[{"provider":"test-source","apiProvider":"synthetic","account":A,"source":"vault",
            "fetchedAt":crate::account_selection::rfc3339(now as i64),
            "usage":{"primary":{"usedPercent":10.0,"windowMinutes":300,
                "resetsAt":crate::account_selection::rfc3339(NOW as i64 + 3_600_000)}}}],
        "completeProviders":["test-source"]
    })).unwrap(), now as i64).unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
}

/// Move the injected clock, keeping A's quota reading fresh.
fn at(now: u64) -> crate::InjectedClock {
    let clock = crate::InjectedClock::set(now);
    reading(now);
    clock
}

/// Both accounts eligible for a plain request, each capped at `limit`.
fn caps(harness: &Harness, limit: u64, enforced: bool) {
    reading(NOW);
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["subscription"] = json!(true);
        config["concurrency"] =
            json!({"enforcement_enabled":enforced,"providers":{"synthetic":{"limit":limit}}});
    });
}

/// A new conversation with no affinity.
fn fresh(id: &str) -> Value {
    let mut body = request(id);
    body["conversation_key"] = json!(id);
    body
}

/// A new conversation the runner may place only on `account`.
fn on(account: &str, id: &str) -> Value {
    let mut body = fresh(id);
    body["candidates"] = json!({"view_digest":format!("{account}-only"),
        "accounts":[{"account_id":account,"credential_id":credential(account)}]});
    body
}

fn count(harness: &Harness, account: &str, now: u64) -> usize {
    crate::account_state::snapshot(harness.store())
        .unwrap()
        .inflight
        .count("synthetic", account, InflightScope::Account, now as i64)
}

async fn admitted_on(route: &Route, body: &Value) -> Value {
    let reply = route.call("route.account_for", body).await;
    response(&reply).clone()
}

async fn fill(route: &Route, account: &str, prefix: &str, n: usize) -> Vec<Value> {
    let mut replies = Vec::new();
    for i in 0..n {
        let reply = admitted_on(route, &on(account, &format!("{prefix}-{i}"))).await;
        assert_eq!(reply["account"]["account_id"], account);
        replies.push(reply);
    }
    replies
}

fn lines() -> (Vec<Value>, Vec<Value>) {
    let mut calls = Vec::new();
    let mut pressure = Vec::new();
    for line in crate::take_operator_infos() {
        if let Some(body) = line.strip_prefix("routing account call ") {
            calls.push(serde_json::from_str(body).unwrap());
        } else if let Some(body) = line.strip_prefix("routing account concurrency would refuse ") {
            pressure.push(serde_json::from_str(body).unwrap());
        }
    }
    (calls, pressure)
}

fn assert_concurrency_full(reply: &(FrameType, Value), reset_at_ms: u64) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
    assert_eq!(
        reply.1["detail"],
        json!({"reason":"concurrency_full","reset_at_ms":reset_at_ms}),
        "{}",
        reply.1
    );
}

type Rows = BTreeMap<String, Vec<Vec<rusqlite::types::Value>>>;

/// A concurrency refusal's only store write is its decision row.
fn assert_only_a_refusal_row(harness: &Harness, before: &Rows, admission_id: &str) {
    let mut after = super::super::what_if_tests::rows(harness);
    let mut before = before.clone();
    let decisions = after.remove("account_decision").unwrap();
    assert_eq!(
        decisions.len(),
        before.remove("account_decision").unwrap().len() + 1
    );
    assert_eq!(after, before);
    let refusal = &harness.store().account_decisions("serve", 1).unwrap()[0];
    assert_eq!(refusal.outcome, "refuse");
    assert_eq!(refusal.code, "concurrency_full");
    assert_eq!(refusal.admission_id.as_deref(), Some(admission_id));
    assert_eq!(refusal.account_id, None);
}

#[tokio::test]
async fn enforced_cap_moves_new_conversations_to_a_free_sibling_then_refuses_with_one_row() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 6, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    fill(&runner, A, "a", 6).await;
    assert_eq!(count(&harness, A, NOW), 6);
    lines();
    let seventh = admitted_on(&runner, &fresh("seventh")).await;
    assert_eq!(seventh["account"]["account_id"], B);
    fill(&runner, B, "b", 5).await;
    assert_eq!((count(&harness, A, NOW), count(&harness, B, NOW)), (6, 6));
    let (_, pressure) = lines();
    assert!(pressure.is_empty(), "{pressure:?}");
    let before = super::super::what_if_tests::rows(&harness);
    // Every reservation expires a minute after NOW, after the ceiling.
    assert_concurrency_full(
        &runner.call("route.account_for", &fresh("eighth")).await,
        NOW + 30_000,
    );
    assert_only_a_refusal_row(&harness, &before, "eighth");
    let (calls, pressure) = lines();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        pressure,
        vec![
            json!({"surface":"routing/v1","consumer":"class-runner","class":"worker",
            "model":"model","refused":true,"accounts":[
                {"account_id":B,"count":6,"cap":6},{"account_id":A,"count":6,"cap":6}]})
        ]
    );
    // Legacy `route.account_for` answers the allocator's own code and reset,
    // also on a route that carries a head scope: it is never a head call.
    let head = harness.bind(reserved("class-runner"), Some("head")).await;
    for route in [&runner, &head] {
        let mut legacy = fresh("legacy");
        legacy["caller"] = json!({"runner":"class-runner"});
        let refused = route.call("route.account_for", &legacy).await;
        assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
        assert_eq!(refused.1["code"], "concurrency_full", "{}", refused.1);
        assert_eq!(refused.1["detail"]["reset_at_ms"], NOW + 30_000);
    }
    let (_, pressure) = lines();
    assert_eq!(pressure.len(), 2);
    assert!(pressure
        .iter()
        .all(|line| line["surface"] == "route.account_for" && line["refused"] == true));
    // A head call is never refused: with no free sibling it goes over the cap.
    let admitted = admitted_on(&head, &fresh("head")).await;
    assert_eq!(count(&harness, A, NOW) + count(&harness, B, NOW), 13);
    let over = admitted["account"]["account_id"].as_str().unwrap();
    assert_eq!(count(&harness, over, NOW), 7);
    let (_, pressure) = lines();
    assert!(pressure.is_empty(), "{pressure:?}");
}

#[tokio::test]
async fn unenforced_cap_admits_and_picks_as_before_and_only_logs_would_refuse() {
    let _clock = crate::InjectedClock::set(NOW);
    let mut answers = Vec::new();
    for limit in [None, Some(6)] {
        let _inputs = striatum_store::replay::Inputs::new(NOW as i64).install();
        let (harness, _) = setup("synthetic", false);
        caps(&harness, limit.unwrap_or(100), false);
        let runner = harness.bind(reserved("class-runner"), None).await;
        fill(&runner, A, "a", 6).await;
        fill(&runner, B, "b", 6).await;
        lines();
        // The kit-wide harness check also fails any `concurrency_full` served
        // with enforcement off; this burst goes well over both caps.
        for i in 0..10 {
            let reply = runner
                .call_raw("route.account_for", &fresh(&format!("burst-{i}")))
                .await;
            assert_eq!(reply.0, FrameType::Response);
            answers.push(reply);
        }
        let reply = runner.call_raw("route.account_for", &fresh("over")).await;
        assert_eq!(reply.0, FrameType::Response);
        let (_, pressure) = lines();
        assert_eq!(
            pressure.len(),
            11 * usize::from(limit.is_some()),
            "{pressure:?}"
        );
        assert!(pressure.iter().all(|line| line.get("refused").is_none()));
        answers.push(reply);
    }
    assert_eq!(answers[..11], answers[11..]);
}

/// A confirmed exhaustion fence on `account` until `reset_at_ms`.
fn block(harness: &Harness, account: &str, reset_at_ms: i64) {
    use striatum_core::accounts::{
        ConfirmationSource, Fence, FenceScope, GeneralWindow, ResetTime,
    };
    let scope = FenceScope::General(GeneralWindow::FiveHour);
    let fence = Fence {
        account_id: account.into(),
        pool: Some(scope.into()),
        recovery_set: Vec::new(),
        observed_at_ms: NOW as i64,
        reset: ResetTime::At(reset_at_ms),
        retry_hint_ms: None,
        source: ConfirmationSource::BrocaAccountLimit,
    };
    harness
        .store()
        .account_transaction(|tx| {
            tx.insert_fence(&striatum_store::AccountFenceRow {
                fence_id: format!("enforcement-fence-{account}"),
                provider: "synthetic".into(),
                account_id: account.into(),
                scope_json: serde_json::to_string(&scope).unwrap(),
                fence_json: serde_json::to_string(&fence).unwrap(),
                source: "broca_limit_exhausted".into(),
                observed_at: NOW as i64,
                principal: "test".into(),
                admission_id: None,
                observation_id: None,
                created_at: NOW as i64,
                cleared_at: None,
                cleared_reason: None,
            })
        })
        .unwrap();
}

#[tokio::test]
async fn enforced_kept_affinity_is_refused_and_blocked_siblings_neither_free_nor_reword_it() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    admitted_on(&runner, &on(A, "kept")).await;
    let before = super::super::what_if_tests::rows(&harness);
    // The same conversation again: its affinity keeps it on full A although B
    // has room, and the refusal leaves that affinity as it was.
    let mut again = fresh("kept-again");
    again["conversation_key"] = json!("kept");
    assert_concurrency_full(
        &runner.call("route.account_for", &again).await,
        NOW + 30_000,
    );
    assert_only_a_refusal_row(&harness, &before, "kept-again");
    assert_eq!((count(&harness, A, NOW), count(&harness, B, NOW)), (1, 0));
    lines();
    // A blocked sibling is not eligible: it neither frees the route nor
    // changes the reason.
    block(&harness, B, NOW as i64 + 600_000);
    assert_concurrency_full(
        &runner.call("route.account_for", &fresh("b-blocked")).await,
        NOW + 30_000,
    );
    let (_, pressure) = lines();
    assert_eq!(
        pressure[0]["accounts"],
        json!([{"account_id":A,"count":1,"cap":1}])
    );
    // With every account blocked the allocator's own answer stands, byte for
    // byte as with enforcement off.
    let mut answers = Vec::new();
    for enforced in [true, false] {
        let _inputs = striatum_store::replay::Inputs::new(NOW as i64).install();
        let (harness, _) = setup("synthetic", false);
        caps(&harness, 1, enforced);
        let runner = harness.bind(reserved("class-runner"), None).await;
        admitted_on(&runner, &on(A, "kept")).await;
        block(&harness, A, NOW as i64 + 600_000);
        block(&harness, B, NOW as i64 + 600_000);
        answers.push(
            runner
                .call_raw("route.account_for", &fresh("both-blocked"))
                .await,
        );
    }
    assert_eq!(answers[0].0, FrameType::Error);
    let body: Value = serde_json::from_slice(&answers[0].1).unwrap();
    assert_eq!(body["code"], "account_hold", "{body}");
    assert_eq!(answers[0], answers[1]);
}

#[tokio::test]
async fn enforced_head_calls_take_room_first_keep_affinity_and_never_wait() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 2, true);
    crate::routing_v1::consumers::replace_catalog(&json!([
        {"module_id":"replaceable-host","nonce_verified":true,"provides":["agent-host/v1"]},
        {"module_id":"class-owner","nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[{"name":"mason"}]}}}
    ]));
    harness.edit_config(|config| config["consumer_priorities"] = json!({"mason":"interactive"}));
    let runner = harness.bind(reserved("class-runner"), None).await;
    let owner = harness.bind(reserved("class-owner"), None).await;
    let head_runner = harness.bind(reserved("class-runner"), Some("head")).await;
    let scoped = session(&harness, "replaceable-host", "head-session", "head").await;
    admitted_on(&head_runner, &on(A, "kept")).await;
    admitted_on(&runner, &on(A, "filler")).await;
    lines();
    // Kept on its affinity account: a head call stays there, over the cap,
    // while B is empty.
    let mut kept = fresh("kept-head");
    kept["conversation_key"] = json!("kept");
    let stayed = admitted_on(&head_runner, &kept).await;
    assert_eq!(stayed["account"]["account_id"], A);
    assert_eq!((count(&harness, A, NOW), count(&harness, B, NOW)), (3, 0));
    // No affinity: a head call takes a free sibling first, for a runner
    // module's call and for a call the head scope authorises.
    for (route, id) in [(&head_runner, "runner-head"), (&scoped, "scope-head")] {
        let placed = admitted_on(route, &fresh(id)).await;
        assert_eq!(placed["account"]["account_id"], B, "{id}");
    }
    assert_eq!(count(&harness, B, NOW), 2);
    let (calls, pressure) = lines();
    assert_eq!(calls.len(), 3);
    assert!(calls.iter().all(|call| call["head_call"] == true));
    assert!(pressure.is_empty(), "{pressure:?}");
    // A runner module's call on a route with no head scope is refused, also
    // when the decision it is bound to names an interactive consumer.
    let selected = owner
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30,"eq":0}}),
        )
        .await;
    let mut interactive = fresh("interactive");
    interactive["decision_id"] = response(&selected)["decision_id"].clone();
    lines();
    for body in [fresh("plain"), interactive] {
        let refused = runner.call("route.account_for", &body).await;
        assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
        assert_eq!(
            refused.1["detail"]["reason"], "concurrency_full",
            "{}",
            refused.1
        );
    }
    let (_, pressure) = lines();
    assert_eq!(pressure.len(), 2, "{pressure:?}");
    assert_eq!(pressure[1]["class"], "interactive");
}

async fn observe(route: &Route, body: Value) {
    let reply = route.call("route.account_observe", &body).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
}

fn renew(conversation: &str, id: &str, admitted: &Value) -> Value {
    let mut body = on(A, id);
    body["conversation_key"] = json!(conversation);
    body["mode"] = json!("renew");
    body["expected_pin"] =
        json!({"id":admitted["pin"]["id"],"generation":admitted["pin"]["generation"]});
    body
}

#[tokio::test]
async fn enforced_cap_checks_only_new_holds_not_replays_or_renewals_of_a_holder() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 2, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    // Two reservations of one principal, neither started, are two holds.
    fill(&runner, A, "unstarted", 2).await;
    assert_concurrency_full(
        &runner.call("route.account_for", &on(A, "third")).await,
        NOW + 30_000,
    );

    let (harness, _) = setup("synthetic", false);
    caps(&harness, 6, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let gone = admitted_on(&runner, &on(A, "gone")).await;
    observe(
        &runner,
        json!({"event_id":"gone-start","kind":"admission_started",
        "admission_id":"gone","run_id":"gone-run"}),
    )
    .await;
    observe(
        &runner,
        json!({"event_id":"gone-finish","kind":"run_finished",
        "admission_id":"gone","run_id":"gone-run"}),
    )
    .await;
    let held = fill(&runner, A, "a", 6).await;
    observe(
        &runner,
        json!({"event_id":"a-0-start","kind":"admission_started",
        "admission_id":"a-0","run_id":"a-0-run"}),
    )
    .await;
    assert_eq!(count(&harness, A, NOW), 6);
    let replayed = runner.call("route.account_for", &on(A, "a-1")).await;
    assert_eq!(response(&replayed), &held[1]);
    // A renewal of a conversation that holds one of the six adds no hold.
    let renewed = admitted_on(&runner, &renew("a-0", "a-0-renew", &held[0])).await;
    assert_eq!(renewed["account"]["account_id"], A);
    assert_eq!(count(&harness, A, NOW), 6);
    // A renewal from a conversation whose rows on A have all closed is a new
    // hold and is refused.
    assert_concurrency_full(
        &runner
            .call("route.account_for", &renew("gone", "gone-renew", &gone))
            .await,
        NOW + 30_000,
    );
    assert_eq!(count(&harness, A, NOW), 6);
    // At T = NOW + 48 s the five unstarted reservations expire at T + 12 s,
    // which frees a slot then.
    let _later = at(NOW + 48_000);
    assert_concurrency_full(
        &runner.call("route.account_for", &on(A, "at-t")).await,
        NOW + 60_000,
    );
}

#[tokio::test]
async fn enforced_reset_ignores_a_renewal_closing_beside_its_started_run() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let started = admitted_on(&runner, &on(A, "x")).await;
    observe(
        &runner,
        json!({"event_id":"x-start","kind":"admission_started",
        "admission_id":"x","run_id":"x-run"}),
    )
    .await;
    admitted_on(&runner, &renew("x", "x-renew", &started)).await;
    assert_eq!(count(&harness, A, NOW), 1);
    // At T = NOW + 48 s the renewal expires at T + 12 s, but the run it
    // renews keeps the slot: no fall is known before the ceiling.
    let _at = at(NOW + 48_000);
    assert_concurrency_full(
        &runner
            .call("route.account_for", &on(A, "beside-renewal"))
            .await,
        NOW + 48_000 + 30_000,
    );
    // With only the started run left, observed less than 30 minutes ago,
    // the answer is the ceiling as well.
    let _alone = at(NOW + 29 * MINUTE);
    assert_concurrency_full(
        &runner
            .call("route.account_for", &on(A, "started-only"))
            .await,
        NOW + 29 * MINUTE + 30_000,
    );
}

#[tokio::test]
async fn enforced_cap_counts_late_observations_over_the_cap() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 6, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    admitted_on(&runner, &on(A, "old")).await;
    observe(
        &runner,
        json!({"event_id":"old-start","kind":"admission_started",
        "admission_id":"old","run_id":"old-run"}),
    )
    .await;
    let later = NOW + 31 * MINUTE;
    let _later = at(later);
    assert_eq!(count(&harness, A, later), 0);
    fill(&runner, A, "refill", 6).await;
    observe(
        &runner,
        json!({"event_id":"old-attempt","kind":"attempt",
        "admission_id":"old","run_id":"old-run","served_account_id":A,
        "credential_id":credential(A),"observed_at_ms":later}),
    )
    .await;
    assert_eq!(count(&harness, A, later), 7);
    assert_concurrency_full(
        &runner.call("route.account_for", &on(A, "next")).await,
        later + 30_000,
    );
    let expired = later + MINUTE + 1_000;
    let _expired = at(expired);
    assert_eq!(count(&harness, A, expired), 1);
    observe(
        &runner,
        json!({"event_id":"late-start","kind":"admission_started",
        "admission_id":"refill-0","run_id":"late-run"}),
    )
    .await;
    assert_eq!(count(&harness, A, expired), 2);
}

#[tokio::test]
async fn enforced_reset_waits_for_the_fall_below_the_cap_after_a_head_overshoot() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let head = harness.bind(reserved("class-runner"), Some("head")).await;
    admitted_on(&runner, &on(A, "first")).await;
    let _head = at(NOW + 8_000);
    admitted_on(&head, &on(A, "head")).await;
    // At T = NOW + 48 s the first reservation expires at T + 12 s, which
    // leaves the count at its cap; the head call's expires at T + 20 s.
    let _at = at(NOW + 48_000);
    assert_eq!(count(&harness, A, NOW + 48_000), 2);
    assert_concurrency_full(
        &runner.call("route.account_for", &on(A, "refused")).await,
        NOW + 68_000,
    );
}

type Answer = Result<Value, cortexkit_role_routing::errors::Refusal>;

/// Run `bodies` as concurrent requests whose admission commands are all
/// queued behind a paused writer, so they run in one writer batch. Each
/// request waits at its last step before the writer until the writer is
/// paused: the steps before it may themselves need the writer.
fn one_batch(harness: &Harness, routes: [&Route; 2], bodies: [Value; 2]) -> Vec<Answer> {
    let store = harness.store();
    let arrived = Arc::new(std::sync::Barrier::new(3));
    let go = Arc::new(std::sync::Barrier::new(3));
    for body in &bodies {
        let (arrived, go) = (arrived.clone(), go.clone());
        crate::account_selection::after_roster_prepared(
            body["admission_id"].as_str().unwrap(),
            move || {
                arrived.wait();
                go.wait();
            },
        );
    }
    std::thread::scope(|scope| {
        let handles: Vec<_> = routes
            .into_iter()
            .zip(bodies)
            .map(|(route, body)| {
                scope.spawn(move || {
                    let _clock = crate::InjectedClock::set(NOW);
                    route.account_for_direct(&body)
                })
            })
            .collect();
        arrived.wait();
        let (entered, running) = std::sync::mpsc::channel();
        let (resume, paused) = std::sync::mpsc::channel::<()>();
        let barrier = store
            .enqueue_account_command(AccountCommandKind::Maintenance, move |_| {
                entered.send(()).unwrap();
                paused.recv().unwrap();
                Ok(())
            })
            .unwrap();
        running.recv_timeout(Duration::from_secs(5)).unwrap();
        let queued = store.account_command_count_for_test(AccountCommandKind::Admit);
        go.wait();
        let deadline = Instant::now() + Duration::from_secs(10);
        while store.account_command_count_for_test(AccountCommandKind::Admit) < queued + 2
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        let waiting = store.account_command_count_for_test(AccountCommandKind::Admit) - queued;
        resume.send(()).unwrap();
        let answers: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        barrier.wait().unwrap();
        assert_eq!(waiting, 2, "both admissions must wait in one writer queue");
        answers
    })
}

fn placed(answers: &[Answer]) -> Vec<String> {
    let mut accounts: Vec<_> = answers
        .iter()
        .map(|answer| match answer {
            Ok(admitted) => admitted["account"]["account_id"]
                .as_str()
                .unwrap()
                .to_owned(),
            Err(refusal) => {
                assert_eq!(refusal.code, RefusalCode::NoRoute, "{refusal:?}");
                assert_eq!(refusal.detail["reason"], "concurrency_full");
                "refused".to_owned()
            }
        })
        .collect();
    accounts.sort();
    accounts
}

#[tokio::test]
async fn enforced_last_slot_goes_to_one_request_in_one_batch_or_two() {
    let _clock = crate::InjectedClock::set(NOW);
    for batched in [true, false] {
        // With no other account one request reserves and the other is refused;
        // with B free the other lands there.
        for (bodies, expected) in [
            ([on(A, "x"), on(A, "y")], [A, "refused"]),
            ([fresh("v"), fresh("w")], [A, B]),
        ] {
            let (harness, _) = setup("synthetic", false);
            caps(&harness, 1, true);
            let first = harness.bind(reserved("class-runner"), None).await;
            let second = harness.bind(reserved("class-runner"), None).await;
            let answers = if batched {
                one_batch(&harness, [&first, &second], bodies)
            } else {
                let [one, two] = bodies;
                vec![
                    first.account_for_direct(&one),
                    second.account_for_direct(&two),
                ]
            };
            let mut expected = expected.map(str::to_owned).to_vec();
            expected.sort();
            assert_eq!(placed(&answers), expected, "batched={batched}");
            let admitted = answers.iter().filter(|answer| answer.is_ok()).count();
            assert_eq!(count(&harness, A, NOW) + count(&harness, B, NOW), admitted);
        }
    }
}

/// Fail the commit of the first batch whose admission commands have all run,
/// counted from `ran` admission commands.
fn fail_commit_after(harness: &Harness, ran: usize, armed: Arc<AtomicBool>) {
    // Weak: the hook lives on the store's own connection.
    let store = Arc::downgrade(harness.store());
    harness
        .store()
        .with_writer_for_test(move |conn| {
            conn.commit_hook(Some(move || {
                armed.load(Ordering::SeqCst)
                    && store.upgrade().is_some_and(|store| {
                        store
                            .account_command_row_changes_for_test(AccountCommandKind::Admit)
                            .len()
                            >= ran
                    })
            }));
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn enforced_store_failures_admit_nothing_and_leave_the_count() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    let first = harness.bind(reserved("class-runner"), None).await;
    let second = harness.bind(reserved("class-runner"), None).await;
    // The reserving command fails: its savepoint rolls back and leaves no hold.
    let path = harness.store_path();
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER refuse_admission BEFORE INSERT ON account_admission \
             BEGIN SELECT RAISE(FAIL, 'admission refused'); END;",
        )
        .unwrap();
    let failed = first.account_for_direct(&on(A, "failing")).unwrap_err();
    assert_eq!(
        failed.code,
        RefusalCode::AccountSelectionUnavailable,
        "{failed:?}"
    );
    assert_eq!(count(&harness, A, NOW), 0);
    Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TRIGGER refuse_admission;")
        .unwrap();
    // A failed batch transaction gives every command in it the store error,
    // the one an earlier command's reservation refused included.
    let armed = Arc::new(AtomicBool::new(true));
    let ran = harness
        .store()
        .account_command_row_changes_for_test(AccountCommandKind::Admit)
        .len();
    fail_commit_after(&harness, ran + 2, armed.clone());
    let answers = one_batch(&harness, [&first, &second], [on(A, "x"), on(A, "y")]);
    armed.store(false, Ordering::SeqCst);
    harness
        .store()
        .with_writer_for_test(|conn| {
            conn.commit_hook(None::<fn() -> bool>);
            Ok(())
        })
        .unwrap();
    for answer in &answers {
        let refusal = answer.as_ref().unwrap_err();
        assert_eq!(
            refusal.code,
            RefusalCode::AccountSelectionUnavailable,
            "{refusal:?}"
        );
    }
    assert_eq!(count(&harness, A, NOW), 0);
    // Nothing of the failed batch holds the slot: run again, the batch
    // reserves it once.
    let answers = one_batch(
        &harness,
        [&first, &second],
        [on(A, "x-again"), on(A, "y-again")],
    );
    let mut expected = vec![A.to_owned(), "refused".to_owned()];
    expected.sort();
    assert_eq!(placed(&answers), expected);
    assert_eq!(count(&harness, A, NOW), 1);
}
