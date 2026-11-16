//! Rounded quota readings must not evict a session before a real refusal.

use super::*;

const A: &str = "oauth-account";
const B: &str = "apikey-account";

fn readings(used: f64, minutes: u64) {
    let mut state = crate::account_quota::AccountQuotaState::default();
    let entries: Vec<_> = [(A, used), (B, 10.0)]
        .into_iter()
        .map(|(account, used)| {
            json!({
                "provider":"test-source", "apiProvider":"synthetic", "account":account,
                "source":"vault", "fetchedAt":crate::account_selection::rfc3339(NOW as i64),
                "usage":{"primary":{"usedPercent":used,"windowMinutes":minutes,
                    "resetsAt":crate::account_selection::rfc3339(NOW as i64 + 7_200_000)}}
            })
        })
        .collect();
    crate::account_quota::apply_provider_reply(
        &mut state,
        "test-source",
        &serde_json::to_vec(&json!({"result":entries,"completeProviders":["test-source"]}))
            .unwrap(),
        NOW as i64,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
}

fn fixture(used: f64, minutes: u64) -> Arc<Harness> {
    let (harness, _) = setup("synthetic", false);
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["subscription"] = json!(true);
        config["providers"]["synthetic"]["keep_free_percent"] = json!(30);
    });
    readings(used, minutes);
    equivalent(&harness);
    harness
}

fn equivalent(harness: &Harness) {
    crate::account_state::assert_load_equivalent(harness.store(), NOW as i64);
}

async fn admit(harness: &Harness, route: &Route, id: &str, only_a: bool) -> Value {
    let mut body = request(id);
    body["hold_wait_max_s"] = json!(0);
    if only_a {
        body["candidates"] = json!({"view_digest":"a-only",
            "accounts":[{"account_id":A,"credential_id":"oauth:test"}]});
    }
    let reply = route.call("route.account_for", &body).await;
    equivalent(harness);
    response(&reply).clone()
}

#[tokio::test]
async fn sticky_head_keeps_account_and_generation_through_99_100_99() {
    let _clock = crate::InjectedClock::set(NOW);
    for minutes in [300, 10_080] {
        let harness = fixture(10.0, minutes);
        let head = harness.bind(reserved("class-runner"), Some("head")).await;
        let first = admit(&harness, &head, "first", true).await;
        assert_eq!(first["account"]["account_id"], A);
        for (id, used) in [("nearly-full", 99.0), ("full", 100.0), ("below-full", 99.0)] {
            readings(used, minutes);
            equivalent(&harness);
            let next = admit(&harness, &head, id, false).await;
            assert_eq!(next["account"], first["account"], "{minutes}m at {used}%");
            assert_eq!(next["pin"], first["pin"], "{minutes}m at {used}%");
        }
    }
}

#[tokio::test]
async fn sticky_head_moves_immediately_after_confirmed_account_limit() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = fixture(10.0, 300);
    let head = harness.bind(reserved("class-runner"), Some("head")).await;
    let first = admit(&harness, &head, "first", true).await;
    readings(100.0, 300);
    equivalent(&harness);
    let observed = head
        .call(
            "route.account_observe",
            &json!({
                "event_id":"limit", "kind":"account_limit_exhausted", "admission_id":"first",
                "limit_window":"five_hour", "observed_at_ms":NOW,
                "resets_at_ms":NOW+7_200_000, "resets_at_source":"header"
            }),
        )
        .await;
    response(&observed);
    equivalent(&harness);
    let fences = harness
        .store()
        .account_read(|tx| tx.active_pool_fences("synthetic"))
        .unwrap();
    assert_eq!(
        fences.len(),
        1,
        "refusal must be accepted as confirmed quota evidence"
    );
    let next = admit(&harness, &head, "after-limit", false).await;
    assert_eq!(next["account"]["account_id"], B);
    assert_ne!(next["pin"]["id"], first["pin"]["id"]);
    assert_eq!(
        next["pin"]["generation"].as_u64().unwrap(),
        first["pin"]["generation"].as_u64().unwrap() + 1
    );
}

#[tokio::test]
async fn new_head_avoids_full_reading() {
    let _clock = crate::InjectedClock::set(NOW);
    for minutes in [300, 10_080] {
        let harness = fixture(100.0, minutes);
        let head = harness
            .bind(reserved("class-runner"), Some("new-head"))
            .await;
        let first = admit(&harness, &head, "new", false).await;
        assert_eq!(first["account"]["account_id"], B);
    }
}

#[tokio::test]
async fn sticky_worker_still_retains_past_keep_free_margin() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = fixture(69.0, 10_080);
    let worker = harness.bind(reserved("class-runner"), None).await;
    let first = admit(&harness, &worker, "first", true).await;
    assert_eq!(first["account"]["account_id"], A);
    readings(70.0, 10_080);
    equivalent(&harness);
    let at_margin = admit(&harness, &worker, "at-margin", false).await;
    assert_eq!(at_margin["pin"], first["pin"]);
    readings(71.0, 10_080);
    equivalent(&harness);
    let next = admit(&harness, &worker, "past-margin", false).await;
    assert_eq!(next["account"]["account_id"], A);
    assert_eq!(next["pin"], first["pin"]);
    let mut fresh = request("new-worker");
    fresh["conversation_key"] = json!("new-worker");
    let fresh = worker.call("route.account_for", &fresh).await;
    equivalent(&harness);
    assert_eq!(response(&fresh)["account"]["account_id"], B);
}

#[tokio::test]
async fn sticky_head_still_moves_on_non_quota_credit_block() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = fixture(10.0, 300);
    let head = harness.bind(reserved("class-runner"), Some("head")).await;
    let first = admit(&harness, &head, "first", true).await;
    readings(99.0, 300);
    equivalent(&harness);
    let observed = head
        .call(
            "route.account_observe",
            &json!({
                "event_id":"credit", "kind":"account_limit_exhausted", "admission_id":"first",
                "limit_window":"credit", "retry_at_ms":NOW+60_000
            }),
        )
        .await;
    response(&observed);
    equivalent(&harness);
    let next = admit(&harness, &head, "after-credit", false).await;
    assert_eq!(next["account"]["account_id"], B);
    assert_eq!(
        next["pin"]["generation"].as_u64().unwrap(),
        first["pin"]["generation"].as_u64().unwrap() + 1
    );
}
