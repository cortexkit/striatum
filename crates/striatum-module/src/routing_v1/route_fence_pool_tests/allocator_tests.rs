use super::*;
use crate::routing_v1::conformance_tests::Route;
use striatum_core::accounts::{ApplicableWindow, Decision, ExclusionReason};
use striatum_core::route_state_store::{FactSource, RouteStateStore};

const FETCHED: i64 = NOW - 60_000;
const LIVE_OPEN: [f64; 4] = [0.0, 0.0, 1.17, 0.59];
const LIVE_SPENT: [f64; 4] = [23.63, 8.22, 74.3, 53.86];

fn publish(open: [f64; 4], spent: [f64; 4]) {
    publish_accounts(&[(OPEN_ACCOUNT, open), (SPENT_ACCOUNT, spent)]);
}

fn publish_accounts(accounts: &[(&str, [f64; 4])]) {
    let entry = |account: &str, used: [f64; 4]| {
        let windows: Vec<_> = [
            ("gemini-5h", 300),
            ("gemini-weekly", 10080),
            ("3p-5h", 300),
            ("3p-weekly", 10080),
        ]
        .into_iter()
        .zip(used)
        .map(|((id, minutes), used)| window(id, used, minutes, NOW + minutes * 60_000))
        .collect();
        // Insula publishes the Code Assist pools without the vendor of its own API.
        json!({"provider":"antigravity", "account":account, "source":"vault",
            "fetchedAt":crate::account_selection::rfc3339(FETCHED),
            "usage":{"extraRateWindows":windows}})
    };
    let entries: Vec<_> = accounts.iter().map(|&(id, used)| entry(id, used)).collect();
    let body = json!({"result":entries,
        "completeProviders":["antigravity"]});
    let mut state = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_provider_reply(
        &mut state,
        "antigravity",
        &serde_json::to_vec(&body).unwrap(),
        NOW,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
}

fn request(model: &str, id: &str, bytes: u64) -> Value {
    json!({"provider":"google", "model":model, "admission_id":id, "conversation_key":id,
        "initial_input_bytes":bytes, "request_input_tokens":bytes.div_ceil(4), "hold_wait_max_s":0})
}

fn only_open(request: &mut Value) {
    request["candidates"] = json!({"view_digest":"two-google-sign-ins",
        "accounts":[{"account_id":OPEN_ACCOUNT, "credential_id":OPEN}]});
}

async fn admit(runner: &Route, request: &Value) -> Value {
    let (ty, reply) = runner.call("route.account_for", request).await;
    assert_eq!(ty, subc_protocol::FrameType::Response, "{reply}");
    reply["result"].clone()
}

fn decision(harness: &Harness, reply: &Value) -> Decision {
    let id = reply["reason"]["decision_id"].as_str().unwrap();
    let row = harness.store().account_decision(id).unwrap().unwrap();
    serde_json::from_str(&row.decision_json).unwrap()
}

fn assert_pool_weights(decision: &Decision, account: &str, used: [f64; 2]) {
    let note = decision.account_note(account).unwrap();
    let windows: Vec<_> = note
        .window_weights
        .iter()
        .map(|window| (window.window, window.used_percent))
        .collect();
    assert_eq!(
        windows,
        [
            (ApplicableWindow::FiveHour, used[0]),
            (ApplicableWindow::SevenDay, used[1]),
        ],
        "{account} must be scored on exactly the requested pool: {note:?}"
    );
    assert!(!note.exclusions.contains(&ExclusionReason::QuotaMissing));
}

#[tokio::test]
async fn cold_antigravity_gemini_uses_headroom_when_readings_arrive() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let harness = setup();
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut seed = request(CARBON, "existing-load", 65_536);
    only_open(&mut seed);
    assert_eq!(
        admit(&runner, &seed).await["account"]["account_id"],
        OPEN_ACCOUNT
    );

    let fallback = admit(&runner, &request(CARBON, "no-readings", 50_000)).await;
    assert_eq!(fallback["account"]["account_id"], SPENT_ACCOUNT);
    let fallback = decision(&harness, &fallback);
    for (account, pending) in [(OPEN_ACCOUNT, 65_536), (SPENT_ACCOUNT, 0)] {
        let note = fallback.account_note(account).unwrap();
        assert_eq!(note.exclusions, [ExclusionReason::QuotaMissing]);
        assert!(note.window_weights.is_empty());
        assert_eq!(note.weight, Some(1.0));
        assert_eq!(note.pending_bytes, Some(pending));
    }
    assert_eq!(fallback.admission().unwrap().quota_observed_at_ms, None);

    publish(LIVE_OPEN, LIVE_SPENT);
    let answer = admit(&runner, &request(CARBON, "gemini-with-quota", 250_000)).await;
    let recorded = decision(&harness, &answer);
    assert_eq!(
        answer["account"]["account_id"], OPEN_ACCOUNT,
        "{recorded:?}"
    );
    assert_pool_weights(&recorded, OPEN_ACCOUNT, [0.0, 0.0]);
    assert_pool_weights(&recorded, SPENT_ACCOUNT, [23.63, 8.22]);
    assert_eq!(
        recorded.admission().unwrap().quota_observed_at_ms,
        Some(FETCHED)
    );
    let open = recorded.account_note(OPEN_ACCOUNT).unwrap();
    let spent = recorded.account_note(SPENT_ACCOUNT).unwrap();
    assert!(open.pending_bytes.unwrap() > spent.pending_bytes.unwrap());
    assert!(open.score.unwrap() < spent.score.unwrap());
}

#[tokio::test]
async fn cold_antigravity_claude_scores_only_third_party_windows() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let harness = setup_models(&[CLAUDE]);
    let runner = harness.bind(reserved(RUNNER), None).await;
    publish(LIVE_OPEN, LIVE_SPENT);

    let answer = admit(&runner, &request(CLAUDE, "claude-with-quota", 50_000)).await;
    let recorded = decision(&harness, &answer);
    assert_pool_weights(&recorded, OPEN_ACCOUNT, [1.17, 0.59]);
    assert_pool_weights(&recorded, SPENT_ACCOUNT, [74.3, 53.86]);
    assert_eq!(answer["account"]["account_id"], OPEN_ACCOUNT);
    let open = recorded.account_note(OPEN_ACCOUNT).unwrap();
    let spent = recorded.account_note(SPENT_ACCOUNT).unwrap();
    assert!(open.score.unwrap() < spent.score.unwrap());
    let policy = harness.operator_config().allocator_policy("google");
    assert_eq!(open.window_weights[0].reserve, policy.reserve_five_hour);
    assert_eq!(open.window_weights[1].reserve, policy.reserve_seven_day);
}

#[tokio::test]
async fn spent_antigravity_pool_excludes_cold_but_not_sticky_work() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let harness = setup_models(&[CARBON, CLAUDE]);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut first = request(CARBON, "sticky", 50_000);
    only_open(&mut first);
    let first = admit(&runner, &first).await;
    assert_eq!(first["account"]["account_id"], OPEN_ACCOUNT);

    // Each account is spent on only one pool. Poll readings alone must
    // neither move the sticky conversation nor fence the other pool.
    publish([100.0, 0.0, 10.0, 10.0], [10.0, 10.0, 10.0, 100.0]);
    for (model, chosen, exhausted, period) in [
        (
            CARBON,
            SPENT_ACCOUNT,
            OPEN_ACCOUNT,
            ApplicableWindow::FiveHour,
        ),
        (
            CLAUDE,
            OPEN_ACCOUNT,
            SPENT_ACCOUNT,
            ApplicableWindow::SevenDay,
        ),
    ] {
        let answer = admit(&runner, &request(model, model, 50_000)).await;
        let recorded = decision(&harness, &answer);
        assert_eq!(answer["account"]["account_id"], chosen, "{recorded:?}");
        let note = recorded.account_note(exhausted).unwrap();
        assert!(
            note.exclusions.contains(&ExclusionReason::BelowReserve {
                windows: vec![period],
            }),
            "own pool exhausted: {note:?}"
        );
        assert!(recorded.account_note(chosen).unwrap().exclusions.is_empty());
    }
    let mut follow_up = request(CARBON, "sticky-follow-up", 50_000);
    follow_up["conversation_key"] = json!("sticky");
    let follow_up = admit(&runner, &follow_up).await;
    assert_eq!(follow_up["account"], first["account"]);
    assert_eq!(follow_up["pin"], first["pin"]);
}

#[tokio::test]
async fn antigravity_reading_requires_one_catalog_provider_and_exact_account_id() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let ambiguous = setup();
    ambiguous
        .store()
        .upsert_fact(
            &crate::tests::fact("second-provider", CARBON),
            FactSource::Override,
        )
        .unwrap();
    let runner = ambiguous.bind(reserved(RUNNER), None).await;
    publish(LIVE_OPEN, LIVE_SPENT);
    let answer = admit(&runner, &request(CARBON, "ambiguous-provider", 50_000)).await;
    let recorded = decision(&ambiguous, &answer);
    for account in [OPEN_ACCOUNT, SPENT_ACCOUNT] {
        let note = recorded.account_note(account).unwrap();
        assert_eq!(note.exclusions, [ExclusionReason::QuotaMissing]);
        assert!(note.window_weights.is_empty());
    }

    let unique = setup();
    let runner = unique.bind(reserved(RUNNER), None).await;
    publish_accounts(&[(OPEN_ACCOUNT, LIVE_OPEN), ("unlisted-account", LIVE_SPENT)]);
    let answer = admit(&runner, &request(CARBON, "unique-provider", 50_000)).await;
    let recorded = decision(&unique, &answer);
    let unmatched = recorded.account_note(SPENT_ACCOUNT).unwrap();
    assert_eq!(unmatched.exclusions, [ExclusionReason::QuotaMissing]);
    assert!(unmatched.window_weights.is_empty());
    assert!(recorded.account_note("unlisted-account").is_none());
    assert_pool_weights(&recorded, OPEN_ACCOUNT, [0.0, 0.0]);
}
