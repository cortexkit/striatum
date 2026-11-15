use super::*;
use cortexkit_role_routing::errors::{AccountUnservableReason, Refusal};

#[tokio::test]
async fn continuation_declared_unservable_preparation_refuses_for_every_reason() {
    for reason in [
        AccountUnservableReason::CandidatesDisjoint,
        AccountUnservableReason::BoundTierUnavailable,
        AccountUnservableReason::StoredStateInvalid,
        AccountUnservableReason::AllocatorInvariant,
        AccountUnservableReason::NoEligibleAccount,
    ] {
        let (_subject, _, runner, mut request) = paused().await;
        let id = format!("unservable-{reason:?}");
        request["selection_id"] = json!(id);
        let refusal =
            Refusal::account_unservable(reason, vec!["offered".into()], vec!["eligible".into()]);
        let detail = json!(refusal.detail);
        account_selection::inject_continuation_preparation_failure(
            &id,
            account_selection::OpReply::typed(
                refusal.code.as_str(),
                refusal.message,
                detail.clone(),
            ),
        );
        let answer = result(runner.call("route.account_for", &request).await);
        assert_eq!(
            answer,
            json!({"kind":"refuse", "routing_code":"account_unservable", "detail":detail})
        );
        assert_eq!(
            result(runner.call("route.account_for", &request).await),
            answer
        );
    }
}

#[tokio::test]
async fn stable_refusal_continuation_unknown_stored_tier_refuses() {
    let (subject, _, runner, request) = paused().await;
    subject
        .harness()
        .store()
        .record_service_tier(
            request["previous_decision_id"].as_str().unwrap(),
            "future-tier",
            &[],
        )
        .unwrap();
    let answer = result(runner.call("route.account_for", &request).await);
    assert_eq!(
        answer,
        json!({"kind":"refuse", "routing_code":"account_unservable",
        "detail":{"reason":"stored_state_invalid"}})
    );
}
