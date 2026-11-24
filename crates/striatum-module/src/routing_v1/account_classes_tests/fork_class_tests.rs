//! A fork runs on its parent's exact login, but only when that login may serve
//! the child's own routing decision.

use super::*;

async fn decision(caller: &super::super::conformance_tests::Route, consumer: &str) -> Value {
    let reply = caller
        .call(
            "route.select",
            &json!({"consumer":consumer,"requirements":{"iq":30,"eq":0}}),
        )
        .await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"]["decision_id"].clone()
}

fn fork(id: &str, decision: &Value) -> Value {
    let mut request = account_request("synthetic", id);
    request["fork_of"] = json!({"admission_id":"parent"});
    request["decision_id"] = decision.clone();
    request
}

/// The parent runs on the subscription login. A child whose decision allows
/// only pay-per-token accounts must not be admitted on that subscription
/// login, and must not be moved to the API key either: the fork is refused.
/// A child whose decision allows the subscription class still forks onto it.
#[tokio::test]
async fn fork_of_refuses_a_parent_login_outside_the_child_decisions_account_class() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    oauth_reading(10.0);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    assert_credential(
        &runner
            .call("route.account_for", &account_request("synthetic", "parent"))
            .await,
        "oauth:test",
    );

    let paid = decision(&caller, "codemode").await;
    let reply = runner
        .call("route.account_for", &fork("paid-fork", &paid))
        .await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "account_unservable", "{}", reply.1);
    assert_eq!(
        reply.1["detail"]["reason"], "no_eligible_account",
        "{}",
        reply.1
    );
    assert!(harness
        .store()
        .v1_admission(&format!("reserved:{RUNNER}"), "paid-fork")
        .unwrap()
        .is_none());

    let subscription = decision(&caller, "mason").await;
    assert_credential(
        &runner
            .call(
                "route.account_for",
                &fork("subscription-fork", &subscription),
            )
            .await,
        "oauth:test",
    );
    crate::account_state::assert_published_equals_fresh_load(harness.store(), "fork class");
}
