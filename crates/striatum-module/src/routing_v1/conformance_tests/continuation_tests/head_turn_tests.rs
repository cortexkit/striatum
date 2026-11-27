use super::*;

#[tokio::test]
async fn head_turn_continue_admits_full_replacement_and_holds_a_slot() {
    let _clock = crate::InjectedClock::set(1_790_683_200_000);
    let (subject, _, runner, mut request) = paused().await;
    let harness = subject.harness();
    let credentials: Vec<_> = fixture::continue_credentials()
        .into_iter()
        .filter(|credential| {
            ["account-1", "account-2"].contains(&credential.account.account_id.as_str())
        })
        .collect();
    subject
        .configure_continue_credentials(&credentials)
        .await
        .unwrap();
    harness.edit_config(|config| {
        config["concurrency"] = json!({"enforcement_enabled":true,"providers":{
            fixture::SUBSCRIPTION_PROVIDER:{"limit":1}
        }});
    });
    let replacement = credentials
        .iter()
        .find(|credential| credential.account.account_id != request["exhausted"]["account"])
        .unwrap();
    let account = &replacement.account;
    let body = json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
        "admission_id":"fill-replacement","conversation_key":"fill-replacement",
        "candidates":{"view_digest":"replacement-only","accounts":[{
            "account_id":account.account_id,"credential_id":account.credential_id}]}});
    result(runner.call("route.account_for", &body).await);

    // Check the ordinary call before the head turn gives this conversation
    // a slot on the replacement account.
    let mut ordinary = request.clone();
    ordinary["selection_id"] = json!("ordinary-continue");
    ordinary["head_turn"] = json!(false);
    let refused = result(runner.call("route.account_for", &ordinary).await);
    assert_ne!(refused["kind"], "offer", "{refused}");

    crate::take_operator_infos();
    request["head_turn"] = json!(true);
    let offer = result(runner.call("route.account_for", &request).await);
    assert_eq!(offer["kind"], "offer", "{offer}");
    assert_eq!(offer["account"], account.account_id);
    let calls: Vec<Value> = crate::take_operator_infos()
        .iter()
        .filter_map(|line| {
            line.strip_prefix("routing account call ")
                .map(|body| serde_json::from_str(body).unwrap())
        })
        .collect();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["head_call"], true);
    assert_eq!(calls[0]["head_call_source"], "head_turn");
    let state = crate::account_state::snapshot(harness.store()).unwrap();
    assert_eq!(
        state.inflight.count(
            fixture::SUBSCRIPTION_PROVIDER,
            &account.account_id,
            crate::account_state::InflightScope::Account,
            account_selection::now_ms()
        ),
        2
    );
}
