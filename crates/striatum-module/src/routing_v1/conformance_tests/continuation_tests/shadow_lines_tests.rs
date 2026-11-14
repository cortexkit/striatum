use super::*;

fn pressure() -> Vec<Value> {
    crate::take_operator_infos()
        .into_iter()
        .filter_map(|line| {
            line.strip_prefix("routing account concurrency would refuse ")
                .map(|body| serde_json::from_str(body).unwrap())
        })
        .collect()
}

#[tokio::test]
async fn account_shadow_continue_counts_full_replacements_once_but_not_head_or_replay() {
    let _clock = crate::InjectedClock::set(1_790_683_200_000);
    for head in [false, true] {
        let (subject, _, runner, request) = paused_in_scope(false, head.then_some("head")).await;
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
            config["concurrency"] = json!({"providers":{
                fixture::SUBSCRIPTION_PROVIDER:{"limit":1}
            }});
        });
        let mut replacements = Vec::new();
        for credential in credentials {
            if credential.account.account_id == request["exhausted"]["account"] {
                continue;
            }
            let account = credential.account;
            let body = json!({"provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
                "admission_id":format!("fill-{}",account.account_id),"conversation_key":format!("fill-{}",account.account_id),
                "candidates":{"view_digest":"one-replacement","accounts":[{
                    "account_id":account.account_id,"credential_id":account.credential_id}]}});
            result(runner.call("route.account_for", &body).await);
            replacements.push(account.account_id);
        }
        replacements.sort();
        pressure();
        // Shadow evaluation describes the offer that would exceed capacity;
        // it must not change the offer, exhaustion evidence or replay record.
        let offer = result(runner.call("route.account_for", &request).await);
        assert_eq!(offer["kind"], "offer");
        let events = pressure();
        assert_eq!(events.len(), usize::from(!head), "{events:?}");
        if !head {
            assert_eq!(
                events[0],
                json!({"surface":"routing/v1","consumer":fixture::SUMMARY,
                "model":"model-c","accounts":replacements.iter().map(|account|
                    json!({"account_id":account,"count":1,"cap":1})).collect::<Vec<_>>()})
            );
        }
        assert_eq!(
            result(runner.call("route.account_for", &request).await),
            offer
        );
        assert!(pressure().is_empty());
        let state = crate::account_state::snapshot(harness.store()).unwrap();
        assert_eq!(
            state.inflight.count(
                fixture::SUBSCRIPTION_PROVIDER,
                offer["account"].as_str().unwrap(),
                crate::account_state::InflightScope::Account,
                account_selection::now_ms()
            ),
            2
        );
    }
}
