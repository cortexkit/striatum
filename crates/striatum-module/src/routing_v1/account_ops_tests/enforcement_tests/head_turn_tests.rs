//! Runner declarations exempt only concurrency refusal on both account surfaces.

use super::*;

#[tokio::test]
async fn head_turn_auto_enrolled_first_party_runner_is_trusted_on_both_surfaces() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        let (harness, _) = setup("synthetic", false);
        caps(&harness, 1, true);
        let runner = harness.bind(reserved("class-runner"), None).await;
        fill(&runner, A, "full", 1).await;
        harness.edit_config(|config| config["runners"] = json!([]));
        {
            let shared = harness.credentials();
            let mut credentials = shared.lock().unwrap();
            crate::routing_v1::runners::DescribeCache::default().catalog(
                &json!([{"module_id":"class-runner","first_party":true,
                "incarnation":"daemon-a","spawn":7,
                "role_declarations":{"routing/v1":{"runner":{}}}}]),
                &[],
                &mut credentials,
                NOW as i64,
            );
        }
        assert!(harness.operator_config().v1_runners().is_empty());
        lines();
        let admitted = admitted_on(&runner, &flagged(on(A, "automatic-head"), legacy)).await;
        assert_eq!(admitted["account"]["account_id"], A);
        assert_eq!(count(&harness, A, NOW), 2);
        let (calls, pressure) = lines();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["head_call_source"], "head_turn");
        assert!(pressure.is_empty(), "{pressure:?}");
    }
}

fn account_choice(admitted: &Value) -> Value {
    let account = &admitted["account"];
    json!({"provider":account["provider"],"account_id":account["account_id"],
        "credential_id":account["credential_id"],"method":account["method"]})
}

fn flagged(mut body: Value, legacy: bool) -> Value {
    body["head_turn"] = json!(true);
    if legacy {
        body["caller"] = json!({"runner":"class-runner"});
    }
    body
}

#[tokio::test]
async fn head_turn_enrolled_runner_is_admitted_over_cap_and_still_counts_on_both_surfaces() {
    let _clock = crate::InjectedClock::set(NOW);
    let mut surfaces = Vec::new();
    for legacy in [false, true] {
        let (harness, _) = setup("synthetic", false);
        caps(&harness, 1, true);
        let runner = harness.bind(reserved("class-runner"), None).await;
        fill(&runner, A, "full", 1).await;
        lines();
        let admitted = admitted_on(&runner, &flagged(on(A, "head-turn"), legacy)).await;
        assert_eq!(admitted["account"]["account_id"], A);
        assert_eq!(count(&harness, A, NOW), 2);
        let (calls, pressure) = lines();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["head_call"], true);
        assert_eq!(calls[0]["head_call_source"], "head_turn");
        surfaces.push((
            account_choice(&admitted),
            calls[0]["head_call"].clone(),
            calls[0]["head_call_source"].clone(),
        ));
        assert!(pressure.is_empty(), "{pressure:?}");
        assert_concurrency_full(
            &runner.call("route.account_for", &on(A, "still-full")).await,
            NOW + 30_000,
        );
        assert_eq!(count(&harness, A, NOW), 2);
    }
    assert_eq!(
        surfaces[0], surfaces[1],
        "v1 and legacy head marks and account choices"
    );
}

#[tokio::test]
async fn head_turn_nonrunner_and_direct_caller_are_ignored_not_granted_body_runner_authority() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness, 1, true);
    let runner = harness.bind(reserved("class-runner"), None).await;
    fill(&runner, A, "full", 1).await;
    for principal in [reserved("outsider"), json!({"kind":"direct"})] {
        let caller = harness.bind(principal, None).await;
        for legacy in [true, false] {
            lines();
            let reply = caller
                .call("route.account_for", &flagged(on(A, "untrusted"), legacy))
                .await;
            assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
            if legacy {
                assert_eq!(reply.1["code"], "concurrency_full", "{}", reply.1);
            } else {
                // The routing/v1 path still applies its separate caller authorization.
                assert_eq!(reply.1["code"], "account_not_authorized", "{}", reply.1);
            }
            let (calls, _) = lines();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0]["head_call"], false);
            assert_eq!(calls[0].get("head_call_source"), Some(&Value::Null));
            assert_eq!(count(&harness, A, NOW), 1);
        }
    }
}

#[tokio::test]
async fn head_turn_keeps_account_choice_and_eligibility() {
    let _clock = crate::InjectedClock::set(NOW);
    let mut surface_choices = Vec::new();
    for legacy in [false, true] {
        let mut choices = Vec::new();
        for head_turn in [false, true] {
            let (harness, _) = setup("synthetic", false);
            caps(&harness, 1, true);
            let runner = harness.bind(reserved("class-runner"), None).await;
            fill(&runner, A, "full", 1).await;
            let mut body = flagged(fresh("choice"), legacy);
            body["head_turn"] = json!(head_turn);
            lines();
            let admitted = admitted_on(&runner, &body).await;
            choices.push(account_choice(&admitted));
            assert_eq!(admitted["account"]["account_id"], B);
            let (calls, _) = lines();
            assert_eq!(calls[0]["head_call"], head_turn);
            assert_eq!(
                calls[0].get("head_call_source"),
                Some(&if head_turn {
                    json!("head_turn")
                } else {
                    Value::Null
                })
            );
            // A full eligible account is allowed, but an empty candidate set is not.
            body["admission_id"] = json!("ineligible");
            body["conversation_key"] = json!("ineligible");
            body["candidates"] = json!({"view_digest":"empty","accounts":[]});
            let refused = runner.call("route.account_for", &body).await;
            assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
            assert_eq!(count(&harness, A, NOW) + count(&harness, B, NOW), 2);
        }
        assert_eq!(choices[0], choices[1]);
        surface_choices.push(choices);
    }
    assert_eq!(
        surface_choices[0], surface_choices[1],
        "v1 and legacy account choices"
    );
}

#[tokio::test]
async fn head_turn_enforcement_off_preserves_admission_and_scope_source_wins() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        let mut results = Vec::new();
        for head_turn in [false, true] {
            let (harness, _) = setup("synthetic", false);
            caps(&harness, 1, false);
            let runner = harness.bind(reserved("class-runner"), None).await;
            fill(&runner, A, "full", 1).await;
            let mut body = flagged(on(A, "shadow"), legacy);
            body["head_turn"] = json!(head_turn);
            lines();
            let admitted = admitted_on(&runner, &body).await;
            let mut reason = admitted["reason"].clone();
            reason.as_object_mut().unwrap().remove("decision_id");
            results.push((admitted["account"].clone(), reason));
            assert_eq!(count(&harness, A, NOW), 2);
            let (calls, pressure) = lines();
            assert_eq!(calls[0]["head_call"], head_turn);
            assert_eq!(
                calls[0].get("head_call_source"),
                Some(&if head_turn {
                    json!("head_turn")
                } else {
                    Value::Null
                })
            );
            assert_eq!(pressure.len(), usize::from(!head_turn));
            let head_scope = harness.bind(reserved("class-runner"), Some("head")).await;
            lines();
            admitted_on(&head_scope, &flagged(on(A, "both-sources"), false)).await;
            assert_eq!(lines().0[0]["head_call_source"], "scope");
        }
        assert_eq!(results[0], results[1]);
    }
}
