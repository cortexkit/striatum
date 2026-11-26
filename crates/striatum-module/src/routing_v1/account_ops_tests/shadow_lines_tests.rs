use super::*;

fn events() -> (Vec<Value>, Vec<Value>) {
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

fn caps(harness: &Harness) {
    harness.edit_config(|config| {
        config["concurrency"] = json!({"providers":{"synthetic":{"limit":1}}});
    });
}

#[tokio::test]
async fn account_shadow_marks_scope_independently_of_authorization_and_logs_refused_calls() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    hosts(&[("replaceable-host", true, true)]);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let head_runner = harness.bind(reserved("class-runner"), Some("head")).await;
    let head = session(&harness, "replaceable-host", "head-session", "head").await;
    let denied = harness.bind(reserved("outsider"), None).await;
    events();
    response(&runner.call("route.account_for", &request("worker")).await);
    response(
        &head_runner
            .call("route.account_for", &request("runner-head"))
            .await,
    );
    response(&head.call("route.account_for", &request("scope-head")).await);
    assert_eq!(
        denied.call("route.account_for", &request("denied")).await.1["code"],
        "account_not_authorized"
    );
    let (calls, pressure) = events();
    assert!(pressure.is_empty());
    assert_eq!(calls.len(), 4);
    for (call, head_call, owner) in [
        (&calls[0], false, Value::Null),
        (&calls[1], true, json!("reserved:prefrontal")),
        (&calls[2], true, json!("reserved:replaceable-host")),
        (&calls[3], false, Value::Null),
    ] {
        assert_eq!(call["surface"], "routing/v1");
        assert_eq!(call["head_call"], head_call);
        assert_eq!(call["scope_owner"], owner);
        assert_eq!(
            call["scope_kind"],
            if head_call {
                json!("head")
            } else {
                Value::Null
            }
        );
        assert!(call["harness"].is_null());
        assert!(call["consumer"].is_null());
    }
    assert_eq!(calls[1]["principal"], "reserved:class-runner");
    assert_eq!(calls[1]["runner_module"], "class-runner");
    assert_eq!(calls[2]["principal"], "reserved:bridge-relay");
    assert!(calls[2]["runner_module"].is_null());
}

#[tokio::test]
async fn account_shadow_consumers_and_declared_harness_do_not_change_admission_identity() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let owner = harness.bind(reserved("class-owner"), None).await;
    let runner = harness.bind(reserved("class-runner"), None).await;
    for consumer in ["mason", "codemode"] {
        let selected = owner
            .call(
                "route.select",
                &json!({"consumer":consumer,"requirements":{"iq":30,"eq":0}}),
            )
            .await;
        let decision = response(&selected)["decision_id"].clone();
        let mut body = request(consumer);
        body["conversation_key"] = json!(consumer);
        body["decision_id"] = decision;
        events();
        let admitted = runner.call("route.account_for", &body).await;
        response(&admitted);
        let (calls, _) = events();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["consumer"], consumer);
        assert!(calls[0]["harness"].is_null());
        // The caller-bearing surface accepts harness as metadata. Replaying
        // only that metadata must keep the same admission and stored reply.
        body["admission_id"] = json!(format!("legacy-{consumer}"));
        body["caller"] = json!({"runner":"class-runner","harness":"pi"});
        let first = runner.call("route.account_for", &body).await;
        response(&first);
        body["caller"]["harness"] = json!("opencode2");
        let replay = runner.call("route.account_for", &body).await;
        assert_eq!(replay, first);
        let (calls, pressure) = events();
        assert_eq!(calls.len(), 2);
        assert!(pressure.is_empty());
        for (call, harness) in calls.iter().zip(["pi", "opencode2"]) {
            assert_eq!(call["surface"], "route.account_for");
            assert_eq!(call["consumer"], consumer);
            assert_eq!(call["harness"], harness);
            assert_eq!(call["head_call"], false);
            assert!(call["scope_kind"].is_null());
        }
    }
}

#[tokio::test]
async fn account_shadow_counts_ten_calls_but_not_head_replay_renew_or_allocator_refusal() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness);
    let runner = harness.bind(reserved("class-runner"), None).await;
    let initial = runner.call("route.account_for", &request("first")).await;
    response(&initial);
    events();
    let mut last = Value::Null;
    for n in 0..10 {
        let mut body = request(&format!("burst-{n}"));
        body["conversation_key"] = json!(format!("burst-{n}"));
        last = body;
        response(&runner.call("route.account_for", &last).await);
    }
    let (calls, pressure) = events();
    assert_eq!(calls.len(), 10);
    assert_eq!(pressure.len(), 10);
    for (n, event) in pressure.iter().enumerate() {
        assert_eq!(
            event,
            &json!({"surface":"routing/v1","consumer":"class-runner","class":"worker","model":"model",
            "accounts":[{"account_id":"oauth-account","count":n+1,"cap":1}]})
        );
    }
    response(&runner.call("route.account_for", &last).await);
    let mut renew = request("renew");
    renew["mode"] = json!("renew");
    renew["expected_pin"] = json!({"id":response(&initial)["pin"]["id"],"generation":response(&initial)["pin"]["generation"]});
    response(&runner.call("route.account_for", &renew).await);
    let head = harness.bind(reserved("class-runner"), Some("head")).await;
    response(&head.call("route.account_for", &request("head")).await);
    let mut legacy = request("legacy-head-route");
    legacy["caller"] = json!({"runner":"class-runner","harness":"pi"});
    response(&head.call("route.account_for", &legacy).await);
    let mut refused = request("no-candidates");
    refused["candidates"] = json!({"view_digest":"empty","accounts":[]});
    assert_eq!(
        runner.call("route.account_for", &refused).await.0,
        FrameType::Error
    );
    let (calls, pressure) = events();
    assert_eq!(calls.len(), 5);
    assert_eq!(calls[3]["head_call"], false);
    assert_eq!(calls[3]["harness"], "pi");
    assert_eq!(pressure.len(), 1, "{pressure:?}");
    assert_eq!(pressure[0]["surface"], "route.account_for");
}

#[tokio::test]
async fn account_shadow_kept_affinity_is_full_even_with_a_free_eligible_sibling() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness);
    super::super::account_classes_tests::oauth_reading(10.0);
    harness.edit_config(|config| config["providers"]["synthetic"]["subscription"] = json!(true));
    let runner = harness.bind(reserved("class-runner"), None).await;
    let mut first = request("first");
    first["candidates"] = json!({"view_digest":"oauth-only","accounts":[{"account_id":"oauth-account","credential_id":"oauth:test"}]});
    response(&runner.call("route.account_for", &first).await);
    events();
    let retained = runner.call("route.account_for", &request("retained")).await;
    assert_eq!(
        response(&retained)["account"]["account_id"],
        "oauth-account"
    );
    let mut cold = request("cold");
    cold["conversation_key"] = json!("cold");
    cold["warm_credential"] = json!("oauth:test");
    let imported = runner.call("route.account_for", &cold).await;
    assert_eq!(response(&imported)["reason"]["code"], "imported_affinity");
    let (_, pressure) = events();
    assert_eq!(pressure.len(), 1, "{pressure:?}");
    assert_eq!(
        pressure[0]["accounts"],
        json!([{"account_id":"oauth-account","count":1,"cap":1}])
    );
    let mut sibling = request("sibling");
    sibling["conversation_key"] = json!("sibling");
    sibling["candidates"] = json!({"view_digest":"apikey-only","accounts":[
        {"account_id":"apikey-account","credential_id":"apikey:test"}]});
    response(&runner.call("route.account_for", &sibling).await);
    let mut full = request("all-full");
    full["conversation_key"] = json!("all-full");
    response(&runner.call("route.account_for", &full).await);
    let (_, pressure) = events();
    assert_eq!(pressure.len(), 1);
    assert_eq!(
        pressure[0]["accounts"],
        json!([
            {"account_id":"apikey-account","count":1,"cap":1},
            {"account_id":"oauth-account","count":3,"cap":1}
        ])
    );
}

#[tokio::test]
async fn priority_select_calls_log_one_class_and_head_mark_on_the_select_surface() {
    async fn capture(consumer: &str, declared: Option<&str>) -> Vec<(FrameType, Vec<u8>)> {
        let _clock = crate::InjectedClock::set(NOW);
        // Restart generated identifiers as well as the clock so the comparison
        // covers real handler bytes, not a normalized reply with fields removed.
        let _inputs = striatum_store::replay::Inputs::new(NOW as i64).install();
        let (harness, _) = setup("synthetic", false);
        harness.edit_config(|config| config["head_agents"] = json!(["class-owner.codemode"]));
        let mut declaration = json!({"name":consumer});
        if let Some(class) = declared {
            declaration["priority"] = json!(class);
        }
        crate::routing_v1::consumers::replace_catalog(&json!([{
            "module_id":"class-owner","nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[declaration]}}
        }]));
        let owner = harness.bind(reserved("class-owner"), None).await;
        let runner = harness.bind(reserved("class-runner"), None).await;
        let class = declared.unwrap_or("worker");
        let mut answers = Vec::new();
        for legacy in [false] {
            crate::take_operator_infos();
            let params = if legacy {
                json!({"targetAgent":consumer,"substrate":"broca","executor":"broca",
                    "requirements":{"iq":30,"eq":0}})
            } else {
                json!({"consumer":consumer,"requirements":{"iq":30,"eq":0}})
            };
            let selected = owner.call_raw("route.select", &params).await;
            assert_eq!(selected.0, FrameType::Response);
            let body: Value = serde_json::from_slice(&selected.1).unwrap();
            let decision =
                body["result"][if legacy { "decisionID" } else { "decision_id" }].clone();
            answers.push(selected);
            let lines = crate::take_operator_infos();
            let calls: Vec<Value> = lines
                .iter()
                .filter_map(|line| {
                    line.strip_prefix("routing select call ")
                        .map(|body| serde_json::from_str(body).unwrap())
                })
                .collect();
            assert_eq!(
                calls,
                vec![json!({
                    "surface":if legacy {"route.select"} else {"routing/v1"},
                    "consumer":consumer,"class":class,"head_request":class == "interactive",
                    "pinned":false,"pin_origin":null
                })]
            );
            let mut admission = request(&format!("unchanged-{legacy}"));
            admission["conversation_key"] = admission["admission_id"].clone();
            admission["decision_id"] = decision;
            if legacy {
                admission["caller"] = json!({"runner":"class-runner"});
            }
            let admitted = runner.call_raw("route.account_for", &admission).await;
            assert_eq!(admitted.0, FrameType::Response, "{:?}", admitted.1);
            answers.push(admitted);
        }
        answers
    }
    for (consumer, class) in [
        ("class-owner.mason", "interactive"),
        ("class-owner.mason", "blocking"),
        ("class-owner.mason", "background"),
        ("class-owner.codemode", "worker"),
    ] {
        assert_eq!(
            capture(consumer, None).await,
            capture(consumer, Some(class)).await
        );
    }
}

#[tokio::test]
async fn select_call_line_reports_the_pin_and_its_origin_as_sent() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    crate::routing_v1::consumers::replace_catalog(&json!([{
        "module_id":"class-owner","nonce_verified":true,
        "role_declarations":{"routing/v1":{"consumers":[{"name":"class-owner.mason"}]}}
    }]));
    let owner = harness.bind(reserved("class-owner"), None).await;
    crate::take_operator_infos();
    // The line is written before the pin is resolved, so an unknown model is
    // enough; class-owner isn't enrolled for user pins, and the line still
    // shows what it sent.
    owner
        .call_raw(
            "route.select",
            &json!({"consumer":"class-owner.mason","requirements":{"iq":30,"eq":0},
                "pin":{"model":"nobody/unknown-model"},"pin_origin":"user"}),
        )
        .await;
    let calls: Vec<Value> = crate::take_operator_infos()
        .iter()
        .filter_map(|line| {
            line.strip_prefix("routing select call ")
                .map(|body| serde_json::from_str(body).unwrap())
        })
        .collect();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0]["pinned"], json!(true));
    assert_eq!(calls[0]["pin_origin"], json!("user"));
}

#[tokio::test]
async fn priority_account_pressure_uses_bound_consumer_and_unbound_worker() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    caps(&harness);
    crate::routing_v1::consumers::replace_catalog(&json!([{
        "module_id":"class-owner","nonce_verified":true,
        "role_declarations":{"routing/v1":{"consumers":[
            {"name":"class-owner.mason","priority":"interactive"}
        ]}}
    }, {"module_id":"class-runner","nonce_verified":true,"role_declarations":{"routing/v1":{"consumers":[{"name":"class-runner","priority":"interactive"}]}}}]));
    let owner = harness.bind(reserved("class-owner"), None).await;
    let runner = harness.bind(reserved("class-runner"), None).await;
    response(&runner.call("route.account_for", &request("seed")).await);
    let selected = owner
        .call(
            "route.select",
            &json!({"consumer":"class-owner.mason","requirements":{"iq":30,"eq":0}}),
        )
        .await;
    let decision = response(&selected)["decision_id"].clone();
    for legacy in [false, true] {
        for bound in [false, true] {
            let mut body = request(&format!("priority-{legacy}-{bound}"));
            body["conversation_key"] = body["admission_id"].clone();
            if legacy {
                body["caller"] = json!({"runner":"class-runner"});
            }
            if bound {
                body["decision_id"] = decision.clone();
            }
            events();
            response(&runner.call("route.account_for", &body).await);
            let (calls, pressure) = events();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0]["head_call"], false);
            assert_eq!(pressure.len(), 1, "{pressure:?}");
            assert_eq!(
                pressure[0]["class"],
                if bound { "interactive" } else { "worker" }
            );
            assert_eq!(
                pressure[0]["consumer"],
                if bound {
                    "class-owner.mason"
                } else {
                    "class-runner"
                }
            );
        }
    }
}
