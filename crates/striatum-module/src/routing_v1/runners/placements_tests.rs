use super::placements;
use crate::routing_v1::conformance_tests::{install_catalog, reserved, Harness};
use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use subc_protocol::FrameType;

fn selected(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert!(reply.1["result"]["runner"].is_object(), "{}", reply.1);
    reply.1["result"].clone()
}

fn refused(reply: (FrameType, Value), reason: &str) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_route", "{}", reply.1);
    assert_eq!(reply.1["detail"]["reason"], reason, "{}", reply.1);
}

fn install(harness: &Harness) {
    let mut catalog = fixture::catalog();
    for (index, model) in catalog.models.iter_mut().enumerate() {
        model.provider = if index < 2 { "anthropic" } else { "openai" }.into();
        model.priced_per_token = true;
    }
    catalog.accounts.clear();
    for runner in &mut catalog.runners {
        runner.route_keys = catalog.models.iter().map(|m| m.route_key()).collect();
    }
    catalog.runners[0].module = "thalamus-harness".into();
    catalog.runners[0].harnesses = Some(vec!["opencode2".into(), "claude-code".into()]);
    catalog.runners[0].default_harness = Some("opencode2".into());
    catalog.runners[1].module = "broca".into();
    install_catalog(harness, &catalog);
}

fn request(pin: &str) -> Value {
    json!({"consumer":"mason","requirements":{"iq":0},"pin":pin})
}

fn entries(harness: &Harness, entries: Value) {
    // Refuse a bad document here rather than letting a retained settings
    // snapshot hide a parsing error from the selection assertions.
    crate::config_overrides::validate_document(&json!({"runners":entries}).to_string()).unwrap();
    harness.edit_config(|config| config["runners"] = entries);
}

#[test]
fn settings_placement_pairs_are_unique_including_absence() {
    for entries in [
        json!([{"module":"r","harness":"a"},{"module":"r","harness":"b"}]),
        json!([{"module":"r","harness":"a"},{"module":"r"}]),
    ] {
        crate::config_overrides::validate_document(&json!({"runners":entries}).to_string())
            .unwrap();
    }
    for entries in [
        json!([{"module":"duplicate-module"},{"module":"duplicate-module"}]),
        json!([{"module":"duplicate-module","harness":"a"},{"module":"duplicate-module","harness":"a"}]),
    ] {
        let error =
            crate::config_overrides::validate_document(&json!({"runners":entries}).to_string())
                .unwrap_err();
        assert!(error.contains("duplicate-module"), "{error}");
        assert!(error.contains("listed once"), "{error}");
    }
}

#[tokio::test]
async fn settings_placement_routes_harness_and_caller_intersect_in_list_order() {
    let harness = Harness::new("entry-order");
    install(&harness);
    entries(
        &harness,
        json!([
            {"module":"thalamus-harness","harness":"claude-code","routes":["anthropic"]},
            {"module":"broca"}
        ]),
    );
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    let answer = selected(
        caller
            .call("route.select", &request("anthropic/model-a"))
            .await,
    );
    assert_eq!(answer["runner"]["module"], "thalamus-harness");
    assert_eq!(answer["runner"]["harness"], "claude-code");
    let answer = selected(
        caller
            .call("route.select", &request("openai/model-c"))
            .await,
    );
    assert_eq!(answer["runner"]["module"], "broca");
    let mut params = request("anthropic/model-a");
    params["harness"] = json!("opencode2");
    refused(
        caller.call("route.select", &params).await,
        "harness_unavailable",
    );
    entries(
        &harness,
        json!([
            {"module":"thalamus-harness","harness":"claude-code","routes":["anthropic/model-b"]},
            {"module":"thalamus-harness","harness":"opencode2","routes":["anthropic/model-a"]},
            {"module":"broca"}
        ]),
    );
    let answer = selected(caller.call("route.select", &params).await);
    assert_eq!(answer["runner"]["harness"], "opencode2");
    let answer = selected(
        caller
            .call("route.select", &request("anthropic/model-b"))
            .await,
    );
    assert_eq!(answer["runner"]["harness"], "claude-code");
    let answer = selected(
        caller
            .call("route.select", &request("openai/model-c"))
            .await,
    );
    assert_eq!(answer["runner"]["module"], "broca");
    harness
        .edit_config(|config| config["consumers"]["mason"]["runner"] = json!("thalamus-harness"));
    let answer = selected(caller.call("route.select", &params).await);
    assert_eq!(answer["runner"]["harness"], "opencode2");
    harness.edit_config(|config| config["consumers"]["mason"]["runner"] = json!("absent-module"));
    refused(
        caller
            .call("route.select", &request("anthropic/model-a"))
            .await,
        "no_available_model",
    );
}

fn declaration(harnesses: Value, limits: Value) -> super::RunnerDeclaration {
    super::parse_declaration(&json!({"runner":{
        "pairs":[{"wire_api":"anthropic.messages","auth_method":"apikey"}, {"wire_api":"anthropic.messages","auth_method":"oauth"}],
        "harnesses":harnesses,
        "harness_limits":limits,
        "follows_account_choice":true
    }}))
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn settings_placement_refusals_warn_once_and_recheck_both_inputs() {
    let harness = Harness::new("entry-refused");
    install(&harness);
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    let mut monitor = placements::Monitor::default();
    let mut warnings = Vec::new();
    for (names, limits) in [
        (json!(["other"]), json!({})),
        (
            json!(["claude-code"]),
            json!({"claude-code":{"starts_sessions":false}}),
        ),
    ] {
        harness
            .credentials()
            .lock()
            .unwrap()
            .runner_declarations
            .insert("thalamus-harness".into(), declaration(names, limits));
        let declarations = harness
            .credentials()
            .lock()
            .unwrap()
            .runner_declarations
            .clone();
        monitor.declarations(&declarations, |warning| warnings.push(warning));
        entries(
            &harness,
            json!([
                {"module":"thalamus-harness","harness":"claude-code"}, {"module":"broca"}
            ]),
        );
        let policies = harness.operator_config().v1_runners();
        monitor.settings(policies.clone(), |warning| warnings.push(warning));
        monitor.settings(policies, |warning| warnings.push(warning));
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("thalamus-harness") && warnings[0].contains("refused"));
        let answer = selected(
            caller
                .call("route.select", &request("anthropic/model-a"))
                .await,
        );
        assert_eq!(answer["runner"]["module"], "broca");
    }
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .insert(
            "thalamus-harness".into(),
            declaration(json!(["claude-code"]), json!({})),
        );
    monitor.declarations(
        &harness.credentials().lock().unwrap().runner_declarations,
        |warning| warnings.push(warning),
    );
    let answer = selected(
        caller
            .call("route.select", &request("anthropic/model-a"))
            .await,
    );
    assert_eq!(answer["runner"]["harness"], "claude-code");
    entries(
        &harness,
        json!([{ "module":"thalamus-harness","harness":"missing" }]),
    );
    monitor.settings(harness.operator_config().v1_runners(), |warning| {
        warnings.push(warning)
    });
    assert_eq!(
        warnings.len(),
        2,
        "a settings change must also be rechecked"
    );
    refused(
        caller
            .call("route.select", &request("anthropic/model-a"))
            .await,
        "harness_unavailable",
    );
}

#[tokio::test]
async fn settings_placement_pending_declaration_is_kept_then_activated() {
    let harness = Harness::new("entry-pending");
    install(&harness);
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .remove("thalamus-harness");
    entries(
        &harness,
        json!([{ "module":"thalamus-harness","harness":"claude-code" }]),
    );
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    let mut monitor = placements::Monitor::default();
    let mut warnings = Vec::new();
    let policies = harness.operator_config().v1_runners();
    assert_eq!(policies.len(), 1, "unannounced placements are retained");
    monitor.settings(policies.clone(), |warning| warnings.push(warning));
    monitor.settings(policies, |warning| warnings.push(warning));
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("kept but skipped"));
    refused(
        caller
            .call("route.select", &request("anthropic/model-a"))
            .await,
        "harness_unavailable",
    );
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .insert(
            "thalamus-harness".into(),
            declaration(json!(["claude-code"]), json!({})),
        );
    monitor.declarations(
        &harness.credentials().lock().unwrap().runner_declarations,
        |warning| warnings.push(warning),
    );
    assert_eq!(warnings.len(), 1);
    let answer = selected(
        caller
            .call("route.select", &request("anthropic/model-a"))
            .await,
    );
    assert_eq!(answer["runner"]["harness"], "claude-code");
}

#[tokio::test]
async fn settings_placement_exclusion_is_warning_only_and_limits_explain_empty_routes() {
    let harness = Harness::new("entry-exclude");
    install(&harness);
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .insert(
            "thalamus-harness".into(),
            declaration(
                json!(["claude-code"]),
                json!({"claude-code":{"exclude":["anthropic","-openai"]}}),
            ),
        );
    entries(
        &harness,
        json!([{ "module":"thalamus-harness","harness":"claude-code","routes":["anthropic"] }]),
    );
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    let mut monitor = placements::Monitor::default();
    let mut warnings = Vec::new();
    monitor.declarations(
        &harness.credentials().lock().unwrap().runner_declarations,
        |warning| warnings.push(warning),
    );
    monitor.settings(harness.operator_config().v1_runners(), |warning| {
        warnings.push(warning)
    });
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("all configured routes are excluded; kept"));
    assert!(placements::usable(
        &harness.operator_config().v1_runners()[0],
        harness
            .credentials()
            .lock()
            .unwrap()
            .runner_declarations
            .get("thalamus-harness")
    ));
    refused(
        caller
            .call(
                "route.select",
                &json!({"consumer":"mason","requirements":{"iq":0}}),
            )
            .await,
        "harness_unavailable",
    );
    let pin = caller
        .call("route.select", &request("anthropic/model-a"))
        .await;
    assert_eq!(pin.0, FrameType::Error, "{pin:?}");
    assert_eq!(pin.1["code"], "pin_unavailable", "{pin:?}");
    let reopened = declaration(
        json!(["claude-code"]),
        json!({"claude-code":{"exclude":["anthropic","-anthropic/model-a"]}}),
    );
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .insert("thalamus-harness".into(), reopened);
    let mut monitor = placements::Monitor::default();
    let mut warnings = Vec::new();
    monitor.declarations(
        &harness.credentials().lock().unwrap().runner_declarations,
        |warning| warnings.push(warning),
    );
    monitor.settings(harness.operator_config().v1_runners(), |warning| {
        warnings.push(warning)
    });
    assert!(
        warnings.is_empty(),
        "an admitted descendant keeps the placement open"
    );
    let answer = selected(
        caller
            .call("route.select", &request("anthropic/model-a"))
            .await,
    );
    assert_eq!(answer["runner"]["harness"], "claude-code");
    harness.credentials().lock().unwrap().runner_declarations.insert("thalamus-harness".into(), declaration(json!(["claude-code"]), json!({"claude-code":{"pairs":[{"wire_api":"anthropic.messages","auth_method":"oauth"}]}})));
    refused(
        caller
            .call(
                "route.select",
                &json!({"consumer":"mason","requirements":{"iq":0}}),
            )
            .await,
        "harness_unavailable",
    );
}

#[tokio::test]
async fn settings_placement_describe_uses_first_entry_and_pickers_ignore_harness() {
    let harness = Harness::new("entry-describe");
    install(&harness);
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .clear();
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({
            "catalog_version":41, "resolved_at_ms":123, "models":{
                "anthropic/model-a":{"existence":"present","capability.tool_call":true},
                "anthropic/model-b":{"existence":"present","capability.tool_call":true}
            }
        }))
        .unwrap(),
    )
    .unwrap();
    harness.credentials().lock().unwrap().catalog_snapshot = Some(snapshot);
    entries(
        &harness,
        json!([{ "module":"thalamus-harness","routes":["anthropic/model-a","anthropic/model-b"],"tool_capabilities":["first/v1"] }]),
    );
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    let baseline = caller.call("role.describe", &json!({})).await;
    let bytes = serde_json::to_string(&baseline.1["result"]["runners"]).unwrap();
    assert_eq!(
        bytes,
        r#"[{"follows_account_choice":false,"module":"thalamus-harness","routes":["anthropic/model-a","anthropic/model-b"],"tool_capabilities":["first/v1"],"via_vault":false}]"#
    );
    let head_params = json!({"runner_module":"thalamus-harness"});
    let before = caller.call("route.head_choices", &head_params).await;
    assert_eq!(before.0, FrameType::Response, "{before:?}");
    entries(
        &harness,
        json!([
            { "module":"thalamus-harness","harness":"claude-code","routes":["anthropic/model-a","anthropic/model-b"],"tool_capabilities":["first/v1"] },
            { "module":"thalamus-harness","harness":"opencode2","routes":["openai"],"via_vault":true,"tool_capabilities":["second/v1"] }
        ]),
    );
    let describe = caller.call("role.describe", &json!({})).await;
    assert_eq!(
        serde_json::to_string(&describe.1["result"]["runners"]).unwrap(),
        bytes
    );
    assert_eq!(
        caller.call("route.head_choices", &head_params).await,
        before
    );
}

#[tokio::test]
async fn settings_placement_selects_never_reparse_loaded_rules() {
    let harness = Harness::new("entry-parses");
    install(&harness);
    entries(
        &harness,
        json!([
            {"module":"thalamus-harness","harness":"claude-code","routes":["anthropic/model-a"]},
            {"module":"thalamus-harness","harness":"opencode2","routes":["anthropic/model-b"]}
        ]),
    );
    let config = harness.operator_config();
    let before = placements::PARSES.load(std::sync::atomic::Ordering::Relaxed);
    assert!(before > 0);
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    for _ in 0..3 {
        for (model, expected) in [("model-a", "claude-code"), ("model-b", "opencode2")] {
            let answer = selected(
                caller
                    .call("route.select", &request(&format!("anthropic/{model}")))
                    .await,
            );
            assert_eq!(answer["runner"]["harness"], expected);
        }
    }
    assert!(std::sync::Arc::ptr_eq(&config, &harness.operator_config()));
    assert_eq!(
        placements::PARSES.load(std::sync::atomic::Ordering::Relaxed),
        before
    );
}

#[tokio::test]
async fn settings_placement_later_entry_keeps_its_nonstandard_tier_reach() {
    let _clock = crate::InjectedClock::set(1_800_000_000_000);
    let (harness, mut credentials) =
        crate::routing_v1::service_tier_tests::setup(&["pro_500"], true, true);
    credentials.runner_declarations.insert(
        "tier-runner".into(),
        super::parse_declaration(&json!({"runner":{
            "pairs":[{"wire_api":"openai.responses","auth_method":"oauth"}],
            "harnesses":["early","later"], "follows_account_choice":true
        }}))
        .unwrap()
        .unwrap(),
    );
    harness.set_credentials(credentials);
    entries(
        &harness,
        json!([
            {"module":"tier-runner","harness":"early","routes":["openai/not-sol"],"via_vault":true},
            {"module":"tier-runner","harness":"later","routes":["openai/gpt-6.1-sol"],"via_vault":true}
        ]),
    );
    let caller = harness.bind(reserved("tier-owner"), None).await;
    let answer = selected(caller.call("route.select", &json!({
        "consumer":"mason", "requirements":{"iq":30}, "service_tier":"ultrafast", "harness":"later"
    })).await);
    assert_eq!(answer["runner"]["harness"], "later");
    assert_eq!(answer["service_tier_chosen"], "ultrafast");
}

#[test]
fn consumer_placements_reject_both_spellings_with_consumer_name() {
    for (runner, runners) in [
        (json!("r"), json!([])),
        (Value::Null, json!([])),
        (json!("r"), Value::Null),
    ] {
        let error = crate::config_overrides::validate_document(
            &json!({"consumers":{"named.consumer":{"runner":runner,"runners":runners}}})
                .to_string(),
        )
        .unwrap_err();
        assert!(error.contains("named.consumer"), "{error}");
        assert!(error.contains("mutually exclusive"), "{error}");
    }
}

#[test]
fn consumer_placements_validate_pairs_and_rules_at_load() {
    for entries in [
        json!([{"module":"r","harness":"a"},{"module":"r","harness":"b"}]),
        json!([{"module":"r","harness":"a"},{"module":"r"}]),
        json!([]),
    ] {
        crate::config_overrides::validate_document(
            &json!({"consumers":{"mason":{"runners":entries}}}).to_string(),
        )
        .unwrap();
    }
    for entries in [
        json!([{"module":"duplicate-module"},{"module":"duplicate-module"}]),
        json!([{"module":"duplicate-module","harness":"a"},{"module":"duplicate-module","harness":"a"}]),
    ] {
        let error = crate::config_overrides::validate_document(
            &json!({"consumers":{"mason":{"runners":entries}}}).to_string(),
        )
        .unwrap_err();
        assert!(
            error.contains("duplicate-module") && error.contains("listed once"),
            "{error}"
        );
    }
    for entry in [
        json!({"module":"bad-module","routes":["-anthropic"]}),
        json!({"module":"bad-module","harness":"two words"}),
        json!({"module":"bad-module","tool_capabilities":["two words"]}),
    ] {
        let error = crate::config_overrides::validate_document(
            &json!({"consumers":{"mason":{"runners":[entry]}}}).to_string(),
        )
        .unwrap_err();
        assert!(
            error.contains("consumers.mason.runners") && error.contains("bad-module"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn consumer_placements_inherit_and_replace_both_spellings_before_overlay() {
    let harness = Harness::new("consumer-inheritance");
    install(&harness);
    entries(
        &harness,
        json!([
            {"module":"thalamus-harness","routes":["anthropic/model-b"]},
            {"module":"broca"}
        ]),
    );
    harness.edit_config(|config| {
        config["consumers"]["mason"]["runners"] = json!([
            {"module":"thalamus-harness","harness":"claude-code","routes":["anthropic/model-a"]}
        ]);
        config["consumers"]["mason.child"] = json!({"runner":"broca"});
        config["consumers"]["mason.child.grand"] = json!({"runners":[
            {"module":"thalamus-harness","harness":"opencode2","routes":["anthropic/model-a"]}
        ]});
        config["consumers"]["mason.other"] = json!({"runners":[{"module":"broca"}]});
    });
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    for (consumer, module, harness_name) in [
        ("mason", "thalamus-harness", Some("claude-code")),
        (
            "mason.unlisted.child",
            "thalamus-harness",
            Some("claude-code"),
        ),
        ("mason.child", "broca", None),
        ("mason.child.inherited", "broca", None),
        ("mason.child.grand", "thalamus-harness", Some("opencode2")),
        ("mason.other", "broca", None),
    ] {
        let mut params = request("anthropic/model-a");
        params["consumer"] = json!(consumer);
        let answer = selected(caller.call("route.select", &params).await);
        assert_eq!(answer["runner"]["module"], module, "{consumer}");
        if let Some(name) = harness_name {
            assert_eq!(answer["runner"]["harness"], name, "{consumer}");
        }
    }
}

#[tokio::test]
async fn consumer_placements_order_routes_and_caller_filters_never_widen() {
    let harness = Harness::new("consumer-caller");
    install(&harness);
    entries(
        &harness,
        json!([
            {"module":"broca"},
            {"module":"thalamus-harness","harness":"claude-code","routes":["openai"]}
        ]),
    );
    harness.edit_config(|config| {
        config["consumers"]["mason"]["runners"] = json!([
            {"module":"thalamus-harness","harness":"claude-code","routes":["anthropic/model-b"]},
            {"module":"thalamus-harness","harness":"opencode2","routes":["anthropic/model-a"]}
        ]);
        config["consumers"]["mason.sibling"] = json!({"runners":[
            {"module":"thalamus-harness","harness":"claude-code","routes":["anthropic/model-a"]}
        ]});
    });
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    for (consumer, model, expected) in [
        ("mason", "model-a", "opencode2"),
        ("mason", "model-b", "claude-code"),
        ("mason.sibling", "model-a", "claude-code"),
    ] {
        let mut params = request(&format!("anthropic/{model}"));
        params["consumer"] = json!(consumer);
        let answer = selected(caller.call("route.select", &params).await);
        assert_eq!(answer["runner"]["harness"], expected);
    }
    let mut params = request("anthropic/model-a");
    params["harness"] = json!("opencode2");
    assert_eq!(
        selected(caller.call("route.select", &params).await)["runner"]["harness"],
        "opencode2"
    );
    params["runners"] = json!(["broca"]);
    refused(
        caller.call("route.select", &params).await,
        "harness_unavailable",
    );
    params.as_object_mut().unwrap().remove("harness");
    refused(
        caller.call("route.select", &params).await,
        "harness_unavailable",
    );
    params.as_object_mut().unwrap().remove("runners");
    params["harness"] = json!("missing");
    refused(
        caller.call("route.select", &params).await,
        "harness_unavailable",
    );
    let pin = caller
        .call("route.select", &request("openai/model-c"))
        .await;
    assert_eq!(pin.0, FrameType::Error, "{pin:?}");
    assert_eq!(pin.1["code"], "pin_unavailable", "{pin:?}");
}

#[tokio::test]
async fn consumer_placements_empty_refused_and_pending_lists_never_fall_back() {
    let harness = Harness::new("consumer-refusals");
    install(&harness);
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    for entries in [
        json!([]),
        json!([{"module":"not-global"}]),
        json!([{"module":"thalamus-harness","harness":"unhosted"}]),
    ] {
        harness.edit_config(|config| config["consumers"]["mason"]["runners"] = entries);
        for runners in [Value::Null, json!(["broca"])] {
            let mut params = request("anthropic/model-a");
            if !runners.is_null() {
                params["runners"] = runners;
            }
            refused(
                caller.call("route.select", &params).await,
                "harness_unavailable",
            );
        }
    }
    harness.edit_config(|config| {
        config["consumers"]["mason"]["runners"] = json!([
            {"module":"not-global"},
            {"module":"thalamus-harness","harness":"claude-code"},
            {"module":"broca"}
        ])
    });
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .remove("thalamus-harness");
    let answer = selected(
        caller
            .call("route.select", &request("anthropic/model-a"))
            .await,
    );
    assert_eq!(answer["runner"]["module"], "broca");
    let policies = serde_json::from_value(json!([
        {"module":"thalamus-harness","harness":"claude-code"}
    ]))
    .unwrap();
    let mut monitor = placements::Monitor::default();
    let mut warnings = Vec::new();
    monitor.settings(policies, |warning| warnings.push(warning));
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("thalamus-harness") && warnings[0].contains("kept but skipped"));
    for (limits, module) in [
        (json!({}), "thalamus-harness"),
        (json!({"claude-code":{"starts_sessions":false}}), "broca"),
    ] {
        harness
            .credentials()
            .lock()
            .unwrap()
            .runner_declarations
            .insert(
                "thalamus-harness".into(),
                declaration(json!(["claude-code"]), limits),
            );
        let answer = selected(
            caller
                .call("route.select", &request("anthropic/model-a"))
                .await,
        );
        assert_eq!(answer["runner"]["module"], module);
    }
    // Adding a global runner authorizes the already retained consumer entry.
    harness.edit_config(|config| {
        config["runners"]
            .as_array_mut()
            .unwrap()
            .push(json!({"module":"not-global"}))
    });
    let answer = selected(
        caller
            .call("route.select", &request("anthropic/model-a"))
            .await,
    );
    assert_eq!(answer["runner"]["module"], "not-global");
}

#[tokio::test]
async fn consumer_placements_legacy_expansion_keeps_routes_vault_and_exclusivity() {
    let harness = Harness::new("consumer-legacy");
    install(&harness);
    entries(
        &harness,
        json!([
            {"module":"thalamus-harness","routes":["anthropic/model-a"],"via_vault":true},
            {"module":"broca"}
        ]),
    );
    harness.edit_config(|config| {
        config["consumers"]["mason"]["runner"] = json!("thalamus-harness");
        config["consumers"]["mason"]["tool_capabilities"] = json!([]);
    });
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    let legacy = selected(
        caller
            .call("route.select", &request("anthropic/model-a"))
            .await,
    )["runner"]
        .clone();
    assert_eq!(legacy["module"], "thalamus-harness");
    let outside = caller
        .call("route.select", &request("anthropic/model-b"))
        .await;
    assert_eq!(outside.0, FrameType::Error, "{outside:?}");
    assert_eq!(outside.1["code"], "pin_unavailable");
    harness.edit_config(|config| {
        config["consumers"]["mason"]
            .as_object_mut()
            .unwrap()
            .remove("runner");
        config["consumers"]["mason"]["runners"] = json!([
            {"module":"thalamus-harness","routes":["anthropic/model-a"],"via_vault":true}
        ]);
    });
    assert_eq!(
        selected(
            caller
                .call("route.select", &request("anthropic/model-a"))
                .await
        )["runner"],
        legacy
    );
    // Without a declaration, via_vault still requires a serving credential.
    harness
        .credentials()
        .lock()
        .unwrap()
        .runner_declarations
        .clear();
    harness.credentials().lock().unwrap().apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing::default(),
        crate::now_ms(),
    );
    for legacy_spelling in [false, true] {
        if legacy_spelling {
            harness.edit_config(|config| {
                config["consumers"]["mason"]
                    .as_object_mut()
                    .unwrap()
                    .remove("runners");
                config["consumers"]["mason"]["runner"] = json!("thalamus-harness");
            });
        }
        let policy = harness.operator_config().consumer_policy(
            "mason",
            &Default::default(),
            &Default::default(),
        );
        assert_eq!(
            serde_json::to_value(policy).unwrap()["runners"][0]["via_vault"],
            true
        );
        let answer = caller
            .call("route.select", &request("anthropic/model-a"))
            .await;
        assert_eq!(answer.0, FrameType::Error, "{answer:?}");
        assert_eq!(answer.1["code"], "no_route", "{answer:?}");
        assert_eq!(
            answer.1["detail"]["reason"], "missing_capability",
            "{answer:?}"
        );
    }
    harness.edit_config(|config| config["consumers"]["mason"]["runner"] = json!("absent"));
    for runners in [Value::Null, json!(["broca"])] {
        let mut params = request("anthropic/model-a");
        if !runners.is_null() {
            params["runners"] = runners;
        }
        refused(
            caller.call("route.select", &params).await,
            "no_available_model",
        );
    }
}

#[tokio::test]
async fn consumer_placements_repeated_selects_never_parse_loaded_lists() {
    let harness = Harness::new("consumer-parses");
    install(&harness);
    harness.edit_config(|config| {
        config["consumers"]["mason"]["runners"] = json!([
            {"module":"thalamus-harness","harness":"claude-code","routes":["anthropic/model-a"]}
        ]);
        config["consumers"]["mason.child"] = json!({"runners":[
            {"module":"thalamus-harness","harness":"claude-code","routes":["anthropic/model-b"]}
        ]});
    });
    let config = harness.operator_config();
    let caller = harness.bind(reserved(fixture::CONSUMER), None).await;
    let before = placements::PARSES.load(std::sync::atomic::Ordering::Relaxed);
    assert!(before > 0);
    for _ in 0..3 {
        for (consumer, model) in [("mason", "model-a"), ("mason.child", "model-b")] {
            let mut params = request(&format!("anthropic/{model}"));
            params["consumer"] = json!(consumer);
            assert_eq!(
                selected(caller.call("route.select", &params).await)["runner"]["harness"],
                "claude-code"
            );
        }
    }
    assert!(std::sync::Arc::ptr_eq(&config, &harness.operator_config()));
    assert_eq!(
        placements::PARSES.load(std::sync::atomic::Ordering::Relaxed),
        before
    );
}

fn classifier_harness() -> std::sync::Arc<Harness> {
    let harness = Harness::new("classifier-placements");
    let description: Value = serde_json::from_slice(include_bytes!(
        "../../../tests/fixtures/classifier-describe.reply.json"
    ))
    .unwrap();
    let model = &description["result"]["models"][0];
    let key = model["model"].as_str().unwrap();
    let (provider, name) = key.split_once('/').unwrap();
    let rate = model["price"]["input_per_mtok_usd"].as_f64().unwrap();
    let snapshot = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&json!({
        "catalog_version":1, "resolved_at_ms":1, "models":{key:{
            "existence":"present", "model.kind":"decision",
            "capability.input_modalities":["text"],
            "rate.input":{"state":"priced","currency":"USD","units":(rate * 1e9).round() as i64,"exponent":9},
            "rate.output":{"state":"stated_zero"}
        }}
    })).unwrap()).unwrap();
    let mut fact = crate::routing_v1::conformance_tests::catalog_fact(
        &cortexkit_role_routing_conformance::CatalogModel {
            provider: provider.into(),
            model: name.into(),
            family: None,
            iq: 80,
            context_window: 200_000,
            max_output_tokens: 32_000,
            tool_calls: false,
            modalities: Vec::new(),
            structured_output: false,
            priced_per_token: true,
        },
        0,
    );
    fact.input_rate = snapshot.parts().0[0].input_rate.clone();
    fact.cost_per_m_tok_out = snapshot.parts().0[0].cost_per_mtok_out;
    let facts = vec![fact];
    harness.install_facts(&facts);
    harness
        .store()
        .apply_fusiform_catalog(snapshot.parts().0, snapshot.parts().1)
        .unwrap();
    let mut credentials = crate::broca_credentials::BrocaCredentials::api_test_credentials(&facts);
    credentials.publish_plan_prices(&json!({"prices":[]}));
    credentials.catalog_snapshot = Some(snapshot);
    let mut cache = crate::routing_v1::classifier::DescribeCache::default();
    cache.catalog(&json!([{"module_id":"broca", "nonce_verified":true,"capabilities":{"provides":["classifier/v1"]}}]), &mut credentials, crate::now_ms() as i64);
    cache.reply(
        "broca",
        Some(&description),
        &mut credentials,
        crate::now_ms() as i64,
    );
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["consumers"]["classifier"] = json!({"owner":"broca"});
        config["runners"] = json!([{"module":"chat-runner"}]);
    });
    harness
}

#[tokio::test]
async fn consumer_placements_classifier_excluding_caller_never_substitutes_another_runner() {
    let harness = classifier_harness();
    let caller = harness.bind(reserved("broca"), None).await;
    let params = json!({"consumer":"classifier","requirements":{"iq":0}});
    let baseline = selected(caller.call("route.select", &params).await);
    assert_eq!(baseline["runner"]["module"], "broca");
    harness.edit_config(|config| {
        config["runners"]
            .as_array_mut()
            .unwrap()
            .push(json!({"module":"broca"}));
        config["consumers"]["classifier"]["runners"] = json!([{"module":"broca"}]);
    });
    assert_eq!(
        selected(caller.call("route.select", &params).await)["runner"],
        baseline["runner"]
    );
    harness.edit_config(|config| {
        config["consumers"]["classifier"]["runners"] = json!([{"module":"chat-runner"}]);
    });
    refused(
        caller.call("route.select", &params).await,
        "harness_unavailable",
    );
}

#[tokio::test]
async fn consumer_placements_classifier_empty_or_unauthorized_list_never_falls_back() {
    let harness = classifier_harness();
    let caller = harness.bind(reserved("broca"), None).await;
    let params = json!({"consumer":"classifier","requirements":{"iq":0}});
    assert_eq!(
        selected(caller.call("route.select", &params).await)["runner"]["module"],
        "broca"
    );
    for entries in [json!([]), json!([{"module":"broca"}])] {
        harness.edit_config(|config| config["consumers"]["classifier"]["runners"] = entries);
        refused(
            caller.call("route.select", &params).await,
            "harness_unavailable",
        );
    }
}

#[tokio::test]
async fn consumer_placements_classifier_entry_routes_and_harness_only_narrow() {
    let harness = classifier_harness();
    let caller = harness.bind(reserved("broca"), None).await;
    let params = json!({"consumer":"classifier","requirements":{"iq":0}});
    let baseline = selected(caller.call("route.select", &params).await);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"broca"}]);
        config["consumers"]["classifier"]["runners"] = json!([
            {"module":"broca","routes":[baseline["route"]["provider"].clone()]}
        ]);
    });
    assert_eq!(
        selected(caller.call("route.select", &params).await)["runner"],
        baseline["runner"]
    );
    harness.edit_config(|config| {
        config["consumers"]["classifier"]["runners"][0]["routes"] = json!(["absent-provider"])
    });
    let answer = caller.call("route.select", &params).await;
    assert_eq!(answer.0, FrameType::Error, "{answer:?}");
    assert_eq!(answer.1["code"], "no_route", "{answer:?}");
    harness.edit_config(|config| {
        config["consumers"]["classifier"]["runners"] =
            json!([{"module":"broca","harness":"unhosted"}])
    });
    refused(
        caller.call("route.select", &params).await,
        "harness_unavailable",
    );
}
