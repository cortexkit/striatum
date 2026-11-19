use super::validate_document;

macro_rules! refuses {
    ($test:ident, $input:literal, $path:literal, $hint:literal) => {
        #[test]
        fn $test() {
            let error = validate_document($input).expect_err("old settings spelling must fail");
            assert!(error.contains($path) && error.contains($hint), "{error}");
        }
    };
}

refuses!(
    refuses_explicit_null,
    r#"{"providers":{"p":{"pin_only":null}}}"#,
    "/providers/p/pin_only",
    "omit"
);
refuses!(
    refuses_null_before_runner_conflict,
    r#"{"consumers":{"x":{"runner":"broca","runners":null}}}"#,
    "/consumers/x/runners",
    "omit"
);
refuses!(
    refuses_retention_null,
    r#"{"retention":{"account_selection_days":null}}"#,
    "/retention/account_selection_days",
    "omit"
);
refuses!(
    refuses_old_top_k,
    r#"{"panels":{"audit":{"topK":1,"by":"iq"}}}"#,
    "/panels/audit/topK",
    "top_k"
);
refuses!(
    refuses_old_extra_by,
    r#"{"panels":{"audit":{"topK":1,"by":"iq","extra":1,"extraBy":"iq"}}}"#,
    "/panels/audit/extraBy",
    "extra_by"
);
refuses!(
    refuses_old_context_window,
    r#"{"models":{"p/m":{"context_window":1024}}}"#,
    "/models/p~1m/context_window",
    "context_window_tokens"
);
refuses!(
    refuses_old_speed,
    r#"{"models":{"p/m":{"speed":50}}}"#,
    "/models/p~1m/speed",
    "output_tokens_per_second"
);
refuses!(
    refuses_old_spend_budget,
    r#"{"providers":{"p":{"accounts":{"spend_budget":10}}}}"#,
    "/providers/p/accounts/spend_budget",
    "spend_budget_usd"
);
refuses!(
    refuses_bare_max_hold,
    r#"{"providers":{"p":{"accounts":{"max_hold":90}}}}"#,
    "/providers/p/accounts/max_hold",
    "\"90s\""
);
refuses!(
    refuses_bare_hold_threshold,
    r#"{"accounts":{"hold_when_reset_within":90}}"#,
    "/accounts/hold_when_reset_within",
    "\"90s\""
);
refuses!(
    refuses_bare_unknown_reset_retry,
    r#"{"accounts":{"unknown_reset_retry":90}}"#,
    "/accounts/unknown_reset_retry",
    "\"90s\""
);

#[test]
fn converter_translates_every_old_form_without_touching_ids() {
    use super::settings_shape::{convert, Edit};
    use serde_json::json;

    let before = json!({
        "panels": {"audit":{"topK":7,"by":"iq","extra":1,"extraBy":"eq","floor":null}},
        "models": {"p/m~1":{"speed":50,"context_window":32768}},
        "providers": {"speed":{"accounts": {"spend_budget":12.5,"max_hold":90}}},
        "accounts": {"spend_budget":20,"hold_when_reset_within":900,"unknown_reset_retry":21600},
        "consumers": {"x":{"runner":"broca","runners":null,"pin":null}},
        "retention": {"account_selection_days":null},
        "head_agents": ["First",null,"Last",null],
    });
    let (after, edits) = convert(before).unwrap();
    assert_eq!(
        after,
        json!({
            "panels": {"audit":{"top_k":7,"by":"iq","extra":1,"extra_by":"eq"}},
            "models": {"p/m~1":{"output_tokens_per_second":50,"context_window_tokens":32768}},
            "providers": {"speed":{"accounts": {"spend_budget_usd":12.5,"max_hold":"90s"}}},
            "accounts": {"spend_budget_usd":20,"hold_when_reset_within":"900s","unknown_reset_retry":"21600s"},
            "consumers": {"x":{"runner":"broca"}},
            "retention": {},
            "head_agents": ["First","Last"],
        })
    );
    validate_document(&after.to_string()).unwrap();
    assert_eq!(convert(after.clone()).unwrap(), (after, vec![]));
    let rename = |from: &str, to: &str| Edit::Rename {
        from: from.into(),
        to: to.into(),
    };
    assert_eq!(
        edits,
        [
            rename("/accounts/spend_budget", "/accounts/spend_budget_usd"),
            Edit::Duration("/accounts/hold_when_reset_within".into()),
            Edit::Duration("/accounts/unknown_reset_retry".into()),
            Edit::Remove("/consumers/x/pin".into()),
            Edit::Remove("/consumers/x/runners".into()),
            Edit::Remove("/head_agents/3".into()),
            Edit::Remove("/head_agents/1".into()),
            rename(
                "/models/p~1m~01/context_window",
                "/models/p~1m~01/context_window_tokens"
            ),
            rename(
                "/models/p~1m~01/speed",
                "/models/p~1m~01/output_tokens_per_second"
            ),
            rename("/panels/audit/extraBy", "/panels/audit/extra_by"),
            rename("/panels/audit/topK", "/panels/audit/top_k"),
            Edit::Remove("/panels/audit/floor".into()),
            rename(
                "/providers/speed/accounts/spend_budget",
                "/providers/speed/accounts/spend_budget_usd"
            ),
            Edit::Duration("/providers/speed/accounts/max_hold".into()),
            Edit::Remove("/retention/account_selection_days".into()),
        ]
    );
}

#[test]
fn converter_refuses_rename_collisions_including_null_values() {
    use serde_json::json;
    let error = super::settings_shape::convert(json!({"models":{"p/m":{
        "speed":null,"output_tokens_per_second":50
    }}}))
    .unwrap_err();
    assert!(error.contains("/models/p~1m/speed"), "{error}");
    assert!(
        error.contains("/models/p~1m/output_tokens_per_second"),
        "{error}"
    );
}

#[test]
fn panel_cross_field_rules_are_checked_in_settings_validation() {
    let missing =
        validate_document(r#"{"panels":{"audit":{"top_k":7,"extra":1,"by":"iq"}}}"#).unwrap_err();
    assert!(
        missing.contains("panels.audit: extra_by is required"),
        "{missing}"
    );
    let overflow = validate_document(
        r#"{"panels":{"audit":{"top_k":7,"extra":2,"by":"iq","extra_by":"eq"}}}"#,
    )
    .unwrap_err();
    assert!(
        overflow.contains("panels.audit:") && overflow.contains("top_k + extra must be <= 8"),
        "{overflow}"
    );
    validate_document(r#"{"panels":{"audit":{"top_k":7,"extra":1,"by":"iq","extra_by":"eq"},"lab":{"top_k":8,"by":"iq"}}}"#).unwrap();
}

#[test]
fn new_settings_load_as_written() {
    use serde_json::json;
    let scratch = cortexkit_test_support::scratch_root()
        .join(format!("managed-settings-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).unwrap();
    let path = scratch.join("striatum.jsonc");
    let settings = json!({
        "concurrency":{"providers":{"p":{"limit":"none"}},"models":{"p/m":{"limit":1}}},
        "models":{"p/m":{"context_window_tokens":4096,"output_tokens_per_second":24}},
        "providers":{"p":{"accounts":{"spend_budget_usd":25,"max_hold":"90s","hold_when_reset_within":"15m","unknown_reset_retry":"6h"}}},
        "panels":{"audit":{"top_k":2,"extra":1,"by":"iq","extra_by":"eq"}}
    });
    std::fs::write(&path, settings.to_string()).unwrap();
    let result = super::check_config_offline(&path, None);
    super::live_configs().lock().unwrap().remove(&path);
    std::fs::remove_dir_all(&scratch).unwrap();
    assert!(result.unwrap().contains("runner_route_rules="));
}

fn walk_schema(
    node: &serde_json::Value,
    path: &str,
    visit: &mut impl FnMut(&serde_json::Value, &str),
) {
    visit(node, path);
    for (key, child) in node["properties"].as_object().into_iter().flatten() {
        walk_schema(child, &format!("{path}/{key}"), visit);
    }
    if let Some(children) = node["patternProperties"].as_object() {
        assert_eq!(children.len(), 1, "{path}");
        walk_schema(
            children.values().next().unwrap(),
            &format!("{path}/*"),
            visit,
        );
    }
    if let Some(child) = node.get("items") {
        walk_schema(child, &format!("{path}/*"), visit);
    }
}

#[test]
fn schema_pins_field_metadata_and_omission() {
    use serde_json::{json, Value};
    let schema: Value = serde_json::from_str(&super::routing_config_schema()).unwrap();
    let mut actual = Vec::new();
    for (owner, node) in std::iter::once(("Root", &schema)).chain(
        schema["$defs"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(name, node)| (name.as_str(), node)),
    ) {
        walk_schema(node, "", &mut |node, path| {
            for keyword in ["allOf", "anyOf", "if", "then", "else"] {
                assert!(node.get(keyword).is_none(), "{owner}{path}: {keyword}");
            }
            assert_ne!(node["type"], "null", "{owner}{path}");
            assert!(
                !node["type"]
                    .as_array()
                    .is_some_and(|types| types.contains(&json!("null"))),
                "{owner}{path}"
            );
            if node["type"] == "object" {
                assert_eq!(node["unevaluatedProperties"], false, "{owner}{path}");
            }
            assert!(!node["additionalProperties"].is_object(), "{owner}{path}");
            if path.is_empty() {
                return;
            }
            assert!(
                node["description"].as_str().is_some_and(|s| !s.is_empty()),
                "{owner}{path}"
            );
            assert_eq!(node["x-ck-apply"], "live", "{owner}{path}");
            assert_eq!(node["x-ck-layers"], json!(["user"]), "{owner}{path}");
            match node.get("default") {
                Some(default) => {
                    fn contains_null(v: &Value) -> bool {
                        match v {
                            Value::Object(v) => v.values().any(contains_null),
                            Value::Array(v) => v.iter().any(contains_null),
                            _ => v.is_null(),
                        }
                    }
                    assert!(!contains_null(default), "{owner}{path}");
                    assert!(node.get("x-ck-required").is_none(), "{owner}{path}");
                }
                None => match node["x-ck-required"].as_bool() {
                    Some(false) => actual.push(format!("{owner}{path}")),
                    Some(true) => {}
                    None => panic!(
                        "{owner}{path} declares neither a default nor whether it is required"
                    ),
                },
            }
        });
    }
    let groups = [
        ("Root", "/consumer_priorities/* /consumers/* /head_agents/* /models/* /pin_origin_callers/* /providers/* /quota_model_pools/* /runners/* /subscription_multipliers/*"),
        ("AccountPoolFloor", "/model /pool /window_minutes"),
        ("CapOverride", "/scope"),
        ("CapSettings", "/accounts/* /models/* /plans/* /providers/*"),
        ("ConsumerPolicy", "/below_bar /context_tokens /eq /iq /ladder /ladder/* /max_output_tokens /mode /owner /pay_per_token /pin /projects/* /runner /runners /runners/* /service_tier /service_tiers /service_tiers/* /tool_capabilities /tool_capabilities/* /workload"),
        ("DefaultAccountsPolicy", "/floors/* /hold_when_reset_within /order/* /soft_quota_remaining_percent /spend_budget_reset_at_ms /spend_budget_usd"),
        ("ModelFactOverride", "/context_window_tokens /cost_per_mtok_out /elo /eq /not_for /not_for/* /only_for /only_for/* /output_tokens_per_second /pin_only /pin_only_for /pin_only_for/* /service_tier /speed_multiplier /subscription /subscription_multiplier /vision"),
        ("OverqualificationSection", "/base /doubling /threshold"),
        ("PanelPolicies", "/audit /design_review /diagnose /evaluate /explain /lab /spec"),
        ("PanelPolicy", "/extra_by /floor"),
        ("ProviderAccountsPolicy", "/floors /floors/* /hold_when_reset_within /max_hold /order /order/* /ordered /paid_credits /soft_quota_remaining_percent /spend_budget_reset_at_ms /spend_budget_usd /strategy /unknown_reset_retry"),
        ("ProviderPolicy", "/models /models/* /not_for /not_for/* /only_for /only_for/* /pin_only /pin_only_for /pin_only_for/* /service_tier /subscription /subscription_multiplier"),
        ("RunnerPolicy", "/harness /routes /routes/* /tool_capabilities/*"),
        ("SynthesizerPin", "/variant"),
    ];
    let mut expected: Vec<_> = groups
        .into_iter()
        .flat_map(|(name, paths)| {
            paths
                .split_whitespace()
                .map(move |path| format!("{name}{path}"))
        })
        .collect();
    expected.sort();
    actual.sort();
    assert_eq!(actual, expected);
    for table in ["DefaultAccountsPolicy", "ProviderAccountsPolicy"] {
        for key in ["max_hold", "hold_when_reset_within", "unknown_reset_retry"] {
            let field = &schema["$defs"][table]["properties"][key];
            assert_eq!(field["type"], "string");
            assert_eq!(field["x-ck-kind"], "duration");
        }
    }
    for table in ["CapOverride", "ModelCapOverride"] {
        let limit = &schema["$defs"][table]["properties"]["limit"];
        assert_eq!(limit["type"], json!(["integer", "string"]));
        assert_eq!(limit["pattern"], "^none$");
        assert_eq!(limit["minimum"], 1);
    }
    assert!(
        schema["properties"]["min_best_effort_adequacy"]["description"]
            .as_str()
            .unwrap()
            .contains("0 to 1")
    );
}

#[test]
fn pinned_daemon_sdk_refuses_only_optional_leaves_and_cap_type_lists() {
    use serde_json::{json, Value};
    use subc_client_rs::config::Schema;

    // The locked subc-config SDK returns only the first refusal. Test each version difference
    // separately, then pass both projections together to uncover any others.
    fn project_for_older_sdk(node: &mut Value, optional: bool, caps: bool) {
        if optional
            && node["x-ck-required"] == false
            && node["type"].as_str().is_some_and(|t| t != "object")
        {
            // A sample is only for this compatibility test. Putting these
            // values in the real defaults would override inherited settings.
            let sample = node["enum"]
                .as_array()
                .and_then(|choices| choices.first())
                .cloned()
                .unwrap_or_else(|| match node["type"].as_str().unwrap() {
                    "array" => json!([]),
                    "boolean" => json!(false),
                    "integer" | "number" => json!(1),
                    "string" if node["x-ck-kind"] == "duration" => json!("1s"),
                    "string" => json!("example"),
                    kind => panic!("unexpected optional type {kind}"),
                });
            node.as_object_mut().unwrap().remove("x-ck-required");
            node["default"] = sample;
        }
        if caps && node["type"] == json!(["integer", "string"]) {
            node["type"] = json!("integer");
        }
        for keyword in ["$defs", "properties", "patternProperties"] {
            if let Some(children) = node.get_mut(keyword).and_then(Value::as_object_mut) {
                for child in children.values_mut() {
                    project_for_older_sdk(child, optional, caps);
                }
            }
        }
        if let Some(items) = node.get_mut("items") {
            project_for_older_sdk(items, optional, caps);
        }
    }
    let real: Value = serde_json::from_str(&super::routing_config_schema()).unwrap();
    let check_refusal = |schema, path| {
        let actual = Schema::new(schema)
            .err()
            .expect("the locked SDK rejects this declaration");
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            json!({"code":"schema_invalid", "path":path, "project_id":null})
        );
    };
    check_refusal(real.clone(), "/accounts/floors/*/model");
    let mut no_caps = real.clone();
    project_for_older_sdk(&mut no_caps, false, true);
    check_refusal(no_caps, "/accounts/floors/*/model");
    let mut no_omission = real;
    project_for_older_sdk(&mut no_omission, true, false);
    check_refusal(no_omission.clone(), "/concurrency/accounts/*/limit");
    project_for_older_sdk(&mut no_omission, false, true);
    if let Err(error) = Schema::new(no_omission) {
        panic!("unexpected SDK refusal after removing the two known version differences: {error}");
    }
}
