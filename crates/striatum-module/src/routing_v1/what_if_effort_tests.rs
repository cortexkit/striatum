use super::*;

const A: &str = "example-provider/model-a";
const B: &str = "example-provider/model-b";
const C: &str = "example-subscription/model-c";
const D: &str = "example-subscription/model-d";

/// Fill the fixture's real catalog rows, leaving its transport and accounts in
/// place. Tests then change only the facts involved in the preview.
fn serve_efforts(harness: &Harness, customize: impl Fn(&str, &mut Value)) {
    prepare_runner_preview(harness);
    let models: serde_json::Map<_, _> = harness.store().get_facts().unwrap().into_iter().map(|fact| {
        let route = format!("{}/{}", fact.provider_id, fact.model_id);
        let mut row = json!({
            "existence":"present", "capability.tool_call":true,
            "limit.context":fact.context_window.unwrap_or(100_000.0) as u64,
            "limit.output":fact.max_output_tokens.unwrap_or(8192),
            "model.lineage":fact.model_id,
            "capability.reasoning_options":[{"type":"effort","values":["low","medium","high","xhigh"]}],
        });
        customize(&route, &mut row);
        (route, row)
    }).collect();
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        serde_json::to_vec(&json!({
            "catalog_version":42,"resolved_at_ms":123,"models":models,
        }))
        .unwrap(),
    )
    .unwrap();
    crate::fusiform_catalog::apply_mapped_catalog(
        harness.store(),
        &snapshot,
        Some(&harness.credentials()),
        None,
    )
    .unwrap();
}

fn profiles() -> Value {
    json!({"effort_source":"catalog", "workloads":{(fixture::SUMMARY):"code"}, "iqs":[0],
        "runner_declarations":{"broca":broad_runner_slot()}})
}

fn only(routes: &[&str]) -> Value {
    let rules: serde_json::Map<_, _> = [A, B, C, D]
        .into_iter()
        .map(|route| {
            (
                route.to_owned(),
                if routes.contains(&route) {
                    json!({"pin_only":false})
                } else {
                    json!({"not_for":[fixture::SUMMARY]})
                },
            )
        })
        .collect();
    json!({"models":rules})
}

#[tokio::test]
async fn catalog_effort_minimum_raises_to_lowest_offered_at_or_above_it() {
    let (harness, _) = configured().await;
    serve_efforts(&harness, |route, row| {
        if route == A {
            row["capability.reasoning_options"][0]["values"] = json!(["xhigh", "low", "high"]);
            row["workload.code.effort_default"] = json!("low");
            row["workload.code.effort_minimum"] = json!("medium");
        }
    });
    let mut params = profiles();
    params["policy_patch"] = only(&[A]);
    let answer = evaluate(&operator(&harness).await, params).await;
    let pick = &answer["results"][0]["pick"];
    assert_eq!(pick["route"], A, "{answer}");
    assert_eq!(pick["variant"], "high", "{answer}");
    assert_eq!(pick["effort_source"], "minimum_raised");
}

#[tokio::test]
async fn catalog_effort_minimum_unreachable_filters_a_published_list() {
    let (harness, _) = configured().await;
    serve_efforts(&harness, |route, row| {
        if route == A {
            row["capability.reasoning_options"][0]["values"] = json!(["low", "medium"]);
            row["workload.code.effort_minimum"] = json!("high");
        }
    });
    let mut params = profiles();
    params["policy_patch"] = only(&[A, B]);
    let answer = evaluate(&operator(&harness).await, params).await;
    let result = &answer["results"][0];
    assert_eq!(result["pick"]["route"], B, "{answer}");
    assert!(result["excluded"].as_array().unwrap().contains(&json!({
        "route":A,"reason":"workload_minimum_unreachable","workload":"code","minimum":"high","offered":["low","medium"]
    })), "{answer}");
    assert_eq!(result["minimum_unverified_count"], 0);
}

#[tokio::test]
async fn catalog_effort_minimum_unknown_is_counted_and_does_not_send_an_effort() {
    let (harness, _) = configured().await;
    let golden: Value = serde_json::from_str(include_str!(
        "../../fixtures/fusiform-workload-no-effort.json"
    ))
    .unwrap();
    let no_effort = golden["synthetic-quality-reseller/synthetic-model-no-effort"]
        .as_object()
        .unwrap();
    assert!(!no_effort.contains_key("capability.reasoning_options"));
    serve_efforts(&harness, |route, row| {
        if route == A {
            // Complete the prefix-filtered reseller row under a route the
            // fixture can actually send through its existing account.
            let row = row.as_object_mut().unwrap();
            row.remove("capability.reasoning_options");
            row.extend(no_effort.clone());
        } else if route == B {
            row["capability.reasoning_options"] = Value::Null;
            row["workload.code.effort_minimum"] = json!("medium");
        }
    });
    let direct = operator(&harness).await;
    for (route, minimum) in [(A, "low"), (B, "medium")] {
        let mut params = profiles();
        params["policy_patch"] = only(&[route]);
        params["iqs"] = json!([0, 50]);
        let answer = evaluate(&direct, params).await;
        for result in answer["results"].as_array().unwrap() {
            let pick = &result["pick"];
            assert_eq!(pick["route"], route, "{answer}");
            assert!(pick["variant"].is_null(), "{answer}");
            assert_eq!(pick["minimum_unverified"], minimum, "{answer}");
            assert_eq!(pick["effort_source"], "none");
            assert_eq!(result["minimum_unverified_count"], 1, "{answer}");
        }
    }
}

#[tokio::test]
async fn catalog_effort_workload_exclusion_does_not_block_pins_or_settings_ladders() {
    let (harness, _) = configured().await;
    serve_efforts(&harness, |route, row| {
        if route == A {
            row["workload.code.excluded"] = json!(true);
            row["workload.code.effort_minimum"] = json!("max");
        }
    });
    let direct = operator(&harness).await;
    let mut params = profiles();
    params["policy_patch"] = only(&[A, B]);
    params["panel"] = json!({"class":"audit"});
    let automatic = evaluate(&direct, params.clone()).await;
    let result = &automatic["results"][0];
    assert_eq!(result["pick"]["route"], B, "{automatic}");
    assert!(
        result["excluded"].as_array().unwrap().contains(&json!({
            "route":A,"reason":"workload_excluded","workload":"code"
        })),
        "{automatic}"
    );
    assert!(
        automatic["panels"][0]["excluded"]
            .as_array()
            .unwrap()
            .contains(&json!({
                "route":A,"reason":"workload_excluded","workload":"code"
            })),
        "{automatic}"
    );
    assert!(!automatic["panels"][0]["seats"]
        .as_array()
        .unwrap()
        .iter()
        .any(|seat| seat["route"] == A));
    params.as_object_mut().unwrap().remove("panel");
    harness.edit_config(|c| {
        c["consumers"][fixture::SUMMARY]["pin"] = json!({"model":A, "variant":"low"})
    });
    let pinned = evaluate(&direct, params.clone()).await;
    let pick = &pinned["results"][0]["pick"];
    assert_eq!(pick["variant"], "low", "{pinned}");
    assert_eq!(pick["effort_source"], "pin");
    harness.edit_config(|c| {
        let settings = c["consumers"][fixture::SUMMARY].as_object_mut().unwrap();
        settings.remove("pin");
        settings.insert("mode".into(), json!("ladder"));
        settings.insert("ladder".into(), json!([format!("{A}:low"), B]));
    });
    let ladder = evaluate(&direct, params).await;
    assert_eq!(ladder["results"][0]["pick"]["variant"], "low", "{ladder}");
    assert_eq!(ladder["results"][0]["pick"]["effort_source"], "pin");
}

#[tokio::test]
async fn catalog_effort_uses_adjusted_elo_and_eq_for_bar_and_panel_ranking() {
    let (harness, _) = configured().await;
    serve_efforts(&harness, |route, row| {
        row["quality.graded_effort"] = json!("high");
        row["workload.code.effort_default"] = json!("low");
        let (elo, eq) = match route {
            A => (1900, 2000),
            B => (2400, 2300),
            _ => (1600, 1700),
        };
        row["quality.elo.effort.low"] = json!(elo);
        row["quality.eq.effort.low"] = json!(eq);
    });
    harness.edit_config(|config| {
        config["panels"]["audit"] = json!({"topK":2,"by":"eq","extra":0,"extraBy":"random"});
        config["models"][A]["elo"] = json!(2250);
        config["models"][A]["eq"] = json!(2200);
        config["models"][B]["elo"] = json!(1850);
        config["models"][B]["eq"] = json!(1800);
    });
    let mut params = profiles();
    params["policy_patch"] = only(&[A, B]);
    params["panel"] = json!({"class":"audit"});
    params["iqs"] = json!([100]);
    params["eq"] = json!(100);
    let direct = operator(&harness).await;
    let answer = evaluate(&direct, params).await;
    let row = &answer["results"][0];
    assert_eq!(
        row["top_routable"],
        json!({"elo":2400.0,"eq":2300.0}),
        "{answer}"
    );
    assert_eq!(row["bar"], json!({"elo":2400.0,"eq":2300.0}), "{answer}");
    let pick = &row["pick"];
    assert_eq!(pick["route"], B, "{answer}");
    assert_eq!(
        pick["grade"],
        json!({"elo":2400.0,"eq":2300.0,"elo_effort":"low","eq_effort":"low"})
    );
    let seats = answer["panels"][0]["seats"].as_array().unwrap();
    assert_eq!(seats.len(), 2, "{answer}");
    assert!(
        seats[0]["route"].as_str().unwrap().starts_with(B),
        "{answer}"
    );
    assert_eq!(seats[0]["variant"], "low");
    assert_eq!(seats[0]["effort_source"], "workload_default");
    assert_eq!(seats[0]["grade"], pick["grade"]);
}

#[tokio::test]
async fn catalog_effort_settings_inherit_and_an_unknown_request_workload_shadows_them() {
    let (harness, _) = configured().await;
    serve_efforts(&harness, |_, row| {
        row["quality.graded_effort"] = json!("high");
        row["workload.code.effort_default"] = json!("low");
        row["workload.reason.effort_default"] = json!("medium");
    });
    let child = format!("{}.child", fixture::SUMMARY);
    harness.edit_config(|c| {
        c["consumers"][fixture::SUMMARY]["workload"] = json!("code");
        c["consumers"][&child] = json!({});
    });
    let direct = operator(&harness).await;
    let mut params = profiles();
    params["consumer"] = json!(child);
    params.as_object_mut().unwrap().remove("workloads");
    let pick = |answer: Value| answer["results"][0]["pick"]["variant"].clone();
    assert_eq!(pick(evaluate(&direct, params.clone()).await), "low");
    params["workloads"] = json!({(fixture::SUMMARY):"reason"});
    assert_eq!(pick(evaluate(&direct, params.clone()).await), "medium");
    params["workloads"][&child] = json!("future-workload");
    assert_eq!(pick(evaluate(&direct, params).await), "high");
}

#[tokio::test]
async fn catalog_effort_omission_preserves_recorded_reply_bytes() {
    let _clock = crate::InjectedClock::set(1_800_000_000_000);
    let (harness, _) = configured().await;
    prepare_runner_preview(&harness);
    Connection::open(harness.store_path().with_file_name("history.db")).unwrap().execute_batch(
        "INSERT INTO account_size_refusal VALUES ('example-provider','old','model-a',17,1,0);
         INSERT INTO credential_entitlement_fences VALUES ('example-provider','removed','auth_required','old-listing',0);"
    ).unwrap();
    let direct = operator(&harness).await;
    let mut actual = evaluate(&direct, json!({
        "iqs":[0,5,50,100], "policy_patch":{"models":{(B):{"pin_only":true}}},
        "runner_declarations":{"broca":broad_runner_slot()}, "head_choices":{"runner_module":"broca"},
    })).await;
    // Wall-clock durations aren't routing evidence and vary between requests.
    actual.as_object_mut().unwrap().remove("snapshot_ms");
    actual.as_object_mut().unwrap().remove("elapsed_ms");
    let recorded: Value =
        serde_json::from_str(include_str!("../../fixtures/s8a3-what-if-base.json")).unwrap();
    assert_eq!(
        serde_json::to_string(&actual).unwrap(),
        serde_json::to_string(&recorded).unwrap()
    );
}

#[tokio::test]
async fn catalog_effort_preview_does_not_change_live_selection_or_head_pin_admission() {
    let (harness, consumer) = configured().await;
    serve_efforts(&harness, |route, row| {
        row["quality.graded_effort"] = json!("high");
        row["workload.code.effort_default"] = json!("low");
        if route == A {
            row["workload.code.excluded"] = json!(true);
            row["workload.code.effort_minimum"] = json!("max");
        }
    });
    harness.edit_config(|c| {
        c["variants"][A] = json!("xhigh");
        c["variants"][B] = json!("xhigh");
        c["models"][A]["pin_only"] = json!(false);
        for route in [C, D] {
            c["models"][route]["not_for"] = json!([fixture::SUMMARY]);
        }
    });
    let request = json!({"consumer":fixture::SUMMARY,"requirements":{"iq":0},"pin":A});
    let live = |(ty, answer): (FrameType, Value)| {
        assert_eq!(ty, FrameType::Response, "{answer}");
        answer["result"]["route"].clone()
    };
    let before = live(consumer.call("route.select", &request).await);
    assert_eq!(before["model"], "model-a");
    assert_eq!(before["variant"], "xhigh");
    let direct = operator(&harness).await;
    let state_before = rows(&harness);
    let mut params = profiles();
    params["policy_patch"] = only(&[A, B]);
    params["head_choices"] = json!({"runner_module":"broca"});
    let answer = evaluate(&direct, params).await;
    let pick = &answer["results"][0]["pick"];
    assert_eq!(pick["route"], B, "{answer}");
    assert_eq!(pick["variant"], "low", "{answer}");
    let models = answer["head_choices"]["models"].as_array().unwrap();
    let head = models.iter().find(|m| m["model"] == "model-a").unwrap();
    assert_eq!(head["default_variant"], "high", "{head}");
    assert_eq!(head["effort_source"], "graded");
    assert_eq!(head["pin_only"], false);
    assert!(head.get("minimum_unverified").is_none());
    assert_eq!(rows(&harness), state_before);
    assert_eq!(
        serde_json::to_vec(&live(consumer.call("route.select", &request).await)).unwrap(),
        serde_json::to_vec(&before).unwrap()
    );
}
