use std::collections::BTreeMap;

use cortexkit_role_routing_conformance::fixture;
use rusqlite::{types::Value as SqlValue, Connection};
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness, Route};
use super::dispatch_tests::configured;

async fn operator(harness: &std::sync::Arc<Harness>) -> Route {
    harness.bind(json!({"kind": "direct"}), None).await
}

async fn evaluate(route: &Route, extra: Value) -> Value {
    let mut params = json!({"consumer": fixture::SUMMARY, "iqs": [0, 50, 100]});
    params
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let (ty, reply) = route.call("route.what_if", &params).await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    reply["result"].clone()
}

fn rows(harness: &Harness) -> BTreeMap<String, Vec<Vec<SqlValue>>> {
    let connection = Connection::open(harness.store_path()).unwrap();
    let tables: Vec<String> = connection
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    tables
        .into_iter()
        .map(|table| {
            let mut query = connection
                .prepare(&format!("SELECT * FROM \"{table}\" "))
                .unwrap();
            let columns = query.column_count();
            let rows = query
                .query_map([], |row| {
                    (0..columns)
                        .map(|column| row.get(column))
                        .collect::<Result<Vec<SqlValue>, _>>()
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            (table, rows)
        })
        .collect()
}

#[tokio::test]
async fn what_if_is_an_operator_only_management_query() {
    assert!(crate::handles("route.what_if"));
    let (harness, consumer) = configured().await;
    let op = crate::served_operations()
        .into_iter()
        .find(|op| op.name == "route.what_if")
        .unwrap();
    assert_eq!(op.kind, crate::ManagementOperationKind::Query);
    assert!(super::claims(br#"{"method":"route.what_if","params":{}}"#));
    for route in [
        consumer,
        harness.bind(reserved("broca"), None).await,
        harness.bind(json!({"kind":"unverified"}), None).await,
    ] {
        let (ty, body) = route.call("route.what_if", &json!({})).await;
        assert_eq!(ty, FrameType::Error);
        assert_eq!(body["code"], "not_permitted", "{body}");
    }
    for route in [
        operator(&harness).await,
        harness.bind(reserved("callosum"), None).await,
    ] {
        let response = evaluate(&route, json!({})).await;
        assert_eq!(response["results"].as_array().unwrap().len(), 3);
    }
}

#[tokio::test]
async fn what_if_changes_no_rows_even_when_account_loading_heals_expired_evidence() {
    let (harness, _) = configured().await;
    let operator = operator(&harness).await;
    Connection::open(harness.store_path()).unwrap().execute_batch(
        "INSERT INTO account_size_refusal VALUES ('example-provider','old','model-a','large',1,0);
         INSERT INTO credential_entitlement_fences VALUES ('example-provider','removed','auth_required','old-listing',0);"
    ).unwrap();
    let before = rows(&harness);
    let writes = harness.store().account_write_transaction_count_for_test();
    let answer = evaluate(&operator, json!({"iqs": [0, 5, 50, 100], "policy_patch": {"models": {"example-provider/model-b": {"pin_only":true}}}})).await;
    assert!(answer["results"][0]["pick"].is_object(), "{answer}");
    assert_eq!(
        writes,
        harness.store().account_write_transaction_count_for_test()
    );
    let after = rows(&harness);
    let counts = |rows: &BTreeMap<String, Vec<Vec<SqlValue>>>| {
        rows.iter()
            .map(|(table, rows)| (table.clone(), rows.len()))
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(
        counts(&before),
        counts(&after),
        "a what-if must not insert or prune rows"
    );
    assert_eq!(before, after, "a what-if must not update rows either");
    let refused = evaluate(&operator, json!({"policy_patch":{"models":{"example-provider/model-a":{"not_for":[fixture::SUMMARY]},"example-provider/model-b":{"not_for":[fixture::SUMMARY]},"example-subscription/model-c":{"not_for":[fixture::SUMMARY]},"example-subscription/model-d":{"not_for":[fixture::SUMMARY]}}}})).await;
    assert!(refused["results"][0]["pick"].is_null(), "{refused}");
    assert!(refused["results"][0]["refusal"].is_object(), "{refused}");
    assert_eq!(
        before,
        rows(&harness),
        "refusals must not record failed decisions"
    );
}

#[tokio::test]
async fn what_if_and_select_have_identical_picks_and_scores_for_the_same_inputs() {
    let (harness, consumer) = configured().await;
    let operator = operator(&harness).await;
    let answer = evaluate(&operator, json!({"iqs": [0, 5, 50, 100]})).await;
    for row in answer["results"].as_array().unwrap() {
        let (ty, live) = consumer.call("route.select", &json!({"consumer":fixture::SUMMARY,"requirements":{"iq":row["iq"].as_f64().unwrap() as u8},"detail":true})).await;
        assert_eq!(ty, FrameType::Response, "{live}");
        let live = &live["result"];
        let route = format!(
            "{}/{}",
            live["route"]["provider"].as_str().unwrap(),
            live["route"]["model"].as_str().unwrap()
        );
        assert_eq!(row["pick"]["route"], route, "{row}");
        assert_eq!(row["pick"]["variant"], live["route"]["variant"]);
        let score = &live["detail"]["score"];
        let pricing = &score["objective"]["taskPricing"];
        let task_cost = if pricing["priceUnit"] == "task" {
            pricing["finalCostUsd"].clone()
        } else {
            Value::Null
        };
        assert_eq!(row["pick"]["cost_usd"], task_cost);
        let adequate = row["adequate"]
            .as_array()
            .unwrap()
            .iter()
            .find(|candidate| candidate["route"] == route)
            .unwrap();
        assert_eq!(adequate["fit"], score["metrics"]);
        assert_eq!(adequate["adequacy"], score["adequacy"]);
        assert_eq!(adequate["normalized_cost"], score["objective"]["cost"]);
    }
}

#[tokio::test]
async fn what_if_reports_the_same_estimated_task_dollars_as_live_selection() {
    let (harness, consumer) = configured().await;
    let operator = operator(&harness).await;
    let lookup = super::profile_tests::lookup(
        fixture::SUMMARY,
        50.0,
        &[
            ("example-provider/model-a", 2000.0),
            ("example-provider/model-b", 1000.0),
            ("example-subscription/model-c", 1500.0),
            ("example-subscription/model-d", 3000.0),
        ],
    );
    let before = rows(&harness);
    let answer = operator
        .what_if_with_profiles(&json!({"consumer":fixture::SUMMARY,"iqs":[50]}), &lookup)
        .await
        .unwrap();
    assert_eq!(before, rows(&harness));
    let live = consumer
        .select_with_profiles(
            &json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},"detail":true}),
            &lookup,
        )
        .await
        .unwrap();
    let pick = &answer["results"][0]["pick"];
    assert_eq!(
        pick["route"],
        format!(
            "{}/{}",
            live["route"]["provider"].as_str().unwrap(),
            live["route"]["model"].as_str().unwrap()
        )
    );
    assert!(pick["cost_usd"].is_number(), "{answer}");
    assert_eq!(
        pick["cost_usd"],
        live["detail"]["score"]["objective"]["taskPricing"]["finalCostUsd"]
    );
    let adequate = answer["results"][0]["adequate"].as_array().unwrap();
    let costs: BTreeMap<_, _> = adequate
        .iter()
        .map(|candidate| {
            (
                candidate["route"].as_str().unwrap(),
                candidate["cost_usd"].clone(),
            )
        })
        .collect();
    assert_eq!(costs["example-provider/model-b"], 0.002);
    assert_eq!(costs["example-subscription/model-d"], 0.012);
    assert!(costs["example-provider/model-a"].is_null());
    assert!(costs["example-subscription/model-c"].is_null());
}

#[tokio::test]
async fn what_if_patch_replaces_only_named_fields_and_never_publishes_settings() {
    let (harness, _) = configured().await;
    harness.edit_config(|config| {
        config["models"]["example-provider/model-b"] = json!({"pin_only_for":[fixture::SUMMARY], "not_for":[fixture::SUMMARY], "elo":1900});
    });
    let original = harness.operator_config();
    let parse = |value| {
        serde_json::from_value::<BTreeMap<String, crate::config_overrides::ModelPolicyPatch>>(value)
            .unwrap()
    };
    let patched = original
        .with_model_policy_patch(&parse(
            json!({"example-provider/model-b":{"pin_only_for":[]}}),
        ))
        .unwrap();
    assert!(
        !patched.route_allowed(fixture::SUMMARY, "example-provider/model-b", false),
        "not_for must survive a pin_only_for patch"
    );
    let patched = patched.with_model_policy_patch(&parse(json!({"example-provider/model-b":{"not_for":[], "pin_only":false, "only_for":[fixture::SUMMARY]}}))).unwrap();
    assert!(patched.route_allowed(fixture::SUMMARY, "example-provider/model-b", false));
    assert!(!patched.route_allowed("mason", "example-provider/model-b", false));
    assert!(!original.route_allowed(fixture::SUMMARY, "example-provider/model-b", false));
    let operator = operator(&harness).await;
    let before = evaluate(&operator, json!({"iqs":[0]})).await;
    let changed = evaluate(&operator, json!({"iqs":[0],"policy_patch":{"models":{"example-provider/model-b":{"not_for":[],"pin_only_for":[]}}}})).await;
    assert_eq!(
        changed["results"][0]["pick"]["route"], "example-provider/model-b",
        "{changed}"
    );
    assert_ne!(before["results"][0]["pick"], changed["results"][0]["pick"]);
    assert_eq!(
        before["results"],
        evaluate(&operator, json!({"iqs":[0]})).await["results"]
    );
    assert!(before["results"][0]["excluded"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["route"] == "example-provider/model-b"
            && r["reason"].as_str().unwrap().contains("not_for")));
}

#[tokio::test]
async fn what_if_reports_pin_only_exclusions_and_fractional_demand_bars() {
    let (harness, _) = configured().await;
    harness.edit_config(|config| {
        config["models"]["example-provider/model-b"] = json!({"pin_only_for":[fixture::SUMMARY]});
        config["models"]["example-provider/model-a"] = json!({"elo":1});
    });
    let operator = operator(&harness).await;
    let before = evaluate(&operator, json!({"iqs":[0,100]})).await;
    assert!(
        before["results"][1]["excluded"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["route"] == "example-provider/model-a" && r["reason"] == "below_floor"),
        "{before}"
    );
    assert!(before["results"][0]["excluded"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["route"] == "example-provider/model-b"
            && r["reason"].as_str().unwrap().contains("pin_only_for")));
    let after = evaluate(&operator, json!({"iqs":[0,12.5,100], "policy_patch":{"models":{"example-provider/model-b":{"pin_only_for":[]}}}})).await;
    assert_eq!(after["results"][0]["bar"]["elo"], 0.0);
    assert_eq!(
        after["results"][2]["bar"]["elo"],
        after["results"][2]["top_routable"]["elo"]
    );
    assert_eq!(after["results"][1]["iq"], 12.5);
    assert!(after["results"][1]["bar"]["elo"].as_f64().unwrap() > 0.0);
    assert_eq!(
        after["results"][0]["pick"]["route"],
        "example-provider/model-b"
    );
    assert!(!after["results"][0]["adequate"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn what_if_rejects_invalid_params_before_snapshotting() {
    let (harness, _) = configured().await;
    let operator = operator(&harness).await;
    for patch in [
        json!({"iqs":[]}),
        json!({"iqs":[101]}),
        json!({"eq":-1}),
        json!({"runner":"other"}),
        json!({"policy_patch":{"models":{"bad":{"pin_only":false}}}}),
        json!({"policy_patch":{"models":{"example-provider/model-a":{"elo":2000}}}}),
        json!({"unexpected":true}),
    ] {
        let mut params = json!({"consumer":fixture::SUMMARY,"iqs":[50]});
        params
            .as_object_mut()
            .unwrap()
            .extend(patch.as_object().unwrap().clone());
        let (ty, body) = operator.call("route.what_if", &params).await;
        assert_eq!(ty, FrameType::Error, "{body}");
        assert_eq!(body["code"], "invalid_request", "{body}");
    }
}
