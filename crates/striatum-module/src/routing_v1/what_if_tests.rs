use std::collections::BTreeMap;

use cortexkit_role_routing_conformance::fixture;
use rusqlite::{types::Value as SqlValue, Connection};
use serde_json::{json, Value};
use striatum_core::RouteStateStore;
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness, Route};
use super::dispatch_tests::configured;

#[path = "what_if_concurrency_tests.rs"]
mod concurrency;

#[path = "what_if_sweep_tests.rs"]
mod sweep;

#[path = "what_if_effort_tests.rs"]
mod effort;

pub(super) fn seed_open_offer(
    harness: &Harness,
    provider: &str,
    model: &str,
    account: &str,
    credential: &str,
) {
    sweep::seed_open_offer(harness, provider, model, account, credential);
}

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

pub(super) fn expected_head_effort_detail(public_picker: &Value, elo: f64, eq: f64) -> Value {
    let mut expected = public_picker.clone();
    for model in expected["models"].as_array_mut().unwrap() {
        model["effort_source"] = json!("none");
        model["variant"] = Value::Null;
        model["grade"] = json!({"elo":elo,"eq":eq,
            "elo_effort":"unrecorded","eq_effort":"unrecorded"});
    }
    expected
}

#[tokio::test]
async fn decommissioned_what_if_excludes_route_from_adequacy_and_keeps_reason() {
    let (harness, _) = configured().await;
    let snapshot = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&json!({
        "catalog_version":95,"resolved_at_ms":123,"models":{
            "example-provider/model-b":{"existence":"present","quality.decommissioned":true,"capability.tool_call":true}
        }
    })).unwrap()).unwrap();
    crate::fusiform_catalog::apply_mapped_catalog(
        &harness.store(),
        &snapshot,
        None,
        Some("example-provider"),
    )
    .unwrap();
    let before = rows(&harness);
    let direct = operator(&harness).await;
    let answer = evaluate(&direct, json!({"iqs":[0]})).await;
    let row = &answer["results"][0];
    assert!(
        row["excluded"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["route"] == "example-provider/model-b"
                && entry["reason"] == "decommissioned"),
        "{answer}"
    );
    assert!(
        !row["adequate"]
            .to_string()
            .contains("example-provider/model-b"),
        "{answer}"
    );
    assert_ne!(row["pick"]["route"]["model"], "model-b", "{answer}");
    assert_eq!(rows(&harness), before);
}

#[tokio::test]
async fn s8a3_what_if_matches_recorded_base_response_without_live_commands() {
    let _clock = crate::InjectedClock::set(1_800_000_000_000);
    let (harness, _) = configured().await;
    prepare_runner_preview(&harness);
    let operator = operator(&harness).await;
    Connection::open(harness.store_path().with_file_name("history.db")).unwrap().execute_batch(
        "INSERT INTO account_size_refusal VALUES ('example-provider','old','model-a',17,1,0);
         INSERT INTO credential_entitlement_fences VALUES ('example-provider','removed','auth_required','old-listing',0);"
    ).unwrap();
    let batches = harness.store().account_writer_batch_sizes_for_test();
    let rows_before = rows(&harness);
    let mut answer = evaluate(
        &operator,
        json!({
            "iqs": [0, 5, 50, 100],
            "policy_patch": {"models": {"example-provider/model-b": {"pin_only":true}}},
            "runner_declarations":{"broca":broad_runner_slot()},
            "head_choices":{"runner_module":"broca"}
        }),
    )
    .await;
    // These are timings of the evaluation itself, not routing decisions.
    answer.as_object_mut().unwrap().remove("snapshot_ms");
    answer.as_object_mut().unwrap().remove("elapsed_ms");
    assert_eq!(rows(&harness), rows_before);
    assert_eq!(
        harness.store().account_writer_batch_sizes_for_test(),
        batches
    );
    if std::env::var_os("STRIATUM_RECORD_WHAT_IF").is_some() {
        println!("WHAT_IF_BASE={}", serde_json::to_string(&answer).unwrap());
    } else {
        let recorded: Value =
            serde_json::from_str(include_str!("../../fixtures/s8a3-what-if-base.json")).unwrap();
        assert_eq!(answer, recorded);
    }
}

pub(super) fn rows(harness: &Harness) -> BTreeMap<String, Vec<Vec<SqlValue>>> {
    let state_path = harness.store_path();
    [
        ("main", state_path.clone()),
        ("archive", state_path.with_file_name("history.db")),
    ]
    .into_iter()
    .flat_map(|(schema, path)| {
        let connection = Connection::open(path).unwrap();
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
                let key = if schema == "main" {
                    table
                } else {
                    format!("archive.{table}")
                };
                (key, rows)
            })
            .collect::<Vec<_>>()
    })
    .collect()
}

fn runner_slot() -> Value {
    json!({"runner": {
        "follows_account_choice": true,
        "harnesses": ["codex"],
        "default_harness": "codex"
    }})
}

fn broad_runner_slot() -> Value {
    json!({"runner": {
        "follows_account_choice": true
    }})
}

fn prepare_runner_preview(harness: &Harness) {
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":"broca", "via_vault":true}]);
    });
    let facts = harness.store().get_facts().unwrap();
    let models: serde_json::Map<_, _> = facts
        .iter()
        .map(|fact| {
            let mut fact = fact.clone();
            fact.supports_tool_use = Some(true);
            harness
                .store()
                .upsert_fact(&fact, striatum_core::FactSource::Override)
                .unwrap();
            (
                format!("{}/{}", fact.provider_id, fact.model_id),
                json!({"existence":"present", "capability.tool_call":true}),
            )
        })
        .collect();
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({"catalog_version":41,"resolved_at_ms":123,"models":models}))
            .unwrap(),
    )
    .unwrap();
    let shared = harness.credentials();
    let mut credentials = shared.lock().unwrap();
    credentials.catalog_snapshot = Some(snapshot);
    credentials.runner_declarations.clear();
    super::conformance_tests::publish_fixture_providers(
        &mut credentials,
        "broca",
        &[fixture::PRICED_PROVIDER, fixture::SUBSCRIPTION_PROVIDER],
        true,
    );
}

fn credential_state(harness: &Harness) -> String {
    let shared = harness.credentials();
    let credentials = shared.lock().unwrap();
    format!("{credentials:?}")
}

#[tokio::test]
async fn what_if_runner_declaration_narrows_routes_without_publishing() {
    let (harness, _) = configured().await;
    prepare_runner_preview(&harness);
    let operator = operator(&harness).await;
    let original = evaluate(&operator, json!({"iqs":[0], "runner":"broca"})).await;
    assert!(original["results"][0]["pick"].is_object(), "{original}");
    assert!(!original["results"][0]["adequate"]
        .as_array()
        .unwrap()
        .is_empty());
    let before = rows(&harness);
    let credentials = credential_state(&harness);
    let writes = harness.store().account_write_transaction_count_for_test();
    let narrowed = evaluate(
        &operator,
        json!({
            "iqs":[0], "runner":"broca", "runner_declarations":{"broca":runner_slot()}
        }),
    )
    .await;
    assert!(narrowed["results"][0]["pick"].is_null(), "{narrowed}");
    assert!(narrowed["results"][0]["refusal"].is_object(), "{narrowed}");
    assert!(narrowed["results"][0]["adequate"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        original["results"],
        evaluate(&operator, json!({"iqs":[0], "runner":"broca"})).await["results"]
    );
    assert_eq!(before, rows(&harness));
    assert_eq!(credentials, credential_state(&harness));
    assert_eq!(
        writes,
        harness.store().account_write_transaction_count_for_test()
    );
}

#[tokio::test]
async fn what_if_head_choices_matches_installed_declaration_and_keeps_live_state() {
    let (harness, _) = configured().await;
    prepare_runner_preview(&harness);
    let operator = operator(&harness).await;
    let request = json!({"runner_module":"broca"});
    let (ty, baseline) = operator.call("route.head_choices", &request).await;
    assert_eq!(ty, FrameType::Response, "{baseline}");
    assert!(
        !baseline["result"]["models"].as_array().unwrap().is_empty(),
        "{baseline}"
    );
    let before = rows(&harness);
    let credentials = credential_state(&harness);
    let writes = harness.store().account_write_transaction_count_for_test();
    let slot = runner_slot();
    let preview = evaluate(
        &operator,
        json!({
            "runner_declarations":{"broca":slot},
            "head_choices":{"runner_module":"broca", "harness":"codex"}
        }),
    )
    .await;
    assert_eq!(preview["head_choices"]["models"], json!([]), "{preview}");
    assert_eq!(before, rows(&harness));
    assert_eq!(credentials, credential_state(&harness));
    assert_eq!(
        writes,
        harness.store().account_write_transaction_count_for_test()
    );
    let (ty, unchanged) = operator.call("route.head_choices", &request).await;
    assert_eq!(ty, FrameType::Response, "{unchanged}");
    assert_eq!(baseline, unchanged);
    let policy_models: serde_json::Map<_, _> = harness
        .store()
        .get_facts()
        .unwrap()
        .iter()
        .map(|fact| {
            (
                format!("{}/{}", fact.provider_id, fact.model_id),
                json!({"not_for":["head"]}),
            )
        })
        .collect();
    let policy_preview = evaluate(
        &operator,
        json!({
            "head_choices":request, "policy_patch":{"models":policy_models}
        }),
    )
    .await;
    assert_eq!(
        policy_preview["head_choices"]["models"],
        json!([]),
        "{policy_preview}"
    );

    let shared = harness.credentials();
    // Install through the same verified catalog parser used by catalog refreshes.
    let original_declarations = shared.lock().unwrap().runner_declarations.clone();
    shared.lock().unwrap().runner_declarations = super::runners::declarations_from_catalog(
        &json!([{"module_id":"broca", "nonce_verified":true,
            "role_declarations":{"routing/v1":slot}}]),
    );
    let (ty, installed) = operator
        .call(
            "route.head_choices",
            &json!({"runner_module":"broca", "harness":"codex"}),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{installed}");
    assert_eq!(
        preview["head_choices"],
        expected_head_effort_detail(&installed["result"], 1800.0, 1800.0)
    );
    let replacement = evaluate(
        &operator,
        json!({
            "runner_declarations":{"broca":broad_runner_slot()}, "head_choices":request
        }),
    )
    .await;
    assert_eq!(
        replacement["head_choices"],
        expected_head_effort_detail(&baseline["result"], 1800.0, 1800.0),
        "{replacement}"
    );
    let (ty, still_installed) = operator
        .call(
            "route.head_choices",
            &json!({"runner_module":"broca", "harness":"codex"}),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{still_installed}");
    assert_eq!(installed, still_installed);
    shared.lock().unwrap().runner_declarations = original_declarations;
    assert_eq!(credentials, credential_state(&harness));
    assert_eq!(before, rows(&harness));
}

#[tokio::test]
async fn what_if_refuses_invalid_runner_slots_with_the_catalog_parser_error() {
    let (harness, _) = configured().await;
    let operator = operator(&harness).await;
    let before = rows(&harness);
    let credentials = credential_state(&harness);
    for slot in [
        json!({"runner":{"harnesses":"not-an-array"}}),
        json!({"runner":{"harnesses":[false]}}),
        json!({"runner":{"follows_account_choice":"yes"}}),
        json!({"runner":{}, "consumers":"not-an-array"}),
    ] {
        let error = super::runners::parse_declaration(&slot).unwrap_err();
        let (ty, body) = operator
            .call(
                "route.what_if",
                &json!({
                    "consumer":fixture::SUMMARY, "iqs":[0], "runner_declarations":{"broca":slot}
                }),
            )
            .await;
        assert_eq!(ty, FrameType::Error, "{body}");
        assert_eq!(body["code"], "invalid_request", "{body}");
        assert!(body.to_string().contains(&error), "{body}: {error}");
    }
    assert_eq!(before, rows(&harness));
    assert_eq!(credentials, credential_state(&harness));
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
async fn what_if_changes_no_rows_even_when_account_loading_reconciles_old_evidence() {
    let (harness, _) = configured().await;
    prepare_runner_preview(&harness);
    let operator = operator(&harness).await;
    Connection::open(harness.store_path().with_file_name("history.db")).unwrap().execute_batch(
        "INSERT INTO account_size_refusal VALUES ('example-provider','old','model-a',17,1,0);
         INSERT INTO credential_entitlement_fences VALUES ('example-provider','removed','auth_required','old-listing',0);"
    ).unwrap();
    let before = rows(&harness);
    let writes = harness.store().account_write_transaction_count_for_test();
    let answer = evaluate(
        &operator,
        json!({
            "iqs": [0, 5, 50, 100],
            "policy_patch": {"models": {"example-provider/model-b": {"pin_only":true}}},
            "runner_declarations":{"broca":broad_runner_slot()},
            "head_choices":{"runner_module":"broca"}
        }),
    )
    .await;
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
async fn scoring_anchor_follows_the_effective_mode_for_decisions_and_previews() {
    for (settings, anchor) in [
        (json!({}), 5),
        (
            json!({"providers":{"example-subscription":{"banked_resets":"capacity"}}}),
            5,
        ),
        (json!({"overqualification":{"mode":"cost_scaled"}}), 7),
    ] {
        let (harness, consumer) = configured().await;
        harness.edit_config(|config| {
            if let Some(overqualification) = settings.get("overqualification") {
                config["overqualification"] = overqualification.clone();
            }
            if settings.get("providers").is_some() {
                config["providers"]["example-subscription"]["banked_resets"] = json!("capacity");
            }
        });
        for (request, id_field) in [(
            json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50}}),
            "decision_id",
        )] {
            let (ty, reply) = consumer.call("route.select", &request).await;
            assert_eq!(ty, FrameType::Response, "{reply}");
            let decision = harness
                .store()
                .get_decision_by_id(reply["result"][id_field].as_str().unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(decision.anchor_version, anchor, "{settings}: {reply}");
        }
        let operator = operator(&harness).await;
        let before = rows(&harness);
        assert_eq!(
            evaluate(&operator, json!({"iqs":[50]})).await["anchor_version"],
            anchor
        );
        for (mode, expected) in [("additive", 5), ("cost_scaled", 7)] {
            let preview = evaluate(
                &operator,
                json!({"iqs":[50],"pricing_patch":{"overqualification":mode}}),
            )
            .await;
            assert_eq!(preview["anchor_version"], expected);
        }
        assert_eq!(
            before,
            rows(&harness),
            "preview anchors must not write decisions"
        );
    }
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
        json!({"runner_declarations":{"broca":{}}}),
        json!({"head_choices":{}}),
        json!({"head_choices":{"runner_module":42}}),
        json!({"head_choices":{"runner_module":"broca", "unexpected":true}}),
        json!({"unexpected":true}),
        json!({"pricing_patch":{"unknown":true}}),
        json!({"pricing_patch":{"overqualification":"other"}}),
        json!({"pricing_patch":{"overqualification":{"mode":"cost_scaled"}}}),
        json!({"pricing_patch":{"banked_resets":{"openai":"other"}}}),
        json!({"pricing_patch":{"banked_resets":{"bad/provider":"capacity"}}}),
        json!({"pacing":"max"}),
        json!({"pacing":"expiry"}),
        json!({"quota_override":[]}),
        json!({"quota_override":[],"quota_override_at_ms":0}),
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

#[path = "what_if_pricing_tests.rs"]
mod pricing;

/// Five same-IQ routes in distinct families, with an audit policy of two IQ
/// top seats and one random extra, so the seed decides the third seat.
async fn panel_harness() -> (std::sync::Arc<Harness>, Route, Vec<String>) {
    let harness = Harness::new("what-if-panels");
    let mut catalog = fixture::catalog();
    let original = catalog.models[0].clone();
    catalog.models = ["claude", "gpt", "gemini", "grok", "qwen"]
        .iter()
        .enumerate()
        .map(|(i, family)| {
            let mut model = original.clone();
            model.model = format!("panel-{i}");
            model.family = Some((*family).into());
            model.iq = 90;
            model.context_window = 200_000;
            model.max_output_tokens = 32_000;
            model
        })
        .collect();
    let routes: Vec<_> = catalog.models.iter().map(|m| m.route_key()).collect();
    for runner in &mut catalog.runners {
        runner.route_keys = routes.clone();
    }
    super::conformance_tests::install_catalog(&harness, &catalog);
    super::conformance_tests::install_panel_families(&harness, &catalog);
    harness.edit_config(|c| {
        c["panels"]["audit"] = json!({"top_k":2,"by":"iq","extra":1,"extra_by":"random","floor":0});
        c["consumers"][fixture::SUMMARY] = json!({"owner":"prefrontal"});
    });
    let owner = harness.bind(reserved("prefrontal"), None).await;
    (harness, owner, routes)
}

async fn dry_panels(route: &Route, panel: Value, extra: Value) -> Value {
    let mut params = json!({"consumer": fixture::SUMMARY, "panel": panel});
    params
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let (ty, reply) = route.call("route.what_if", &params).await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    reply["result"].clone()
}

/// The comparable part of each seat: what was seated, where and why.
fn dry_seats(row: &Value) -> Vec<Value> {
    row["seats"]
        .as_array()
        .unwrap()
        .iter()
        .map(|seat| {
            json!({"slot": seat["slot"], "phase": seat["phase"], "route": seat["route"],
                "variant": seat["variant"], "family": seat["family"]})
        })
        .collect()
}

async fn live_panel(
    route: &Route,
    request: &Value,
) -> cortexkit_role_routing::panel::SelectPanelAnswer {
    let (ty, reply) = route.call("route.select_panel", request).await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    serde_json::from_value(reply["result"].clone()).unwrap()
}

fn live_seats(answer: &cortexkit_role_routing::panel::SelectPanelAnswer) -> Vec<Value> {
    answer
        .seats
        .iter()
        .map(|seat| {
            json!({"slot": seat.slot, "phase": seat.phase, "route": seat.answer.route.route_key(),
                "variant": seat.answer.route.variant, "family": seat.family})
        })
        .collect()
}

#[tokio::test]
async fn panel_seats_skip_evidence_but_what_if_keeps_full_evidence() {
    let (harness, owner, _) = panel_harness().await;
    let operator = operator(&harness).await;
    let config = harness.config_path_for_test();
    let before = super::select::evidence_passes(&config);
    for estimate in [true, false] {
        let (ty, answer) = owner.call("route.select_panel", &json!({
            "consumer":fixture::SUMMARY, "class":"audit", "seed":"evidence", "estimate":estimate
        })).await;
        assert_eq!(ty, FrameType::Response, "{answer}");
        assert_eq!(
            answer["result"]["seats"].as_array().unwrap().len(),
            3,
            "{answer}"
        );
        assert_eq!(
            super::select::evidence_passes(&config),
            before,
            "panel estimate={estimate}"
        );
    }
    let panel = dry_panels(
        &operator,
        json!({"class":"audit", "seed":"evidence"}),
        json!({}),
    )
    .await;
    assert_eq!(panel["panels"][0]["seats"].as_array().unwrap().len(), 3);
    assert_eq!(super::select::evidence_passes(&config), before);
    let answer = evaluate(&operator, json!({})).await;
    assert_eq!(super::select::evidence_passes(&config) - before, 3);
    for row in answer["results"].as_array().unwrap() {
        assert!(row["bar"]["elo"].is_number(), "{row}");
        assert!(!row["adequate"].as_array().unwrap().is_empty(), "{row}");
        assert!(!row["excluded"].as_array().unwrap().is_empty(), "{row}");
    }
}

#[tokio::test]
async fn what_if_panel_seats_what_select_panel_seats_for_the_same_seed() {
    let (harness, owner, _) = panel_harness().await;
    let operator = operator(&harness).await;
    let before = rows(&harness);
    let writes = harness.store().account_write_transaction_count_for_test();
    let mut dry = Vec::new();
    for seed in [None, Some("fixed"), Some("other")] {
        let mut panel = json!({"class": "audit"});
        if let Some(seed) = seed {
            panel["seed"] = json!(seed);
        }
        let result = dry_panels(&operator, panel, json!({})).await;
        assert_eq!(result["results"], json!([]), "{result}");
        let row = result["panels"][0].clone();
        assert_eq!(row["class"], "audit");
        assert_eq!(
            row["policy"],
            json!({"source":"settings","top_k":2,"by":"iq","extra":1,"extra_by":"random",
                "floor":0,"width":3})
        );
        assert_eq!(row["seed"], seed.unwrap_or(""), "{row}");
        assert_eq!(
            row["seed_source"],
            if seed.is_some() { "request" } else { "default" }
        );
        assert_eq!(row["shortfall"], Value::Null, "{row}");
        assert_eq!(row["funnel"]["catalog"], 5, "{row}");
        assert_eq!(row["funnel"]["initiallyAdmitted"], 3, "{row}");
        let seats = row["seats"].as_array().unwrap();
        assert_eq!(seats.len(), 3, "{row}");
        assert_eq!(
            seats[0]["chosen_by"],
            "top-2 seat ranked 1 by iq; clears class floor 0"
        );
        assert!(seats[2]["chosen_by"]
            .as_str()
            .unwrap()
            .starts_with("extra seat drawn at random with seed"));
        dry.push((seed, dry_seats(&row)));
    }
    assert_eq!(before, rows(&harness), "a dry-run panel must write nothing");
    assert_eq!(
        writes,
        harness.store().account_write_transaction_count_for_test()
    );
    for (seed, seats) in dry {
        let mut request = json!({"class": "audit", "consumer": fixture::SUMMARY});
        if let Some(seed) = seed {
            request["seed"] = json!(seed);
        }
        let live = live_panel(&owner, &request).await;
        assert_eq!(seats, live_seats(&live), "seed {seed:?}");
    }
}

#[tokio::test]
async fn what_if_panel_seed_decides_the_random_extra_seat_deterministically() {
    let (harness, _, _) = panel_harness().await;
    let operator = operator(&harness).await;
    let mut extras = std::collections::BTreeSet::new();
    for seed in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        let panel = json!({"class": "audit", "seed": seed});
        let first = dry_panels(&operator, panel.clone(), json!({})).await;
        let again = dry_panels(&operator, panel, json!({})).await;
        assert_eq!(first["panels"], again["panels"], "seed {seed}");
        extras.insert(first["panels"][0]["seats"][2]["route"].to_string());
    }
    assert!(
        extras.len() > 1,
        "the seed never changed the extra seat: {extras:?}"
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn what_if_all_panel_classes_keep_the_floor_when_strong_families_are_walled() {
    use super::request_cpu_bench::{representative_fixture, FixtureCounts, FIXTURE_NOW};

    let _clock = crate::InjectedClock::set(FIXTURE_NOW);
    let bench = representative_fixture(FixtureCounts {
        subscription_rules: 12,
        exclusion_rules: 12,
        runner_route_rules: 1,
        catalog_facts: 12,
        listed_credentials: 4,
    });
    let harness = &bench.harness;
    // The CPU fixture starts with equal scores. Give it four strong independent
    // families and eight weak ones so admission walls can expose a quality gap.
    let mut facts = harness.store().get_facts().unwrap();
    for fact in &mut facts {
        let index: usize = fact
            .model_id
            .strip_prefix("model-")
            .unwrap()
            .parse()
            .unwrap();
        fact.family = Some(format!("family-{index}"));
        fact.scores.elo = if index < 4 {
            2000.0 - 20.0 * index as f64
        } else {
            593.0
        };
        harness
            .store()
            .upsert_fact(fact, striatum_core::FactSource::Override)
            .unwrap();
    }
    let operator = operator(harness).await;
    let panel = json!({"class":["audit","design_review","evaluate","diagnose","explain","spec","lab"],"seed":"floor-comparison"});
    let before = rows(harness);
    let baseline = dry_panels(&operator, panel.clone(), json!({})).await;
    let models: serde_json::Map<_, _> = (1..4)
        .map(|index| {
            (
                format!("cpu-provider-0/model-{index}"),
                json!({"not_for":["prefrontal.athena","prefrontal.lab"]}),
            )
        })
        .collect();
    let walled = dry_panels(&operator, panel, json!({"policy_patch":{"models":models}})).await;
    for (scenario, result) in [("baseline", &baseline), ("walled", &walled)] {
        for row in result["panels"].as_array().unwrap() {
            println!(
                "PANEL_COMPARISON {}",
                json!({"scenario":scenario,"class":row["class"],
                "policy":row["policy"],"seats":dry_seats(row),"shortfall":row["shortfall"]})
            );
        }
    }
    assert_eq!(
        before,
        rows(harness),
        "panel previews must not write to the bench store"
    );
    for (result, counts) in [
        (&baseline, [3, 3, 3, 3, 1, 4, 4]),
        (&walled, [1, 1, 1, 1, 1, 1, 4]),
    ] {
        for (row, count) in result["panels"].as_array().unwrap().iter().zip(counts) {
            assert_eq!(row["seats"].as_array().unwrap().len(), count, "{row}");
        }
    }
    for index in [0, 1, 2, 3, 5] {
        let row = &walled["panels"][index];
        assert_eq!(row["seats"][0]["route"], "cpu-provider-0/model-0");
        assert_eq!(row["shortfall"]["code"], "floor_unmeetable", "{row}");
        assert!(
            row["shortfall"]["detail"]["causes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|cause| { cause["reason"] == "below_iq_floor" }),
            "{row}"
        );
    }
}

#[tokio::test]
async fn what_if_panel_policy_patch_changes_seats_like_the_same_live_setting() {
    let (harness, owner, routes) = panel_harness().await;
    let operator = operator(&harness).await;
    let panel = json!({"class": ["audit", "lab"], "seed": "fixed"});
    let baseline = dry_panels(&operator, panel.clone(), json!({})).await;
    let seated: Vec<_> = baseline["panels"][0]["seats"]
        .as_array()
        .unwrap()
        .iter()
        .map(|seat| seat["route"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(seated.len(), 3, "{baseline}");
    assert_eq!(baseline["panels"][1]["class"], "lab");
    assert_eq!(baseline["panels"][1]["policy"]["source"], "default");
    assert_eq!(baseline["panels"][1]["policy"]["width"], 4);
    // Leave only two routes for this consumer: the three-seat audit panel
    // falls short, and neither excluded route may be seated.
    let unseated = routes.iter().find(|r| !seated.contains(r)).unwrap();
    let excluded = [seated[0].clone(), unseated.clone(), seated[2].clone()];
    let patch: serde_json::Map<_, _> = excluded
        .iter()
        .map(|route| (route.clone(), json!({"not_for": [fixture::SUMMARY]})))
        .collect();
    let before = rows(&harness);
    let patched = dry_panels(
        &operator,
        panel.clone(),
        json!({"policy_patch": {"models": patch}}),
    )
    .await;
    assert_eq!(before, rows(&harness));
    let audit = &patched["panels"][0];
    let seats = dry_seats(audit);
    assert_eq!(seats.len(), 2, "{audit}");
    assert!(seats
        .iter()
        .all(|seat| !excluded.contains(&seat["route"].as_str().unwrap().to_owned())));
    assert!(audit["shortfall"]["code"].is_string(), "{audit}");
    let causes = audit["shortfall"]["detail"]["causes"].as_array().unwrap();
    for route in &excluded {
        assert!(
            causes
                .iter()
                .any(|cause| cause["model"] == *route && cause["reason"] == "excluded_by_operator"),
            "{route}: {audit}"
        );
    }
    assert_eq!(
        baseline["panels"],
        dry_panels(&operator, panel, json!({})).await["panels"],
        "a patch must not outlive its call"
    );
    // Applying the same setting for real seats the same routes.
    harness.edit_config(|c| {
        for route in &excluded {
            c["models"][route] = json!({"not_for": [fixture::SUMMARY]});
        }
    });
    let live = live_panel(
        &owner,
        &json!({"class": "audit", "consumer": fixture::SUMMARY, "seed": "fixed"}),
    )
    .await;
    assert_eq!(seats, live_seats(&live));
    let live_shortfall = serde_json::to_value(live.shortfall.unwrap()).unwrap();
    assert_eq!(live_shortfall["code"], audit["shortfall"]["code"]);
}

#[tokio::test]
async fn what_if_panel_refuses_unknown_classes_as_select_panel_does() {
    let (harness, owner, _) = panel_harness().await;
    let operator = operator(&harness).await;
    let (ty, live) = owner
        .call(
            "route.select_panel",
            &json!({"class": "future", "consumer": fixture::SUMMARY}),
        )
        .await;
    assert_eq!(ty, FrameType::Error, "{live}");
    let before = rows(&harness);
    for class in [json!("future"), json!(["audit", "future"])] {
        let (ty, dry) = operator
            .call(
                "route.what_if",
                &json!({"consumer": fixture::SUMMARY, "panel": {"class": class}}),
            )
            .await;
        assert_eq!(ty, FrameType::Error, "{dry}");
        assert_eq!(dry, live);
    }
    for panel in [
        json!({"class": []}),
        json!({"class": "audit", "unexpected": true}),
        json!({"class": "audit", "seed": 7}),
        json!({"class": "audit", "package_tokens": "many"}),
    ] {
        let (ty, body) = operator
            .call(
                "route.what_if",
                &json!({"consumer": fixture::SUMMARY, "panel": panel}),
            )
            .await;
        assert_eq!(ty, FrameType::Error, "{body}");
        assert_eq!(body["code"], "invalid_request", "{body}");
    }
    let (ty, body) = operator
        .call("route.what_if", &json!({"consumer": fixture::SUMMARY}))
        .await;
    assert_eq!(ty, FrameType::Error, "iqs or panel is required: {body}");
    assert_eq!(before, rows(&harness));
}

#[tokio::test]
async fn what_if_patch_that_lifts_pin_only_admits_the_route_like_the_same_setting() {
    let (harness, _) = configured().await;
    harness.edit_config(|config| {
        config["models"]["example-provider/model-b"] = json!({"pin_only":true});
    });
    let operator = operator(&harness).await;
    let before = evaluate(&operator, json!({"iqs":[0]})).await;
    assert_ne!(
        before["results"][0]["pick"]["route"], "example-provider/model-b",
        "{before}"
    );
    let lifted = evaluate(
        &operator,
        json!({"iqs":[0],"policy_patch":{"models":{"example-provider/model-b":{"pin_only":false}}}}),
    )
    .await;
    assert_eq!(
        lifted["results"][0]["pick"]["route"], "example-provider/model-b",
        "{lifted}"
    );
    harness.edit_config(|config| {
        config["models"]["example-provider/model-b"] = json!({"pin_only":false});
    });
    let written = evaluate(&operator, json!({"iqs":[0]})).await;
    assert_eq!(written["results"], lifted["results"]);
}
