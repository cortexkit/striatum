//! Estimates travel through the same bound frame handler as live panels.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use cortexkit_role_routing_conformance::fixture;
use rusqlite::{types::Value as SqlValue, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subc_protocol::FrameType;

use super::conformance_tests::{install_catalog, install_panel_families, reserved, Harness, Route};

const NOW: u64 = 1_800_000_000_000;

async fn configured() -> (Arc<Harness>, Route) {
    let harness = Harness::new("panel-estimate");
    let mut catalog = fixture::catalog();
    let original = catalog.models[0].clone();
    catalog.models = ["claude", "gpt", "gemini"]
        .iter()
        .enumerate()
        .map(|(i, family)| {
            let mut model = original.clone();
            model.model = format!("panel-{i}");
            model.family = Some((*family).into());
            model.iq = 90;
            model.context_window = if i == 2 { 128_000 } else { 200_000 };
            model.max_output_tokens = 32_000;
            model
        })
        .collect();
    let routes: Vec<_> = catalog
        .models
        .iter()
        .map(|model| model.route_key())
        .collect();
    for runner in &mut catalog.runners {
        runner.route_keys = routes.clone();
    }
    install_catalog(&harness, &catalog);
    install_panel_families(&harness, &catalog);
    harness.edit_config(|config| {
        config["panels"]["audit"] =
            json!({"top_k":2,"by":"iq","extra":1,"extra_by":"random","floor":0});
        config["consumers"][fixture::SUMMARY] = json!({"owner":"prefrontal-core"});
    });
    let route = harness.bind(reserved("prefrontal-core"), None).await;
    (harness, route)
}

fn request(tokens: u64) -> Value {
    json!({"class":"audit", "consumer":fixture::SUMMARY, "seed":"fixed",
        "package_tokens":tokens, "temperatures":[0.2,0.4,0.6], "estimate":true})
}

async fn logged_panel(estimate: bool, slow: bool) -> (Value, Vec<String>) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, route) = configured().await;
    let mut params = request(60_000);
    params["estimate"] = json!(estimate);
    crate::stage_timing::LOG_LINES.with(|lines| *lines.borrow_mut() = Some(Vec::new()));
    if slow {
        crate::stage_timing::ACCOUNT_DELAY.with(|delay| {
            delay.set(crate::stage_timing::SLOW_ACCOUNT_CALL + std::time::Duration::from_millis(50))
        });
    }
    let (ty, reply) = harness
        .finish_dispatched(route.request_frame("route.select_panel", &params))
        .await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    let lines = crate::stage_timing::LOG_LINES.with(|lines| lines.borrow_mut().take().unwrap());
    (reply, lines)
}

fn assert_panel_log(reply: &Value, lines: &[String], estimate: bool) {
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(reply["result"]["policy"]["size"], 3);
    let line = &lines[0];
    assert!(
        line.starts_with("[routing] slow route.select_panel total_ms="),
        "{line}"
    );
    for field in [
        "operation=route.select_panel".to_owned(),
        format!("consumer=\"{}\"", fixture::SUMMARY),
        "class=\"audit\"".to_owned(),
        format!("estimate={estimate}"),
        "package_tokens=60000".to_owned(),
        format!(
            "seated={}",
            reply["result"]["seats"].as_array().unwrap().len()
        ),
        format!(
            "requested={}",
            reply["result"]["policy"]["size"].as_u64().unwrap()
        ),
    ] {
        assert!(
            line.split_whitespace().any(|part| part == field),
            "{field}: {line}"
        );
    }
    for field in [
        "total_ms",
        "writer_queue_wait_ms",
        "sql_ms",
        "commit_ms",
        "serialization_ms",
    ] {
        let value = line
            .split_whitespace()
            .find_map(|part| part.strip_prefix(&format!("{field}=")))
            .unwrap();
        let value = value.parse::<f64>().unwrap();
        assert!(
            value >= if field == "total_ms" { 250.0 } else { 0.0 },
            "{line}"
        );
        if field == "sql_ms" {
            assert!(value > 0.0, "{line}");
        }
    }
    assert_eq!(line.lines().count(), 1);
    assert!(
        !line.contains("account_id") && !line.contains("credential"),
        "{line}"
    );
}

#[tokio::test]
async fn slow_panel_estimate_logs_once() {
    let (reply, lines) = logged_panel(true, true).await;
    assert_eq!(reply["result"]["seats"].as_array().unwrap().len(), 2);
    assert_panel_log(&reply, &lines, true);
}

#[tokio::test]
async fn slow_live_panel_logs_once() {
    let (reply, lines) = logged_panel(false, true).await;
    assert_eq!(reply["result"]["seats"].as_array().unwrap().len(), 2);
    assert_panel_log(&reply, &lines, false);
}

#[tokio::test]
async fn fast_panel_logs_nothing() {
    let (reply, lines) = logged_panel(true, true).await;
    assert_panel_log(&reply, &lines, true);
    let (_, lines) = logged_panel(false, false).await;
    assert!(lines.is_empty(), "{lines:?}");
}

// Compare every table, including SQLite sequences: checking only decision rows
// would miss account reconciliation, reservations and probe clock writes.
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
                .prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))
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

fn without_ids(mut answer: Value) -> Value {
    answer.as_object_mut().unwrap().remove("panel_id");
    for seat in answer["seats"].as_array_mut().unwrap() {
        seat["answer"]
            .as_object_mut()
            .unwrap()
            .remove("decision_id");
    }
    answer
}

fn assert_id_free(answer: &Value) {
    assert!(answer.get("panel_id").is_none(), "{answer}");
    for seat in answer["seats"].as_array().unwrap() {
        assert!(seat["answer"].get("decision_id").is_none(), "{answer}");
        assert!(seat["answer"]["runner"]["module"].is_string(), "{answer}");
        assert!(seat["answer"]["context_window"].as_u64().unwrap() > 0);
        assert_eq!(seat["answer"]["max_output_tokens"], 32_000);
    }
}

#[tokio::test]
async fn panel_estimate_matches_live_seats_and_changes_no_store_rows() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, route) = configured().await;
    let before = rows(&harness);
    let writes = harness.store().account_write_transaction_count_for_test();
    let params = request(10_000);
    let (ty, reply) = route.call("route.select_panel", &params).await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    let estimate = reply["result"].clone();
    assert_eq!(estimate["seats"].as_array().unwrap().len(), 3);
    assert_id_free(&estimate);
    assert_eq!(
        rows(&harness),
        before,
        "estimation must leave every store row unchanged"
    );
    assert_eq!(
        harness.store().account_write_transaction_count_for_test(),
        writes
    );
    let mut live_params = params;
    live_params.as_object_mut().unwrap().remove("estimate");
    let (_, live) = route.call("route.select_panel", &live_params).await;
    assert!(live["result"]["panel_id"].is_string(), "{live}");
    assert_eq!(estimate, without_ids(live["result"].clone()));
    assert_ne!(
        rows(&harness),
        before,
        "live selection must exercise the writers"
    );
}

#[tokio::test]
async fn panel_estimate_shortfall_preserves_candidate_package_limits() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, route) = configured().await;
    let before = rows(&harness);
    let mut params = request(60_000);
    let (ty, reply) = route.call("route.select_panel", &params).await;
    assert_eq!(ty, FrameType::Response, "{reply}");
    let estimate = &reply["result"];
    assert_id_free(estimate);
    assert_eq!(estimate["seats"].as_array().unwrap().len(), 2);
    assert_eq!(estimate["shortfall"]["code"], "package_unmeetable");
    let cause = estimate["shortfall"]["detail"]["causes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|cause| cause["model"].as_str().unwrap().ends_with("/panel-2"))
        .unwrap();
    assert_eq!(cause["reason"], "package_too_large");
    assert_eq!(
        cause["detail"],
        json!({"package_tokens":60_000,"budget_tokens":40_000,
        "context_window":128_000,"context_window_known":true,"catalog_max_output_tokens":32_000,
        "output_reserve_tokens":32_000,"prompt_reserve_tokens":16_000})
    );
    assert_eq!(rows(&harness), before);
    params["estimate"] = json!(false);
    let (_, live) = route.call("route.select_panel", &params).await;
    assert_eq!(*estimate, without_ids(live["result"].clone()));
}

#[tokio::test]
async fn panel_estimate_due_model_recovery_test_stays_unclaimed() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = super::metered_tests::quota_probe_harness();
    harness.edit_config(|config| {
        config["panels"]["audit"] = json!({"top_k":1,"by":"iq","extra":0,"floor":0});
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    let before = rows(&harness);
    let mut params = json!({"class":"audit","consumer":"mason","estimate":true,
        "available_routes":["inception/mercury-3"]});
    for _ in 0..2 {
        let (ty, reply) = route.call("route.select_panel", &params).await;
        assert_eq!(ty, FrameType::Response, "{reply}");
        assert_eq!(
            reply["result"]["seats"].as_array().unwrap().len(),
            1,
            "{reply}"
        );
        assert!(harness
            .store()
            .fence_rechecks()
            .unwrap()
            .iter()
            .all(|clock| clock.last_claimed_at.is_none() && clock.claim_owner.is_none()));
        assert_eq!(
            rows(&harness),
            before,
            "a due model test must remain unclaimed"
        );
    }
    // A live selection must claim the same due test. This guards against a
    // fixture whose wall never reached the recovery-reservation path.
    params["estimate"] = json!(false);
    let (_, live) = route.call("route.select_panel", &params).await;
    assert_eq!(
        live["result"]["seats"].as_array().unwrap().len(),
        1,
        "{live}"
    );
    let clocks = harness.store().fence_rechecks().unwrap();
    let clock = clocks
        .iter()
        .find(|clock| clock.kind == "account_route")
        .unwrap();
    assert_eq!(clock.last_claimed_at, Some(NOW as i64));
    assert_eq!(
        clock.claim_owner.as_deref(),
        live["result"]["seats"][0]["answer"]["decision_id"].as_str()
    );
}

#[tokio::test]
async fn panel_estimate_admission_validation_and_normal_flags() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, route) = configured().await;
    let foreign = harness.bind(reserved("foreign-owner"), None).await;
    let before = rows(&harness);
    for patch in [
        json!({"consumer":"not-registered"}),
        json!({"class":"not-configured"}),
        json!({"requirements":{"iq":50}}),
        json!({"runners":123}),
    ] {
        let mut params = request(10_000);
        params
            .as_object_mut()
            .unwrap()
            .extend(patch.as_object().unwrap().clone());
        let estimate = route.call("route.select_panel", &params).await;
        params["estimate"] = json!(false);
        let live = route.call("route.select_panel", &params).await;
        assert_eq!(estimate.0, FrameType::Error, "{estimate:?}");
        assert_eq!(estimate, live);
    }
    let refused = foreign.call("route.select_panel", &request(10_000)).await;
    assert_eq!(refused.1["code"], "consumer_not_owned", "{refused:?}");
    for value in [json!("true"), json!(1), json!([])] {
        let mut params = request(10_000);
        params["estimate"] = value;
        let (_, reply) = route.call("route.select_panel", &params).await;
        assert_eq!(reply["code"], "invalid_request", "{reply}");
        assert_eq!(reply["detail"]["field"], "estimate", "{reply}");
    }
    for value in [json!(true), json!(false), Value::Null] {
        let mut params = request(10_000);
        params["estimate"] = value;
        let (_, reply) = super::describe::without_capability(
            "select_panel_estimate",
            route.call("route.select_panel", &params),
        )
        .await;
        assert_eq!(reply["code"], "invalid_request", "{reply}");
        assert_eq!(reply["detail"]["field"], "estimate", "{reply}");
    }
    assert_eq!(rows(&harness), before);
    let (_, description) = route.call("role.describe", &json!({})).await;
    let groups = description["result"]["capabilities"].as_array().unwrap();
    assert!(groups.contains(&json!("select_panel_estimate")));
    assert!(groups.contains(&json!("consumer_priority")));
    assert!(!groups.contains(&json!("concurrency_full")));
    for value in [json!(false), Value::Null] {
        let mut params = request(10_000);
        params["estimate"] = value;
        let (_, reply) = route.call("route.select_panel", &params).await;
        assert!(reply["result"]["panel_id"].is_string(), "{reply}");
        assert!(reply["result"]["seats"][0]["answer"]["decision_id"].is_string());
    }
}

#[tokio::test]
async fn panel_estimate_retains_project_settings_and_refuses_foreign_stamp() {
    use crate::project_settings::tests::{census, root, write, Fake, ID};
    let _clock = crate::InjectedClock::set(NOW);
    for status in ["applied", "none", "unresolved"] {
        let (harness, route) = configured().await;
        let project = root();
        if status == "applied" {
            write(&project, "{}");
        }
        let source = Fake::new(census(&[project], None));
        source
            .fail
            .store(status == "unresolved", std::sync::atomic::Ordering::Relaxed);
        harness.settings_source(source);
        let mut params = request(10_000);
        params["settings"] = json!({"project_id":ID});
        let before = rows(&harness);
        let (_, estimate) = route.call("route.select_panel", &params).await;
        for seat in estimate["result"]["seats"].as_array().unwrap() {
            assert_eq!(seat["answer"]["project_settings"], status);
        }
        assert_eq!(rows(&harness), before);
        params["estimate"] = json!(false);
        let (_, live) = route.call("route.select_panel", &params).await;
        assert_eq!(estimate["result"], without_ids(live["result"].clone()));
        let scoped = harness
            .bind(reserved("prefrontal-core"), Some("other-project"))
            .await;
        params["estimate"] = json!(true);
        let (_, refused) = scoped.call("route.select_panel", &params).await;
        assert_eq!(refused["code"], "invalid_request", "{refused}");
        assert_eq!(refused["detail"]["field"], "settings.project_id");
    }
}

#[tokio::test]
async fn panel_estimate_serving_bytes_match_vectors() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, route) = configured().await;
    let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors/route-select-panel-estimate-v1");
    let update = std::env::var_os("UPDATE_ROUTE_SELECT_PANEL_ESTIMATE_VECTORS").is_some();
    let mut samples = BTreeMap::new();
    for (name, tokens) in [("three-seats", 10_000), ("shortfall", 60_000)] {
        let frame = route.request_frame("route.select_panel", &request(tokens));
        let request = frame.body.clone();
        let reply = harness.handle(frame).await;
        assert_eq!(reply.header.ty, FrameType::Response);
        let value: Value = serde_json::from_slice(&reply.body).unwrap();
        assert_id_free(&value["result"]);
        // Preserve the handler's bytes rather than reserializing an idealized
        // answer, so wire changes cannot hide behind a test-only encoder.
        let mut pair = b"{\"request\":".to_vec();
        pair.extend_from_slice(&request);
        pair.extend_from_slice(b",\"reply\":");
        pair.extend_from_slice(&reply.body);
        pair.push(b'}');
        samples.insert(format!("{name}.json"), pair);
    }
    let mut sums = String::new();
    for (name, bytes) in &samples {
        let path = directory.join(name);
        if update {
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(&path, bytes).unwrap();
        }
        assert_eq!(
            std::fs::read(path).unwrap(),
            *bytes,
            "{name}: wire byte drift"
        );
        sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(bytes)));
    }
    let sums_path = directory.join("SHA256SUMS");
    if update {
        std::fs::write(&sums_path, &sums).unwrap();
    }
    assert_eq!(std::fs::read_to_string(sums_path).unwrap(), sums);
    let files: BTreeSet<_> = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    let expected: BTreeSet<_> = samples
        .keys()
        .cloned()
        .chain(["README.md".into(), "SHA256SUMS".into()])
        .collect();
    assert_eq!(files, expected, "extra or missing vector files");
}
