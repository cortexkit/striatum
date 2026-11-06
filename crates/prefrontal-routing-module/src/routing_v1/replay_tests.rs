//! Baseline recordings pass through the real frame dispatcher. Only clocks,
//! generated IDs and scratch-directory names are controlled; replies and the
//! persistent effect tables are compared with checked-in observations.

use std::collections::BTreeMap;
use std::path::Path;

use cortexkit_role_routing_conformance::fixture;
use rusqlite::types::ValueRef;
use serde::Deserialize;
use serde_json::{json, Value};

use super::conformance_tests::{install_catalog, reserved, Harness, Route};
use super::dispatch_tests::configured;

const NOW: i64 = 1_800_000_000_000;
const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/replay");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    body_source: String,
    #[serde(default)]
    config: Value,
    #[serde(default)]
    core: bool,
    #[serde(default)]
    project_id: Option<String>,
    steps: Vec<Step>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Step {
    method: String,
    params: Value,
    #[serde(default)]
    runner: bool,
}

fn expand(value: &mut Value, bindings: &BTreeMap<String, Value>) {
    match value {
        Value::String(text) => {
            if let Some(bound) = bindings.get(text) {
                *value = bound.clone();
            } else {
                assert!(!text.starts_with('$'), "unbound replay value: {text}");
            }
        }
        Value::Array(values) => {
            for value in values {
                expand(value, bindings);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                expand(value, bindings);
            }
        }
        _ => {}
    }
}

fn merge(target: &mut Value, patch: &Value) {
    if let (Some(target), Some(patch)) = (target.as_object_mut(), patch.as_object()) {
        for (key, value) in patch {
            merge(target.entry(key.clone()).or_insert(Value::Null), value);
        }
    } else {
        *target = patch.clone();
    }
}

pub(super) fn persistent_effects(harness: &Harness) -> Value {
    let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
    let mut names = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap();
    let names = names
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut tables = serde_json::Map::new();
    for name in names {
        if name.starts_with("sqlite_")
            || ![
                "state",
                "fence",
                "affinity",
                "admission",
                "usage",
                "report",
                "continuation",
                "block",
                "cooldown",
                "lease",
                "pin",
                "decision",
            ]
            .iter()
            .any(|part| name.contains(part))
        {
            continue;
        }
        let mut statement = connection
            .prepare(&format!("SELECT * FROM \"{}\"", name.replace('"', "\"\"")))
            .unwrap();
        let columns: Vec<String> = statement
            .column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut rows = statement
            .query_map([], |row| {
                let mut object = serde_json::Map::new();
                for (index, column) in columns.iter().enumerate() {
                    let value = match row.get_ref(index)? {
                        ValueRef::Null => Value::Null,
                        ValueRef::Integer(value) => json!(value),
                        ValueRef::Real(value) => json!(value),
                        ValueRef::Text(value) => {
                            let text = std::str::from_utf8(value).unwrap();
                            serde_json::from_str(text).unwrap_or_else(|_| json!(text))
                        }
                        ValueRef::Blob(value) => json!(value),
                    };
                    object.insert(column.clone(), value);
                }
                Ok(Value::Object(object))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        // The store seeds neutral state for its bundled catalog. Omit only
        // those untouched rows; every non-neutral health or wall field remains
        // in the oracle, including any new field a later migration adds.
        if name == "model_route_state" {
            rows.retain(|row| {
                !row.as_object()
                    .unwrap()
                    .iter()
                    .all(|(field, value)| match field.as_str() {
                        "route_key" | "capacity_key" | "provider_id" | "model_id" | "variant" => {
                            true
                        }
                        "updated_at"
                        | "consecutive_terminal_failures"
                        | "failure_count"
                        | "quota_error_count" => value == 0,
                        _ => value.is_null(),
                    })
            });
        }
        rows.sort_by_key(Value::to_string);
        tables.insert(name, json!(rows));
    }
    Value::Object(tables)
}

pub(super) fn normalize_paths(value: Value, harness: &Harness) -> Value {
    let path = harness.store_path();
    let directory = path.parent().unwrap().to_str().unwrap();
    let scratch = cortexkit_test_support::scratch_root();
    serde_json::from_str(
        &value
            .to_string()
            .replace(directory, "<replay>")
            .replace(scratch.to_str().unwrap(), "<scratch>"),
    )
    .unwrap()
}

async fn replay(file: &str) {
    let path = Path::new(ROOT).join(format!("{file}.requests.json"));
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert!(!corpus.cases.is_empty(), "{} has no cases", path.display());
    let mut observations = Vec::new();
    for case in corpus.cases {
        assert!(
            !case.body_source.is_empty(),
            "{} has no body source",
            case.name
        );
        assert!(!case.steps.is_empty(), "{} has no requests", case.name);
        let _clock = crate::InjectedClock::set(NOW as u64);
        let _inputs = prefrontal_routing_store::replay::Inputs::new(NOW).install();
        let (harness, mut route) = configured().await;
        if case.core {
            let mut catalog = fixture::catalog();
            catalog.runners[0].module = "broca".into();
            catalog.runners[0].route_keys = catalog
                .models
                .iter()
                .map(|model| model.route_key())
                .collect();
            install_catalog(&harness, &catalog);
            harness.edit_config(|config| {
                for consumer in ["athena", "lab", "core", "mason", "sidekick"] {
                    config["consumers"][consumer] = json!({"owner": "prefrontal-core"});
                }
            });
            route = harness
                .bind(reserved("prefrontal-core"), case.project_id.as_deref())
                .await;
        }
        if !case.config.is_null() {
            harness.edit_config(|config| merge(config, &case.config));
        }
        let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
        let mut bindings = BTreeMap::from([("$consumer".into(), json!(fixture::SUMMARY))]);
        let mut steps = Vec::new();
        for step in case.steps {
            let mut params = step.params;
            expand(&mut params, &bindings);
            let (ty, reply) = if step.runner { &runner } else { &route }
                .call(&step.method, &params)
                .await;
            for key in ["decision_id", "decisionID"] {
                if let Some(id) = reply["result"][key].as_str() {
                    bindings.insert("$decision".into(), json!(id));
                }
            }
            if let Some(credential) = reply["result"]["account"]["credential_id"].as_str() {
                bindings.insert("$credential".into(), json!(credential));
            }
            steps.push(normalize_paths(
                json!({"method": step.method, "params": params,
                    "frame_type": format!("{ty:?}"), "reply": reply,
                    "state": persistent_effects(&harness)}),
                &harness,
            ));
        }
        observations
            .push(json!({"name": case.name, "body_source": case.body_source, "steps": steps}));
    }
    let observed = json!({"clock_ms": NOW, "cases": observations});
    assert_or_record(file, observed);
}

pub(crate) fn assert_or_record(file: &str, mut observed: Value) {
    let golden = Path::new(ROOT).join(format!("{file}.golden.json"));
    if std::env::var_os("PREFRONTAL_RECORD_ROUTING_REPLAY").is_some()
        || (file == "availability-public"
            && std::env::var_os("PREFRONTAL_RECORD_ROUTING_PUBLIC_AVAILABILITY").is_some())
        || (file == "head-choices-difference-9"
            && std::env::var_os("PREFRONTAL_RECORD_SHAPE_DIFFERENCES").is_some())
    {
        std::fs::write(
            &golden,
            format!("{}\n", serde_json::to_string_pretty(&observed).unwrap()),
        )
        .unwrap();
    } else {
        let mut expected: Value = serde_json::from_slice(&std::fs::read(&golden).unwrap()).unwrap();
        if file == "client-shapes" {
            let differences: Value = serde_json::from_slice(
                &std::fs::read(Path::new(ROOT).join("head-choices-difference-9.golden.json"))
                    .unwrap(),
            )
            .unwrap();
            for set in expected["sets"].as_array_mut().unwrap() {
                for body in set["bodies"].as_array_mut().unwrap() {
                    if body["method"] == "route.head_choices" {
                        let difference = differences
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|entry| entry["source"] == body["source"])
                            .unwrap();
                        assert_eq!(body["reply"]["code"], "not_permitted");
                        body["reply"] = difference["reply"].clone();
                        body["frame_type"] = difference["frame_type"].clone();
                    }
                }
            }
        }
        if file == "explain-spellings" {
            // This is a spelling probe, not captured client traffic. Bare
            // decision_id now names the public read, which cannot read an
            // older-surface decision. Keep the other five frozen spellings.
            let step = &mut expected["cases"][0]["steps"][3];
            assert!(step["params"]["decision_id"].is_string());
            step["frame_type"] = json!("Error");
            let refusal = cortexkit_role_routing::errors::Refusal::unknown_decision();
            step["reply"] = json!({"code":refusal.code.as_str(),"message":refusal.message,"detail":refusal.detail});
        }
        apply_report_outcome_difference(&mut expected);
        project_additive_shape_members(&mut observed);
        project_additive_shape_members(&mut expected);
        if file == "availability" {
            apply_availability_difference(&mut expected);
        }
        assert!(
            observed == expected,
            "routing replay differs: {file}: {}",
            first_difference("$", &observed, &expected)
        );
    }
}

/// Keep the historical availability observations intact while declaring each
/// corrected public refusal and its persisted decision effects by row.
fn apply_availability_difference(expected: &mut Value) {
    let differences: Value = serde_json::from_str(include_str!(
        "../../fixtures/replay/availability-difference.json"
    ))
    .unwrap();
    for difference in differences.as_array().unwrap() {
        let case = expected["cases"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|case| case["name"] == difference["row"])
            .unwrap();
        for patch in difference["changes"].as_array().unwrap() {
            let value = case.pointer_mut(patch["path"].as_str().unwrap()).unwrap();
            if patch["op"] == "append" {
                let rows = value.as_array_mut().unwrap();
                assert_eq!(rows.len(), patch["before"].as_u64().unwrap() as usize);
                rows.extend(patch["after"].as_array().unwrap().iter().cloned());
                continue;
            }
            assert_eq!(
                *value, patch["before"],
                "availability difference before: {}",
                patch["path"]
            );
            *value = patch["after"].clone();
        }
    }
}

/// The four declared caller-side outcomes keep affinity and no longer record
/// a provider error. All other recorded replies and effects remain unchanged
/// in the replay expectation.
fn apply_report_outcome_difference(value: &mut Value) {
    let differences: Value = serde_json::from_str(include_str!(
        "../../fixtures/replay/report-outcomes-difference-8.json"
    ))
    .unwrap();
    fn apply(value: &mut Value, differences: &Value) {
        match value {
            Value::Array(values) => values.iter_mut().for_each(|v| apply(v, differences)),
            Value::Object(object) => {
                if object.get("method").and_then(Value::as_str) == Some("route.report") {
                    let params = object.get("params").cloned().unwrap_or(Value::Null);
                    if let Some(difference) = differences
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|d| d["outcome"] == params["outcome"])
                    {
                        if let Some(rows) = object
                            .get_mut("state")
                            .and_then(|s| s.get_mut("model_route_decisions"))
                            .and_then(Value::as_array_mut)
                        {
                            for row in rows.iter_mut().filter(|r| r["id"] == params["decision_id"])
                            {
                                assert_eq!(row["outcome"], difference["before_ledger"]);
                                row["outcome"] = difference["after_ledger"].clone();
                                assert_eq!(row["outcome_error"], params["outcome"]);
                                row["outcome_error"] = Value::Null;
                            }
                        }
                        if let Some(rows) = object
                            .get_mut("state")
                            .and_then(|s| s.get_mut("routing_v1_affinity"))
                            .and_then(Value::as_array_mut)
                        {
                            for row in rows
                                .iter_mut()
                                .filter(|r| r["decision_id"] == params["decision_id"])
                            {
                                assert!(row["ended_at_ms"].is_number());
                                row["ended_at_ms"] = Value::Null;
                            }
                        }
                    }
                }
                object.values_mut().for_each(|v| apply(v, differences));
            }
            _ => {}
        }
    }
    apply(value, &differences);
}

/// Compare the golden response members; dedicated read tests check the new
/// immutable snapshot and additive public status/discovery members.
fn project_additive_shape_members(value: &mut Value) {
    match value {
        Value::Array(values) => values.iter_mut().for_each(project_additive_shape_members),
        Value::Object(object) => {
            // Delivery identities are additive persistence, checked by the
            // tests for usage deduplication and crash recovery, not the pre-add snapshot.
            object.remove("routing_v1_report_delivery");
            if let Some(rows) = object
                .get_mut("routing_v1_decision")
                .and_then(Value::as_array_mut)
            {
                for row in rows {
                    row.as_object_mut().unwrap().remove("selection_json");
                }
            }
            if object.get("method").and_then(Value::as_str) == Some("role.describe") {
                if let Some(result) = object
                    .get_mut("reply")
                    .and_then(|reply| reply.get_mut("result"))
                {
                    for (field, added) in [
                        (
                            "capabilities",
                            &[
                                "explain",
                                "budget_quote",
                                "account_status",
                                "head_choices",
                                "report_outcomes",
                                "report_usage_run_id",
                                "select_pin_slot",
                                "select_available_routes",
                                "select_prefer_prompt_cache",
                                "select_settings",
                                "observe_backoff",
                                "observe_credential_refused",
                                "select_panel",
                                "consumer_models",
                            ][..],
                        ),
                        (
                            "ops",
                            &[
                                "route.explain",
                                "route.budget_quote",
                                "route.account_status",
                                "route.head_choices",
                                "route.select_panel",
                                "route.consumer_models",
                            ][..],
                        ),
                    ] {
                        result[field]
                            .as_array_mut()
                            .unwrap()
                            .retain(|entry| !added.contains(&entry.as_str().unwrap_or("")));
                    }
                }
            }
            if object.get("method").and_then(Value::as_str) == Some("route.account_status") {
                if let Some(result) = object
                    .get_mut("reply")
                    .and_then(|reply| reply.get_mut("result"))
                    .and_then(Value::as_object_mut)
                {
                    result.remove("limit_state");
                    result.remove("reset_at_ms");
                }
            }
            object.values_mut().for_each(project_additive_shape_members);
        }
        _ => {}
    }
}

fn first_difference(path: &str, actual: &Value, expected: &Value) -> String {
    match (actual, expected) {
        (Value::Object(actual), Value::Object(expected)) => {
            for key in actual.keys().chain(expected.keys()) {
                if actual.get(key) != expected.get(key) {
                    return first_difference(
                        &format!("{path}.{key}"),
                        actual.get(key).unwrap_or(&Value::Null),
                        expected.get(key).unwrap_or(&Value::Null),
                    );
                }
            }
        }
        (Value::Array(actual), Value::Array(expected)) => {
            if actual.len() != expected.len() {
                return format!(
                    "{path}: array lengths {} != {}",
                    actual.len(),
                    expected.len()
                );
            }
            for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
                if actual != expected {
                    return first_difference(&format!("{path}[{index}]"), actual, expected);
                }
            }
        }
        _ => {}
    }
    format!("{path}: actual {actual}, expected {expected}")
}

#[tokio::test]
async fn replay_report_outcomes() {
    replay("report-outcomes").await;
}

#[tokio::test]
async fn replay_explain_spellings() {
    replay("explain-spellings").await;
}

#[tokio::test]
async fn replay_legacy_outcomes_and_cooldowns() {
    replay("core").await;
}

#[tokio::test]
async fn replay_client_shapes() {
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(Path::new(ROOT).join("client-sets.json")).unwrap())
            .unwrap();
    let mut sets = Vec::new();
    let mut count = 0;
    let mut head_choices = Vec::new();
    for set in manifest["sets"].as_array().unwrap() {
        let client = set["client"].as_str().unwrap();
        let mut bodies = Vec::new();
        for body in set["bodies"].as_array().unwrap() {
            count += 1;
            let _clock = crate::InjectedClock::set(NOW as u64);
            let path = Path::new(ROOT)
                .join("clients")
                .join(body["path"].as_str().unwrap());
            let bytes = std::fs::read(path).unwrap();
            let params: Value = serde_json::from_slice(&bytes).unwrap();
            let method = body["method"].as_str().unwrap();
            let ids = params["decision_id"].as_str().map(str::to_owned);
            let _inputs = prefrontal_routing_store::replay::Inputs::new(NOW)
                .with_decisions(ids)
                .install();
            let harness = Harness::new("owner-body-replay");
            install_client_catalog(&harness, client);
            let consumer = if client == "magic-context" {
                "mc_summary"
            } else {
                "core"
            };
            let owner = if client == "magic-context" {
                "magic-context"
            } else {
                "prefrontal-core"
            };
            harness.edit_config(|config| {
                config["consumers"][consumer] = json!({"owner":owner});
                if client == "routing-consumer" {
                    config["consumers"]["mason"] = json!({"owner":"prefrontal-core"});
                }
            });
            let principal = if client == "routing-consumer" {
                "prefrontal-core"
            } else {
                client
            };
            let route = harness.bind(reserved(principal), None).await;
            let mut setup = Vec::new();
            if params["decision_id"].is_string() {
                let owner_route = harness.bind(reserved(owner), None).await;
                let pin = if method == "route.account_for" {
                    format!(
                        "{}/{}",
                        params["provider"].as_str().unwrap(),
                        params["model"].as_str().unwrap()
                    )
                } else {
                    "anthropic/claude-opus-5".into()
                };
                let runner = if client == "magic-context" {
                    "thalamus-harness"
                } else {
                    client
                };
                let seed = json!({"consumer":consumer, "requirements":{"iq":50}, "pin":pin,"runners":[runner]});
                let (ty, reply) = owner_route.call("route.select", &seed).await;
                assert_eq!(ty, subc_protocol::FrameType::Response, "{reply}");
                assert_eq!(reply["result"]["decision_id"], params["decision_id"]);
                setup.push(json!({"method":"route.select", "params":seed, "reply":reply}));
            }
            if method == "route.account_observe" {
                let source = if client == "broca" {
                    "broca/01-broca-route.account_for-admit.json"
                } else {
                    "thalamus/route.account_for-1.json"
                };
                let mut seed: Value = serde_json::from_slice(
                    &std::fs::read(Path::new(ROOT).join("clients").join(source)).unwrap(),
                )
                .unwrap();
                seed["admission_id"] = params["admission_id"].clone();
                seed.as_object_mut().unwrap().remove("decision_id");
                let (_, reply) = route.call("route.account_for", &seed).await;
                setup.push(json!({"method":"route.account_for", "params":seed, "reply":reply}));
            }
            let (ty, reply) = route.call(method, &params).await;
            if method == "route.head_choices" {
                use cortexkit_role_routing::head_choices::{
                    check_head_choices, parse_head_choices_request,
                };
                let answer = serde_json::from_value(reply["result"].clone()).unwrap();
                check_head_choices(&parse_head_choices_request(&params).unwrap(), &answer).unwrap();
                head_choices.push(
                    json!({"source":body["path"], "frame_type":format!("{ty:?}"), "reply":reply}),
                );
            }
            use sha2::Digest;
            let sha = format!("{:x}", sha2::Sha256::digest(&bytes));
            bodies.push(normalize_paths(
                json!({"source":body["path"], "sha256":sha,
                "setup":setup, "method":method, "params":params,
                "frame_type":format!("{ty:?}"), "reply":reply,
                "state":persistent_effects(&harness)}),
                &harness,
            ));
        }
        sets.push(
            json!({"client":client, "status":set["status"], "body_source":set["body_source"],
            "blocked_methods":set["blocked_methods"], "bodies":bodies}),
        );
    }
    assert!(count >= 27, "owner corpus must not silently become empty");
    assert_eq!(
        head_choices.len(),
        1,
        "every changed public-choice reply needs its own frozen difference"
    );
    assert_or_record("head-choices-difference-9", json!(head_choices));
    assert_or_record("client-shapes", json!({"clock_ms":NOW, "sets":sets}));
}

async fn broca_decision_tier_pool_admit() -> (std::sync::Arc<Harness>, Route, Value) {
    let mut params: Value =
        serde_json::from_slice(
            &std::fs::read(Path::new(ROOT).join(
                "clients/broca/02-broca-route.account_for-admit-decision-tier-pool-hint.json",
            ))
            .unwrap(),
        )
        .unwrap();
    let harness = Harness::new("broca-candidate-refusal");
    install_client_catalog(&harness, "broca");
    harness.edit_config(|config| {
        config["consumers"]["core"] = json!({"owner":"prefrontal-core"});
    });
    let owner = harness.bind(reserved("prefrontal-core"), None).await;
    let (ty, selected) = owner
        .call(
            "route.select",
            &json!({"consumer":"core", "requirements":{"iq":50},
                "pin":"openai/gpt-6.1-sol", "runners":["broca"]}),
        )
        .await;
    assert_eq!(ty, subc_protocol::FrameType::Response, "{selected}");
    params["decision_id"] = selected["result"]["decision_id"].clone();
    let runner = harness.bind(reserved("broca"), None).await;
    (harness, runner, params)
}

#[tokio::test]
async fn broca_foreign_candidates_keep_unavailable_with_exclusions() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let (_harness, runner, params) = broca_decision_tier_pool_admit().await;
    crate::take_operator_warnings();
    let (ty, reply) = runner.call("route.account_for", &params).await;
    assert_eq!(ty, subc_protocol::FrameType::Error, "{reply}");
    assert_eq!(reply["code"], "account_selection_unavailable", "{reply}");
    assert_eq!(
        reply["detail"]["exclusions"],
        json!([{
            "account_id":"acct-openai-a", "reason":"not_in_runner_candidates",
        }]),
        "{reply}"
    );
    let lines = crate::take_operator_warnings();
    assert_eq!(lines.len(), 1, "{lines:?}");
    let fields: Value = serde_json::from_str(
        lines[0]
            .strip_prefix("[TODO(user): account operation refused] ")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(fields["reason"], "candidates_filter");
    assert_eq!(
        fields["exclusions"],
        "acct-openai-a:not_in_runner_candidates"
    );
    assert_eq!(fields["op"], "route.account_for");
    assert_eq!(fields["principal"], "reserved:broca");
    assert_eq!(fields["provider"], "openai");
    assert_eq!(fields["model"], "gpt-6.1-sol");
    assert_eq!(fields["hold_wait_max_s"], 300);
    for field in [
        "decision_id_present",
        "candidates_present",
        "requested_account_present",
        "conversation_key_present",
    ] {
        assert_eq!(fields[field], true, "{fields}");
    }
    assert!(!lines[0].contains("core-session:"));
    assert!(!lines[0].contains("chatgpt:openai:example-a"));
}

#[tokio::test]
async fn refused_observation_logs_one_reason_without_header_or_credential_material() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let (_harness, runner, _) = broca_decision_tier_pool_admit().await;
    crate::take_operator_warnings();
    let (ty, reply) = runner
        .call(
            "route.account_observe",
            &json!({
                "caller":{"runner":"broca"}, "event_id":"", "kind":"attempt",
                "headers":{"authorization":"Bearer secret-header-value"},
                "unexpected_secret":"secret-request-value",
            }),
        )
        .await;
    assert_eq!(ty, subc_protocol::FrameType::Error, "{reply}");
    assert_eq!(reply["code"], "invalid_request");
    let lines = crate::take_operator_warnings();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].contains("observation_event_id"), "{lines:?}");
    assert!(!lines[0].contains("secret-header-value"));
    assert!(!lines[0].contains("secret-request-value"));
    assert!(!lines[0].contains("authorization"));
}

#[tokio::test]
async fn broca_existing_credential_refused_candidate_is_exhausted() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let (_harness, runner, mut params) = broca_decision_tier_pool_admit().await;
    params["candidates"]["accounts"] = json!([{
        "account_id":"acct-openai-a", "credential_id":"oauth:openai:example-a",
    }]);
    params["requested_account"] = json!({"credential_id":"oauth:openai:example-a"});
    let (ty, observed) = runner
        .call(
            "route.account_observe",
            &json!({
                "event_id":"broca-key-refused", "kind":"credential_refused",
                "credential_id":"oauth:openai:example-a",
            }),
        )
        .await;
    assert_eq!(ty, subc_protocol::FrameType::Response, "{observed}");
    let (ty, reply) = runner.call("route.account_for", &params).await;
    assert_eq!(ty, subc_protocol::FrameType::Error, "{reply}");
    assert_eq!(reply["code"], "account_pool_exhausted", "{reply}");
}

#[tokio::test]
async fn broca_foreign_candidates_do_not_require_login_when_real_account_is_blocked() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let (_harness, runner, params) = broca_decision_tier_pool_admit().await;
    let (ty, observed) = runner
        .call(
            "route.account_observe",
            &json!({
                "event_id":"broca-key-refused", "kind":"credential_refused",
                "credential_id":"oauth:openai:example-a",
            }),
        )
        .await;
    assert_eq!(ty, subc_protocol::FrameType::Response, "{observed}");
    let (ty, reply) = runner.call("route.account_for", &params).await;
    assert_eq!(ty, subc_protocol::FrameType::Error, "{reply}");
    assert_eq!(reply["code"], "account_selection_unavailable", "{reply}");
    assert_eq!(
        reply["detail"]["exclusions"],
        json!([{
            "account_id":"acct-openai-a", "reason":"not_in_runner_candidates",
        }]),
        "{reply}"
    );
}

fn install_client_catalog(harness: &Harness, client: &str) {
    let mut catalog = fixture::catalog();
    let routes = [
        ("anthropic", "claude-opus-5"),
        ("anthropic", "claude-opus-5-5"),
        ("openai", "gpt-6.1-sol"),
        ("openai", "gpt-6-luna"),
    ];
    for (model, (provider, name)) in catalog.models.iter_mut().zip(routes) {
        model.provider = provider.into();
        model.model = name.into();
        model.priced_per_token = false;
    }
    if client == "routing-consumer" {
        let mut model = catalog.models[3].clone();
        model.model = "gpt-5.5".into();
        catalog.models.push(model.clone());
        model.model = "gpt-5.4".into();
        catalog.models.push(model);
    }
    for (account, suffix) in catalog.accounts.iter_mut().zip(["a", "b"]) {
        account.provider = "anthropic".into();
        account.account_id = format!("acct-example-{suffix}");
        account.credential_id = format!("oauth:anthropic:example-{suffix}");
        account.method = "oauth".into();
    }
    let mut openai = catalog.accounts[0].clone();
    openai.provider = "openai".into();
    openai.account_id = "acct-openai-a".into();
    openai.credential_id = "oauth:openai:example-a".into();
    catalog.accounts.push(openai);
    let runner = catalog.runners[0].clone();
    catalog.runners = ["broca", "thalamus-harness", "thalamus-pi"]
        .into_iter()
        .map(|module| {
            let mut runner = runner.clone();
            runner.module = module.into();
            runner.route_keys = catalog
                .models
                .iter()
                .map(|model| model.route_key())
                .collect();
            runner
        })
        .collect();
    install_catalog(harness, &catalog);
    if client == "ckdesk" {
        super::conformance_tests::install_head_snapshot(harness);
    }
}

#[test]
fn replay_budget_limits() {
    use prefrontal_routing_core::package_ceiling::{route_output_limit, seat_package_ceiling};
    let rows: Vec<_> = [
        ("ordinary", Some(200_000), Some(32_000), 40_000),
        ("zero-output", Some(200_000), Some(0), 40_000),
        ("placeholder-output", Some(200_000), Some(200_000), 40_000),
        ("missing-output", Some(200_000), None, 40_000),
        ("missing-context", None, Some(32_000), 40_000),
        ("prompt-fills-window", Some(200_000), Some(32_000), 200_000),
    ]
    .into_iter()
    .map(|(name, context, output, prompt)| {
        let limit = route_output_limit(output, context, prompt);
        json!({"name": name, "context_window": context, "max_output_tokens": output,
            "prompt_tokens": prompt, "package": seat_package_ceiling(name, context, output),
            "output_limit": format!("{limit:?}"), "output_tokens": limit.tokens()})
    })
    .collect();
    assert_or_record(
        "budgets",
        json!({"source":"prefrontal-routing-core::package_ceiling::{seat_package_ceiling,route_output_limit}", "rows": rows}),
    );
}

#[tokio::test]
async fn replay_availability_cases() {
    let mut cases = Vec::new();
    for scenario in [
        "available",
        "route-cooldown",
        "quota-wall",
        "provider-wall",
        "credential-fence",
        "no-eligible-account",
        "all-accounts-held",
        "capacity-exhausted",
        "unsupported-pin",
        "unsupported-runner",
        "unknown-reset",
        "all-routes-timed-unavailable",
        "all-routes-untimed-unavailable",
    ] {
        let _clock = crate::InjectedClock::set(NOW as u64);
        let _inputs = prefrontal_routing_store::replay::Inputs::new(NOW).install();
        let (harness, route) = configured().await;
        let key = "example-subscription/model-c";
        let affinity_request = json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},"affinity":"existing-live-affinity"});
        let (_, affinity) = route.call("route.select", &affinity_request).await;
        let mut setup = Value::Null;
        if scenario == "all-routes-timed-unavailable" {
            let mut updates = Vec::new();
            for model in fixture::catalog().models {
                let params = json!({"routeKey":model.route_key(),"capacityKey":model.route_key(),"cooldownUntil":NOW+600_000});
                let (_, reply) = route.call("route.record_cooldown", &params).await;
                updates.push(json!({"params":params,"reply":reply}));
            }
            setup = json!(updates);
        }
        if ["route-cooldown", "quota-wall", "provider-wall"].contains(&scenario) {
            let params = json!({"routeKey":key, "capacityKey":key,
                "cooldownUntil": if scenario == "route-cooldown" {json!(NOW+600_000)} else {Value::Null},
                "quotaExhaustedUntil": if scenario != "route-cooldown" {json!(NOW+600_000)} else {Value::Null},
                "providerWide":scenario == "provider-wall", "wallClass":"quota"});
            let (_, reply) = route.call("route.record_cooldown", &params).await;
            setup = json!({"params":params, "reply":reply});
        }
        if scenario == "credential-fence" {
            for account in fixture::catalog().accounts {
                harness
                    .store()
                    .record_account_route_fence(&prefrontal_routing_store::AccountRouteFence {
                        provider: "example-subscription".into(),
                        model: "model-c".into(),
                        credential_id: account.credential_id,
                        until_ms: NOW + 600_000,
                        decision_id: "prior-decision".into(),
                        recorded_at: NOW,
                    })
                    .unwrap();
            }
        }
        if ["no-eligible-account", "all-accounts-held", "unknown-reset"].contains(&scenario) {
            let mut quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
            let mut readings = Vec::new();
            for reading in quota.readings.readings.values_mut() {
                reading.received_at_ms = NOW;
                reading.entry["fetchedAt"] = json!(crate::account_selection::rfc3339(NOW));
                for name in ["primary", "secondary"] {
                    if let Some(window) = reading.entry["usage"][name].as_object_mut() {
                        window.insert("usedPercent".into(), json!(100.0));
                        if scenario == "unknown-reset" {
                            window.remove("resetsAt");
                        } else {
                            let reset = NOW
                                + if scenario == "all-accounts-held" {
                                    5_000
                                } else {
                                    600_000
                                };
                            window.insert(
                                "resetsAt".into(),
                                json!(crate::account_selection::rfc3339(reset)),
                            );
                        }
                    }
                }
                readings.push(reading.entry.clone());
            }
            setup = json!({"quota_readings": readings});
        }
        let status_params = json!({"routeKey":key, "capacityKey":key, "now":NOW});
        let (_, status) = route.call("route.cooldown_status", &status_params).await;
        let mut launches = Vec::new();
        for kind in ["fallback", "no-fallback", "probe"] {
            let mut request =
                json!({"consumer":fixture::SUMMARY, "requirements":{"iq":50}, "pin":key});
            if kind == "fallback" {
                request["pin_fallback"] = json!("route");
            }
            if kind == "probe" {
                request["hold_ok"] = json!(false);
                request["hold_wait_max_s"] = json!(0);
            }
            if scenario == "unsupported-pin" {
                request["pin"] = json!("missing/model");
            }
            if scenario == "unsupported-runner" {
                request["runners"] = json!(["missing-runner"]);
            }
            if scenario == "all-routes-untimed-unavailable" {
                request["exclude"] = json!(fixture::catalog()
                    .models
                    .iter()
                    .map(|model| model.route_key())
                    .collect::<Vec<_>>());
            }
            if scenario == "capacity-exhausted" && kind != "probe" {
                request["concurrency_slots"] =
                    json!([{"key":key, "running":1, "limit":1, "queued":1}]);
            }
            let (ty, reply) = route.call("route.select", &request).await;
            let after_select = persistent_effects(&harness);
            let settled = if kind == "probe" {
                if let Some(id) = reply["result"]["decision_id"].as_str() {
                    let params = json!({"decision_id":id, "outcome":"cancelled"});
                    let (_, report) = route.call("route.report", &params).await;
                    json!({"params":params, "reply":report})
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            };
            launches.push(
                json!({"kind":kind, "params":request, "frame_type":format!("{ty:?}"),
                "reply":reply, "after_select":after_select, "settlement":settled,
                "state":persistent_effects(&harness)}),
            );
        }
        cases.push(normalize_paths(
            json!({"name":scenario, "setup":setup, "prior_affinity":{"params":affinity_request,"reply":affinity},
            "cooldown_status":{"params":status_params, "reply":status}, "launches":launches}),
            &harness,
        ));
    }
    assert_or_record(
        "availability-public",
        json!({"clock_ms":NOW, "body_source":"public routing/v1 route.select replies through the real frame handler; availability table scenarios",
            "cases":cases.iter().map(|case| json!({"name":case["name"], "launches":case["launches"].as_array().unwrap().iter().map(|launch|
                json!({"kind":launch["kind"],"params":launch["params"],"frame_type":launch["frame_type"],"reply":launch["reply"]})
            ).collect::<Vec<_>>() })).collect::<Vec<_>>() }),
    );
    if std::env::var_os("PREFRONTAL_RECORD_ROUTING_PUBLIC_AVAILABILITY").is_some() {
        return;
    }
    assert_or_record(
        "availability",
        json!({"clock_ms":NOW,
        "body_source":"routing_v1/select_options_tests.rs pin/fallback bodies; core::route_is_cooled; explicit policy probes for each availability-table cause",
        "cases":cases}),
    );
}
