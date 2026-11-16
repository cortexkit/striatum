//! Routing owns the state assertions for the request bodies core records by path.
use super::conformance_tests::{Harness, Route};
use serde_json::{json, Value};
use std::path::Path;
use striatum_core::{DecisionInput, FactSource, RouteOutcome, RouteStateStore};
use striatum_store::V1Decision;

fn cases(kind: &str) -> Vec<Value> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/routing-wire/states.json");
    let corpus: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let cases: Vec<_> = corpus["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["kind"] == kind)
        .cloned()
        .collect();
    assert!(!cases.is_empty(), "missing {kind} fixture bodies");
    cases
}
async fn core(label: &str) -> (std::sync::Arc<Harness>, Route) {
    let harness = Harness::new(label);
    harness.edit_config(|config| {
        config["consumers"]["mason"] = json!({"owner":"core"});
    });
    let route = harness
        .bind(json!({"kind":"reserved","module_id":"core"}), None)
        .await;
    (harness, route)
}
fn report_snapshot(path: &Path) -> Value {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut tables = serde_json::Map::new();
    for (name,query) in [
        ("route","SELECT route_key,cooldown_until,quota_exhausted_until,failure_count,quota_error_count,consecutive_terminal_failures,quota_pressure,wall_class FROM model_route_state ORDER BY route_key"),
        ("affinity","SELECT principal,consumer,affinity,decision_id,ended_at_ms FROM routing_v1_affinity ORDER BY affinity"),
        ("route_fence","SELECT * FROM account_route_fence"),
        ("credential_fence","SELECT * FROM credential_entitlement_fences"),
        ("pool_fence","SELECT * FROM account_fence"),
    ] {
        let mut statement=conn.prepare(query).unwrap();let columns=statement.column_count();
        let rows=statement.query_map([],|row|{let mut values=Vec::new();for index in 0..columns{values.push(match row.get_ref(index)?{
            rusqlite::types::ValueRef::Null=>Value::Null,rusqlite::types::ValueRef::Integer(v)=>json!(v),rusqlite::types::ValueRef::Real(v)=>json!(v),rusqlite::types::ValueRef::Text(v)=>json!(std::str::from_utf8(v).unwrap()),rusqlite::types::ValueRef::Blob(_)=>panic!("unexpected blob"),
        });}Ok(Value::Array(values))}).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();tables.insert(name.into(),json!(rows));
    }
    Value::Object(tables)
}

#[tokio::test]
async fn every_core_outcome_keeps_the_recorded_cooldown_penalty_affinity_and_fences() {
    for case in cases("report_outcome") {
        let _clock = crate::InjectedClock::set(2_000);
        let (harness, route) = core("core-outcome-state").await;
        harness
            .store()
            .update_route_state("p/m", "p/m", &Default::default())
            .unwrap();
        harness
            .store()
            .record_decision_with_selection(
                &DecisionInput {
                    decision_id: Some("D".into()),
                    decision_kind: "single".into(),
                    consumer: Some("mason".into()),
                    target_agent: "mason".into(),
                    requirements_json: "{}".into(),
                    score_json: "{}".into(),
                    rejected_json: "[]".into(),
                    selected_route_key: Some("p/m".into()),
                    selected_provider_id: Some("p".into()),
                    selected_model_id: Some("m".into()),
                    ..Default::default()
                },
                Some("broca"),
                Some("mason"),
            )
            .unwrap();
        harness
            .store()
            .v1_record_decision(&V1Decision {
                decision_id: "D".into(),
                principal: "reserved:core".into(),
                consumer: "mason".into(),
                allowed_account_class: Default::default(),
                project_id: None,
                provider: "p".into(),
                model: "m".into(),
                variant: None,
                runner_module: "broca".into(),
                affinity: Some("live-affinity".into()),
                created_at_ms: 1,
                expires_at_ms: 10_000,
            })
            .unwrap();
        let (ty, reply) = route.call("route.report", &case["input"]["body"]).await;
        assert_eq!(ty, subc_protocol::FrameType::Response, "{reply}");
        assert_eq!(
            report_snapshot(&harness.store_path()),
            case["expected"],
            "{}",
            case["input"]["outcome"]
        );
        assert!(harness
            .store()
            .v1_affinity_decision("reserved:core", "mason", "live-affinity")
            .unwrap()
            .is_some());
    }
}

async fn billing_wall_state(kind: &str) {
    use striatum_core::model_facts::{ModelFact, ModelRequirements};
    use striatum_core::model_router::{select_model_route, SelectModelRouteInput};
    for case in cases(kind) {
        let reset = case["input"]["requests"][0]["params"]["cooldownUntil"].as_f64();
        let _clock = crate::InjectedClock::set(reset.map_or(2_000, |reset| reset as u64 - 60_000));
        let (harness, route) = core("core-billing-wall-state").await;
        let provider = case["input"]["provider"].as_str().unwrap();
        let model = case["input"]["model"].as_str().unwrap();
        for (provider, model, cost) in [
            (provider, model, 1.0),
            ("anthropic", "claude-opus-4-8", 9.0),
        ] {
            let fact:ModelFact=serde_json::from_value(json!({"providerID":provider,"modelID":model,"scores":{"elo":80.0,"eq":1700.0,"speed":90.0},"contextWindow":200000.0,"costTier":2.0,"costPerMTokOut":cost,"supportsVision":true,"supportsToolUse":true,"confidence":"curated"})).unwrap();
            harness
                .store()
                .upsert_fact(&fact, FactSource::Override)
                .unwrap();
        }
        let select = || {
            select_model_route(SelectModelRouteInput {
                target_agent: "mason".into(),
                target_is_head: false,
                requirements: ModelRequirements { iq: 30.0, eq: 0.0 },
                available_models: [
                    format!("{provider}/{model}"),
                    "anthropic/claude-opus-4-8".into(),
                ]
                .into_iter()
                .collect(),
                facts: harness.store().get_facts().unwrap(),
                runtime_snapshot: Some(
                    harness
                        .store()
                        .build_snapshot(crate::now_ms() as f64)
                        .unwrap(),
                ),
                exclude_route_keys: Vec::new(),
                operator_exclude_keys: Vec::new(),
                required_capabilities: None,
                is_subscription_route: None,
                min_best_effort_adequacy: None,
                prefer_prompt_cache: false,
                providers_without_prompt_cache: Default::default(),
                providers_without_cache_write_reports: Default::default(),
            })
            .unwrap()
            .route_key
        };
        assert_eq!(json!(select()), case["expected"]["before"]);
        for request in case["input"]["requests"].as_array().unwrap() {
            let (ty, reply) = route
                .call(request["method"].as_str().unwrap(), &request["params"])
                .await;
            assert_eq!(ty, subc_protocol::FrameType::Response, "{reply}");
        }
        assert_eq!(json!(select()), case["expected"]["after"]);
    }
}
#[tokio::test]
async fn a_dry_route_selection_after_the_billing_wall_does_not_pick_the_walled_provider() {
    billing_wall_state("billing_wall").await;
}
#[tokio::test]
async fn a_billing_wall_naming_one_credential_leaves_the_provider_selectable() {
    billing_wall_state("named_billing_wall").await;
}

fn backoff(kind: &str) {
    use striatum_core::{failure_backoff_transition, FailureBackoffConfig};
    for case in cases(kind) {
        let body = &case["input"]["requests"][0]["params"];
        let outcome = crate::parse_outcome(
            body["outcome"].as_str().unwrap(),
            body["retryAfterSecs"].as_u64(),
        )
        .unwrap();
        let actual = failure_backoff_transition(
            2,
            outcome,
            body["lastError"].as_str(),
            1000,
            FailureBackoffConfig::default(),
        );
        assert_eq!(json!(format!("{actual:?}")), case["expected"]);
    }
}
#[test]
fn structured_output_and_aggregator_contract_failures_do_not_charge_route_health() {
    backoff("contract_backoff");
    backoff("provider_backoff");
}
#[test]
fn broca_request_invalid_worker_poll_and_stream_end_without_charging_the_route() {
    backoff("request_invalid_backoff");
}
#[test]
fn broca_account_limits_keep_route_neutral_and_named_accounts_try_fresh_admission() {
    use striatum_core::{
        failure_backoff_transition, FailureBackoffConfig, FailureBackoffTransition,
    };
    for case in cases("account_limit_backoff") {
        let body = &case["input"]["requests"][0]["params"];
        let outcome = crate::parse_outcome(body["outcome"].as_str().unwrap(), None).unwrap();
        let mut streak = 0;
        let mut backoff = false;
        for _ in 0..3 {
            if let FailureBackoffTransition::Failure {
                consecutive_failures,
                cooldown_until,
                ..
            } = failure_backoff_transition(
                streak,
                outcome,
                body["lastError"].as_str(),
                1000,
                FailureBackoffConfig::default(),
            ) {
                streak = consecutive_failures;
                backoff |= cooldown_until.is_some();
            }
        }
        assert_eq!(json!(backoff), case["expected"]);
        assert_eq!(outcome, RouteOutcome::Interrupted);
    }
}

#[tokio::test]
async fn broca_generation_multi_entry_keeps_two_speed_samples_without_a_token_total() {
    let (harness, route) = core("core-generation-speed-samples").await;
    for case in cases("speed_usage") {
        let (ty, reply) = route.call("route.record_usage", &case["input"]).await;
        assert_eq!(ty, subc_protocol::FrameType::Response, "{reply}");
        assert_eq!(reply["result"]["recorded"], case["expected"]["recorded"]);
        assert_eq!(
            reply["result"]["usage_recorded"],
            case["expected"]["usage_recorded"]
        );
    }
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM model_route_usage", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    let mut stmt=conn.prepare("SELECT id,route_key,service_tier,speed,output_tokens,reasoning_tokens,duration_ms FROM model_route_speed_samples ORDER BY id").unwrap();
    let samples = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        samples,
        vec![
            (
                "run-1:gen:0".into(),
                "fake/fake".into(),
                "fast".into(),
                "fast".into(),
                30,
                10,
                100
            ),
            (
                "run-1:gen:1".into(),
                "other/second".into(),
                "fast".into(),
                "standard".into(),
                60,
                20,
                200
            )
        ]
    );
}

async fn no_token_relay_state(label: &str, kind: &str) {
    let records = cases(kind);
    assert!(records
        .iter()
        .all(|case| case["input"]["requests"].as_array().unwrap().is_empty()));
    let (harness, _) = core(label).await;
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM model_route_usage", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert!(harness
        .store()
        .get_class_vector("ephemeral", crate::now_ms() as f64 + 1000.0)
        .unwrap()
        .is_none());
}
#[tokio::test]
async fn a_mason_broca_run_keeps_usage_without_sending_a_token_relay_row() {
    no_token_relay_state("core-no-token-relay", "no_usage_mason").await;
}
#[tokio::test]
async fn redelivery_and_a_second_legacy_run_never_restore_the_deleted_token_relay() {
    no_token_relay_state("core-no-second-token-relay", "no_usage_redelivery").await;
}

fn observation_fixture(test: &str) {
    use striatum_core::usage_facts::{
        build_model_speed_observation, build_speed_sample_observation, build_usage_observation,
    };
    let mut compared = 0;
    for kind in [
        "usage_observation",
        "model_speed_observation",
        "speed_observation",
    ] {
        for case in cases(kind).into_iter().filter(|case| {
            case["input"]["test"]
                .as_str()
                .is_some_and(|name| name.ends_with(&format!("::{test}")))
        }) {
            let facts: striatum_core::NativeUsageFacts =
                serde_json::from_value(case["input"]["facts"].clone()).unwrap();
            let observed = match kind {
                "usage_observation" => format!("{:?}", build_usage_observation(&facts, &[])),
                "model_speed_observation" => {
                    format!("{:?}", build_model_speed_observation(&facts, &[]))
                }
                _ => format!("{:?}", build_speed_sample_observation(&facts, &[])),
            };
            assert_eq!(json!(observed), case["expected"], "{test}");
            compared += 1;
            if test == "broca_service_tier_generation_without_tiers_is_standard" {
                let observation = build_speed_sample_observation(&facts, &[]).unwrap();
                assert_eq!(
                    striatum_core::usage_facts::speed_sample_population(
                        observation.service_tier.as_deref(),
                        observation.speed.as_deref()
                    ),
                    (Some("standard"), Some("standard"))
                );
            }
        }
    }
    assert!(compared > 0, "missing observations for {test}");
}
macro_rules! observation_tests {
    ($($name:ident),+ $(,)?)=>{$(#[test] fn $name(){observation_fixture(stringify!($name));})+};
}
observation_tests! {
    broca_generation_live_openai_entry_without_applied_tier_is_standard,
    broca_generation_routing_usage_does_not_turn_partial_timing_into_speed,
    provider_fallback_routing_usage_keeps_unknown_reasoning,
    a_run_usage_names_the_tasks_model_agent_and_run,
    workload_classes_follow_routings_agent_mapping,
    replay_core_usage_attribution,
    broca_service_tier_usage_routing_and_generation_samples,
    broca_service_tier_generation_without_tiers_is_standard,
    finished_sidekick_run_reports_generation_duration_and_requested_tier,
}
