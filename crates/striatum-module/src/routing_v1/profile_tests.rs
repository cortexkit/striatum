use std::collections::HashMap;

use serde_json::json;
use striatum_core::task_profile::{ProfileLevel, TaskProfile, TokenVector};
use striatum_core::{fact_to_route, ModelFact};
use subc_protocol::FrameType;

use super::dispatch_tests::configured;

pub(super) fn task_pricing(
    harness: &super::conformance_tests::Harness,
    decision: &str,
) -> serde_json::Value {
    let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
    let score: String = connection
        .query_row(
            "SELECT score_json FROM model_route_decisions WHERE id = ?1",
            [decision],
            |row| row.get(0),
        )
        .unwrap();
    let score: serde_json::Value = serde_json::from_str(&score).unwrap();
    score["objective"]["taskPricing"].clone()
}

pub(super) fn lookup(
    workload: &'static str,
    iq: f64,
    outputs: &[(&str, f64)],
) -> impl Fn(&str, f64, &[ModelFact]) -> crate::select::TaskProfileLookup + Sync {
    let profiles: HashMap<_, _> = outputs
        .iter()
        .map(|(key, output)| {
            (
                (*key).to_owned(),
                TaskProfile {
                    tokens: TokenVector {
                        output_excluding_reasoning: *output,
                        ..Default::default()
                    },
                    level: ProfileLevel::OwnCell,
                    task_count: 20,
                    own_task_count: 20,
                    own_weight: 1.0,
                    keeps_tokenizer: true,
                },
            )
        })
        .collect();
    move |actual_workload, actual_iq, facts| {
        assert_eq!(actual_workload, workload);
        assert_eq!(actual_iq, iq);
        crate::select::TaskProfileLookup {
            profiles: facts
                .iter()
                .filter_map(|fact| {
                    let key = fact_to_route(fact).route_key;
                    profiles.get(&key).map(|profile| (key, profile.clone()))
                })
                .collect(),
            providers_without_cache_write_reports: Default::default(),
        }
    }
}

#[tokio::test]
async fn select_does_not_read_task_profiles_from_the_store() {
    let (harness, route) = configured().await;
    let connection = rusqlite::Connection::open(harness.store_path()).unwrap();
    // Only the task aggregation query requires this index. Ordinary routing
    // fact, speed and cold-start class-vector reads still work without it.
    connection
        .execute_batch("DROP INDEX model_route_usage_task_idx")
        .unwrap();
    assert!(harness
        .store()
        .closed_task_usage(super::now_ms() as i64)
        .is_err());
    let (ty, body) = route
        .call(
            "route.select",
            &json!({
                "consumer": cortexkit_role_routing_conformance::fixture::SUMMARY,
                "requirements": {"iq": 50},
            }),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{body}");
}
