//! Media reports retain account-scoped effects without re-admitting a removed consumer.

use super::*;
use rusqlite::types::Value as SqlValue;

fn health(h: &Harness) -> Vec<(String, Vec<Vec<SqlValue>>)> {
    let conn = rusqlite::Connection::open(h.store_path()).unwrap();
    [
        ("model_route_state", "SELECT * FROM model_route_state ORDER BY rowid"),
        ("account_route_source", "SELECT provider,credential_id,model,deadline,
            json_extract(source_metadata_json,'$.decision_id'),recorded_at,last_reported_at,wall_class
            FROM block WHERE kind='account_route' AND source_present=1 ORDER BY source_order,origin_key"),
        ("non_quota_route_sources", "SELECT provider,credential_id,model,deadline,
            json_extract(source_metadata_json,'$.decision_id'),recorded_at,last_reported_at,wall_class
            FROM block WHERE kind IN ('non_quota_route','transient_route') AND source_present=1 ORDER BY source_order,origin_key"),
        ("credential_source", "SELECT provider,credential_id,code,listing_fingerprint,observed_at
            FROM block WHERE kind='credential' AND source_present=1 ORDER BY source_order,origin_key"),
    ]
    .into_iter()
    .map(|(label, sql)| {
        let mut query = conn.prepare(sql).unwrap();
        let columns = query.column_count();
        let rows = query.query_map([], |row| {
            (0..columns).map(|column| row.get(column)).collect()
        }).unwrap().collect::<Result<_, _>>().unwrap();
        (label.into(), rows)
    })
    .collect()
}

async fn media_report(status: &str, outcome: &str) -> (std::sync::Arc<Harness>, Value, Value) {
    let h = setup(vec![engine("remote", false)]);
    h.install_facts(&[crate::tests::fact("media-vendor", "remote")]);
    h.set_credentials(media_credentials(vec![key("opaque-media")]));
    let route = h.bind(reserved("colliculus"), None).await;
    let answer = select(&route, &request()).await;
    let id = answer["decision_id"].as_str().unwrap();
    let conn = rusqlite::Connection::open(h.store_path()).unwrap();
    // Nonzero counters make accidental failure charging or healing visible.
    conn.execute(
        "UPDATE model_route_state SET consecutive_terminal_failures=2,failure_count=3",
        [],
    )
    .unwrap();
    let reset = crate::now_ms() + 60_000;
    if status == "rejected" {
        // Existing walls must survive rejection as well as counters; an empty
        // account would not expose accidental healing by a neutral report.
        h.store()
            .record_credential_entitlement_fence(
                &striatum_core::route_state_store::CredentialRefusal {
                    provider_id: "media-vendor".into(),
                    credential_id: "opaque-media".into(),
                    code: "credential_refused".into(),
                },
                None,
                crate::now_ms() as i64,
            )
            .unwrap();
        h.store()
            .record_account_route_fence(&striatum_store::AccountRouteFence {
                provider: "media-vendor".into(),
                credential_id: "opaque-media".into(),
                model: "remote".into(),
                until_ms: reset as i64,
                decision_id: id.into(),
                recorded_at: crate::now_ms() as i64,
            })
            .unwrap();
    }
    let before = health(&h);
    let body = json!({"decision_id":id,"outcome":outcome,"credential_id":"opaque-media", "reset_at_ms":reset,
        "runner_code":"account_limit_exhausted", "attempt_outcome":{"status":status}});
    // Engine reports are still valid after their consumer declaration disappears.
    h.edit_config(|config| config["consumers"] = json!({}));
    super::super::super::consumers::replace_catalog(&json!([]));
    let reply = route.call("route.report", &body).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["result"]["outcome"], outcome);
    let ledger: String = conn
        .query_row(
            "SELECT outcome FROM model_route_decisions WHERE id=?1",
            [id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        ledger,
        if outcome == "cancelled" {
            "cancelled"
        } else {
            "error"
        }
    );
    if status == "rejected" {
        assert_eq!(health(&h), before);
    } else {
        // Reports record outcomes, not model-health evidence; only the serving account is fenced.
        assert_eq!(health(&h)[0], before[0]);
    }
    let after = health(&h);
    assert_eq!(route.call("route.report", &body).await, reply);
    assert_eq!(health(&h), after);
    (h, answer, json!(reset))
}

#[tokio::test]
async fn media_outcomes_attempt_fences_the_serving_account_and_records_outcome() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let (h, answer, reset) = media_report("quota", "failed").await;
            let fences = h
                .store()
                .account_route_fences(crate::now_ms() as i64)
                .unwrap();
            assert_eq!(fences.len(), 1);
            assert_eq!(fences[0].provider, "media-vendor");
            assert_eq!(fences[0].model, "remote");
            assert_eq!(fences[0].credential_id, "opaque-media");
            assert_eq!(fences[0].decision_id, answer["decision_id"]);
            assert_eq!(json!(fences[0].until_ms), reset);
        },
    )
    .await;
}

#[tokio::test]
async fn media_outcomes_rejected_preserves_fences_and_route_health() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let (h, _, _) = media_report("rejected", "cancelled").await;
            assert_eq!(
                h.store()
                    .account_route_fences(crate::now_ms() as i64)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(h.store().credential_entitlement_fences().unwrap().len(), 1);
        },
    )
    .await;
}

#[tokio::test]
async fn media_outcomes_llm_attempt_reports_still_readmit_the_consumer() {
    use cortexkit_role_routing_conformance::fixture;
    let (h, route) = super::super::super::dispatch_tests::configured().await;
    let answer = select(
        &route,
        &json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},
        "pin":"example-subscription/model-c"}),
    )
    .await;
    h.edit_config(|config| config["consumers"][fixture::SUMMARY]["owner"] = json!("other"));
    let before = health(&h);
    let reply = route
        .call(
            "route.report",
            &json!({"decision_id":answer["decision_id"],"outcome":"completed",
        "attempt_outcome":{"status":"served"}}),
        )
        .await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "consumer_not_owned");
    assert_eq!(health(&h), before);
    assert!(h
        .store()
        .v1_report(answer["decision_id"].as_str().unwrap())
        .unwrap()
        .is_none());
}
