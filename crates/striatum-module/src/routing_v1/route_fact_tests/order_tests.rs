use super::*;

fn permutations(items: &mut [usize], at: usize, out: &mut Vec<Vec<usize>>) {
    if at == items.len() {
        out.push(items.to_vec());
        return;
    }
    for next in at..items.len() {
        items.swap(at, next);
        permutations(items, at + 1, out);
        items.swap(at, next);
    }
}

async fn end_state(order: &[usize], legacy: bool) -> Value {
    let _ids = striatum_store::replay::Inputs::new(NOW as i64).install();
    let (harness, _) = super::super::account_classes_tests::setup("synthetic", false);
    harness.edit_config(|config| config["runners"] = json!([{"module":"broca","via_vault":true}]));
    let runner = harness.bind(reserved("broca"), None).await;
    let owner = harness.bind(reserved("class-owner"), None).await;
    let selected = result(
        owner
            .call(
                "route.select",
                &json!({"consumer":"mason", "pin":"synthetic/model", "requirements":{"iq":0}}),
            )
            .await,
    );
    let id = &selected["decision_id"];
    let admitted = result(
        runner
            .call(
                "route.account_for",
                &json!({"provider":"synthetic", "model":"model", "decision_id":id,
        "conversation_key":"conversation", "admission_id":"admission"}),
            )
            .await,
    );
    let account = &admitted["account"]["account_id"];
    let credential = &admitted["account"]["credential_id"];
    let mut observations = vec![
        json!({"event_id":"start", "kind":"admission_started", "admission_id":"admission", "run_id":"run"}),
        json!({"event_id":"attempt", "kind":"attempt", "admission_id":"admission", "run_id":"run", "served_account_id":account, "credential_id":credential,
            "attempt_outcome":{"status":"served"}}),
        json!({"event_id":"limit", "kind":"account_limit_exhausted", "admission_id":"admission", "limit_window":"credit", "observed_at_ms":NOW, "retry_at_ms":NOW+60_000}),
        json!({"event_id":"refusal", "kind":"credential_refused", "admission_id":"admission", "credential_id":credential, "provider_code":"credit_exhausted"}),
        finish(false)["params"].clone(),
    ];
    if legacy {
        for observation in &mut observations {
            observation["caller"] = json!({"runner":"broca"});
        }
        // The legacy twin records attempt evidence, but omits routing/v1-only
        // members: attempt_outcome, retry_at_ms and provider_code.
        observations[1]
            .as_object_mut()
            .unwrap()
            .remove("attempt_outcome");
        observations[2]
            .as_object_mut()
            .unwrap()
            .remove("retry_at_ms");
        observations[2]["resets_at_ms"] = json!(NOW + 60_000);
        observations[2]["resets_at_source"] = json!("retry_after");
        observations[3]
            .as_object_mut()
            .unwrap()
            .remove("provider_code");
    }
    for index in order {
        if *index == 5 {
            result(owner.call("route.report", &json!({"decision_id":id, "outcome":"completed", "usage":{"run_id":"run", "output_tokens":20}})).await);
        } else {
            result(
                runner
                    .call("route.account_observe", &observations[*index])
                    .await,
            );
        }
    }
    let state = crate::account_state::snapshot(harness.store()).unwrap();
    let admission = harness
        .store()
        .account_read(|tx| tx.admission("reserved:broca", "admission"))
        .unwrap()
        .unwrap();
    let affinity = harness
        .store()
        .account_read(|tx| tx.affinity("reserved:broca", &admission.conversation_hash, "synthetic"))
        .unwrap()
        .unwrap();
    let mut blocks: Vec<_> = state.blocks.blocks(NOW as i64).unwrap().blocks.into_iter().map(|block| {
        json!({"kind":block.kind, "scope":format!("{:?}", block.scope), "cause":format!("{:?}", block.cause),
            "evidence_at":block.evidence_at, "next_recheck_at":block.next_recheck_at, "cleared_models":block.cleared_models})
    }).collect();
    blocks.sort_by_key(Value::to_string);
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let report: (String, String, i64) = conn
        .query_row(
            "SELECT delivery_key,body_json,created_at_ms FROM routing_v1_report_delivery",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    let usage: (i64, i64) = conn
        .query_row(
            "SELECT count(*),sum(output_tokens) FROM model_route_usage",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    json!({"admission":{"state":admission.state, "account":admission.account_id, "credential":admission.credential_id,
        "charged":admission.charged, "run":admission.run_id, "started_at":admission.started_at, "finished_at":admission.finished_at, "last_observed_at":admission.last_observed_at},
        "lease":affinity.lease_valid_until, "open":state.admissions.open_admission("reserved:broca", "admission").is_some(),
        "inflight":state.inflight.count("synthetic", account.as_str().unwrap(), crate::account_state::InflightScope::Account, NOW as i64),
        "blocks":blocks, "report":report, "usage":usage})
}

#[tokio::test]
async fn every_admission_fact_order_has_the_in_order_end_state() {
    let _clock = crate::InjectedClock::set(NOW);
    let mut orders = Vec::new();
    permutations(&mut [0, 1, 2, 3, 4, 5], 0, &mut orders);
    assert_eq!(orders.len(), 720);
    for legacy in [false, true] {
        let expected = end_state(&[0, 1, 2, 3, 4, 5], legacy).await;
        assert_eq!(expected["admission"]["state"], "finished");
        assert_eq!(expected["inflight"], 0);
        assert_eq!(expected["usage"], json!([1, 20]));
        assert!(!expected["blocks"].as_array().unwrap().is_empty());
        for order in &orders {
            assert_eq!(
                end_state(order, legacy).await,
                expected,
                "legacy={legacy} order={order:?}"
            );
        }
    }
}
