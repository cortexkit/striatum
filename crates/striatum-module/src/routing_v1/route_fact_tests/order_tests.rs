use super::*;
use crate::routing_v1::conformance_tests::case_shards::{self, CaseLabel, CaseShard};

const TEMPLATE_ENV: &str = "CK_STRIATUM_ADMISSION_ORDER_TEMPLATE";
const REFERENCE_ENV: &str = "CK_STRIATUM_ADMISSION_ORDER_REFERENCE";

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

async fn end_state(
    order: &[usize],
    legacy: bool,
    check_load: bool,
    template: Option<&std::path::Path>,
) -> Value {
    let _ids = striatum_store::replay::Inputs::new(NOW as i64).install();
    let (harness, _) = match template {
        Some(template) => {
            super::super::account_classes_tests::setup_from_template("synthetic", false, template)
        }
        None => super::super::account_classes_tests::setup("synthetic", false),
    };
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
        if check_load {
            crate::account_state::assert_load_equivalent(harness.store(), NOW as i64);
        }
    }
    if check_load {
        let mut repeated_start = observations[0].clone();
        repeated_start["event_id"] = json!("second-start-evidence");
        result(runner.call("route.account_observe", &repeated_start).await);
        crate::account_state::assert_load_equivalent(harness.store(), NOW as i64);
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

fn schema(path: &std::path::Path) -> (i64, Vec<(String, String, String, Option<String>)>) {
    let conn = rusqlite::Connection::open(path).unwrap();
    let version = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    let mut stmt = conn
        .prepare("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
        .unwrap();
    let objects = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    (version, objects)
}

async fn assert_every_order(legacy: bool, test: &str) {
    let _clock = crate::InjectedClock::set(NOW);
    if let Some(shard) = CaseShard::current() {
        let template = std::path::PathBuf::from(std::env::var_os(TEMPLATE_ENV).unwrap());
        let expected: Value = serde_json::from_str(&std::env::var(REFERENCE_ENV).unwrap()).unwrap();
        let mut orders = Vec::new();
        permutations(&mut [0, 1, 2, 3, 4, 5], 0, &mut orders);
        assert_eq!(orders.len(), 720);
        let mut cases = 0;
        let mut compared = 0;
        for (index, order) in orders
            .iter()
            .enumerate()
            .filter(|(index, _)| shard.contains(*index))
        {
            let _case = CaseLabel(|| format!("legacy={legacy} permutation {index}: {order:?}"));
            let actual = end_state(order, legacy, false, Some(&template)).await;
            cases += 1;
            assert_eq!(actual, expected, "legacy={legacy} order={order:?}");
            compared += 1;
        }
        shard.report(&[cases, compared]);
        return;
    }
    // Migrate once, then copy a consistent SQLite snapshot instead of rerunning
    // the entire migration chain for each order. Each copy still gets its own
    // setup, selection, admission, caches and six production frame-handler calls.
    let fresh = Harness::new("admission-order-template");
    let template = fresh.store_path().with_file_name("template.db");
    rusqlite::Connection::open(fresh.store_path())
        .unwrap()
        .execute("VACUUM INTO ?1", [template.to_str().unwrap()])
        .unwrap();
    let copy = Harness::from_template("admission-order-schema-check", &template);
    assert_eq!(schema(&copy.store_path()), schema(&fresh.store_path()));
    drop(copy);
    // Keep the reference on the original fresh-store path so it also checks
    // that reusing the migrated schema preserves the end state.
    let expected = end_state(&[0, 1, 2, 3, 4, 5], legacy, false, None).await;
    assert_eq!(expected["admission"]["state"], "finished");
    assert_eq!(expected["inflight"], 0);
    assert_eq!(expected["usage"], json!([1, 20]));
    assert!(!expected["blocks"].as_array().unwrap().is_empty());
    let expected = expected.to_string();
    let [cases, compared] = case_shards::run(module_path!(), test, |command| {
        command
            .env(TEMPLATE_ENV, &template)
            .env(REFERENCE_ENV, &expected);
    });
    assert_eq!(cases, 720);
    assert_eq!(compared, 720);
    println!("legacy={legacy}: compared all {compared} admission fact permutations");
}

#[tokio::test]
async fn every_admission_fact_order_has_the_in_order_end_state() {
    assert_every_order(
        false,
        stringify!(every_admission_fact_order_has_the_in_order_end_state),
    )
    .await;
}

#[tokio::test]
async fn every_legacy_admission_fact_order_has_the_in_order_end_state() {
    assert_every_order(
        true,
        stringify!(every_legacy_admission_fact_order_has_the_in_order_end_state),
    )
    .await;
}

#[tokio::test]
async fn finished_then_started_matches_fresh_account_state_in_both_shapes() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false, true] {
        end_state(&[1, 2, 3, 4, 0, 5], legacy, true, None).await;
    }
}
