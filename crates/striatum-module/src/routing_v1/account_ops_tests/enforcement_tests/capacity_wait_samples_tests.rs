//! Send the two recorded Agent Runner asks through routing's frame handler.
//! Both asks name the same conversation. The first arrives while another run
//! holds the only slot; the second uses a fresh admission ID after that run
//! finishes. The fixture bytes are params, not full request frames.

use super::*;
use crate::routing_v1::what_if_tests::rows;

const REQUEST: &[u8] =
    include_bytes!("../../../../fixtures/thalamus-capacity-wait-v1/request.json");
const RE_ASK: &[u8] = include_bytes!("../../../../fixtures/thalamus-capacity-wait-v1/re-ask.json");
const RUNNER: &str = "thalamus-capacity-runner";
const ACCOUNT: &str = "catalog-account-probe-apikey";

async fn recorded_ask(runner: &Route, params: &[u8], phase: &str) -> (FrameType, Value) {
    let (kind, bytes) = runner.call_raw_params("route.account_for", params).await;
    eprintln!("{phase}: {}", std::str::from_utf8(&bytes).unwrap());
    (kind, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn thalamus_capacity_wait_reasks_with_a_fresh_admission_id() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = Harness::new("thalamus-capacity-wait");
    let mut fact = crate::routing_v1::account_classes_tests::account_fact("probe");
    fact.model_id = "probe".into();
    fact.auth_methods = Some(vec!["apikey".into()]);
    harness.install_facts(&[fact]);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":RUNNER,"via_vault":true}]);
        config["concurrency"] =
            json!({"enforcement_enabled":true,"providers":{"probe":{"limit":1,"scope":"account"}}});
    });
    let runner = harness.bind(reserved(RUNNER), None).await;
    let principal = format!("reserved:{RUNNER}");

    let holder = runner
        .call(
            "route.account_for",
            &json!({"provider":"probe","model":"probe","conversation_key":"holder",
                "admission_id":"holder"}),
        )
        .await;
    assert_eq!(response(&holder)["account"]["account_id"], ACCOUNT);
    observe(
        &runner,
        json!({"event_id":"holder-start","kind":"admission_started",
            "admission_id":"holder","run_id":"holder-run"}),
    )
    .await;
    let started = harness
        .store()
        .account_read(|tx| tx.admission(&principal, "holder"))
        .unwrap()
        .unwrap();
    assert_eq!(started.state, "started");
    assert_eq!(started.run_id.as_deref(), Some("holder-run"));
    let holds = |now| {
        crate::account_state::snapshot(harness.store())
            .unwrap()
            .inflight
            .count("probe", ACCOUNT, InflightScope::Account, now)
    };
    assert_eq!(holds(NOW as i64), 1);

    let first: Value = serde_json::from_slice(REQUEST).unwrap();
    let second: Value = serde_json::from_slice(RE_ASK).unwrap();
    let first_id = first["admission_id"].as_str().unwrap();
    let second_id = second["admission_id"].as_str().unwrap();
    assert_ne!(first_id, second_id);
    assert_eq!(first["conversation_key"], second["conversation_key"]);

    let before = rows(&harness);
    let reset = NOW + 30_000;
    let refused = recorded_ask(&runner, REQUEST, "Capacity refusal").await;
    assert_concurrency_full(&refused, reset);
    assert_only_a_refusal_row(&harness, &before, first_id);

    let _after_wait = crate::InjectedClock::set(reset);
    assert_eq!(holds(reset as i64), 1);
    observe(
        &runner,
        json!({"event_id":"holder-finish","kind":"run_finished",
            "admission_id":"holder","run_id":"holder-run"}),
    )
    .await;
    assert_eq!(holds(reset as i64), 0);

    let granted = recorded_ask(&runner, RE_ASK, "Capacity re-ask grant").await;
    let answer = response(&granted);
    assert_eq!(answer["account"]["account_id"], ACCOUNT);
    assert_eq!(answer["admission"]["id"], second_id);
    let replay = |id| harness.store().v1_admission(&principal, id).unwrap();
    let recorded: Value = serde_json::from_str(&replay(second_id).unwrap().answer_json).unwrap();
    assert_eq!(&recorded, answer);
    assert!(replay(first_id).is_none());

    // The grant now holds the slot for this conversation. The original ID
    // can use it, but must receive its own admission, not replay either reply.
    assert_eq!(holds(reset as i64), 1);
    let retried = recorded_ask(&runner, REQUEST, "Original admission ID retried").await;
    let retried = response(&retried);
    assert_eq!(retried["account"]["account_id"], ACCOUNT);
    assert_eq!(retried["admission"]["id"], first_id);
    assert_ne!(
        retried["reason"]["decision_id"],
        answer["reason"]["decision_id"]
    );
    let recorded: Value = serde_json::from_str(&replay(first_id).unwrap().answer_json).unwrap();
    assert_eq!(&recorded, retried);
}
