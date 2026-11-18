use serde_json::{json, Value};

use super::*;
use crate::fusiform_catalog::{apply_mapped_catalog, decode_catalog_get};

const MODEL: &str = "synthetic-quality-reseller/synthetic-model";
const EXCLUDED: &str = "synthetic-quality-reseller/synthetic-excluded";

fn golden_complete() -> Value {
    let golden: Value = serde_json::from_str(include_str!(
        "../../fixtures/fusiform-workload-efforts.json"
    ))
    .unwrap();
    let mut payload = golden["response"].clone();
    // Fusiform served a prefix-filtered reply. Add the existence and options
    // the full read needs without modifying any of the captured workload keys.
    for row in payload["models"].as_object_mut().unwrap().values_mut() {
        row["existence"] = json!("present");
        row["capability.reasoning_options"] = json!([{"type":"effort","values":["low","high"]}]);
    }
    payload
}

#[test]
fn fusiform_golden_workload_facts_survive_store_reopen() {
    let scratch = cortexkit_test_support::ScratchDir::new("fusiform-workload-effort-facts");
    let open = || {
        striatum_store::SqliteRouteStateStore::open(&striatum_store::dev_descriptor(
            "workload-efforts",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap()
    };
    let store = open();
    let mut payload = golden_complete();
    // Fusiform's served shape (wire contract, capability.reasoning_default).
    payload["models"][MODEL]["capability.reasoning_default"] =
        json!({"type":"effort","value":"low"});
    payload["models"][EXCLUDED]["workload.reason.excluded"] = json!(false);
    let snapshot = decode_catalog_get(serde_json::to_vec(&payload).unwrap()).unwrap();
    apply_mapped_catalog(&store, &snapshot, None, Some("synthetic-quality-reseller")).unwrap();
    drop(store);
    let store = open();
    let all = store.catalog_efforts().unwrap();
    let row = &all[MODEL];
    assert_eq!(row.graded.as_deref(), Some("high"));
    assert_eq!(row.vendor_default.as_deref(), Some("low"));
    assert_eq!(row.elo["low"], 1200.0);
    assert_eq!(row.eq["low"], 1400.0);
    for workload in WORKLOADS {
        assert_eq!(row.workloads[workload].default.as_deref(), Some("high"));
        assert_eq!(row.workloads[workload].minimum.as_deref(), Some("low"));
        assert!(!row.workloads[workload].excluded);
    }
    assert!(all[EXCLUDED].workloads["code"].excluded);
    assert!(!all[EXCLUDED].workloads.contains_key("reason"));

    payload["catalog_version"] = json!(124);
    payload["models"][MODEL] = json!({"existence":"present"});
    let snapshot = decode_catalog_get(serde_json::to_vec(&payload).unwrap()).unwrap();
    apply_mapped_catalog(&store, &snapshot, None, Some("synthetic-quality-reseller")).unwrap();
    assert!(!store.catalog_efforts().unwrap().contains_key(MODEL));
}

#[test]
fn invalid_and_unoffered_effort_keys_are_skipped_without_refusing_the_row() {
    let mut payload = golden_complete();
    let row = &mut payload["models"][MODEL];
    row["capability.reasoning_options"][0]["values"] = json!(["low", "high", "turbo"]);
    row["quality.graded_effort"] = json!("max");
    row["capability.reasoning_default"] = json!({"type":"effort","value":"medium"});
    row["quality.elo.effort.xhigh"] = json!(2000);
    row["quality.eq.effort.typo"] = json!(2000);
    row["workload.code.effort_default"] = json!("xhigh");
    row["workload.code.effort_minimum"] = json!("typo");
    row["workload.quick.effort_default"] = json!(42);
    let decoded = decode_catalog_get(serde_json::to_vec(&payload).unwrap()).unwrap();
    let row = decoded
        .facts
        .iter()
        .find(|fact| fact.model_id == "synthetic-model")
        .unwrap();
    let efforts = row.catalog_efforts.as_ref().unwrap();
    assert!(row.present);
    assert_eq!(
        row.reasoning_efforts,
        Some(vec!["low".into(), "high".into(), "turbo".into()])
    );
    assert_eq!(efforts.offered, Some(vec!["low".into(), "high".into()]));
    assert_eq!(efforts.graded, None);
    assert_eq!(efforts.vendor_default, None);
    assert_eq!(efforts.elo, [("low".into(), 1200.0)].into());
    assert_eq!(efforts.eq, [("low".into(), 1400.0)].into());
    assert!(!efforts.workloads.contains_key("code"));
    assert_eq!(efforts.workloads["quick"].default, None);
}

#[test]
fn vendor_default_reads_only_the_typed_effort_entry() {
    let decode_default = |value: Value| {
        let mut payload = golden_complete();
        payload["models"][MODEL]["capability.reasoning_default"] = value;
        let decoded = decode_catalog_get(serde_json::to_vec(&payload).unwrap()).unwrap();
        decoded
            .facts
            .iter()
            .find(|fact| fact.model_id == "synthetic-model")
            .unwrap()
            .catalog_efforts
            .as_ref()
            .and_then(|efforts| efforts.vendor_default.clone())
    };
    assert_eq!(
        decode_default(json!({"type":"effort","value":"high"})).as_deref(),
        Some("high")
    );
    // A bare string, another entry type or a missing value is not the served
    // shape; each is skipped rather than guessed at.
    assert_eq!(decode_default(json!("high")), None);
    assert_eq!(
        decode_default(json!({"type":"budget","value":"high"})),
        None
    );
    assert_eq!(decode_default(json!({"type":"effort"})), None);
}
