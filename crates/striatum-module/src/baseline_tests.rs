//! Validate the frozen routing baseline and its associated configuration.

use serde_json::Value;
use sha2::{Digest, Sha256};

const CONFIG: &str = include_str!("../fixtures/routing-config-live-2026-10-24.jsonc");
const BASELINE: &str = include_str!("../fixtures/routing-baseline-2026-10-24.json");

fn baseline() -> Value {
    serde_json::from_str(BASELINE).expect("the baseline fixture is JSON")
}

/// The checked-in config is byte-for-byte the copy the owner supplied
/// (238 lines, sha256 9447e7e8b5803b1b...), and the baseline names it.
#[test]
fn baseline_config_is_the_owner_supplied_live_copy() {
    let baseline = baseline();
    let digest = Sha256::digest(CONFIG.as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(hex, baseline["config"]["sha256"].as_str().unwrap());
    assert!(hex.starts_with("9447e7e8b5803b1b"));
    assert_eq!(CONFIG.lines().count(), 238);
    assert_eq!(
        baseline["config"]["file"],
        "routing-config-live-2026-10-24.jsonc"
    );
}

/// The baseline covers one request per configured slot pin, and every
/// request has a checked-in answer (and no answer lacks a request).
#[test]
fn baseline_lists_each_slot_pin_and_an_answer_per_request() {
    let baseline = baseline();
    let requests = baseline["requests"].as_array().unwrap();
    let names: Vec<&str> = requests
        .iter()
        .map(|request| request["name"].as_str().unwrap())
        .collect();
    let slot = |slot: &str| {
        requests.iter().any(|request| {
            request["params"]["pin"]["slot"] == slot
                || (slot == "synthesizer" && request["op"] == "synthesizer")
        })
    };
    for pinned in ["synthesizer", "lab_planner", "spec_designer"] {
        assert!(slot(pinned), "no request for the {pinned} slot pin");
    }
    let expected = baseline["expected"].as_object().unwrap();
    let mut answered: Vec<&str> = expected.keys().map(String::as_str).collect();
    let mut asked = names.clone();
    answered.sort_unstable();
    asked.sort_unstable();
    assert_eq!(answered, asked);
    for row in baseline["credential_listing"]["result"]["credentials"]
        .as_array()
        .unwrap()
    {
        assert!(
            row["auth_method"].is_string(),
            "vault row {} carries auth_method",
            row["id"]
        );
    }
}
