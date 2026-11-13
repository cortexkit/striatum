use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use cortexkit_role_routing_conformance::fixture;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subc_protocol::FrameType;

use super::super::dispatch_tests::configured;

const NOW: u64 = 1_800_000_000_000;
const COMPANIONS: &[(&str, &str)] = &[
    ("served", "completed"),
    ("served_empty", "completed"),
    ("quota", "failed"),
    ("throttled", "failed"),
    ("credential", "failed"),
    ("unavailable", "failed"),
    ("rejected", "cancelled"),
];

async fn capture(status: &str, outcome: &str) -> [(String, Vec<u8>); 2] {
    let _clock = crate::InjectedClock::set(NOW);
    let _ids = striatum_store::replay::Inputs::new(NOW as i64).install();
    // A failed report must not change the next sample's selection or account state.
    let (harness, route) = configured().await;
    harness.edit_config(|config| {
        config["providers"]["example-subscription"]["subscription_multiplier"] = json!(22);
    });
    let (ty, selection) = route
        .call(
            "route.select",
            &json!({"consumer":fixture::SUMMARY,"requirements":{"iq":50},
                "pin":"example-subscription/model-c"}),
        )
        .await;
    assert_eq!(ty, FrameType::Response, "{status}: {selection}");
    assert_eq!(
        selection["result"]["route"]["provider"],
        "example-subscription"
    );
    assert_eq!(selection["result"]["route"]["model"], "model-c");
    let id = selection["result"]["decision_id"].as_str().unwrap();
    let mut params = json!({"decision_id":id,"outcome":outcome,
        "attempt_outcome":{"status":status}});
    if status == "throttled" {
        params["attempt_outcome"]["retry_after_ms"] = json!(30_000);
    }
    let frame = route.request_frame("route.report", &params);
    let request = frame.body.clone();
    let reply = harness.handle(frame).await;
    assert_eq!(reply.header.ty, FrameType::Response, "{status}");
    let body: Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(
        body,
        json!({"result":{"decision_id":id,"report_id":format!("report:{id}"),
            "outcome":outcome}}),
        "{status}"
    );
    // Keep the actual request and response bodies, not re-encoded JSON values.
    [
        (format!("{status}.request.json"), request),
        (format!("{status}.reply.json"), reply.body),
    ]
}

fn verify(samples: BTreeMap<String, Vec<u8>>) {
    let directory =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test-vectors/route-report-attempt-v1");
    let update = std::env::var_os("UPDATE_ROUTE_REPORT_ATTEMPT_VECTORS").is_some();
    let mut expected: BTreeSet<_> = samples.keys().cloned().collect();
    expected.extend(["README.md".into(), "SHA256SUMS".into()]);
    for entry in std::fs::read_dir(&directory).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        assert!(expected.contains(&name), "unlisted sample file: {name}");
    }
    for (name, bytes) in &samples {
        let path = directory.join(name);
        if update {
            std::fs::write(&path, bytes).unwrap();
        }
        let actual = std::fs::read(&path).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(&actual, bytes, "sample byte difference: {name}");
    }
    // The manifest also covers the fixture documentation, but never itself.
    let mut files = samples;
    files.insert(
        "README.md".into(),
        std::fs::read(directory.join("README.md")).unwrap(),
    );
    let sums: String = files
        .iter()
        .map(|(name, bytes)| format!("{:x}  {name}\n", Sha256::digest(bytes)))
        .collect();
    let path = directory.join("SHA256SUMS");
    if update {
        std::fs::write(&path, &sums).unwrap();
    }
    assert_eq!(
        std::fs::read_to_string(&path).expect("SHA256SUMS must exist"),
        sums,
        "SHA256SUMS: byte or digest difference"
    );
}

#[tokio::test]
async fn serving_bytes_match_tagged_vectors() {
    let mut samples = BTreeMap::new();
    for (status, outcome) in COMPANIONS {
        for (name, bytes) in capture(status, outcome).await {
            assert!(samples.insert(name, bytes).is_none());
        }
    }
    verify(samples);
}
