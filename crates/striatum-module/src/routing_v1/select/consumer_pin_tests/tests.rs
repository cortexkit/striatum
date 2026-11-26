//! Raw frame-handler samples: stable clock and IDs, without byte normalization.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use sha2::{Digest, Sha256};

use super::*;

const NOW: u64 = 1_800_000_000_000;

async fn capture(stem: &str) -> [(String, Vec<u8>); 2] {
    let _clock = crate::InjectedClock::set(NOW);
    let _ids = striatum_store::replay::Inputs::new(NOW as i64).install();
    let (harness, route) = configured(CHAIR).await;
    let consumer = if stem == "unpinned" {
        "prefrontal.athena"
    } else {
        CHAIR
    };
    if stem == "pin-unavailable" {
        harness.edit_config(|config| {
            config["consumers"][CHAIR]["pin"]["model"] = json!("anthropic/missing");
        });
    }
    let params = request(consumer);
    let frame = route.request_frame("route.select", &params);
    let request_bytes = frame.body.clone();
    let reply = harness.handle(frame).await;
    let body: Value = serde_json::from_slice(&reply.body).unwrap();
    if stem == "pin-unavailable" {
        assert_eq!(reply.header.ty, FrameType::Error, "{body}");
        let _: cortexkit_role_routing::errors::Refusal =
            serde_json::from_value(body.clone()).unwrap();
        assert_eq!(body["code"], "pin_unavailable", "{body}");
        assert_eq!(body["detail"]["reason"], "unknown_model", "{body}");
        assert_eq!(body["detail"]["pin"], "anthropic/missing:medium", "{body}");
    } else {
        assert_eq!(reply.header.ty, FrameType::Response, "{body}");
        let parsed = parse_select_request_serving(&params, describe::CAPABILITIES).unwrap();
        let typed: SelectAnswer = serde_json::from_value(body["result"].clone()).unwrap();
        check_answer_serving(&parsed, &typed, describe::CAPABILITIES).unwrap();
        if stem == "pinned-chair" {
            assert_chair(&body["result"]);
        } else {
            assert_eq!(
                body["result"]["route"],
                json!({"provider":"anthropic","model":"claude-fable-5-2"}),
                "{body}"
            );
            assert!(body["result"].get("pin_outcome").is_none(), "{body}");
        }
    }
    [
        (format!("{stem}.request.json"), request_bytes),
        (format!("{stem}.reply.json"), reply.body),
    ]
}

#[tokio::test]
async fn consumer_pin_serving_bytes_match_vectors() {
    let mut samples = BTreeMap::new();
    for stem in ["pinned-chair", "unpinned", "pin-unavailable"] {
        for (name, bytes) in capture(stem).await {
            assert!(samples.insert(name, bytes).is_none());
        }
    }
    let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors/route-select-consumer-pin-v1");
    let update = std::env::var_os("UPDATE_ROUTE_SELECT_CONSUMER_PIN_VECTORS").is_some();
    let expected: BTreeSet<_> = samples
        .keys()
        .cloned()
        .chain(["README.md".into(), "SHA256SUMS".into()])
        .collect();
    for entry in std::fs::read_dir(&directory).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        assert!(expected.contains(&name), "unlisted sample file: {name}");
    }
    for (name, bytes) in &samples {
        let path = directory.join(name);
        if update {
            std::fs::write(&path, bytes).unwrap();
        }
        assert_eq!(
            std::fs::read(&path).unwrap(),
            *bytes,
            "sample byte difference: {name}"
        );
    }
    samples.insert(
        "README.md".into(),
        std::fs::read(directory.join("README.md")).unwrap(),
    );
    let sums: String = samples
        .iter()
        .map(|(name, bytes)| format!("{:x}  {name}\n", Sha256::digest(bytes)))
        .collect();
    let path = directory.join("SHA256SUMS");
    if update {
        std::fs::write(&path, &sums).unwrap();
    }
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        sums,
        "SHA256SUMS: byte or digest difference"
    );
}
