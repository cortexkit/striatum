use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use striatum_core::RouteStateStore;
use subc_protocol::FrameType;

use super::account_classes_tests::setup;
use super::conformance_tests::reserved;

const START: u64 = 1_800_000_000_000;
const REFUSED_AT: u64 = START + 48_000;
const RESET: u64 = START + 60_000;

struct Sample {
    request: Vec<u8>,
    reply: Vec<u8>,
    frame_type: FrameType,
    legacy_reply: Option<(FrameType, Value)>,
}

async fn capture(stem: &str) -> Sample {
    let _clock = crate::InjectedClock::set(START);
    let _ids = striatum_store::replay::Inputs::new(START as i64).install();
    let (harness, _) = setup("synthetic", false);
    if stem == "basal-flow-select" {
        // The shared harness seeds a larger catalog; none of it belongs in this
        // publicly pinned Basal sample.
        let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
        conn.execute(
            "DELETE FROM model_facts WHERE route_key != ?1",
            ["synthetic/model"],
        )
        .unwrap();
        let facts = harness.store().get_facts().unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].provider_id, "synthetic");
        assert_eq!(facts[0].model_id, "model");
        super::consumers::replace_catalog(&json!([{
            "module_id":"basal","nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[
                {"name":"basal.flow","placement":{"module":"broca"}}
            ]}}
        }]));
    }
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["subscription"] = json!(true);
        config["concurrency"] =
            json!({"enforcement_enabled":true,"providers":{"synthetic":{"limit":1}}});
        if stem == "basal-flow-select" {
            config["consumers"].as_object_mut().unwrap().remove("flow");
            config["runners"] = json!([
                {"module":"class-runner","via_vault":true},
                {"module":"broca","via_vault":true}
            ]);
        }
    });
    let mut quota = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_provider_reply(
        &mut quota,
        "sample-source",
        &serde_json::to_vec(&json!({
            "result":[{"provider":"sample-source","apiProvider":"synthetic",
                "account":"oauth-account","source":"vault",
                "fetchedAt":crate::account_selection::rfc3339(START as i64),
                "usage":{"primary":{"usedPercent":10.0,"windowMinutes":300,
                    "resetsAt":crate::account_selection::rfc3339(START as i64 + 3_600_000)}}}],
            "completeProviders":["sample-source"]
        }))
        .unwrap(),
        START as i64,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = quota;
    let runner = harness.bind(reserved("class-runner"), None).await;
    let consumer = harness.bind(reserved("class-owner"), None).await;
    let basal = harness.bind(reserved("basal"), None).await;
    // Both eligible accounts hold a real admission reservation. Their expiry,
    // twelve seconds after the refusal, must win over the thirty-second ceiling.
    for (account, credential) in [
        ("oauth-account", "oauth:test"),
        ("apikey-account", "apikey:test"),
    ] {
        let reply = runner
            .call(
                "route.account_for",
                &json!({"provider":"synthetic","model":"model",
                    "conversation_key":account,"admission_id":format!("fill-{account}"),
                    "candidates":{"view_digest":format!("{account}-only"),
                        "accounts":[{"account_id":account,"credential_id":credential}]}}),
            )
            .await;
        assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
        assert_eq!(reply.1["result"]["account"]["account_id"], account);
    }
    let _refusal_clock = crate::InjectedClock::set(REFUSED_AT);
    let account_params = json!({"provider":"synthetic","model":"model",
        "conversation_key":"sample-refused","admission_id":"sample-refused"});
    let (route, method, params) = match stem {
        "select" => (
            &consumer,
            "route.select",
            json!({"consumer":"mason","requirements":{"iq":30},"detail":true}),
        ),
        "account-for" => (&runner, "route.account_for", account_params.clone()),
        "basal-flow-select" => (
            &basal,
            "route.select",
            json!({"consumer":"basal.flow","requirements":{"iq":50,"tools":"none"},
                "eligible_routes":[{"provider":"synthetic","model":"model"}]}),
        ),
        _ => panic!("unknown sample: {stem}"),
    };
    let frame = route.request_frame(method, &params);
    let request = frame.body.clone();
    let reply = harness.handle(frame).await;
    let legacy_reply = if stem == "account-for" {
        let mut legacy = account_params;
        legacy["caller"] = json!({"runner":"class-runner"});
        Some(runner.call("route.account_for", &legacy).await)
    } else {
        None
    };
    Sample {
        request,
        reply: reply.body,
        frame_type: reply.header.ty,
        legacy_reply,
    }
}

#[tokio::test]
async fn basal_flow_select_refuses_only_the_synthetic_route() {
    let sample = capture("basal-flow-select").await;
    let request: Value = serde_json::from_slice(&sample.request).unwrap();
    assert!(request["params"].get("detail").is_none());
    let body = refusal(&sample);
    if let Some(rejected) = body["detail"].get("rejected") {
        let rejected = rejected.as_array().unwrap();
        assert_eq!(rejected.len(), 1, "{body}");
        assert_eq!(rejected[0]["routeKey"], "synthetic/model", "{body}");
        assert_eq!(rejected[0]["reason"], "concurrency_full", "{body}");
    }
}

fn refusal(sample: &Sample) -> Value {
    assert_eq!(
        sample.frame_type,
        FrameType::Error,
        "{}",
        String::from_utf8_lossy(&sample.reply)
    );
    let body: Value = serde_json::from_slice(&sample.reply).unwrap();
    let _: cortexkit_role_routing::errors::Refusal = serde_json::from_value(body.clone()).unwrap();
    assert_eq!(body["code"], "no_route", "{body}");
    assert_eq!(body["detail"]["reason"], "concurrency_full", "{body}");
    assert_eq!(body["detail"]["reset_at_ms"], RESET, "{body}");
    assert!(body.get("is_wait").is_none(), "{body}");
    assert!(body["detail"].get("is_wait").is_none(), "{body}");
    body
}

#[tokio::test]
async fn select_refusal_is_concurrency_full_with_rejections() {
    let sample = capture("select").await;
    let body = refusal(&sample);
    let rejected = body["detail"]["rejected"].as_array().unwrap();
    let full: Vec<_> = rejected
        .iter()
        .filter(|route| route["reason"] == "concurrency_full")
        .collect();
    assert_eq!(full.len(), 1, "{body}");
    assert_eq!(full[0]["routeKey"], "synthetic/model", "{body}");
}

#[tokio::test]
async fn account_for_refusal_preserves_allocator_reset_without_is_wait() {
    let sample = capture("account-for").await;
    let body = refusal(&sample);
    assert_eq!(
        body["detail"],
        json!({"reason":"concurrency_full","reset_at_ms":RESET})
    );
    let legacy = sample.legacy_reply.unwrap();
    assert_eq!(legacy.0, FrameType::Error, "{}", legacy.1);
    assert_eq!(legacy.1["code"], "concurrency_full", "{}", legacy.1);
    assert_eq!(
        legacy.1["detail"]["reset_at_ms"],
        body["detail"]["reset_at_ms"]
    );
}

fn verify(samples: BTreeMap<String, Vec<u8>>) {
    let directory =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test-vectors/route-concurrency-full-v1");
    let update = std::env::var_os("UPDATE_CONCURRENCY_FULL_SAMPLES").is_some();
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
async fn handler_samples_regenerate_byte_for_byte() {
    let mut samples = BTreeMap::new();
    for stem in ["select", "account-for", "basal-flow-select"] {
        let sample = capture(stem).await;
        refusal(&sample);
        // Keep the handler's bytes, not a parsed and reserialized equivalent.
        assert!(samples
            .insert(format!("{stem}.request.json"), sample.request)
            .is_none());
        assert!(samples
            .insert(format!("{stem}.reply.json"), sample.reply)
            .is_none());
    }
    verify(samples);
}
