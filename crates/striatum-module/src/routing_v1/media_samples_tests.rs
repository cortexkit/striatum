//! Preserve the real dispatcher's media wire bytes independently of replay goldens.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

use cortexkit_role_routing::select_media::{
    check_answer, parse_select_media_request, SelectMediaAnswer,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subc_protocol::FrameType;

use crate::routing_v1::{
    conformance_tests::{reserved, Harness, Route},
    consumers, describe,
};

const NOW: i64 = 1_750_000_000_000;
const VOICE: &str = "colliculus.voice_message";

fn served_row(route: &str) -> Value {
    let payloads: Value =
        serde_json::from_str(include_str!("../../fixtures/served-payloads.json")).unwrap();
    payloads
        .as_object()
        .unwrap()
        .values()
        .map(|payload| &payload["response"]["models"][route])
        .find(|row| {
            row.as_object()
                .is_some_and(|row| row.keys().any(|key| key.starts_with("rate.")))
        })
        .expect("pinned rate-bearing row")
        .clone()
}

fn fixture() -> Arc<Harness> {
    let harness = super::super::setup(vec![]);
    consumers::replace_catalog(&json!([{
        "module_id":"colliculus", "nonce_verified":true, "role_declarations":{
            "routing/v1":{"consumers":[
                {"name":VOICE,"kind":"transcribe","selectors":["callosum"]},
                {"name":"colliculus.speech","kind":"synthesize"}
            ]},
            "media-provider/v1":{"engines":[
                {"kind":"transcribe","provider":"media-vendor","model":"fixture-transcribe",
                    "local":false,"formats":["audio/wav"],"max_duration_ms":60000,"latency_class":"interactive"},
                {"kind":"transcribe","provider":"media-vendor","model":"fixture-premium",
                    "local":false,"formats":["audio/wav"],"max_duration_ms":60000,"latency_class":"interactive"},
                {"kind":"synthesize","provider":"media-vendor","model":"fixture-speech",
                    "local":false,"formats":["audio/wav"],"max_input_bytes":10000,"latency_class":"interactive"}
            ]}
        }
    }]));
    harness.edit_config(|config| {
        config["consumers"][VOICE] = json!({"mode":"demand","iq":40});
        config["consumers"]["colliculus.speech"] = json!({"mode":"demand","iq":40});
    });
    let mut audio = served_row("openai/gpt-transcribe");
    audio["quality.grade"] = json!(80);
    let mut speech = served_row("elevenlabs/eleven_flash_v2_5");
    speech["quality.grade"] = json!(80);
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({
            "catalog_version":41,"resolved_at_ms":NOW,
            "quality_scores":{"graded_by":"operator","graded_at_ms":NOW},
            "models":{
                "media-vendor/fixture-transcribe":audio,
                "media-vendor/fixture-speech":speech,
                "media-vendor/phone-transcriber":{
                    "existence":"present","model.type":"transcription","quality.grade":80
                },
                "media-vendor/fixture-premium":{
                    "existence":"present","model.type":"transcription","quality.grade":100
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    harness.credentials().lock().unwrap().catalog_snapshot = Some(Arc::new(snapshot));
    harness
}

fn voice_request() -> Value {
    json!({"consumer":VOICE,"kind":"transcribe","privacy":"remote_allowed",
        "requirements":{"duration_ms":30000,"format":"audio/wav"}})
}

#[derive(Default)]
struct Samples(BTreeMap<String, Vec<u8>>);

impl Samples {
    async fn capture(
        &mut self,
        name: &str,
        harness: &Harness,
        route: &Route,
        params: &Value,
        expected_model: Option<&str>,
    ) -> Value {
        let frame = route.request_frame("route.select", params);
        let request = frame.body.clone();
        let reply = harness.handle(frame).await;
        let value: Value = serde_json::from_slice(&reply.body).unwrap();
        if let Some(model) = expected_model {
            assert_eq!(reply.header.ty, FrameType::Response, "{name}: {value}");
            let result = &value["result"];
            assert_eq!(result["engine"]["model"], model, "{name}: {value}");
            if params.get("caller_local").is_none() {
                assert_eq!(result["engine"]["module"], "colliculus");
            }
            assert_eq!(result["expires_at_ms"], NOW + 600_000);
            let request = parse_select_media_request(
                params,
                &describe::capabilities(),
                &["transcribe", "synthesize"],
            )
            .unwrap();
            let answer: SelectMediaAnswer = serde_json::from_value(result.clone()).unwrap();
            check_answer(&request, &answer).unwrap();
            // A typed round-trip also rejects extra wire members that serde would ignore.
            assert_eq!(serde_json::to_value(answer).unwrap(), *result);
        } else {
            assert_eq!(reply.header.ty, FrameType::Error, "{name}: {value}");
            assert_eq!(value["code"], "no_route", "{name}: {value}");
            assert_eq!(value["detail"], json!({"reason":"no_available_model"}));
        }
        // Keep the captured bodies verbatim; reserializing would hide wire-order drift.
        let mut pair = b"{\"request\":".to_vec();
        pair.extend_from_slice(&request);
        pair.extend_from_slice(b",\"reply\":");
        pair.extend_from_slice(&reply.body);
        pair.push(b'}');
        assert!(self.0.insert(format!("{name}.json"), pair).is_none());
        value
    }

    fn verify(self) {
        let directory =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test-vectors/route-select-media-v1");
        let update = std::env::var_os("UPDATE_ROUTE_SELECT_MEDIA_VECTORS").is_some();
        let mut sums = String::new();
        for (name, bytes) in &self.0 {
            let path = directory.join(name);
            if update {
                std::fs::create_dir_all(&directory).unwrap();
                std::fs::write(&path, bytes).unwrap();
            }
            assert_eq!(std::fs::read(&path).expect(name), *bytes, "{name}");
            sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(bytes)));
        }
        let path = directory.join("SHA256SUMS");
        if update {
            std::fs::write(&path, &sums).unwrap();
        }
        assert_eq!(std::fs::read_to_string(path).unwrap(), sums, "SHA256SUMS");
        let files: BTreeSet<_> = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        let expected: BTreeSet<_> = self
            .0
            .keys()
            .cloned()
            .chain(["README.md".into(), "SHA256SUMS".into()])
            .collect();
        assert_eq!(files, expected, "extra or missing vector files");
    }
}

#[tokio::test]
async fn serving_bytes_match_media_vectors() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let _ids = striatum_store::replay::Inputs::new(NOW).install();
    let harness = fixture();
    let phone = harness.bind(reserved("callosum"), None).await;
    let colliculus = harness.bind(reserved("colliculus"), None).await;
    let mut samples = Samples::default();
    let voice = voice_request();
    let selected = samples
        .capture(
            "voice-message-transcribe",
            &harness,
            &phone,
            &voice,
            Some("fixture-transcribe"),
        )
        .await;
    assert_eq!(selected["result"]["price"]["basis"], "metered");
    assert_eq!(
        selected["result"]["price"]["rates"]["input_audio.per_second"]["rate"],
        0.000075
    );

    let speech = json!({"consumer":"colliculus.speech","kind":"synthesize",
            "privacy":"remote_allowed","requirements":{"input_bytes":24,"input_characters":24}});
    let selected = samples
        .capture(
            "speech",
            &harness,
            &colliculus,
            &speech,
            Some("fixture-speech"),
        )
        .await;
    assert_eq!(
        selected["result"]["price"]["rates"]["input.per_character"]["rate"],
        0.00004
    );

    // A request pin admits a pin-only model; an ordinary ladder must still skip it.
    harness.edit_config(|config| {
        config["models"]["media-vendor/fixture-premium"] = json!({"pin_only":true});
    });
    let mut pinned = voice.clone();
    pinned["pin"] = json!("media-vendor/fixture-premium");
    let selected = samples
        .capture("pinned", &harness, &phone, &pinned, Some("fixture-premium"))
        .await;
    assert!(selected["result"].get("price").is_none());

    harness.edit_config(|config| {
        config["consumers"][VOICE]["mode"] = json!("ladder");
        config["consumers"][VOICE]["ladder"] = json!([
            "media-vendor/missing",
            "media-vendor/fixture-premium",
            "media-vendor/fixture-transcribe"
        ]);
    });
    samples
        .capture(
            "ladder",
            &harness,
            &phone,
            &voice,
            Some("fixture-transcribe"),
        )
        .await;

    let mut local_only = voice;
    local_only["privacy"] = json!("local_only");
    samples
        .capture("no-route", &harness, &phone, &local_only, None)
        .await;
    harness.edit_config(|config| {
        config["consumers"][VOICE]["mode"] = json!("demand");
    });
    capture_phone_voice(&mut samples, &harness, &phone).await;
    samples.verify();
}

async fn capture_phone_voice(samples: &mut Samples, harness: &Harness, phone: &Route) {
    let mut request = voice_request();
    request["caller_local"] = json!([{
        "kind":"transcribe","provider":"media-vendor","model":"phone-transcriber",
        "local":true,"formats":["audio/wav"],"max_duration_ms":60000,
        "latency_class":"interactive"
    }]);
    let selected = samples
        .capture(
            "voice-message-caller-local",
            harness,
            phone,
            &request,
            Some("phone-transcriber"),
        )
        .await;
    assert_eq!(selected["result"]["engine"]["caller_local"], true);
    assert_eq!(selected["result"]["engine"]["local"], true);
    assert_eq!(selected["result"]["engine"]["index"], 0);
    assert!(selected["result"]["engine"].get("module").is_none());
    assert!(selected["result"].get("price").is_none());
}

#[tokio::test]
async fn production_phone_voice_offer_uses_equal_catalog_grade_without_price() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let _ids = striatum_store::replay::Inputs::new(NOW).install();
    let harness = fixture();
    let phone = harness.bind(reserved("callosum"), None).await;
    capture_phone_voice(&mut Samples::default(), &harness, &phone).await;
}
