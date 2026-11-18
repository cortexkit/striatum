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
use striatum_core::{route_state_store::RouteStateUpdate, RouteStateStore};
use subc_protocol::FrameType;

use crate::routing_v1::{
    conformance_tests::{reserved, Harness, Route},
    consumers, describe,
    engines::MEDIA_KINDS,
};

const NOW: i64 = 1_750_000_000_000;
const VOICE: &str = "colliculus.voice_message";
const DICTATION: &str = "colliculus.dictation_token";
const VOICE_FRONT: &str = "colliculus.voice_front_token";

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
    fixture_for_phone(false)
}

fn fixture_for_phone(with_phone_languages: bool) -> Arc<Harness> {
    let harness = super::super::setup(vec![]);
    let mut declarations = json!([{
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
    }]);
    if with_phone_languages {
        declarations[0]["role_declarations"]["media-provider/v1"]["engines"][0]["languages"] =
            json!(["fr"]);
    }
    consumers::replace_catalog(&declarations);
    harness.edit_config(|config| {
        config["consumers"][VOICE] = json!({"mode":"demand","iq":40});
        config["consumers"]["colliculus.speech"] = json!({"mode":"demand","iq":40});
    });
    let mut audio = served_row("openai/gpt-transcribe");
    audio["quality.grade.transcription"] = json!(80);
    let mut speech = served_row("elevenlabs/eleven_flash_v2_5");
    speech["quality.grade.speech"] = json!(80);
    let mut catalog = json!({
        "catalog_version":41,"resolved_at_ms":NOW,
        "quality_scores":{"graded_by":"operator","graded_at_ms":NOW},
        "models":{
            "media-vendor/fixture-transcribe":audio,
            "media-vendor/fixture-speech":speech,
            "media-vendor/phone-transcriber":{
                "existence":"present","model.type":"transcription","quality.grade.transcription":80
            },
            "media-vendor/fixture-premium":{
                "existence":"present","model.type":"transcription","quality.grade.transcription":100
            }
        }
    });
    if with_phone_languages {
        catalog["models"]["apple/phone-transcriber"] = json!({"existence":"present","model.type":"transcription","quality.grade.transcription":80});
    }
    let snapshot =
        crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&catalog).unwrap())
            .unwrap();
    let snapshot = (*snapshot).clone().with_local_media_grade(
        "local:media-vendor/phone-transcriber",
        "transcription",
        80.0,
    );
    let snapshot = if with_phone_languages {
        snapshot.with_local_media_grade("local:apple/phone-transcriber", "transcription", 80.0)
    } else {
        snapshot
    };
    harness.credentials().lock().unwrap().catalog_snapshot = Some(Arc::new(snapshot));
    harness
}

fn voice_request() -> Value {
    json!({"consumer":VOICE,"kind":"transcribe","privacy":"remote_allowed",
        "requirements":{"duration_ms":30000,"format":"audio/wav"}})
}

fn fixture_for_realtime(with_realtime: bool) -> Arc<Harness> {
    let harness = super::super::setup(vec![]);
    let mut media: Value = serde_json::from_str(include_str!(
        "../../fixtures/colliculus-5b/media-role-describe.json"
    ))
    .unwrap();
    let routing: Value = serde_json::from_str(include_str!(
        "../../fixtures/colliculus-5b/routing-v1-declaration.json"
    ))
    .unwrap();
    assert_eq!(media["engines"].as_array().unwrap().len(), 10);
    assert_eq!(routing["consumers"].as_array().unwrap().len(), 6);
    if !with_realtime {
        // Keep stream_transcribe: the same model cannot satisfy a different kind.
        media["engines"]
            .as_array_mut()
            .unwrap()
            .retain(|engine| engine["kind"] != "realtime");
        assert_eq!(media["engines"].as_array().unwrap().len(), 8);
    }
    crate::take_operator_warnings();
    consumers::replace_catalog(&json!([{
        "module_id":"colliculus", "nonce_verified":true, "role_declarations":{
            "routing/v1":routing, "media-provider/v1":media
        }
    }]));
    assert!(crate::take_operator_warnings().is_empty());
    harness.edit_config(|config| {
        config["consumers"][DICTATION] = json!({"iq":40});
        config["consumers"][VOICE_FRONT] = json!({"iq":40});
    });
    let mut key = super::super::key("opaque-openai-media");
    key.provider_ids = vec!["openai".into()];
    harness.set_credentials(super::super::media_credentials(vec![key]));
    install_realtime_catalog(&harness, 80, 80);
    harness
}

fn install_realtime_catalog(harness: &Harness, live_grade: u8, voice_grade: u8) {
    let mut live = served_row("openai/gpt-live-transcribe");
    live["quality.grade"] = json!(live_grade);
    let mut voice = served_row("openai/gpt-realtime-2.1");
    voice["quality.grade"] = json!(voice_grade);
    // Realtime has no operator grade kind; these retired bare grades must be ignored.
    for row in [&mut live, &mut voice] {
        row["existence"] = json!("present");
        row["model.type"] = json!("realtime");
    }
    let catalog = json!({
        "catalog_version":41,"resolved_at_ms":NOW,
        "quality_scores":{"graded_by":"operator","graded_at_ms":NOW},
        "models":{
            "openai/gpt-live-transcribe":live,
            "openai/gpt-realtime-2.1":voice
        }
    });
    let snapshot =
        crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&catalog).unwrap())
            .unwrap();
    assert_eq!(
        snapshot.media_grade("realtime", "openai/gpt-live-transcribe"),
        None
    );
    assert_eq!(
        snapshot.media_grade("realtime", "openai/gpt-realtime-2.1"),
        None
    );
    harness.credentials().lock().unwrap().catalog_snapshot = Some(snapshot);
}

#[tokio::test]
async fn colliculus_5b_declarations_parse_without_warnings_and_keep_other_consumers() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let routing: Value = serde_json::from_str(include_str!(
        "../../fixtures/colliculus-5b/routing-v1-declaration.json"
    ))
    .unwrap();
    let media: Value = serde_json::from_str(include_str!(
        "../../fixtures/colliculus-5b/media-role-describe.json"
    ))
    .unwrap();
    let parsed = cortexkit_role_routing::declarations::parse_consumer_declarations_serving(
        &routing,
        &describe::capabilities(),
    )
    .unwrap();
    assert_eq!(parsed.consumers.len(), 6);
    let parsed = cortexkit_role_routing::parse_engine_declarations(&media).unwrap();
    assert_eq!(parsed.engines.len(), 10);
    let previous: Value = serde_json::from_str(include_str!(
        "../../fixtures/colliculus-media-v1/routing-v1-declaration.json"
    ))
    .unwrap();
    assert_eq!(
        routing["consumers"].as_array().unwrap()[..4],
        previous["consumers"].as_array().unwrap()[..4]
    );
    for (index, consumer, model) in [
        (4, DICTATION, "gpt-live-transcribe"),
        (5, VOICE_FRONT, "gpt-realtime-2.1"),
    ] {
        assert_eq!(
            routing["consumers"][index],
            json!({
                "name":consumer, "kind":"realtime", "selectors":["callosum"],
                "ladder":[format!("openai/{model}")]
            })
        );
    }
    // Catalog ingestion must accept both complete declarations without warnings.
    let _harness = fixture_for_realtime(true);
}

async fn assert_realtime_ladder(consumer: &str, model: &str, other: &str, cause: &str) {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let harness = fixture_for_realtime(true);
    // Without the declared ladder, demand mode must prefer the other engine.
    // Both engines remain priced and keyed; only the other engine meets IQ 100.
    let (live_grade, voice_grade) = if model == "gpt-live-transcribe" {
        (80, 100)
    } else {
        (100, 80)
    };
    install_realtime_catalog(&harness, live_grade, voice_grade);
    harness.edit_config(|config| config["consumers"][consumer]["iq"] = json!(100));
    match cause {
        "available" => {}
        "no-key" => {
            // Fence the sole key for this model only, leaving the other engine usable.
            harness
                .store()
                .record_account_route_fence(&striatum_store::AccountRouteFence {
                    provider: "openai".into(),
                    credential_id: "opaque-openai-media".into(),
                    model: model.into(),
                    until_ms: NOW + 60_000,
                    decision_id: String::new(),
                    recorded_at: NOW,
                })
                .unwrap();
        }
        "excluded" => harness.edit_config(|config| {
            config["models"][format!("openai/{model}")] = json!({"not_for":[consumer]});
        }),
        _ => panic!("unknown realtime ladder test cause: {cause}"),
    }
    let phone = harness.bind(reserved("callosum"), None).await;
    let request = json!({"consumer":consumer,"kind":"realtime",
        "requirements":{},"privacy":"remote_allowed"});
    let (frame, reply) = phone.call("route.select", &request).await;
    if cause == "available" {
        assert_eq!(frame, FrameType::Response, "{reply}");
        assert_eq!(
            reply["result"]["engine"]["model"], model,
            "{consumer} must never select {other}: {reply}"
        );
        assert_eq!(reply["result"]["engine"]["provider"], "openai");
        assert_eq!(reply["result"]["engine"]["module"], "colliculus");
    } else {
        assert_eq!(
            frame,
            FrameType::Error,
            "{consumer} must refuse, never substitute {other}: {reply}"
        );
        assert_eq!(reply["code"], "no_route", "{reply}");
        assert_eq!(reply["detail"], json!({"reason":"no_available_model"}));
    }
}

#[tokio::test]
async fn colliculus_5b_dictation_never_selects_voice() {
    assert_realtime_ladder(
        DICTATION,
        "gpt-live-transcribe",
        "gpt-realtime-2.1",
        "available",
    )
    .await;
}

#[tokio::test]
async fn colliculus_5b_dictation_no_key_never_substitutes_voice() {
    assert_realtime_ladder(
        DICTATION,
        "gpt-live-transcribe",
        "gpt-realtime-2.1",
        "no-key",
    )
    .await;
}

#[tokio::test]
async fn colliculus_5b_dictation_excluded_never_substitutes_voice() {
    assert_realtime_ladder(
        DICTATION,
        "gpt-live-transcribe",
        "gpt-realtime-2.1",
        "excluded",
    )
    .await;
}

#[tokio::test]
async fn colliculus_5b_voice_never_selects_dictation() {
    assert_realtime_ladder(
        VOICE_FRONT,
        "gpt-realtime-2.1",
        "gpt-live-transcribe",
        "available",
    )
    .await;
}

#[tokio::test]
async fn colliculus_5b_voice_no_key_never_substitutes_dictation() {
    assert_realtime_ladder(
        VOICE_FRONT,
        "gpt-realtime-2.1",
        "gpt-live-transcribe",
        "no-key",
    )
    .await;
}

#[tokio::test]
async fn colliculus_5b_voice_excluded_never_substitutes_dictation() {
    assert_realtime_ladder(
        VOICE_FRONT,
        "gpt-realtime-2.1",
        "gpt-live-transcribe",
        "excluded",
    )
    .await;
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
            let request =
                parse_select_media_request(params, &describe::capabilities(), MEDIA_KINDS).unwrap();
            let answer: SelectMediaAnswer = serde_json::from_value(result.clone()).unwrap();
            check_answer(&request, &answer).unwrap();
            // A typed round-trip also rejects extra wire members that serde would ignore.
            assert_eq!(serde_json::to_value(answer).unwrap(), *result);
        } else {
            assert_eq!(reply.header.ty, FrameType::Error, "{name}: {value}");
            assert_eq!(value["code"], "no_route", "{name}: {value}");
            assert_eq!(value["detail"], json!({"reason":"no_available_model"}));
        }
        self.insert(name, &request, &reply.body);
        value
    }

    async fn capture_method(
        &mut self,
        name: &str,
        harness: &Harness,
        route: &Route,
        method: &str,
        params: &Value,
        error: Option<(&str, Option<Value>)>,
    ) -> Value {
        let frame = route.request_frame(method, params);
        let request = frame.body.clone();
        let reply = harness.handle(frame).await;
        let value: Value = serde_json::from_slice(&reply.body).unwrap();
        if let Some((code, detail)) = error {
            assert_eq!(reply.header.ty, FrameType::Error, "{name}: {value}");
            assert_eq!(value["code"], code, "{name}: {value}");
            if let Some(detail) = detail {
                assert_eq!(value["detail"], detail, "{name}: {value}");
            }
            assert!(value.get("retry_at_ms").is_none(), "{name}: {value}");
        } else {
            assert_eq!(reply.header.ty, FrameType::Response, "{name}: {value}");
        }
        self.insert(name, &request, &reply.body);
        value
    }

    fn insert(&mut self, name: &str, request: &[u8], reply: &[u8]) {
        // Keep the captured bodies verbatim; reserializing would hide wire-order drift.
        let mut pair = b"{\"request\":".to_vec();
        pair.extend_from_slice(request);
        pair.extend_from_slice(b",\"reply\":");
        pair.extend_from_slice(reply);
        pair.push(b'}');
        assert!(self.0.insert(format!("{name}.json"), pair).is_none());
    }

    fn verify(self) {
        self.verify_set("route-select-media-v1", "UPDATE_ROUTE_SELECT_MEDIA_VECTORS");
    }

    fn verify_set(self, name: &str, update_variable: &str) {
        let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../test-vectors")
            .join(name);
        let update = std::env::var_os(update_variable).is_some();
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
async fn serving_bytes_match_transcription_bias_vectors() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let _ids = striatum_store::replay::Inputs::new(NOW).install();
    let harness = fixture_for_realtime(true);
    harness.edit_config(|config| {
        config["consumers"][VOICE] = json!({"mode":"demand","iq":40});
    });
    let mut key = super::super::key("opaque-transcribe-media");
    key.provider_ids = vec!["openai".into(), "google".into()];
    harness.set_credentials(super::super::media_credentials(vec![key]));
    let mut audio = served_row("openai/gpt-transcribe");
    audio["quality.grade.transcription"] = json!(80);
    // Preserve the older samples' priced/adequate versus unpriced/higher-grade contrast.
    let catalog = json!({
        "catalog_version":41,"resolved_at_ms":NOW,
        "quality_scores":{"graded_by":"operator","graded_at_ms":NOW},
        "models":{
            "openai/gpt-transcribe":audio,
            "google/gemini-3.8-flash":{
                "existence":"present","model.type":"transcription","quality.grade.transcription":100
            },
            "apple/phone-transcriber":{
                "existence":"present","model.type":"transcription","quality.grade.transcription":80
            }
        }
    });
    let snapshot =
        crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&catalog).unwrap())
            .unwrap();
    let snapshot = (*snapshot).clone().with_local_media_grade(
        "local:apple/phone-transcriber",
        "transcription",
        80.0,
    );
    harness.credentials().lock().unwrap().catalog_snapshot = Some(Arc::new(snapshot));
    let phone = harness.bind(reserved("callosum"), None).await;
    let base: Value = serde_json::from_str(include_str!(
        "../../../../test-vectors/route-select-media-v1/voice-message-caller-local-en.json"
    ))
    .unwrap();
    let mut request = base["request"]["params"].clone();
    request["caller_local"][0]["bias_limit"] = json!({"unit":"phrases","max":100});
    request["requirements"]["bias"] = json!(true);
    let mut samples = Samples::default();
    for (name, wants_bias, offers_bias, model) in [
        (
            "voice-message-bias-caller-local",
            true,
            true,
            "phone-transcriber",
        ),
        ("voice-message-bias-remote", true, false, "gpt-transcribe"),
        (
            "voice-message-no-bias-caller-local",
            false,
            true,
            "phone-transcriber",
        ),
    ] {
        if !wants_bias {
            request["requirements"]
                .as_object_mut()
                .unwrap()
                .remove("bias");
        }
        if offers_bias {
            request["caller_local"][0]["bias_limit"] = json!({"unit":"phrases","max":100});
        } else {
            request["caller_local"][0]
                .as_object_mut()
                .unwrap()
                .remove("bias_limit");
        }
        let selected = samples
            .capture(name, &harness, &phone, &request, Some(model))
            .await;
        let result = &selected["result"];
        if offers_bias {
            assert_eq!(
                result["engine"],
                json!({
                    "caller_local":true,"index":0,"local":true,
                    "model":"phone-transcriber","provider":"apple"
                })
            );
            assert!(result.get("price").is_none());
        } else {
            assert_eq!(
                result["engine"],
                json!({
                    "caller_local":false,"local":false,"module":"colliculus",
                    "model":"gpt-transcribe","provider":"openai"
                })
            );
            assert_eq!(
                result["price"]["rates"]["input_audio.per_second"]["rate"],
                0.000075
            );
        }
    }
    samples.verify_set(
        "route-select-media-v1.5",
        "UPDATE_TRANSCRIPTION_BIAS_VECTORS",
    );
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
    let decision = selected["result"]["decision_id"].clone();
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
    capture_redemption(&mut samples, &harness, &colliculus, &decision).await;
    capture_refusals(&mut samples).await;
    capture_unavailable(&mut samples).await;
    capture_phone_languages(&mut samples).await;
    capture_dictation(&mut samples).await;
    capture_voice_front(&mut samples).await;
    samples.verify();
}

async fn capture_dictation(samples: &mut Samples) {
    let request = json!({"consumer":DICTATION,"kind":"realtime",
        "requirements":{},"privacy":"remote_allowed"});
    for (name, with_realtime) in [
        ("dictation-token-realtime", true),
        ("dictation-token-no-engine", false),
    ] {
        let harness = fixture_for_realtime(with_realtime);
        let phone = harness.bind(reserved("callosum"), None).await;
        let selected = samples
            .capture(
                name,
                &harness,
                &phone,
                &request,
                with_realtime.then_some("gpt-live-transcribe"),
            )
            .await;
        if with_realtime {
            assert_eq!(selected["result"]["engine"]["provider"], "openai");
            assert_eq!(selected["result"]["engine"]["caller_local"], false);
        }
    }
}

async fn capture_voice_front(samples: &mut Samples) {
    let harness = fixture_for_realtime(true);
    let phone = harness.bind(reserved("callosum"), None).await;
    let request = json!({"consumer":VOICE_FRONT,"kind":"realtime",
        "requirements":{},"privacy":"remote_allowed"});
    let selected = samples
        .capture(
            "voice-front-token-realtime",
            &harness,
            &phone,
            &request,
            Some("gpt-realtime-2.1"),
        )
        .await;
    assert_eq!(selected["result"]["engine"]["provider"], "openai");
    assert_eq!(selected["result"]["engine"]["caller_local"], false);
}

async fn capture_phone_languages(samples: &mut Samples) {
    let harness = fixture_for_phone(true);
    let phone = harness.bind(reserved("callosum"), None).await;
    let mut request = voice_request();
    request["caller_local"] = json!([{
        "formats":["audio/wav"],"kind":"transcribe","languages":["en-US"],
        "latency_class":"realtime","local":true,"max_duration_ms":600000,
        "model":"phone-transcriber","provider":"apple"
    }]);
    request["requirements"]["language"] = json!("en-US");
    let selected = samples
        .capture(
            "voice-message-caller-local-en",
            &harness,
            &phone,
            &request,
            Some("phone-transcriber"),
        )
        .await;
    assert_eq!(selected["result"]["engine"]["provider"], "apple");
    assert_eq!(selected["result"]["engine"]["caller_local"], true);
    assert_eq!(selected["result"]["engine"]["index"], 0);
    assert!(selected["result"]["engine"].get("module").is_none());
    assert!(selected["result"].get("price").is_none());

    request["requirements"]["language"] = json!("fr-FR");
    let selected = samples
        .capture(
            "voice-message-caller-local-fr",
            &harness,
            &phone,
            &request,
            Some("fixture-transcribe"),
        )
        .await;
    assert_eq!(selected["result"]["engine"]["caller_local"], false);
    assert_eq!(selected["result"]["engine"]["module"], "colliculus");
    assert_eq!(selected["result"]["engine"]["provider"], "media-vendor");
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

async fn capture_redemption(
    samples: &mut Samples,
    harness: &Arc<Harness>,
    executor: &Route,
    decision: &Value,
) {
    let mut request = json!({
        "admission_id":"media-before-expiry","conversation_key":"voice-message",
        "provider":"media-vendor","model":"fixture-transcribe",
        "decision_id":decision,"hold_wait_max_s":0
    });
    let admitted = samples
        .capture_method(
            "decision-admitted",
            harness,
            executor,
            "route.account_for",
            &request,
            None,
        )
        .await;
    assert_eq!(admitted["result"]["admission"]["state"], "reserved");
    assert_eq!(
        admitted["result"]["account"]["provider"],
        request["provider"]
    );
    let binding = harness
        .store()
        .v1_decision(decision.as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(binding.runner_module, "colliculus");
    assert_eq!(binding.provider, request["provider"].as_str().unwrap());
    assert_eq!(binding.model, request["model"].as_str().unwrap());

    let foreign = harness.bind(reserved("stranger"), None).await;
    request["admission_id"] = json!("media-wrong-caller");
    samples
        .capture_method(
            "decision-caller-mismatch",
            harness,
            &foreign,
            "route.account_for",
            &request,
            Some(("account_not_authorized", None)),
        )
        .await;

    harness.credentials().lock().unwrap().engine_clock_offset_ms = 600_001;
    request["admission_id"] = json!("media-after-expiry");
    samples
        .capture_method(
            "decision-expired",
            harness,
            executor,
            "route.account_for",
            &request,
            Some(("unknown_decision", None)),
        )
        .await;
    harness.credentials().lock().unwrap().engine_clock_offset_ms = 0;
}

async fn capture_refusals(samples: &mut Samples) {
    let harness = fixture();
    let phone = harness.bind(reserved("callosum"), None).await;
    for (name, patch, code, field) in [
        (
            "unknown-consumer",
            json!({"consumer":"unknown.media"}),
            "unknown_consumer",
            None,
        ),
        (
            "invalid-request",
            json!({"effect_id":"not-supported"}),
            "invalid_request",
            Some("effect_id"),
        ),
        (
            "kind-mismatch",
            json!({"kind":"synthesize"}),
            "invalid_request",
            Some("kind"),
        ),
        (
            "invalid-requirements",
            json!({"requirements":{"duration_ms":-1}}),
            "invalid_requirements",
            Some("duration_ms"),
        ),
        (
            "requirements-unmet",
            json!({"requirements":{"language":"fr"}}),
            "no_route",
            None,
        ),
    ] {
        let mut request = voice_request();
        for (key, value) in patch.as_object().unwrap() {
            request[key] = value.clone();
        }
        let detail = (code == "no_route").then(|| json!({"reason":"no_available_model"}));
        let reply = samples
            .capture_method(
                name,
                &harness,
                &phone,
                "route.select",
                &request,
                Some((code, detail)),
            )
            .await;
        if let Some(field) = field {
            assert_eq!(reply["detail"]["field"], field);
        }
    }
    for (name, principal) in [
        ("caller-not-owner", reserved("stranger")),
        ("caller-direct", json!({"kind":"direct"})),
    ] {
        let route = harness.bind(principal, None).await;
        samples
            .capture_method(
                name,
                &harness,
                &route,
                "route.select",
                &voice_request(),
                Some(("consumer_not_owned", None)),
            )
            .await;
    }
    let channel = harness.reserve_channel();
    let flow = harness.bind_frame(json!({
        "op":"route.bind","route_channel":channel,"epoch":1,
        "target":{"kind":"management_surface","module_id":crate::DEFAULT_MODULE_ID},
        "identity":{"project_root":cortexkit_test_support::scratch_root(),"harness":"media-samples","session":"flow-session"},
        "principal":reserved("callosum"),
        "scope":{"owner":reserved("callosum"),"ref":"phone-media-flow","scope_epoch":1,
            "kind":"head","owner_authorized":false,"attributes":{"flow_id":"fixture-flow"}}
    }), channel, 1).await;
    samples
        .capture_method(
            "flow-scope-not-supported",
            &harness,
            &flow,
            "route.select",
            &voice_request(),
            Some(("flow_scope_not_supported", None)),
        )
        .await;

    consumers::replace_catalog(&json!([{"module_id":"colliculus","nonce_verified":true,
        "role_declarations":{"routing/v1":{"consumers":[{"name":VOICE,"kind":"transcribe","selectors":["callosum"]}]}}}]));
    samples
        .capture_method(
            "no-engines",
            &harness,
            &phone,
            "route.select",
            &voice_request(),
            Some(("no_route", Some(json!({"reason":"no_available_model"})))),
        )
        .await;

    consumers::replace_catalog(
        &json!([{"module_id":"colliculus","nonce_verified":true,"role_declarations":{
            "routing/v1":{"consumers":[{"name":"colliculus.video","kind":"video","selectors":["callosum"]}]},
        "media-provider/v1":{"engines":[{"kind":"video","provider":"media-vendor","model":"fixture-video","local":false,
            "formats":["video/mp4"],"languages":["en"],"latency_class":"interactive"}]}
        }}]),
    );
    let video = json!({"consumer":"colliculus.video","kind":"video","privacy":"remote_allowed",
        "requirements":{"output_duration_ms":1000}});
    samples
        .capture_method(
            "missing-capability",
            &harness,
            &phone,
            "route.select",
            &video,
            Some(("no_route", Some(json!({"reason":"missing_capability"})))),
        )
        .await;
}

async fn capture_unavailable(samples: &mut Samples) {
    for cause in [
        "route-cooldown",
        "route-quota-exhausted",
        "route-failure-backoff",
        "credential-entitlement-fenced",
        "credential-billing-fenced",
        "credential-refused",
        "pool-billing-wall",
        "pool-rate-limit-wall",
        "account-route-fence",
        "listing-missing",
        "listing-stale",
        "listing-failed",
        "listing-grant-missing",
        "no-admissible-media-credential",
    ] {
        let harness = fixture();
        let until = NOW + 60_000;
        let store = harness.store();
        match cause {
            "route-cooldown" | "route-quota-exhausted" | "route-failure-backoff" => {
                for model in ["fixture-transcribe", "fixture-premium"] {
                    let key = format!("media-vendor/{model}");
                    store
                        .update_route_state(
                            &key,
                            &key,
                            &RouteStateUpdate {
                                cooldown_until: (cause == "route-cooldown").then_some(until as f64),
                                quota_exhausted_until: (cause == "route-quota-exhausted")
                                    .then_some(until as f64),
                                ..Default::default()
                            },
                        )
                        .unwrap();
                    if cause == "route-failure-backoff" {
                        crate::routing_v1::native_fixture_sources::failure_backoff(
                            store,
                            &key,
                            until as f64,
                            None,
                        );
                    }
                }
                if cause == "route-failure-backoff" {
                    // Publish the native fixture input to the serving snapshot.
                    store.reload_account_state_for_test().unwrap();
                }
            }
            "credential-entitlement-fenced"
            | "credential-billing-fenced"
            | "credential-refused" => {
                store
                    .record_credential_entitlement_fence(
                        &striatum_core::route_state_store::CredentialRefusal {
                            provider_id: "media-vendor".into(),
                            credential_id: "opaque-media".into(),
                            code: match cause {
                                "credential-entitlement-fenced" => "ENTITLEMENT_ERROR",
                                "credential-billing-fenced" => "billing_exhausted",
                                _ => "credential_refused",
                            }
                            .into(),
                        },
                        None,
                        NOW,
                    )
                    .unwrap();
            }
            "pool-billing-wall" | "pool-rate-limit-wall" => {
                store
                    .pool_wall(
                        "media-vendor",
                        "opaque-media",
                        if cause == "pool-billing-wall" {
                            striatum_store::PoolWallKind::Billing
                        } else {
                            striatum_store::PoolWallKind::RateLimited
                        },
                        until,
                        "synthetic provider wall",
                        NOW,
                    )
                    .unwrap();
            }
            "account-route-fence" => {
                for model in ["fixture-transcribe", "fixture-premium"] {
                    store
                        .record_account_route_fence(&striatum_store::AccountRouteFence {
                            provider: "media-vendor".into(),
                            credential_id: "opaque-media".into(),
                            model: model.into(),
                            until_ms: until,
                            decision_id: String::new(),
                            recorded_at: NOW,
                        })
                        .unwrap();
                }
            }
            _ => {
                let mut row = super::super::key("opaque-media");
                if cause == "no-admissible-media-credential" {
                    row.categories = vec!["llm-provider".into()];
                }
                let mut credentials = crate::broca_credentials::BrocaCredentials::default();
                if cause != "listing-missing" {
                    credentials.apply_listing(
                        &super::super::listing(vec![row], cause != "listing-grant-missing"),
                        if cause == "listing-stale" {
                            1
                        } else {
                            NOW as u64
                        },
                    );
                }
                if cause == "listing-failed" {
                    credentials.note_failure("synthetic listing failure");
                }
                credentials.catalog_snapshot = harness
                    .credentials()
                    .lock()
                    .unwrap()
                    .catalog_snapshot
                    .clone();
                harness.set_credentials(credentials);
            }
        }
        let phone = harness.bind(reserved("callosum"), None).await;
        let mut captured = Samples::default();
        captured
            .capture_method(
                "reachability-unavailable",
                &harness,
                &phone,
                "route.select",
                &voice_request(),
                Some(("no_route", Some(json!({"reason":"no_available_model"})))),
            )
            .await;
        let (name, pair) = captured.0.pop_first().unwrap();
        if let Some(previous) = samples.0.get(&name) {
            // These causes intentionally share one sample only while every wire byte agrees.
            assert_eq!(previous, &pair, "reachability cause {cause}");
        } else {
            samples.0.insert(name, pair);
        }
    }

    let harness = fixture();
    let phone = harness.bind(reserved("callosum"), None).await;
    // Removing an actual selection table exercises storage failure through the handler.
    rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .execute("DROP TABLE routing_engine_eval", [])
        .unwrap();
    samples
        .capture_method(
            "store-error",
            &harness,
            &phone,
            "route.select",
            &voice_request(),
            Some(("store_error", None)),
        )
        .await;
}

#[tokio::test]
async fn production_phone_voice_offer_uses_equal_catalog_grade_without_price() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let _ids = striatum_store::replay::Inputs::new(NOW).install();
    let harness = fixture();
    let phone = harness.bind(reserved("callosum"), None).await;
    capture_phone_voice(&mut Samples::default(), &harness, &phone).await;
}
