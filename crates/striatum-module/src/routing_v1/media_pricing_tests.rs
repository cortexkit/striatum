//! Prices use copied pinned rows; modified rows below are function-test inputs,
//! not claims that the catalog serves additional units or billing shapes.

use super::*;

fn row(route: &str) -> Value {
    let payloads: Value =
        serde_json::from_str(include_str!("../../fixtures/served-payloads.json")).unwrap();
    payloads
        .as_object()
        .unwrap()
        .values()
        .map(|payload| &payload["response"]["models"][route])
        .find(|row| {
            row.get("existence").is_some()
                && row
                    .as_object()
                    .is_some_and(|row| row.keys().any(|key| key.starts_with("rate.")))
        })
        .expect("pinned rate-bearing row")
        .clone()
}

fn speech() -> Value {
    row("elevenlabs/eleven_flash_v2_5")
}
fn tokens() -> Value {
    row("google/gemini-3.8-flash-tts")
}
fn audio() -> Value {
    row("openai/gpt-transcribe")
}

fn install(h: &Harness, rows: &[(&str, Value)]) -> Arc<crate::fusiform_catalog::CatalogSnapshot> {
    let models: serde_json::Map<_, _> = rows
        .iter()
        .map(|(model, row)| (format!("media-vendor/{model}"), row.clone()))
        .collect();
    let snapshot = Arc::new(
        crate::fusiform_catalog::decode_catalog_get(
            &serde_json::to_vec(&json!({
                "catalog_version":1,"resolved_at_ms":1,"models":models
            }))
            .unwrap(),
        )
        .unwrap(),
    );
    h.credentials().lock().unwrap().catalog_snapshot = Some(snapshot.clone());
    snapshot
}

fn offer(model: &str, kind: &str, local: bool) -> Value {
    let mut value = engine(model, local);
    value["kind"] = json!(kind);
    value["max_input_bytes"] = json!(100000);
    value["max_duration_ms"] = json!(100000);
    value
}

fn job(kind: &str) -> Value {
    json!({"consumer":"colliculus","kind":kind,"privacy":"remote_allowed",
        "requirements":{"input_bytes":8,"duration_ms":1000}})
}

fn score(h: &Harness, answer: &Value) -> Value {
    let decision = h
        .store()
        .get_decision_by_id(answer["decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    serde_json::from_str(&decision.score_json).unwrap()
}

fn near(actual: &Value, expected: f64) {
    assert!(
        (actual.as_f64().expect("priced number") - expected).abs() < 1e-12,
        "{actual} != {expected}"
    );
}

#[tokio::test]
async fn media_cost_speech_utf8_ceiling_ranks_before_tokens_and_round_trips_price() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![
                offer("a-tokens", "synthesize", false),
                offer("z-characters", "synthesize", false),
            ]);
            install(&h, &[("a-tokens", tokens()), ("z-characters", speech())]);
            let route = h.bind(reserved("colliculus"), None).await;
            let text = "hé🙂";
            let mut params = job("synthesize");
            params["requirements"]["input_bytes"] = json!(text.len());
            for level in [None, Some(40)] {
                if let Some(level) = level {
                    params["requirements"]["iq"] = json!(level);
                }
                let answer = select(&route, &params).await;
                assert_eq!(answer["engine"]["model"], "z-characters");
                let expected = text.len() as f64 * 0.00004;
                near(&score(&h, &answer)["cost"], expected);
                near(&answer["price"]["estimated_task_usd"], expected);
                near(
                    &answer["price"]["rates"]["input.per_character"]["rate"],
                    0.00004,
                );
                assert!(expected >= text.chars().count() as f64 * 0.00004);
                assert_eq!(answer["price"]["basis"], "metered");
                assert_eq!(
                    answer["price"]["rates"]["input.per_character"]
                        .as_object()
                        .unwrap()
                        .len(),
                    1
                );
                assert_eq!(answer["price"].as_object().unwrap().len(), 3);
                assert_eq!(
                    score(&h, &answer)["objective"]["taskPricing"]["basis"],
                    "bytes_as_characters_ceiling"
                );
                let reproduced = answer["price"]["rates"]["input.per_character"]["rate"]
                    .as_f64()
                    .unwrap()
                    * text.len() as f64;
                near(&score(&h, &answer)["cost"], reproduced);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_cost_all_incurred_rates_are_required_and_missing_quantity_quotes_units() {
    super::super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![offer("speech", "synthesize", false)]);
        let route = h.bind(reserved("colliculus"), None).await;
        let params = job("synthesize");
        install(&h, &[("speech", speech())]);
        near(&score(&h, &select(&route, &params).await)["cost"], 0.00032);
        // A measurable charge plus a known zero sums to the measurable charge.
        for state in ["stated_zero", "not_billed"] {
            let mut synthetic = speech();
            synthetic["rate.output_audio"] = json!({"state":state});
            install(&h, &[("speech", synthetic)]);
            let answer = select(&route, &params).await;
            near(&score(&h, &answer)["cost"], 0.00032);
            assert_eq!(answer["price"]["rates"].as_object().unwrap().len(), 1);
        }
        for state in ["priced", "unpriced", "billed_as", "future"] {
            let mut synthetic = speech();
            synthetic["rate.output_audio"] = tokens()["rate.output_audio"].clone();
            synthetic["rate.output_audio"]["state"] = json!(state);
            if state == "billed_as" { synthetic["rate.output_audio"]["key"] = json!("rate.input.per_character"); }
            install(&h, &[("speech", synthetic)]);
            let answer = select(&route, &params).await;
            assert!(answer.get("price").is_none());
            assert_eq!(score(&h, &answer)["cost"], Value::Null, "{state}");
        }
        install(&h, &[("speech", speech())]);
        let mut missing = params.clone();
        missing["requirements"].as_object_mut().unwrap().remove("input_bytes");
        let answer = select(&route, &missing).await;
        assert!(answer["price"]["rates"].get("input.per_character").is_some());
        assert!(answer["price"].get("estimated_task_usd").is_none());
        near(&score(&h, &answer)["cost"], 0.00004);
        for synthetic in [json!({"existence":"present","model.type":"speech"}), json!({"existence":"present","model.type":"speech","rate.output":{"state":"stated_zero"}})] {
            install(&h, &[("speech", synthetic)]);
            let answer = select(&route, &params).await;
            assert!(answer.get("price").is_none());
            assert_eq!(score(&h, &answer)["cost"], Value::Null);
        }
    }).await;
}

#[tokio::test]
async fn media_cost_closed_pair_table_never_infers_roles_units_or_tokens() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![offer("unit", "synthesize", false)]);
            install(&h, &[("unit", speech())]);
            let route = h.bind(reserved("colliculus"), None).await;
            assert!(select(&route, &job("synthesize"))
                .await
                .get("price")
                .is_some());
            for kind in ["synthesize", "image", "video", "realtime"] {
                super::declare(kind, &[offer("unit", kind, false)]);
                for key in [
                    "rate.output.per_character",
                    "rate.input_audio.per_second",
                    "rate.output.per_video_second",
                    "rate.input.per_byte",
                    "rate.output.per_image",
                ] {
                    let mut synthetic = speech();
                    let rate = synthetic
                        .as_object_mut()
                        .unwrap()
                        .remove("rate.input.per_character")
                        .unwrap();
                    synthetic[key] = rate;
                    install(&h, &[("unit", synthetic)]);
                    let answer = select(&route, &job(kind)).await;
                    assert!(answer.get("price").is_none(), "{kind} {key}");
                    assert_eq!(score(&h, &answer)["cost"], Value::Null, "{kind} {key}");
                }
                install(&h, &[("unit", tokens())]);
                let answer = select(&route, &job(kind)).await;
                assert!(answer.get("price").is_none());
                assert_eq!(score(&h, &answer)["cost"], Value::Null);
                if kind != "synthesize" {
                    install(&h, &[("unit", speech())]);
                    let answer = select(&route, &job(kind)).await;
                    assert_eq!(score(&h, &answer)["cost"], Value::Null);
                }
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_cost_quantity_rules_fail_closed_and_log_once_per_model_rate() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![offer("quantity-log", "synthesize", false)]);
            let route = h.bind(reserved("colliculus"), None).await;
            install(&h, &[("quantity-log", speech())]);
            assert!(select(&route, &job("synthesize"))
                .await
                .get("price")
                .is_some());
            for field in ["increment", "minimum_quantity"] {
                for shape in [
                    json!({"kind":"stated","quantity":1000}),
                    json!({"kind":"future"}),
                ] {
                    let mut synthetic = speech();
                    synthetic["rate.input.per_character"][field] = shape;
                    // Even a later unmeasurable charge must not suppress logging.
                    synthetic["rate.output_audio"] = tokens()["rate.output_audio"].clone();
                    synthetic["rate.output_audio"][field] = json!({"kind":"future"});
                    install(&h, &[("quantity-log", synthetic)]);
                    for _ in 0..2 {
                        let answer = select(&route, &job("synthesize")).await;
                        assert!(answer.get("price").is_none());
                        assert_eq!(score(&h, &answer)["cost"], Value::Null);
                    }
                }
            }
            let mut unspecified = speech();
            unspecified["rate.input.per_character"]
                .as_object_mut()
                .unwrap()
                .remove("increment");
            unspecified["rate.input.per_character"]
                .as_object_mut()
                .unwrap()
                .remove("minimum_quantity");
            install(&h, &[("quantity-log", unspecified)]);
            near(
                &score(&h, &select(&route, &job("synthesize")).await)["cost"],
                0.00032,
            );
        },
    )
    .await;
}

#[tokio::test]
async fn media_cost_local_and_caller_offers_never_emit_catalog_price() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![offer("speech", "synthesize", false)]);
            install(&h, &[("speech", speech())]);
            let route = h.bind(reserved("colliculus"), None).await;
            assert!(select(&route, &job("synthesize"))
                .await
                .get("price")
                .is_some());
            super::declare("synthesize", &[offer("local", "synthesize", true)]);
            let answer = select(&route, &job("synthesize")).await;
            assert!(answer.get("price").is_none());
            assert_eq!(score(&h, &answer)["cost"], Value::Null);
            install(&h, &[("local", speech())]);
            let answer = select(&route, &job("synthesize")).await;
            near(&score(&h, &answer)["cost"], 0.00032);
            assert!(answer.get("price").is_none());
            let mut params = job("synthesize");
            params["privacy"] = json!("local_only");
            params["caller_local"] = json!([offer("speech", "synthesize", true)]);
            super::declare("synthesize", &[]);
            install(&h, &[("speech", speech())]);
            let answer = select(&route, &params).await;
            assert_eq!(answer["engine"]["caller_local"], true);
            assert!(answer.get("price").is_none());
            assert_eq!(score(&h, &answer)["cost"], 0.0);
        },
    )
    .await;
}

#[tokio::test]
async fn media_cost_transcription_unit_price_normalizes_and_ignores_extra_rates_and_rules() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![offer("audio", "transcribe", false)]);
            let route = h.bind(reserved("colliculus"), None).await;
            for kind in ["transcribe", "stream_transcribe"] {
                super::declare(kind, &[offer("audio", kind, false)]);
                let mut synthetic = audio();
                synthetic["rate.output"] = tokens()["rate.output_audio"].clone();
                synthetic["rate.input_audio.per_second"]["increment"] =
                    json!({"kind":"stated","quantity":60});
                synthetic["rate.input_audio.per_second"]["minimum_quantity"] =
                    json!({"kind":"future"});
                let catalog = install(&h, &[("audio", synthetic)]);
                let mut params = job(kind);
                params["requirements"]["duration_ms"] = json!(60000);
                let answer = select(&route, &params).await;
                near(
                    &answer["price"]["rates"]["input_audio.per_second"]["rate"],
                    0.000075,
                );
                let emitted = &answer["price"]["rates"]["input_audio.per_second"];
                assert_eq!(emitted.as_object().unwrap().len(), 1);
                near(&score(&h, &answer)["cost"], 0.0045);
                near(
                    &score(&h, &answer)["cost"],
                    emitted["rate"].as_f64().unwrap() * 60.0,
                );
                let parsed =
                    cortexkit_role_routing::select_engine::parse_select_engine_request(&params)
                        .unwrap();
                near(
                    &score(&h, &answer)["cost"],
                    catalog
                        .engine_cost(&parsed, "media-vendor", "audio")
                        .unwrap(),
                );
                let legacy = super::super::selected(&route, &params).await;
                near(&score(&h, &legacy)["cost"], 0.0045);
                assert!(legacy.get("price").is_none());
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_cost_legacy_speech_ranking_stays_unpriced() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![
                offer("a-tokens", "synthesize", false),
                offer("z-characters", "synthesize", false),
            ]);
            install(&h, &[("a-tokens", tokens()), ("z-characters", speech())]);
            let route = h.bind(reserved("colliculus"), None).await;
            let answer = super::super::selected(&route, &job("synthesize")).await;
            assert_eq!(answer["engine"]["model"], "a-tokens");
            assert_eq!(score(&h, &answer)["cost"], Value::Null);
        },
    )
    .await;
}

#[tokio::test]
async fn media_cost_legacy_speech_answer_keeps_three_members_without_price() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![offer("speech", "synthesize", false)]);
            install(&h, &[("speech", speech())]);
            let route = h.bind(reserved("colliculus"), None).await;
            let answer = super::super::selected(&route, &job("synthesize")).await;
            assert!(answer.get("price").is_none());
            assert_eq!(answer.as_object().unwrap().len(), 3);
        },
    )
    .await;
}

#[tokio::test]
async fn media_cost_transcription_parity_preserves_base_token_lookup() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![offer("audio", "transcribe", false)]);
            install(&h, &[("audio", row("google/gemini-3.5-transcribe-live"))]);
            let route = h.bind(reserved("colliculus"), None).await;
            for kind in ["transcribe", "stream_transcribe"] {
                super::declare(kind, &[offer("audio", kind, false)]);
                let params = job(kind);
                let media = select(&route, &params).await;
                let legacy = super::super::selected(&route, &params).await;
                near(&score(&h, &media)["cost"], 0.0000875);
                assert_eq!(score(&h, &media)["cost"], score(&h, &legacy)["cost"]);
                assert!(media.get("price").is_none());
                assert!(legacy.get("price").is_none());
            }
        },
    )
    .await;
}
