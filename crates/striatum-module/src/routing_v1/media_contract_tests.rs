//! Catalog-backed media behavior through the real routing handler.

use super::*;

fn catalog(h: &Harness, models: Value) {
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({
            "catalog_version":1,"resolved_at_ms":1,
            "quality_scores":{"graded_by":"operator","graded_at_ms":1},"models":models
        }))
        .unwrap(),
    )
    .unwrap();
    h.credentials().lock().unwrap().catalog_snapshot = Some(snapshot);
}

fn video(model: &str) -> Value {
    let mut value = engine(model, true);
    value["kind"] = json!("video");
    value
}

fn video_row() -> Value {
    json!({"existence":"present","model.type":"video","quality.grade":80,
        "limit.output_duration_ms":1000,"capability.aspect_ratios":["16:9"],"capability.output_audio":true})
}

async fn video_choice(field: &str, invalid: Value, required: Value) {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![video("a-invalid"), video("z-valid")]);
            let mut bad = video_row();
            bad["quality.grade"] = json!(100);
            bad[field] = invalid;
            catalog(
                &h,
                json!({"media-vendor/a-invalid":bad,"media-vendor/z-valid":video_row()}),
            );
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = request();
            params["kind"] = json!("video");
            params["requirements"] = required;
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-valid");
        },
    )
    .await;
}

#[tokio::test]
async fn video_output_duration_uses_the_output_limit_not_input_duration() {
    video_choice(
        "limit.output_duration_ms",
        json!(999),
        json!({"output_duration_ms":1000}),
    )
    .await;
}

#[tokio::test]
async fn video_aspect_ratio_requires_an_exact_catalog_string() {
    video_choice(
        "capability.aspect_ratios",
        json!(["32:18"]),
        json!({"aspect_ratio":"16:9"}),
    )
    .await;
}

#[tokio::test]
async fn video_audio_matches_the_catalog_boolean_in_both_directions() {
    video_choice(
        "capability.output_audio",
        json!(false),
        json!({"audio":true}),
    )
    .await;
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![video("silent")]);
            let mut row = video_row();
            row["capability.output_audio"] = json!(false);
            catalog(&h, json!({"media-vendor/silent":row}));
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = request();
            params["kind"] = json!("video");
            params["requirements"] = json!({"audio":false});
            assert_eq!(select(&route, &params).await["engine"]["model"], "silent");
        },
    )
    .await;
}

#[tokio::test]
async fn video_missing_and_malformed_facts_fail_closed_by_name() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![video("only")]);
            let route = h.bind(reserved("colliculus"), None).await;
            for (key, field, requirement, malformed) in [
                (
                    "limit.output_duration_ms",
                    "output_duration_ms",
                    json!(1000),
                    vec![json!(-1), json!(1.5), json!("1000"), Value::Null],
                ),
                (
                    "capability.aspect_ratios",
                    "aspect_ratio",
                    json!("16:9"),
                    vec![json!("16:9"), json!(["16:9", 3]), Value::Null],
                ),
                (
                    "capability.output_audio",
                    "audio",
                    json!(false),
                    vec![json!(0), json!("false"), Value::Null],
                ),
            ] {
                for invalid in std::iter::once(None).chain(malformed.into_iter().map(Some)) {
                    let mut row = video_row();
                    row.as_object_mut().unwrap().remove(key);
                    if let Some(value) = invalid {
                        row[key] = value;
                    }
                    catalog(&h, json!({"media-vendor/only":row}));
                    let mut params = request();
                    params["kind"] = json!("video");
                    params["requirements"] = json!({field:requirement});
                    let result = refuse(&route, &params, "no_route", None).await;
                    assert!(
                        result["message"].as_str().unwrap().contains(key),
                        "{result}"
                    );
                }
            }
        },
    )
    .await;
}

#[tokio::test]
async fn pinned_served_video_rows_still_refuse_all_unstated_output_facts() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let fixture: Value =
                serde_json::from_str(include_str!("../../fixtures/served-payloads.json")).unwrap();
            let mut checked = 0;
            for payload in fixture.as_object().unwrap().values() {
                let response = &payload["response"];
                let Some(models) = response["models"].as_object() else {
                    continue;
                };
                for (key, row) in models
                    .iter()
                    .filter(|(_, row)| row["model.type"] == "video")
                {
                    let (provider, model) = key.split_once('/').unwrap();
                    let mut declaration = video(model);
                    declaration["provider"] = json!(provider);
                    let h = setup(vec![declaration]);
                    let snapshot = crate::fusiform_catalog::decode_catalog_get(
                        &serde_json::to_vec(response).unwrap(),
                    )
                    .unwrap();
                    h.credentials().lock().unwrap().catalog_snapshot = Some(snapshot);
                    let route = h.bind(reserved("colliculus"), None).await;
                    for (field, value, fact) in [
                        ("output_duration_ms", json!(1), "limit.output_duration_ms"),
                        ("aspect_ratio", json!("16:9"), "capability.aspect_ratios"),
                        ("audio", json!(false), "capability.output_audio"),
                    ] {
                        assert!(
                            row.get(fact).is_none(),
                            "pinned fixture unexpectedly states {key} {fact}"
                        );
                        let mut params = request();
                        params["kind"] = json!("video");
                        params["requirements"] = json!({field:value});
                        let refusal = refuse(&route, &params, "no_route", None).await;
                        assert!(
                            refusal["message"].as_str().unwrap().contains(fact),
                            "{refusal}"
                        );
                        checked += 1;
                    }
                }
            }
            assert!(checked >= 3, "the pinned fixture must contain a video row");
        },
    )
    .await;
}

#[tokio::test]
async fn speech_prices_unicode_scalars_before_utf8_bytes_and_keeps_the_byte_fallback() {
    super::super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let mut speech = engine("speech", false);
        speech["kind"] = json!("synthesize");
        speech["max_input_bytes"] = json!(100);
        let h = setup(vec![speech]);
        catalog(&h, json!({"media-vendor/speech":{"existence":"present","model.type":"speech",
            "rate.input.per_character":{"state":"priced","currency":"USD","units":1,"exponent":0,"per":1}}}));
        let route = h.bind(reserved("colliculus"), None).await;
        let mut params = request();
        params["kind"] = json!("synthesize");
        // Four Unicode scalars occupy nine UTF-8 bytes in this text.
        let text = "é🙂á";
        params["requirements"] = json!({"input_characters":text.chars().count(),"input_bytes":text.len()});
        assert_eq!(select(&route, &params).await["price"]["estimated_task_usd"], 4.0);
        params["requirements"].as_object_mut().unwrap().remove("input_characters");
        assert_eq!(select(&route, &params).await["price"]["estimated_task_usd"], 9.0);
        params["requirements"]["input_characters"] = json!(0);
        assert_eq!(select(&route, &params).await["price"]["estimated_task_usd"], 0.0);
    }).await;
}

#[tokio::test]
async fn quantity_free_media_compares_matching_unit_rates_without_inventing_an_estimate() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![
                engine("a-expensive", false),
                engine("z-cheap", false),
                engine("unpriced", false),
            ]);
            ranking::catalog(
                &h,
                &[
                    ("a-expensive", "transcription", Some(80.0), Some(10)),
                    ("z-cheap", "transcription", Some(80.0), Some(1)),
                    ("unpriced", "transcription", Some(80.0), None),
                ],
            );
            ranking::measure(&h, "a-expensive", 0.9);
            ranking::measure(&h, "z-cheap", 0.8);
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = request();
            params["requirements"]["iq"] = json!(40);
            let result = select(&route, &params).await;
            assert_eq!(result["engine"]["model"], "z-cheap");
            assert_eq!(
                result["price"]["rates"],
                json!({"input.per_audio_second":{"rate":1.0}})
            );
            assert!(
                result["price"].get("estimated_task_usd").is_none(),
                "{result}"
            );
            params["pin"] = json!("media-vendor/unpriced");
            assert!(select(&route, &params).await.get("price").is_none());
        },
    )
    .await;
}

#[tokio::test]
async fn equal_quantity_free_unit_rates_prefer_only_a_known_lower_minimum() {
    super::super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        for (kind, model_type, rate_key) in [("transcribe","transcription","rate.input_audio.per_second"),("synthesize","speech","rate.input.per_character")] {
        let declarations = ["a-high","z-low"].iter().map(|model| { let mut value = engine(model,false); value["kind"] = json!(kind); value }).collect();
        let h = setup(declarations);
        ranking::measure(&h,"a-high",0.9);
        ranking::measure(&h,"z-low",0.8);
        let route = h.bind(reserved("colliculus"), None).await;
        let mut params = request();
        params["kind"] = json!(kind);
        params["requirements"]["iq"] = json!(40);
        for (low, expected) in [(json!({"kind":"minimum","quantity":10}),"z-low"),(json!({"kind":"none"}),"z-low"),(json!({"kind":"unknown"}),"a-high")] {
            let row = |minimum| json!({"existence":"present","model.type":model_type,"quality.grade":80,
                rate_key:{"state":"priced","currency":"USD","units":2,"exponent":0,"per":1,"minimum_quantity":minimum}});
            catalog(&h,json!({"media-vendor/a-high":row(json!({"kind":"minimum","quantity":30})),"media-vendor/z-low":row(low)}));
            assert_eq!(select(&route,&params).await["engine"]["model"],expected);
        }
        }
    }).await;
}

#[tokio::test]
async fn mixed_unknown_minima_keep_a_transitive_order_in_every_candidate_permutation() {
    super::super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        for order in [["a","b","c"],["a","c","b"],["b","a","c"],["b","c","a"],["c","a","b"],["c","b","a"]] {
            let h = setup(order.iter().map(|model| engine(model,false)).collect());
            let row = |minimum| json!({"existence":"present","model.type":"transcription","quality.grade":80,
                "rate.input_audio.per_second":{"state":"priced","currency":"USD","units":2,"exponent":0,"per":1,"minimum_quantity":minimum}});
            catalog(&h,json!({"media-vendor/a":row(json!({"kind":"minimum","quantity":30})),"media-vendor/b":row(json!({"kind":"unknown"})),"media-vendor/c":row(json!({"kind":"minimum","quantity":10}))}));
            for (model, score) in [("a",0.9),("b",0.8),("c",0.7)] { ranking::measure(&h,model,score); }
            let operator = h.bind(json!({"kind":"direct"}),None).await;
            let mut params = request();
            params["iqs"] = json!([40]);
            let (frame,reply) = operator.call("route.what_if",&params).await;
            assert_eq!(frame,FrameType::Response,"{reply}");
            let routes: Vec<_> = reply["result"]["results"][0]["adequate"].as_array().unwrap().iter().map(|entry| entry["route"].as_str().unwrap()).collect();
            assert_eq!(routes,vec!["media-vendor/a","media-vendor/b","media-vendor/c"],"declaration order: {order:?}");
            // Check the complete order, not only the selected winner: all six
            // permutations must preserve A < B, B < C and therefore A < C.
            let position = |route| routes.iter().position(|value| *value == route).unwrap();
            assert!(position("media-vendor/a") < position("media-vendor/b"));
            assert!(position("media-vendor/b") < position("media-vendor/c"));
            assert!(position("media-vendor/a") < position("media-vendor/c"));
        }
    }).await;
}

#[tokio::test]
async fn phone_voice_offer_uses_its_catalog_grade_and_zero_cost_but_never_a_price() {
    super::super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![ranking::engine("remote",false)]);
        let remote = json!({"existence":"present","model.type":"transcription","quality.grade":80,
            "rate.input_audio.per_second":{"state":"priced","currency":"USD","units":1,"exponent":0,"per":1}});
        catalog(&h,json!({"media-vendor/remote":remote}));
        let route = h.bind(reserved("colliculus"), None).await;
        let mut params = ranking::job();
        params["consumer"] = json!("colliculus.voice_message");
        params["requirements"]["iq"] = json!(40);
        params["caller_local"] = json!([ranking::engine("phone",true)]);
        assert_eq!(select(&route,&params).await["engine"]["model"],"remote");
        catalog(&h,json!({"media-vendor/remote":remote,"media-vendor/phone":{"existence":"present","model.type":"transcription","quality.grade":80}}));
        let with_row = select(&route,&params).await;
        assert_eq!(with_row["engine"]["model"],"phone");
        assert_eq!(with_row["engine"]["caller_local"],true);
        assert!(with_row.get("price").is_none());
    }).await;
}

#[tokio::test]
async fn ungraded_phone_offer_has_known_zero_cost_without_a_catalog_row() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("a-remote", false)]);
            ranking::catalog(&h, &[("a-remote", "transcription", None, Some(1))]);
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = request();
            params["requirements"]["iq"] = json!(75);
            params["caller_local"] = json!([engine("z-phone", true)]);
            let result = select(&route, &params).await;
            assert_eq!(result["engine"]["model"], "z-phone");
            assert!(result.get("price").is_none());
        },
    )
    .await;
}

#[tokio::test]
async fn quantity_bearing_transcription_keeps_its_existing_estimate_and_rate_spelling() {
    super::super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![ranking::engine("remote",false)]);
        ranking::catalog(&h,&[("remote","transcription",Some(80.0),Some(2))]);
        let route = h.bind(reserved("colliculus"), None).await;
        let answer = select(&route,&ranking::job()).await;
        assert_eq!(answer["price"],json!({"basis":"metered","rates":{"input_audio.per_second":{"rate":2.0}},"estimated_task_usd":2.0}));
    }).await;
}
