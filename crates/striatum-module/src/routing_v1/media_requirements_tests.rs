//! Requirements bound both the usable engines and the grade scale before ranking.

use super::*;

fn no_route_detail(value: &Value) {
    assert_eq!(value["detail"], json!({"reason":"no_available_model"}));
}

#[tokio::test]
async fn media_requirements_eligible_subset_binds_all_kinds_pins_and_ladders() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![]);
            let route = h.bind(reserved("colliculus"), None).await;
            for kind in super::super::super::engines::MEDIA_KINDS {
                let engines: Vec<_> = ["a", "z"]
                    .into_iter()
                    .map(|model| {
                        let mut engine = engine(model, false);
                        engine["kind"] = json!(kind);
                        engine
                    })
                    .collect();
                declare(kind, &engines);
                let mut params = request();
                params["kind"] = json!(kind);
                assert_eq!(select(&route, &params).await["engine"]["model"], "a");
                params["eligible_routes"] = json!([
                    {"provider":"media-vendor","model":"z"},
                    {"provider":"media-vendor","model":"z"}
                ]);
                let answer = select(&route, &params).await;
                assert_eq!(answer["engine"]["model"], "z");
                let parsed = cortexkit_role_routing::select_media::parse_select_media_request(
                    &params,
                    &super::super::super::describe::capabilities(),
                    super::super::super::engines::MEDIA_KINDS,
                )
                .unwrap();
                check_answer(&parsed, &serde_json::from_value(answer).unwrap()).unwrap();
                params["pin"] = json!("media-vendor/a");
                let refusal = refuse(&route, &params, "pin_unavailable", None).await;
                assert_eq!(
                    refusal["detail"],
                    json!({"pin":"media-vendor/a","reason":"not_available"})
                );
                params.as_object_mut().unwrap().remove("pin");
                h.edit_config(|config| {
                    config["consumers"]["colliculus"]["ladder"] = json!(["media-vendor/a"]);
                    config["consumers"]["colliculus"]["mode"] = json!("ladder");
                });
                no_route_detail(&refuse(&route, &params, "no_route", None).await);
                h.edit_config(|config| {
                    config["consumers"]["colliculus"]
                        .as_object_mut()
                        .unwrap()
                        .remove("ladder");
                    config["consumers"]["colliculus"]
                        .as_object_mut()
                        .unwrap()
                        .remove("mode");
                });
                // Media eligibility uses the contract's provider/model identity;
                // an optional RouteRef variant does not invent an engine variant.
                params["eligible_routes"] =
                    json!([{"provider":"media-vendor","model":"z","variant":"fast"}]);
                assert_eq!(select(&route, &params).await["engine"]["model"], "z");
                params["eligible_routes"] = json!([]);
                no_route_detail(&refuse(&route, &params, "no_route", None).await);
            }
        },
    )
    .await;
}

async fn narrows_requirement(field: &str, wanted: Value, fact: &str, supported: Value) {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let mut capable = engine("z-capable", false);
            capable[fact] = supported;
            let h = setup(vec![engine("a-missing", false), capable]);
            let route = h.bind(reserved("colliculus"), None).await;
            assert_eq!(
                select(&route, &request()).await["engine"]["model"],
                "a-missing"
            );
            let mut params = request();
            params["requirements"][field] = wanted;
            assert_eq!(
                select(&route, &params).await["engine"]["model"],
                "z-capable"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn media_requirements_format_uses_contract_mime_matching() {
    narrows_requirement(
        "format",
        json!("audio/ogg; codecs=opus"),
        "formats",
        json!(["audio/ogg"]),
    )
    .await;
}

#[tokio::test]
async fn media_requirements_duration_requires_a_sufficient_declared_limit() {
    narrows_requirement("duration_ms", json!(1000), "max_duration_ms", json!(1000)).await;
}

#[tokio::test]
async fn media_requirements_latency_uses_contract_order() {
    narrows_requirement(
        "latency_class",
        json!("realtime"),
        "latency_class",
        json!("realtime"),
    )
    .await;
}

#[tokio::test]
async fn media_requirements_privacy_and_caller_local_offers_narrow_candidates() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("a-remote", false)]);
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = request();
            params["caller_local"] = json!([engine("z-phone", true)]);
            assert_eq!(select(&route, &params).await["engine"]["model"], "a-remote");
            params["privacy"] = json!("local_only");
            let answer = select(&route, &params).await;
            assert_eq!(answer["engine"], json!({"provider":"media-vendor","model":"z-phone", "local":true,"caller_local":true,"index":0}));
            params["caller_local"] = json!([]);
            no_route_detail(&refuse(&route, &params, "no_route", None).await);
        },
    ).await;
}

#[tokio::test]
async fn media_requirements_metric_floor_excludes_higher_grade_and_unmeasured_routes() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("high", false), engine("low", false), engine("unknown", false)]);
            ranking::catalog(&h, &[
                ("high", "transcription", Some(100.0), None),
                ("low", "transcription", Some(60.0), None),
                ("unknown", "transcription", Some(110.0), None),
            ]);
            ranking::measure(&h, "high", 0.7);
            ranking::measure(&h, "low", 0.9);
            let route = h.bind(reserved("colliculus"), None).await;
            assert_eq!(select(&route, &request()).await["engine"]["model"], "unknown");
            let mut params = request();
            // Engine evaluations store higher-is-better scores, including one minus
            // word error rate (WER) for transcription.
            // The floor must use that same score, not the catalog grade or ranking metric.
            params["requirements"] = json!({"metric":"unmeasured-ranking", "metric_limit":{"metric":"wer","at_least":0.8}});
            assert_eq!(select(&route, &params).await["engine"]["model"], "low");
            params["requirements"]["metric_limit"] = json!({"metric":"wer","at_most":0.8});
            assert_eq!(select(&route, &params).await["engine"]["model"], "high");
            params["requirements"]["metric_limit"] = json!({"metric":"wer","at_least":0.95});
            no_route_detail(&refuse(&route, &params, "no_route", None).await);
        },
    ).await;
}

#[tokio::test]
async fn media_requirements_failed_route_is_outside_top_and_settings_pin() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let mut top = ranking::engine("top", false);
            top["formats"] = json!(["audio/ogg"]);
            let h = setup(vec![
                top,
                ranking::engine("middle", false),
                ranking::engine("cheap", false),
            ]);
            ranking::catalog(
                &h,
                &[
                    ("top", "transcription", Some(100.0), Some(3)),
                    ("middle", "transcription", Some(60.0), Some(2)),
                    ("cheap", "transcription", Some(30.0), Some(1)),
                ],
            );
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = ranking::job();
            params["requirements"]["iq"] = json!(40);
            assert_eq!(select(&route, &params).await["engine"]["model"], "middle");
            params["requirements"]["format"] = json!("audio/wav");
            // Removing grade 100 makes the bar 24 rather than 40, admitting grade 30.
            assert_eq!(select(&route, &params).await["engine"]["model"], "cheap");
            h.edit_config(|config| {
                config["consumers"]["colliculus"]["pin"] = json!({"model":"media-vendor/top"})
            });
            assert_eq!(
                refuse(&route, &params, "pin_unavailable", None).await["detail"],
                json!({"pin":"media-vendor/top","reason":"not_available"})
            );
        },
    )
    .await;
}

#[tokio::test]
async fn media_requirements_exhaustion_keeps_contract_no_route_detail() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for cause in ["format", "privacy", "policy", "fence", "reachability"] {
                let h = setup(vec![engine("only", false)]);
                let route = h.bind(reserved("colliculus"), None).await;
                let mut params = request();
                match cause {
                    "format" => params["requirements"]["format"] = json!("audio/ogg"),
                    "privacy" => params["privacy"] = json!("local_only"),
                    "policy" => h.edit_config(|config| {
                        config["models"]["media-vendor/only"] = json!({"not_for":["colliculus"]})
                    }),
                    "fence" => ranking::fence(&h, "only"),
                    "reachability" => h.set_credentials(media_credentials(vec![])),
                    _ => unreachable!(),
                }
                no_route_detail(&refuse(&route, &params, "no_route", None).await);
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_requirements_request_pin_bypasses_only_model_pin_only() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("a-auto", false), engine("z-pin", false)]);
            let route = h.bind(reserved("colliculus"), None).await;
            for rules in [
                json!({"pin_only":true}),
                json!({"pin_only_for":["colliculus"]}),
            ] {
                h.edit_config(|config| config["models"]["media-vendor/z-pin"] = rules);
                let mut params = request();
                assert_eq!(select(&route, &params).await["engine"]["model"], "a-auto");
                params["pin"] = json!("media-vendor/z-pin");
                assert_eq!(select(&route, &params).await["engine"]["model"], "z-pin");
                params["requirements"]["format"] = json!("audio/ogg");
                assert_eq!(
                    refuse(&route, &params, "pin_unavailable", None).await["detail"]["reason"],
                    "not_available"
                );
            }
            for rules in [
                json!({"only_for":["other"]}),
                json!({"not_for":["colliculus"]}),
            ] {
                h.edit_config(|config| config["models"]["media-vendor/z-pin"] = rules);
                let mut params = request();
                params["pin"] = json!("media-vendor/z-pin");
                assert_eq!(
                    refuse(&route, &params, "pin_unavailable", None).await["detail"],
                    json!({"pin":"media-vendor/z-pin","reason":"excluded"})
                );
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_requirements_select_engine_retains_legacy_filter_and_refusal() {
    let h = setup(vec![engine("only", false)]);
    let route = h.bind(reserved("colliculus"), None).await;
    assert_eq!(
        selected(&route, &request()).await["engine"]["model"],
        "only"
    );
    let mut params = request();
    params["requirements"]["format"] = json!("audio/ogg");
    let reply = route.call("route.select_engine", &params).await;
    assert_eq!(reply.0, FrameType::Error);
    assert_eq!(reply.1["code"], "no_engine");
    assert_eq!(reply.1["detail"], json!({"filter":"requirements"}));
    params["requirements"] = json!({"metric_limit":{"metric":"wer","at_least":0.8}});
    let reply = route.call("route.select_engine", &params).await;
    assert_eq!(reply.0, FrameType::Error);
    assert_eq!(reply.1["code"], "invalid_requirements");
    assert_eq!(reply.1["detail"]["field"], "metric_limit");
}
