//! Preview assertions use the same handler and catalog as live media selection.

use super::ranking::{catalog, engine, fence, job, measure};
use super::*;

async fn operator(h: &Arc<Harness>) -> Route {
    h.bind(json!({"kind":"direct"}), None).await
}

async fn preview(route: &Route, params: &Value) -> Value {
    let (frame, value) = route.call("route.what_if", params).await;
    assert_eq!(frame, FrameType::Response, "{value}");
    value["result"].clone()
}

async fn invalid(route: &Route, params: &Value, field: &str) {
    let (frame, value) = route.call("route.what_if", params).await;
    assert_eq!(frame, FrameType::Error, "{value}");
    assert_eq!(value["code"], "invalid_request", "{value}");
    assert_eq!(value["detail"]["field"], field, "{value}");
}

fn routes(row: &Value) -> Vec<&str> {
    row["adequate"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["route"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn media_preview_no_level_reports_top_grade_and_null_unknowns() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![
                engine("high", false),
                engine("low", false),
                engine("unknown", false),
            ]);
            catalog(
                &h,
                &[
                    ("high", "transcription", Some(80.0), Some(2)),
                    ("low", "transcription", Some(60.0), Some(1)),
                ],
            );
            let op = operator(&h).await;
            let answer = preview(&op, &job()).await;
            assert_eq!(
                answer
                    .as_object()
                    .unwrap()
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>(),
                [
                    "anchor_version",
                    "concurrency",
                    "consumer",
                    "elapsed_ms",
                    "results",
                    "snapshot_ms"
                ]
            );
            assert_eq!(answer["results"].as_array().unwrap().len(), 1);
            let row = &answer["results"][0];
            assert_eq!(row["iq"], Value::Null);
            assert_eq!(row["bar"], json!({}));
            assert_eq!(row["top_routable"], json!({"grade":80.0}));
            assert_eq!(
                routes(row),
                [
                    "media-vendor/high",
                    "media-vendor/low",
                    "media-vendor/unknown"
                ]
            );
            assert_eq!(
                row["pick"],
                json!({"route":"media-vendor/high", "grade":80.0, "cost_usd":2.0})
            );
            assert_eq!(
                row["adequate"][2],
                json!({"route":"media-vendor/unknown", "grade":null, "cost_usd":null})
            );
            catalog(
                &h,
                &[
                    ("high", "transcription", None, Some(2)),
                    ("low", "transcription", None, Some(1)),
                ],
            );
            let answer = preview(&op, &job()).await;
            assert_eq!(answer["results"][0]["bar"], json!({}));
            assert_eq!(answer["results"][0]["top_routable"], json!({}));
            assert_eq!(answer["results"][0]["pick"]["route"], "media-vendor/low");
        },
    )
    .await;
}

#[tokio::test]
async fn media_preview_sweeps_exact_levels_with_settings_and_declaration_precedence() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let engines = vec![engine("high", false), engine("cheap", false)];
            let h = setup(engines.clone());
            catalog(
                &h,
                &[
                    ("high", "transcription", Some(80.0), Some(2)),
                    ("cheap", "transcription", Some(60.0), Some(1)),
                ],
            );
            let op = operator(&h).await;
            let live = h.bind(reserved("colliculus"), None).await;
            let mut params = job();
            params["requirements"]["iq"] = json!(100);
            params["iqs"] = json!([0, 50.5, 100]);
            let answer = preview(&op, &params).await;
            for (index, level, bar, pick) in [
                (0, 0.0, 0.0, "cheap"),
                (1, 50.5, 40.4, "cheap"),
                (2, 100.0, 80.0, "high"),
            ] {
                let row = &answer["results"][index];
                assert_eq!(row["iq"], level);
                assert_eq!(row["bar"]["grade"], bar);
                assert_eq!(row["pick"]["route"], format!("media-vendor/{pick}"));
            }
            h.edit_config(|config| config["consumers"]["colliculus"]["iq"] = json!(100));
            let answer = preview(&op, &params).await;
            for row in answer["results"].as_array().unwrap() {
                assert_eq!(row["iq"], 100.0);
                assert_eq!(row["pick"]["route"], "media-vendor/high");
            }
            params.as_object_mut().unwrap().remove("iqs");
            let selected = select(&live, &params).await;
            assert_eq!(selected["engine"]["model"], "high");
            assert_eq!(
                preview(&op, &params).await["results"][0]["pick"]["route"],
                "media-vendor/high"
            );
            h.edit_config(|config| {
                config["consumers"]["colliculus"]
                    .as_object_mut()
                    .unwrap()
                    .remove("iq");
            });
            declare("transcribe", &engines);
            params["requirements"] = json!({"duration_ms":1000});
            let answer = preview(&op, &params).await;
            assert_eq!(answer["results"][0]["iq"], 40.0);
            assert_eq!(answer["results"][0]["pick"]["route"], "media-vendor/cheap");
            assert_eq!(select(&live, &params).await["engine"]["model"], "cheap");
        },
    )
    .await;
}

#[tokio::test]
async fn media_preview_closed_members_bounds_and_contract_validation() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("ok", false)]);
            let op = operator(&h).await;
            assert!(preview(&op, &request()).await["results"][0]["pick"].is_object());
            for field in [
                "caller_local",
                "eq",
                "runner",
                "policy_patch",
                "pricing_patch",
                "runner_declarations",
                "panel",
                "head_choices",
                "pin",
                "exclude",
                "exclude_providers",
                "eligible_routes",
            ] {
                let mut params = request();
                params[field] = Value::Null;
                invalid(&op, &params, field).await;
            }
            for iqs in [
                json!([]),
                json!(vec![0; 102]),
                json!([-1]),
                json!([100.1]),
                json!([null]),
                json!(["NaN"]),
                Value::Null,
            ] {
                let mut params = request();
                params["iqs"] = iqs;
                invalid(&op, &params, "iqs").await;
            }
            let mut params = request();
            params["iqs"] = json!(vec![50; 101]);
            assert_eq!(
                preview(&op, &params).await["results"]
                    .as_array()
                    .unwrap()
                    .len(),
                101
            );
            params = request();
            params.as_object_mut().unwrap().remove("privacy");
            invalid(&op, &params, "privacy").await;
            params = request();
            params["requirements"]["tools"] = json!("required");
            let (frame, reply) = op.call("route.what_if", &params).await;
            assert_eq!(frame, FrameType::Error);
            assert_eq!(reply["code"], "invalid_requirements");
            for kind in ["search", "transcription", "llm"] {
                params = request();
                params["kind"] = json!(kind);
                invalid(&op, &params, "kind").await;
            }
            declare("image", &[engine("ok", false)]);
            let answer = preview(&op, &request()).await;
            assert_eq!(answer["results"][0]["refusal"]["code"], "invalid_request");
            assert_eq!(answer["results"][0]["refusal"]["detail"]["field"], "kind");
        },
    )
    .await;
}

#[tokio::test]
async fn media_preview_operator_registration_and_ownership_admission() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("ok", false)]);
            let op = operator(&h).await;
            assert_eq!(
                preview(&op, &request()).await["results"][0]["pick"]["route"],
                "media-vendor/ok"
            );
            let callosum = h.bind(reserved("callosum"), None).await;
            assert_eq!(
                preview(&callosum, &request()).await["results"][0]["pick"]["route"],
                "media-vendor/ok"
            );
            let owner = h.bind(reserved("colliculus"), None).await;
            let (frame, reply) = owner.call("route.what_if", &request()).await;
            assert_eq!(frame, FrameType::Error);
            assert_eq!(reply["code"], "not_permitted");
            let mut params = request();
            params["consumer"] = json!("unregistered.consumer");
            invalid(&op, &params, "consumer").await;
        },
    )
    .await;
}

#[tokio::test]
async fn media_preview_exclusions_bar_refusals_and_measurement_order_match_select() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![
                engine("top", false),
                engine("best", false),
                engine("cheap", false),
                engine("bad", false),
            ]);
            catalog(
                &h,
                &[
                    ("top", "transcription", Some(100.0), Some(3)),
                    ("best", "transcription", Some(60.0), Some(2)),
                    ("cheap", "transcription", Some(60.0), Some(1)),
                    ("bad", "transcription", Some(30.0), Some(0)),
                ],
            );
            measure(&h, "best", 0.9);
            measure(&h, "cheap", 0.8);
            fence(&h, "top");
            let op = operator(&h).await;
            let mut params = job();
            params["iqs"] = json!([40, 100]);
            let answer = preview(&op, &params).await;
            let row = &answer["results"][0];
            assert_eq!(row["top_routable"]["grade"], 100.0);
            assert_eq!(row["bar"]["grade"], 40.0);
            assert_eq!(routes(row), ["media-vendor/cheap", "media-vendor/best"]);
            assert_eq!(row["pick"]["route"], "media-vendor/cheap");
            assert_eq!(row["excluded"][0]["route"], "media-vendor/top");
            assert_eq!(answer["results"][1]["pick"]["route"], "media-vendor/best");
            assert_eq!(answer["results"][1]["adequate"], json!([]));
            h.edit_config(|config| {
                config["consumers"]["colliculus"]["below_bar"] = json!("refuse")
            });
            let answer = preview(&op, &params).await;
            assert_eq!(answer["results"][1]["pick"], Value::Null);
            assert_eq!(answer["results"][1]["refusal"]["code"], "no_route");
            params = job();
            params["requirements"]["metric_limit"] = json!({"metric":"wer", "at_least":0.85});
            let answer = preview(&op, &params).await;
            assert_eq!(answer["results"][0]["pick"]["route"], "media-vendor/best");
            assert_eq!(
                answer["results"][0]["excluded"].as_array().unwrap().len(),
                3
            );
            params = job();
            params["privacy"] = json!("local_only");
            let answer = preview(&op, &params).await;
            assert_eq!(answer["results"][0]["pick"], Value::Null);
            assert_eq!(answer["results"][0]["refusal"]["code"], "no_route");
            assert_eq!(
                answer["results"][0]["excluded"].as_array().unwrap().len(),
                4
            );
        },
    )
    .await;
}

#[tokio::test]
async fn media_preview_settings_pins_ladders_and_model_policy() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("high", false), engine("cheap", false)]);
            catalog(
                &h,
                &[
                    ("high", "transcription", Some(80.0), Some(2)),
                    ("cheap", "transcription", Some(60.0), Some(1)),
                ],
            );
            h.edit_config(|config| {
                config["models"] = json!({"media-vendor/high":{"pin_only":true}});
            });
            let op = operator(&h).await;
            assert_eq!(
                preview(&op, &job()).await["results"][0]["pick"]["route"],
                "media-vendor/cheap"
            );
            h.edit_config(|config| {
                config["consumers"]["colliculus"]["pin"] = json!({"model":"media-vendor/high"})
            });
            assert_eq!(
                preview(&op, &job()).await["results"][0]["pick"]["route"],
                "media-vendor/high"
            );
            fence(&h, "high");
            let answer = preview(&op, &job()).await;
            assert_eq!(answer["results"][0]["pick"], Value::Null);
            assert_eq!(answer["results"][0]["refusal"]["code"], "pin_unavailable");
            h.edit_config(|config| {
                config["consumers"]["colliculus"]
                    .as_object_mut()
                    .unwrap()
                    .remove("pin");
                config["consumers"]["colliculus"]["mode"] = json!("ladder");
                config["consumers"]["colliculus"]["ladder"] =
                    json!(["media-vendor/high", "media-vendor/cheap"]);
            });
            assert_eq!(
                preview(&op, &job()).await["results"][0]["pick"]["route"],
                "media-vendor/cheap"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn media_preview_account_healing_and_decisions_stay_disposable() {
    super::super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![engine("ok", false)]);
        catalog(&h, &[("ok", "transcription", Some(80.0), Some(1))]);
        measure(&h, "ok", 0.9);
        let op = operator(&h).await;
        rusqlite::Connection::open(h.store_path().with_file_name("history.db")).unwrap().execute_batch("INSERT INTO account_size_refusal VALUES ('media-vendor','old','ok',17,1,0); INSERT INTO credential_entitlement_fences VALUES ('media-vendor','removed','auth_required','old-listing',0);").unwrap();
        let before = super::super::super::what_if_tests::rows(&h);
        let batches = h.store().account_writer_batch_sizes_for_test();
        let credentials = format!("{:?}", h.credentials().lock().unwrap().listed_credentials());
        let mut params = job(); params["iqs"] = json!([0, 50, 100]);
        assert_eq!(preview(&op, &params).await["results"][0]["pick"]["route"], "media-vendor/ok");
        assert_eq!(super::super::super::what_if_tests::rows(&h), before);
        assert_eq!(h.store().account_writer_batch_sizes_for_test(), batches);
        assert_eq!(format!("{:?}", h.credentials().lock().unwrap().listed_credentials()), credentials);
    }).await;
}

#[tokio::test]
async fn media_preview_all_six_kinds_match_select_without_a_level() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for kind in [
                "transcribe",
                "stream_transcribe",
                "synthesize",
                "realtime",
                "image",
                "video",
            ] {
                let mut offered = engine("ok", false);
                offered["kind"] = json!(kind);
                let h = setup(vec![offered]);
                let op = operator(&h).await;
                let live = h.bind(reserved("colliculus"), None).await;
                let mut params = request();
                params["kind"] = json!(kind);
                let row = preview(&op, &params).await["results"][0].clone();
                assert_eq!(row["iq"], Value::Null);
                assert_eq!(
                    row["pick"],
                    json!({"route":"media-vendor/ok", "grade":null, "cost_usd":null})
                );
                assert_eq!(select(&live, &params).await["engine"]["model"], "ok");
                h.set_credentials(media_credentials(vec![]));
                let row = preview(&op, &params).await["results"][0].clone();
                assert_eq!(row["pick"], Value::Null);
                assert_eq!(row["refusal"]["code"], "no_route");
                assert_eq!(
                    row["excluded"][0],
                    json!({"route":"media-vendor/ok", "reason":"no eligible media credential"})
                );
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_preview_speech_job_cost_and_decommissioned_exclusions() {
    super::super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let mut offered = engine("speech", false); offered["kind"] = json!("synthesize");
        offered["max_input_bytes"] = json!(100000);
        let h = setup(vec![offered]);
        let payloads: Value = serde_json::from_str(include_str!("../../fixtures/served-payloads.json")).unwrap();
        let row = payloads.as_object().unwrap().values().map(|payload| &payload["response"]["models"]["elevenlabs/eleven_flash_v2_5"]).find(|row| row.get("rate.input.per_character").is_some()).unwrap().clone();
        let install = |row: Value| {
            let snapshot = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&json!({
                "catalog_version":1,"resolved_at_ms":1,"models":{"media-vendor/speech":row}
            })).unwrap()).unwrap();
            h.credentials().lock().unwrap().catalog_snapshot = Some(snapshot);
        };
        install(row.clone());
        let op = operator(&h).await;
        let live = h.bind(reserved("colliculus"), None).await;
        let params = json!({"consumer":"colliculus", "kind":"synthesize", "privacy":"remote_allowed", "requirements":{"input_bytes":8}});
        let previewed = preview(&op, &params).await;
        assert_eq!(previewed["results"][0]["pick"]["route"], "media-vendor/speech", "{previewed}");
        let expected = 8.0 * 0.00004;
        assert!((previewed["results"][0]["pick"]["cost_usd"].as_f64().unwrap() - expected).abs() < 1e-12);
        assert!((select(&live, &params).await["price"]["estimated_task_usd"].as_f64().unwrap() - expected).abs() < 1e-12);
        let mut retired = row; retired["quality.decommissioned"] = json!(true); install(retired);
        let previewed = preview(&op, &params).await;
        assert_eq!(previewed["results"][0]["pick"], Value::Null);
        assert_eq!(previewed["results"][0]["excluded"], json!([{"route":"media-vendor/speech","reason":"decommissioned"}]));
    }).await;
}

#[tokio::test]
async fn media_preview_excluded_contains_only_removed_route_identities() {
    super::super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let mut wrong_format = engine("shared", false);
        wrong_format["formats"] = json!(["audio/ogg"]);
        let h = setup(vec![wrong_format.clone(), engine("local", true)]);
        super::super::super::consumers::replace_catalog(&json!([
            {"module_id":"colliculus","nonce_verified":true,"role_declarations":{"media-provider/v1":{"engines":[wrong_format, engine("local", true)]}}},
            {"module_id":"other","nonce_verified":true,"role_declarations":{"media-provider/v1":{"engines":[engine("shared", false)]}}}
        ]));
        catalog(&h, &[("shared", "transcription", Some(80.0), Some(1))]);
        fence(&h, "local");
        let op = operator(&h).await;
        let mut params = job(); params["requirements"]["format"] = json!("audio/wav");
        let row = preview(&op, &params).await["results"][0].clone();
        assert_eq!(routes(&row), ["media-vendor/shared", "media-vendor/local"]);
        assert_eq!(row["excluded"], json!([]));
        let live = h.bind(reserved("colliculus"), None).await;
        assert_eq!(select(&live, &params).await["engine"]["model"], "shared");
    }).await;
}
