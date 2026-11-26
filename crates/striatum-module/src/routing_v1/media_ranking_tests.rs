//! Ranking fixtures use real catalog grades and prices, not injected rank scores.

use super::*;
use std::sync::Arc;

pub(super) fn engine(model: &str, local: bool) -> Value {
    let mut engine = super::engine(model, local);
    engine["max_duration_ms"] = json!(60000);
    engine
}

pub(super) fn catalog(
    h: &Harness,
    rows: &[(&str, &str, Option<f64>, Option<i64>)],
) -> Arc<crate::fusiform_catalog::CatalogSnapshot> {
    let models: serde_json::Map<String, Value> = rows
        .iter()
        .map(|(model, kind, grade, cost)| {
            let mut row = json!({"existence":"present", "model.type":kind});
            if let Some(grade) = grade {
                row[format!("quality.grade.{kind}")] = json!(grade);
            }
            if let Some(cost) = cost {
                row["rate.input_audio.per_second"] =
                    json!({"state":"priced","currency":"USD","units":cost,"exponent":0,"per":1});
            }
            (
                if model.contains('/') {
                    (*model).into()
                } else {
                    format!("media-vendor/{model}")
                },
                row,
            )
        })
        .collect();
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({
            "catalog_version":1,"resolved_at_ms":1,
            "quality_scores":{"graded_by":"operator","graded_at_ms":1},"models":models
        }))
        .unwrap(),
    )
    .unwrap();
    h.credentials().lock().unwrap().catalog_snapshot = Some(snapshot.clone());
    snapshot
}

pub(super) fn job() -> Value {
    let mut params = request();
    params["requirements"]["duration_ms"] = json!(1000);
    params
}

pub(super) fn measure(h: &Harness, model: &str, score: f64) {
    h.store()
        .record_engine_evaluation(&striatum_store::EngineEvaluation {
            kind: "transcribe".into(),
            provider: "media-vendor".into(),
            model: model.into(),
            metric: "wer".into(),
            score,
            set_id: "same".into(),
            measured_at_ms: 100,
        })
        .unwrap();
}

pub(super) fn fence(h: &Harness, model: &str) {
    let key = format!("media-vendor/{model}");
    crate::record_cooldown_update(h.store(), &serde_json::from_value(json!({
        "routeKey":key,"capacityKey":key,"cooldownUntil":crate::now_ms()+60000,"lastError":"HTTP 503"
    })).unwrap()).unwrap();
}

#[tokio::test]
async fn media_ranking_grade_then_measurement_without_level_and_cost_with_level() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![
                engine("a", false),
                engine("z", false),
                engine("ungraded", false),
            ]);
            catalog(
                &h,
                &[
                    ("a", "transcription", Some(80.0), Some(1)),
                    ("z", "transcription", Some(80.0), Some(2)),
                    ("ungraded", "transcription", None, Some(0)),
                ],
            );
            measure(&h, "a", 0.8);
            measure(&h, "z", 0.9);
            measure(&h, "ungraded", 1.0);
            let route = h.bind(reserved("colliculus"), None).await;
            assert_eq!(select(&route, &job()).await["engine"]["model"], "z");
            let mut params = job();
            params["requirements"]["iq"] = json!(40);
            assert_eq!(select(&route, &params).await["engine"]["model"], "a");
            catalog(
                &h,
                &[
                    ("a", "transcription", Some(90.0), Some(1)),
                    ("z", "transcription", Some(80.0), Some(2)),
                ],
            );
            assert_eq!(select(&route, &job()).await["engine"]["model"], "a");
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_linear_bar_survives_route_fence() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![
                engine("a-top", false),
                engine("b-low", false),
                engine("z-middle", false),
            ]);
            catalog(
                &h,
                &[
                    ("a-top", "transcription", Some(100.0), Some(3)),
                    ("b-low", "transcription", Some(30.0), Some(1)),
                    ("z-middle", "transcription", Some(60.0), Some(2)),
                ],
            );
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = job();
            params["requirements"]["iq"] = json!(40);
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-middle");
            fence(&h, "a-top");
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-middle");
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_bar_survives_account_route_fence_and_pool_wall() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for route_fence in [false, true] {
                let mut top = engine("top", false);
                top["provider"] = json!("high-provider");
                let h = setup(vec![top, engine("a-low", false), engine("z-middle", false)]);
                let mut top_key = key("high-key");
                top_key.provider_ids = vec!["high-provider".into()];
                h.set_credentials(media_credentials(vec![key("opaque-media"), top_key]));
                catalog(
                    &h,
                    &[
                        ("high-provider/top", "transcription", Some(100.0), Some(3)),
                        ("a-low", "transcription", Some(30.0), Some(1)),
                        ("z-middle", "transcription", Some(60.0), Some(2)),
                    ],
                );
                let now = crate::now_ms() as i64;
                if route_fence {
                    h.store()
                        .record_account_route_fence(&striatum_store::AccountRouteFence {
                            provider: "high-provider".into(),
                            credential_id: "high-key".into(),
                            model: "top".into(),
                            until_ms: now + 60000,
                            decision_id: String::new(),
                            recorded_at: now,
                        })
                        .unwrap();
                } else {
                    h.store()
                        .pool_wall(
                            "high-provider",
                            "high-key",
                            striatum_store::PoolWallKind::Billing,
                            now + 60000,
                            "insufficient_quota",
                            now,
                        )
                        .unwrap();
                }
                let route = h.bind(reserved("colliculus"), None).await;
                let mut params = job();
                params["requirements"]["iq"] = json!(40);
                assert_eq!(select(&route, &params).await["engine"]["model"], "z-middle");
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_resolves_each_level_source_without_llm_defaults() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let engines = vec![engine("a-top", false), engine("z-cheap", false)];
            let h = setup(engines.clone());
            declare("transcribe", &engines);
            catalog(
                &h,
                &[
                    ("a-top", "transcription", Some(100.0), Some(2)),
                    ("z-cheap", "transcription", Some(60.0), Some(1)),
                ],
            );
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = job();
            params["consumer"] = json!("colliculus.voice_message");
            h.edit_config(|config| config["consumers"]["colliculus"]["iq"] = json!(90));
            params["requirements"]["iq"] = json!(40);
            assert_eq!(select(&route, &params).await["engine"]["model"], "a-top");
            h.edit_config(|config| {
                config["consumers"]["colliculus"]
                    .as_object_mut()
                    .unwrap()
                    .remove("iq");
            });
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-cheap");
            params["requirements"].as_object_mut().unwrap().remove("iq");
            // The declaration's IQ is 40. Its language-model context/output
            // limits and tools requirement must not constrain a media engine.
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-cheap");
            h.edit_config(|config| {
                config["consumers"]["colliculus.voice_message"] = json!({"iq":95})
            });
            assert_eq!(select(&route, &params).await["engine"]["model"], "a-top");
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_below_bar_refuses_mason_and_best_effort_prefers_graded() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("a-top", false), engine("z-low", false)]);
            h.edit_config(|config| {
                config["consumers"]["prefrontal.mason"] = json!({"owner":"colliculus"})
            });
            catalog(
                &h,
                &[
                    ("a-top", "transcription", Some(100.0), Some(2)),
                    ("z-low", "transcription", Some(30.0), Some(1)),
                ],
            );
            fence(&h, "a-top");
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = job();
            params["requirements"]["iq"] = json!(80);
            params["caller_local"] = json!([engine("phone", true)]);
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-low");
            params["consumer"] = json!("prefrontal.mason.worker");
            refuse(&route, &params, "no_route", None).await;
            h.edit_config(|config| {
                config["consumers"]["prefrontal.mason"]["below_bar"] = json!("best_effort")
            });
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-low");
            params["privacy"] = json!("local_only");
            assert_eq!(select(&route, &params).await["engine"]["model"], "phone");
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_unknown_grade_measurement_and_price_sort_last() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for iq in [None, Some(40)] {
                let h = setup(vec![engine("a-unknown", false), engine("z-known", false)]);
                let route = h.bind(reserved("colliculus"), None).await;
                let mut params = job();
                if let Some(iq) = iq {
                    params["requirements"]["iq"] = json!(iq);
                }
                catalog(
                    &h,
                    &[
                        ("a-unknown", "transcription", None, Some(0)),
                        ("z-known", "transcription", Some(80.0), Some(2)),
                    ],
                );
                assert_eq!(select(&route, &params).await["engine"]["model"], "z-known");
                catalog(
                    &h,
                    &[
                        ("a-unknown", "transcription", Some(80.0), Some(2)),
                        ("z-known", "transcription", Some(80.0), Some(2)),
                    ],
                );
                measure(&h, "z-known", 0.8);
                assert_eq!(select(&route, &params).await["engine"]["model"], "z-known");
                catalog(
                    &h,
                    &[
                        ("a-unknown", "transcription", Some(80.0), None),
                        ("z-known", "transcription", Some(80.0), Some(2)),
                    ],
                );
                measure(&h, "a-unknown", 0.8);
                assert_eq!(select(&route, &params).await["engine"]["model"], "z-known");
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_all_ungraded_uses_cost_before_measurement_with_or_without_level() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("a-accurate", false), engine("z-cheap", false)]);
            catalog(
                &h,
                &[
                    ("a-accurate", "transcription", None, Some(2)),
                    ("z-cheap", "transcription", None, Some(1)),
                ],
            );
            measure(&h, "a-accurate", 0.99);
            measure(&h, "z-cheap", 0.5);
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = job();
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-cheap");
            params["requirements"]["iq"] = json!(100);
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-cheap");
            h.credentials().lock().unwrap().catalog_snapshot = None;
            assert_eq!(
                select(&route, &params).await["engine"]["model"],
                "a-accurate"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_stream_reads_transcription_grades_with_its_own_top() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let mut stream = engine("z-stream", false);
            stream["kind"] = json!("stream_transcribe");
            let mut low = engine("a-low", false);
            low["kind"] = json!("stream_transcribe");
            let h = setup(vec![engine("transcribe-top", false), stream, low]);
            catalog(
                &h,
                &[
                    ("transcribe-top", "transcription", Some(100.0), Some(3)),
                    ("z-stream", "transcription", Some(60.0), Some(2)),
                    ("a-low", "transcription", Some(30.0), Some(1)),
                ],
            );
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = job();
            params["kind"] = json!("stream_transcribe");
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-stream");
            params["requirements"]["iq"] = json!(40);
            assert_eq!(select(&route, &params).await["engine"]["model"], "a-low");
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_top_obeys_static_keys_policy_privacy_and_requirements() {
    super::super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let mut missing_key = engine("a-no-key",false); missing_key["provider"] = json!("absent-provider");
        let h = setup(vec![missing_key,engine("z-top",false),engine("z-cheap",false)]);
        // The fixture lists a media API key for media-vendor, but none whose
        // provider coverage includes absent-provider. Its row cannot set the scale.
        let wire: Value = json!({"catalog_version":2,"resolved_at_ms":1,"quality_scores":{"graded_by":"operator","graded_at_ms":1},"models":{
            "absent-provider/a-no-key":{"existence":"present","model.type":"transcription","quality.grade.transcription":1000},
            "media-vendor/z-top":{"existence":"present","model.type":"transcription","quality.grade.transcription":100,"rate.input_audio.per_second":{"state":"priced","currency":"USD","units":2,"exponent":0,"per":1}},
            "media-vendor/z-cheap":{"existence":"present","model.type":"transcription","quality.grade.transcription":30,"rate.input_audio.per_second":{"state":"priced","currency":"USD","units":1,"exponent":0,"per":1}}
        }});
        h.credentials().lock().unwrap().catalog_snapshot = Some(crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&wire).unwrap()).unwrap());
        let route = h.bind(reserved("colliculus"),None).await;
        let mut params = job(); params["requirements"]["iq"] = json!(40);
        assert_eq!(select(&route,&params).await["engine"]["model"],"z-top");
        h.edit_config(|config| config["models"]["media-vendor/z-top"] = json!({"not_for":["colliculus"]}));
        assert_eq!(select(&route,&params).await["engine"]["model"],"z-cheap");
        h.edit_config(|config| { config["models"].as_object_mut().unwrap().remove("media-vendor/z-top"); });
        measure(&h,"z-top",0.5); measure(&h,"z-cheap",0.9);
        params["requirements"]["metric_limit"] = json!({"metric":"wer","at_least":0.8});
        assert_eq!(select(&route,&params).await["engine"]["model"],"z-cheap");
        params["requirements"].as_object_mut().unwrap().remove("metric_limit");
        params["privacy"] = json!("local_only"); params["caller_local"] = json!([engine("z-top",true)]);
        assert_eq!(select(&route,&params).await["engine"]["caller_local"],true);
    }).await;
}

#[test]
fn media_ranking_join_is_reused_and_invalidated_by_either_catalog() {
    let engines = vec![engine("a", false)];
    let h = setup(engines.clone());
    catalog(&h, &[("a", "transcription", Some(80.0), None)]);
    let join = || super::super::super::consumers::media_routes(&h.credentials().lock().unwrap());
    let joined = join();
    assert_eq!(joined.by_kind["transcribe"].grades["media-vendor/a"], 80.0);
    assert!(Arc::ptr_eq(&joined, &join()));
    declare("transcribe", &engines);
    assert!(Arc::ptr_eq(&joined, &join()));
    catalog(&h, &[("a", "transcription", Some(90.0), None)]);
    let regraded = join();
    assert!(!Arc::ptr_eq(&joined, &regraded));
    assert_eq!(
        regraded.by_kind["transcribe"].grades["media-vendor/a"],
        90.0
    );
    declare("transcribe", &[engine("b", false)]);
    let redeclared = join();
    assert!(!Arc::ptr_eq(&regraded, &redeclared));
    assert_eq!(redeclared.by_kind["transcribe"].routes[0].engine.model, "b");
    assert!(redeclared.by_kind["transcribe"].grades.is_empty());
}

#[tokio::test]
async fn media_ranking_caller_offer_inherits_its_kind_specific_catalog_grade() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("a-shared", false), engine("z-local", true)]);
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = job();
            params["requirements"]["iq"] = json!(100);
            params["privacy"] = json!("local_only");
            params["caller_local"] = json!([engine("a-shared", true)]);
            assert_eq!(select(&route, &params).await["engine"]["model"], "a-shared");
            catalog(
                &h,
                &[
                    ("a-shared", "transcription", Some(100.0), Some(1)),
                    ("z-local", "transcription", Some(60.0), None),
                ],
            );
            assert_eq!(select(&route, &params).await["engine"]["model"], "a-shared");
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_excluded_local_engine_neither_sets_bar_nor_wins() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![
                engine("a-excluded", true),
                engine("b-top", false),
                engine("z-cheap", false),
            ]);
            catalog(
                &h,
                &[
                    ("a-excluded", "transcription", Some(1000.0), None),
                    ("b-top", "transcription", Some(100.0), Some(3)),
                    ("z-cheap", "transcription", Some(60.0), Some(1)),
                ],
            );
            h.edit_config(|config| {
                config["models"]["media-vendor/a-excluded"] = json!({"not_for":["colliculus"]});
                config["consumers"]["colliculus"]["below_bar"] = json!("refuse");
            });
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = job();
            params["requirements"]["iq"] = json!(40);
            params["caller_local"] = json!([engine("a-excluded", true)]);
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-cheap");
            params["requirements"].as_object_mut().unwrap().remove("iq");
            assert_eq!(select(&route, &params).await["engine"]["model"], "b-top");
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_each_nontranscribe_kind_uses_only_its_grade_kind() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for (kind, row_type) in [
                ("synthesize", "speech"),
                ("realtime", "realtime"),
                ("image", "image"),
                ("video", "video"),
            ] {
                let mut a = engine("a-wrong-type", false);
                a["kind"] = json!(kind);
                let mut z = engine("z-graded", false);
                z["kind"] = json!(kind);
                let h = setup(vec![a, z]);
                catalog(
                    &h,
                    &[
                        ("a-wrong-type", "transcription", Some(1000.0), None),
                        ("z-graded", row_type, Some(60.0), None),
                    ],
                );
                let route = h.bind(reserved("colliculus"), None).await;
                let mut params = request();
                params["kind"] = json!(kind);
                assert_eq!(
                    select(&route, &params).await["engine"]["model"],
                    if kind == "realtime" {
                        "a-wrong-type"
                    } else {
                        "z-graded"
                    }
                );
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_mirrors_kit_grade_and_ungraded_level_cases() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let engines = vec![engine("model-c", false), engine("model-d", false)];
            let h = setup(engines.clone());
            let route = h.bind(reserved("colliculus"), None).await;
            catalog(
                &h,
                &[
                    ("model-c", "transcription", Some(400.0), Some(100)),
                    ("model-d", "transcription", Some(400.0), Some(1)),
                ],
            );
            measure(&h, "model-c", 0.9);
            measure(&h, "model-d", 0.5);
            let mut params = job();
            assert_eq!(select(&route, &params).await["engine"]["model"], "model-c");
            catalog(
                &h,
                &[
                    ("model-c", "transcription", None, Some(100)),
                    ("model-d", "transcription", Some(400.0), Some(1)),
                ],
            );
            assert_eq!(select(&route, &params).await["engine"]["model"], "model-d");
            params["requirements"]["metric_limit"] = json!({"metric":"wer","at_least":0.8});
            assert_eq!(select(&route, &params).await["engine"]["model"], "model-c");
            params["requirements"]["metric_limit"] = json!({"metric":"wer","at_most":0.6});
            assert_eq!(select(&route, &params).await["engine"]["model"], "model-d");
            params = job();
            params["requirements"]["iq"] = json!(75);
            catalog(
                &h,
                &[
                    ("model-c", "transcription", None, Some(10)),
                    ("model-d", "transcription", None, Some(1)),
                ],
            );
            assert_eq!(select(&route, &params).await["engine"]["model"], "model-d");
            declare("transcribe", &engines);
            params["requirements"].as_object_mut().unwrap().remove("iq");
            catalog(
                &h,
                &[
                    ("model-c", "transcription", Some(400.0), Some(10)),
                    ("model-d", "transcription", Some(200.0), Some(1)),
                ],
            );
            assert_eq!(select(&route, &params).await["engine"]["model"], "model-d");
            params["requirements"]["iq"] = json!(75);
            params["caller_local"] = json!([engine("caller", true)]);
            assert_eq!(select(&route, &params).await["engine"]["model"], "model-c");
            declare("transcribe", &[]);
            assert_eq!(select(&route, &params).await["engine"]["model"], "caller");
        },
    )
    .await;
}

#[tokio::test]
async fn media_ranking_legacy_entry_ignores_grade_level_and_below_bar() {
    let h = setup(vec![engine("a-grade", false), engine("z-accurate", false)]);
    catalog(
        &h,
        &[
            ("a-grade", "transcription", Some(100.0), Some(1)),
            ("z-accurate", "transcription", Some(30.0), Some(2)),
        ],
    );
    measure(&h, "a-grade", 0.5);
    measure(&h, "z-accurate", 0.9);
    h.edit_config(|config| {
        config["consumers"]["colliculus"]["iq"] = json!(100);
        config["consumers"]["colliculus"]["below_bar"] = json!("refuse");
    });
    let route = h.bind(reserved("colliculus"), None).await;
    assert_eq!(
        selected(&route, &job()).await["engine"]["model"],
        "z-accurate"
    );
}
