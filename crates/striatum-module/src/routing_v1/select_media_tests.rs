//! The optional media entry shares engine admission, not LLM request defaults.

use super::*;
use cortexkit_role_routing::select_media::{check_answer, SelectMediaAnswer};

#[path = "media_ranking_tests.rs"]
mod ranking;

#[path = "media_pricing_tests.rs"]
mod pricing;

#[path = "media_policy_tests.rs"]
mod policy;

async fn select(route: &Route, params: &Value) -> Value {
    let (frame, value) = route.call("route.select", params).await;
    assert_eq!(frame, FrameType::Response, "{value}");
    value["result"].clone()
}

async fn refuse(route: &Route, params: &Value, code: &str, field: Option<&str>) -> Value {
    let (frame, value) = route.call("route.select", params).await;
    assert_eq!(frame, FrameType::Error, "{value}");
    assert_eq!(value["code"], code, "{value}");
    if let Some(field) = field {
        assert_eq!(value["detail"]["field"], field, "{value}");
    }
    value
}

fn declare(kind: &str, engines: &[Value]) {
    super::super::consumers::replace_catalog(&json!([
        {"module_id":"colliculus","nonce_verified":true,"role_declarations":{
            "media-provider/v1":{"engines":engines},
            "routing/v1":{"consumers":[{"name":"colliculus", "kind":kind, "iq":40,
                "eq":70,"context_window":999999,"max_output_tokens":999999,"tools":"required"}]}
        }}
    ]));
}

#[tokio::test]
async fn media_entry_selects_all_six_kinds_without_catalog_or_billing() {
    super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let kinds = &[
            "transcribe",
            "stream_transcribe",
            "synthesize",
            "realtime",
            "image",
            "video",
        ];
        let engines: Vec<_> = kinds
            .iter()
            .map(|kind| {
                let mut value = engine(kind, false);
                value["kind"] = json!(kind);
                value
            })
            .collect();
        let h = setup(engines.clone());
        let route = h.bind(reserved("colliculus"), None).await;
        for kind in kinds {
            declare(kind, &engines);
            let mut params = request();
            params["kind"] = json!(kind);
            params["requirements"]["iq"] = json!(40);
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(1000),
                select(&route, &params),
            )
            .await
            .expect("media must not wait for the first billing load");
            assert_eq!(result["engine"]["model"], *kind);
            assert_eq!(result.as_object().unwrap().len(), 3);
            let parsed = cortexkit_role_routing::select_media::parse_select_media_request(
                &params,
                &super::super::describe::capabilities(),
                kinds,
            )
            .unwrap();
            let answer: SelectMediaAnswer = serde_json::from_value(result).unwrap();
            check_answer(&parsed, &answer).unwrap();
            params.as_object_mut().unwrap().remove("kind");
            let answer = select(&route, &params).await;
            let row = h
                .store()
                .get_decision_by_id(answer["decision_id"].as_str().unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&row.requirements_json).unwrap()["kind"],
                *kind
            );
        }
    })
    .await;
}

#[tokio::test]
async fn media_entry_refuses_unserved_kinds_and_cross_kind_requests() {
    super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![engine("remote", false)]);
        let route = h.bind(reserved("colliculus"), None).await;
        for kind in ["transcription", "speech", "search", "future-engine"] {
            let mut params = request();
            params["kind"] = json!(kind);
            refuse(&route, &params, "invalid_request", Some("kind")).await;
        }
        declare("search", &[]);
        let mut params = request();
        params.as_object_mut().unwrap().remove("kind");
        refuse(&route, &params, "invalid_request", Some("kind")).await;
        declare("transcription", &[engine("remote", false)]);
        assert_eq!(select(&route, &params).await["engine"]["model"], "remote");
        params["kind"] = json!("image");
        refuse(&route, &params, "invalid_request", Some("kind")).await;
        declare("image", &[engine("wrong-kind", false)]);
        let refusal = refuse(&route, &params, "no_route", None).await;
        assert_eq!(refusal["detail"], json!({"reason":"no_available_model"}));
    })
    .await;
}

#[tokio::test]
async fn media_entry_uses_closed_media_requirements_and_missing_video_facts() {
    super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![engine("remote", false)]);
        let route = h.bind(reserved("colliculus"), None).await;
        for field in [
            "eq",
            "context_tokens",
            "context_window",
            "max_output_tokens",
            "tools",
            "modalities",
            "structured_output",
            "input_characters",
        ] {
            let mut params = request();
            params["requirements"][field] = json!(10);
            refuse(&route, &params, "invalid_requirements", Some(field)).await;
        }
        let mut params = request();
        params["eq"] = json!(50);
        refuse(&route, &params, "invalid_request", Some("eq")).await;
        params = request();
        params.as_object_mut().unwrap().remove("privacy");
        refuse(&route, &params, "invalid_request", Some("privacy")).await;
        let mut video = engine("video", false);
        video["kind"] = json!("video");
        declare("video", &[video]);
        params = request();
        params["kind"] = json!("video");
        assert_eq!(select(&route, &params).await["engine"]["model"], "video");
        for (field, value) in [
            ("output_duration_ms", json!(1000)),
            ("aspect_ratio", json!("16:9")),
            ("audio", json!(false)),
        ] {
            params["requirements"] = json!({field: value});
            let refusal = refuse(&route, &params, "no_route", None).await;
            assert_eq!(refusal["detail"], json!({"reason":"missing_capability"}));
            assert!(refusal["message"].as_str().unwrap().contains(field));
        }
    })
    .await;
}

#[tokio::test]
async fn media_entry_applies_caller_local_admission_privacy_requirements_and_fences() {
    super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![engine("remote", false)]);
        let owner = h.bind(reserved("callosum"), None).await;
        let foreign = h.bind(reserved("stranger"), None).await;
        let mut params = request();
        params["consumer"] = json!("phone.media");
        params["privacy"] = json!("local_only");
        params["caller_local"] = json!([engine("phone", true)]);
        params["requirements"]["language"] = json!("en-US");
        let answer = select(&owner, &params).await;
        assert_eq!(answer["engine"]["caller_local"], true);
        assert_eq!(answer["engine"]["index"], 0);
        refuse(&foreign, &params, "consumer_not_owned", None).await;
        params["requirements"]["duration_ms"] = json!(1);
        refuse(&owner, &params, "no_route", None).await;
        params["requirements"] = json!({});
        params.as_object_mut().unwrap().remove("caller_local");
        refuse(&owner, &params, "no_route", None).await;
        let route = h.bind(reserved("colliculus"), None).await;
        h.edit_config(|config| {
            config["providers"]["media-vendor"] = json!({"not_for":["colliculus"]})
        });
        refuse(&route, &request(), "no_route", None).await;
        h.edit_config(|config| config["providers"]["media-vendor"] = json!({"pin_only":true}));
        assert_eq!(
            select(&route, &request()).await["engine"]["model"],
            "remote"
        );
        h.set_credentials(media_credentials(vec![]));
        refuse(&route, &request(), "no_route", None).await;
    })
    .await;
}

#[tokio::test]
async fn media_entry_ranks_measurement_before_identity_without_cost_and_enforces_metric_limit() {
    super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![engine("a", false), engine("z", false)]);
        let route = h.bind(reserved("colliculus"), None).await;
        facts(
            &h,
            &[
                ("a", "transcribe", None, Some(0.0)),
                ("z", "transcribe", None, Some(10.0)),
            ],
        );
        // Evaluation scores are higher-is-better, including 1 - word error rate.
        for (model, score) in [("a", 0.1), ("z", 0.9)] {
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
        assert_eq!(select(&route, &request()).await["engine"]["model"], "z");
        let mut params = request();
        params["requirements"]["metric_limit"] = json!({"metric":"wer", "at_least":0.95});
        refuse(&route, &params, "no_route", None).await;
        params["requirements"]["metric_limit"]["at_least"] = json!(0.8);
        assert_eq!(select(&route, &params).await["engine"]["model"], "z");
        params["requirements"]["metric_limit"]["metric"] = json!("missing");
        refuse(&route, &params, "no_route", None).await;
        params["requirements"] = json!({"metric":"missing"});
        facts(
            &h,
            &[
                ("a", "transcribe", None, Some(10.0)),
                ("z", "transcribe", None, Some(0.0)),
            ],
        );
        assert_eq!(select(&route, &params).await["engine"]["model"], "a");
        params["requirements"]["metric_limit"] = json!({"metric":"wer","at_least":0.8});
        assert_eq!(select(&route, &params).await["engine"]["model"], "z");
        let row = h
            .store()
            .get_decision_by_id(
                select(&route, &params).await["decision_id"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&row.score_json).unwrap()["cost"],
            Value::Null
        );
    })
    .await;
}

#[tokio::test]
async fn media_entry_persists_engine_decisions_and_reuses_account_and_report_binding() {
    super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![engine("remote", false)]);
        let route = h.bind(reserved("colliculus"), Some("stamped-project")).await;
        let answer = select(&route, &request()).await;
        let id = answer["decision_id"].as_str().unwrap();
        let row = h.store().get_decision_by_id(id).unwrap().unwrap();
        assert_eq!(row.decision_kind, "engine");
        assert_eq!(row.project_id.as_deref(), Some("stamped-project"));
        let binding = h.store().v1_decision(id).unwrap().unwrap();
        assert_eq!(binding.allowed_account_class, striatum_core::accounts::AllowedAccountClass::PricedPerTokenOnly);
        assert_eq!(serde_json::from_str::<Value>(&h.store().v1_selection_json(id).unwrap().unwrap()).unwrap(), answer);
        let mut params = json!({"admission_id":"first","conversation_key":"job","provider":"media-vendor","model":"remote","decision_id":id,"hold_wait_max_s":0});
        assert_eq!(route.call("route.account_for", &params).await.0, FrameType::Response);
        let mut llm_key = key("llm-only");
        llm_key.categories = vec!["llm-provider".into()];
        h.set_credentials(media_credentials(vec![llm_key]));
        params["admission_id"] = json!("llm-only");
        assert_eq!(route.call("route.account_for", &params).await.1["code"], "auth_required");
        let mut credentials = media_credentials(vec![key("opaque-media")]);
        credentials.engine_clock_offset_ms = 600001;
        h.set_credentials(credentials);
        params["admission_id"] = json!("expired");
        assert_eq!(route.call("route.account_for", &params).await.1["code"], "unknown_decision");
        // Removing the consumer after selection must not revoke an engine report.
        h.edit_config(|config| { config["consumers"] = json!({}); });
        super::super::consumers::replace_catalog(&json!([]));
        assert_eq!(route.call("route.report", &json!({"decision_id":id,"outcome":"completed"})).await.0, FrameType::Response);
        assert_eq!(route.call("route.explain", &json!({"decision_id":id})).await.1["code"], "unknown_decision");
    }).await;
}

#[tokio::test]
async fn media_entry_nontranscribe_has_no_default_metric_and_identity_precedes_module() {
    super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![]);
        let route = h.bind(reserved("colliculus"), None).await;
        for kind in ["stream_transcribe", "synthesize", "realtime", "image", "video"] {
            let mut a = engine("a", false);
            let mut z = engine("z", false);
            a["kind"] = json!(kind);
            z["kind"] = json!(kind);
            super::super::consumers::replace_catalog(&json!([
                {"module_id":"a-module","nonce_verified":true,"role_declarations":{"media-provider/v1":{"engines":[z]}}},
                {"module_id":"z-module","nonce_verified":true,"role_declarations":{"media-provider/v1":{"engines":[a]}}}
            ]));
            h.store().record_engine_evaluation(&striatum_store::EngineEvaluation {
                kind:kind.into(), provider:"media-vendor".into(), model:"z".into(),
                metric:"wer".into(), score:0.9, set_id:"same".into(), measured_at_ms:100,
            }).unwrap();
            let mut params = request();
            params["kind"] = json!(kind);
            assert_eq!(select(&route, &params).await["engine"]["model"], "a");
            params["requirements"]["metric"] = json!("wer");
            assert_eq!(select(&route, &params).await["engine"]["model"], "z");
        }
    }).await;
}

#[tokio::test]
async fn media_entry_reuses_decommissioned_and_route_fence_gates() {
    super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![engine("remote", false)]);
        let route = h.bind(reserved("colliculus"), None).await;
        assert_eq!(select(&route, &request()).await["engine"]["model"], "remote");
        crate::record_cooldown_update(h.store(), &serde_json::from_value(json!({
            "routeKey":"media-vendor/remote", "capacityKey":"media-vendor/remote",
            "cooldownUntil":crate::now_ms()+60000, "lastError":"HTTP 503"
        })).unwrap()).unwrap();
        refuse(&route, &request(), "no_route", None).await;
        for local in [false, true] {
            let h = setup(vec![engine("retired", local)]);
            let route = h.bind(reserved("colliculus"), None).await;
            let snapshot = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&json!({
                "catalog_version":94,"resolved_at_ms":123,"models":{
                    "media-vendor/retired":{"existence":"present","quality.decommissioned":true,"model.kind":"transcribe"}
                }
            })).unwrap()).unwrap();
            crate::fusiform_catalog::apply_mapped_catalog(h.store(), &snapshot, None, Some("media-vendor")).unwrap();
            refuse(&route, &request(), "no_route", None).await;
        }
    }).await;
}

#[tokio::test]
async fn media_entry_resolves_explicit_llm_and_refuses_media_members_on_llm() {
    super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let (_h, route) = super::super::dispatch_tests::configured().await;
        let mut params = json!({"consumer":cortexkit_role_routing_conformance::fixture::SUMMARY,
            "kind":"llm","requirements":{"iq":50}});
        let answer = select(&route, &params).await;
        assert!(answer["route"].is_object());
        assert!(answer["runner"].is_object());
        assert!(answer.get("engine").is_none());
        params["kind"] = Value::Null;
        assert!(select(&route, &params).await["route"].is_object());
        params["kind"] = json!("llm");
        for field in [
            "duration_ms",
            "input_bytes",
            "metric",
            "output_duration_ms",
            "aspect_ratio",
            "audio",
            "metric_limit",
        ] {
            params["requirements"] = json!({"iq":50,field:1});
            refuse(&route, &params, "invalid_requirements", Some(field)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn media_entry_keeps_omitted_llm_pick_and_answer_shape() {
    let (_h, route) = super::super::dispatch_tests::configured().await;
    let params = json!({"consumer":cortexkit_role_routing_conformance::fixture::SUMMARY,"requirements":{"iq":50}});
    let baseline = select(&route, &params).await;
    super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let answer = select(&route, &params).await;
        for field in ["route", "runner", "limits"] {
            assert_eq!(answer[field], baseline[field], "{field}");
        }
        assert!(answer.get("engine").is_none());
    })
    .await;
}

#[tokio::test]
async fn media_entry_keeps_legacy_engine_wire_cost_and_no_engine_refusal() {
    super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        let h = setup(vec![engine("a", false), engine("z", false)]);
        let route = h.bind(reserved("colliculus"), None).await;
        facts(
            &h,
            &[
                ("a", "transcribe", None, Some(10.0)),
                ("z", "transcribe", None, Some(0.0)),
            ],
        );
        assert_eq!(selected(&route, &request()).await["engine"]["model"], "z");
        let mut params = request();
        params["kind"] = json!("search");
        let result = route.call("route.select_engine", &params).await;
        assert_eq!(result.1["code"], "no_engine");
        assert_eq!(result.1["detail"], json!({"filter":"kind"}));
        params.as_object_mut().unwrap().remove("kind");
        assert_eq!(
            route.call("route.select_engine", &params).await.1["detail"]["field"],
            "kind"
        );
    })
    .await;
}
