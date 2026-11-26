use super::ranking::{catalog, engine, job};
use super::*;

const MODEL: &str = "gpt-transcribe";

fn openai_engine(provider: &str) -> Value {
    let mut engine = engine(MODEL, false);
    engine["provider"] = json!(provider);
    engine
}

fn openai_key(provider: &str, mapped: bool) -> ListedCredential {
    let mut key = key(provider);
    key.provider_ids = vec![provider.into()];
    if mapped {
        key.serves = vec!["openai".into()];
        key.fusiform_provider_id = Some("openai".into());
    }
    key
}

#[tokio::test]
async fn media_catalog_alias_inherits_transcription_price_and_grade_but_keeps_route_identity() {
    advertised_media(async {
        let h = setup(vec![
            openai_engine("openai"),
            openai_engine("openai-platform"),
        ]);
        h.set_credentials(media_credentials(vec![
            openai_key("openai", true),
            openai_key("openai-platform", true),
        ]));
        catalog(
            &h,
            &[(
                "openai/gpt-transcribe",
                "transcription",
                Some(80.0),
                Some(2),
            )],
        );
        let live = h.bind(reserved("colliculus"), None).await;
        let operator = h.bind(json!({"kind":"direct"}), None).await;

        for provider in ["openai", "openai-platform"] {
            if provider == "openai-platform" {
                // A policy for the catalog route must not exclude the platform engine.
                h.edit_config(|config| {
                    config["models"]["openai/gpt-transcribe"]["not_for"] = json!(["colliculus"]);
                });
            }
            let mut params = job();
            params["eligible_routes"] = json!([{"provider":provider,"model":MODEL}]);
            let answer = select(&live, &params).await;
            assert_eq!(answer["engine"]["provider"], provider);
            assert_eq!(answer["engine"]["model"], MODEL);
            assert_eq!(
                answer["price"],
                json!({"basis":"metered",
                "rates":{"input_audio.per_second":{"rate":2.0}},"estimated_task_usd":2.0})
            );
            let decision_id = answer["decision_id"].as_str().unwrap();
            let decision = h.store().v1_decision(decision_id).unwrap().unwrap();
            assert_eq!(decision.provider, provider);

            let (frame, preview) = operator.call("route.what_if", &job()).await;
            assert_eq!(frame, FrameType::Response, "{preview}");
            assert_eq!(
                preview["result"]["results"][0]["pick"],
                json!({"route":format!("{provider}/{MODEL}"), "grade":80.0, "cost_usd":2.0})
            );

            let (frame, admission) = live.call("route.account_for", &json!({
                "admission_id":provider,"conversation_key":provider,
                "provider":provider,"model":MODEL,"decision_id":decision_id,"hold_wait_max_s":0
            })).await;
            assert_eq!(frame, FrameType::Response, "{admission}");
            assert_eq!(admission["result"]["account"]["credential_id"], provider);
        }
    })
    .await;
}

#[tokio::test]
async fn media_catalog_alias_unmapped_engine_needs_its_own_key_and_stays_ungraded() {
    advertised_media(async {
        let h = setup(vec![openai_engine("openai-platform")]);
        let canonical = openai_key("openai", true);
        h.set_credentials(media_credentials(vec![canonical.clone()]));
        catalog(
            &h,
            &[(
                "openai/gpt-transcribe",
                "transcription",
                Some(80.0),
                Some(2),
            )],
        );
        let live = h.bind(reserved("colliculus"), None).await;
        let (frame, refused) = live.call("route.select", &job()).await;
        assert_eq!(frame, FrameType::Error, "{refused}");
        assert_eq!(refused["code"], "no_route");
        assert_eq!(refused["detail"], json!({"reason":"no_available_model"}));

        h.credentials().lock().unwrap().apply_listing(
            &listing(vec![canonical, openai_key("openai-platform", false)], true),
            crate::now_ms(),
        );
        let answer = select(&live, &job()).await;
        assert_eq!(answer["engine"]["provider"], "openai-platform");
        assert!(answer.get("price").is_none(), "{answer}");
        let operator = h.bind(json!({"kind":"direct"}), None).await;
        let (frame, preview) = operator.call("route.what_if", &job()).await;
        assert_eq!(frame, FrameType::Response, "{preview}");
        assert_eq!(
            preview["result"]["results"][0]["pick"],
            json!({
                "route":"openai-platform/gpt-transcribe", "grade":null, "cost_usd":null
            })
        );
    })
    .await;
}

#[tokio::test]
async fn media_catalog_alias_uses_video_output_facts_and_respects_catalog_withdrawal() {
    advertised_media(async {
        let mut video = engine("video-model", false);
        video["provider"] = json!("openai-platform");
        video["kind"] = json!("video");
        let h = setup(vec![video]);
        h.set_credentials(media_credentials(vec![openai_key("openai-platform", true)]));
        let install = |decommissioned| {
            let payload = json!({"catalog_version":1,"resolved_at_ms":1,"models":{
                "openai/video-model":{"existence":"present","model.type":"video",
                    "limit.output_duration_ms":1000,"capability.aspect_ratios":["16:9"],
                    "capability.output_audio":true,"quality.decommissioned":decommissioned}
            }});
            h.credentials().lock().unwrap().catalog_snapshot = Some(
                crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&payload).unwrap())
                    .unwrap(),
            );
        };
        install(false);
        let live = h.bind(reserved("colliculus"), None).await;
        let mut params = request();
        params["kind"] = json!("video");
        params["requirements"] =
            json!({"output_duration_ms":1000,"aspect_ratio":"16:9","audio":true});
        assert_eq!(
            select(&live, &params).await["engine"]["provider"],
            "openai-platform"
        );

        install(true);
        let (frame, refused) = live.call("route.select", &params).await;
        assert_eq!(frame, FrameType::Error, "{refused}");
        assert_eq!(refused["code"], "no_route");
        assert_eq!(refused["detail"], json!({"reason":"no_available_model"}));
    })
    .await;
}
