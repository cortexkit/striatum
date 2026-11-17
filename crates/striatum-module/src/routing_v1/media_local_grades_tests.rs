use super::*;
use sha2::{Digest, Sha256};

#[tokio::test]
async fn approved_voice_grades_use_declared_level_without_select_time_fusiform_calls() {
    advertised_media(async {
        let bytes = include_bytes!(
            "../../fixtures/colliculus-voice-message-demand/routing-v1-declaration.json"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "2fa96a8a79cdfa1213d8560e2df73bdcb200cd672fb47ee29029972feff466ba"
        );
        let declaration: Value = serde_json::from_slice(bytes).unwrap();
        assert!(declaration["consumers"][0].get("mode").is_none());
        let mut remote = ranking::engine("gpt-transcribe", false);
        remote["provider"] = json!("openai");
        let mut hydrogen = ranking::engine("gemini-4-hydrogen", false);
        hydrogen["provider"] = json!("google");
        let h = setup(vec![remote.clone(), hydrogen.clone()]);
        super::super::super::consumers::replace_catalog(&json!([{
            "module_id":"colliculus","nonce_verified":true,"role_declarations":{
                "routing/v1":declaration,"media-provider/v1":{"engines":[remote,hydrogen]}
            }
        }]));
        let mut credential = key("approved-media");
        credential.provider_ids = vec!["openai".into(), "google".into()];
        h.set_credentials(media_credentials(vec![credential]));
        let snapshot = ranking::catalog(
            &h,
            &[
                (
                    "openai/gpt-transcribe",
                    "transcription",
                    Some(91.0),
                    Some(1),
                ),
                ("google/gemini-4-hydrogen", "chat", None, None),
            ],
        );
        let snapshot = (*snapshot).clone().with_local_media_grade(
            "local:apple/phone-transcriber",
            "transcription",
            59.0,
        );
        h.credentials().lock().unwrap().catalog_snapshot = Some(std::sync::Arc::new(snapshot));
        let mut phone = ranking::engine("phone-transcriber", true);
        phone["provider"] = json!("apple");
        let mut params = ranking::job();
        params["consumer"] = json!("colliculus.voice_message");
        params["caller_local"] = json!([phone]);
        let route = h.bind(reserved("callosum"), None).await;
        let (evidence, calls) = crate::fusiform_catalog::count_calls(async {
            let (_, evidence) =
                crate::routing_v1::engines::grade_observer::capture(async {
                    assert_eq!(
                        select(&route, &params).await["engine"]["model"],
                        "gpt-transcribe"
                    );
                    params["privacy"] = json!("local_only");
                    let private = select(&route, &params).await;
                    assert_eq!(private["engine"]["model"], "phone-transcriber");
                    assert_eq!(private["engine"]["caller_local"], true);
                    params["privacy"] = json!("remote_allowed");
                    crate::record_cooldown_update(h.store(), &serde_json::from_value(json!({
                "routeKey":"openai/gpt-transcribe", "capacityKey":"openai/gpt-transcribe",
                "cooldownUntil":crate::now_ms()+60000,"lastError":"HTTP 503"
            })).unwrap()).unwrap();
                    assert_eq!(
                        select(&route, &params).await["engine"]["model"],
                        "phone-transcriber"
                    );
                })
                .await;
            evidence
        })
        .await;
        use crate::routing_v1::engines::grade_observer::Evidence;
        assert_eq!(
            evidence,
            vec![
                Evidence {
                    iq: Some(75.0),
                    top: Some(91.0),
                    bar: Some(68.25),
                    adequate: 1
                },
                Evidence {
                    iq: Some(75.0),
                    top: Some(59.0),
                    bar: Some(44.25),
                    adequate: 1
                },
                Evidence {
                    iq: Some(75.0),
                    top: Some(91.0),
                    bar: Some(68.25),
                    adequate: 0
                },
            ]
        );
        assert_eq!(calls, 0, "selection must use cached Fusiform grades");
    })
    .await;
}
