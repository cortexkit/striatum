//! Engines share chat's provider/model policy keys, not its metered pin requirement.

use super::*;

#[tokio::test]
async fn media_policy_model_pin_only_blocks_module_and_caller_local_choices() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for (local, caller_local) in [(false, false), (true, false), (true, true)] {
                let engines = vec![engine("a-manual", local), engine("z-auto", local)];
                let h = setup(if caller_local {
                    vec![]
                } else {
                    engines.clone()
                });
                let route = h.bind(reserved("colliculus"), None).await;
                let mut params = request();
                if caller_local {
                    params["caller_local"] = json!(engines);
                }
                h.edit_config(|config| {
                    config["models"]["media-vendor/a-manual"] = json!({"pin_only":true});
                });
                assert_eq!(select(&route, &params).await["engine"]["model"], "z-auto");
                assert_eq!(
                    selected(&route, &params).await["engine"]["model"],
                    "a-manual"
                );
                h.edit_config(|config| {
                    config["models"]["media-vendor/z-auto"] = json!({"pin_only":true});
                });
                let refusal = refuse(&route, &params, "no_route", None).await;
                assert_eq!(refusal["detail"], json!({"reason":"no_available_model"}));
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_policy_model_pin_only_for_matches_whole_consumer_segments() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("a-manual", false), engine("z-auto", false)]);
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = request();
            params["consumer"] = json!("colliculus.voice_message");
            for (names, expected) in [
                (json!(["colliculus"]), "z-auto"),
                (json!(["colliculus.voice_message"]), "z-auto"),
                (json!(["colliculus.voice"]), "a-manual"),
                (json!([]), "a-manual"),
            ] {
                h.edit_config(|config| {
                    config["models"]["media-vendor/a-manual"] = json!({"pin_only_for":names});
                });
                assert_eq!(select(&route, &params).await["engine"]["model"], expected);
            }
            let converted = crate::config_convert::convert_value(json!({
                "model_routing":{"exclude":{"all":["media-vendor/a-manual"]}}
            }))
            .unwrap();
            h.edit_config(|config| config["models"] = converted["models"].clone());
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-auto");
            h.edit_config(|config| {
                config["models"]["media-vendor/a-manual"] = json!({"pin_only":false});
            });
            assert_eq!(select(&route, &params).await["engine"]["model"], "a-manual");
        },
    )
    .await;
}

#[tokio::test]
async fn media_policy_pin_only_routes_are_outside_the_grade_top() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("a-manual", false), engine("z-auto", false)]);
            let catalog = crate::fusiform_catalog::decode_catalog_get(
                &serde_json::to_vec(&json!({
                    "catalog_version":1,"resolved_at_ms":1,
                    "quality_scores":{"graded_by":"operator","graded_at_ms":1},
                    "models":{
                        "media-vendor/a-manual":{"model.type":"transcription","quality.grade":100},
                        "media-vendor/z-auto":{"model.type":"transcription","quality.grade":30}
                    }
                }))
                .unwrap(),
            )
            .unwrap();
            h.credentials().lock().unwrap().catalog_snapshot = Some(catalog);
            h.edit_config(|config| {
                config["models"]["media-vendor/a-manual"] = json!({"pin_only":true});
                config["consumers"]["colliculus"]["below_bar"] = json!("refuse");
            });
            let route = h.bind(reserved("colliculus"), None).await;
            let mut params = request();
            params["requirements"]["iq"] = json!(100);
            // At level 100 the remaining grade-30 engine must clear its own
            // scale, rather than the excluded grade-100 engine's scale.
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-auto");
        },
    )
    .await;
}

#[tokio::test]
async fn media_policy_provider_chat_pin_rules_leave_gemini_automatic() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let mut gemini = engine("gemini-3.8-flash", false);
            gemini["provider"] = json!("google");
            let h = setup(vec![gemini]);
            let mut credential = key("google-media");
            credential.provider_ids = vec!["google".into()];
            h.set_credentials(media_credentials(vec![credential]));
            let route = h.bind(reserved("colliculus"), None).await;
            let converted = crate::config_convert::convert_value(json!({
                "model_routing":{"exclude":{"all":["google"]}}
            }))
            .unwrap();
            for rules in [
                json!({"pin_only":true}),
                json!({"pin_only_for":["colliculus"]}),
                converted["providers"]["google"].clone(),
            ] {
                h.edit_config(|config| {
                    config["providers"] = json!({"google":rules});
                    config["models"] = json!({});
                });
                let answer = select(&route, &request()).await;
                assert_eq!(answer["engine"]["provider"], "google");
                assert_eq!(answer["engine"]["model"], "gemini-3.8-flash");
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_policy_remote_choice_has_no_metered_pin_only_default() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("remote", false)]);
            h.edit_config(|config| {
                config["providers"] = json!({});
                config["models"] = json!({});
            });
            let route = h.bind(reserved("colliculus"), None).await;
            assert_eq!(
                select(&route, &request()).await["engine"]["model"],
                "remote"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn media_policy_consumer_restrictions_and_provider_models_bind_local_engines() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for (local, caller_local) in [(false, false), (true, false), (true, true)] {
                let engines = vec![engine("shared-chat-model", local)];
                let h = setup(if caller_local {
                    vec![]
                } else {
                    engines.clone()
                });
                let route = h.bind(reserved("colliculus"), None).await;
                let mut params = request();
                params["consumer"] = json!("colliculus.voice_message");
                if caller_local {
                    params["caller_local"] = json!(engines);
                }
                for entity in ["providers", "models"] {
                    let key = if entity == "providers" {
                        "media-vendor"
                    } else {
                        "media-vendor/shared-chat-model"
                    };
                    for rules in [
                        json!({"only_for":["other"]}),
                        json!({"not_for":["colliculus"]}),
                    ] {
                        h.edit_config(|config| {
                            config["providers"] = json!({});
                            config["models"] = json!({});
                            config[entity][key] = rules;
                        });
                        refuse(&route, &params, "no_route", None).await;
                    }
                }
                h.edit_config(|config| {
                    config["models"] = json!({});
                    config["providers"] = json!({"media-vendor":{"models":["other"]}});
                });
                refuse(&route, &params, "no_route", None).await;
                h.edit_config(|config| {
                    config["providers"]["media-vendor"] =
                        json!({"only_for":["colliculus"],"not_for":["colliculus.voice"]});
                });
                assert_eq!(
                    select(&route, &params).await["engine"]["model"],
                    "shared-chat-model"
                );
                params["exclude"] = json!(["media-vendor/shared-chat-model"]);
                refuse(&route, &params, "no_route", None).await;
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_policy_compatibility_entry_keeps_pin_and_local_exemptions() {
    let h = setup(vec![engine("remote", false)]);
    let route = h.bind(reserved("colliculus"), None).await;
    for entity in ["providers", "models"] {
        let key = if entity == "providers" {
            "media-vendor"
        } else {
            "media-vendor/remote"
        };
        for rules in [
            json!({"pin_only":true}),
            json!({"pin_only_for":["colliculus"]}),
        ] {
            h.edit_config(|config| {
                config["providers"] = json!({});
                config["models"] = json!({});
                config[entity][key] = rules;
            });
            assert_eq!(
                selected(&route, &request()).await["engine"]["model"],
                "remote"
            );
        }
        for rules in [
            json!({"only_for":["other"]}),
            json!({"not_for":["colliculus"]}),
        ] {
            h.edit_config(|config| config[entity][key] = rules);
            let (frame, value) = route.call("route.select_engine", &request()).await;
            assert_eq!(frame, FrameType::Error, "{value}");
            assert_eq!(value["code"], "no_engine", "{value}");
            let mut params = request();
            params["caller_local"] = json!([engine("remote", true)]);
            assert_eq!(
                selected(&route, &params).await["engine"]["caller_local"],
                true
            );
        }
    }
}

#[tokio::test]
async fn media_policy_llm_keeps_provider_pin_rules_and_metered_default() {
    use cortexkit_role_routing_conformance::fixture;

    let h = Harness::new("llm-media-policy-boundary");
    let mut fact =
        super::super::super::conformance_tests::catalog_fact(&fixture::catalog().models[0], 0);
    fact.provider_id = "chat-vendor".into();
    fact.model_id = "shared-chat-model".into();
    fact.auth_methods = Some(vec!["apikey".into()]);
    fact.billing_plane = Some(striatum_core::billing::BillingPlane {
        provider_id: "chat-vendor".into(),
        auth_method: "apikey".into(),
        kind: "upstream".into(),
        selection_reason: "metered policy fixture".into(),
        rates: [(
            "rate.output".into(),
            striatum_core::billing::BillingRate::Priced { rate: 1.0 },
        )]
        .into(),
    });
    rusqlite::Connection::open(h.store_path())
        .unwrap()
        .execute("DELETE FROM model_facts", [])
        .unwrap();
    h.install_facts(&[fact]);
    h.edit_config(|config| config["consumers"]["codemode"]["pay_per_token"] = json!(true));
    let route = h.bind(reserved(fixture::CONSUMER), None).await;
    let params = json!({"consumer":"codemode","requirements":{"iq":0}});
    assert_eq!(
        select(&route, &params).await["route"]["model"],
        "shared-chat-model"
    );
    for rules in [
        json!({"pin_only":true}),
        json!({"pin_only_for":["codemode"]}),
    ] {
        h.edit_config(|config| config["providers"] = json!({"chat-vendor":rules}));
        refuse(&route, &params, "no_route", None).await;
    }
    h.edit_config(|config| config["providers"] = json!({}));
    refuse(&route, &params, "no_route", None).await;
}
