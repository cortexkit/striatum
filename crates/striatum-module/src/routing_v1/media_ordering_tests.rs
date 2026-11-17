//! Pins and ladders choose identities; they do not relax media admission.

use super::*;

fn set_pin(h: &Harness, model: &str) {
    h.edit_config(|config| {
        config["consumers"]["colliculus"]["pin"] = json!({"model":format!("media-vendor/{model}")});
    });
}

async fn unavailable(route: &Route, params: &Value, pin: &str, reason: &str) {
    let refusal = refuse(route, params, "pin_unavailable", None).await;
    assert_eq!(refusal["detail"], json!({"pin":pin,"reason":reason}));
}

#[tokio::test]
async fn media_ordering_settings_pin_precedes_mode_and_bypasses_model_pin_only() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for (local, offered) in [(false, false), (true, false), (true, true)] {
                let engines = vec![engine("a-auto", local), engine("z-pinned", local)];
                let h = setup(if offered { vec![] } else { engines.clone() });
                let route = h.bind(reserved("colliculus"), None).await;
                let mut params = request();
                params["consumer"] = json!("colliculus.voice_message");
                if offered {
                    params["caller_local"] = json!(engines);
                }
                for rules in [
                    json!({"pin_only":true}),
                    json!({"pin_only_for":["colliculus"]}),
                ] {
                    h.edit_config(|config| {
                        config["models"]["media-vendor/z-pinned"] = rules;
                        config["consumers"]["colliculus"]["mode"] = json!("demand");
                        config["consumers"]["colliculus"]["ladder"] =
                            json!(["media-vendor/a-auto"]);
                    });
                    set_pin(&h, "z-pinned");
                    assert_eq!(select(&route, &params).await["engine"]["model"], "z-pinned");
                }
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_ordering_unavailable_pin_never_substitutes_or_reports_reset() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for cause in [
                "missing",
                "wrong-kind",
                "no-key",
                "fenced",
                "requirements",
                "variant",
            ] {
                let mut pinned = engine("z-pinned", false);
                if cause == "wrong-kind" {
                    pinned["kind"] = json!("image");
                }
                let h = setup(if cause == "missing" {
                    vec![engine("a-auto", true)]
                } else {
                    vec![engine("a-auto", true), pinned]
                });
                let route = h.bind(reserved("colliculus"), None).await;
                set_pin(&h, "z-pinned");
                let mut params = request();
                match cause {
                    "no-key" => h.set_credentials(media_credentials(vec![])),
                    "fenced" => {
                        crate::record_cooldown_update(h.store(), &serde_json::from_value(json!({
                        "routeKey":"media-vendor/z-pinned", "capacityKey":"media-vendor/z-pinned",
                        "cooldownUntil":crate::now_ms()+60000, "lastError":"HTTP 503"
                    })).unwrap()).unwrap();
                    }
                    "requirements" => params["requirements"]["format"] = json!("audio/flac"),
                    "variant" => h.edit_config(|config| {
                        config["consumers"]["colliculus"]["pin"]["variant"] = json!("fast")
                    }),
                    _ => {}
                }
                let pin = if cause == "variant" {
                    "media-vendor/z-pinned:fast"
                } else {
                    "media-vendor/z-pinned"
                };
                unavailable(&route, &params, pin, "not_available").await;
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_ordering_request_pin_precedes_settings_and_keeps_hard_admission() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![
                engine("a-auto", false),
                engine("y-request", false),
                engine("z-settings", false),
            ]);
            let route = h.bind(reserved("colliculus"), None).await;
            set_pin(&h, "z-settings");
            h.edit_config(|config| {
                config["models"]["media-vendor/y-request"] = json!({"pin_only":true})
            });
            let mut params = request();
            params["pin"] = json!("media-vendor/y-request");
            assert_eq!(
                select(&route, &params).await["engine"]["model"],
                "y-request"
            );
            for extra in [
                json!({"exclude":["media-vendor/y-request"]}),
                json!({"exclude_providers":["media-vendor"]}),
                json!({"eligible_routes":[{"provider":"media-vendor","model":"a-auto"}]}),
                json!({"requirements":{"metric_limit":{"metric":"wer","at_most":0.1}}}),
                json!({"privacy":"local_only"}),
            ] {
                let mut narrowed = params.clone();
                for (key, value) in extra.as_object().unwrap() {
                    narrowed[key] = value.clone();
                }
                unavailable(&route, &narrowed, "media-vendor/y-request", "not_available").await;
            }
            params["pin"] = json!("media-vendor/absent");
            unavailable(&route, &params, "media-vendor/absent", "not_available").await;
            // The published parser owns malformed request pins. Settings variants
            // are unavailable identities instead, since they never pass that parser.
            params["pin"] = json!("media-vendor/y-request:fast");
            unavailable(&route, &params, "media-vendor/y-request:fast", "invalid").await;
            let mut video = engine("video", false);
            video["kind"] = json!("video");
            declare("video", &[video]);
            params = request();
            params["kind"] = json!("video");
            params["pin"] = json!("media-vendor/video");
            params["requirements"]["audio"] = json!(true);
            unavailable(&route, &params, "media-vendor/video", "not_available").await;
        },
    )
    .await;
}

#[tokio::test]
async fn media_ordering_pins_never_bypass_only_for_or_not_for() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for (local, offered) in [(false, false), (true, false), (true, true)] {
                let engines = vec![engine("a-auto", local), engine("z-pinned", local)];
                let h = setup(if offered { vec![] } else { engines.clone() });
                let route = h.bind(reserved("colliculus"), None).await;
                let mut params = request();
                params["consumer"] = json!("colliculus.voice_message");
                params["pin"] = json!("media-vendor/z-pinned");
                if offered {
                    params["caller_local"] = json!(engines);
                }
                for entity in ["models", "providers"] {
                    let key = if entity == "models" {
                        "media-vendor/z-pinned"
                    } else {
                        "media-vendor"
                    };
                    for rules in [
                        json!({"only_for":["other"]}),
                        json!({"not_for":["colliculus.voice_message"]}),
                    ] {
                        h.edit_config(|config| {
                            config["models"] = json!({});
                            config["providers"] = json!({});
                            config[entity][key] = rules;
                        });
                        unavailable(&route, &params, "media-vendor/z-pinned", "excluded").await;
                    }
                }
                // A parent pin is valid for the parent while a child-specific
                // restriction still refuses the inherited pin at request time.
                h.edit_config(|config| {
                    config["models"] =
                        json!({"media-vendor/z-pinned":{"not_for":["colliculus.voice_message"]}});
                    config["providers"] = json!({});
                });
                set_pin(&h, "z-pinned");
                params.as_object_mut().unwrap().remove("pin");
                unavailable(&route, &params, "media-vendor/z-pinned", "excluded").await;
            }
        },
    )
    .await;
}

fn declare_ladder(engines: &[Value], ladder: &[&str]) {
    super::super::super::consumers::replace_catalog(&json!([
        {"module_id":"colliculus","nonce_verified":true,"role_declarations":{
            "media-provider/v1":{"engines":engines},
            "routing/v1":{"consumers":[{"name":"colliculus","kind":"transcribe","ladder":ladder}]}
        }}
    ]));
}

#[tokio::test]
async fn media_ordering_ladders_keep_order_and_never_widen_model_policy() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for declared in [false, true] {
                let engines = vec![
                    engine("a-auto", false),
                    engine("y-first", false),
                    engine("z-manual", false),
                ];
                let h = setup(engines.clone());
                let route = h.bind(reserved("colliculus"), None).await;
                let ladder = [
                    "media-vendor/absent",
                    "media-vendor/a-auto:fast",
                    "media-vendor/z-manual",
                    "media-vendor/y-first",
                    "media-vendor/a-auto",
                ];
                if declared {
                    declare_ladder(&engines, &ladder);
                }
                h.edit_config(|config| {
                    config["models"]["media-vendor/z-manual"] = json!({"pin_only":true});
                    if !declared {
                        config["consumers"]["colliculus"]["mode"] = json!("ladder");
                        config["consumers"]["colliculus"]["ladder"] = json!(ladder);
                    }
                });
                assert_eq!(
                    select(&route, &request()).await["engine"]["model"],
                    "y-first"
                );
                let exhausted = &ladder[..ladder.len() - 1];
                if declared {
                    declare_ladder(&engines, exhausted);
                } else {
                    h.edit_config(|config| {
                        config["consumers"]["colliculus"]["ladder"] = json!(exhausted)
                    });
                }
                h.edit_config(|config| {
                    config["models"]["media-vendor/y-first"] =
                        json!({"pin_only_for":["colliculus"]})
                });
                refuse(&route, &request(), "no_route", None).await;
                // There is still an automatic candidate, but it is outside the
                // person's ladder, so exhaustion must not select outside it.
                if declared {
                    declare_ladder(&engines, &[]);
                } else {
                    h.edit_config(|config| config["consumers"]["colliculus"]["ladder"] = json!([]));
                }
                refuse(&route, &request(), "no_route", None).await;
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_ordering_module_identity_never_falls_back_to_a_caller_offer() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            for ladder in [false, true] {
                let module = engine("shared", false);
                let h = setup(vec![engine("a-auto", true), module.clone()]);
                let route = h.bind(reserved("colliculus"), None).await;
                h.edit_config(|config| {
                    if ladder {
                        config["consumers"]["colliculus"]["mode"] = json!("ladder");
                        config["consumers"]["colliculus"]["ladder"] =
                            json!(["media-vendor/shared"]);
                    } else {
                        config["consumers"]["colliculus"]["pin"] =
                            json!({"model":"media-vendor/shared"});
                    }
                });
                let mut params = request();
                params["caller_local"] = json!([engine("shared", true)]);
                let answer = select(&route, &params).await;
                assert_eq!(answer["engine"]["model"], "shared");
                assert_eq!(answer["engine"]["caller_local"], false);
                params["privacy"] = json!("local_only");
                if ladder {
                    refuse(&route, &params, "no_route", None).await;
                } else {
                    unavailable(&route, &params, "media-vendor/shared", "not_available").await;
                }
                // A declaration for a different kind does not claim this identity.
                let mut other_kind = module;
                other_kind["kind"] = json!("image");
                declare("transcribe", &[engine("a-auto", true), other_kind]);
                let answer = select(&route, &params).await;
                assert_eq!(answer["engine"]["model"], "shared");
                assert_eq!(answer["engine"]["caller_local"], true);
                assert_eq!(answer["engine"]["index"], 0);
                params.as_object_mut().unwrap().remove("caller_local");
                if ladder {
                    refuse(&route, &params, "no_route", None).await;
                } else {
                    unavailable(&route, &params, "media-vendor/shared", "not_available").await;
                }
            }
        },
    )
    .await;
}

#[tokio::test]
async fn media_ordering_decommissioned_is_never_automatic_laddered_or_pinned() {
    super::super::super::describe::with_candidate_capabilities(&["select_kind", "select_media"], async {
        for local in [false, true] {
            let h = setup(vec![engine("a-retired", local), engine("z-live", local)]);
            let route = h.bind(reserved("colliculus"), None).await;
            let catalog = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&json!({
                "catalog_version":1,"resolved_at_ms":1,"models":{
                    "media-vendor/a-retired":{"model.type":"transcription","quality.decommissioned":true}
                }
            })).unwrap()).unwrap();
            h.credentials().lock().unwrap().catalog_snapshot = Some(catalog);
            let mut params = request();
            params["caller_local"] = json!([engine("a-retired", true)]);
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-live");
            h.edit_config(|config| {
                config["consumers"]["colliculus"]["mode"] = json!("ladder");
                config["consumers"]["colliculus"]["ladder"] = json!(["media-vendor/a-retired", "media-vendor/z-live"]);
            });
            assert_eq!(select(&route, &params).await["engine"]["model"], "z-live");
            set_pin(&h, "a-retired");
            unavailable(&route, &params, "media-vendor/a-retired", "not_available").await;
        }
    }).await;
}

#[tokio::test]
async fn media_ordering_project_pins_are_not_media_pins() {
    super::super::super::describe::with_candidate_capabilities(
        &["select_kind", "select_media"],
        async {
            let h = setup(vec![engine("a-auto", false), engine("z-project", false)]);
            h.edit_config(|config| {
                config["consumers"]["colliculus"]["projects"]["media-project"] =
                    json!({"pin":"media-vendor/z-project"});
            });
            let route = h.bind(reserved("colliculus"), Some("media-project")).await;
            assert_eq!(
                select(&route, &request()).await["engine"]["model"],
                "a-auto"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn media_ordering_select_engine_ignores_settings_pin_ladder_and_mode() {
    let h = setup(vec![engine("a-auto", false), engine("z-settings", false)]);
    let route = h.bind(reserved("colliculus"), None).await;
    h.edit_config(|config| {
        config["consumers"]["colliculus"]["mode"] = json!("ladder");
        config["consumers"]["colliculus"]["ladder"] = json!(["media-vendor/z-settings"]);
    });
    assert_eq!(
        selected(&route, &request()).await["engine"]["model"],
        "a-auto"
    );
    set_pin(&h, "z-settings");
    assert_eq!(
        selected(&route, &request()).await["engine"]["model"],
        "a-auto"
    );
    set_pin(&h, "absent");
    assert_eq!(
        selected(&route, &request()).await["engine"]["model"],
        "a-auto"
    );
}
