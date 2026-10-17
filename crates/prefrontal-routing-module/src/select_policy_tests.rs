mod consumer_and_pin {
    use super::*;

    struct PolicyRig {
        _rig: Rig,
        store: SharedStore,
        path: PathBuf,
        quota: SharedQuotaCache,
    }

    impl PolicyRig {
        fn new(config: serde_json::Value) -> Self {
            let rig = Rig::new("consumer-and-pin");
            let store = Arc::new(rig.open_store());
            let path = rig.dir.join("routing.jsonc");
            std::fs::write(&path, config.to_string()).unwrap();
            for (provider, model, cost) in [("openai", "pin", 0.1), ("fallback", "safe", 10.0)] {
                let mut fact = fact(provider, model);
                fact.cost_per_m_tok_out = Some(cost);
                store.upsert_fact(&fact, FactSource::Override).unwrap();
            }
            Self {
                _rig: rig,
                store,
                path,
                quota: Arc::new(Mutex::new(QuotaCache::default())),
            }
        }

        fn params(&self) -> serde_json::Value {
            json!({
                "targetAgent": "oracle", "consumer": "lab",
                "requirements": {"iq": 30, "eq": 0},
                "availableModels": ["openai/pin", "fallback/safe"],
            })
        }

        async fn select(&self, params: serde_json::Value) -> serde_json::Value {
            let frame = Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Interactive, false),
                7,
                1,
                99,
                serde_json::to_vec(&json!({"method": ROUTE_SELECT_OP, "params": params})).unwrap(),
            )
            .unwrap();
            let (tx, mut rx) = mpsc::channel(1);
            let credentials = Arc::new(Mutex::new(BrocaCredentials::api_test_credentials(
                &self.store.get_facts().unwrap(),
            )));
            handle_select_request(
                frame,
                1,
                &tx,
                &self.quota,
                &self.store,
                &Arc::new(Mutex::new(None)),
                &self.path,
                &self.path,
                &credentials,
                &empty_opencode_auth(),
            )
            .await
            .unwrap();
            let reply = rx.recv().await.unwrap();
            serde_json::from_slice(&reply.body).unwrap()
        }

        fn decisions(&self) -> usize {
            self.store
                .list_decisions(&DecisionListFilter {
                    limit: 100,
                    ..Default::default()
                })
                .unwrap()
                .len()
        }

        fn pin_params(&self) -> serde_json::Value {
            let mut params = self.params();
            params["pin"] = json!({"slot": "lab_planner"});
            params["pin_fallback"] = json!("route");
            params
        }
    }

    fn planner_config() -> serde_json::Value {
        json!({"model_routing": {"pins": {"lab_planner": {"model": "openai/pin"}}}})
    }

    #[tokio::test]
    async fn consumer_groups_apply_only_to_the_named_consumer() {
        let _guard = crate::config_overrides::multiplier_map_guard();
        for group in ["mason", "sidekick", "athena", "lab", "core"] {
            let rig =
                PolicyRig::new(json!({"model_routing": {"exclude": {group: ["openai/pin"]}}}));
            for consumer in ["mason", "sidekick", "athena", "lab", "core"] {
                let mut params = rig.params();
                params["consumer"] = json!(consumer);
                let reply = rig.select(params).await;
                assert_eq!(
                    reply["result"]["selected"]["capacityKey"],
                    if consumer == group {
                        "fallback/safe"
                    } else {
                        "openai/pin"
                    },
                    "group={group} consumer={consumer}: {reply}"
                );
                assert!(reply["result"].get("pin_rejected").is_none(), "{reply}");
            }
            let mut params = rig.params();
            params.as_object_mut().unwrap().remove("consumer");
            params["targetAgent"] = json!(group);
            let reply = rig.select(params).await;
            assert_eq!(
                reply["result"]["selected"]["capacityKey"], "openai/pin",
                "{reply}"
            );
        }
    }

    #[tokio::test]
    async fn group_and_caller_negations_cannot_lift_all_provider_exclusions() {
        let _guard = crate::config_overrides::multiplier_map_guard();
        let rig = PolicyRig::new(json!({"model_routing": {"exclude": {
            "all": ["anthropic"], "mason": ["-anthropic"]
        }}}));
        for model in ["one", "two"] {
            rig.store
                .upsert_fact(&fact("anthropic", model), FactSource::Override)
                .unwrap();
        }
        for consumer in [Some("mason"), None] {
            let mut params = rig.params();
            if let Some(consumer) = consumer {
                params["consumer"] = json!(consumer);
            } else {
                params.as_object_mut().unwrap().remove("consumer");
            }
            params["availableModels"] = json!(["anthropic/one", "anthropic/two", "fallback/safe"]);
            params["excludeRouteKeys"] = json!(["-anthropic"]);
            let reply = rig.select(params).await;
            assert_eq!(
                reply["result"]["selected"]["capacityKey"], "fallback/safe",
                "{reply}"
            );
            for model in ["anthropic/one", "anthropic/two"] {
                assert_eq!(
                    rejection_reason(&reply, model).as_deref(),
                    Some("excluded_by_operator")
                );
            }
        }
    }

    struct TargetWarnings(Arc<Mutex<Vec<String>>>);

    impl tracing::Subscriber for TargetWarnings {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct TargetVisitor<'a>(&'a mut Vec<String>);
            impl tracing::field::Visit for TargetVisitor<'_> {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "target_agent" {
                        self.0.push(value.to_owned());
                    }
                }
                fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            }
            if event.metadata().level() == &tracing::Level::WARN {
                event.record(&mut TargetVisitor(&mut self.0.lock().unwrap()));
            }
        }
    }

    #[tokio::test]
    async fn missing_consumer_logs_each_target_agent_once() {
        let _guard = crate::config_overrides::multiplier_map_guard();
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let _subscriber = tracing::subscriber::set_default(TargetWarnings(Arc::clone(&warnings)));
        let rig = PolicyRig::new(json!({}));
        let mut params = rig.params();
        params.as_object_mut().unwrap().remove("consumer");
        params["targetAgent"] = json!("consumer-warning-wire-a");
        rig.select(params.clone()).await;
        rig.select(params.clone()).await;
        params["targetAgent"] = json!("consumer-warning-wire-b");
        rig.select(params.clone()).await;
        params["targetAgent"] = json!("consumer-warning-wire-explicit");
        params["consumer"] = json!("core");
        rig.select(params).await;
        assert_eq!(
            *warnings.lock().unwrap(),
            ["consumer-warning-wire-a", "consumer-warning-wire-b"]
        );
    }

    #[tokio::test]
    async fn admitted_pin_bypasses_every_exclude_group_with_variant() {
        let _guard = crate::config_overrides::multiplier_map_guard();
        let rig = PolicyRig::new(json!({"model_routing": {
            "exclude": {"all": ["openai"], "lab": ["openai/pin"]},
        }}));
        let mut params = rig.params();
        params["pin"] = json!({"model": "openai/pin", "variant": "high"});
        params["pin_fallback"] = json!("none");
        let reply = rig.select(params).await;
        assert_eq!(
            reply["result"]["selected"]["capacityKey"], "openai/pin",
            "{reply}"
        );
        assert_eq!(
            reply["result"]["selected"]["promptParams"]["variant"], "high",
            "{reply}"
        );
        assert_eq!(
            reply["result"]["selected"]["model"]["variant"], "high",
            "{reply}"
        );
        assert_eq!(
            reply["result"].get("pin_rejected"),
            Some(&serde_json::Value::Null)
        );
        assert!(reply["result"]["decisionID"].is_string(), "{reply}");
        assert_eq!(rig.decisions(), 1);
    }

    async fn blocked_planner(cause: &str, fallback: &str) {
        let _guard = crate::config_overrides::multiplier_map_guard();
        let rig = PolicyRig::new(planner_config());
        let mut params = rig.pin_params();
        params["pin_fallback"] = json!(fallback);
        if fallback == "none" {
            params["pin"] = json!({"model": "openai/pin", "variant": "high"});
        }
        match cause {
            "cooldown_active" | "quota_exhausted" => {
                let until = Some(now_ms() as f64 + 3_600_000.0);
                rig.store
                    .update_route_state(
                        "openai/pin",
                        "openai/pin",
                        &prefrontal_routing_core::RouteStateUpdate {
                            cooldown_until: if cause == "cooldown_active" {
                                until
                            } else {
                                None
                            },
                            quota_exhausted_until: if cause == "quota_exhausted" {
                                until
                            } else {
                                None
                            },
                            ..Default::default()
                        },
                    )
                    .unwrap();
            }
            "context_too_small" => {
                let mut pin = fact("openai", "pin");
                pin.context_window = Some(4096.0);
                rig.store.upsert_fact(&pin, FactSource::Override).unwrap();
                params.as_object_mut().unwrap().remove("requirements");
                params["requestedRequirements"] =
                    json!({"iq": 30, "caps": {"minContext": 100_000}});
            }
            "missing_vision" => {
                let mut pin = fact("openai", "pin");
                pin.supports_vision = Some(false);
                rig.store.upsert_fact(&pin, FactSource::Override).unwrap();
                params.as_object_mut().unwrap().remove("requirements");
                params["requestedRequirements"] = json!({"iq": 30, "caps": {"minContext": 512}});
                params["requiredCapabilities"] = json!({"vision": true});
            }
            "caller_excluded" => {
                params["excludeRouteKeys"] = json!(["openai/pin"]);
            }
            "not_available" => {
                params["availableModels"] = json!(["fallback/safe"]);
            }
            _ => panic!("unknown fixture cause: {cause}"),
        }
        let reply = rig.select(params).await;
        assert_eq!(reply["result"]["pin_rejected"], cause, "{reply}");
        if fallback == "route" {
            assert_eq!(
                reply["result"]["selected"]["capacityKey"], "fallback/safe",
                "{reply}"
            );
            assert_eq!(rig.decisions(), 1);
        } else {
            assert_eq!(
                reply["result"]["selected"],
                serde_json::Value::Null,
                "{reply}"
            );
            assert_eq!(
                reply["result"]["decisionID"],
                serde_json::Value::Null,
                "{reply}"
            );
            assert_eq!(rig.decisions(), 0);
        }
    }

    #[tokio::test]
    async fn lab_planner_pin_cooled_routes_to_fallback() {
        blocked_planner("cooldown_active", "route").await;
    }

    #[tokio::test]
    async fn lab_planner_pin_quota_blocked_routes_to_fallback() {
        blocked_planner("quota_exhausted", "route").await;
    }

    #[tokio::test]
    async fn lab_planner_pin_below_min_context_routes_to_fallback() {
        blocked_planner("context_too_small", "route").await;
    }

    #[tokio::test]
    async fn lab_planner_pin_caller_excluded_routes_to_fallback() {
        blocked_planner("caller_excluded", "route").await;
    }

    #[tokio::test]
    async fn pin_required_capabilities_and_availability_are_enforced() {
        blocked_planner("missing_vision", "route").await;
        blocked_planner("not_available", "route").await;
    }

    #[tokio::test]
    async fn rejected_frozen_pin_with_none_creates_no_decision() {
        blocked_planner("cooldown_active", "none").await;
        blocked_planner("caller_excluded", "none").await;
    }

    #[tokio::test]
    async fn lab_planner_edit_is_observed_after_reload_debounce() {
        let _guard = crate::config_overrides::multiplier_map_guard();
        let rig = PolicyRig::new(planner_config());
        let first = rig.select(rig.pin_params()).await;
        assert_eq!(
            first["result"]["selected"]["capacityKey"], "openai/pin",
            "{first}"
        );
        std::fs::write(
            &rig.path,
            json!({"model_routing": {"pins": {
                "lab_planner": {"model": "fallback/safe"}
            }}})
            .to_string(),
        )
        .unwrap();
        let pending = rig.select(rig.pin_params()).await;
        assert_eq!(
            pending["result"]["selected"]["capacityKey"], "openai/pin",
            "{pending}"
        );
        config_overrides::settle_live_config(&rig.path);
        let reloaded = rig.select(rig.pin_params()).await;
        assert_eq!(
            reloaded["result"]["selected"]["capacityKey"], "fallback/safe",
            "{reloaded}"
        );
        assert_eq!(
            reloaded["result"].get("pin_rejected"),
            Some(&serde_json::Value::Null)
        );
    }

    #[tokio::test]
    async fn unset_pin_slot_routes_normally_with_consumer_fences() {
        let _guard = crate::config_overrides::multiplier_map_guard();
        let rig = PolicyRig::new(json!({"model_routing": {"exclude": {"lab": ["openai"]}}}));
        let mut params = rig.pin_params();
        params["pin_fallback"] = json!("none");
        let reply = rig.select(params).await;
        assert_eq!(
            reply["result"]["selected"]["capacityKey"], "fallback/safe",
            "{reply}"
        );
        assert_eq!(
            reply["result"].get("pin_rejected"),
            Some(&serde_json::Value::Null)
        );
    }

    /// `exclude` keeps a route out of automatic choice only. For every group, an
    /// unpinned request from the fenced consumer never gets the route, while a
    /// launch that names it with `model=` is admitted.
    #[tokio::test]
    async fn launch_model_pin_admits_a_route_every_exclude_group_fences_from_automatic_choice() {
        let _guard = crate::config_overrides::multiplier_map_guard();
        for group in ["all", "mason", "sidekick", "athena", "lab", "core"] {
            let rig = PolicyRig::new(json!({"model_routing": {
                "exclude": {group: ["openai"]},
            }}));
            let consumers: &[&str] = if group == "all" {
                &["mason", "sidekick", "athena", "lab", "core"]
            } else {
                &[group]
            };
            for consumer in consumers {
                let mut params = rig.params();
                params["consumer"] = json!(consumer);
                let automatic = rig.select(params.clone()).await;
                assert_eq!(
                    automatic["result"]["selected"]["capacityKey"], "fallback/safe",
                    "group={group} consumer={consumer}: {automatic}"
                );
                assert_eq!(
                    rejection_reason(&automatic, "openai/pin").as_deref(),
                    Some("excluded_by_operator"),
                    "group={group} consumer={consumer}: {automatic}"
                );
                params["modelPin"] = json!("openai/pin");
                let pinned = rig.select(params).await;
                assert_eq!(
                    pinned["result"]["selected"]["capacityKey"], "openai/pin",
                    "group={group} consumer={consumer}: {pinned}"
                );
                assert!(pinned.get("pin_rejected").is_none(), "{pinned}");
                assert!(pinned["result"].get("pin_rejected").is_none(), "{pinned}");
            }
        }
    }

    /// Lifting the exclude refusal for a named model leaves the real
    /// availability and quota walls in place.
    #[tokio::test]
    async fn launch_model_pin_of_an_excluded_route_still_meets_availability_and_quota_walls() {
        let _guard = crate::config_overrides::multiplier_map_guard();
        let rig = PolicyRig::new(json!({"model_routing": {"exclude": {"all": ["openai"]}}}));
        let mut params = rig.params();
        params["consumer"] = json!("mason");
        params["modelPin"] = json!("openai/pin");
        params["availableModels"] = json!(["fallback/safe"]);
        let unavailable = rig.select(params.clone()).await;
        assert_eq!(unavailable["code"], "not_available", "{unavailable}");
        params["availableModels"] = json!(["openai/pin", "fallback/safe"]);
        rig.store
            .update_route_state(
                "openai/pin",
                "openai/pin",
                &prefrontal_routing_core::RouteStateUpdate {
                    quota_exhausted_until: Some(now_ms() as f64 + 3_600_000.0),
                    ..Default::default()
                },
            )
            .unwrap();
        let walled = rig.select(params).await;
        assert_eq!(walled["code"], "quota_exhausted", "{walled}");
    }

    #[tokio::test]
    async fn pin_cannot_spend_weekly_head_reserve() {
        let _guard = crate::config_overrides::multiplier_map_guard();
        let mut config = planner_config();
        config["model_routing"]["keep_free_percent"] = json!({"openai": 20});
        let rig = PolicyRig::new(config);
        let now = now_ms();
        *rig.quota.lock().unwrap() = QuotaCache {
            available: true,
            refreshed_at_ms: now,
            windows: vec![usage(json!({"provider": "codex", "usage": {
                "primary": {"usedPercent": 0.0, "windowMinutes": 300,
                    "resetsAt": rfc3339_utc(now + 3_600_000)},
                "secondary": {"usedPercent": 95.0, "windowMinutes": 10080,
                    "resetsAt": rfc3339_utc(now + 86_400_000)}
            }}))],
            ..Default::default()
        };
        let mut params = rig.pin_params();
        params["targetAgent"] = json!("mason");
        params["providerAuthTypes"] = json!({"openai": "oauth"});
        let reply = rig.select(params).await;
        assert_eq!(
            reply["result"]["pin_rejected"], "subscription_reserve",
            "{reply}"
        );
        assert_eq!(
            reply["result"]["selected"]["capacityKey"], "fallback/safe",
            "{reply}"
        );
    }

    #[tokio::test]
    async fn fallback_does_not_relax_the_pin_that_admission_refused() {
        let _guard = crate::config_overrides::multiplier_map_guard();
        let rig = PolicyRig::new(planner_config());
        rig.store
            .update_route_state(
                "openai/pin",
                "openai/pin",
                &prefrontal_routing_core::RouteStateUpdate {
                    quota_exhausted_until: Some(now_ms() as f64 + 3_600_000.0),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut params = rig.pin_params();
        params["availableModels"] = json!(["openai/pin"]);
        let reply = rig.select(params).await;
        assert!(reply.get("result").is_none(), "{reply}");
        assert_eq!(reply["pin_rejected"], "quota_exhausted", "{reply}");
    }

    #[test]
    fn consumer_and_pin_shapes_refuse_unknown_values_and_mixed_pins() {
        for params in [
            json!({"consumer": "explore"}),
            json!({"pin": {"slot": "synthesizer"}}),
            json!({"pin": {"slot": "lab_planner", "model": "openai/pin"}}),
            json!({"pin": {"model": "openai/pin", "extra": true}}),
            json!({"pin_fallback": "maybe"}),
        ] {
            assert!(
                serde_json::from_value::<SelectParams>(params.clone()).is_err(),
                "{params}"
            );
        }
    }
}
