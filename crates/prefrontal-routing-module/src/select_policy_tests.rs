mod consumer_and_pin {
    use super::*;

    struct PolicyRig {
        _rig: Rig,
        store: SharedStore,
        path: PathBuf,
        quota: SharedQuotaCache,
    }

    impl PolicyRig {
        fn new(mut config: serde_json::Value) -> Self {
            // These launch fixtures exercise caller-supplied `availableModels`.
            // They name no workload, so that list decides availability; the
            // OpenCode mapping keeps the same list authoritative for any
            // request that does name a workload.
            config["model_routing"]["substrates"] = json!({"all": {"default": "opencode"}});
            let rig = Rig::new("consumer-and-pin");
            let store = Arc::new(rig.open_store());
            let path = rig.dir.join("routing.jsonc");
            std::fs::write(&path, config.to_string()).unwrap();
            for (provider, model, cost) in [("openai", "pin", 0.1), ("fallback", "safe", 10.0)] {
                let mut fact = fact(provider, model);
                fact.cost_per_m_tok_out = Some(cost);
                store.upsert_fact(&fact, FactSource::Override).unwrap();
            }
            let policy = Self {
                _rig: rig,
                store,
                path,
                quota: Arc::new(Mutex::new(QuotaCache::default())),
            };
            for (provider, model) in [("openai", "pin"), ("fallback", "safe")] {
                policy.set_auth_methods(provider, model, Some(vec!["apikey".into()]));
            }
            policy
        }

        fn set_auth_methods(&self, provider: &str, model: &str, methods: Option<Vec<String>>) {
            use prefrontal_routing_store::{FusiformFactUpdate, FusiformCatalogCursor};
            let fact = self.store.get_fact_by_route_key(&format!("{provider}/{model}")).unwrap().unwrap();
            let update = FusiformFactUpdate {
                provider_id: provider.into(), model_id: model.into(), auth_methods: methods, present: true,
                context_window: fact.context_window, max_output_tokens: fact.max_output_tokens.map(|n| n as u64),
                cost_tier: fact.cost_tier, cost_per_mtok_out: fact.cost_per_m_tok_out,
                input_rate: fact.input_rate, cache_read_rate: fact.cache_read_rate, cache_write_rate: fact.cache_write_rate, reasoning_rate: fact.reasoning_rate,
                supports_vision: fact.supports_vision, supports_tool_use: fact.supports_tool_use,
                reasoning_efforts: None, canonical_id: None, upstream_status: None,
            };
            self.store.apply_fusiform_catalog(&[update], FusiformCatalogCursor { catalog_version: 1, resolved_at_ms: 1 }).unwrap();
        }

        fn params(&self) -> serde_json::Value {
            json!({
                "targetAgent": "oracle", "consumer": "lab",
                "harness": "opencode",
                "requirements": {"iq": 30, "eq": 0},
                "availableModels": ["openai/pin", "fallback/safe"],
            })
        }

        async fn select(&self, params: serde_json::Value) -> serde_json::Value {
            self.select_with_planes(params, &[]).await
        }

        async fn select_with_planes(&self, params: serde_json::Value, planes: &[prefrontal_routing_core::billing::BillingPlane]) -> serde_json::Value {
            self.select_with_catalog(params, planes, json!({"prices": []})).await
        }

        async fn select_with_catalog(&self, params: serde_json::Value, planes: &[prefrontal_routing_core::billing::BillingPlane], plan_prices: serde_json::Value) -> serde_json::Value {
            self.select_with_listing(params, planes, plan_prices, None).await
        }

        async fn select_with_listing(&self, params: serde_json::Value, planes: &[prefrontal_routing_core::billing::BillingPlane], plan_prices: serde_json::Value, listing: Option<cortexkit_claustrum_client::ScopedCredentialListing>) -> serde_json::Value {
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
            let mut facts = self.store.get_facts().unwrap();
            for fact in &mut facts {
                fact.billing_plane = planes.iter().find(|plane| plane.provider_id == fact.provider_id).cloned();
            }
            let mut credentials = BrocaCredentials::api_test_credentials(&facts);
            credentials.publish_plan_prices(&plan_prices);
            if let Some(listing) = listing {
                credentials.apply_listing(&listing, now_ms());
            }
            let credentials = Arc::new(Mutex::new(credentials));
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

    fn billing_plane(provider: &str, auth: &str, kind: &str, output: Option<f64>) -> prefrontal_routing_core::billing::BillingPlane {
        use prefrontal_routing_core::billing::{BillingPlane, BillingRate};
        BillingPlane {
            provider_id: provider.into(), auth_method: auth.into(), kind: kind.into(),
            selection_reason: "test credential".into(),
            rates: output.map(|rate| ("rate.output".into(), BillingRate::Priced { rate })).into_iter().collect(),
        }
    }

    #[tokio::test]
    async fn vault_apikey_admits_catalog_unknown_routes_for_automatic_and_pinned_metered_selection() {
        use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
        let rig = PolicyRig::new(json!({}));
        rig.set_auth_methods("openai", "pin", None);
        let listing = ScopedCredentialListing {
            view: "api-view".into(),
            credentials: vec![ListedCredential {
                id: "opaque".into(),
                credential_type: "apikey".into(),
                state: "active".into(),
                categories: vec!["llm-provider".into()],
                provider_ids: vec!["openai".into()],
                ..Default::default()
            }],
            ..Default::default()
        };
        for consumer in ["codemode", "flow"] {
            for pinned in [false, true] {
                let mut params = rig.params();
                params["consumer"] = json!(consumer);
                params["availableModels"] = json!(["openai/pin"]);
                if pinned {
                    params["modelPin"] = json!("openai/pin");
                }
                let reply = rig.select_with_listing(params, &[], json!({"prices": []}), Some(listing.clone())).await;
                assert_eq!(reply["result"]["selected"]["capacityKey"], "openai/pin", "{reply}");
            }
        }
        assert_eq!(rig.store.get_fact_by_route_key("openai/pin").unwrap().unwrap().auth_methods, None);
    }

    #[tokio::test]
    async fn codemode_prefers_metered_over_higher_scoring_subscription_but_mason_does_not() {
        let rig = PolicyRig::new(json!({}));
        let mut subscription = fact("openai", "pin");
        subscription.scores.elo = 90.0;
        rig.store.upsert_fact(&subscription, FactSource::Override).unwrap();
        let planes = [billing_plane("openai", "chatgpt", "quota_proxy", Some(0.01))];
        for (consumer, expected) in [("codemode", "fallback/safe"), ("mason", "openai/pin")] {
            let mut params = rig.params();
            params["consumer"] = json!(consumer);
            let reply = rig.select_with_planes(params, &planes).await;
            assert_eq!(reply["result"]["selected"]["capacityKey"], expected, "{consumer}: {reply}");
        }
        let mut params = rig.params();
        params.as_object_mut().unwrap().remove("consumer");
        params["targetAgent"] = json!("codemode");
        let reply = rig.select_with_planes(params, &planes).await;
        assert_eq!(reply["result"]["selected"]["capacityKey"], "openai/pin", "{reply}");
    }

    #[tokio::test]
    async fn flow_never_selects_unpriced_or_monthly_apikey_allowance_even_when_pinned() {
        let rig = PolicyRig::new(json!({}));
        for plane in [
            billing_plane("openai", "apikey", "upstream", None),
            billing_plane("openai", "apikey", "quota_proxy", Some(0.01)),
        ] {
            let mut params = rig.params();
            params["consumer"] = json!("flow");
            let reply = rig.select_with_planes(params.clone(), &[plane.clone()]).await;
            assert_eq!(reply["result"]["selected"]["capacityKey"], "fallback/safe", "{reply}");
            params["availableModels"] = json!(["openai/pin"]);
            let reply = rig.select_with_planes(params.clone(), &[plane.clone()]).await;
            assert_eq!(reply["code"], "no_available_model", "{reply}");
            assert!(reply["message"].as_str().unwrap().contains("consumer flow"), "{reply}");
            params["modelPin"] = json!("openai/pin");
            let reply = rig.select_with_planes(params, &[plane]).await;
            assert!(reply.get("result").is_none(), "{reply}");
        }
    }

    #[tokio::test]
    async fn allowed_providers_drops_winner_and_records_fence_on_selection_and_refusal() {
        let rig = PolicyRig::new(json!({}));
        let absent = rig.select(rig.params()).await;
        assert_eq!(absent["result"]["selected"]["capacityKey"], "openai/pin", "{absent}");
        let absent_row = rig.store.get_decision_by_id(absent["result"]["decisionID"].as_str().unwrap()).unwrap().unwrap();
        assert_eq!(absent_row.allowed_providers_json, None);
        let mut params = rig.params();
        params["allowed_providers"] = json!(["fallback"]);
        let reply = rig.select(params.clone()).await;
        assert_eq!(reply["result"]["selected"]["capacityKey"], "fallback/safe", "{reply}");
        let row = rig.store.get_decision_by_id(reply["result"]["decisionID"].as_str().unwrap()).unwrap().unwrap();
        assert_eq!(row.allowed_providers_json.as_deref(), Some("[\"fallback\"]"));
        params["modelPin"] = json!("openai/pin");
        assert!(rig.select(params.clone()).await.get("result").is_none());
        params.as_object_mut().unwrap().remove("modelPin");
        params["allowed_providers"] = json!(["missing"]);
        params["sendID"] = json!("allowed-refusal");
        let reply = rig.select(params).await;
        assert_eq!(reply["code"], "no_available_model", "{reply}");
        assert!(reply["message"].as_str().unwrap().contains("allowed_providers filter"), "{reply}");
        let row = rig.store.get_decision_by_send_id("allowed-refusal").unwrap().unwrap();
        assert_eq!(row.allowed_providers_json.as_deref(), Some("[\"missing\"]"));
    }

    #[tokio::test]
    async fn unknown_consumer_and_invalid_allowed_providers_are_refused_by_name() {
        let rig = PolicyRig::new(json!({}));
        let mut params = rig.params();
        params["consumer"] = json!("unknown-consumer");
        let reply = rig.select(params).await;
        assert_eq!(reply["code"], "invalid_request", "{reply}");
        assert!(reply["message"].as_str().unwrap().contains("unknown-consumer"), "{reply}");
        for invalid in [json!([]), json!(["openai", "openai"]), json!([""]), json!(["openai/pin"]), json!(null), json!("openai")] {
            let mut params = rig.params();
            params["allowed_providers"] = invalid;
            let reply = rig.select(params).await;
            assert_eq!(reply["code"], "invalid_request", "{reply}");
            assert!(reply["message"].as_str().unwrap().contains("allowed_providers"), "{reply}");
        }
        assert_eq!(rig.decisions(), 0);
    }

    #[tokio::test]
    async fn exclusions_precede_allowed_providers_filter() {
        let rig = PolicyRig::new(json!({"model_routing": {"exclude": {"all": ["openai"]}}}));
        let mut params = rig.params();
        params["allowed_providers"] = json!(["fallback"]);
        let reply = rig.select(params).await;
        assert_eq!(rejection_reason(&reply, "openai/pin").as_deref(), Some("excluded_by_operator"), "{reply}");
    }

    #[tokio::test]
    async fn metered_consumers_fail_closed_when_auth_methods_are_unknown() {
        let rig = PolicyRig::new(json!({}));
        rig.set_auth_methods("openai", "pin", None);
        for consumer in ["codemode", "flow"] {
            let mut params = rig.params();
            params["consumer"] = json!(consumer);
            let reply = rig.select(params).await;
            assert_eq!(reply["result"]["selected"]["capacityKey"], "fallback/safe", "{reply}");
        }
    }

    #[tokio::test]
    async fn plan_membership_vetoes_priced_upstream_api_planes_and_unknown_membership_fails_closed() {
        let rig = PolicyRig::new(json!({}));
        let plane = billing_plane("openai", "apikey", "upstream", Some(0.01));
        for consumer in ["codemode", "flow"] {
            let mut params = rig.params();
            params["consumer"] = json!(consumer);
            let reply = rig.select_with_catalog(params.clone(), &[plane.clone()], json!({"prices": [{"provider_id": "openai", "refusal": {"code": "unpriced"}}]})).await;
            assert_eq!(reply["result"]["selected"]["capacityKey"], "fallback/safe", "{reply}");
            params["modelPin"] = json!("openai/pin");
            let reply = rig.select_with_catalog(params.clone(), &[plane.clone()], json!({"prices": [{"provider_id": "openai"}]})).await;
            assert!(reply.get("result").is_none(), "{reply}");
            params.as_object_mut().unwrap().remove("modelPin");
            for unknown in [json!(null), json!({"prices": [{}]})] {
                let reply = rig.select_with_catalog(params.clone(), &[plane.clone()], unknown).await;
                assert_eq!(reply["code"], "no_available_model", "{reply}");
            }
        }
    }

    #[tokio::test]
    async fn consumer_groups_apply_only_to_the_named_consumer() {
        for group in ["mason", "sidekick", "athena", "lab", "core", "codemode", "flow"] {
            let rig =
                PolicyRig::new(json!({"model_routing": {"exclude": {group: ["openai/pin"]}}}));
            for consumer in ["mason", "sidekick", "athena", "lab", "core", "codemode", "flow"] {
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

    /// The Spec Campaigns V2 designer's route request: the `spec_designer`
    /// pin slot with no fallback route, from the athena consumer.
    fn designer_params(rig: &PolicyRig) -> serde_json::Value {
        let mut params = rig.params();
        params["consumer"] = json!("athena");
        params["pin"] = json!({"slot": "spec_designer"});
        params["pin_fallback"] = json!("none");
        params
    }

    /// Unpinned, this rig selects `openai/pin`; each pin below names
    /// `fallback/safe` so a pinned answer is distinguishable from a normal one.
    #[tokio::test]
    async fn spec_designer_pin_wins_with_its_variant() {
        let rig = PolicyRig::new(json!({"model_routing": {"pins": {
            "spec_designer": {"model": "fallback/safe", "variant": "high"},
        }}}));
        let reply = rig.select(designer_params(&rig)).await;
        assert_eq!(
            reply["result"]["selected"]["capacityKey"], "fallback/safe",
            "{reply}"
        );
        assert_eq!(
            reply["result"]["selected"]["promptParams"]["variant"], "high",
            "{reply}"
        );
        assert_eq!(
            reply["result"].get("pin_rejected"),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(rig.decisions(), 1);
    }

    #[tokio::test]
    async fn unset_spec_designer_pin_routes_normally_and_ignores_the_synthesizer_pin() {
        let rig = PolicyRig::new(json!({"model_routing": {"pins": {
            "synthesizer": {"model": "fallback/safe", "variant": "high"},
        }}}));
        let reply = rig.select(designer_params(&rig)).await;
        assert_eq!(
            reply["result"]["selected"]["capacityKey"], "openai/pin",
            "{reply}"
        );
        assert_eq!(
            reply["result"].get("pin_rejected"),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(rig.decisions(), 1);
    }

    #[tokio::test]
    async fn excluded_spec_designer_pin_is_refused_by_name_without_a_fallback() {
        let rig = PolicyRig::new(json!({"model_routing": {"pins": {
            "spec_designer": {"model": "fallback/safe"},
        }}}));
        let mut params = designer_params(&rig);
        params["excludeRouteKeys"] = json!(["fallback/safe"]);
        let reply = rig.select(params).await;
        assert_eq!(
            reply["result"]["pin_rejected"], "caller_excluded",
            "{reply}"
        );
        assert_eq!(
            reply["result"]["selected"],
            serde_json::Value::Null,
            "{reply}"
        );
        assert_eq!(rig.decisions(), 0);
    }

    /// `exclude` keeps a route out of automatic choice only. For every group, an
    /// unpinned request from the fenced consumer never gets the route, while a
    /// launch that names it with `model=` is admitted.
    #[tokio::test]
    async fn launch_model_pin_admits_a_route_every_exclude_group_fences_from_automatic_choice() {
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
        let rig = PolicyRig::new(json!({"model_routing": {"exclude": {"all": ["openai"]}}}));
        let mut params = rig.params();
        params["consumer"] = json!("mason");
        params["modelPin"] = json!("openai/pin");
        params["availableModels"] = json!(["fallback/safe"]);
        let unavailable = rig.select(params.clone()).await;
        // These requests name no workload, so they are host-run selections and
        // keep the refusal code hosts have always received for an unlisted pin.
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

    /// Give `openai/pin` a curated default variant, the last rule a selection uses.
    fn with_curated_variant(rig: &PolicyRig, variant: &str) {
        let mut pin = fact("openai", "pin");
        pin.cost_per_m_tok_out = Some(0.1);
        pin.prompt_defaults = Some(prefrontal_routing_core::model_facts::ModelPromptDefaults {
            variant: Some(variant.into()),
            ..Default::default()
        });
        rig.store.upsert_fact(&pin, FactSource::Override).unwrap();
    }

    fn variant_of(reply: &serde_json::Value) -> (String, String) {
        (
            reply["result"]["selected"]["promptParams"]["variant"]
                .as_str()
                .unwrap_or("(none)")
                .to_string(),
            reply["result"]["variantSelection"]["source"]
                .as_str()
                .unwrap_or("(none)")
                .to_string(),
        )
    }

    /// A selection runs at the caller's named variant, else the consumer's
    /// group, else `all`, else the route's curated default.
    #[tokio::test]
    async fn variant_precedence_runs_pin_then_consumer_then_all_then_curated() {
        let rig = PolicyRig::new(json!({"model_routing": {"variants": {
            "all": {"openai/pin": "high"},
            "mason": {"openai/pin": "xhigh"},
        }}}));
        with_curated_variant(&rig, "medium");
        let select = |consumer: Option<&str>, pin: Option<serde_json::Value>| {
            let mut params = rig.params();
            match consumer {
                Some(consumer) => params["consumer"] = json!(consumer),
                None => {
                    params.as_object_mut().unwrap().remove("consumer");
                }
            }
            if let Some(pin) = pin {
                params["pin"] = pin;
                params["pin_fallback"] = json!("none");
            }
            rig.select(params)
        };
        let pinned = select(
            Some("mason"),
            Some(json!({"model": "openai/pin", "variant": "low"})),
        )
        .await;
        assert_eq!(variant_of(&pinned), ("low".into(), "pin".into()), "{pinned}");
        let consumer = select(Some("mason"), None).await;
        assert_eq!(
            variant_of(&consumer),
            ("xhigh".into(), "consumer".into()),
            "{consumer}"
        );
        // Neither a consumer without a group entry nor a request naming no
        // consumer reads another consumer's group.
        for other in [Some("sidekick"), None] {
            let all = select(other, None).await;
            assert_eq!(variant_of(&all), ("high".into(), "all".into()), "{all}");
        }
        // The decision log names the rule that chose the variant.
        let mut params = rig.params();
        params["consumer"] = json!("mason");
        params["sendID"] = json!("variant-consumer");
        rig.select(params).await;
        let logged = rig
            .store
            .get_decision_by_send_id("variant-consumer")
            .unwrap()
            .unwrap()
            .explanation_json
            .unwrap();
        assert!(
            logged.contains(
                "variant xhigh for openai/pin from model_routing.variants for the consumer"
            ),
            "{logged}"
        );

        let curated = PolicyRig::new(json!({"model_routing": {"variants": {
            "athena": {"openai/pin": "xhigh"},
        }}}));
        with_curated_variant(&curated, "medium");
        let mut params = curated.params();
        params["consumer"] = json!("mason");
        let reply = curated.select(params).await;
        assert_eq!(
            variant_of(&reply),
            ("medium".into(), "curated".into()),
            "{reply}"
        );
    }

    /// The operator's own split: one route, two consumers, two variants.
    #[tokio::test]
    async fn the_same_route_runs_xhigh_for_athena_and_high_for_mason() {
        let rig = PolicyRig::new(json!({"model_routing": {"variants": {
            "athena": {"openai/pin": "xhigh"},
            "mason": {"openai/pin": "high"},
        }}}));
        for (consumer, expected) in [("athena", "xhigh"), ("mason", "high")] {
            let mut params = rig.params();
            params["consumer"] = json!(consumer);
            let reply = rig.select(params).await;
            assert_eq!(
                variant_of(&reply),
                (expected.into(), "consumer".into()),
                "{consumer}: {reply}"
            );
        }
    }

    fn publish_efforts(store: &SharedStore, version: i64, efforts: &[&str]) {
        store
            .apply_fusiform_catalog(
                &[prefrontal_routing_store::FusiformFactUpdate {
                    provider_id: "openai".into(),
                    model_id: "pin".into(),
                    auth_methods: None,
                    present: true,
                    context_window: Some(200_000.0),
                    max_output_tokens: None,
                    cost_tier: Some(2.0),
                    cost_per_mtok_out: Some(0.1),
                    input_rate: None,
                    cache_read_rate: None,
                    cache_write_rate: None,
                    reasoning_rate: None,
                    supports_vision: Some(true),
                    supports_tool_use: Some(true),
                    reasoning_efforts: Some(efforts.iter().map(|e| (*e).to_string()).collect()),
                    canonical_id: None,
                    upstream_status: None,
                }],
                prefrontal_routing_store::FusiformCatalogCursor {
                    catalog_version: version,
                    resolved_at_ms: version,
                },
            )
            .unwrap();
    }

    /// A variant the catalog stops publishing after load does not fail the
    /// selection: the next rule runs and the skip is in the reply and the log.
    #[tokio::test]
    async fn a_variant_the_catalog_drops_falls_back_and_is_logged() {
        let rig = PolicyRig::new(json!({"model_routing": {"variants": {
            "all": {"openai/pin": "high"},
            "mason": {"openai/pin": "xhigh"},
        }}}));
        publish_efforts(&rig.store, 1, &["low", "medium", "high", "xhigh"]);
        let catalog_store = Arc::clone(&rig.store);
        config_overrides::attach_reasoning_catalog(
            &rig.path,
            Arc::new(move || {
                catalog_store
                    .reasoning_catalog()
                    .map_err(|error| error.to_string())
            }),
        )
        .unwrap();
        let mut params = rig.params();
        params["consumer"] = json!("mason");
        params["sendID"] = json!("before-drop");
        let before = rig.select(params.clone()).await;
        assert_eq!(variant_of(&before), ("xhigh".into(), "consumer".into()), "{before}");

        publish_efforts(&rig.store, 2, &["low", "medium", "high"]);
        params["sendID"] = json!("after-drop");
        let after = rig.select(params).await;
        assert_eq!(variant_of(&after), ("high".into(), "all".into()), "{after}");
        let skipped = "model_routing.variants.mason.openai/pin = xhigh skipped for openai/pin: the catalog no longer publishes it (published: low, medium, high)";
        assert_eq!(
            after["result"]["variantSelection"]["fallbacks"],
            json!([skipped]),
            "{after}"
        );
        let logged = rig
            .store
            .get_decision_by_send_id("after-drop")
            .unwrap()
            .unwrap()
            .explanation_json
            .unwrap();
        assert!(
            logged.contains(&format!("variant fallback: {skipped}")),
            "{logged}"
        );
    }

    #[tokio::test]
    async fn pin_cannot_spend_weekly_head_reserve() {
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
