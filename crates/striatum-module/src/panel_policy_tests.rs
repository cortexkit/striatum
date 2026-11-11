mod class_panel_policy {
    use super::*;

    struct PanelRig {
        _rig: Rig,
        store: SharedStore,
        path: PathBuf,
    }

    impl PanelRig {
        fn new(mut config: serde_json::Value) -> Self {
            if config.get("model_routing").is_none() {
                config["model_routing"] = json!({});
            }
            config["model_routing"]["demand_exponent"] = json!(1.0);
            let rig = Rig::new("class-panel-policy");
            let store: SharedStore = Arc::new(rig.open_store());
            let path = rig.dir.join("routing.jsonc");
            crate::test_config::write(&path, config.to_string()).unwrap();
            for (provider, model, family, elo) in [
                ("openai", "gpt-6-astra", "gpt", 1800.0),
                ("kimi-code-plan-global", "k3", "kimi", 1750.0),
                ("anthropic", "a", "claude", 1700.0),
                ("anthropic", "b", "claude", 1690.0),
                ("google", "g", "gemini", 1680.0),
                ("xai", "g", "grok", 1650.0),
                ("low", "small", "qwen", 100.0),
            ] {
                let mut row = fact(provider, model);
                row.family = Some(family.into());
                row.scores.elo = elo;
                store.upsert_fact(&row, FactSource::Override).unwrap();
            }
            Self {
                _rig: rig,
                store,
                path,
            }
        }

        fn params(&self, class: &str) -> serde_json::Value {
            json!({
                "panelSpec": {"class": class},
                "availableModels": ["openai/gpt-6-astra", "kimi-code-plan-global/k3",
                    "anthropic/a", "anthropic/b", "google/g", "xai/g", "low/small"],
                "taskId": "audit-consult",
            })
        }

        async fn select(&self, params: serde_json::Value) -> serde_json::Value {
            let frame = Frame::build(
                FrameType::Request,
                Flags::new(false, Priority::Interactive, false),
                7,
                1,
                99,
                serde_json::to_vec(&json!({"method": ROUTE_SELECT_PANEL_OP, "params": params}))
                    .unwrap(),
            )
            .unwrap();
            let (tx, mut rx) = mpsc::channel(1);
            handle_select_request(
                frame,
                1,
                &tx,
                &Arc::new(Mutex::new(QuotaCache::default())),
                &self.store,
                &Arc::new(Mutex::new(None)),
                &self.path,
                &self.path,
                &empty_broca_credentials(),
            )
            .await
            .unwrap();
            serde_json::from_slice(&rx.recv().await.unwrap().body).unwrap()
        }

        fn edit(&self, config: serde_json::Value, mtime: u64) {
            crate::test_config::write(&self.path, config.to_string()).unwrap();
            set_config_mtime(&self.path, mtime);
        }
    }

    fn routes(reply: &serde_json::Value) -> Vec<&str> {
        reply["result"]["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|member| member["routeKey"].as_str().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn every_automatic_request_runs_audit_policy_and_ignores_classifier_hints() {
        let rig = PanelRig::new(json!({"model_routing": {"exclude": {
            "mason": ["openai/gpt-6-astra", "kimi-code-plan-global/k3"]
        }}}));
        for selection in [None, Some("n_pick")] {
            for size in [1, 5, 99] {
                let mut params = rig.params("AUDIT");
                params["panelSpec"] = json!({"class": "AUDIT", "size": size,
                    "iqFloor": 999, "diversity": "none", "temperatureSpread": [0.1, 0.2, 0.3]});
                if let Some(selection) = selection {
                    params["selection"] = json!(selection);
                }
                let reply = rig.select(params).await;
                assert_eq!(
                    routes(&reply),
                    [
                        "openai/gpt-6-astra",
                        "kimi-code-plan-global/k3",
                        "anthropic/a"
                    ]
                );
                let policy = &reply["result"]["policy"];
                assert_eq!(policy["selection"], "class_policy");
                assert_eq!(policy["class"], "audit");
                assert_eq!(policy["width"], 3);
                assert_eq!(policy["floor"], 80);
                assert_eq!(policy["seed"], "audit-consult");
                assert_eq!(
                    policy["seats"],
                    json!([
                        {"slot": 0, "routeKey": "openai/gpt-6-astra", "reason": "topK", "rank": 1},
                        {"slot": 1, "routeKey": "kimi-code-plan-global/k3", "reason": "topK", "rank": 2},
                        {"slot": 2, "routeKey": "anthropic/a", "reason": "extra", "rank": null},
                    ])
                );
                let members = reply["result"]["members"].as_array().unwrap();
                let families: HashSet<_> = members
                    .iter()
                    .map(|m| m["family"].as_str().unwrap())
                    .collect();
                assert_eq!(families.len(), 3);
                for (member, temperature) in members.iter().zip([0.1, 0.2, 0.3]) {
                    assert_eq!(member["temperature"], temperature);
                }
                assert!(!reply["result"]["runnerUp"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|spare| spare["phase"] == "extra" && spare["routeKey"] == "low/small"));
            }
        }
        let decisions = rig.store.get_decisions_for_task("audit-consult").unwrap();
        assert_eq!(decisions.len(), 6);
        let recorded: serde_json::Value =
            serde_json::from_str(decisions[0].explanation_json.as_ref().unwrap()).unwrap();
        assert_eq!(recorded["selection"], "class_policy");
        assert_eq!(recorded["policy"]["width"], 3);
    }

    #[tokio::test]
    async fn replay_athena_and_spec_panels() {
        let _clock = crate::InjectedClock::set(1_800_000_000_000);
        let _inputs = striatum_store::replay::Inputs::new(1_800_000_000_000).install();
        let config = json!({"model_routing": {"variants": {
            "athena": {"openai/gpt-6-astra": "xhigh", "anthropic/a": "high"}
        }}});
        let rig = PanelRig::new(config.clone());
        let mut panels = Vec::new();
        for (name, class) in [("Athena audit", "AUDIT"), ("Spec round zero", "SPEC")] {
            let mut request = rig.params(class);
            request["seed"] = json!("explicit-seed");
            let reply = rig.select(request.clone()).await;
            assert!(reply["result"]["members"].as_array().is_some_and(|members| !members.is_empty()), "{reply}");
            let budgets: Vec<_> = reply["result"]["members"].as_array().unwrap().iter().map(|member| {
                let key = member["routeKey"].as_str().unwrap();
                let fact = rig.store.get_fact_by_route_key(key).unwrap().unwrap();
                striatum_core::package_ceiling::seat_package_ceiling(
                    key, fact.context_window.map(|n| n as u64), fact.max_output_tokens,
                )
            }).collect();
            panels.push(json!({"name": name, "request": request, "reply": reply, "budgets": budgets}));
        }
        crate::routing_v1::replay_tests::assert_or_record("panels", json!({
            "body_source": "src/panel_policy_tests.rs::PanelRig::params; core consult/runtime.rs::round_zero_select_input uses AUDIT and SPEC",
            "clock_ms": 1_800_000_000_000_u64, "policy": config,
            "catalog": rig.store.get_facts().unwrap(), "panels": panels,
        }));
    }

    #[tokio::test]
    async fn explicit_seed_and_lab_round_seeds_drive_golden_draws() {
        let rig = PanelRig::new(json!({"model_routing": {"panels": {"lab": {
            "topK": 2, "by": "iq", "extra": 1, "extraBy": "random", "floor": 80
        }}}}));
        for (class, seed, extra) in [
            ("AUDIT", "explicit-seed", "anthropic/b"),
            ("lab", "lab-consult#1", "anthropic/a"),
            ("lab", "lab-consult#2", "google/g"),
        ] {
            let mut params = rig.params(class);
            params["seed"] = json!(seed);
            let reply = rig.select(params.clone()).await;
            assert_eq!(routes(&reply)[2], extra, "{reply}");
            assert_eq!(reply["result"]["policy"]["seed"], seed);
            assert_eq!(routes(&rig.select(params).await), routes(&reply));
        }
    }

    /// Athena panels read `variants.athena` for every seat, the extra draw
    /// included; lab panels read `variants.lab`. Each seat's variant and the
    /// rule that chose it are recorded beside it in `seatedVariants`.
    #[tokio::test]
    async fn panel_seats_run_their_class_consumers_variants_including_the_extra_draw() {
        let rig = PanelRig::new(json!({"model_routing": {"variants": {
            "all": {"anthropic": "low"},
            "athena": {"openai/gpt-6-astra": "xhigh", "anthropic/a": "high"},
            "lab": {"openai/gpt-6-astra": "medium"},
            "mason": {"openai/gpt-6-astra": "max", "anthropic/a": "max"},
        }}}));
        let audit = rig.select(rig.params("AUDIT")).await;
        assert_eq!(
            routes(&audit),
            ["openai/gpt-6-astra", "kimi-code-plan-global/k3", "anthropic/a"],
            "{audit}"
        );
        let policy = &audit["result"]["policy"];
        assert_eq!(policy["seats"][2]["reason"], "extra", "{policy}");
        let members = audit["result"]["members"].as_array().unwrap();
        for (slot, variant, source) in [
            (0, Some("xhigh"), "consumer"),
            (1, None, "curated"),
            (2, Some("high"), "consumer"),
        ] {
            assert_eq!(
                members[slot]["promptParams"]["variant"].as_str(),
                variant,
                "slot {slot}: {audit}"
            );
            let seated = &policy["seatedVariants"][slot];
            assert_eq!(seated["slot"], slot);
            assert_eq!(seated["reasoningVariant"].as_str(), variant, "{seated}");
            assert_eq!(seated["reasoningVariantSource"], source, "{seated}");
        }
        // The recorded panel decision carries the same per-seat record.
        let decisions = rig.store.get_decisions_for_task("audit-consult").unwrap();
        let recorded: serde_json::Value =
            serde_json::from_str(decisions[0].explanation_json.as_ref().unwrap()).unwrap();
        assert_eq!(
            recorded["policy"]["seatedVariants"][0]["reasoningVariant"],
            "xhigh"
        );

        let lab = rig.select(rig.params("lab")).await;
        let lab_members = lab["result"]["members"].as_array().unwrap();
        assert!(!lab_members.is_empty(), "{lab}");
        for member in lab_members {
            let expected = match member["routeKey"].as_str().unwrap() {
                "openai/gpt-6-astra" => Some("medium"),
                route if route.starts_with("anthropic/") => Some("low"),
                _ => None,
            };
            assert_eq!(member["promptParams"]["variant"].as_str(), expected, "{lab}");
        }
    }

    #[tokio::test]
    async fn class_exclusions_follow_lab_or_athena_and_never_mason() {
        let rig = PanelRig::new(json!({"model_routing": {"exclude": {
            "athena": ["openai/gpt-6-astra"], "lab": ["kimi-code-plan-global/k3"]
        }}}));
        for class in [
            "AUDIT",
            "DESIGN_REVIEW",
            "DIAGNOSE",
            "EVALUATE",
            "EXPLAIN",
            "SPEC",
            "lab",
        ] {
            let reply = rig.select(rig.params(class)).await;
            let excluded = if class == "lab" {
                "kimi-code-plan-global/k3"
            } else {
                "openai/gpt-6-astra"
            };
            assert!(!routes(&reply).contains(&excluded), "{class}: {reply}");
            assert!(reply["result"]["runnerUp"]
                .as_array()
                .unwrap()
                .iter()
                .all(|spare| spare["routeKey"] != excluded));
            let mut fenced = rig.params(class);
            fenced["availableModels"] = json!([excluded]);
            let refused = rig.select(fenced).await;
            assert!(routes(&refused).is_empty());
            assert!(
                refused["result"]["shortfall"]["causes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|cause| cause["model"] == excluded
                        && cause["reason"] == "excluded_by_operator"),
                "{refused}"
            );
            if class == "lab" {
                assert_eq!(routes(&reply)[0], "openai/gpt-6-astra");
            }
        }
    }

    #[tokio::test]
    async fn migrated_provider_exclude_cannot_be_lifted_by_group_or_caller_negations() {
        let rig = PanelRig::new(json!({"model_routing": {"exclude": {
            "all": ["anthropic"], "athena": ["-anthropic"], "lab": ["-anthropic"]
        }}}));
        for class in ["AUDIT", "lab"] {
            let mut params = rig.params(class);
            params["excludeRouteKeys"] = json!(["-anthropic"]);
            let reply = rig.select(params).await;
            assert!(
                !routes(&reply)
                    .iter()
                    .any(|key| key.starts_with("anthropic/")),
                "{reply}"
            );
            assert!(reply["result"]["runnerUp"]
                .as_array()
                .unwrap()
                .iter()
                .all(|spare| !spare["routeKey"]
                    .as_str()
                    .unwrap()
                    .starts_with("anthropic/")));
            let mut fenced = rig.params(class);
            fenced["availableModels"] = json!(["anthropic/a", "anthropic/b"]);
            fenced["excludeRouteKeys"] = json!(["-anthropic"]);
            let refused = rig.select(fenced).await;
            assert!(routes(&refused).is_empty());
            for route in ["anthropic/a", "anthropic/b"] {
                assert!(
                    refused["result"]["shortfall"]["causes"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|cause| cause["model"] == route
                            && cause["reason"] == "excluded_by_operator"),
                    "{refused}"
                );
            }
        }
    }

    #[tokio::test]
    async fn last_match_within_a_group_still_allows_a_route_exception() {
        let rig = PanelRig::new(json!({"model_routing": {"exclude": {
            "athena": ["anthropic", "-anthropic/b"]
        }}}));
        let mut params = rig.params("AUDIT");
        params["availableModels"] = json!(["anthropic/a", "anthropic/b"]);
        let reply = rig.select(params).await;
        assert_eq!(routes(&reply), ["anthropic/b"]);
    }

    #[tokio::test]
    async fn live_policy_and_synthesizer_pin_edits_change_the_next_selection_after_debounce() {
        let rig = PanelRig::new(json!({"model_routing": {
            "panels": {"audit": {"topK": 1, "by": "iq"}},
            "pins": {"synthesizer": {"model": "synth/old", "variant": "high"}}
        }}));
        let params = rig.params("AUDIT");
        let before = rig.select(params.clone()).await;
        assert_eq!(routes(&before).len(), 1);
        assert_eq!(
            before["result"]["policy"]["pins"]["synthesizer"],
            json!({"model": "synth/old", "variant": "high"})
        );
        assert_eq!(before["result"]["policy"]["floor"], serde_json::Value::Null);
        rig.edit(
            json!({"model_routing": {
                "panels": {"audit": {"topK": 2, "by": "eq"}},
                "pins": {"synthesizer": {"model": "synth/new"}}
            }}),
            2_000_000_001,
        );
        let pending = rig.select(params.clone()).await;
        assert_eq!(pending["result"]["policy"], before["result"]["policy"]);
        crate::config_overrides::settle_live_config(&rig.path);
        let after = rig.select(params.clone()).await;
        assert_eq!(routes(&after).len(), 2);
        assert_eq!(after["result"]["policy"]["by"], "eq");
        assert_eq!(after["result"]["policy"]["extra"], 0);
        assert_eq!(
            after["result"]["policy"]["extraBy"],
            serde_json::Value::Null
        );
        assert_eq!(after["result"]["policy"]["seed"], serde_json::Value::Null);
        assert_eq!(
            after["result"]["policy"]["pins"]["synthesizer"],
            json!({"model": "synth/new", "variant": null})
        );
        let recorded = rig
            .store
            .get_decision_by_id(before["result"]["decisionID"].as_str().unwrap())
            .unwrap()
            .unwrap();
        let stored: serde_json::Value =
            serde_json::from_str(recorded.explanation_json.as_ref().unwrap()).unwrap();
        assert_eq!(stored["policy"], before["result"]["policy"]);
        rig.edit(
            json!({"model_routing": {"panels": {"audit": {"topK": 0, "by": "iq"}}}}),
            2_000_000_002,
        );
        crate::config_overrides::settle_live_config(&rig.path);
        let invalid = rig.select(params).await;
        assert_eq!(invalid["result"]["policy"], after["result"]["policy"]);
    }

    // Snapshot of the previous core reply decoder. Independent types catch wire
    // incompatibilities that reusing routing's own output types could hide.
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[allow(dead_code)]
    struct PreviousPanelMember {
        route_key: String,
        #[serde(default)]
        prompt_params: serde_json::Value,
        #[serde(default)]
        family: Option<String>,
        #[serde(default)]
        temperature: Option<f64>,
        #[serde(default)]
        max_output_tokens: Option<u64>,
        #[serde(default)]
        context_window: Option<u64>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PreviousPanelResult {
        #[serde(default)]
        members: Vec<PreviousPanelMember>,
        #[serde(default)]
        runner_up: Vec<PreviousPanelMember>,
        #[serde(default)]
        shortfall: Option<serde_json::Value>,
        #[serde(default)]
        policy: Option<serde_json::Value>,
        #[serde(rename = "decisionID")]
        decision_id: String,
    }

    #[tokio::test]
    async fn previous_core_decoder_accepts_new_reply_for_request_without_consumer() {
        let rig = PanelRig::new(json!({}));
        let mut params = rig.params("AUDIT");
        params["panelSpec"] =
            json!({"class": "AUDIT", "size": 1, "iqFloor": 0, "diversity": "none"});
        assert!(params.get("consumer").is_none());
        let reply = rig.select(params).await;
        let old: PreviousPanelResult = serde_json::from_value(reply["result"].clone()).unwrap();
        assert_eq!(old.members.len(), 3);
        assert_eq!(old.members[0].route_key, "openai/gpt-6-astra");
        assert!(!old.runner_up.is_empty());
        assert!(old.shortfall.is_none());
        assert_eq!(old.policy.unwrap()["selection"], "class_policy");
        assert_eq!(
            old.decision_id,
            reply["result"]["decisionID"].as_str().unwrap()
        );
    }

    #[tokio::test]
    async fn roster_selection_or_nonempty_roster_is_refused_without_a_decision() {
        let rig = PanelRig::new(json!({}));
        for patch in [
            json!({"selection": "roster"}),
            json!({"roster": [{"model": "openai/gpt-6-astra"}]}),
            json!({"selection": "n_pick", "roster": ["retired-roster-shape"]}),
        ] {
            let mut params = rig.params("AUDIT");
            params
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            let reply = rig.select(params).await;
            assert_eq!(reply["code"], "roster_removed", "{reply}");
        }
        assert!(rig
            .store
            .get_decisions_for_task("audit-consult")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn core_panel_replacement_fixtures_keep_recorded_seats_spares_and_policy() {
        use striatum_core as routing;
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures/routing-wire/panels.json");
        let corpus: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let cases = corpus["cases"].as_array().unwrap();
        assert!(!cases.is_empty());
        for case in cases {
            let input = &case["input"];
            let request = &input["request"];
            let facts: Vec<routing::ModelFact> =
                serde_json::from_value(input["facts"].clone()).unwrap();
            // Match the original core test adapter, which called the scorer
            // directly: class policy replaces the size and floor placeholders.
            let mut spec = request["panelSpec"].clone();
            spec["size"] = json!(1);
            spec["iqFloor"] = json!(0.0);
            let panel_spec: routing::PanelSpec = serde_json::from_value(spec).unwrap();
            let recorded = &input["policy"];
            let policy = if recorded.is_null() {
                routing::ClassPanelPolicy::default_for_class(panel_spec.class)
            } else {
                routing::ClassPanelPolicy {
                    top_k: recorded["topK"].as_u64().unwrap() as usize,
                    by: match recorded["by"].as_str().unwrap() {
                        "iq" => routing::PanelOrder::Iq,
                        "eq" => routing::PanelOrder::Eq,
                        "cost" => routing::PanelOrder::Cost,
                        other => panic!("unknown panel order {other}"),
                    },
                    extra: recorded["extra"].as_u64().unwrap() as usize,
                    extra_by: recorded["extraBy"].as_str().map(|order| match order {
                        "random" => routing::PanelExtraOrder::Random,
                        "iq" => routing::PanelExtraOrder::Iq,
                        "eq" => routing::PanelExtraOrder::Eq,
                        "cost" => routing::PanelExtraOrder::Cost,
                        other => panic!("unknown extra order {other}"),
                    }),
                    floor: serde_json::from_value(recorded["floor"].clone()).unwrap(),
                }
            };
            let context = routing::panel::PanelSelectionContext {
                seed: request["seed"]
                    .as_str()
                    .unwrap_or_else(|| request["taskId"].as_str().unwrap())
                    .into(),
                pins: routing::PanelPins {
                    synthesizer: input["synthesizer_pin"].as_object().map(|pin| {
                        routing::PanelSynthesizerPin {
                            model: pin["model"].as_str().unwrap().into(),
                            variant: pin["variant"].as_str().map(str::to_owned),
                        }
                    }),
                },
                canonical_ids: Default::default(),
            };
            let panel = routing::panel::select_panel_with_policy_context(
                routing::SelectPanelInput {
                    package_tokens: serde_json::from_value(request["packageTokens"].clone()).unwrap(),
                    panel_spec,
                    selection: serde_json::from_value(json!("n_pick")).unwrap(),
                    roster: Vec::new(),
                    available_models: facts
                        .iter()
                        .map(|fact| format!("{}/{}", fact.provider_id, fact.model_id))
                        .collect(),
                    facts,
                    runtime_snapshot: None,
                    exclude_route_keys: serde_json::from_value(request["excludeRouteKeys"].clone())
                        .unwrap(),
                    operator_exclude_keys: Vec::new(),
                    required_capabilities: None,
                    availability_substrate: routing::AvailabilitySubstrate::Broca,
                    availability_label: "served".into(),
                    is_subscription_route: None,
                },
                &routing::DEFAULT_ROUTING_CONFIG,
                &policy,
                &context,
            );
            match panel {
                Ok(panel) => {
                    let mut actual = serde_json::to_value(panel).unwrap();
                    actual["decisionID"] = json!("live-test-panel");
                    let expected = case["result"]["Ok"]
                        .as_object()
                        .expect("recorded successful scorer reply");
                    for (key, value) in expected {
                        assert_eq!(&actual[key], value, "{key}: {request}");
                    }
                }
                Err(error) => assert_eq!(case["result"]["Err"], json!(error.0), "{request}"),
            }
        }
    }
}
