use super::*;

#[tokio::test]
async fn absent_settings_keeps_runners_unenrolled_but_installs_safe_billing_and_quality_defaults() {
    let harness = Harness::new("zero-config");

    let mut facts = Vec::new();
    for (provider, method, price) in [
        ("anthropic", "oauth", 12.0),
        ("anthropic-platform", "apikey", 1.0),
        ("openrouter", "apikey", 2.0),
    ] {
        let mut fact = catalog_fact(&fixture::catalog().models[0], 0);
        fact.provider_id = provider.into();
        fact.model_id = "zero-config-model".into();
        fact.cost_per_m_tok_out = Some(price);
        fact.billing_plane = Some(striatum_core::billing::BillingPlane {
            provider_id: provider.into(),
            auth_method: method.into(),
            kind: "upstream".into(),
            selection_reason: "zero-config fixture".into(),
            rates: [(
                "rate.output".into(),
                striatum_core::billing::BillingRate::Priced { rate: price },
            )]
            .into_iter()
            .collect(),
        });
        facts.push(fact);
    }
    rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .execute("DELETE FROM model_facts", [])
        .unwrap();
    harness.install_facts(&facts);

    let modules = json!([
        {"module_id":"prefrontal-core", "nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[
                {"name":"mason"}, {"name":"sidekick"}, {"name":"head"},
                {"name":"codemode"}, {"name":"flow"}
            ]}}},
        {"module_id":"callosum", "nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[{"name":"phone.media"}]}}},
        {"module_id":"magic-context", "nonce_verified":true,
            "role_declarations":{"routing/v1":{"consumers":[{"name":"mc_summary"}]}}},
        {"module_id":"broca", "nonce_verified":true,
            "role_declarations":{"routing/v1":{"runner":{
                "follows_account_choice":true,
                "pairs":[{"wire_api":"anthropic.messages", "auth_method":"oauth"},
                    {"wire_api":"anthropic.messages", "auth_method":"apikey"}],
                "harnesses":["broca"]
            }}}},
        {"module_id":"thalamus", "nonce_verified":true,
            "role_declarations":{"routing/v1":{"runner":{
                "follows_account_choice":true,
                "pairs":[{"wire_api":"anthropic.messages", "auth_method":"oauth"},
                    {"wire_api":"anthropic.messages", "auth_method":"apikey"}],
                "harnesses":["opencode", "pi", "claude-code"], "default_harness":"opencode"
            }}}}
    ]);
    crate::routing_v1::consumers::replace_catalog(&modules);
    let shared = harness.credentials();
    {
        let mut credentials = shared.lock().unwrap();
        credentials.runner_declarations =
            crate::routing_v1::runners::declarations_from_catalog(&modules);
        assert_eq!(credentials.runner_declarations.len(), 2);
        credentials.wire_apis = facts
            .iter()
            .map(|fact| {
                (
                    format!("{}/{}", fact.provider_id, fact.model_id),
                    "anthropic.messages".into(),
                )
            })
            .collect();
        let catalog = crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts)
            .with_wire_apis(&credentials.wire_apis);
        credentials.publish_billing_catalog(catalog);
        // Positive fixture control only: enrolling Broca in an in-memory
        // registry reaches every row. The missing settings file is untouched.
        let enrolled = crate::routing_v1::runners::Registry::build(
            vec![crate::config_policy::RunnerPolicy {
                harness: None,
                module: "broca".into(),
                routes: None,
                tool_capabilities: vec![],
                via_vault: true,
            }],
            &credentials,
            &facts,
            crate::now_ms(),
        );
        assert_eq!(
            enrolled.policies()[0].routes.as_ref().unwrap().len(),
            3,
            "the no-file refusal must not be explained by missing credentials or wire support"
        );
    }

    // The general harness opts fixture providers into automatic selection.
    // A different, never-read path exercises first-install defaults, rather
    // than settings retained after deletion of a previously loaded file.
    std::fs::remove_file(harness.config_path.lock().unwrap().clone()).unwrap();
    let path = harness.dir.join("striatum.jsonc");
    *harness.config_path.lock().unwrap() = path.clone();
    assert!(!path.exists());
    let config = harness.operator_config();
    assert!(config.v1_consumers().is_empty());
    // A launch nonce attests installation, not first-party ownership. Until the
    // daemon publishes that distinction, declarations alone cannot enroll runners.
    assert!(config.v1_runners().is_empty());
    let consumers = crate::routing_v1::consumers::effective(&config);
    for (owner, names) in [
        (
            "prefrontal-core",
            &["mason", "sidekick", "head", "codemode", "flow"][..],
        ),
        ("callosum", &["phone.media"][..]),
        ("magic-context", &["mc_summary"][..]),
    ] {
        let caller = harness.bind(reserved(owner), None).await;
        for consumer in names {
            assert!(
                consumers.knows(consumer),
                "catalog consumer {consumer} was not registered"
            );
            let (ty, reply) = caller
                .call(
                    "route.select",
                    &json!({
                        "consumer":consumer, "requirements":{"iq":80}, "detail":true
                    }),
                )
                .await;
            assert_eq!(ty, FrameType::Error, "{reply}");
            assert_eq!(reply["code"], "no_route", "{reply}");
            assert_eq!(reply["detail"]["reason"], "no_available_model", "SURPRISING: a declared consumer and declared Broca runner still cannot route without operator runner enrollment: {reply}");
        }
    }

    for (runner, harnesses) in [
        ("broca", &["broca"][..]),
        ("thalamus", &["opencode", "pi", "claude-code"][..]),
    ] {
        let caller = harness.bind(reserved(runner), None).await;
        // account_for has no harness member: all harnesses under a runner share
        // the attested module's authorization, even when their pairs differ.
        for fact in &facts {
            let (ty, reply) = caller
                .call(
                    "route.account_for",
                    &json!({
                        "provider":fact.provider_id, "model":fact.model_id,
                        "conversation_key":"zero-config", "admission_id":fact.provider_id
                    }),
                )
                .await;
            assert_eq!(ty, FrameType::Error, "{reply}");
            assert_eq!(reply["code"], "account_not_authorized", "SURPRISING: declaring {runner} with harnesses {harnesses:?} does not authorize account admission: {reply}");
        }
    }

    let overrides = config.apply(&mut facts);
    assert_eq!(overrides.applied, 0);
    let routing = overrides.routing_config;
    assert_eq!(routing.demand_exponent, 0.35);
    assert_eq!(routing.overqualification.threshold, 40.0);
    assert_eq!(routing.overqualification.doubling, 10.0);
    assert_eq!(routing.overqualification.base, 0.05);
    assert!(routing.allow_best_effort);
    assert_eq!(striatum_core::routing_config::ADEQUACY_FLOOR, 0.95);
    assert_eq!(
        config
            .min_band()
            .unwrap_or(striatum_core::routing_config::MIN_BEST_EFFORT_ADEQUACY),
        0.6
    );
    assert!(config.operator_policy().exclude.is_empty());
    assert!(config.operator_policy().subscription.is_empty());
    assert!(config.account_flags().enabled);
    assert!(!config.is_head_agent("head"));
    let allocator = config.allocator_policy("anthropic-platform");
    assert_eq!(
        allocator.strategy,
        striatum_core::accounts::AccountStrategy::Sticky
    );
    assert_eq!(allocator.spend_budget, None);
    assert!(
        allocator.paid_credits,
        "SURPRISING: paid credits are enabled without a configured spending budget"
    );

    for consumer in [
        "mason",
        "sidekick",
        "head",
        "mc_summary",
        "codemode",
        "flow",
    ] {
        let policy = config.consumer_policy(consumer, &Default::default(), &consumers.policies);
        assert_eq!(
            policy.below_bar.unwrap_or_default(),
            if consumer == "mason" {
                crate::config_policy::BelowBarPolicy::Refuse
            } else {
                crate::config_policy::BelowBarPolicy::BestEffort
            }
        );
        // Code-mode and flow now share subscription eligibility with every
        // ordinary consumer; only an explicit pay_per_token flag restricts it.
        assert!(!crate::select_policy::priced_per_token(
            &policy,
            crate::select_policy::Consumer::from_id(consumer)
        ));
        for fact in &facts {
            let route = format!("{}/{}", fact.provider_id, fact.model_id);
            // The spend fence is billing-plane based even without enrollment:
            // subscriptions stay automatic, metered routes need a pin.
            assert_eq!(
                config.route_allowed_on_plane(consumer, &route, false, fact.billing_plane.as_ref()),
                fact.provider_id == "anthropic",
                "{consumer} / {route}"
            );
            assert!(config.route_allowed_on_plane(
                consumer,
                &route,
                true,
                fact.billing_plane.as_ref()
            ));
        }
    }

    // Credential availability is separate from automatic spending permission:
    // pins may still use metered credentials. OAuth establishes subscription
    // coverage without operator-authored subscription labels.
    let credentials = shared.lock().unwrap();
    let providers = facts.iter().collect();
    for fact in &facts {
        let class = account_selection::resolve_class(
            credentials.listed_credentials(),
            fact,
            &providers,
            &credentials,
            &[],
            false,
            false,
            config.metered_fallback(&fact.provider_id),
            |_| true,
        );
        assert_eq!(class.rows.len(), 1, "{}", fact.provider_id);
        assert_eq!(class.subscription, fact.provider_id == "anthropic");
        assert!(!config.metered_fallback(&fact.provider_id));
        assert_eq!(fact.subscription_multiplier, None);
        assert_eq!(config.pricing().keep_free_percent(&fact.provider_id), 0.0);
    }
    drop(credentials);

    let cold_pool = striatum_core::subscription::SubscriptionPool {
        plans: vec![],
        now_ms: crate::now_ms() as i64,
        keep_free_percent: 0.0,
        ..Default::default()
    };
    let price = cold_pool.price(12.0, None, 0.0, false).unwrap();
    assert_eq!(price.value_multiplier, 1.0);
    assert_eq!(price.value_source, "unmeasured");
    assert_eq!(price.plan_pressure, 1.0);
    assert_eq!(
        price.dollars(),
        12.0,
        "SURPRISING: an unmeasured subscription gets no subsidy relative to list price"
    );
    assert_eq!(
        striatum_core::RouteRuntimeState::default().quota_pressure,
        None
    );
    assert!(
        !path.exists(),
        "routing must not create settings to serve these requests"
    );
}

#[tokio::test]
async fn codemode_and_flow_default_to_subscriptions_and_metered_pins() {
    let harness = Harness::new("one-shot-billing-defaults");
    let facts: Vec<_> = [("plan", "oauth", 12.0), ("payg", "apikey", 1.0)]
        .into_iter()
        .map(|(provider, method, price)| {
            let mut fact = catalog_fact(&fixture::catalog().models[0], 0);
            fact.provider_id = provider.into();
            fact.model_id = "model".into();
            fact.cost_per_m_tok_out = Some(price);
            fact.auth_methods = Some(vec![method.into()]);
            fact.billing_plane = Some(striatum_core::billing::BillingPlane {
                provider_id: provider.into(),
                auth_method: method.into(),
                kind: "upstream".into(),
                selection_reason: "one-shot fixture".into(),
                rates: [(
                    "rate.output".into(),
                    striatum_core::billing::BillingRate::Priced { rate: price },
                )]
                .into(),
            });
            fact
        })
        .collect();
    rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .execute("DELETE FROM model_facts", [])
        .unwrap();
    harness.install_facts(&facts);
    // Enroll a runner explicitly: catalog first-party attestation is separate
    // from these billing defaults. Remove the harness's metered opt-ins.
    harness.edit_config(|config| {
        config["providers"] = json!({});
    });
    let caller = harness.bind(reserved("example-consumer"), None).await;
    let described = caller.call("role.describe", &json!({})).await;
    assert_eq!(described.0, FrameType::Response, "{described:?}");
    for consumer in ["codemode", "flow"] {
        let entry = described.1["result"]["consumers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == consumer)
            .unwrap();
        assert_eq!(entry["priced_only"], false);
        let request = json!({"consumer":consumer,"requirements":{"iq":0}});
        let selected = caller.call("route.select", &request).await;
        assert_eq!(selected.0, FrameType::Response, "{selected:?}");
        assert_eq!(
            selected.1["result"]["route"]["provider"], "plan",
            "{selected:?}"
        );
        let mut pinned = request.clone();
        pinned["pin"] = json!("payg/model");
        let selected = caller.call("route.select", &pinned).await;
        assert_eq!(selected.0, FrameType::Response, "{selected:?}");
        assert_eq!(selected.1["result"]["route"]["provider"], "payg");
        pinned["pin"] = json!("plan/model");
        let selected = caller.call("route.select", &pinned).await;
        assert_eq!(selected.0, FrameType::Response, "{selected:?}");
    }
    harness.edit_config(|config| {
        config["consumers"]["codemode"]["pay_per_token"] = json!(true);
    });
    let refused = caller
        .call(
            "route.select",
            &json!({"consumer":"codemode","requirements":{"iq":0},"pin":"plan/model"}),
        )
        .await;
    assert_eq!(refused.0, FrameType::Error, "{refused:?}");
    let described = caller.call("role.describe", &json!({})).await;
    assert!(described.1["result"]["consumers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry["id"] == "codemode" && entry["priced_only"] == true));
}

#[tokio::test]
async fn absent_settings_keeps_remote_media_engines_automatic() {
    use cortexkit_claustrum_client::{ListedCredential, ListedGrant, ScopedCredentialListing};
    let harness = Harness::new("zero-config-media");
    let path = harness.dir.join("never-loaded-striatum.jsonc");
    *harness.config_path.lock().unwrap() = path.clone();
    assert!(!path.exists());
    // Media engines have no subscription alternative or pin operation. The
    // LLM-only spend fence must not disable an installed phone transcription path.
    crate::routing_v1::consumers::replace_catalog(&json!([
        {"module_id":"callosum","nonce_verified":true,"role_declarations":{
            "routing/v1":{"consumers":[{"name":"phone.media"}]},
            "media-provider/v1":{"engines":[{"kind":"transcribe","provider":"openai",
                "model":"gpt-transcribe","local":false,"formats":["audio/wav"],
                "languages":["en"],"latency_class":"interactive"}]}
        }}
    ]));
    let mut credentials = crate::broca_credentials::BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "zero-config-media".into(),
            credentials: vec![ListedCredential {
                id: "phone-transcription-key".into(),
                categories: vec!["media-provider".into()],
                credential_type: "apikey".into(),
                auth_method: Some("apikey".into()),
                state: "active".into(),
                provider_ids: vec!["openai".into()],
                ..Default::default()
            }],
            grant_tuples: vec![ListedGrant {
                selector_kind: "category".into(),
                selector: "media-provider".into(),
                operation: "list".into(),
            }],
            ..Default::default()
        },
        crate::now_ms(),
    );
    harness.set_credentials(credentials);
    let caller = harness.bind(reserved("callosum"), None).await;
    let selected = caller.call("route.select_engine", &json!({
        "consumer":"phone.media", "kind":"transcribe", "requirements":{}, "privacy":"remote_allowed"
    })).await;
    assert_eq!(selected.0, FrameType::Response, "{selected:?}");
    assert_eq!(selected.1["result"]["engine"]["provider"], "openai");
    assert_eq!(selected.1["result"]["engine"]["model"], "gpt-transcribe");
    assert!(harness.operator_config().v1_runners().is_empty());
    assert!(harness
        .operator_config()
        .operator_policy()
        .exclude
        .is_empty());
    assert!(
        !path.exists(),
        "media selection must not create routing settings"
    );
}
