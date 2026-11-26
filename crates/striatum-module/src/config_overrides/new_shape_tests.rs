use super::*;
use crate::config_policy::{ConsumerMode, ConsumerPolicy};
use serde_json::json;

fn config(value: serde_json::Value) -> LiveConfig {
    LiveConfig::new(parse_config(&value.to_string()).unwrap(), None)
}

#[test]
fn athena_slot_rules_use_prefrontal_consumer_root() {
    let error = parse_config(r#"{"consumers":{"synthesizer":{"pin":{"model":"p/m"}}},"providers":{"p":{"not_for":["prefrontal.athena"]}}}"#).unwrap_err();
    assert!(error.contains("prefrontal.athena"), "{error}");
}

#[test]
fn old_consumer_key_is_independent_of_prefrontal_policy() {
    let config = config(json!({"consumers": {
        "prefrontal-core.mason": {"iq": 25},
        "prefrontal.mason": {"iq": 70}
    }}));
    for (consumer, iq) in [("prefrontal-core.mason", 25.0), ("prefrontal.mason", 70.0)] {
        assert_eq!(
            config
                .consumer_policy(consumer, &ConsumerPolicy::default(), &BTreeMap::new())
                .iq,
            Some(iq)
        );
    }
}

#[test]
fn explicit_pin_origin_enrollment_matches_only_the_configured_principal() {
    let config = parse_config(r#"{"pin_origin_callers":["reserved:prefrontal-core"]}"#).unwrap();
    assert_eq!(config.pin_origin_callers, ["reserved:prefrontal-core"]);
    let config = LiveConfig::new(config, None);
    assert!(config.honours_pin_origin("reserved:prefrontal-core"));
    assert!(!config.honours_pin_origin("reserved:prefrontal"));
    assert!(
        !self::config(json!({"pin_origin_callers":[]})).honours_pin_origin("reserved:prefrontal")
    );
}

#[test]
fn mason_defaults_and_explicit_pin_callers() {
    let config = config(json!({"pin_origin_callers":["reserved:operator"]}));
    assert_eq!(
        config
            .consumer_policy(
                "prefrontal.mason.review",
                &Default::default(),
                &BTreeMap::new()
            )
            .below_bar,
        Some(crate::config_policy::BelowBarPolicy::Refuse)
    );
    assert!(config.honours_pin_origin("reserved:operator"));
    assert!(!config.honours_pin_origin("reserved:prefrontal"));
    assert!(!config.honours_pin_origin("reserved:prefrontal-core"));
    assert!(crate::select_policy::Consumer::from_id("prefrontal.mason").is_some());
}

#[test]
fn below_bar_policy_defaults_and_inherits_per_consumer_key() {
    use crate::config_policy::BelowBarPolicy;

    let config = config(json!({"consumers": {
        "workers": {"below_bar": "refuse"},
        "workers.review": {"iq": 80},
        "workers.review.quick": {"below_bar": "best_effort"}
    }}));
    for name in ["workers", "workers.review", "workers.review.deep"] {
        let policy = config.consumer_policy(name, &ConsumerPolicy::default(), &BTreeMap::new());
        assert_eq!(policy.below_bar, Some(BelowBarPolicy::Refuse));
    }
    let policy = config.consumer_policy(
        "workers.review.quick.child",
        &ConsumerPolicy::default(),
        &BTreeMap::new(),
    );
    assert_eq!(policy.below_bar, Some(BelowBarPolicy::BestEffort));
    let other = config.consumer_policy(
        "workers-other",
        &ConsumerPolicy::default(),
        &BTreeMap::new(),
    );
    assert_eq!(
        other.below_bar.unwrap_or_default(),
        BelowBarPolicy::BestEffort
    );
    assert!(parse_config(r#"{"consumers":{"workers":{"below_bar":"ignore"}}}"#).is_err());
}

#[test]
fn dotted_consumers_resolve_name_rules_without_reusing_root_exclusions() {
    let config = config(json!({
        "providers": {"p": {"not_for": ["prefrontal.athena"]}},
        "models": {
            "p/gather": {"not_for": [], "only_for": ["prefrontal.athena.gather"]},
            "q/review": {"not_for": ["prefrontal.athena.gather"]},
            "q/manual": {"pin_only": true},
            "q/athena-manual": {"pin_only_for": ["prefrontal.athena"]}
        }
    }));
    for consumer in [
        "prefrontal.athena.gather",
        "prefrontal.athena.gather.regather",
    ] {
        let exclusions = config.consumer_exclusions(consumer);
        assert_eq!(exclusions, ["p", "-p/gather", "q/review"]);
        assert!(config.route_allowed(consumer, "p/gather", false));
        assert!(!config.route_allowed(consumer, "q/review", false));
        for route in ["q/manual", "q/athena-manual"] {
            assert!(!config.route_allowed(consumer, route, false));
            assert!(config.route_allowed(consumer, route, true));
        }
        assert_eq!(config.exclude_groups().all, ["q/manual"]);
        assert_eq!(
            config.dotted_rule_refusal(consumer, "q/review").as_deref(),
            Some("models[\"q/review\"].not_for lists \"prefrontal.athena.gather\"")
        );
    }
    assert_eq!(
        config.consumer_exclusions("prefrontal.athena"),
        ["p", "p/gather"]
    );
    assert!(!config.route_allowed("prefrontal.athena", "p/gather", false));
    assert!(config.route_allowed("prefrontal.athena", "q/review", false));
    for consumer in ["nosuch", "nosuch.gather"] {
        assert!(config.consumer_exclusions(consumer).is_empty());
        assert!(crate::select_policy::Consumer::from_id(consumer).is_none());
        assert!(!config.route_allowed(consumer, "p/gather", false));
        assert!(config.route_allowed(consumer, "q/review", false));
    }
    assert!(matches!(
        crate::select_policy::Consumer::from_id("prefrontal.athena.gather"),
        Some(crate::select_policy::Consumer::Athena)
    ));
}

#[test]
fn dotted_consumer_settings_keep_the_existing_operator_ancestor_walk() {
    let config = config(json!({"consumers": {
        "athena": {
            "iq": 41, "eq": 52, "service_tier": "fast",
            "workload": "reason", "pin": {"model": "p/model"},
            "projects": {"project": {"pin": "p/project"}}
        },
        "athena.gather": {"iq": 31}
    }}));
    for consumer in ["athena.gather", "athena.gather.regather"] {
        let policy = config.consumer_policy(consumer, &ConsumerPolicy::default(), &BTreeMap::new());
        assert_eq!(policy.iq, Some(31.0));
        assert_eq!(policy.eq, Some(52.0));
        assert_eq!(policy.ladder, Some(vec!["p/model".into()]));
        assert_eq!(config.consumer_workload(consumer), Some("reason"));
        assert_eq!(config.project_pin("project", consumer), Some("p/project"));
        assert_eq!(
            config.service_tier(consumer, "p", "model"),
            Some(cortexkit_role_routing::service_tier::ServiceTier::Fast)
        );
    }
}

#[test]
fn top_level_elo_overrides_is_refused_by_name() {
    for value in [json!(null), json!({}), json!({"p/model": 1400})] {
        let error = parse_config(&json!({"elo_overrides": value}).to_string()).unwrap_err();
        assert!(
            error.contains("config_key_removed: elo_overrides"),
            "{error}"
        );
        assert!(error.contains("models.<id>.elo"), "{error}");
    }
}

#[test]
fn nested_quality_overrides_beat_operator_grades() {
    let config = config(json!({"models": {"p/model": {"elo": 1400, "eq": 1500}}}));
    let mut facts = vec![serde_json::from_value(json!({
        "providerID": "p", "modelID": "model",
        "scores": {"elo": 1284, "eq": 1599.8, "speed": 0},
        "scoreProvenance": "operator_grade", "eqSource": "operator_grade",
        "confidence": "curated"
    }))
    .unwrap()];
    config.apply(&mut facts);
    config.apply(&mut facts);
    assert_eq!(facts[0].scores.elo, 1400.0);
    assert_eq!(facts[0].scores.eq, 1500.0);
    assert_eq!(facts[0].score_provenance, ScoreProvenance::EloOverride);
    assert_eq!(facts[0].eq_source.as_deref(), Some("override"));
}

#[test]
fn model_overlays_and_dormant_keys_use_catalog_model_identity() {
    let clef = "cloudflare-workers-ai/@cf/cloudflare/clef";
    let xhigh = "openai/gpt-6.2-sol/xhigh";
    let missing = "cloudflare-workers-ai/@cf/cloudflare/missing";
    let config = config(json!({"models": {
        (clef): {"elo": 1500},
        (xhigh): {"elo": 1600},
        (missing): {"elo": 1700},
    }}));
    let fact = |provider, model, variant| {
        serde_json::from_value::<ModelFact>(json!({
            "providerID": provider, "modelID": model, "variant": variant,
            "scores": {"elo": 1000, "eq": 1000, "speed": 0}, "confidence": "curated"
        }))
        .unwrap()
    };
    let mut facts = vec![
        fact("cloudflare-workers-ai", "@cf/cloudflare/clef", None),
        fact("cloudflare-workers-ai", "@cf/cloudflare/clef-flash", None),
        fact("openai", "gpt-6.2-sol", Some("xhigh")),
        fact("openai", "gpt-6.2-sol", Some("high")),
    ];
    let catalog = CatalogModelIds::from_facts(&facts);
    assert_eq!(
        parse_model_key(clef, Some(&catalog)),
        Some(("cloudflare-workers-ai", "@cf/cloudflare/clef", None))
    );
    assert_eq!(
        parse_model_key(xhigh, Some(&catalog)),
        Some(("openai", "gpt-6.2-sol", Some("xhigh")))
    );
    assert_eq!(config.dormant_model_keys(&facts), [missing]);
    let outcome = config.apply(&mut facts);
    assert_eq!(outcome.applied, 2);
    assert_eq!(outcome.unknown_keys, [missing]);
    assert_eq!(
        facts.iter().map(|fact| fact.scores.elo).collect::<Vec<_>>(),
        [1500.0, 1000.0, 1600.0, 1000.0]
    );
}

#[test]
fn absent_model_config_is_dormant_and_reported_without_creating_candidates() {
    let config = config(json!({"models": {
        "p/stale": {"elo": 1500, "eq": 1500},
        "p/policy-only": {"pin_only": true}
    }}));
    let mut facts = Vec::new();
    let outcome = config.apply(&mut facts);
    assert!(
        facts.is_empty(),
        "config must not add candidates: {facts:?}"
    );
    assert_eq!(outcome.applied, 0);
    assert_eq!(outcome.unknown_keys, ["p/policy-only", "p/stale"]);
    facts.push(
        serde_json::from_value(json!({
            "providerID": "p", "modelID": "stale",
            "scores": {"elo": 1000, "eq": 1000, "speed": 0}, "confidence": "curated"
        }))
        .unwrap(),
    );
    let warmed = config.apply(&mut facts);
    assert_eq!(warmed.unknown_keys, ["p/policy-only"]);
    assert_eq!(warmed.applied, 1);
    assert_eq!((facts[0].scores.elo, facts[0].scores.eq), (1500.0, 1500.0));
}

#[test]
fn converter_moves_elo_overrides_to_models_with_their_original_precedence() {
    for input in [
        json!({"model_routing": {
            "elo_overrides": {"p/model": 1400},
            "models": {"p/model": {"elo": 900, "eq": 1200}}
        }}),
        json!({
            "elo_overrides": {"p/model": 1400},
            "models": {"p/model": {"elo": 900, "eq": 1200}}
        }),
    ] {
        let converted = crate::config_convert::convert_value(input).unwrap();
        assert!(converted.get("elo_overrides").is_none());
        assert_eq!(converted["models"]["p/model"]["elo"], 1400);
        assert_eq!(converted["models"]["p/model"]["eq"], 1200);
        parse_config(&converted.to_string()).unwrap();
        assert_eq!(
            crate::config_convert::convert_value(converted.clone()).unwrap(),
            converted
        );
    }
}

#[test]
fn old_shape_is_refused_even_when_empty_or_null() {
    for value in [
        json!({"model_routing": {}}),
        json!({"model_routing": null}),
        json!({"account_selection": {"enabled": false}}),
        json!({"score_alias": {}}),
    ] {
        let error = parse_config(&value.to_string()).unwrap_err();
        assert!(error.contains(crate::config_convert::COMMAND), "{error}");
    }
    // The converter cannot convert a top-level pins object, so the loader
    // names the manual move instead of sending the operator to it.
    let error = parse_config(&json!({"pins": {}}).to_string()).unwrap_err();
    assert!(error.contains("config_key_moved: pins"), "{error}");
}

#[test]
fn entity_restrictions_match_dotted_segments_and_pins_cannot_bypass_not_for() {
    let config = config(json!({
        "providers": {"p": {"only_for": ["mc"]}},
        "models": {"p/blocked": {"not_for": ["mc.dream"]}, "p/manual": {"pin_only": true}}
    }));
    assert!(config.route_allowed("mc.dream.curate", "p/allowed", false));
    assert!(!config.route_allowed("mcx", "p/allowed", false));
    assert!(!config.route_allowed("mc.dream.curate", "p/blocked", true));
    assert!(!config.route_allowed("mc.dream.curate", "p/manual", false));
    assert!(config.route_allowed("mc.dream.curate", "p/manual", true));
}

#[test]
fn source_is_chosen_before_specificity_for_each_consumer_key() {
    let config = config(json!({"consumers": {
        "mc": {"mode": "ladder", "runner": "operator-parent"},
        "mc.dream": {"runner": "operator-child"},
        "mc.dream.curate": {"eq": 60.0}
    }}));
    let declared = serde_json::from_value(json!({
        "mc": {"context_tokens": 1000},
        "mc.dream.curate": {"mode": "demand", "eq": 20.0, "context_tokens": 2000}
    }))
    .unwrap();
    let request =
        serde_json::from_value(json!({"mode": "demand", "eq": 40.0, "iq": 70.0})).unwrap();
    let result = config.consumer_policy("mc.dream.curate", &request, &declared);
    assert_eq!(result.mode, Some(ConsumerMode::Ladder));
    assert_eq!(result.runner.as_deref(), Some("operator-child"));
    assert_eq!(result.eq, Some(60.0));
    assert_eq!(result.iq, Some(70.0));
    assert_eq!(result.context_tokens, Some(2000));
    assert!(config
        .consumer_policy("mcx", &ConsumerPolicy::default(), &declared)
        .mode
        .is_none());
}

#[test]
fn conversion_round_trip_keeps_every_policy_and_materializes_data_defaults() {
    let old = json!({"model_routing": {
        "consumers": ["mc.dream.curate", "mason", "codemode"],
        "exclude": {"all": ["p", "-p/open"], "mason": ["p/blocked"]},
        "subscription": ["fireworks-ai", "-fireworks-ai/metered"],
        "pins": {"synthesizer": {"model": "p/model", "variant": "high"}, "lab_planner": {"model": "p/model"}, "spec_designer": {"model": "p/model", "variant": "medium"}},
        "project_pins": {"project": {"mason": "p/model:low"}},
        "score_alias": {"alias": "p"}
    }, "account_selection": {"enabled": false}});
    let converted = crate::config_convert::convert_value(old).unwrap();
    assert_eq!(
        converted["consumers"]["mason"]["projects"]["project"]["pin"],
        "p/model:low"
    );
    assert_eq!(
        converted["models"]["p/blocked"]["not_for"],
        json!(["mason"])
    );
    assert!(converted["consumers"]["codemode"]["pay_per_token"].is_null());
    assert!(converted["consumers"]["flow"]["pay_per_token"].is_null());
    assert!(converted.get("score_alias").is_none());
    assert!(converted.get("pins").is_none());
    let in_memory = parse_config(&converted.to_string()).unwrap();
    let scratch = cortexkit_test_support::ScratchDir::new("converted-round-trip");
    let path = scratch.join("routing.jsonc");
    crate::test_config::write(&path, converted.to_string()).unwrap();
    initialize_config(&path).unwrap();
    let loaded = live_config(&path);
    assert_eq!(
        serde_json::to_value(&in_memory).unwrap(),
        serde_json::to_value(&loaded.config).unwrap()
    );
    assert_eq!(
        loaded
            .slot_pin("synthesizer")
            .and_then(|pin| pin.variant.as_deref()),
        Some("high")
    );
    assert_eq!(
        loaded
            .slot_pin("spec_designer")
            .and_then(|pin| pin.variant.as_deref()),
        Some("medium")
    );
    assert_eq!(loaded.project_pin("project", "mason"), Some("p/model:low"));
    assert!(loaded.account_flags().enabled);
    assert!(converted
        .get("accounts")
        .and_then(|accounts| accounts.get("enabled"))
        .is_none());
    let error = parse_config(r#"{"accounts":{"enabled":false}}"#).unwrap_err();
    assert!(error.contains("accounts.enabled"), "{error}");
    assert!(error.contains(crate::config_convert::COMMAND), "{error}");
    assert!(loaded
        .v1_consumers()
        .iter()
        .all(|name| !crate::config_policy::is_slot(name)));
    let defaults: serde_json::Value =
        serde_json::from_str(include_str!("../../data/config_convert_defaults.json")).unwrap();
    assert_eq!(converted["runners"], defaults["runners"]);
    for provider in defaults["providers_without_prompt_cache"]
        .as_array()
        .unwrap()
    {
        assert_eq!(
            converted["providers"][provider.as_str().unwrap()]["prompt_cache"],
            false
        );
    }
    let operator = loaded.operator_policy();
    let result = striatum_core::launch_policy::resolve_routing_policy(
        [
            "fireworks-ai/new-route".to_owned(),
            "fireworks-ai/metered".to_owned(),
        ],
        &operator,
        &ModelRoutingPolicyInput::default(),
        &HashMap::new(),
    );
    assert!(result
        .subscription_keys
        .contains(&"fireworks-ai/new-route".to_owned()));
    assert!(!result
        .subscription_keys
        .contains(&"fireworks-ai/metered".to_owned()));
    assert_eq!(
        crate::config_convert::convert_value(converted.clone()).unwrap(),
        converted
    );
}

#[test]
fn earlier_entity_config_loses_the_retired_flag_in_one_conversion() {
    for enabled in [json!(false), json!(true), json!(null)] {
        let input = json!({"providers":{"p":{}}, "accounts":{"enabled":enabled}});
        let error = parse_config(&input.to_string()).unwrap_err();
        assert!(error.contains("accounts.enabled"), "{error}");
        assert!(error.contains(crate::config_convert::COMMAND), "{error}");
        let converted = crate::config_convert::convert_value(input).unwrap();
        assert!(converted.get("accounts").is_none());
        assert!(parse_config(&converted.to_string()).is_ok());
    }
}

#[test]
fn converter_preserves_runner_order_and_rejects_map_shape() {
    let converted = crate::config_convert::convert_value(
        json!({"model_routing": {"runners": [{"module": "z-last"}, {"module": "a-first"}]}}),
    )
    .unwrap();
    let config = config(converted);
    assert_eq!(
        config
            .v1_runners()
            .iter()
            .map(|runner| runner.module.as_str())
            .collect::<Vec<_>>(),
        ["z-last", "a-first"]
    );
    assert!(parse_config(r#"{"runners":{"p/*":["runner"]}}"#).is_err());
}

#[test]
fn conversion_discards_removed_family_rules_and_retired_quota_families() {
    let document = crate::config_convert::convert(
        r#"{
        "model_routing": {},
        "model_families": {"other": ["*"], "gpt": ["p/*"]},
        "accounts": {"model_families": [{"model": "claude-fake", "family": "opus"}]}
    }"#,
    )
    .unwrap();
    parse_config(&document).unwrap();
    let value: serde_json::Value = serde_json::from_str(&document).unwrap();
    assert!(value.get("model_families").is_none());
    // Quota families now come from Insula's window ids, so the converter
    // drops the old per-model table as well.
    assert!(value.pointer("/accounts/model_families").is_none());
}

#[test]
fn top_level_family_rules_are_refused_with_catalog_cutover_guidance() {
    for value in [json!(null), json!({}), json!({"gpt": ["*"]}), json!([])] {
        let error = parse_config(&json!({"model_families": value}).to_string()).unwrap_err();
        assert!(error.contains("model_families"), "{error}");
        assert!(error.contains("Fusiform's model.lineage"), "{error}");
        assert!(error.contains("delete this section"), "{error}");
    }
    parse_config(r#"{"accounts":{"model_families":[{"model":"claude-fake","family":"opus"}]}}"#)
        .unwrap();
}

#[test]
fn converter_never_emits_retired_account_model_families() {
    for input in [
        json!({"model_routing": {}, "accounts": {"model_families": []}}),
        json!({"model_routing": {}, "account_selection": {"model_families": []}}),
        json!({"accounts": {"model_families": []}, "providers": {
            "anthropic": {"accounts": {"model_families": []}}
        }}),
        json!({"model_routing": {}}),
    ] {
        let converted = crate::config_convert::convert_value(input).unwrap();
        assert!(converted["accounts"].get("model_families").is_none());
        assert!(converted["providers"]["anthropic"]["accounts"]
            .get("model_families")
            .is_none());
    }
    assert!(crate::config_convert::defaults()
        .get("account_model_families")
        .is_none());
}

#[test]
fn conversion_keeps_other_consumers_provider_restrictions_on_model_exceptions() {
    let converted = crate::config_convert::convert_value(json!({"model_routing": {
        "exclude": {
            "mason": ["p"],
            "sidekick": ["p", "-p/exception", "p/sidekick-too", "q/sidekick-only"]
        }
    }}))
    .unwrap();
    assert_eq!(
        converted["models"]["p/exception"]["not_for"],
        json!(["mason"])
    );
    assert_eq!(
        converted["models"]["p/sidekick-too"]["not_for"],
        json!(["mason", "sidekick"])
    );
    let config = config(converted);
    assert!(!config.route_allowed("mason", "p/exception", true));
    assert!(config.route_allowed("sidekick", "p/exception", false));
    assert!(!config.route_allowed("sidekick", "p/other", false));
    assert!(!config.route_allowed("mason", "p/other", false));
    assert!(!config.route_allowed("sidekick", "q/sidekick-only", false));
    assert!(config.route_allowed("mason", "q/sidekick-only", false));
}

/// A scarce subscription model can serve Athena automatically while masons
/// reach it only by an explicit pin; consumers outside `only_for` stay refused
/// even when pinned.
#[test]
fn pin_only_for_requires_a_pin_only_for_the_named_consumers() {
    let config = config(json!({
        "models": {"anthropic/claude-opus-5-5": {
            "only_for": ["athena", "lab", "mason"],
            "pin_only_for": ["mason"]
        }}
    }));
    let opus = "anthropic/claude-opus-5-5";
    assert!(config.route_allowed("athena", opus, false));
    assert!(config.route_allowed("athena.seat", opus, false));
    assert!(!config.route_allowed("mason", opus, false));
    assert!(config.route_allowed("mason", opus, true));
    assert!(!config.route_allowed("sidekick", opus, true));
    assert!(config
        .config
        .route_refusal("mason", opus, false)
        .unwrap()
        .contains("pin_only_for"));
}

#[test]
fn consumer_pins_load_when_the_config_does_not_refuse_them_for_their_consumer() {
    // The pinned model is refused for sidekick only, so the athena and lab
    // pins stay servable and the config loads.
    let config = config(json!({
        "consumers": {
            "synthesizer": {"pin": {"model": "anthropic/claude-opus-5-5", "variant": "medium"}},
            "lab_planner": {"pin": {"model": "anthropic/claude-opus-5-5"}},
            "spec_designer": {"pin": {"model": "anthropic/claude-opus-5-5"}},
            "mason": {"projects": {"proj": {"pin": "anthropic/claude-opus-5-5:high"}}}
        },
        "models": {"anthropic/claude-opus-5-5": {"not_for": ["sidekick"], "pin_only": true}}
    }));
    assert_eq!(
        config.slot_pin("synthesizer").unwrap().model,
        "anthropic/claude-opus-5-5"
    );
    assert!(!config.route_allowed("sidekick", "anthropic/claude-opus-5-5", true));
}

#[test]
fn config_convert_refuses_a_pin_the_converted_exclusions_would_refuse() {
    // An old-shape exclusion becomes a `not_for`, which also refuses explicit
    // pins, so the converted config must not load with the synthesizer pinned
    // to a route the athena exclusion names.
    let converted = crate::config_convert::convert(
        r#"{"model_routing": {
            "exclude": {"prefrontal.athena": ["anthropic/claude-opus-5-5"]},
            "pins": {"synthesizer": {"model": "anthropic/claude-opus-5-5"}}
        }}"#,
    )
    .unwrap();
    let error = validate_document(&converted).unwrap_err();
    assert!(
        error.contains("consumers.synthesizer.pin")
            && error.contains("anthropic/claude-opus-5-5")
            && error.contains(r#"not_for lists "prefrontal.athena""#),
        "{error}"
    );
}

#[test]
fn metered_defaults_follow_billing_planes_and_explicit_entity_overrides() {
    use striatum_core::billing::{BillingPlane, BillingRate};
    let route = "new-provider/model";
    let mut plane = BillingPlane {
        provider_id: "new-provider".into(),
        auth_method: "apikey".into(),
        kind: "upstream".into(),
        selection_reason: "test".into(),
        rates: BTreeMap::from([("rate.output".into(), BillingRate::Priced { rate: 1.0 })]),
    };
    for method in ["apikey", "gateway"] {
        plane.auth_method = method.into();
        for document in [json!({}), json!({"consumers":{"sidekick":{"iq":50}}})] {
            let config = config(document);
            for consumer in ["mason", "head", "codemode", "flow", "new.consumer"] {
                assert!(!config.route_allowed_on_plane(consumer, route, false, Some(&plane)));
                assert!(config.route_allowed_on_plane(consumer, route, true, Some(&plane)));
            }
        }
    }
    // Missing rate evidence cannot silently opt an API key into spending.
    plane.rates.clear();
    assert!(!config(json!({})).route_allowed_on_plane("head", route, false, Some(&plane)));
    for document in [
        json!({"providers":{"new-provider":{"pin_only":false}}}),
        json!({"providers":{"new-provider":{"pin_only":true}},"models":{"new-provider/model":{"pin_only":false}}}),
    ] {
        assert!(config(document).route_allowed_on_plane("head", route, false, Some(&plane)));
    }
    let restricted = config(
        json!({"providers":{"new-provider":{"pin_only":false}}, "models":{"new-provider/model":{"pin_only":true,"not_for":["mason"]}}}),
    );
    assert!(!restricted.route_allowed_on_plane("head", route, false, Some(&plane)));
    assert!(restricted.route_allowed_on_plane("head", route, true, Some(&plane)));
    assert!(!restricted.route_allowed_on_plane("mason", route, true, Some(&plane)));
    for (method, kind) in [
        ("oauth", "quota_proxy"),
        ("chatgpt", "quota_proxy"),
        ("apikey", "quota_proxy"),
    ] {
        plane.auth_method = method.into();
        plane.kind = kind.into();
        assert!(config(json!({})).route_allowed_on_plane("codemode", route, false, Some(&plane)));
    }
}

#[test]
fn masons_refuse_below_bar_by_default_but_operator_policy_wins() {
    use crate::config_policy::BelowBarPolicy::{BestEffort, Refuse};
    let defaults = config(json!({}));
    for consumer in ["prefrontal.mason", "prefrontal.mason.review.deep"] {
        assert_eq!(
            defaults
                .consumer_policy(consumer, &Default::default(), &BTreeMap::new())
                .below_bar,
            Some(Refuse)
        );
    }
    for consumer in ["sidekick", "codemode", "flow", "prefrontal.masonry", "head"] {
        assert_eq!(
            defaults
                .consumer_policy(consumer, &Default::default(), &BTreeMap::new())
                .below_bar
                .unwrap_or_default(),
            BestEffort
        );
    }
    let overrides = config(
        json!({"consumers":{"prefrontal.mason":{"below_bar":"best_effort"},"prefrontal.mason.strict":{"below_bar":"refuse"}}}),
    );
    assert_eq!(
        overrides
            .consumer_policy(
                "prefrontal.mason.review",
                &Default::default(),
                &BTreeMap::new()
            )
            .below_bar,
        Some(BestEffort)
    );
    assert_eq!(
        overrides
            .consumer_policy(
                "prefrontal.mason.strict.child",
                &Default::default(),
                &BTreeMap::new()
            )
            .below_bar,
        Some(Refuse)
    );
}

#[test]
fn explicit_subscription_marks_keep_kimi_review_and_grok_automatic() {
    use striatum_core::billing::BillingPlane;
    let mut plane = BillingPlane {
        provider_id: String::new(),
        auth_method: "apikey".into(),
        kind: "upstream".into(),
        selection_reason: "flat-rate plan using an API key".into(),
        rates: BTreeMap::new(),
    };
    let plans = config(json!({
        "providers": {"kimi-for-coding":{"subscription":true}, "kimi-code-plan-global":{"subscription":true}, "xai":{"subscription":true}, "other":{"subscription":false}},
        "models": {"xai/metered":{"subscription":false}, "other/plan":{"subscription":true}, "xai/manual":{"pin_only":true}}
    }));
    for (consumer, route) in [
        ("athena.review", "kimi-code-plan-global/k3.1-pro"),
        ("athena.review", "kimi-for-coding/kimi-k3"),
        ("head", "xai/grok-4.7"),
        ("flow", "other/plan"),
    ] {
        plane.provider_id = route.split('/').next().unwrap().into();
        assert!(
            plans.route_allowed_on_plane(consumer, route, false, Some(&plane)),
            "{consumer}: {route}"
        );
    }
    assert!(!plans.route_allowed_on_plane("head", "xai/metered", false, Some(&plane)));
    assert!(!plans.route_allowed_on_plane("head", "xai/manual", false, Some(&plane)));
    let parent_false = config(json!({"providers":{"xai":{"subscription":false}}}));
    assert!(!parent_false.route_allowed_on_plane("head", "xai/grok-4.7", false, Some(&plane)));
}
#[test]
fn banked_reset_pricing_is_provider_scoped_and_defaults_to_emergency() {
    for (setting, capacity) in [
        ("", false),
        (r#", "banked_resets":"emergency""#, false),
        (r#", "banked_resets":"capacity""#, true),
    ] {
        let document =
            format!(r#"{{"providers":{{"openai":{{"keep_free_percent":5{setting}}}}}}}"#);
        let config = parse_config(&document).unwrap();
        let live = LiveConfig::new(config, Some(document));
        assert_eq!(
            live.pricing().banked_resets_capacity.contains("openai"),
            capacity
        );
        assert!(!live.pricing().banked_resets_capacity.contains("anthropic"));
    }
    assert!(parse_config(r#"{"providers":{"openai":{"banked_resets":"automatic"}}}"#).is_err());
}

#[test]
fn module_consumer_policy_inherits_without_legacy_aliases() {
    let config = config(json!({"consumers": {
        "prefrontal.sidekick": {
            "iq":71, "eq":52, "below_bar":"refuse", "service_tier":"fast",
            "workload":"lookup", "pin":{"model":"p/model"},
            "projects":{"project":{"pin":"p/project"}}
        },
        "magic-context.dream": {"iq":71},
        "magic-context.dream.map-memories": {"iq":31}
    }, "providers":{"p":{"only_for":["prefrontal.sidekick"]}}}));
    let consumer = "prefrontal.sidekick.explore";
    assert_eq!(config.consumer_workload(consumer), Some("lookup"));
    let policy = config.consumer_policy(consumer, &Default::default(), &BTreeMap::new());
    assert_eq!(policy.iq, Some(71.0));
    assert_eq!(policy.eq, Some(52.0));
    assert_eq!(
        policy.below_bar,
        Some(crate::config_policy::BelowBarPolicy::Refuse)
    );
    assert_eq!(policy.ladder, Some(vec!["p/model".into()]));
    assert_eq!(config.project_pin("project", consumer), Some("p/project"));
    assert_eq!(
        config.service_tier(consumer, "p", "model"),
        Some(cortexkit_role_routing::service_tier::ServiceTier::Fast)
    );
    assert!(config.route_allowed(consumer, "p/model", true));
    let legacy = config.consumer_policy("sidekick.explore", &Default::default(), &BTreeMap::new());
    assert_eq!(legacy.iq, None);
    assert_eq!(config.project_pin("project", "sidekick.explore"), None);
    assert_eq!(config.service_tier("sidekick.explore", "p", "model"), None);
    assert!(!config.route_allowed("sidekick.explore", "p/model", true));
    assert_eq!(
        config
            .consumer_policy(
                "magic-context.dream.map-memories",
                &Default::default(),
                &BTreeMap::new()
            )
            .iq,
        Some(31.0)
    );
    assert_eq!(
        config
            .consumer_policy("mc_dream_map", &Default::default(), &BTreeMap::new())
            .iq,
        None
    );
}

#[test]
fn removed_model_naming_setting_is_a_clear_config_error() {
    for document in [
        json!({"model_naming":{}}),
        json!({"model_naming":null}),
        json!({"model_routing":{"model_naming":{}}}),
        json!({"model_routing":{"model_naming":null}}),
    ] {
        let error = parse_config(&document.to_string()).unwrap_err();
        assert!(
            error.contains("config_key_removed: model_naming"),
            "{error}"
        );
        assert!(error.contains("removed"), "{error}");
        let error = crate::config_convert::convert_value(document).unwrap_err();
        assert!(
            error.contains("model_naming") && error.contains("removed"),
            "{error}"
        );
    }
    let converted = crate::config_convert::convert_value(json!({"model_routing":{}})).unwrap();
    assert!(converted.get("model_naming").is_none());
    parse_config(&converted.to_string()).unwrap();
}

#[test]
fn removed_substrates_setting_is_a_clear_config_error() {
    let error = parse_config(r#"{"model_routing":{"substrates":{"all":{"default":"broca"}}}}"#)
        .unwrap_err();
    assert!(
        error.contains("config_key_removed: substrates") && error.contains("legacy route.select"),
        "{error}"
    );
    for document in [
        json!({"substrates":{}}),
        json!({"model_routing":{"substrates":{}}}),
    ] {
        let error = crate::config_convert::convert_value(document).unwrap_err();
        assert!(
            error.contains("substrates") && error.contains("legacy route.select"),
            "{error}"
        );
    }
}

#[test]
fn converter_dropped_scoped_policy_entry_changes_v1_document_and_policy() {
    let consumer = "prefrontal.mason";
    let source = json!({"model_routing":{"exclude":{consumer:["p/m"]}}});
    let converted = crate::config_convert::convert_value(source.clone()).unwrap();
    assert_eq!(converted["models"]["p/m"]["not_for"], json!([consumer]));
    assert!(!config(converted.clone()).route_allowed(consumer, "p/m", true));

    let mut without_entry = source;
    without_entry["model_routing"]["exclude"][consumer] = json!([]);
    let converted_without = crate::config_convert::convert_value(without_entry).unwrap();
    assert_ne!(converted["models"], converted_without["models"]);
    assert!(converted_without["models"].get("p/m").is_none());
    assert!(config(converted_without).route_allowed(consumer, "p/m", true));
}

#[test]
fn consumer_exclusion_groups_agree_with_the_route_rule_for_nested_model_entries() {
    use striatum_core::provider_model_spec::{parse_spec_list, resolve_spec};
    let mason = "prefrontal.mason";
    let sidekick = "prefrontal.sidekick";
    let models = [
        // Mason may not use model m, except its v variant.
        json!({"p/m": {"not_for": [mason]}, "p/m/v": {"not_for": []}}),
        json!({"p/m": {"only_for": [sidekick]}, "p/m/v": {"only_for": [mason]}}),
        // The variant's only_for admits Mason, but it inherits m's not_for.
        json!({"p/m": {"not_for": [mason]}, "p/m/v": {"only_for": [mason]}}),
        json!({"p/m": {"only_for": [mason]}, "p/m/v": {"not_for": [sidekick]}}),
    ];
    let providers = [
        json!({}),
        json!({"p": {"not_for": [mason]}}),
        json!({"p": {"only_for": [sidekick]}}),
    ];
    for models in &models {
        for providers in &providers {
            let config = config(json!({"providers": providers, "models": models}));
            for consumer in [mason, "prefrontal.mason.review", sidekick] {
                let exclusions = config.consumer_exclusions(consumer);
                let specs = parse_spec_list(&exclusions);
                for route in ["p/m", "p/m/v", "p/m/v/x", "p/m/w", "p/n"] {
                    assert_eq!(
                        resolve_spec(&specs, route, false),
                        !config.route_allowed(consumer, route, true),
                        "{consumer} on {route}: providers {providers} models {models} group {exclusions:?}"
                    );
                }
            }
        }
    }
}

/// The per-model consumer scan the tier resolver replaced: it copied every
/// consumer entry, merged scoped route tiers over user ones, then kept the
/// entries matching `consumer`, most specific first.
fn scanned_service_tier(
    config: &LiveConfig,
    consumer: &str,
    provider: &str,
    model: &str,
    project: Option<&BTreeMap<String, crate::tier_settings::Consumer>>,
) -> Option<cortexkit_role_routing::service_tier::ServiceTier> {
    use crate::config_policy::{consumer_matches, consumer_specificity};
    let mut entries: Vec<(String, Option<_>, BTreeMap<String, _>)> = config
        .config
        .consumers
        .iter()
        .map(|(id, policy)| {
            let scoped = project.and_then(|entries| entries.get(id));
            let mut tiers = policy.service_tiers.clone().unwrap_or_default();
            if let Some(scoped) = scoped.and_then(|entry| entry.service_tiers.as_ref()) {
                tiers.extend(scoped.iter().map(|(route, tier)| (route.clone(), *tier)));
            }
            let tier = scoped
                .and_then(|entry| entry.service_tier)
                .or(policy.service_tier);
            (id.clone(), tier, tiers)
        })
        .chain(
            project
                .into_iter()
                .flatten()
                .filter(|(id, _)| !config.config.consumers.contains_key(*id))
                .map(|(id, entry)| {
                    let tiers = entry.service_tiers.clone().unwrap_or_default();
                    (id.clone(), entry.service_tier, tiers)
                }),
        )
        .filter(|(name, _, _)| consumer_matches(name, consumer))
        .collect();
    entries.sort_by_key(|(name, _, _)| std::cmp::Reverse(consumer_specificity(name)));
    let route = format!("{provider}/{model}");
    entries
        .iter()
        .find_map(|(_, _, tiers)| tiers.get(&route).copied())
        .or_else(|| {
            entries
                .iter()
                .find_map(|(_, _, tiers)| tiers.get(provider).copied())
        })
        .or_else(|| entries.iter().find_map(|(_, tier, _)| *tier))
        .or(config
            .config
            .model_routing
            .models
            .get(&route)
            .and_then(|model| model.rules.service_tier))
        .or(config
            .config
            .providers
            .get(provider)
            .and_then(|provider| provider.rules.service_tier))
        .map(crate::tier_settings::Tier::value)
}

#[test]
fn tier_resolver_matches_the_per_model_consumer_scan_it_replaced() {
    let config = config(json!({
        "consumers": {
            "prefrontal": {"service_tiers": {"google": "fast"}},
            "prefrontal.sidekick": {
                "service_tier": "standard",
                "service_tiers": {"openai": "fast", "openai/m1": "ultrafast"}
            },
            "prefrontal.sidekick.review": {"service_tier": "ultrafast"},
            "other": {"service_tier": "fast"}
        },
        "models": {"openai/m2": {"service_tier": "standard"}, "xai/z": {"service_tier": "fast"}},
        "providers": {"anthropic": {"service_tier": "fast"}, "xai": {"service_tier": "ultrafast"}}
    }));
    let project: BTreeMap<String, crate::tier_settings::Consumer> = serde_json::from_value(json!({
        "prefrontal.sidekick": {"service_tiers": {"openai": "standard"}},
        "prefrontal.sidekick.review": {"service_tiers": {"anthropic": "ultrafast"}},
        "prefrontal.lab": {"service_tier": "fast"},
        "prefrontal.lab.deep": {"service_tiers": {"openai/m1": "standard"}}
    }))
    .unwrap();
    let mut compared = 0;
    for project in [None, Some(&project)] {
        for consumer in [
            "prefrontal",
            "prefrontal.sidekick",
            "prefrontal.sidekick.review",
            "prefrontal.sidekick.review.deep",
            "prefrontal.lab",
            "prefrontal.lab.deep",
            "other",
            "nobody",
        ] {
            let resolver = config.tier_resolver(consumer, project);
            for (provider, model) in [
                ("openai", "m1"),
                ("openai", "m2"),
                ("openai", "m3"),
                ("google", "x"),
                ("anthropic", "y"),
                ("xai", "z"),
                ("xai", "w"),
                ("unlisted", "v"),
            ] {
                assert_eq!(
                    resolver.resolve(provider, model),
                    scanned_service_tier(&config, consumer, provider, model, project),
                    "{consumer} on {provider}/{model} with project {}",
                    project.is_some()
                );
                compared += 1;
            }
        }
    }
    assert_eq!(compared, 128);
}
