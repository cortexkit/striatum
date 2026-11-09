use super::*;
use crate::config_policy::{ConsumerMode, ConsumerPolicy};
use serde_json::json;

fn config(value: serde_json::Value) -> LiveConfig {
    LiveConfig::new(parse_config(&value.to_string()).unwrap(), None)
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
fn nonlegacy_consumer_variants_are_materialized_with_dotted_inheritance() {
    let config = config(json!({
        "variants": {"p/model": "low"},
        "consumers": {
            "mc_summary": {"variants": {"p/model": "high"}},
            "mc_summary.child": {}
        }
    }));
    let groups = serde_json::to_value(config.variant_groups()).unwrap();
    assert_eq!(groups["consumers"]["mc_summary"]["p/model"], "high");
    assert_eq!(groups["consumers"]["mc_summary.child"]["p/model"], "high");
}

#[test]
fn a_consumer_named_all_does_not_replace_top_level_variants() {
    let config = config(json!({
        "variants": {"p/model": "low"},
        "consumers": {"all": {"variants": {"p/model": "high"}}}
    }));
    let request = crate::variant_policy::VariantRequest {
        route_key: "p/model",
        provider_id: "p",
        model_id: "model",
        explicit: None,
        curated: None,
        consumer: Some("all"),
    };
    let choice =
        crate::variant_policy::resolve(config.variant_groups(), &BTreeMap::new(), None, &request);
    assert_eq!(choice.variant.as_deref(), Some("high"));
    let other = crate::variant_policy::VariantRequest {
        consumer: Some("other"),
        ..request
    };
    let choice =
        crate::variant_policy::resolve(config.variant_groups(), &BTreeMap::new(), None, &other);
    assert_eq!(choice.variant.as_deref(), Some("low"));
}

#[test]
fn converter_omits_empty_variants_but_keeps_consumer_registration() {
    for variants in [json!({}), json!({"all": {}, "mason": {"p/model": "high"}})] {
        let converted = crate::config_convert::convert_value(json!({"model_routing": {
            "consumers": ["mason", "mc_summary"], "variants": variants
        }}))
        .unwrap();
        let consumers = converted["consumers"].as_object().unwrap();
        assert!(consumers.contains_key("mc_summary"));
        for (name, consumer) in consumers {
            if let Some(variants) = consumer.get("variants") {
                assert!(
                    !variants.as_object().unwrap().is_empty(),
                    "{name}: {consumer}"
                );
            }
        }
    }
    let converted = crate::config_convert::convert_value(json!({"consumers": {
        "mc_summary": {"variants": {}}, "mason": {"variants": {"p/model": "high"}}
    }}))
    .unwrap();
    assert_eq!(converted["consumers"]["mc_summary"], json!({}));
    assert_eq!(
        converted["consumers"]["mason"]["variants"],
        json!({"p/model": "high"})
    );
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
        json!({"pins": {}}),
        json!({"score_alias": {}}),
    ] {
        let error = parse_config(&value.to_string()).unwrap_err();
        assert!(error.contains(crate::config_convert::COMMAND), "{error}");
    }
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
        "variants": {"mason": {"p/model": "high"}},
        "pins": {"synthesizer": {"model": "p/model", "variant": "high"}, "lab_planner": {"model": "p/model"}, "spec_designer": {"model": "p/model", "variant": "medium"}},
        "project_pins": {"project": {"mason": "p/model:low"}},
        "score_alias": {"alias": "p"}
    }, "account_selection": {"enabled": false}});
    let converted = crate::config_convert::convert_value(old).unwrap();
    assert_eq!(
        converted["consumers"]["mason"]["variants"]["p/model"],
        "high"
    );
    assert_eq!(
        converted["consumers"]["mason"]["projects"]["project"]["pin"],
        "p/model:low"
    );
    assert_eq!(
        converted["models"]["p/blocked"]["not_for"],
        json!(["mason"])
    );
    assert_eq!(converted["consumers"]["codemode"]["pay_per_token"], true);
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
    let result = prefrontal_routing_core::launch_policy::resolve_routing_policy(
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
            "exclude": {"athena": ["anthropic/claude-opus-5-5"]},
            "pins": {"synthesizer": {"model": "anthropic/claude-opus-5-5"}}
        }}"#,
    )
    .unwrap();
    let error = validate_document(&converted).unwrap_err();
    assert!(
        error.contains("consumers.synthesizer.pin")
            && error.contains("anthropic/claude-opus-5-5")
            && error.contains(r#"not_for lists "athena""#),
        "{error}"
    );
}
