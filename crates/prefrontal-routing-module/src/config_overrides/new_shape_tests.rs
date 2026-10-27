use super::*;
use crate::config_policy::{ConsumerMode, ConsumerPolicy};
use serde_json::json;

fn config(value: serde_json::Value) -> LiveConfig {
    LiveConfig::new(parse_config(&value.to_string()).unwrap(), None)
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
    let scratch = prefrontal_test_support::ScratchDir::new("converted-round-trip");
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
fn conversion_preserves_first_match_family_rule_order() {
    let document = crate::config_convert::convert(
        r#"{
        "model_routing": {},
        "model_families": {"other": ["*"], "gpt": ["p/*"]}
    }"#,
    )
    .unwrap();
    let config = parse_config(&document).unwrap();
    assert_eq!(config.model_families.0[0].family, GrandFamily::Other);
    assert_eq!(config.model_families.0[1].family, GrandFamily::Gpt);
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
