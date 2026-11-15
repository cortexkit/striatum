use std::sync::atomic::Ordering;

use cortexkit_role_routing::placement::HarnessCapability::{self, ForkAnswer, OneOff, OutputCap};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use striatum_core::RouteStateStore;

use super::{harness_facts::PARSES, parse_declaration, DescribeCache, Registry, RunnerDeclaration};
use crate::routing_v1::{describe, dispatch_tests::configured};

#[path = "harness_choice_tests.rs"]
mod harness_choice_tests;

const GROUP: &str = cortexkit_role_routing::capabilities::RUNNER_HARNESS_FACTS;

fn slot(limits: Value) -> Value {
    let tags: std::collections::BTreeSet<_> =
        cortexkit_role_routing_conformance::fixture::catalog()
            .runners
            .into_iter()
            .flat_map(|runner| runner.tool_providers)
            .flat_map(|provider| provider.capabilities)
            .collect();
    json!({"runner": {
        "pairs": [{"wire_api":"anthropic.messages", "auth_method":"apikey"},
                  {"wire_api":"anthropic.messages", "auth_method":"oauth"}],
        "harnesses":["primary","secondary","inherited"], "default_harness":"primary",
        "tool_capabilities": tags,
        "standing_limits":{"explicit_providers":["anthropic"]},
        "harness_limits": limits
    }})
}

fn read(slot: &Value) -> RunnerDeclaration {
    parse_declaration(slot).unwrap().unwrap()
}

fn catalog(slot: &Value, module: &str) -> Value {
    json!([{"module_id":module,"nonce_verified":true,"role_declarations":{"routing/v1":slot}}])
}

fn prepare_selects(harness: &crate::routing_v1::conformance_tests::Harness) {
    let models: serde_json::Map<_, _> = harness
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .map(|mut fact| {
            fact.supports_tool_use = Some(true);
            harness
                .store()
                .upsert_fact(&fact, striatum_core::FactSource::Override)
                .unwrap();
            (
                format!("{}/{}", fact.provider_id, fact.model_id),
                json!({"existence":"present","capability.tool_call":true}),
            )
        })
        .collect();
    let snapshot = crate::fusiform_catalog::decode_catalog_get(
        &serde_json::to_vec(&json!({"catalog_version":41,"resolved_at_ms":123,"models":models}))
            .unwrap(),
    )
    .unwrap();
    harness.credentials().lock().unwrap().catalog_snapshot = Some(std::sync::Arc::new(snapshot));
}

#[tokio::test]
async fn candidate_harness_facts_decode_and_inherit_without_granting_unknown_tokens() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let value = slot(json!({"primary": {
            "starts_sessions":false, "capabilities":["one_off","fork_answer","future_token"],
            "exclude":["anthropic", "-anthropic/allowed"],
            "pairs":[{"wire_api":"anthropic.messages","auth_method":"apikey"}]
        }}));
        let declaration = read(&value);
        assert!(declaration.declares_harness_facts());
        let primary = declaration.limits_for_harness(Some("primary")).unwrap();
        assert_eq!(primary.starts_sessions, Some(false));
        assert_eq!(
            primary
                .capabilities
                .as_ref()
                .unwrap()
                .iter()
                .map(|cap| cap.as_str())
                .collect::<Vec<_>>(),
            ["one_off", "fork_answer", "future_token"]
        );
        assert!(declaration.harness_supports("primary", &OneOff));
        assert!(declaration.harness_supports("primary", &ForkAnswer));
        assert!(!declaration.harness_supports("primary", &OutputCap));
        let unknown: HarnessCapability = serde_json::from_value(json!("future_token")).unwrap();
        assert!(!declaration.harness_supports("primary", &unknown));
        assert!(declaration.harness_excludes("primary", "anthropic/blocked/high"));
        assert!(!declaration.harness_excludes("primary", "anthropic/allowed/high"));
        assert!(!declaration.harness_excludes("primary", "anthropic-platform/blocked"));
        let inherited = declaration.limits_for_harness(Some("inherited")).unwrap();
        assert_eq!(inherited.starts_sessions, Some(true));
        assert_eq!(inherited.pairs.as_ref().unwrap().len(), 2);
        assert_eq!(inherited.explicit_providers, Some(vec!["anthropic".into()]));
        assert!(!declaration.harness_supports("inherited", &OneOff));
        assert!(!declaration.harness_excludes("inherited", "anthropic/blocked"));
        let mut stopped = value.clone();
        stopped["runner"]["starts_sessions"] = json!(false);
        stopped["runner"]["harness_limits"]["primary"]["starts_sessions"] = json!(true);
        assert_eq!(
            read(&stopped)
                .limits_for_harness(Some("primary"))
                .unwrap()
                .starts_sessions,
            Some(false)
        );
        assert_eq!(
            read(&stopped)
                .limits_for_harness(Some("inherited"))
                .unwrap()
                .starts_sessions,
            Some(false)
        );
    })
    .await;
}

#[tokio::test]
async fn declaring_is_per_runner_and_non_null_empty_or_unknown_is_a_declaration() {
    describe::with_candidate_capabilities(&[GROUP], async {
        for facts in [
            json!({}),
            json!({"starts_sessions":null,"capabilities":null,"exclude":null}),
        ] {
            let legacy = read(&slot(json!({"primary":facts})));
            assert!(!legacy.declares_harness_facts());
            for cap in [OneOff, ForkAnswer, OutputCap] {
                assert!(legacy.harness_supports("primary", &cap));
                assert!(legacy.harness_supports("inherited", &cap));
            }
        }
        for facts in [
            json!({"starts_sessions":true}),
            json!({"capabilities":[]}),
            json!({"exclude":[]}),
            json!({"capabilities":["future_token"]}),
        ] {
            let declaring = read(&slot(
                json!({"primary":facts, "secondary":{"capabilities":[]}}),
            ));
            assert!(declaring.declares_harness_facts(), "{facts}");
            for harness in ["primary", "secondary", "inherited"] {
                for cap in [OneOff, ForkAnswer, OutputCap] {
                    assert!(
                        !declaring.harness_supports(harness, &cap),
                        "{harness}: {facts}"
                    );
                }
                assert_eq!(
                    declaring
                        .limits_for_harness(Some(harness))
                        .unwrap()
                        .starts_sessions,
                    Some(true)
                );
                assert!(!declaring.harness_excludes(harness, "anthropic/model"));
            }
            let legacy = read(&slot(json!({})));
            assert!(legacy.harness_supports("primary", &OutputCap));
        }
    })
    .await;
}

#[tokio::test]
async fn refused_typed_map_is_wholly_legacy_and_warns_at_catalog_read() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let valid = slot(json!({"primary":{"capabilities":["one_off"],"exclude":["anthropic"]}}));
        assert!(read(&valid).declares_harness_facts());
        for invalid in [
            json!({"exclude":["anthropic//model"]}),
            json!({"capabilities":["not a runner token"]}),
            json!({"pairs":[]}),
        ] {
            let mut value = valid.clone();
            value["runner"]["harness_limits"]["secondary"] = invalid;
            let declarations =
                super::declarations_from_catalog(&catalog(&value, "invalid-harness-map"));
            let declaration = &declarations["invalid-harness-map"];
            assert!(!declaration.declares_harness_facts());
            assert!(declaration.harness_limits_error().is_some());
            assert!(declaration.limits_for_harness(Some("primary")).is_err());
            assert!(declaration.harness_supports("primary", &OutputCap));
            assert!(!declaration.harness_excludes("primary", "anthropic/model"));
            let mut credentials = crate::broca_credentials::BrocaCredentials::default();
            credentials.runner_declarations = declarations;
            for harness in [None, Some("primary"), Some("secondary"), Some("inherited")] {
                let policies: Vec<crate::config_policy::RunnerPolicy> =
                    serde_json::from_value(json!([{"module":"invalid-harness-map"}])).unwrap();
                let registry = Registry::build_for_harness(policies, &credentials, &[], 0, harness);
                assert_eq!(registry.entries[0].pairs.as_ref().unwrap().len(), 2);
                assert_eq!(
                    registry.entries[0]
                        .standing_limits
                        .as_ref()
                        .unwrap()
                        .explicit_providers,
                    Some(vec!["anthropic".into()])
                );
            }
        }
        let warnings = crate::take_operator_warnings();
        assert!(
            warnings
                .iter()
                .any(|line| line.contains("invalid-harness-map")
                    && line.contains("exclude")
                    && line.contains("using runner-level limits for all harnesses")),
            "{warnings:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn held_group_refuses_entire_fact_map_and_preserves_runner_level_reach() {
    // Production serves the group; withholding it here pins what a router that
    // doesn't serve it reads from the same declaration.
    describe::without_capability(GROUP, async {
        let (harness, _) = configured().await;
        harness
            .edit_config(|config| config["runners"] = json!([{"module":"broca","via_vault":true}]));
        let value = slot(json!({"primary":{
            "exclude":[], "pairs":[{"wire_api":"anthropic.messages","auth_method":"apikey"}]
        }}));
        let live = read(&value);
        assert!(!live.declares_harness_facts());
        assert!(live.harness_limits_error().is_some());
        let facts = harness.store().get_facts().unwrap();
        let shared = harness.credentials();
        let mut credentials = shared.lock().unwrap();
        credentials.runner_declarations.insert("broca".into(), live);
        let live_generation = credentials.serving_generation();
        let config = harness.operator_config();
        let live_registry = Registry::build_for_selection(
            config.as_ref(),
            &credentials,
            &facts,
            crate::now_ms(),
            None,
        );
        assert_eq!(live_registry.entries[0].pairs.as_ref().unwrap().len(), 2);
        assert!(crate::take_operator_warnings()
            .iter()
            .any(|line| line.contains("runner broca:")
                && line.contains("using runner-level limits for all harnesses")));
        drop(credentials);
        describe::with_candidate_capabilities(&[GROUP], async {
            let candidate = read(&value);
            assert!(candidate.declares_harness_facts());
            let mut credentials = shared.lock().unwrap();
            credentials
                .runner_declarations
                .insert("broca".into(), candidate);
            assert_ne!(credentials.serving_generation(), live_generation);
            let candidate_registry = Registry::build_for_selection(
                config.as_ref(),
                &credentials,
                &facts,
                crate::now_ms(),
                None,
            );
            assert_eq!(
                candidate_registry.entries[0].pairs.as_ref().unwrap().len(),
                1
            );
        })
        .await;
    })
    .await;
}

#[tokio::test]
async fn broca_after_fixture_is_verbatim_and_declares_no_output_capability() {
    let bytes = include_bytes!("broca-runner-slot.after.json");
    assert_eq!(
        format!("{:x}", Sha256::digest(bytes)),
        "3066e75ce8c1d22fe1ee8b988e855365013bd31eefac2e47638d7a667f038755"
    );
    describe::with_candidate_capabilities(&[GROUP], async {
        let declaration = read(&serde_json::from_slice(bytes).unwrap());
        assert!(declaration.declares_harness_facts());
        assert!(declaration.harness_supports("broca", &OneOff));
        assert!(declaration.harness_supports("broca", &ForkAnswer));
        assert!(!declaration.harness_supports("broca", &OutputCap));
        assert_eq!(
            declaration
                .limits_for_harness(Some("broca"))
                .unwrap()
                .capabilities,
            Some(vec![OneOff, ForkAnswer])
        );
    })
    .await;
}

#[tokio::test]
async fn repeated_selects_parse_no_harness_facts_and_refresh_invalidates_views() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        prepare_selects(&harness);
        harness.edit_config(|config| {
            config["runners"] = json!([{"module":"broca","via_vault":true}]);
            config["consumers"]["harness_facts_test"] = json!({});
        });
        let config = harness.operator_config();
        let snapshot = harness.store().get_facts_with_generation().unwrap();
        let shared = harness.credentials();
        let mut cache = DescribeCache::default();
        let mut value = slot(json!({"primary":{"exclude":[]}}));
        value["runner"]
            .as_object_mut()
            .unwrap()
            .remove("standing_limits");
        let before = PARSES.load(Ordering::Relaxed);
        let old = {
            let mut credentials = shared.lock().unwrap();
            cache.catalog(
                &catalog(&value, "broca"),
                &config.v1_runners(),
                &mut credentials,
                crate::now_ms() as i64,
            );
            assert!(credentials.runner_declarations["broca"].declares_harness_facts());
            credentials.snapshot()
        };
        assert_eq!(PARSES.load(Ordering::Relaxed), before + 1);
        let old_index = crate::serving_index::ServingIndex::get(&config, &old, &snapshot);
        let old_registry = Registry::build_for_selection(
            config.as_ref(),
            &old,
            &snapshot.facts,
            crate::now_ms(),
            None,
        );
        let parsed = PARSES.load(Ordering::Relaxed);
        for _ in 0..3 {
            let (ty, reply) = route
                .call(
                    "route.select",
                    &json!({
                        "consumer": "harness_facts_test",
                        "requirements":{"iq":0}
                    }),
                )
                .await;
            assert_eq!(ty, subc_protocol::FrameType::Response, "{reply}");
            assert!(reply["result"]["runner"].is_object(), "{reply}");
        }
        assert_eq!(
            PARSES.load(Ordering::Relaxed),
            parsed,
            "select must not decode a declaration or its harness facts"
        );
        let mut changed = value.clone();
        changed["runner"]["harness_limits"]["primary"]["exclude"] = json!(["anthropic"]);
        let new = {
            let mut credentials = shared.lock().unwrap();
            cache.catalog(
                &catalog(&changed, "broca"),
                &config.v1_runners(),
                &mut credentials,
                crate::now_ms() as i64,
            );
            credentials.snapshot()
        };
        assert_eq!(PARSES.load(Ordering::Relaxed), parsed + 1);
        assert_ne!(old.serving_generation(), new.serving_generation());
        let new_index = crate::serving_index::ServingIndex::get(&config, &new, &snapshot);
        assert!(!std::sync::Arc::ptr_eq(&old_index, &new_index));
        assert!(std::sync::Arc::ptr_eq(
            &new_index,
            &crate::serving_index::ServingIndex::get(&config, &new, &snapshot)
        ));
        let new_registry = Registry::build_for_selection(
            config.as_ref(),
            &new,
            &snapshot.facts,
            crate::now_ms(),
            None,
        );
        assert!(!old_registry.entries[0]
            .wire_declaration
            .as_ref()
            .unwrap()
            .harness_excludes("primary", "anthropic/model"));
        assert!(new_registry.entries[0]
            .wire_declaration
            .as_ref()
            .unwrap()
            .harness_excludes("primary", "anthropic/model"));
        assert_eq!(PARSES.load(Ordering::Relaxed), parsed + 1);
    })
    .await;
}

#[tokio::test]
async fn what_if_overlay_facts_have_disposable_serving_and_registry_views() {
    describe::with_candidate_capabilities(&[GROUP], async {
        let (harness, route) = configured().await;
        prepare_selects(&harness);
        harness.edit_config(|config| {
            config["runners"] = json!([{"module":"broca","via_vault":true}]);
            config["consumers"]["harness_facts_test"] = json!({});
        });
        let config = harness.operator_config();
        let snapshot = harness.store().get_facts_with_generation().unwrap();
        let shared = harness.credentials();
        let mut value = slot(json!({"primary":{"capabilities":["output_cap"]}}));
        value["runner"]
            .as_object_mut()
            .unwrap()
            .remove("standing_limits");
        shared
            .lock()
            .unwrap()
            .runner_declarations
            .insert("broca".into(), read(&value));
        let live = shared.lock().unwrap().snapshot();
        let live_index = crate::serving_index::ServingIndex::get(&config, &live, &snapshot);
        let live_registry = Registry::build_for_selection(
            config.as_ref(),
            &live,
            &snapshot.facts,
            crate::now_ms(),
            None,
        );
        let mut overlay = value.clone();
        overlay["runner"]["harness_limits"]["primary"] = json!({
            "exclude":["anthropic"], "capabilities":["one_off"],
            "pairs":[{"wire_api":"anthropic.messages","auth_method":"apikey"}]
        });
        let proposed = crate::routing_v1::what_if::overlay_runner_declarations(
            &live,
            [("broca".into(), read(&overlay))].into_iter().collect(),
        );
        let proposed_index = crate::serving_index::ServingIndex::get(&config, &proposed, &snapshot);
        assert!(!std::sync::Arc::ptr_eq(&live_index, &proposed_index));
        let proposed_registry = Registry::build_for_selection(
            config.as_ref(),
            &proposed,
            &snapshot.facts,
            crate::now_ms(),
            None,
        );
        assert_eq!(
            proposed_registry.entries[0].pairs.as_ref().unwrap().len(),
            1
        );
        let declaration = proposed_registry.entries[0]
            .wire_declaration
            .as_ref()
            .unwrap();
        assert!(declaration.declares_harness_facts());
        assert!(declaration.harness_excludes("primary", "anthropic/model"));
        assert!(declaration.harness_supports("primary", &OneOff));
        assert!(!declaration.harness_supports("primary", &OutputCap));
        drop(proposed);
        drop(proposed_index);
        let operator = harness.bind(json!({"kind":"direct"}), None).await;
        let (ty, reply) = operator
            .call(
                "route.what_if",
                &json!({
                    "consumer":"harness_facts_test", "iqs":[0],
                    "runner":"broca", "runner_declarations":{"broca":overlay}
                }),
            )
            .await;
        assert_eq!(ty, subc_protocol::FrameType::Response, "{reply}");
        let still_live = shared.lock().unwrap().snapshot();
        assert_eq!(live.serving_generation(), still_live.serving_generation());
        assert!(std::sync::Arc::ptr_eq(
            &live_index,
            &crate::serving_index::ServingIndex::get(&config, &still_live, &snapshot)
        ));
        let still_live_registry = Registry::build_for_selection(
            config.as_ref(),
            &still_live,
            &snapshot.facts,
            crate::now_ms(),
            None,
        );
        assert_eq!(
            live_registry.entries[0].pairs,
            still_live_registry.entries[0].pairs
        );
        assert!(!still_live_registry.entries[0]
            .wire_declaration
            .as_ref()
            .unwrap()
            .harness_excludes("primary", "anthropic/model"));
        assert!(still_live_registry.entries[0]
            .wire_declaration
            .as_ref()
            .unwrap()
            .harness_supports("primary", &OutputCap));
        let (ty, reply) = route
            .call(
                "route.select",
                &json!({
                    "consumer":"harness_facts_test","requirements":{"iq":0}
                }),
            )
            .await;
        assert_eq!(ty, subc_protocol::FrameType::Response, "{reply}");
    })
    .await;
}

#[test]
fn contract_route_spec_and_settings_matcher_vectors_pin_their_differences() {
    use cortexkit_role_routing::route_spec;
    use std::collections::BTreeSet;
    use striatum_core::provider_model_spec::{
        parse_spec, parse_spec_list, resolve_spec, spec_names_key,
    };

    // Pinned contract 0.7.21 route_spec.rs:51-95 and routing's
    // provider_model_spec.rs:205-273, 398-556 supply these patterns and keys.
    // Cross the sets as well as checking the original paired vectors so prefix,
    // whitespace and malformed-segment differences cannot disappear unnoticed.
    let mut patterns: BTreeSet<String> = [
        "provider",
        " provider/model ",
        "-provider",
        "-provider/model",
        "openrouter/anthropic/claude",
        "p/m:variant",
        "",
        " ",
        "-",
        "/model",
        "p/",
        "p//m",
        "-p//m",
        "p",
        "p/m",
        "p/*",
        "-p",
        "-p/m",
        "other",
        "p/m/child",
        "-p/m/child/deep",
        "   ",
        "/m",
        "- /m",
        "- p/",
        "anthropic",
        "openai/pin",
        "-openai/pin/high",
        "-anthropic",
        "-openai/pin",
        "local",
        "-local/safe",
        "openai/pin/high",
        "-openai/gpt-5.4",
        "openai/",
        "openai",
        "-openai/gpt-5.5",
        "openai/gpt-5.5",
        "openai/gpt-5.4",
        "anthropic-platform",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    for body in [
        "p",
        "p/m",
        "p/m/child",
        "p/m/v/high",
        "q",
        "q/a",
        "q/a//b",
        "-",
        "p//",
        " p/m ",
    ] {
        patterns.insert(body.into());
        patterns.insert(format!("-{body}"));
    }
    let keys = [
        "p/m",
        "prefix/m",
        "p/m/v",
        "p/model",
        "p/m:variant",
        "openrouter/anthropic/claude/v",
        "p/other",
        "p",
        "p/",
        "p/m/child",
        "p/m/child/deep",
        "p/m/child/deep/v/extra",
        "p/m/v/high",
        "p/m2",
        "q/a//b/v",
        "q/a/z",
        "absent/m/v/extra",
        "",
        "/m",
        "anthropic/one",
        "anthropic/two/high",
        "openai/pin",
        "openai/pin/low",
        "local/unsafe",
        "openai/pin/high",
        "local/safe",
        "other/model",
        "openai/gpt-5.4",
        "openai/gpt-5.5",
        "openai/gpt-5.5/high",
        "openai/gpt-5.5-mini",
        "anthropic/claude-opus-4-8",
        "anthropic-platform/claude-opus-4-1",
        "anthropic/claude-opus-4-1",
        "anthropic/opus",
    ];
    let mut differences = BTreeSet::new();
    for pattern in &patterns {
        for key in keys {
            let contract = route_spec::matches(pattern, key);
            let settings = parse_spec(pattern).is_some_and(|spec| spec_names_key(&spec, key));
            if contract != settings {
                differences.insert((pattern.clone(), key.to_owned(), contract, settings));
            }
        }
    }
    // Settings trim again after a negation and accept empty interior segments;
    // the contract does neither. Preserve those settings semantics.
    let expected: BTreeSet<(String, String, bool, bool)> = [
        ("- p/m ", "p/m"),
        ("- p/m ", "p/m/child"),
        ("- p/m ", "p/m/child/deep"),
        ("- p/m ", "p/m/child/deep/v/extra"),
        ("- p/m ", "p/m/v"),
        ("- p/m ", "p/m/v/high"),
        ("-q/a//b", "q/a//b/v"),
        ("q/a//b", "q/a//b/v"),
    ]
    .into_iter()
    .map(|(pattern, key)| (pattern.into(), key.into(), false, true))
    .collect();
    assert_eq!(
        differences, expected,
        "matcher differences: {differences:#?}"
    );
    for (spec, key, expected) in [
        ("p", "p/m", true),
        ("p", "prefix/m", false),
        ("p/m", "p/m", true),
        ("p/m", "p/m/v", true),
        ("p/m", "p/model", false),
        ("p/m", "p/m:variant", false),
        (
            "openrouter/anthropic/claude",
            "openrouter/anthropic/claude/v",
            true,
        ),
        ("p/*", "p/m", false),
        ("p/m:variant", "p/m:variant", true),
    ] {
        assert_eq!(route_spec::matches(spec, key), expected);
        assert_eq!(
            parse_spec(spec).is_some_and(|spec| spec_names_key(&spec, key)),
            expected
        );
    }
    let lists: &[(&[&str], &str, bool)] = &[
        (&["p/m", "-p"], "p/m", true),
        (&["-p", "p/m"], "p/m", true),
        (&["p/m", "-p/m"], "p/m/v", false),
        (&["-p", "p"], "p/other", true),
        (&["-", "p/", "other"], "p/m", false),
        (&["openai", "-openai/gpt-5.5"], "openai/gpt-5.4", true),
        (&["openai", "-openai/gpt-5.5"], "openai/gpt-5.5", false),
        (&["openai/gpt-5.5"], "openai/gpt-5.5/high", true),
        (&["openai/gpt-5.5"], "openai/gpt-5.5-mini", false),
    ];
    for (list, key, expected) in lists {
        // Contract's base is the value a positive match returns. Settings' base
        // is the fallback when nothing matches; exclusion lists start false.
        let contract = route_spec::resolve(list.iter().copied(), key, true).unwrap_or(false);
        let settings = resolve_spec(
            &parse_spec_list(&list.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>()),
            key,
            false,
        );
        assert_eq!(contract, *expected, "{list:?}: {key}");
        assert_eq!(settings, *expected, "{list:?}: {key}");
    }
    println!(
        "compared {} pattern/route pairs, 9 contract pairs and {} resolution vectors",
        patterns.len() * keys.len(),
        lists.len()
    );
}
