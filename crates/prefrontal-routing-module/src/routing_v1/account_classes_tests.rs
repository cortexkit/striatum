//! Payment class is fixed by selection; account exhaustion never grants spending.

use std::sync::Arc;

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use prefrontal_routing_core::accounts::{
    ConfirmationSource, Fence, FenceScope, GeneralWindow, ResetTime,
};
use prefrontal_routing_core::billing::{BillingPlane, BillingRate};
use prefrontal_routing_core::route_state_store::CredentialRefusal;
use prefrontal_routing_store::AccountFenceRow;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness};
use crate::broca_credentials::BrocaCredentials;

const NOW: u64 = 1_800_000_000_000;
const RUNNER: &str = "class-runner";
const OWNER: &str = "class-owner";

fn catalog_fact(harness: &Harness, fact: &prefrontal_routing_core::ModelFact) {
    use prefrontal_routing_store::{FusiformCatalogCursor, FusiformFactUpdate};
    use std::sync::atomic::{AtomicU64, Ordering};
    static VERSION: AtomicU64 = AtomicU64::new(1);
    harness
        .store()
        .apply_fusiform_catalog(
            &[FusiformFactUpdate {
                provider_id: fact.provider_id.clone(),
                model_id: fact.model_id.clone(),
                auth_methods: fact.auth_methods.clone(),
                present: true,
                context_window: fact.context_window,
                max_output_tokens: fact.max_output_tokens,
                cost_tier: fact.cost_tier,
                cost_per_mtok_out: fact.cost_per_m_tok_out,
                input_rate: None,
                cache_read_rate: None,
                cache_write_rate: None,
                reasoning_rate: None,
                supports_vision: fact.supports_vision,
                supports_tool_use: fact.supports_tool_use,
                reasoning_efforts: None,
                canonical_id: None,
                upstream_status: None,
            }],
            FusiformCatalogCursor {
                catalog_version: VERSION.fetch_add(1, Ordering::Relaxed) as i64,
                resolved_at_ms: NOW as i64,
            },
        )
        .unwrap();
}

fn setup(provider: &str, converted_live: bool) -> (Arc<Harness>, BrocaCredentials) {
    setup_at_version(provider, converted_live, None)
}

fn setup_at_version(
    provider: &str,
    converted_live: bool,
    version: Option<u32>,
) -> (Arc<Harness>, BrocaCredentials) {
    let harness = Harness::new_at_version("account-classes", version);
    let mut fact = crate::tests::fact(provider, "model");
    fact.auth_methods = Some(vec!["oauth".into(), "apikey".into()]);
    fact.billing_plane = Some(BillingPlane {
        provider_id: provider.into(),
        auth_method: "apikey".into(),
        kind: "upstream".into(),
        selection_reason: "test metered plane".into(),
        rates: [("rate.output".into(), BillingRate::Priced { rate: 9.0 })]
            .into_iter()
            .collect(),
    });
    harness.install_facts(&[fact.clone()]);
    catalog_fact(&harness, &fact);
    let mut subscription = fact.clone();
    subscription.billing_plane.as_mut().unwrap().auth_method = "oauth".into();
    subscription.billing_plane.as_mut().unwrap().kind = "subscription".into();
    subscription
        .billing_plane
        .as_mut()
        .unwrap()
        .rates
        .insert("rate.output".into(), BillingRate::Priced { rate: 1.0 });
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "mixed-vault".into(),
            credentials: ["oauth", "apikey"]
                .into_iter()
                .map(|method| ListedCredential {
                    id: format!("{method}:test"),
                    account_id: Some(format!("{method}-account")),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec![provider.into()],
                    serves: vec![provider.into()],
                    credential_type: method.into(),
                    auth_method: Some(method.into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &[fact.clone()], NOW as i64);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact, subscription]),
    );
    credentials.publish_plan_prices(&json!({"prices":[]}));
    harness.set_credentials(credentials.clone());
    harness.edit_config(|config| {
        if converted_live {
            *config = crate::config_convert::convert_value(
                serde_json::from_str::<Value>(
                    &subc_jsonc::jsonc_to_json(include_str!(
                        "../../fixtures/routing-config-live-2026-10-24.jsonc"
                    ))
                    .unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
        }
        config["runners"] = json!([{"module":RUNNER,"via_vault":true}]);
        config["consumers"] = json!({
            "mason":{"owner":OWNER,"mode":"demand"},
            "codemode":{"owner":OWNER,"mode":"demand","pay_per_token":true},
            "flow":{"owner":OWNER,"mode":"demand","pay_per_token":true}
        });
    });
    (harness, credentials)
}

fn account_request(provider: &str, id: &str) -> Value {
    json!({"provider":provider,"model":"model","conversation_key":format!("conversation-{id}"),"admission_id":id})
}

fn assert_credential(reply: &(FrameType, Value), credential: &str) {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(
        reply.1["result"]["account"]["credential_id"], credential,
        "{}",
        reply.1
    );
}

fn exhaust_oauth(harness: &Harness, provider: &str) {
    let scope = FenceScope::General(GeneralWindow::FiveHour);
    let fence = Fence {
        account_id: "oauth-account".into(),
        pool: Some(scope.into()),
        recovery_set: Vec::new(),
        observed_at_ms: NOW as i64,
        reset: ResetTime::At(NOW as i64 + 600_000),
        retry_hint_ms: None,
        source: ConfirmationSource::BrocaAccountLimit,
    };
    harness
        .store()
        .account_transaction(|tx| {
            tx.insert_fence(&AccountFenceRow {
                fence_id: "class-fence".into(),
                provider: provider.into(),
                account_id: fence.account_id.clone(),
                scope_json: serde_json::to_string(&scope).unwrap(),
                fence_json: serde_json::to_string(&fence).unwrap(),
                source: "broca_limit_exhausted".into(),
                observed_at: NOW as i64,
                principal: "test".into(),
                admission_id: None,
                observation_id: None,
                created_at: NOW as i64,
                cleared_at: None,
                cleared_reason: None,
            })
        })
        .unwrap();
}

#[tokio::test]
async fn decisions_keep_both_wire_classes_after_consumer_edits() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    oauth_reading(10.0);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut decisions = Vec::new();
    for consumer in ["mason", "codemode", "flow"] {
        for legacy in [false, true] {
            let mut request = json!({"consumer":consumer,"requirements":{"iq":30,"eq":0}});
            if legacy {
                request.as_object_mut().unwrap().remove("consumer");
                request["targetAgent"] = json!(consumer);
                request["substrate"] = json!("broca");
                request["executor"] = json!("broca");
            }
            let reply = caller.call("route.select", &request).await;
            assert_eq!(
                reply.0,
                FrameType::Response,
                "{consumer} legacy={legacy}: {}",
                reply.1
            );
            let id = reply.1["result"][if legacy { "decisionID" } else { "decision_id" }]
                .as_str()
                .unwrap()
                .to_owned();
            decisions.push((consumer, legacy, id));
        }
    }
    harness.edit_config(|config| {
        config["consumers"]["mason"]["pay_per_token"] = json!(true);
        config["consumers"]["codemode"]["pay_per_token"] = json!(false);
        config["consumers"]["flow"]["pay_per_token"] = json!(false);
    });
    for (consumer, legacy, id) in decisions {
        let mut request = account_request("synthetic", &format!("{consumer}-{legacy}"));
        request[if legacy {
            "model_decision_id"
        } else {
            "decision_id"
        }] = json!(id);
        if legacy {
            request["caller"] = json!({"runner":RUNNER});
        }
        let reply = runner.call("route.account_for", &request).await;
        assert_credential(
            &reply,
            if consumer == "mason" {
                "oauth:test"
            } else {
                "apikey:test"
            },
        );
    }
    for legacy in [false, true] {
        let mut request = account_request("synthetic", &format!("no-id-{legacy}"));
        if legacy {
            request["caller"] = json!({"runner":RUNNER});
        }
        assert_credential(
            &runner.call("route.account_for", &request).await,
            "oauth:test",
        );
    }
}

#[tokio::test]
async fn ten_default_admissions_never_spend_after_subscription_exhaustion_or_entitlement_refusal() {
    let _clock = crate::InjectedClock::set(NOW);
    for (provider, converted_live) in [("synthetic", false), ("openai", true)] {
        for entitlement in [false, true] {
            let (harness, _) = setup(provider, converted_live);
            let caller = harness.bind(reserved(OWNER), None).await;
            let codemode = caller
                .call(
                    "route.select",
                    &json!({"consumer":"codemode","requirements":{"iq":0}}),
                )
                .await;
            assert_eq!(codemode.0, FrameType::Response, "{}", codemode.1);
            let id = codemode.1["result"]["decision_id"].as_str().unwrap();
            if entitlement {
                harness
                    .store()
                    .record_credential_entitlement_fence(
                        &CredentialRefusal {
                            provider_id: provider.into(),
                            credential_id: "oauth:test".into(),
                            code: "ENTITLEMENT_ERROR".into(),
                        },
                        None,
                        NOW as i64,
                    )
                    .unwrap();
            } else {
                exhaust_oauth(&harness, provider);
            }
            let runner = harness.bind(reserved(RUNNER), None).await;
            let mut metered = account_request(provider, "codemode-no-fallback");
            metered["decision_id"] = json!(id);
            assert_credential(
                &runner.call("route.account_for", &metered).await,
                "apikey:test",
            );
            for index in 0..10 {
                let reply = runner
                    .call(
                        "route.account_for",
                        &account_request(provider, &format!("blocked-{index}")),
                    )
                    .await;
                assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
                assert!(
                    matches!(
                        reply.1["code"].as_str(),
                        Some("account_hold" | "no_eligible_account")
                    ),
                    "{}",
                    reply.1
                );
                if !entitlement && reply.1["code"] == "no_eligible_account" {
                    assert_eq!(
                        reply.1["detail"]["reset_at_ms"],
                        NOW + 600_000,
                        "{}",
                        reply.1
                    );
                }
            }
            harness.edit_config(|config| {
                config["providers"][provider]["accounts"]["metered_fallback"] = json!(true)
            });
            assert_credential(
                &runner
                    .call("route.account_for", &account_request(provider, "opt-in"))
                    .await,
                "apikey:test",
            );
            metered["admission_id"] = json!("codemode-with-fallback");
            assert_credential(
                &runner.call("route.account_for", &metered).await,
                "apikey:test",
            );
        }
    }
}

#[test]
fn demand_prices_only_the_class_that_account_for_may_serve() {
    use prefrontal_routing_core::RouteStateStore;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Mutex;
    let _clock = crate::InjectedClock::set(NOW);
    for (blocked, fallback, expected_subscription, expected_method, expected_rate) in [
        (false, None, true, "oauth", 1.0),
        (true, None, true, "oauth", 1.0),
        (true, Some(false), true, "oauth", 1.0),
        (false, Some(true), true, "oauth", 1.0),
        (true, Some(true), false, "apikey", 9.0),
    ] {
        let (harness, credentials) = setup("synthetic", false);
        let credentials = Arc::new(Mutex::new(credentials));
        if blocked {
            exhaust_oauth(&harness, "synthetic");
        }
        harness.edit_config(|config| {
            config["providers"] = match fallback {
                Some(enabled) => json!({"synthetic":{"accounts":{"metered_fallback":enabled}}}),
                None => json!({}),
            };
        });
        let config = harness.operator_config();
        let mut facts = harness.store().get_facts().unwrap();
        let targets = BTreeMap::new();
        let sources = crate::SelectionPolicySources {
            live_config: &config,
            operator_config: &config,
            operator_config_path: std::path::Path::new("unused"),
            catalog_targets: &targets,
        };
        let priced = crate::price_selection_inputs(
            &mut facts,
            "{}".into(),
            &sources,
            harness.store(),
            &credentials,
            true,
            false,
        )
        .unwrap();
        let plane = facts
            .iter()
            .find(|fact| fact.provider_id == "synthetic" && fact.model_id == "model")
            .unwrap()
            .billing_plane
            .as_ref()
            .unwrap();
        assert_eq!(
            plane.auth_method, expected_method,
            "blocked={blocked} fallback={fallback:?}"
        );
        assert_eq!(
            plane.rates["rate.output"],
            BillingRate::Priced {
                rate: expected_rate
            }
        );
        let mut snapshot = harness.store().build_snapshot(NOW as f64).unwrap();
        let candidates = crate::select_policy::prepare_candidates(
            &facts,
            &mut snapshot,
            HashSet::from(["synthetic/model".into()]),
            HashMap::new(),
            crate::select_policy::CandidateSource::Vault {
                credentials: &credentials,
                fences: &priced.entitlement_fences,
                stale: &[],
                defer_pool_pin: false,
            },
            Some(crate::select_policy::Consumer::Mason),
            false,
            None,
            None,
            &config.operator_policy(),
            &targets,
            priced.classes.as_ref(),
        );
        assert_eq!(
            candidates.subscription_keys.contains("synthetic/model"),
            expected_subscription
        );
    }
}

#[tokio::test]
async fn an_apikey_only_model_has_no_subscription_account_to_withhold_it() {
    use prefrontal_routing_core::{FactSource, RouteStateStore};
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let mut fact = harness
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|fact| fact.provider_id == "synthetic" && fact.model_id == "model")
        .unwrap();
    fact.auth_methods = Some(vec!["apikey".into()]);
    harness
        .store()
        .upsert_fact(&fact, FactSource::Override)
        .unwrap();
    catalog_fact(&harness, &fact);
    let runner = harness.bind(reserved(RUNNER), None).await;
    assert_credential(
        &runner
            .call(
                "route.account_for",
                &account_request("synthetic", "api-only"),
            )
            .await,
        "apikey:test",
    );
}

#[tokio::test]
async fn migrated_062_decisions_keep_codemode_metered_and_mason_subscription_on_both_wires() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup_at_version("synthetic", false, Some(62));
    let mut decisions = Vec::new();
    for consumer in ["mason", "codemode"] {
        for legacy in [false, true] {
            let id = format!("historical-{consumer}-{legacy}");
            harness
                .store()
                .seed_preupgrade_decision_for_test(
                    &prefrontal_routing_store::V1Decision {
                        decision_id: id.clone(),
                        principal: format!("reserved:{OWNER}"),
                        consumer: consumer.into(),
                        allowed_account_class: Default::default(),
                        project_id: None,
                        provider: "synthetic".into(),
                        model: "model".into(),
                        variant: None,
                        runner_module: RUNNER.into(),
                        affinity: None,
                        created_at_ms: NOW as i64,
                        expires_at_ms: NOW as i64 + 60_000,
                    },
                    !legacy,
                )
                .unwrap();
            decisions.push((consumer, legacy, id));
        }
    }
    let harness = super::conformance_tests::upgrade_harness(harness);
    harness.edit_config(|config| {
        config["consumers"]["mason"]["pay_per_token"] = json!(true);
        config["consumers"]["codemode"]["pay_per_token"] = json!(false);
    });
    let runner = harness.bind(reserved(RUNNER), None).await;
    for (consumer, legacy, id) in decisions {
        let mut request = account_request("synthetic", &format!("migrated-{consumer}-{legacy}"));
        request[if legacy {
            "model_decision_id"
        } else {
            "decision_id"
        }] = json!(id);
        if legacy {
            request["caller"] = json!({ "runner": RUNNER });
        }
        assert_credential(
            &runner.call("route.account_for", &request).await,
            if consumer == "codemode" {
                "apikey:test"
            } else {
                "oauth:test"
            },
        );
    }
}

#[tokio::test]
async fn the_frozen_empty_mapping_joins_only_its_single_catalog_vendor() {
    use prefrontal_routing_core::{FactSource, RouteStateStore};
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("openai", true);
    let fixture: Value =
        serde_json::from_str(include_str!("../../fixtures/claustrum-list-scoped.json")).unwrap();
    let row = fixture["reply"]["result"]["credentials"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == "chatgpt:openai")
        .unwrap();
    let mut row = ListedCredential {
        id: row["id"].as_str().unwrap().into(),
        credential_type: row["type"].as_str().unwrap().into(),
        auth_method: Some(row["auth_method"].as_str().unwrap().into()),
        state: row["state"].as_str().unwrap().into(),
        categories: serde_json::from_value(row["categories"].clone()).unwrap(),
        provider_ids: serde_json::from_value(row["provider_ids"].clone()).unwrap(),
        serves: serde_json::from_value(row["serves"].clone()).unwrap(),
        ..Default::default()
    };
    let mut fact = harness
        .store()
        .get_facts()
        .unwrap()
        .into_iter()
        .find(|fact| fact.provider_id == "openai" && fact.model_id == "model")
        .unwrap();
    fact.auth_methods = Some(vec!["chatgpt".into()]);
    harness
        .store()
        .upsert_fact(&fact, FactSource::Override)
        .unwrap();
    catalog_fact(&harness, &fact);
    fact.billing_plane = Some(BillingPlane {
        provider_id: "openai".into(),
        auth_method: "chatgpt".into(),
        kind: "subscription".into(),
        selection_reason: "test subscription plane".into(),
        rates: [("rate.output".into(), BillingRate::Priced { rate: 1.0 })]
            .into_iter()
            .collect(),
    });
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact.clone()]),
    );
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "single-vendor".into(),
            credentials: vec![row.clone()],
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &[fact.clone()], NOW as i64);
    assert!(credentials.mapped_providers().contains("openai"));
    assert_eq!(
        credentials.health_report(NOW).metrics.unwrap()["unmappedCredentialIds"],
        json!([])
    );
    harness.set_credentials(credentials.clone());
    let caller = harness.bind(reserved(OWNER), None).await;
    let selection = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":0}}),
        )
        .await;
    assert_eq!(selection.0, FrameType::Response, "{}", selection.1);
    let runner = harness.bind(reserved(RUNNER), None).await;
    assert_credential(
        &runner
            .call(
                "route.account_for",
                &account_request("openai", "single-vendor"),
            )
            .await,
        "chatgpt:openai",
    );
    row.serves.push("second-vendor".into());
    let second = crate::tests::fact("second-vendor", "model");
    harness
        .store()
        .upsert_fact(&second, FactSource::Override)
        .unwrap();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "ambiguous-vendor".into(),
            credentials: vec![row],
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &[fact, second], NOW as i64);
    assert!(!credentials.mapped_providers().contains("openai"));
    assert_eq!(
        credentials.health_report(NOW).metrics.unwrap()["unmappedCredentialIds"],
        json!(["chatgpt:openai"])
    );
    harness.set_credentials(credentials);
    let reply = runner
        .call(
            "route.account_for",
            &account_request("openai", "ambiguous-vendor"),
        )
        .await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_eligible_account");
}

fn oauth_reading(used_percent: f64) {
    let mut state = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_provider_reply(&mut state, "test-source", &serde_json::to_vec(&json!({
        "result":[{"provider":"test-source","apiProvider":"synthetic","account":"oauth-account","source":"vault",
            "fetchedAt":crate::account_selection::rfc3339(NOW as i64),
            "usage":{"primary":{"usedPercent":used_percent,"windowMinutes":300,
                "resetsAt":crate::account_selection::rfc3339(NOW as i64 + 3_600_000)}}}],
        "completeProviders":["test-source"]
    })).unwrap(), NOW as i64).unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
}

#[tokio::test]
async fn absent_readings_admit_without_fabricated_quota_and_known_headroom_orders_first() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    harness.edit_config(|config| config["providers"]["synthetic"]["subscription"] = json!(false));
    let runner = harness.bind(reserved(RUNNER), None).await;
    let absent = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "all-absent"),
        )
        .await;
    assert_eq!(absent.0, FrameType::Response, "{}", absent.1);
    assert!(
        absent.1["result"]["reason"]["quota_fetched_at_ms"].is_null(),
        "{}",
        absent.1
    );
    oauth_reading(10.0);
    for index in 0..10 {
        assert_credential(
            &runner
                .call(
                    "route.account_for",
                    &account_request("synthetic", &format!("known-first-{index}")),
                )
                .await,
            "oauth:test",
        );
    }
    oauth_reading(100.0);
    assert_credential(
        &runner
            .call(
                "route.account_for",
                &account_request("synthetic", "known-exhausted-with-absent-sibling"),
            )
            .await,
        "apikey:test",
    );
    let only_oauth = credentials
        .listed_credentials()
        .iter()
        .find(|row| row.id == "oauth:test")
        .unwrap()
        .clone();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "only-exhausted".into(),
            credentials: vec![only_oauth],
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    let reply = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "only-known-exhausted"),
        )
        .await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "no_eligible_account");
}

#[tokio::test]
async fn pay_per_token_never_uses_subscription_when_the_api_key_disappears() {
    let _clock = crate::InjectedClock::set(NOW);
    for key_has_reading in [false, true] {
        let (harness, mut credentials) = setup("synthetic", false);
        oauth_reading(10.0);
        if key_has_reading {
            crate::account_quota::apply_provider_reply(&mut crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap(), "key-source", &serde_json::to_vec(&json!({
                "result":[{"provider":"key-source","apiProvider":"synthetic","account":"apikey-account","source":"vault",
                    "fetchedAt":crate::account_selection::rfc3339(NOW as i64),
                    "usage":{"primary":{"usedPercent":20.0,"windowMinutes":300,
                        "resetsAt":crate::account_selection::rfc3339(NOW as i64 + 3_600_000)}}}],
                "completeProviders":["key-source"]
            })).unwrap(), NOW as i64).unwrap();
        }
        let caller = harness.bind(reserved(OWNER), None).await;
        let runner = harness.bind(reserved(RUNNER), None).await;
        let mut decisions = Vec::new();
        for consumer in ["codemode", "flow"] {
            let reply = caller
                .call(
                    "route.select",
                    &json!({"consumer":consumer,"requirements":{"iq":0}}),
                )
                .await;
            assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
            let id = reply.1["result"]["decision_id"]
                .as_str()
                .unwrap()
                .to_owned();
            let mut request = account_request("synthetic", consumer);
            request["decision_id"] = json!(id);
            assert_credential(
                &runner.call("route.account_for", &request).await,
                "apikey:test",
            );
            decisions.push((consumer, id));
        }
        let oauth = credentials
            .listed_credentials()
            .iter()
            .find(|row| row.id == "oauth:test")
            .unwrap()
            .clone();
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "api-key-removed".into(),
                credentials: vec![oauth],
                ..Default::default()
            },
            NOW,
        );
        harness.set_credentials(credentials);
        for coverage_override in [false, true] {
            if coverage_override {
                harness.edit_config(|config| {
                    config["providers"]["synthetic"]["subscription"] = json!(false)
                });
            }
            for (consumer, id) in &decisions {
                let mut request = account_request(
                    "synthetic",
                    &format!("removed-{consumer}-{coverage_override}"),
                );
                request["decision_id"] = json!(id);
                let reply = runner.call("route.account_for", &request).await;
                assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
                assert_eq!(reply.1["code"], "no_eligible_account");
            }
        }
    }
}
