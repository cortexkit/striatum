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

#[tokio::test]
async fn credential_probe_admission_is_single_and_completion_heals_its_credential() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    let shared = Arc::new(std::sync::Mutex::new(credentials));
    let refusal = CredentialRefusal {
        provider_id: "synthetic".into(),
        credential_id: "oauth:test".into(),
        code: "billing_exhausted".into(),
    };
    let recorded = NOW as i64 - crate::credential_fences::FENCE_PROBE_RECHECK_MS;
    crate::credential_fences::record(harness.store(), &shared, &refusal, recorded).unwrap();
    let runner = harness.bind(reserved(RUNNER), None).await;
    let blocked = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "exactly-six-hours"),
        )
        .await;
    assert_eq!(blocked.1["code"], "no_eligible_account", "{}", blocked.1);
    let _later = crate::InjectedClock::set(NOW + 1);
    let config = harness.operator_config();
    let policy = config.allocator_policy("synthetic");
    let cached = crate::account_selection::ClassContext::load(
        harness.store(),
        &shared,
        config,
        crate::ACCOUNT_QUOTA.get(),
        NOW as i64 + 1,
    )
    .unwrap();
    let caller = harness.bind(reserved(OWNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30},"pin":"synthetic/model"}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    assert_eq!(
        harness.store().credential_entitlement_fences().unwrap()[0].observed_at_ms,
        recorded,
        "model preflight cannot consume the probe"
    );
    let mut request = account_request("synthetic", "probe");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    let admitted = runner.call("route.account_for", &request).await;
    assert_credential(&admitted, "oauth:test");
    assert_eq!(
        admitted.1["result"]["reason"]["code"],
        "credential_fence_probe"
    );
    assert_eq!(
        runner.call("route.account_for", &request).await,
        admitted,
        "replay does not spend another probe"
    );
    assert_eq!(
        harness.store().credential_entitlement_fences().unwrap()[0].observed_at_ms,
        NOW as i64 + 1
    );
    let account_decision = harness
        .store()
        .account_transaction(|tx| {
            Ok(tx
                .admission(&format!("reserved:{RUNNER}"), "probe")?
                .unwrap()
                .decision_id)
        })
        .unwrap();
    let logged = harness
        .store()
        .account_decision(&account_decision)
        .unwrap()
        .unwrap();
    let logged: Value = serde_json::from_str(&logged.decision_json).unwrap();
    assert!(logged["explanation"]["credential_fence_probe"]
        .as_str()
        .unwrap()
        .contains("oauth:test"));
    let mut stale_request = account_request("synthetic", "racing-probe");
    stale_request["caller"] = json!({"runner":RUNNER});
    let body = serde_json::to_vec(
        &json!({"method":crate::account_selection::ACCOUNT_FOR_OP,"params":stale_request}),
    )
    .unwrap();
    let ctx = crate::account_selection::Ctx {
        store: harness.store(),
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: crate::config_overrides::AccountSelectionFlags { enabled: true },
        policy: &policy,
        now_ms: NOW as i64 + 1,
        classes: Some(cached),
    };
    assert_eq!(
        crate::account_selection::handle(
            crate::account_selection::ACCOUNT_FOR_OP,
            &body,
            &format!("reserved:{RUNNER}"),
            &ctx
        )
        .code(),
        Some("no_eligible_account"),
        "a stale cached roster cannot reserve a second probe"
    );
    let completed = caller
        .call(
            "route.report",
            &json!({"decision_id":selected.1["result"]["decision_id"],"outcome":"completed"}),
        )
        .await;
    assert_eq!(completed.0, FrameType::Response, "{}", completed.1);
    assert!(harness
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .is_empty());
    let healed = runner
        .call("route.account_for", &account_request("synthetic", "healed"))
        .await;
    assert_credential(&healed, "oauth:test");
    assert_ne!(
        healed.1["result"]["reason"]["code"],
        "credential_fence_probe"
    );
}

#[tokio::test]
async fn a_credential_probe_is_a_fallback_and_an_entitlement_failure_renews_it() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["subscription"] = json!(false);
        config["providers"]["synthetic"]["accounts"] =
            json!({"strategy":"ordered","order":["oauth-account","apikey-account"]});
    });
    let shared = Arc::new(std::sync::Mutex::new(credentials));
    let stale = NOW as i64 - crate::credential_fences::FENCE_PROBE_RECHECK_MS - 1;
    let refusal = |credential: &str| CredentialRefusal {
        provider_id: "synthetic".into(),
        credential_id: credential.into(),
        code: "ENTITLEMENT_ERROR".into(),
    };
    crate::credential_fences::record(harness.store(), &shared, &refusal("oauth:test"), stale)
        .unwrap();
    let runner = harness.bind(reserved(RUNNER), None).await;
    assert_credential(
        &runner
            .call(
                "route.account_for",
                &account_request("synthetic", "healthy-before-probe"),
            )
            .await,
        "apikey:test",
    );
    assert_eq!(
        harness.store().credential_entitlement_fences().unwrap()[0].observed_at_ms,
        stale
    );
    crate::credential_fences::record(
        harness.store(),
        &shared,
        &refusal("apikey:test"),
        NOW as i64,
    )
    .unwrap();
    let caller = harness.bind(reserved(OWNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let mut request = account_request("synthetic", "entitlement-probe");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    let admitted = runner.call("route.account_for", &request).await;
    assert_credential(&admitted, "oauth:test");
    assert_eq!(
        admitted.1["result"]["reason"]["code"],
        "credential_fence_probe"
    );
    let _later = crate::InjectedClock::set(NOW + 1);
    let failed=caller.call("route.report",&json!({"decision_id":selected.1["result"]["decision_id"],"outcome":"failed","credential_id":"oauth:test","runner_code":"ENTITLEMENT_ERROR"})).await;
    assert_eq!(failed.0, FrameType::Response, "{}", failed.1);
    let fence = harness
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .into_iter()
        .find(|fence| fence.credential_id == "oauth:test")
        .unwrap();
    assert_eq!(fence.observed_at_ms, NOW as i64 + 1);
    assert_eq!(fence.code, "ENTITLEMENT_ERROR");
    let blocked = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "after-entitlement-probe"),
        )
        .await;
    assert_eq!(blocked.1["code"], "no_eligible_account", "{}", blocked.1);
}

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

pub(super) fn setup(provider: &str, converted_live: bool) -> (Arc<Harness>, BrocaCredentials) {
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
    credentials.refresh_serving_methods(&Default::default(), &[fact.clone()], NOW as i64, &[]);
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
    credentials.refresh_serving_methods(&Default::default(), &[fact.clone()], NOW as i64, &[]);
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
    credentials.refresh_serving_methods(&Default::default(), &[fact, second], NOW as i64, &[]);
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
async fn provider_strategies_never_escape_the_subscription_class() {
    let _clock = crate::InjectedClock::set(NOW);
    for strategy in ["round_robin", "ordered"] {
        let (harness, _) = setup("synthetic", false);
        oauth_reading(10.0);
        harness.edit_config(|config| config["providers"]["synthetic"]["accounts"] = json!({
            "strategy":strategy,"ordered":"fallback-first","order":["oauth-account","apikey-account"],
            "max_hold":"30s","hold_when_reset_within":"15m","unknown_reset_retry":"2h"
        }));
        let runner = harness.bind(reserved(RUNNER), None).await;
        for index in 0..10 {
            assert_credential(
                &runner
                    .call(
                        "route.account_for",
                        &account_request("synthetic", &format!("{strategy}-{index}")),
                    )
                    .await,
                "oauth:test",
            );
        }
        exhaust_oauth(&harness, "synthetic");
        let blocked = runner
            .call(
                "route.account_for",
                &account_request("synthetic", &format!("{strategy}-blocked")),
            )
            .await;
        assert_eq!(blocked.0, FrameType::Error, "{}", blocked.1);
        assert!(
            matches!(
                blocked.1["code"].as_str(),
                Some("account_hold" | "no_eligible_account")
            ),
            "{}",
            blocked.1
        );
        harness.edit_config(|config| {
            config["providers"]["synthetic"]["accounts"]["metered_fallback"] = json!(true)
        });
        assert_credential(
            &runner
                .call(
                    "route.account_for",
                    &account_request("synthetic", &format!("{strategy}-fallback")),
                )
                .await,
            "apikey:test",
        );
    }
}

#[tokio::test]
async fn child_avoids_the_parents_started_pin() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    harness.edit_config(|config| config["providers"]["synthetic"]["subscription"] = json!(false));
    let runner = harness.bind(reserved(RUNNER), None).await;
    let parent=runner.call("route.account_for",&json!({"provider":"synthetic","model":"model","conversation_key":"parent","admission_id":"parent"})).await;
    assert_eq!(parent.0, FrameType::Response, "{}", parent.1);
    let parent_account = parent.1["result"]["account"]["account_id"]
        .as_str()
        .unwrap();
    let started=runner.call("route.account_observe",&json!({"event_id":"parent-start","kind":"admission_started","admission_id":"parent","run_id":"run","served_account_id":parent_account})).await;
    assert_eq!(started.0, FrameType::Response, "{}", started.1);
    let child=runner.call("route.account_for",&json!({"provider":"synthetic","model":"model","conversation_key":"child","parent_conversation_key":"parent","admission_id":"child"})).await;
    assert_eq!(child.0, FrameType::Response, "{}", child.1);
    assert_ne!(
        child.1["result"]["account"]["account_id"],
        parent.1["result"]["account"]["account_id"]
    );
    let conflict=runner.call("route.account_for",&json!({"provider":"synthetic","model":"model","conversation_key":"child","parent_conversation_key":"different-parent","admission_id":"child"})).await;
    assert_eq!(conflict.1["code"], "admission_conflict", "{}", conflict.1);
}

#[tokio::test]
async fn model_eligibility_applies_to_pins_and_clears_with_the_vault_listing() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    let first = crate::tests::fact("synthetic", "model");
    let mut other = crate::tests::fact("synthetic", "other");
    other.auth_methods = Some(vec!["oauth".into(), "apikey".into()]);
    harness.install_facts(&[first.clone(), other.clone()]);
    catalog_fact(&harness, &other);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[first, other]),
    );
    harness.set_credentials(credentials.clone());
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let admitted = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "not-found"),
        )
        .await;
    assert_credential(&admitted, "oauth:test");
    let observed=runner.call("route.account_observe",&json!({"event_id":"not-found","kind":"scoped_not_found","admission_id":"not-found","credential_id":"oauth:test","view_digest":"mixed-vault"})).await;
    assert_eq!(observed.0, FrameType::Response, "{}", observed.1);
    assert_eq!(harness.store().account_eligibility().unwrap().len(), 1);
    let pin = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30},"pin":"synthetic/model"}),
        )
        .await;
    assert_eq!(pin.0, FrameType::Error, "{}", pin.1);
    assert_ne!(pin.1["code"], "account_hold");
    let next = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "not-found-next"),
        )
        .await;
    assert_eq!(next.1["code"], "no_eligible_account", "{}", next.1);
    let mut unaffected = account_request("synthetic", "404-other-model");
    unaffected["model"] = json!("other");
    assert_credential(
        &runner.call("route.account_for", &unaffected).await,
        "oauth:test",
    );
    let mut rows = credentials.listed_credentials().to_vec();
    rows.iter_mut()
        .find(|row| row.id == "oauth:test")
        .unwrap()
        .plan_tier = Some("changed-plan".into());
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "changed-vault".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    assert_credential(
        &runner
            .call(
                "route.account_for",
                &account_request("synthetic", "listed-again"),
            )
            .await,
        "oauth:test",
    );
    assert!(harness.store().account_eligibility().unwrap().is_empty());
}

#[tokio::test]
async fn report_eligibility_is_model_or_account_scoped_and_never_a_hold() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    let mut other = crate::tests::fact("synthetic", "other");
    other.auth_methods = Some(vec!["oauth".into(), "apikey".into()]);
    let first = crate::tests::fact("synthetic", "model");
    harness.install_facts(&[first.clone(), other.clone()]);
    catalog_fact(&harness, &other);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[first, other]),
    );
    harness.set_credentials(credentials);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    for (index, scope) in ["model", "account"].into_iter().enumerate() {
        let pin = if scope == "model" {
            "synthetic/other"
        } else {
            "synthetic/model"
        };
        let decision = caller
            .call(
                "route.select",
                &json!({"consumer":"mason","requirements":{"iq":30},"pin":pin}),
            )
            .await;
        assert_eq!(decision.0, FrameType::Response, "{}", decision.1);
        let report=caller.call("route.report",&json!({"decision_id":decision.1["result"]["decision_id"],"outcome":"refused","credential_id":"oauth:test","eligibility":{"scope":scope,"code":"not_on_plan"}})).await;
        assert_eq!(report.0, FrameType::Response, "{}", report.1);
        let mut request = account_request("synthetic", &format!("eligibility-{index}"));
        if scope == "model" {
            assert_credential(
                &runner.call("route.account_for", &request).await,
                "oauth:test",
            );
        } else {
            let answer = runner.call("route.account_for", &request).await;
            assert_eq!(answer.1["code"], "no_eligible_account", "{}", answer.1);
        }
        request["model"] = json!("other");
        request["admission_id"] = json!(format!("other-{index}"));
        let answer = runner.call("route.account_for", &request).await;
        assert_eq!(answer.1["code"], "no_eligible_account", "{}", answer.1);
    }
}

#[tokio::test]
async fn size_shaped_429_restricts_only_its_input_band_and_success_clears_it() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let select = json!({"consumer":"mason","requirements":{"iq":30},"pin":"synthetic/model"});
    let decision = caller.call("route.select", &select).await;
    assert_eq!(decision.0, FrameType::Response, "{}", decision.1);
    let report=caller.call("route.report",&json!({"decision_id":decision.1["result"]["decision_id"],"outcome":"refused","credential_id":"oauth:test","runner_code":"size_shaped_429","request_input_tokens":169_000,"reset_at_ms":NOW+600_000})).await;
    assert_eq!(report.0, FrameType::Response, "{}", report.1);
    let mut large = account_request("synthetic", "large");
    large["initial_input_bytes"] = json!(169_000 * 4);
    let blocked = runner.call("route.account_for", &large).await;
    assert_eq!(blocked.1["code"], "no_eligible_account", "{}", blocked.1);
    let mut small = account_request("synthetic", "small");
    small["initial_input_bytes"] = json!(4096 * 4);
    assert_credential(
        &runner.call("route.account_for", &small).await,
        "oauth:test",
    );
    let next = caller.call("route.select", &select).await;
    assert_eq!(next.0, FrameType::Response, "{}", next.1);
    let _later = crate::InjectedClock::set(NOW + 1);
    let success=caller.call("route.report",&json!({"decision_id":next.1["result"]["decision_id"],"outcome":"completed","credential_id":"oauth:test","request_input_tokens":169_000})).await;
    assert_eq!(success.0, FrameType::Response, "{}", success.1);
    large["admission_id"] = json!("large-after-success");
    assert_credential(
        &runner.call("route.account_for", &large).await,
        "oauth:test",
    );
}

#[tokio::test]
async fn entitlement_is_credential_wide_and_old_billing_refusals_keep_other_keys_usable() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    let mut rows = credentials.listed_credentials().to_vec();
    for row in &mut rows {
        row.credential_type = "apikey".into();
        row.auth_method = Some("apikey".into());
    }
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "two-keys".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials.clone());
    let runner = harness.bind(reserved(RUNNER), None).await;
    let caller = harness.bind(reserved(OWNER), None).await;
    for (index, code) in ["billing_exhausted", "ENTITLEMENT_ERROR"]
        .into_iter()
        .enumerate()
    {
        crate::credential_fences::record(
            harness.store(),
            &Arc::new(std::sync::Mutex::new(credentials.clone())),
            &CredentialRefusal {
                provider_id: "synthetic".into(),
                credential_id: "apikey:test".into(),
                code: code.into(),
            },
            NOW as i64 - 20 * 60_000,
        )
        .unwrap();
        assert_credential(
            &runner
                .call(
                    "route.account_for",
                    &account_request("synthetic", &format!("other-key-{index}")),
                )
                .await,
            "oauth:test",
        );
        let selected = caller
            .call(
                "route.select",
                &json!({"consumer":"mason","requirements":{"iq":30}}),
            )
            .await;
        assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
        let mut bound = account_request("synthetic", &format!("bound-other-key-{index}"));
        bound["decision_id"] = selected.1["result"]["decision_id"].clone();
        assert_credential(
            &runner.call("route.account_for", &bound).await,
            "oauth:test",
        );
    }
    let mut other = crate::tests::fact("synthetic", "other");
    other.auth_methods = Some(vec!["apikey".into()]);
    let first = crate::tests::fact("synthetic", "model");
    harness.install_facts(&[first.clone(), other.clone()]);
    catalog_fact(&harness, &other);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[first, other]),
    );
    harness.set_credentials(credentials.clone());
    let mut request = account_request("synthetic", "other-model");
    request["model"] = json!("other");
    assert_credential(
        &runner.call("route.account_for", &request).await,
        "oauth:test",
    );
    let mut rows = credentials.listed_credentials().to_vec();
    rows.iter_mut()
        .find(|row| row.id == "apikey:test")
        .unwrap()
        .plan_tier = Some("new".into());
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "new-listing".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    assert_credential(
        &runner
            .call(
                "route.account_for",
                &account_request("synthetic", "key-restored"),
            )
            .await,
        "apikey:test",
    );
}

#[tokio::test]
async fn ladder_holds_a_blocked_entry_but_demand_and_no_wait_use_the_next_model() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    let first = crate::tests::fact("synthetic", "model");
    let mut other = crate::tests::fact("secondary", "model");
    other.auth_methods = Some(vec!["oauth".into(), "apikey".into()]);
    harness.install_facts(&[first.clone(), other.clone()]);
    catalog_fact(&harness, &other);
    let mut rows = credentials.listed_credentials().to_vec();
    for row in &mut rows {
        row.provider_ids.push("secondary".into());
    }
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "two-providers".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[first, other]),
    );
    harness.set_credentials(credentials);
    harness.edit_config(|config| config["consumers"]["mason"]=json!({"owner":OWNER,"mode":"ladder","ladder":["synthetic/model","secondary/model"]}));
    exhaust_oauth(&harness, "synthetic");
    let caller = harness.bind(reserved(OWNER), None).await;
    let request = json!({"consumer":"mason","requirements":{"iq":30}});
    let held = caller.call("route.select", &request).await;
    assert_eq!(held.0, FrameType::Response, "{}", held.1);
    assert_eq!(held.1["result"]["route"]["provider"], "synthetic");
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut admission = account_request("synthetic", "selected-held-route");
    admission["decision_id"] = held.1["result"]["decision_id"].clone();
    let held_account = runner.call("route.account_for", &admission).await;
    assert_eq!(held_account.1["code"], "account_hold", "{}", held_account.1);
    let mut no_wait = request.clone();
    no_wait["hold_ok"] = json!(false);
    let moved = caller.call("route.select", &no_wait).await;
    assert_eq!(moved.0, FrameType::Response, "{}", moved.1);
    assert_eq!(moved.1["result"]["route"]["provider"], "secondary");
    harness
        .edit_config(|config| config["providers"]["synthetic"]["accounts"]["max_hold"] = json!(30));
    let moved = caller.call("route.select", &request).await;
    assert_eq!(moved.0, FrameType::Response, "{}", moved.1);
    assert_eq!(moved.1["result"]["route"]["provider"], "secondary");
    harness.edit_config(|config| config["consumers"]["mason"]["mode"] = json!("demand"));
    let demand = caller.call("route.select", &request).await;
    assert_eq!(demand.0, FrameType::Response, "{}", demand.1);
    assert_eq!(demand.1["result"]["route"]["provider"], "secondary");
}

#[tokio::test]
async fn paid_credit_readings_are_explicit_fresh_inputs_and_refusals_cannot_rewrite_them() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    oauth_reading(100.0);
    let state = crate::ACCOUNT_QUOTA.get().unwrap();
    {
        let mut guard = state.lock().unwrap();
        let reading = guard.readings.readings.values_mut().next().unwrap();
        reading.entry["spend"] = json!([{"id":"credits","label":"Credits","funding":"purchased","basis":"reported","remaining":{"minor":100,"exponent":0,"unit":"credit"}}]);
    }
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["accounts"]["paid_credits"] = json!(true)
    });
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let admitted = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "credits"),
        )
        .await;
    assert_credential(&admitted, "oauth:test");
    assert_eq!(
        admitted.1["result"]["reason"]["code"], "paid_credits",
        "{}",
        admitted.1
    );
    let before = state.lock().unwrap().readings.readings.clone();
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let second = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(second.0, FrameType::Response, "{}", second.1);
    for (decision, code) in [
        (selected, "credit_insufficient"),
        (second, "billing_exhausted"),
    ] {
        let refused=caller.call("route.report",&json!({"decision_id":decision.1["result"]["decision_id"],"outcome":"refused","credential_id":"oauth:test","runner_code":code})).await;
        assert_eq!(refused.0, FrameType::Response, "{}", refused.1);
        assert_eq!(
            state.lock().unwrap().readings.readings,
            before,
            "refusal is not a credit reading"
        );
    }
    let blocked = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "credits-refused"),
        )
        .await;
    assert_eq!(blocked.1["code"], "no_eligible_account", "{}", blocked.1);
}

#[tokio::test]
async fn failure_reports_with_resets_are_advisory_and_preserve_account_affinity() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let before_account = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "before-failure"),
        )
        .await;
    assert_eq!(
        before_account.0,
        FrameType::Response,
        "{}",
        before_account.1
    );
    let request = json!({"consumer":"mason","requirements":{"iq":30},"affinity":"sticky"});
    let first = caller.call("route.select", &request).await;
    assert_eq!(first.0, FrameType::Response, "{}", first.1);
    let report=caller.call("route.report",&json!({"decision_id":first.1["result"]["decision_id"],"outcome":"failed","runner_code":"unconfirmed_429","reset_at_ms":NOW+3_600_000})).await;
    assert_eq!(report.0, FrameType::Response, "{}", report.1);
    assert!(harness
        .store()
        .v1_affinity_decision(&format!("reserved:{OWNER}"), "mason", "sticky")
        .unwrap()
        .is_none());
    let second = caller.call("route.select", &request).await;
    assert_eq!(second.0, FrameType::Response, "{}", second.1);
    assert_eq!(second.1["result"]["route"], first.1["result"]["route"]);
    let mut request = account_request("synthetic", "after-failure");
    request["conversation_key"] = json!("conversation-before-failure");
    let after_account = runner.call("route.account_for", &request).await;
    assert_eq!(after_account.0, FrameType::Response, "{}", after_account.1);
    assert_eq!(
        after_account.1["result"]["pin"]["id"],
        before_account.1["result"]["pin"]["id"]
    );
    assert!(harness
        .store()
        .account_size_refusals(NOW as i64)
        .unwrap()
        .is_empty());
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
