//! What `route.select` answers for a key-pool provider.
//!
//! A key-pool provider (`key_pool`) is one an API key names but the catalog
//! does not: its routes are the rows it borrows from the one catalog provider
//! its active keys `serves`. Legacy and routing/v1 select admit those routes;
//! v1 names the pool and credential by public id in the runner's auth hint.

use std::sync::Arc;
use std::time::{Duration, Instant};

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use cortexkit_role_routing_conformance::wire::select::{
    check_answer, parse_select_request, SelectAnswer,
};
use cortexkit_role_routing_conformance::CatalogModel;
use serde_json::{json, Value};
use striatum_core::billing::{BillingPlane, BillingRate};
use striatum_core::ModelFact;
use striatum_store::{FusiformCatalogCursor, FusiformFactUpdate};
use subc_protocol::FrameType;

use super::conformance_tests::{catalog_fact, reserved, Harness};
use crate::broca_credentials::BrocaCredentials;

mod credential_refused_tests;

mod account_writer {
    use super::*;

    async fn ok(
        route: &super::super::conformance_tests::Route,
        method: &str,
        params: Value,
    ) -> Value {
        let reply = route.call(method, &params).await;
        assert_eq!(reply.0, FrameType::Response, "{method}: {}", reply.1);
        reply.1["result"].clone()
    }

    #[tokio::test]
    async fn handler_and_background_coverage_has_no_outside_writer_changes() {
        let now = 1_800_000_000_000;
        let _clock = crate::InjectedClock::set(now);
        let harness = harness_with_routed_accounts(Some(&["apikey"]));
        let owner = harness.bind(reserved("example-consumer"), None).await;
        let runner = harness.bind(reserved("zeta-runner"), None).await;
        let operator = harness.bind(json!({"kind":"direct"}), None).await;
        let store = harness.store();
        let outside = store.outside_account_writer_count_for_test();
        let selected = ok(&owner, "route.select", json!({"consumer":"mason","requirements":{"iq":30},"pin":format!("{POOL}/{POOL_MODEL_SHORT}"),"affinity":"writer-coverage"})).await;
        let affinities: i64 = store
            .with_reader_for_test(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM routing_v1_affinity WHERE decision_id=?1",
                    [selected["decision_id"].as_str().unwrap()],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(
            affinities, 1,
            "selection coverage must include a real account affinity write"
        );
        let admission = json!({"provider":POOL,"model":POOL_MODEL_SHORT,"decision_id":selected["decision_id"],"conversation_key":"writer-coverage","admission_id":"writer-coverage"});
        let admitted = ok(&runner, "route.account_for", admission.clone()).await;
        assert_eq!(ok(&runner, "route.account_for", admission).await, admitted);
        ok(&runner, "route.account_observe", json!({"kind":"admission_started","event_id":"writer-start","admission_id":"writer-coverage","run_id":"writer-run"})).await;
        let lease = ok(
            &operator,
            "route.pool_acquire",
            json!({"provider":POOL,"model":POOL_MODEL_SHORT,"session":"writer-session"}),
        )
        .await;
        ok(&operator, "route.pool_renew", json!({"provider":POOL,"session":"writer-session","leaseId":lease["leaseId"],"credentialId":lease["credentialId"]})).await;
        ok(&operator, "route.pool_wall", json!({"provider":POOL,"credentialId":lease["credentialId"],"kind":"rate_limited","untilMs":now+60_000})).await;
        ok(&operator, "route.pool_release", json!({"provider":POOL,"session":"writer-session","leaseId":lease["leaseId"],"endSession":true})).await;
        ok(&runner, "route.account_for", json!({"mode":"continue","run_id":"writer-run","admission_id":"writer-coverage","conversation_key":"writer-coverage","previous_decision_id":selected["decision_id"],"selection_id":"writer-continuation",
            "exhausted":{"credential_id":admitted["account"]["credential_id"],"account":admitted["account"]["account_id"],"limit_window":"five_hour","observed_at_ms":now,"resets_at_ms":now+300_000}})).await;
        ok(&runner, "route.account_observe", json!({"kind":"run_finished","event_id":"writer-finish","admission_id":"writer-coverage","run_id":"writer-run"})).await;
        ok(
            &runner,
            "route.account_release",
            json!({"admission_id":"writer-coverage"}),
        )
        .await;
        ok(
            &owner,
            "route.report",
            json!({"decision_id":selected["decision_id"],"outcome":"completed"}),
        )
        .await;
        ok(
            &operator,
            "route.clear_credential_fences",
            json!({"credentialId":"apikey:pool-vendor:main"}),
        )
        .await;

        let plain = ok(
            &owner,
            "route.select",
            json!({"consumer":"mason","requirements":{"iq":30},"pin":PLAIN}),
        )
        .await;
        let plain_admission = ok(&runner, "route.account_for", json!({"provider":BORROWED,"model":"model-a","decision_id":plain["decision_id"],"conversation_key":"writer-plain","admission_id":"writer-plain"})).await;
        let plain_key = plain_admission["account"]["credential_id"].clone();
        ok(&runner, "route.account_observe", json!({"event_id":"writer-ineligible","kind":"scoped_not_found","admission_id":"writer-plain","credential_id":plain_key,"view_digest":"key-pool-listing"})).await;
        assert!(!store.account_eligibility().unwrap().is_empty());
        ok(&runner, "route.account_observe", json!({"event_id":"writer-credit","kind":"account_limit_exhausted","admission_id":"writer-plain","limit_window":"credit"})).await;
        ok(&operator, "route.clear_cooldown", json!({"routeKey":PLAIN})).await;
        ok(&runner, "route.account_observe", json!({"event_id":"writer-credential","kind":"credential_refused","admission_id":"writer-plain","provider_code":"ENTITLEMENT_ERROR"})).await;
        assert!(!store.credential_entitlement_fences().unwrap().is_empty());
        ok(&owner, "route.report", json!({"decision_id":plain["decision_id"],"outcome":"refused","credential_id":plain_key,"runner_code":"size_shaped_429","request_input_tokens":169_000,"reset_at_ms":now+60_000})).await;
        assert!(store
            .fence_rechecks()
            .unwrap()
            .iter()
            .any(|origin| origin.kind == "size" || origin.kind == "non_quota_size"));
        let credentials = harness.credentials();
        let original_listing = credentials.lock().unwrap().listed_credentials().to_vec();
        let mut replaced_listing = original_listing.clone();
        for row in &mut replaced_listing {
            row.plan_tier = Some("writer-replaced-plan".into());
        }
        for (view, rows) in [
            ("writer-replaced", replaced_listing),
            ("writer-restored", original_listing),
        ] {
            credentials.lock().unwrap().apply_listing(
                &ScopedCredentialListing {
                    view: view.into(),
                    credentials: rows,
                    ..Default::default()
                },
                now,
            );
            ok(
                &owner,
                "route.select",
                json!({"consumer":"mason","requirements":{"iq":30},"pin":PLAIN}),
            )
            .await;
            assert!(store.account_eligibility().unwrap().is_empty());
            assert!(store.credential_entitlement_fences().unwrap().is_empty());
        }
        let mut persisted = None;
        let roster = striatum_store::PersistedVaultRoster {
            view: "writer-roster".into(),
            roster_json: "{}".into(),
            fetched_at: now as i64,
        };
        crate::account_quota::persist_roster_if_changed(store, roster.clone(), &mut persisted)
            .await
            .unwrap();
        let roster_commands =
            store.account_command_count_for_test(striatum_store::AccountCommandKind::Maintenance);
        crate::account_quota::persist_roster_if_changed(store, roster, &mut persisted)
            .await
            .unwrap();
        assert_eq!(
            store.account_command_count_for_test(striatum_store::AccountCommandKind::Maintenance),
            roster_commands
        );
        let mut quota = crate::quota_events::bench::QuotaBurst::new(store.clone(), 1).await;
        quota.next().await;
        assert_eq!(quota.applied_bodies(), 2);
        for provider_wide in [false, true] {
            ok(&operator, "route.record_cooldown", json!({"routeKey":PLAIN,"capacityKey":PLAIN,"cooldownUntil":now+60_000,"providerWide":provider_wide,"lastError":"HTTP 503 provider maintenance"})).await;
        }

        let snapshot = crate::fusiform_catalog::decode_catalog_get(&serde_json::to_vec(&json!({
            "catalog_version":100,"resolved_at_ms":now,
            "models":{format!("{BORROWED}/writer-new-model"):{"existence":"present","capability.tool_call":true},PLAIN:{"existence":"present"},POOL_ROUTE:{"existence":"present"}}
        })).unwrap()).unwrap();
        crate::fusiform_catalog::apply_mapped_catalog(
            store,
            &snapshot,
            Some(&harness.credentials()),
            Some(BORROWED),
        )
        .unwrap();
        let inherited: bool = store.with_reader_for_test(|conn| conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM model_route_state WHERE route_key=?1 AND cooldown_until>?2)",
            rusqlite::params![format!("{BORROWED}/writer-new-model"), now as i64], |row| row.get(0)
        )).unwrap();
        assert!(
            inherited,
            "the catalogue insert must create inherited route state"
        );
        ok(
            &operator,
            "route.model_upsert",
            json!({"providerID":BORROWED,"modelID":"writer-new-model","elo":1000,"eq":1000}),
        )
        .await;
        ok(&operator, "route.clear_cooldown", json!({"routeKey":PLAIN})).await;
        ok(
            &operator,
            "route.clear_failure_backoff",
            json!({"routeKey":PLAIN}),
        )
        .await;
        crate::fusiform_catalog::report_unserved_routes(
            store,
            &Arc::new(std::sync::Mutex::new(
                crate::fusiform_catalog::CatalogReachHealth::new(600_000),
            )),
        )
        .unwrap();
        assert_eq!(
            store.account_command_count_for_test(striatum_store::AccountCommandKind::Tick),
            0
        );
        crate::account_writer::tick(store, now as i64 + 120_000).unwrap();
        let budget = crate::retention::PassBudget {
            batch_rows: 64,
            detail_batch_rows: 64,
            max_detail_batches: 1,
            max_batches_per_table: 1,
            max_shadow_purge_batches: 1,
            pause: Duration::ZERO,
        };
        let report = crate::retention::run_retention_pass(
            store,
            &crate::retention::RetentionWindows::default(),
            now as i64 + 40 * 86_400_000,
            &budget,
        );
        assert!(
            report.account_admissions + report.account_continuations > 0,
            "{report:?}"
        );
        assert_eq!(store.outside_account_writer_count_for_test(), outside);
    }
}

/// The catalog provider whose rows the key pool borrows.
const BORROWED: &str = "catalog-provider";
/// The key-pool provider: no catalog rows of its own.
const POOL: &str = "pool-provider";
/// The catalog provider's own route.
const PLAIN: &str = "catalog-provider/model-a";
/// A second catalog provider that the key pool borrows from. It sells nothing
/// of its own; every row it has comes from the pool key's `serves`.
const POOL_VENDOR: &str = "pool-vendor";
/// The one route `POOL_VENDOR` sells, which the key pool borrows and is the
/// cheapest route of all.
const POOL_ROUTE: &str = "pool-vendor/model-pool";
/// The model the pool provider's own route carries, borrowed from `POOL_ROUTE`.
/// A pin or a ladder entry names this route on the pool provider itself.
const POOL_MODEL_SHORT: &str = "model-pool";

fn model(route_key: &str, iq: u8) -> CatalogModel {
    let (provider, model) = route_key.split_once('/').unwrap();
    CatalogModel {
        provider: provider.into(),
        model: model.into(),
        family: None,
        iq,
        context_window: 200_000,
        max_output_tokens: 32_000,
        tool_calls: true,
        modalities: Default::default(),
        structured_output: false,
        priced_per_token: true,
    }
}

/// The catalog's rows: the catalog provider's own, and the row the key pool
/// borrows from `POOL_VENDOR`.
fn catalog_models() -> Vec<CatalogModel> {
    // The key pool's route is both the ablest and (by the pricing below) the
    // cheapest, so it wins wherever it is allowed to decide and is the only
    // route a high demand can use.
    vec![model(POOL_ROUTE, 100), model(PLAIN, 30)]
}

/// One routing fact per catalog row, each with a billing plane.
fn routing_facts() -> Vec<ModelFact> {
    catalog_models()
        .iter()
        .enumerate()
        .map(|(rank, model)| {
            let mut fact = catalog_fact(model, rank);
            fact.billing_plane = Some(BillingPlane {
                provider_id: fact.provider_id.clone(),
                auth_method: "apikey".into(),
                kind: "upstream".into(),
                selection_reason: "test credential".into(),
                rates: [(
                    "rate.output".to_owned(),
                    BillingRate::Priced {
                        rate: fact.cost_per_m_tok_out.unwrap(),
                    },
                )]
                .into_iter()
                .collect(),
            });
            fact.auth_methods = Some(vec!["apikey".into()]);
            // The key pool's route is the cheaper of the two, so cost alone
            // would choose it wherever nothing stops it.
            if fact.model_id.ends_with("model-pool") {
                fact.cost_tier = Some(0.5);
                fact.cost_per_m_tok_out = Some(0.5);
                if let Some(plane) = fact.billing_plane.as_mut() {
                    plane
                        .rates
                        .insert("rate.output".to_owned(), BillingRate::Priced { rate: 0.5 });
                }
            }
            fact
        })
        .collect()
}

fn key(id: &str, providers: &[&str], serves: &[&str]) -> ListedCredential {
    ListedCredential {
        id: id.into(),
        account_id: (!providers.contains(&POOL)).then(|| format!("{id}-workspace")),
        categories: vec!["llm-provider".into()],
        credential_type: "apikey".into(),
        auth_method: Some("apikey".into()),
        provider_ids: providers.iter().map(|p| (*p).into()).collect(),
        serves: serves.iter().map(|p| (*p).into()).collect(),
        state: "active".into(),
        ..Default::default()
    }
}

/// Routing with a catalog provider, its own key, and a key pool: the pool key
/// names the pool provider and `serves` the catalog provider whose rows the
/// pool borrows.
fn harness() -> Arc<Harness> {
    harness_with_routed_accounts(None)
}

fn harness_with_routed_accounts(methods: Option<&[&str]>) -> Arc<Harness> {
    harness_with_pool_keys(methods, &[])
}

fn harness_with_pool_keys(
    methods: Option<&[&str]>,
    additional_keys: &[ListedCredential],
) -> Arc<Harness> {
    let harness = Harness::new("key-pool");
    let facts = routing_facts();
    harness.install_facts(&facts);
    let updates: Vec<_> = facts
        .iter()
        .map(|fact| FusiformFactUpdate {
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
            upstream_status: None, model_type: None, model_kind: None,
            lineage: fact.family.clone(),
        })
        .collect();
    harness
        .store()
        .apply_fusiform_catalog(
            &updates,
            FusiformCatalogCursor {
                catalog_version: 1,
                resolved_at_ms: 1,
            },
        )
        .unwrap();
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "key-pool-listing".into(),
            credentials: [
                vec![
                    key("apikey:catalog:main", &[BORROWED], &[BORROWED]),
                    key("apikey:pool-vendor:main", &[POOL], &[POOL_VENDOR]),
                ],
                additional_keys.to_vec(),
            ]
            .concat(),
            ..Default::default()
        },
        crate::now_ms(),
    );
    credentials.refresh_serving_methods(&Default::default(), &facts, crate::now_ms() as i64, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts),
    );
    credentials.publish_plan_prices(&json!({ "prices": [] }));
    if let Some(methods) = methods {
        credentials.runner_declarations.insert(
            "zeta-runner".into(),
            super::runners::parse_declaration(&json!({"runner": {
                "pairs": [{"wire_api":"anthropic.messages", "auth_method":"apikey"}],
                "follows_account_choice": true
            }}))
            .unwrap()
            .unwrap(),
        );
        credentials.runner_routed_accounts.insert(
            "zeta-runner".into(),
            methods.iter().map(|method| (*method).into()).collect(),
        );
        credentials.wire_apis = facts
            .iter()
            .map(|fact| {
                (
                    format!("{}/{}", fact.provider_id, fact.model_id),
                    "anthropic.messages".into(),
                )
            })
            .collect();
        credentials.publish_billing_catalog(
            crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts)
                .with_wire_apis(&credentials.wire_apis),
        );
    }
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"] = json!([{ "module": "zeta-runner" }]);
        config["providers"][POOL]["pin_only"] = json!(false);
    });
    harness
}

fn select(consumer: &str) -> Value {
    json!({ "consumer": consumer, "requirements": { "iq": 30 } })
}

fn legacy_select() -> Value {
    json!({
        "targetAgent": "mason", "consumer": "mason", "executor": "broca",
        "substrate": "broca", "requirements": {"iq": 30, "eq": 0},
    })
}

fn answered_route(reply: &(FrameType, Value)) -> String {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let route = &reply.1["result"]["route"];
    format!(
        "{}/{}",
        route["provider"].as_str().unwrap(),
        route["model"].as_str().unwrap()
    )
}

fn legacy_route(reply: &(FrameType, Value)) -> String {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    reply.1["result"]["selected"]["routeKey"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn assert_pool_answer(reply: &(FrameType, Value), request: &Value) {
    assert_eq!(answered_route(reply), format!("{POOL}/{POOL_MODEL_SHORT}"));
    let answer: SelectAnswer = serde_json::from_value(reply.1["result"].clone()).unwrap();
    check_answer(&parse_select_request(request).unwrap(), &answer).unwrap();
    assert_eq!(answer.runner.model.provider, POOL_VENDOR);
    assert_eq!(answer.runner.model.model, POOL_MODEL_SHORT);
    assert_eq!(
        reply.1["result"]["runner"]["auth"],
        json!({
            "pool": POOL, "credential_id": "apikey:pool-vendor:main"
        })
    );
}

/// Records runner-owned pool auth and admission, unlisted/missing-group fallback,
/// pinned pools (including a key blocked by an entitlement refusal alongside a
/// healthy key), and legacy launch identities, plus one vendor model on both
/// subscription and pool routes.
/// Pool providers borrow vendor rows; they are not ordinary catalog providers
/// that happen to have API keys.
#[tokio::test]
async fn replay_key_pool_public_cases() {
    use striatum_core::route_state_store::CredentialRefusal;

    const NOW: i64 = 1_800_000_000_000;
    let mut cases = Vec::new();
    for (name, methods, pinned, fenced) in [
        (
            "runner-owned-pool-auth",
            Some(&["apikey"][..]),
            false,
            false,
        ),
        (
            "unlisted-method-fallback",
            Some(&["oauth"][..]),
            false,
            false,
        ),
        ("missing-group-fallback", None, false, false),
        ("pinned-pool-route", Some(&["apikey"][..]), true, false),
        (
            "pinned-pool-fenced-sibling",
            Some(&["oauth"][..]),
            true,
            true,
        ),
    ] {
        let _clock = crate::InjectedClock::set(NOW as u64);
        let _inputs = striatum_store::replay::Inputs::new(NOW).install();
        let sibling = key("apikey:pool-vendor:second", &[POOL], &[POOL_VENDOR]);
        let additional = if fenced { vec![sibling] } else { Vec::new() };
        let harness = harness_with_pool_keys(methods, &additional);
        harness.edit_config(clear_pool_exclusions);
        if fenced {
            let mut credentials = BrocaCredentials::default();
            credentials.apply_listing(
                &ScopedCredentialListing {
                    view: "key-pool-listing".into(),
                    credentials: vec![key("apikey:pool-vendor:main", &[POOL], &[POOL_VENDOR])],
                    ..Default::default()
                },
                NOW as u64,
            );
            crate::credential_fences::record(
                harness.store(),
                &Arc::new(std::sync::Mutex::new(credentials)),
                &CredentialRefusal {
                    provider_id: POOL.into(),
                    credential_id: "apikey:pool-vendor:main".into(),
                    code: "ENTITLEMENT_ERROR".into(),
                },
                NOW,
            )
            .unwrap();
        }
        let owner = harness.bind(reserved("example-consumer"), None).await;
        let runner = harness.bind(reserved("zeta-runner"), None).await;
        let pool_route = format!("{POOL}/{POOL_MODEL_SHORT}");
        let mut request = select("mason");
        if pinned {
            request["pin"] = json!(pool_route);
        }
        let selected = owner.call("route.select", &request).await;
        assert_eq!(answered_route(&selected), pool_route, "{name}");
        let answer: SelectAnswer = serde_json::from_value(selected.1["result"].clone()).unwrap();
        check_answer(&parse_select_request(&request).unwrap(), &answer).unwrap();
        assert_eq!(answer.runner.model.provider, POOL_VENDOR);
        assert_eq!(answer.runner.model.model, POOL_MODEL_SHORT);
        let runner_owned = methods.is_some_and(|methods| methods.contains(&"apikey"));
        let credential = if fenced {
            "apikey:pool-vendor:second"
        } else {
            "apikey:pool-vendor:main"
        };
        let auth = if runner_owned {
            json!({"pool":POOL})
        } else {
            json!({"pool":POOL,"credential_id":credential})
        };
        assert_eq!(selected.1["result"]["runner"]["auth"], auth, "{name}");
        if pinned {
            assert_eq!(selected.1["result"]["pin_outcome"], "pinned", "{name}");
        }
        let mut launches = vec![json!({
            "kind":"routing-v1-select", "method":"route.select", "params":request,
            "frame_type":format!("{:?}", selected.0), "reply":selected.1,
        })];
        if runner_owned {
            let admit = json!({
                "mode":"admit", "provider":answer.route.provider, "model":answer.route.model,
                "pool":answer.runner.auth.as_ref().unwrap().pool,
                "requested_account":{"credential_id":credential},
                "conversation_key":"pooled-conversation", "admission_id":"pooled-admit",
                "decision_id":answer.decision_id,
            });
            let admitted = runner.call("route.account_for", &admit).await;
            assert_eq!(admitted.0, FrameType::Response, "{name}: {admitted:?}");
            assert_eq!(admitted.1["result"]["account"]["provider"], POOL);
            assert_eq!(admitted.1["result"]["account"]["credential_id"], credential);
            launches.push(json!({
                "kind":"routing-v1-account-for", "method":"route.account_for", "params":admit,
                "frame_type":format!("{:?}", admitted.0), "reply":admitted.1,
            }));
        }
        let mut legacy = legacy_select();
        if pinned {
            legacy["modelPin"] = json!(pool_route);
        }
        let selected = owner.call("route.select", &legacy).await;
        assert_eq!(legacy_route(&selected), pool_route, "{name}");
        launches.push(json!({
            "kind":"legacy-select", "method":"route.select", "params":legacy,
            "frame_type":format!("{:?}", selected.0), "reply":selected.1,
        }));
        cases.push(super::replay_tests::normalize_paths(
            json!({"name":name,"launches":launches}),
            &harness,
        ));
    }
    {
        let _clock = crate::InjectedClock::set(NOW as u64);
        let _inputs = striatum_store::replay::Inputs::new(NOW).install();
        let (harness, mut credentials) = super::account_classes_tests::setup(POOL_VENDOR, false);
        let mut rows = credentials.listed_credentials().to_vec();
        rows.push(key("apikey:pool-vendor:main", &[POOL], &[POOL_VENDOR]));
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "subscription-and-pool".into(),
                credentials: rows,
                ..Default::default()
            },
            NOW as u64,
        );
        let mut fact = crate::tests::fact(POOL_VENDOR, "model");
        fact.auth_methods = Some(vec!["oauth".into(), "apikey".into()]);
        credentials.refresh_serving_methods(&Default::default(), &[fact], NOW, &[]);
        harness.set_credentials(credentials);
        let owner = harness.bind(reserved("class-owner"), None).await;
        let runner = harness.bind(reserved("class-runner"), None).await;
        let mut launches = Vec::new();
        for (kind, provider) in [("subscription", POOL_VENDOR), ("pool", POOL)] {
            let pin = format!("{provider}/model");
            let mut request = select("mason");
            request["pin"] = json!(pin);
            let selected = owner.call("route.select", &request).await;
            assert_eq!(answered_route(&selected), pin);
            let answer: SelectAnswer =
                serde_json::from_value(selected.1["result"].clone()).unwrap();
            check_answer(&parse_select_request(&request).unwrap(), &answer).unwrap();
            assert_eq!(answer.runner.model.provider, POOL_VENDOR);
            assert_eq!(answer.runner.model.model, "model");
            if provider == POOL {
                assert_eq!(
                    selected.1["result"]["runner"]["auth"],
                    json!({"pool":POOL,"credential_id":"apikey:pool-vendor:main"})
                );
            } else {
                assert!(answer.runner.auth.is_none());
            }
            launches.push(json!({
                "kind":format!("{kind}-routing-v1-select"), "method":"route.select", "params":request,
                "frame_type":format!("{:?}", selected.0), "reply":selected.1,
            }));
            if provider == POOL_VENDOR {
                let admit = json!({
                    "mode":"admit", "provider":provider, "model":"model",
                    "conversation_key":"subscription-conversation", "admission_id":"subscription-admit",
                    "decision_id":answer.decision_id,
                });
                let admitted = runner.call("route.account_for", &admit).await;
                assert_eq!(admitted.0, FrameType::Response, "{admitted:?}");
                assert_eq!(admitted.1["result"]["account"]["method"], "oauth");
                assert_eq!(
                    admitted.1["result"]["account"]["credential_id"],
                    "oauth:test"
                );
                launches.push(json!({
                    "kind":"subscription-routing-v1-account-for", "method":"route.account_for", "params":admit,
                    "frame_type":format!("{:?}", admitted.0), "reply":admitted.1,
                }));
            }
            let mut legacy = legacy_select();
            legacy["modelPin"] = json!(pin);
            let selected = owner.call("route.select", &legacy).await;
            assert_eq!(legacy_route(&selected), pin);
            launches.push(json!({
                "kind":format!("{kind}-legacy-select"), "method":"route.select", "params":legacy,
                "frame_type":format!("{:?}", selected.0), "reply":selected.1,
            }));
        }
        cases.push(super::replay_tests::normalize_paths(
            json!({"name":"same-vendor-model-subscription-and-pool", "launches":launches}),
            &harness,
        ));
    }
    super::replay_tests::assert_or_record(
        "key-pool-public",
        json!({"clock_ms":NOW,
            "body_source":"public routing/v1 select and runner account_for replies plus legacy select replies through the real frame handler; synthetic alias-provider key pools",
            "cases":cases}),
    );
}

/// With no pin and no ladder, the key pool's route is the cheapest adequate
/// one. Operator policy that keeps such a provider out of the automatic order
/// is cleared first, so the only thing left that can keep it out of a v1 answer
/// is the pool's standing policy. Legacy and routing/v1 select answer the same route.
#[tokio::test]
async fn the_automatic_order_answers_the_same_cheaper_pool_route_on_both_surfaces() {
    let harness = harness();
    // Clear every exclusion naming the key-pool provider, so its borrowed
    // route is a normal automatic candidate.
    harness.edit_config(clear_pool_exclusions);
    let route = harness.bind(reserved("example-consumer"), None).await;

    let v1 = route.call("route.select", &select("mason")).await;
    assert_pool_answer(&v1, &select("mason"));

    let legacy = route.call("route.select", &legacy_select()).await;
    assert_eq!(
        legacy_route(&legacy),
        format!("{POOL}/{POOL_MODEL_SHORT}"),
        "the legacy shape still answers the cheaper key-pool route: {}",
        legacy.1
    );
}

/// Remove every operator exclusion entry whose spec names the key-pool
/// provider or one of its routes, at any depth of the configuration.
fn clear_pool_exclusions(value: &mut Value) {
    match value {
        Value::Array(entries) => {
            entries.retain(|entry| {
                entry.as_str().is_none_or(|spec| {
                    let spec = spec.trim_start_matches('-').trim();
                    let model = spec.split_once('/').map_or(spec, |(_, model)| model);
                    !(spec.starts_with(POOL)
                        || spec.starts_with(POOL_VENDOR)
                        || spec.starts_with(&format!("-{POOL}"))
                        || (model == POOL_MODEL_SHORT && spec.contains(POOL)))
                })
            });
            for entry in entries.iter_mut() {
                clear_pool_exclusions(entry);
            }
        }
        Value::Object(map) => {
            for entry in map.values_mut() {
                clear_pool_exclusions(entry);
            }
        }
        _ => {}
    }
}

/// The first adequate ladder entry wins, including a key-pool route.
#[tokio::test]
async fn a_ladder_answers_its_first_key_pool_entry() {
    let harness = harness();
    harness.edit_config(|config| {
        config["consumers"]["mason"]["mode"] = json!("ladder");
        config["consumers"]["mason"]["ladder"] =
            json!([format!("{POOL}/{POOL_MODEL_SHORT}"), PLAIN]);
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    let v1 = route.call("route.select", &select("mason")).await;
    assert_pool_answer(&v1, &select("mason"));
}

#[tokio::test]
async fn a_ladder_of_only_key_pool_entries_answers_on_v1() {
    let harness = harness();
    harness.edit_config(|config| {
        config["consumers"]["mason"]["mode"] = json!("ladder");
        config["consumers"]["mason"]["ladder"] = json!([format!("{POOL}/{POOL_MODEL_SHORT}")]);
    });
    let route = harness.bind(reserved("example-consumer"), None).await;
    let v1 = route.call("route.select", &select("mason")).await;
    assert_pool_answer(&v1, &select("mason"));
}

/// A pool route can be the only adequate automatic candidate.
#[tokio::test]
async fn only_a_key_pool_route_adequate_answers_on_v1() {
    let harness = harness();
    harness.edit_config(|config| {
        clear_pool_exclusions(config);
        // The catalog provider's own route is not for mason any more, so the
        // key pool's route is the only one left that can serve the request.
        config["models"][PLAIN]["not_for"] = json!(["mason"]);
    });
    let route = harness.bind(reserved("example-consumer"), None).await;

    let v1 = route.call("route.select", &select("mason")).await;
    assert_pool_answer(&v1, &select("mason"));

    // The same demand on the legacy shape still gets the key-pool route.
    let legacy = route
        .call(
            "route.select",
            &json!({
                "targetAgent": "mason", "consumer": "mason", "executor": "broca",
                "substrate": "broca", "requirements": {"iq": 100, "eq": 0},
            }),
        )
        .await;
    assert_eq!(
        legacy_route(&legacy),
        format!("{POOL}/{POOL_MODEL_SHORT}"),
        "{}",
        legacy.1
    );
}

#[tokio::test]
async fn the_legacy_surface_answers_the_key_pool_route() {
    let harness = harness();
    let route = harness.bind(reserved("example-consumer"), None).await;
    // A pin names the route outright, so the legacy surface must serve it: it
    // selects and leases a pool key for exactly this case.
    let mut legacy = legacy_select();
    legacy["modelPin"] = json!(format!("{POOL}/model-pool"));
    let legacy = route.call("route.select", &legacy).await;
    assert_eq!(
        legacy_route(&legacy),
        format!("{POOL}/model-pool"),
        "the legacy shape leases a pool key for a pinned key-pool route: {}",
        legacy.1
    );
}

#[tokio::test]
async fn a_v1_pin_on_a_key_pool_route_is_served() {
    let harness = harness();
    let route = harness.bind(reserved("example-consumer"), None).await;
    let mut pinned = select("mason");
    pinned["pin"] = json!(format!("{POOL}/model-pool"));
    let reply = route.call("route.select", &pinned).await;
    assert_pool_answer(&reply, &pinned);
    assert_eq!(reply.1["result"]["pin_outcome"], "pinned");
}

#[tokio::test]
async fn pooled_pins_skip_a_fenced_key_but_refuse_when_every_key_is_fenced() {
    use striatum_core::route_state_store::CredentialRefusal;
    const NOW: u64 = 1_800_000_000_000;
    let _clock = crate::InjectedClock::set(NOW);
    for pin_kind in ["explicit", "slot", "project"] {
        let sibling = key("apikey:pool-vendor:second", &[POOL], &[POOL_VENDOR]);
        let harness = harness_with_pool_keys(None, &[sibling.clone()]);
        let mut credentials = BrocaCredentials::default();
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "key-pool-listing".into(),
                credentials: vec![
                    key("apikey:catalog:main", &[BORROWED], &[BORROWED]),
                    key("apikey:pool-vendor:main", &[POOL], &[POOL_VENDOR]),
                    sibling,
                ],
                ..Default::default()
            },
            NOW,
        );
        let credentials = Arc::new(std::sync::Mutex::new(credentials));
        crate::credential_fences::record(
            harness.store(),
            &credentials,
            &CredentialRefusal {
                provider_id: POOL.into(),
                credential_id: "apikey:pool-vendor:main".into(),
                code: "ENTITLEMENT_ERROR".into(),
            },
            NOW as i64,
        )
        .unwrap();
        assert_eq!(
            crate::credential_fences::live(harness.store(), &credentials)
                .unwrap()
                .len(),
            1
        );
        let mut request = select("mason");
        let pin = format!("{POOL}/{POOL_MODEL_SHORT}");
        match pin_kind {
            "explicit" => request["pin"] = json!(pin),
            "slot" => {
                request["pin_slot"] = json!("synthesizer");
                harness.edit_config(|config| {
                    config["consumers"]["synthesizer"]["pin"] = json!({"model":pin})
                });
            }
            "project" => harness.edit_config(|config| {
                config["consumers"]["mason"]["projects"]["pool-project"] = json!({"pin":pin})
            }),
            _ => unreachable!(),
        }
        let route = harness
            .bind(reserved("example-consumer"), Some("pool-project"))
            .await;
        let mut legacy = legacy_select();
        legacy["modelPin"] = json!(pin);
        let legacy = route.call("route.select", &legacy).await;
        assert_eq!(legacy_route(&legacy), pin, "{pin_kind}");
        let reply = route.call("route.select", &request).await;
        assert_eq!(answered_route(&reply), pin, "{pin_kind}");
        assert_eq!(
            reply.1["result"]["runner"]["auth"]["credential_id"], "apikey:pool-vendor:second",
            "{pin_kind}: {reply:?}"
        );
        let answer: SelectAnswer = serde_json::from_value(reply.1["result"].clone()).unwrap();
        let request =
            cortexkit_role_routing_conformance::wire::select::parse_select_request_serving(
                &request,
                super::describe::CAPABILITIES,
            )
            .unwrap();
        check_answer(&request, &answer).unwrap();
        crate::credential_fences::record(
            harness.store(),
            &credentials,
            &CredentialRefusal {
                provider_id: POOL.into(),
                credential_id: "apikey:pool-vendor:second".into(),
                code: "ENTITLEMENT_ERROR".into(),
            },
            NOW as i64,
        )
        .unwrap();
        let mut legacy = legacy_select();
        legacy["modelPin"] = json!(pin);
        let legacy = route.call("route.select", &legacy).await;
        assert_eq!(legacy.0, FrameType::Error, "all keys fenced: {legacy:?}");
        assert_eq!(legacy.1["code"], "explicit_provider_pool_exhausted");
        let mut request = select("mason");
        request["pin"] = json!(pin);
        let refused = route.call("route.select", &request).await;
        assert_eq!(refused.0, FrameType::Error, "all keys fenced: {refused:?}");
        assert_eq!(refused.1["code"], "pin_unavailable");
    }
}

#[tokio::test]
async fn a_non_pool_provider_never_gets_runner_auth() {
    let harness = harness();
    let route = harness.bind(reserved("example-consumer"), None).await;
    let mut request = select("mason");
    request["pin"] = json!(PLAIN);
    let reply = route.call("route.select", &request).await;
    assert_eq!(answered_route(&reply), PLAIN);
    let answer: SelectAnswer = serde_json::from_value(reply.1["result"].clone()).unwrap();
    check_answer(&parse_select_request(&request).unwrap(), &answer).unwrap();
    assert!(answer.runner.auth.is_none());
    assert!(reply.1["result"]["runner"].get("auth").is_none());
}

#[tokio::test]
async fn an_unlisted_sign_in_method_still_requires_the_select_credential() {
    let harness = harness_with_routed_accounts(Some(&["oauth"]));
    let route = harness.bind(reserved("example-consumer"), None).await;
    let mut request = select("mason");
    request["pin"] = json!(format!("{POOL}/{POOL_MODEL_SHORT}"));
    let reply = route.call("route.select", &request).await;
    assert_pool_answer(&reply, &request);
}

#[tokio::test]
async fn a_routed_apikey_runner_admits_renews_and_releases_the_pool_credential() {
    use cortexkit_role_routing_conformance::wire::{
        account::AccountForAnswer, observe::AccountObserveAnswer, wire::read_reply,
    };
    let harness = harness_with_routed_accounts(Some(&["apikey"]));
    let owner = harness.bind(reserved("example-consumer"), None).await;
    let runner = harness.bind(reserved("zeta-runner"), None).await;
    let mut request = select("mason");
    request["pin"] = json!(format!("{POOL}/{POOL_MODEL_SHORT}"));
    let selected = owner.call("route.select", &request).await;
    assert_eq!(
        answered_route(&selected),
        format!("{POOL}/{POOL_MODEL_SHORT}")
    );
    let answer: SelectAnswer = serde_json::from_value(selected.1["result"].clone()).unwrap();
    check_answer(&parse_select_request(&request).unwrap(), &answer).unwrap();
    assert_eq!(answer.runner.model.provider, POOL_VENDOR);
    assert_eq!(selected.1["result"]["runner"]["auth"], json!({"pool":POOL}));

    // Under §§5.2 and 9.1, account_for names the decision's routing route;
    // runner.model names only the vendor send and never owns its accounts.
    let mut admit = json!({
        "mode":"admit", "provider":answer.route.provider, "model":answer.route.model,
        "pool":answer.runner.auth.as_ref().unwrap().pool,
        "requested_account":{"credential_id":"apikey:pool-vendor:main"},
        "conversation_key":"pooled-conversation", "admission_id":"pooled-admit",
        "decision_id":answer.decision_id,
    });
    for (field, value) in [("provider", POOL_VENDOR), ("model", "vendor-only-model")] {
        let mut wrong_route = admit.clone();
        wrong_route[field] = json!(value);
        let refused = runner.call("route.account_for", &wrong_route).await;
        assert_eq!(refused.0, FrameType::Error, "{refused:?}");
        assert_eq!(refused.1["code"], "unknown_decision", "{refused:?}");
    }
    let first = runner.call("route.account_for", &admit).await;
    assert_eq!(first.0, FrameType::Response, "{first:?}");
    let typed: AccountForAnswer = read_reply(&serde_json::to_vec(&first.1).unwrap()).unwrap();
    assert_eq!(first.1["result"]["account"]["provider"], POOL);
    assert_eq!(
        first.1["result"]["account"]["account_id"],
        "apikey:pool-vendor:main"
    );
    assert_eq!(first.1["result"]["account"]["method"], "apikey");
    assert_eq!(
        first.1["result"]["account"]["credential_id"],
        "apikey:pool-vendor:main"
    );
    assert_eq!(runner.call("route.account_for", &admit).await, first);
    assert!(!typed.pin.id.is_empty());
    admit["mode"] = json!("renew");
    admit.as_object_mut().unwrap().remove("requested_account");
    admit["admission_id"] = json!("pooled-renew");
    admit["expected_pin"] = json!({"id":typed.pin.id, "generation":typed.pin.generation});
    let renewed = runner.call("route.account_for", &admit).await;
    assert_eq!(renewed.0, FrameType::Response, "{renewed:?}");
    let _: AccountForAnswer = read_reply(&serde_json::to_vec(&renewed.1).unwrap()).unwrap();
    for member in ["id", "generation"] {
        assert_eq!(
            renewed.1["result"]["pin"][member],
            first.1["result"]["pin"][member]
        );
    }
    assert_eq!(renewed.1["result"]["account"], first.1["result"]["account"]);
    let released = runner
        .call(
            "route.account_observe",
            &json!({
                "event_id":"pooled-finished", "kind":"run_finished", "admission_id":"pooled-renew"
            }),
        )
        .await;
    assert_eq!(released.0, FrameType::Response, "{released:?}");
    let _: AccountObserveAnswer = read_reply(&serde_json::to_vec(&released.1).unwrap()).unwrap();
    assert_eq!(released.1["result"]["observation"]["bound"], true);
    harness
        .store()
        .account_transaction(|tx| {
            let row = tx
                .admission("reserved:zeta-runner", "pooled-renew")?
                .unwrap();
            assert_eq!(row.state, "finished");
            let affinity = tx
                .affinity(&row.principal, &row.conversation_hash, POOL)?
                .unwrap();
            assert!(affinity
                .lease_valid_until
                .is_none_or(|until| until <= crate::now_ms() as i64));
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn a_pooled_apikey_backoff_blocks_new_admissions_without_releasing_the_pin() {
    const NOW: u64 = 1_800_000_000_000;
    let _clock = crate::InjectedClock::set(NOW);
    let harness = harness_with_routed_accounts(Some(&["apikey"]));
    let runner = harness.bind(reserved("zeta-runner"), None).await;
    let admit = json!({
        "mode":"admit", "provider":POOL, "model":POOL_MODEL_SHORT, "pool":POOL,
        "conversation_key":"first", "admission_id":"first",
    });
    let first = runner.call("route.account_for", &admit).await;
    assert_eq!(first.0, FrameType::Response, "{first:?}");
    let backoff = runner
        .call(
            "route.account_observe",
            &json!({
                "event_id":"pool-backoff", "kind":"transient_backoff", "admission_id":"first",
                "retry_at_ms":NOW + 3_600_000,
            }),
        )
        .await;
    assert_eq!(backoff.0, FrameType::Response, "{backoff:?}");
    assert_eq!(backoff.1["result"]["observation"]["bound"], true);
    let mut next = admit;
    next["admission_id"] = json!("next");
    next["conversation_key"] = json!("next");
    let refused = runner.call("route.account_for", &next).await;
    assert_eq!(refused.0, FrameType::Error, "{refused:?}");
    assert_eq!(refused.1["code"], "account_pool_exhausted", "{refused:?}");
    assert_eq!(refused.1["detail"]["reset_at_ms"], NOW + 3_600_000);
    harness
        .store()
        .account_transaction(|tx| {
            let row = tx.admission("reserved:zeta-runner", "first")?.unwrap();
            let affinity = tx
                .affinity(&row.principal, &row.conversation_hash, POOL)?
                .unwrap();
            assert_eq!(
                affinity.pin_id,
                first.1["result"]["pin"]["id"].as_str().unwrap()
            );
            assert!(affinity.lease_valid_until.unwrap() > NOW as i64);
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn pooled_keys_without_account_ids_support_optional_runner_candidates() {
    use cortexkit_role_routing_conformance::wire::{account::AccountForAnswer, wire::read_reply};
    let harness = harness_with_pool_keys(
        Some(&["apikey"]),
        &[key("apikey:pool-vendor:second", &[POOL], &[POOL_VENDOR])],
    );
    let owner = harness.bind(reserved("example-consumer"), None).await;
    let runner = harness.bind(reserved("zeta-runner"), None).await;
    let mut selection = select("mason");
    selection["pin"] = json!(format!("{POOL}/{POOL_MODEL_SHORT}"));
    let selected = owner.call("route.select", &selection).await;
    assert_eq!(selected.0, FrameType::Response, "{selected:?}");
    let mut request = json!({
        "mode":"admit", "provider":POOL, "model":POOL_MODEL_SHORT, "pool":POOL,
        "conversation_key":"no-candidates", "admission_id":"no-candidates",
        "decision_id":selected.1["result"]["decision_id"],
    });
    let first = runner.call("route.account_for", &request).await;
    assert_eq!(first.0, FrameType::Response, "{first:?}");
    let _: AccountForAnswer = read_reply(&serde_json::to_vec(&first.1).unwrap()).unwrap();
    let credential = first.1["result"]["account"]["credential_id"]
        .as_str()
        .unwrap();
    assert!(["apikey:pool-vendor:main", "apikey:pool-vendor:second"].contains(&credential));
    assert_eq!(first.1["result"]["account"]["account_id"], credential);

    let offered = if credential == "apikey:pool-vendor:main" {
        "apikey:pool-vendor:second"
    } else {
        "apikey:pool-vendor:main"
    };
    request["admission_id"] = json!("offered-key");
    request["conversation_key"] = json!("offered-key");
    request["candidates"] = json!({
        "view_digest":"runner-readable-keys",
        "accounts":[{"account_id":offered, "credential_id":offered}]
    });
    let narrowed = runner.call("route.account_for", &request).await;
    assert_eq!(narrowed.0, FrameType::Response, "{narrowed:?}");
    let _: AccountForAnswer = read_reply(&serde_json::to_vec(&narrowed.1).unwrap()).unwrap();
    assert_eq!(narrowed.1["result"]["account"]["account_id"], offered);
    assert_eq!(narrowed.1["result"]["account"]["credential_id"], offered);

    // A runner's candidates can narrow the pool's available credentials, but
    // cannot add a credential absent from the pool.
    request["admission_id"] = json!("unlisted-key");
    request["conversation_key"] = json!("unlisted-key");
    request["candidates"]["accounts"] = json!([{
        "account_id":"apikey:pool-vendor:absent", "credential_id":"apikey:pool-vendor:absent"
    }]);
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{refused:?}");
}

#[tokio::test]
async fn a_slow_billing_plane_answers_routing_not_ready_within_the_v1_bound() {
    let harness = harness();
    harness.set_credentials(BrocaCredentials::default());
    let route = harness.bind(reserved("example-consumer"), None).await;
    let started = Instant::now();
    let reply = route.call("route.select", &select("mason")).await;
    let waited = started.elapsed();
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "routing_not_ready", "{}", reply.1);
    assert!(
        waited < Duration::from_secs(2),
        "the typed refusal must beat a caller's 3 s timeout, waited {waited:?}"
    );
}
