//! Payment class is fixed by selection; account exhaustion never grants spending.

use crate::account_selection::admission::after_roster_prepared;
use crate::account_selection::block_recovery::repair_route_fences;
use crate::account_selection::class_context::{
    before_launch_command, CONTEXT_LOADS, LEGACY_CONTEXT_REPAIRS,
};
use std::sync::Arc;

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use serde_json::{json, Value};
use striatum_core::accounts::{ConfirmationSource, Fence, FenceScope, GeneralWindow, ResetTime};
use striatum_core::billing::{BillingPlane, BillingRate};
use striatum_core::route_state_store::CredentialRefusal;
use striatum_core::RouteStateStore;
use striatum_store::AccountFenceRow;
use subc_protocol::FrameType;

use super::conformance_tests::{reserved, Harness};
use crate::broca_credentials::BrocaCredentials;

const NOW: u64 = 1_800_000_000_000;
const RUNNER: &str = "class-runner";
const OWNER: &str = "class-owner";

mod fork_class_tests;
mod listing_repair_race_tests;
mod paid_anthropic_shape_tests;
mod pinned_keep_free_tests;
mod transient_backoff_select_tests;

fn assert_configured_runner_caller_without_writer(harness: &Harness, frame: &subc_protocol::Frame) {
    let path = harness.store_path().with_extension("caller.jsonc");
    let config = crate::config_convert::convert_value(json!({
        "runners": [{"module": RUNNER, "via_vault": true}]
    }))
    .unwrap();
    crate::test_config::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let credentials = harness.credentials();
    let quota_cache = Default::default();
    let caller = super::Caller::for_route(frame.header.channel, frame.header.epoch);
    let acquisitions = harness.store().writer_acquisition_count_for_test();
    assert_eq!(
        super::accounts::admission_runner(
            &caller,
            &super::Deps {
                quota_cache: &quota_cache,
                store: harness.store(),
                routing_config_path: &path,
                operator_config_path: &path,
                broca_credentials: &credentials,
            }
        )
        .as_deref(),
        Some(RUNNER)
    );
    assert_eq!(
        harness.store().writer_acquisition_count_for_test(),
        acquisitions
    );
}

#[tokio::test]
async fn configured_runner_store_failure_keeps_account_operation_error_codes() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let request = account_request("synthetic", "store-unavailable");
    let malformed = runner.call("route.account_observe", &json!({})).await;
    assert_eq!(malformed.1["code"], "invalid_request", "{}", malformed.1);
    rusqlite::Connection::open(harness.store_path())
        .unwrap()
        .execute_batch("DROP TABLE model_facts")
        .unwrap();
    assert_configured_runner_caller_without_writer(
        &harness,
        &runner.request_frame("route.account_for", &request),
    );
    let unavailable = runner.call("route.account_for", &request).await;
    assert_eq!(unavailable.0, FrameType::Error);
    assert_eq!(
        unavailable.1["code"], "account_selection_unavailable",
        "{}",
        unavailable.1
    );
    let malformed_after = runner.call("route.account_observe", &json!({})).await;
    assert_eq!(malformed_after, malformed);
    let observe = runner
        .call(
            "route.account_observe",
            &json!({
                "event_id":"store-unavailable", "kind":"run_finished",
                "admission_id":"missing", "run_id":"run"
            }),
        )
        .await;
    assert_eq!(observe.0, FrameType::Error);
    assert_eq!(observe.1["code"], "store_error", "{}", observe.1);
}

#[tokio::test]
async fn configured_runner_poisoned_credentials_reaches_its_account_operation() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let credentials = harness.credentials();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = credentials.lock().unwrap();
        panic!("poison the credentials snapshot");
    }))
    .is_err());
    let request = account_request("synthetic", "credentials-unavailable");
    assert_configured_runner_caller_without_writer(
        &harness,
        &runner.request_frame("route.account_for", &request),
    );
    let unavailable = runner.call("route.account_for", &request).await;
    assert_eq!(unavailable.0, FrameType::Error);
    assert_eq!(
        unavailable.1["code"], "account_selection_unavailable",
        "{}",
        unavailable.1
    );
    let observe = runner
        .call(
            "route.account_observe",
            &json!({
                "event_id":"credentials-unavailable", "kind":"run_finished",
                "admission_id":"missing", "run_id":"run"
            }),
        )
        .await;
    assert_eq!(observe.0, FrameType::Error);
    assert_eq!(observe.1["code"], "store_error", "{}", observe.1);
}

#[tokio::test]
async fn a_legacy_transient_wall_mislabeled_quota_never_refuses_a_model_pin() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    harness
        .store()
        .update_route_state(
            "synthetic/model/xhigh",
            "synthetic/model",
            &striatum_core::RouteStateUpdate {
                cooldown_until: Some((NOW + 1_800_000) as f64),
                observed_at_ms: Some(NOW as i64),
                wall_class: Some(striatum_core::RouteWallClass::Quota),
                ..Default::default()
            },
        )
        .unwrap();
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    conn.execute(
        "UPDATE model_route_state SET last_error=?1 WHERE route_key='synthetic/model/xhigh'",
        [
            json!({"class":"transient","status":503,"provider_code":"server_is_overloaded"})
                .to_string(),
        ],
    )
    .unwrap();
    let caller = harness.bind(reserved(OWNER), None).await;
    let request =
        json!({"consumer":"mason", "requirements":{"iq":30}, "pin":"synthetic/model:xhigh"});
    let admitted = caller.call("route.select", &request).await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    harness
        .store()
        .update_route_state(
            "synthetic/model",
            "synthetic/model",
            &striatum_core::RouteStateUpdate {
                cooldown_until: Some((NOW + 1_800_000) as f64),
                quota_exhausted_until: Some((NOW + 1_800_000) as f64),
                observed_at_ms: Some(NOW as i64),
                wall_class: Some(striatum_core::RouteWallClass::Quota),
                last_error: Some("quota exhausted".into()),
                ..Default::default()
            },
        )
        .unwrap();
    let refused = caller.call("route.select", &request).await;
    assert_eq!(
        refused.0,
        FrameType::Error,
        "a real quota wall still refuses the pin: {}",
        refused.1
    );
    assert_eq!(refused.1["code"], "pin_unavailable", "{}", refused.1);
    assert_eq!(refused.1["detail"]["reset_at_ms"], NOW + 1_800_000);
}

#[tokio::test]
async fn transient_overload_rotates_a_fresh_model_pin_but_never_walls_the_account_pool() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "three-subscription-accounts".into(),
            credentials: ["first", "second", "third"]
                .into_iter()
                .map(|id| ListedCredential {
                    id: format!("oauth:{id}"),
                    account_id: Some(id.into()),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec!["synthetic".into()],
                    serves: vec!["synthetic".into()],
                    credential_type: "oauth".into(),
                    auth_method: Some("oauth".into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        NOW,
    );
    let mut fact = crate::tests::fact("synthetic", "model");
    fact.auth_methods = Some(vec!["oauth".into()]);
    credentials.refresh_serving_methods(&Default::default(), &[fact], NOW as i64, &[]);
    harness.set_credentials(credentials);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut started = account_request("synthetic", "already-running");
    started["candidates"] = json!({"view_digest":"test","accounts":[{"account_id":"first","credential_id":"oauth:first"}]});
    assert_credential(
        &runner.call("route.account_for", &started).await,
        "oauth:first",
    );
    let observed = runner
        .call(
            "route.account_observe",
            &json!({
                "event_id":"already-running-started", "kind":"admission_started",
                "admission_id":"already-running", "run_id":"running-before-overload"
            }),
        )
        .await;
    assert_eq!(observed.0, FrameType::Response, "{}", observed.1);
    let mut fresh = account_request("synthetic", "fresh-prefers-walled");
    fresh["candidates"] = json!({"view_digest":"test","accounts":[{"account_id":"first","credential_id":"oauth:first"}]});
    assert_credential(
        &runner.call("route.account_for", &fresh).await,
        "oauth:first",
    );
    let record =
        |credential: &str, class: &str, until: u64| {
            crate::record_cooldown_update(harness.store(), &serde_json::from_value(json!({
            "routeKey":"synthetic/model/xhigh", "capacityKey":"synthetic/model",
            "credentialId":credential, "wallClass":class, "cooldownUntil":until,
            "lastError":"{\"class\":\"transient\",\"provider_code\":\"server_is_overloaded\"}"
        })).unwrap()).unwrap();
        };
    record("oauth:first", "transient", NOW + 5_000);
    let mut continued = account_request("synthetic", "already-running");
    continued["admission_id"] = json!("already-running-next");
    let held = runner.call("route.account_for", &continued).await;
    assert_eq!(held.1["code"], "account_hold", "{}", held.1);
    assert_eq!(held.1["detail"]["until_ms"], NOW + 5_000);
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let held_credential: String = conn
        .query_row(
            "SELECT credential_id FROM pending_account_hold",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(held_credential, "oauth:first");
    let fences = harness
        .store()
        .transient_account_route_fences(NOW as i64)
        .unwrap();
    assert_eq!(fences.len(), 1);
    assert_eq!(fences[0].credential_id, "oauth:first");
    assert_eq!(fences[0].until_ms, NOW as i64 + 5_000);
    let selected = caller
        .call(
            "route.select",
            &json!({
                "consumer":"mason", "requirements":{"iq":30}, "pin":"synthetic/model:xhigh"
            }),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let mut request = account_request("synthetic", "fresh-prefers-walled");
    request["admission_id"] = json!("fresh-prefers-walled-next");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    let admitted = runner.call("route.account_for", &request).await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    assert!(
        matches!(
            admitted.1["result"]["account"]["credential_id"].as_str(),
            Some("oauth:second" | "oauth:third")
        ),
        "{}",
        admitted.1
    );
    for id in ["second", "third"] {
        let mut request = account_request("synthetic", &format!("selectable-{id}"));
        request["candidates"] = json!({"view_digest":"test","accounts":[{
            "account_id":id,"credential_id":format!("oauth:{id}")
        }]});
        assert_credential(
            &runner.call("route.account_for", &request).await,
            &format!("oauth:{id}"),
        );
        record(&format!("oauth:{id}"), "transient", NOW + 5_000);
    }
    let selected = caller
        .call(
            "route.select",
            &json!({
                "consumer":"mason", "requirements":{"iq":30}, "pin":"synthetic/model"
            }),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let mut request = account_request("synthetic", "all-transient-last-resort");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    let admitted = runner.call("route.account_for", &request).await;
    assert_eq!(
        admitted.0,
        FrameType::Error,
        "pending transient retries cannot be bypassed as a last resort: {}",
        admitted.1
    );
    let _retry = crate::InjectedClock::set(NOW + 5_001);
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30},"pin":"synthetic/model"}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    request["admission_id"] = json!("all-transient-single-probe");
    let admitted = runner.call("route.account_for", &request).await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);

    harness
        .store()
        .update_route_state(
            "synthetic/model",
            "synthetic/model",
            &striatum_core::RouteStateUpdate {
                observed_at_ms: Some((NOW + 5_001) as i64),
                cooldown_until: Some((NOW + 1_800_000) as f64),
                quota_exhausted_until: Some((NOW + 1_800_000) as f64),
                wall_class: Some(striatum_core::RouteWallClass::Quota),
                last_error: Some("quota exhausted".into()),
                ..Default::default()
            },
        )
        .unwrap();
    record("oauth:first", "transient", NOW + 5_000);
    let refused = caller
        .call(
            "route.select",
            &json!({
                "consumer":"mason", "requirements":{"iq":30}, "pin":"synthetic/model"
            }),
        )
        .await;
    assert_eq!(
        refused.0,
        FrameType::Error,
        "a transient must not lift a quota wall: {}",
        refused.1
    );
    assert_eq!(refused.1["code"], "pin_unavailable", "{}", refused.1);
    assert_eq!(refused.1["detail"]["reset_at_ms"], NOW + 1_800_000);
    let cleared = harness
        .store()
        .clear_route_cooldowns("synthetic/model")
        .unwrap()
        .unwrap();
    assert_eq!(cleared.account_fences.len(), 1);
    assert!(harness
        .store()
        .transient_account_route_fences(NOW as i64)
        .unwrap()
        .is_empty());
}

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
    let recorded = NOW as i64 - crate::credential_fences::FENCE_PROBE_RECHECK_MS + 1;
    crate::credential_fences::record(harness.store(), &shared, &refusal, recorded).unwrap();
    let runner = harness.bind(reserved(RUNNER), None).await;
    let blocked = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "before-one-hour"),
        )
        .await;
    // One probe opens at the one-hour boundary, not one millisecond later.
    assert_eq!(blocked.1["code"], "account_pool_exhausted", "{}", blocked.1);
    assert_eq!(
        blocked.1["detail"]["reset_at_ms"],
        json!(NOW + 1),
        "{}",
        blocked.1
    );
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
        recorded
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
    // The stale roster cannot send a second probe. Revalidation must retain
    // the winning claim's fence and retry time, not blame runner candidates.
    let lost = crate::account_selection::handle(
        crate::account_selection::ACCOUNT_FOR_OP,
        &body,
        &format!("reserved:{RUNNER}"),
        &ctx,
    );
    assert_eq!(
        lost.code(),
        Some("account_pool_exhausted"),
        "a stale cached roster cannot reserve a second probe"
    );
    match lost {
        crate::account_selection::OpReply::Error { detail, .. } => {
            assert_eq!(detail.unwrap()["reset_at_ms"], NOW + 3_600_001)
        }
        _ => unreachable!(),
    }
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
    assert_eq!(blocked.1["code"], "account_pool_exhausted", "{}", blocked.1);
    assert_eq!(
        blocked.1["detail"]["reset_at_ms"],
        json!(NOW + 3_600_000),
        "{}",
        blocked.1
    );
}

pub(super) fn catalog_fact(harness: &Harness, fact: &striatum_core::ModelFact) {
    use std::sync::atomic::{AtomicU64, Ordering};
    use striatum_store::{FusiformCatalogCursor, FusiformFactUpdate};
    static VERSION: AtomicU64 = AtomicU64::new(1);
    harness
        .store()
        .apply_fusiform_catalog(
            &[FusiformFactUpdate {
                provider_id: fact.provider_id.clone(),
                model_id: fact.model_id.clone(),
                auth_methods: fact.auth_methods.clone(),
                login_methods_documented: fact.login_methods_documented,
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
                catalog_efforts: None,
                canonical_id: None,
                upstream_status: None,
                model_type: None,
                model_kind: None,
                mode_of: None,
                lineage: None,
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

pub(super) fn account_fact(provider: &str) -> striatum_core::ModelFact {
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
    fact
}

pub(super) fn setup_at_version(
    provider: &str,
    converted_live: bool,
    version: Option<u32>,
) -> (Arc<Harness>, BrocaCredentials) {
    let harness = Harness::new_at_version("account-classes", version);
    configure_harness(harness, provider, converted_live, version)
}

pub(super) fn setup_from_template(
    provider: &str,
    converted_live: bool,
    template: &std::path::Path,
) -> (Arc<Harness>, BrocaCredentials) {
    let harness = Harness::from_template("account-classes", template);
    configure_harness(harness, provider, converted_live, None)
}

fn configure_harness(
    harness: Arc<Harness>,
    provider: &str,
    converted_live: bool,
    version: Option<u32>,
) -> (Arc<Harness>, BrocaCredentials) {
    let fact = account_fact(provider);
    // Historical fixtures seed only decisions before upgrading. Current fact
    // writers require the current schema, so install those facts after upgrade.
    if version.is_none() {
        harness.install_facts(&[fact.clone()]);
        catalog_fact(&harness, &fact);
    }
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
            *config = crate::test_config::convert_archived(include_str!(
                "../../fixtures/routing-config-live-2026-10-24.jsonc"
            ));
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
    exhaust_oauth_until(harness, provider, NOW as i64 + 600_000);
}

fn exhaust_oauth_until(harness: &Harness, provider: &str, reset_at_ms: i64) {
    let scope = FenceScope::General(GeneralWindow::FiveHour);
    let fence = Fence {
        account_id: "oauth-account".into(),
        pool: Some(scope.into()),
        recovery_set: Vec::new(),
        observed_at_ms: NOW as i64,
        reset: ResetTime::At(reset_at_ms),
        retry_hint_ms: None,
        source: ConfirmationSource::BrocaAccountLimit,
    };
    harness
        .store()
        .account_transaction(|tx| {
            tx.insert_fence(&AccountFenceRow {
                fence_id: format!("class-fence-{reset_at_ms}"),
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
        for legacy in [false] {
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
async fn paid_only_runner_candidates_require_a_routing_decision() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let key = json!({"account_id":"apikey-account", "credential_id":"apikey:test"});
    let subscription = json!({"account_id":"oauth-account", "credential_id":"oauth:test"});
    let mut request = account_request("synthetic", "paid-only-no-decision");
    request["candidates"] = json!({"view_digest":"paid-only", "accounts":[key.clone()]});
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "invalid_request", "{}", refused.1);
    assert_eq!(refused.1["detail"]["field"], "decision_id", "{}", refused.1);
    assert_eq!(
        refused.1["detail"]["exclusions"],
        json!([{"account_id":"apikey-account", "credential_id":"apikey:test",
            "reason":"routing_decision_required"}]),
        "{}",
        refused.1
    );
    assert_eq!(
        cortexkit_role_routing::refusals::retry_class(
            refused.1["code"].as_str().unwrap(),
            &refused.1["detail"]
        ),
        cortexkit_role_routing::refusals::RetryClass::Permanent
    );
    for (id, accounts) in [
        ("subscription-only", json!([subscription.clone()])),
        ("mixed", json!([key, subscription])),
    ] {
        let mut request = account_request("synthetic", id);
        request["candidates"] = json!({"view_digest":id, "accounts":accounts});
        assert_credential(
            &runner.call("route.account_for", &request).await,
            "oauth:test",
        );
    }
}

#[tokio::test]
async fn pinned_paid_pool_decision_requires_the_selected_provider_not_the_runner_provider() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("anthropic", false);
    let mut rows = credentials.listed_credentials().to_vec();
    let key = rows.iter_mut().find(|row| row.id == "apikey:test").unwrap();
    key.id = "apikey:anthropic:erin".into();
    key.account_id = Some("erin".into());
    key.provider_ids = vec!["anthropic-platform".into()];
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "anthropic-paid-pool".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(
        &Default::default(),
        &[account_fact("anthropic")],
        NOW as i64,
        &[],
    );
    credentials.runner_declarations.insert(
        RUNNER.into(),
        super::runners::parse_declaration(&json!({"runner": {
            "pairs":[{"wire_api":"anthropic.messages", "auth_method":"apikey"},
                {"wire_api":"anthropic.messages", "auth_method":"oauth"}],
            "follows_account_choice":true
        }}))
        .unwrap()
        .unwrap(),
    );
    credentials
        .runner_routed_accounts
        .insert(RUNNER.into(), vec!["apikey".into(), "oauth".into()]);
    credentials
        .wire_apis
        .insert("anthropic/model".into(), "anthropic.messages".into());
    let planes = credentials.billing_catalog.clone();
    credentials.publish_billing_catalog(planes.with_wire_apis(&credentials.wire_apis));
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["providers"]["anthropic-platform"] = json!({"pin_only":true, "only_for":["mason"]});
    });
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut unselected = account_request("anthropic", "unselected-erin");
    unselected["candidates"] = json!({"view_digest":"erin-only", "accounts":[{
        "account_id":"erin", "credential_id":"apikey:anthropic:erin"
    }]});
    let refused = runner.call("route.account_for", &unselected).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "invalid_request", "{}", refused.1);
    assert_eq!(refused.1["detail"]["field"], "decision_id", "{}", refused.1);
    assert_eq!(
        refused.1["detail"]["exclusions"],
        json!([{
            "account_id":"erin", "credential_id":"apikey:anthropic:erin", "reason":"routing_decision_required"
        }]),
        "{}",
        refused.1
    );
    let selected = caller
        .call(
            "route.select",
            &json!({
                "consumer":"mason", "pin":"anthropic-platform/model", "requirements":{"iq":0}
            }),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    assert_eq!(
        selected.1["result"]["route"]["provider"], "anthropic-platform",
        "{}",
        selected.1
    );
    assert_eq!(
        selected.1["result"]["runner"]["model"]["provider"], "anthropic",
        "{}",
        selected.1
    );
    assert_eq!(
        selected.1["result"]["runner"]["auth"]["pool"], "anthropic-platform",
        "{}",
        selected.1
    );
    for offered in [false, true] {
        for pool_hint in [false, true] {
            let mut request =
                account_request("anthropic", &format!("pool-decision-{offered}-{pool_hint}"));
            request["decision_id"] = selected.1["result"]["decision_id"].clone();
            if pool_hint {
                request["pool"] = json!("anthropic-platform");
            }
            if offered {
                request["candidates"] = json!({"view_digest":"erin-only", "accounts":[{
                    "account_id":"erin", "credential_id":"apikey:anthropic:erin"
                }]});
            }
            let refused = runner.call("route.account_for", &request).await;
            assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
            assert_eq!(refused.1["code"], "unknown_decision", "{}", refused.1);
            assert_eq!(refused.1["detail"], json!({}), "{}", refused.1);
            // The decision binds the paid pool, not the catalog provider the
            // runner uses to speak to Anthropic. Sending the selected route keeps
            // account admission in that pool without weakening decision binding.
            request["provider"] = json!("anthropic-platform");
            let admitted = runner.call("route.account_for", &request).await;
            assert_credential(&admitted, "apikey:anthropic:erin");
            assert_eq!(
                admitted.1["result"]["account"]["provider"],
                "anthropic-platform"
            );
            assert_eq!(admitted.1["result"]["account"]["method"], "apikey");
            assert_eq!(
                admitted.1["result"]["reason"]["decision_id"],
                request["decision_id"]
            );
        }
    }
}

#[tokio::test]
async fn ten_default_admissions_never_spend_after_subscription_exhaustion_or_entitlement_refusal() {
    let _clock = crate::InjectedClock::set(NOW);
    for (provider, converted_live) in [("synthetic", false), ("openai", true)] {
        for entitlement in [false, true] {
            let (harness, _) = setup(provider, converted_live);
            // Opt automatic spending in while testing the separate account
            // fallback fence after subscription exhaustion.
            harness.edit_config(|config| config["providers"][provider]["pin_only"] = json!(false));
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
                // Quota exhaustion is `account_pool_exhausted` timed by its
                // reset; fenced credentials without a reset are
                // `account_not_authorized`. A short hold stays a hold.
                assert!(
                    matches!(
                        reply.1["code"].as_str(),
                        Some("account_hold" | "account_pool_exhausted" | "account_not_authorized")
                    ),
                    "{}",
                    reply.1
                );
                if !entitlement && reply.1["code"] == "account_pool_exhausted" {
                    assert_eq!(
                        reply.1["detail"]["reset_at_ms"],
                        NOW + 600_000,
                        "{}",
                        reply.1
                    );
                }
                if reply.1["code"] == "account_not_authorized" {
                    assert_eq!(reply.1["detail"], json!({}), "{}", reply.1);
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
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Mutex;
    use striatum_core::RouteStateStore;
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
            false,
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
    use striatum_core::{FactSource, RouteStateStore};
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
    let (harness, credentials) = setup_at_version("synthetic", false, Some(62));
    let mut decisions = Vec::new();
    for consumer in ["mason", "codemode"] {
        for legacy in [false, true] {
            let id = format!("historical-{consumer}-{legacy}");
            harness
                .store()
                .seed_preupgrade_decision_for_test(
                    &striatum_store::V1Decision {
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
    let fact = account_fact("synthetic");
    harness.install_facts(&[fact.clone()]);
    catalog_fact(&harness, &fact);
    harness.set_credentials(credentials);
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
    use striatum_core::{FactSource, RouteStateStore};
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
    // No credential serves the payment class, so the provider needs a login
    // the runner does not have.
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "auth_required");
    assert_eq!(reply.1["detail"], json!({}), "{}", reply.1);
}

pub(super) fn oauth_reading(used_percent: f64) {
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
async fn account_reserve_does_not_rescale_demand_to_a_weaker_model() {
    assert_account_unavailability_keeps_frontier("reserve").await;
}

#[tokio::test]
async fn account_exhaustion_does_not_rescale_demand_to_a_weaker_model() {
    assert_account_unavailability_keeps_frontier("exhausted").await;
}

#[tokio::test]
async fn refused_credential_does_not_rescale_demand_to_a_weaker_model() {
    assert_account_unavailability_keeps_frontier("refused").await;
}

#[tokio::test]
async fn stale_credential_listing_does_not_rescale_demand_to_a_weaker_model() {
    assert_account_unavailability_keeps_frontier("stale").await;
}

async fn assert_account_unavailability_keeps_frontier(block: &str) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    harness.edit_config(|config| {
        config["runners"] = json!([
            {"module":RUNNER,"via_vault":true,"routes":["synthetic/model"]},
            {"module":"direct-runner","routes":["fallback/model"]}
        ])
    });
    oauth_reading(0.0);
    let mut top = crate::tests::fact("synthetic", "model");
    top.scores.elo = 1_800.0;
    top.auth_methods = Some(vec!["oauth".into()]);
    let mut weak = crate::tests::fact("fallback", "model");
    weak.scores.elo = 500.0;
    weak.auth_methods = Some(vec!["apikey".into()]);
    harness.install_facts(&[top.clone(), weak.clone()]);
    catalog_fact(&harness, &top);
    catalog_fact(&harness, &weak);
    let mut rows = credentials.listed_credentials().to_vec();
    rows.push(ListedCredential {
        id: "apikey:fallback".into(),
        account_id: Some("fallback-account".into()),
        state: "active".into(),
        categories: vec!["llm-provider".into()],
        provider_ids: vec!["fallback".into()],
        serves: vec!["fallback".into()],
        credential_type: "apikey".into(),
        auth_method: Some("apikey".into()),
        ..Default::default()
    });
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "frontier".into(),
            credentials: rows.clone(),
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(
        &Default::default(),
        &[top.clone(), weak.clone()],
        NOW as i64,
        &[],
    );
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[top.clone(), weak]),
    );
    harness.set_credentials(credentials.clone());
    let caller = harness.bind(reserved(OWNER), None).await;
    let request = json!({"consumer":"mason", "requirements":{"iq":90}, "detail":true});
    let baseline = caller.call("route.select", &request).await;
    assert_eq!(baseline.0, FrameType::Response, "{block}: {}", baseline.1);
    assert_eq!(
        baseline.1["result"]["route"]["provider"], "synthetic",
        "{}",
        baseline.1
    );
    match block {
        "reserve" => oauth_reading(99.0),
        "exhausted" => {
            // This control is confirmed exhaustion, not a quota refusal
            // contradicted by the earlier healthy baseline reading.
            oauth_reading(100.0);
            exhaust_oauth(&harness, "synthetic");
        }
        "refused" => {
            crate::credential_fences::record(
                harness.store(),
                &Arc::new(std::sync::Mutex::new(credentials.clone())),
                &CredentialRefusal {
                    provider_id: "synthetic".into(),
                    credential_id: "oauth:test".into(),
                    code: "ENTITLEMENT_ERROR".into(),
                },
                NOW as i64,
            )
            .unwrap();
        }
        "stale" => {
            credentials.apply_listing(
                &ScopedCredentialListing {
                    view: "frontier-stale".into(),
                    credentials: rows.clone(),
                    ..Default::default()
                },
                NOW - 15 * 60_000 - 1,
            );
            harness.set_credentials(credentials.clone());
        }
        _ => unreachable!(),
    }
    if matches!(block, "reserve" | "exhausted") {
        let classes = crate::account_selection::ClassContext::read(
            harness.store(),
            &Arc::new(std::sync::Mutex::new(credentials.clone())),
            harness.operator_config(),
            crate::ACCOUNT_QUOTA.get(),
            vec![top],
            NOW as i64,
        )
        .unwrap();
        // Structured selection preflight retains the quota cause even when
        // temporal filtering has removed every account from the raw roster.
        assert_eq!(
            classes.blocked_reset("synthetic", "model", false),
            if block == "reserve" {
                Some(Some(NOW as i64 + 3_600_000))
            } else {
                Some(Some(NOW as i64 + 600_000))
            }
        );
    }
    let blocked = caller.call("route.select", &request).await;
    assert_eq!(blocked.0, FrameType::Error, "{block}: {}", blocked.1);
    assert_eq!(blocked.1["code"], "no_route", "{block}: {}", blocked.1);
    assert_eq!(
        blocked.1["detail"]["reason"],
        if !matches!(block, "exhausted" | "reserve") {
            json!(cortexkit_role_routing::errors::NoRouteReason::AllInadequate)
        } else {
            json!("accounts_blocked")
        },
        "{block}: {}",
        blocked.1
    );
    if block == "exhausted" {
        let runner = harness.bind(reserved(RUNNER), None).await;
        let admission = runner
            .call(
                "route.account_for",
                &account_request("synthetic", "frontier-exhausted"),
            )
            .await;
        assert_eq!(admission.0, FrameType::Error, "{}", admission.1);
        assert_eq!(
            admission.1["code"], "account_pool_exhausted",
            "quota filtering is not an authorization failure: {}",
            admission.1
        );
        assert_eq!(admission.1["detail"]["reset_at_ms"], NOW as i64 + 600_000);
    }
    if matches!(block, "refused" | "stale") {
        let rejected = blocked.1["detail"]["rejected"].as_array().unwrap();
        let top = rejected
            .iter()
            .find(|route| route["routeKey"] == "synthetic/model")
            .unwrap();
        assert_eq!(
            top,
            &if block == "refused" {
                json!({"routeKey":"synthetic/model", "reason":"credential_unentitled", "credentialIds":["oauth:test"]})
            } else {
                json!({"routeKey":"synthetic/model", "reason":"not_available"})
            }
        );
    }

    // A provider with no serving credential cannot define the maximum model
    // scores used to interpret IQ, unlike a listed but unavailable credential.
    rows.retain(|row| {
        !row.provider_ids
            .iter()
            .any(|provider| provider == "synthetic")
    });
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "no-top-credential".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW + 1,
    );
    harness.set_credentials(credentials);
    let unreachable = caller.call("route.select", &request).await;
    assert_eq!(
        unreachable.0,
        FrameType::Response,
        "{block}: {}",
        unreachable.1
    );
    assert_eq!(
        unreachable.1["result"]["route"]["provider"], "fallback",
        "{}",
        unreachable.1
    );
}

#[tokio::test]
async fn provider_strategies_never_escape_the_subscription_class() {
    let _clock = crate::InjectedClock::set(NOW);
    for strategy in ["round_robin", "ordered"] {
        let (harness, _) = setup("synthetic", false);
        oauth_reading(10.0);
        harness.edit_config(|config| config["providers"]["synthetic"]["accounts"] = json!({
            "strategy":strategy,"ordered":"fallback-first","order":["oauth-account","apikey-account"],
            "max_hold":"30s","unknown_reset_retry":"2h"
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
                Some("account_hold" | "account_pool_exhausted")
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
    // The eligibility refusal filtered the subscription class, so the caller
    // may not use it.
    assert_eq!(next.1["code"], "account_not_authorized", "{}", next.1);
    assert_eq!(next.1["detail"], json!({}), "{}", next.1);
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
            assert_eq!(answer.1["code"], "account_not_authorized", "{}", answer.1);
            assert_eq!(answer.1["detail"], json!({}), "{}", answer.1);
        }
        request["model"] = json!("other");
        request["admission_id"] = json!(format!("other-{index}"));
        let answer = runner.call("route.account_for", &request).await;
        assert_eq!(answer.1["code"], "account_not_authorized", "{}", answer.1);
        assert_eq!(answer.1["detail"], json!({}), "{}", answer.1);
    }
}

#[tokio::test]
async fn size_shaped_429_restricts_only_its_band_and_only_an_admitted_size_probe_clears_it() {
    exercise_size_probe(false).await;
}

#[tokio::test]
async fn size_quota_refusal_contradicted_by_headroom_remains_non_quota_until_its_size_probe() {
    exercise_size_probe(true).await;
}

async fn exercise_size_probe(contradicted: bool) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    if contradicted {
        oauth_weekly_reading_at(0.0, NOW as i64 + 86_400_000, NOW as i64);
    }
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let select = json!({"consumer":"mason","requirements":{"iq":30},"pin":"synthetic/model"});
    let decision = caller.call("route.select", &select).await;
    assert_eq!(decision.0, FrameType::Response, "{}", decision.1);
    let report=caller.call("route.report",&json!({"decision_id":decision.1["result"]["decision_id"],"outcome":"refused","credential_id":"oauth:test","runner_code":"size_shaped_429","request_input_tokens":169_000,"reset_at_ms":NOW+10_000})).await;
    assert_eq!(report.0, FrameType::Response, "{}", report.1);
    let origins = harness.store().fence_rechecks().unwrap();
    let kind = if contradicted {
        "non_quota_size"
    } else {
        "size"
    };
    assert!(origins.iter().any(|origin| origin.kind == kind));
    let mut large = account_request("synthetic", "large");
    large["initial_input_bytes"] = json!(169_000 * 4);
    let blocked = runner.call("route.account_for", &large).await;
    assert_eq!(blocked.1["code"], "account_not_authorized", "{}", blocked.1);
    assert_eq!(blocked.1["detail"], json!({}), "{}", blocked.1);
    let mut small = account_request("synthetic", "small");
    small["initial_input_bytes"] = json!(4096 * 4);
    assert_credential(
        &runner.call("route.account_for", &small).await,
        "oauth:test",
    );
    // Unknown-size selection still serves ordinary traffic. A token count
    // supplied later with its report is not a claimed size-class admission.
    let unknown = caller.call("route.select", &select).await;
    assert_eq!(unknown.0, FrameType::Response, "{}", unknown.1);
    let _later = crate::InjectedClock::set(NOW + 1);
    if contradicted {
        oauth_weekly_reading_at(0.0, NOW as i64 + 86_400_000, NOW as i64 + 1);
    }
    let success=caller.call("route.report",&json!({"decision_id":unknown.1["result"]["decision_id"],"outcome":"completed","credential_id":"oauth:test","request_input_tokens":169_000})).await;
    assert_eq!(success.0, FrameType::Response, "{}", success.1);
    assert_eq!(
        harness
            .store()
            .account_size_refusals(NOW as i64 + 1)
            .unwrap()
            .len(),
        1,
        "neither unknown-size success nor headroom clears a contradictory non-quota size wall"
    );
    large["admission_id"] = json!("large-still-fenced");
    assert_eq!(
        runner.call("route.account_for", &large).await.0,
        FrameType::Error
    );
    let _due = crate::InjectedClock::set(NOW + 10_000);
    let probe = caller.call("route.select", &select).await;
    assert_eq!(probe.0, FrameType::Response, "{}", probe.1);
    large["admission_id"] = json!("large-probe");
    large["decision_id"] = probe.1["result"]["decision_id"].clone();
    assert_credential(
        &runner.call("route.account_for", &large).await,
        "oauth:test",
    );
    let mut concurrent = large.clone();
    concurrent.as_object_mut().unwrap().remove("decision_id");
    concurrent["admission_id"] = json!("second-size-probe");
    concurrent["conversation_key"] = json!("second-size-probe");
    assert_eq!(
        runner.call("route.account_for", &concurrent).await.0,
        FrameType::Error,
        "one size-aware request claims the class"
    );
    let success=caller.call("route.report",&json!({"decision_id":probe.1["result"]["decision_id"],"outcome":"completed","credential_id":"oauth:test"})).await;
    assert_eq!(success.0, FrameType::Response, "{}", success.1);
    assert!(harness
        .store()
        .account_size_refusals(NOW as i64 + 10_000)
        .unwrap()
        .is_empty());
    concurrent["admission_id"] = json!("large-after-success");
    assert_credential(
        &runner.call("route.account_for", &concurrent).await,
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

#[test]
fn reconciliation_writes_once_and_rechecks_new_facts_with_the_same_listing() {
    use striatum_store::AccountCommandKind::Repair;
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    let fingerprint = match credentials.listing_fingerprint("oauth:test") {
        crate::broca_credentials::ListingLookup::Listed(value) => value,
        _ => panic!("the fixture must have a successful listing"),
    };
    let fact = striatum_store::AccountEligibilityFact {
        provider: "synthetic".into(),
        credential_id: "oauth:test".into(),
        model: "unlisted-model".into(),
        listing_fingerprint: None,
        code: "ineligible".into(),
        observed_at: 1,
    };
    harness.store().record_account_eligibility(&fact).unwrap();
    let commands = harness.store().account_command_count_for_test(Repair);
    let rows_before = harness
        .store()
        .account_command_row_changes_for_test(Repair)
        .len();
    let outside = harness.store().outside_account_writer_count_for_test();
    let hints = Arc::new(std::sync::Barrier::new(20));
    let readers: Vec<_> = (0..20)
        .map(|_| {
            let store = harness.store().clone();
            let credentials = harness.credentials();
            let config = harness.operator_config();
            let hints = hints.clone();
            std::thread::spawn(move || {
                let _clock = crate::InjectedClock::set(NOW);
                // All callers observe the missing fingerprint before any command
                // is submitted, so the writer must recheck twenty stale hints.
                crate::credential_fences::before_listing_repair_for_test(move || {
                    hints.wait();
                });
                crate::account_selection::ClassContext::load(
                    &store,
                    &credentials,
                    config,
                    None,
                    NOW as i64,
                )
                .unwrap();
            })
        })
        .collect();
    for reader in readers {
        reader.join().unwrap();
    }
    assert_eq!(
        harness.store().account_command_count_for_test(Repair) - commands,
        20
    );
    let changes = harness.store().account_command_row_changes_for_test(Repair);
    let changes = &changes[rows_before..];
    assert_eq!(changes.len(), 20);
    assert_eq!(changes.iter().filter(|rows| **rows > 0).count(), 1);
    assert_eq!(
        changes.iter().sum::<u64>(),
        1,
        "only the missing fingerprint changes"
    );
    assert_eq!(
        harness.store().account_eligibility().unwrap()[0]
            .listing_fingerprint
            .as_deref(),
        Some(fingerprint.as_str())
    );
    let load = || {
        crate::account_selection::ClassContext::load(
            harness.store(),
            &harness.credentials(),
            harness.operator_config(),
            None,
            NOW as i64,
        )
        .unwrap()
    };
    let commands = harness.store().account_command_count_for_test(Repair);
    load();
    assert_eq!(
        harness.store().account_command_count_for_test(Repair),
        commands,
        "an identical later load enqueues no repair"
    );
    harness
        .store()
        .record_account_eligibility(&striatum_store::AccountEligibilityFact {
            observed_at: 2,
            ..fact
        })
        .unwrap();
    load();
    assert_eq!(
        harness.store().account_command_count_for_test(Repair) - commands,
        1,
        "a new fact without a fingerprint must adopt even an unchanged listing"
    );
    assert_eq!(
        harness
            .store()
            .account_command_row_changes_for_test(Repair)
            .last(),
        Some(&1)
    );
    load();
    assert_eq!(
        harness.store().account_command_count_for_test(Repair) - commands,
        1
    );
    let mut listing = credentials.listed_credentials().to_vec();
    for row in &mut listing {
        row.plan_tier = Some("new-plan".into());
    }
    credentials.apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            view: "changed-listing".into(),
            credentials: listing,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    load();
    assert_eq!(
        harness.store().account_command_count_for_test(Repair) - commands,
        2
    );
    assert!(harness.store().account_eligibility().unwrap().is_empty());
    assert_eq!(
        harness.store().outside_account_writer_count_for_test(),
        outside
    );
}

#[tokio::test]
async fn unchanged_context_reads_without_reserving_the_writer() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    let fingerprint = match credentials.listing_fingerprint("oauth:test") {
        crate::broca_credentials::ListingLookup::Listed(fingerprint) => fingerprint,
        _ => panic!("the fixture must have a successful listing"),
    };
    harness
        .store()
        .record_account_eligibility(&striatum_store::AccountEligibilityFact {
            provider: "synthetic".into(),
            credential_id: "oauth:test".into(),
            model: "unlisted-model".into(),
            listing_fingerprint: Some(fingerprint),
            code: "ineligible".into(),
            observed_at: NOW as i64,
        })
        .unwrap();
    let shared = Arc::new(std::sync::Mutex::new(credentials));
    let writer = rusqlite::Connection::open(harness.store_path()).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let before = harness.store().account_write_transaction_count_for_test();
    let context = crate::account_selection::ClassContext::load(
        harness.store(),
        &shared,
        harness.operator_config(),
        crate::ACCOUNT_QUOTA.get(),
        NOW as i64,
    );
    writer.execute_batch("ROLLBACK").unwrap();
    assert!(context.is_ok(), "a context with nothing to reconcile must read while another connection holds the writer: {:?}", context.err());
    assert_eq!(
        harness.store().account_write_transaction_count_for_test() - before,
        0
    );
    assert_eq!(harness.store().account_eligibility().unwrap().len(), 1);
}

#[tokio::test]
async fn unchanged_quota_hold_reads_without_reserving_the_writer() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    oauth_weekly_reading_at(100.0, NOW as i64 + 3_600_000, NOW as i64);
    harness
        .store()
        .record_credential_entitlement_fence(
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: "synthetic".into(),
                credential_id: "oauth:test".into(),
                code: "billing_exhausted".into(),
            },
            None,
            NOW as i64,
        )
        .unwrap();
    let credentials = harness.credentials();
    let load = || {
        crate::account_selection::ClassContext::load(
            harness.store(),
            &credentials,
            harness.operator_config(),
            crate::ACCOUNT_QUOTA.get(),
            NOW as i64,
        )
    };
    assert_eq!(load().unwrap().refusals.len(), 1);
    assert_eq!(
        harness
            .store()
            .account_read(|tx| tx.active_pool_fences("synthetic"))
            .unwrap()
            .len(),
        1
    );
    let writer = rusqlite::Connection::open(harness.store_path()).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let before = harness.store().account_write_transaction_count_for_test();
    let result = load();
    writer.execute_batch("ROLLBACK").unwrap();
    assert!(
        result.is_ok(),
        "unchanged hold reserved the writer: {:?}",
        result.err()
    );
    assert_eq!(
        harness.store().account_write_transaction_count_for_test(),
        before
    );
}

#[tokio::test]
async fn selection_reuses_one_read_only_context_across_ladder_entries() {
    let (loads, _, _, _) = select_past_held_ladder_entry().await;
    assert_eq!(loads, 1);
}

#[tokio::test]
async fn selection_with_unchanged_context_sends_no_repairs_and_one_affinity_command() {
    let (_, batches, repairs, selections) = select_past_held_ladder_entry().await;
    assert_eq!(repairs, 0);
    assert_eq!(selections, 1);
    assert_eq!(batches, selections);
}

async fn select_past_held_ladder_entry() -> (u64, u64, u64, u64) {
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
    let loads = CONTEXT_LOADS.with(|loads| loads.get());
    let writes = harness.store().account_write_transaction_count_for_test();
    let repairs = harness
        .store()
        .account_command_count_for_test(striatum_store::AccountCommandKind::Repair);
    let selections = harness
        .store()
        .account_command_count_for_test(striatum_store::AccountCommandKind::Selection);
    let serving = harness.store().account_serving_counts().unwrap();
    let selected = caller
        .select_with_profiles(
            &json!({"consumer":"mason","requirements":{"iq":30},"hold_ok":false}),
            &|_, _, _| crate::select::TaskProfileLookup {
                profiles: Default::default(),
                providers_without_cache_write_reports: Default::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        selected["route"]["provider"], "secondary",
        "the held first entry must be checked before choosing the second"
    );
    assert_eq!(
        harness.store().account_serving_counts().unwrap(),
        serving,
        "model selection must not create account admissions"
    );
    (
        CONTEXT_LOADS.with(|loads| loads.get()) - loads,
        harness.store().account_write_transaction_count_for_test() - writes,
        harness
            .store()
            .account_command_count_for_test(striatum_store::AccountCommandKind::Repair)
            - repairs,
        harness
            .store()
            .account_command_count_for_test(striatum_store::AccountCommandKind::Selection)
            - selections,
    )
}

#[tokio::test]
async fn runner_hold_caps_default_to_900_and_survive_selection_and_admission() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    harness.edit_config(|config| {
        config["consumers"]["mason"] = json!({
            "owner":OWNER,"mode":"ladder","ladder":["synthetic/model"]
        })
    });
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let request = json!({"consumer":"mason","requirements":{"iq":30}});
    let default_selected = caller.call("route.select", &request).await;
    assert_eq!(
        default_selected.0,
        FrameType::Response,
        "{}",
        default_selected.1
    );
    let default_id = default_selected.1["result"]["decision_id"]
        .as_str()
        .unwrap();
    let stored = harness
        .store()
        .get_decision_by_id(default_id)
        .unwrap()
        .unwrap();
    let input: Value = serde_json::from_str(&stored.requirements_json).unwrap();
    assert_eq!(input["hold_wait_max_s"], 900);

    exhaust_oauth(&harness, "synthetic");
    let ten_minutes = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "default-ten-minutes"),
        )
        .await;
    assert_eq!(ten_minutes.1["code"], "account_hold", "{}", ten_minutes.1);

    exhaust_oauth_until(&harness, "synthetic", NOW as i64 + 2_400_000);
    let mut patient = request.clone();
    patient["hold_wait_max_s"] = json!(3000);
    let selected = caller.call("route.select", &patient).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let selected_id = selected.1["result"]["decision_id"].clone();
    for cap in [None, Some(900), Some(0)] {
        let mut limited = request.clone();
        if let Some(cap) = cap {
            limited["hold_wait_max_s"] = json!(cap);
        }
        let reply = caller.call("route.select", &limited).await;
        assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    }
    for linked in [false, true] {
        for (index, cap) in [None, Some(900), Some(0), Some(3000)]
            .into_iter()
            .enumerate()
        {
            let mut admission = account_request("synthetic", &format!("wait-{linked}-{index}"));
            if linked {
                admission["decision_id"] = selected_id.clone();
            }
            if let Some(cap) = cap {
                admission["hold_wait_max_s"] = json!(cap);
            }
            let reply = runner.call("route.account_for", &admission).await;
            if cap == Some(3000) {
                assert_eq!(reply.1["code"], "account_hold", "{}", reply.1);
            } else {
                assert_eq!(reply.1["code"], "account_pool_exhausted", "{}", reply.1);
            }
        }
    }
    let mut admission = account_request("synthetic", "default-selection-cap");
    admission["decision_id"] = json!(default_id);
    admission["hold_wait_max_s"] = json!(3000);
    let reply = runner.call("route.account_for", &admission).await;
    assert_eq!(reply.1["code"], "account_pool_exhausted", "{}", reply.1);

    patient["hold_wait_max_s"] = json!(10_000);
    let selected = caller.call("route.select", &patient).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let id = selected.1["result"]["decision_id"].as_str().unwrap();
    let stored = harness.store().get_decision_by_id(id).unwrap().unwrap();
    let input: Value = serde_json::from_str(&stored.requirements_json).unwrap();
    assert_eq!(input["hold_wait_max_s"], 3600);
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["accounts"]["max_hold"] = json!("1800s")
    });
    admission["admission_id"] = json!("current-provider-ceiling");
    admission["decision_id"] = json!(id);
    let reply = runner.call("route.account_for", &admission).await;
    assert_eq!(reply.1["code"], "account_pool_exhausted", "{}", reply.1);
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
    harness.edit_config(|c| c["providers"]["synthetic"]["accounts"]["max_hold"] = json!("30s"));
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
    // The exhausted reading keeps quota evidence binding. The retry hint is
    // its next fifteen-minute check, not a promise that the quota resets then.
    assert_eq!(blocked.1["code"], "account_pool_exhausted", "{}", blocked.1);
    assert_eq!(
        blocked.1["detail"]["reset_at_ms"],
        json!(NOW as i64 + 900_000),
        "{}",
        blocked.1
    );
}

/// Contract 0.5.0: a failed report with a future reset fences the decision's
/// route on the account that served it until a recheck. Before 0.5.0 such a
/// report was only advisory and this test pinned that the same route and
/// account were answered again; the contract now says the opposite. What still
/// holds: the failure ends affinity, the fence is not a size refusal, and the
/// refusals name the next check so callers can retry without inventing recovery.
#[tokio::test]
async fn failure_reports_with_resets_fence_the_route_on_the_serving_account() {
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
    let reset = NOW + 3_600_000;
    let report=caller.call("route.report",&json!({"decision_id":first.1["result"]["decision_id"],"outcome":"failed","runner_code":"unconfirmed_429","reset_at_ms":reset})).await;
    assert_eq!(report.0, FrameType::Response, "{}", report.1);
    assert!(harness
        .store()
        .v1_affinity_decision(&format!("reserved:{OWNER}"), "mason", "sticky")
        .unwrap()
        .is_none());
    // The only route's only account is fenced: selection names the reset.
    let second = caller.call("route.select", &request).await;
    assert_eq!(second.0, FrameType::Error, "{}", second.1);
    assert_eq!(second.1["code"], "no_route", "{}", second.1);
    assert_eq!(
        second.1["detail"]["reason"], "accounts_blocked",
        "{}",
        second.1
    );
    assert_eq!(
        second.1["detail"]["reset_at_ms"],
        json!(NOW + 15 * 60_000),
        "{}",
        second.1
    );
    let mut request = account_request("synthetic", "after-failure");
    request["conversation_key"] = json!("conversation-before-failure");
    let after_account = runner.call("route.account_for", &request).await;
    assert_eq!(after_account.0, FrameType::Error, "{}", after_account.1);
    assert_eq!(
        after_account.1["code"], "account_pool_exhausted",
        "{}",
        after_account.1
    );
    assert_eq!(
        after_account.1["detail"]["reset_at_ms"],
        json!(NOW + 15 * 60_000),
        "{}",
        after_account.1
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
    // The only subscription account sits within its reserve; the public
    // refusal schedules a quota recheck before the distant provider reset.
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(
        reply.1["code"], "account_pool_reserve_limited",
        "{}",
        reply.1
    );
    assert_eq!(
        reply.1["detail"]["reset_at_ms"],
        NOW + 900_000,
        "{}",
        reply.1
    );
    assert!(reply.1["detail"]["retry_after_s"].as_u64().unwrap() > 0);
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
                // The metered class lost its only credential, so the provider
                // needs a login the runner does not have.
                let reply = runner.call("route.account_for", &request).await;
                assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
                assert_eq!(reply.1["code"], "auth_required", "{}", reply.1);
                assert_eq!(reply.1["detail"], json!({}), "{}", reply.1);
            }
        }
    }
}
fn oauth_weekly_reading_at(used_percent: f64, reset_ms: i64, fetched_ms: i64) {
    let mut state = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_provider_reply(
        &mut state,
        "test-source",
        &serde_json::to_vec(&json!({
            "result":[{"provider":"test-source","apiProvider":"synthetic","account":"oauth-account","source":"vault",
                "fetchedAt":crate::account_selection::rfc3339(fetched_ms),
                "usage":{"primary":{"usedPercent":used_percent,"windowMinutes":10080,
                    "resetsAt":crate::account_selection::rfc3339(reset_ms)}}}],
            "completeProviders":["test-source"]
        }))
        .unwrap(),
        fetched_ms,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
}

#[tokio::test]
async fn pin_reserve_retry_by_recheck_and_keep_near_reset() {
    for (weekly_reset, expected_retry) in [
        (NOW + 60_000, NOW + 60_000),
        (NOW + 86_400_000, NOW + 900_000),
    ] {
        pin_availability_reserve_case(weekly_reset, expected_retry).await;
    }
}

async fn pin_availability_reserve_case(weekly_reset: u64, expected_retry: u64) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    harness.edit_config(|config| {
        config["model_routing"]["keep_free_percent"] = json!({"synthetic":20});
    });
    let mut quota = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_provider_reply(
        &mut quota,
        "test-source",
        &serde_json::to_vec(&json!({"result":[
            {"provider":"test-source","apiProvider":"synthetic","account":"oauth-account","source":"vault",
                "fetchedAt":crate::account_selection::rfc3339(NOW as i64),
                "usage":{
                    "primary":{"usedPercent":0.0,"windowMinutes":300,
                        "resetsAt":crate::account_selection::rfc3339((NOW + 60_000) as i64)},
                    "secondary":{"usedPercent":95.0,"windowMinutes":10080,
                        "resetsAt":crate::account_selection::rfc3339(weekly_reset as i64)}}},
            {"provider":"test-source","apiProvider":"synthetic","account":"apikey-account","source":"vault",
                "fetchedAt":crate::account_selection::rfc3339(NOW as i64),
                "usage":{"primary":{"usedPercent":100.0,"windowMinutes":10080,
                    "resetsAt":crate::account_selection::rfc3339((NOW + 30_000) as i64)}}}
        ],"completeProviders":["test-source"]})).unwrap(),
        NOW as i64,
    ).unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = quota;
    let operator = harness.bind(json!({"kind":"direct"}), None).await;
    let what_if = operator
        .call("route.what_if", &json!({"consumer":"mason","iqs":[0]}))
        .await;
    assert_eq!(what_if.0, FrameType::Response, "{what_if:?}");
    assert!(
        what_if.1.to_string().contains("subscription_reserve"),
        "{what_if:?}"
    );

    let caller = harness.bind(reserved(OWNER), None).await;
    for variant in ["xhigh", "max"] {
        let pin = format!("synthetic/model:{variant}");
        let reply = caller
            .call(
                "route.select",
                &json!({"consumer":"mason","pin":pin,
            "requirements":{"iq":100},"hold_wait_max_s":0}),
            )
            .await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
        let refusal: cortexkit_role_routing::errors::Refusal =
            serde_json::from_value(reply.1).unwrap();
        assert_eq!(refusal.code.as_str(), "pin_unavailable", "{refusal:?}");
        assert_eq!(refusal.detail["reason"], "quota_exhausted", "{refusal:?}");
        assert_eq!(refusal.wait_until_ms(), Some(expected_retry), "{refusal:?}");
        assert!(
            refusal.message.contains("subscription_reserve"),
            "{refusal:?}"
        );
    }
}

#[tokio::test]
async fn pin_availability_timed_account_block_names_its_recovery() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    let mut listing = credentials.listed_credentials().to_vec();
    listing.retain(|row| row.id == "oauth:test");
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "only-subscription-account".into(),
            credentials: listing,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    let reset = NOW + 600_000;
    oauth_weekly_reading_at(10.0, reset as i64, NOW as i64);
    let caller = harness.bind(reserved(OWNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"mason",
        "requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{selected:?}");
    oauth_weekly_reading_at(100.0, reset as i64, NOW as i64);
    let reported = caller
        .call(
            "route.report",
            &json!({
                "decision_id":selected.1["result"]["decision_id"], "outcome":"refused",
                "credential_id":"oauth:test", "runner_code":"billing_exhausted"
            }),
        )
        .await;
    assert_eq!(reported.0, FrameType::Response, "{reported:?}");
    let blocks = harness
        .store()
        .account_transaction(|tx| tx.active_pool_fences("synthetic"))
        .unwrap();
    assert_eq!(blocks.len(), 1);
    let block: Fence = serde_json::from_str(&blocks[0].evidence.fence_json).unwrap();
    assert_eq!(block.reset, ResetTime::At(reset as i64));
    let reply = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","pin":"synthetic/model",
        "requirements":{"iq":30},"hold_wait_max_s":0}),
        )
        .await;
    assert_eq!(reply.0, FrameType::Error, "{reply:?}");
    let refusal: cortexkit_role_routing::errors::Refusal = serde_json::from_value(reply.1).unwrap();
    assert_eq!(refusal.code.as_str(), "pin_unavailable", "{refusal:?}");
    assert_eq!(refusal.detail["reason"], "quota_exhausted", "{refusal:?}");
    assert_eq!(refusal.wait_until_ms(), Some(reset), "{refusal:?}");
}

#[tokio::test]
async fn subscription_billing_refusal_keeps_reset_evidence_but_stale_readings_allow_a_probe() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let reset = NOW as i64 + 7 * 24 * 60 * 60_000;
    oauth_weekly_reading_at(10.0, reset, NOW as i64);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let decision = selected.1["result"]["decision_id"]
        .as_str()
        .unwrap()
        .to_owned();
    oauth_weekly_reading_at(100.0, reset, NOW as i64);
    let reported = caller
        .call(
            "route.report",
            &json!({"decision_id":decision,"outcome":"refused","credential_id":"oauth:test","runner_code":"billing_exhausted"}),
        )
        .await;
    assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
    let blocks = harness
        .store()
        .account_transaction(|tx| tx.active_pool_fences("synthetic"))
        .unwrap();
    assert_eq!(
        blocks.len(),
        1,
        "the refusal creates an ordinary quota block"
    );
    let fence: striatum_core::accounts::Fence =
        serde_json::from_str(&blocks[0].evidence.fence_json).unwrap();
    assert_eq!(fence.reset, striatum_core::accounts::ResetTime::At(reset));
    let blocked = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "quota-held"),
        )
        .await;
    assert_eq!(blocked.0, FrameType::Error, "{}", blocked.1);
    assert_eq!(blocked.1["code"], "account_pool_exhausted", "{}", blocked.1);
    assert_eq!(
        blocked.1["detail"]["reset_at_ms"],
        json!(NOW as i64 + 900_000),
        "the reset is evidence, while the next check is fifteen minutes away: {}",
        blocked.1
    );
    let probe_at = NOW as i64 + crate::credential_fences::FENCE_PROBE_RECHECK_MS + 1;
    let _later = crate::InjectedClock::set(probe_at as u64);
    // The original reading is stale now; a distant reset cannot veto a trial.
    let still = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "quota-held-after-probe-interval"),
        )
        .await;
    assert_credential(&still, "oauth:test");
    let again = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "quota-probe-already-taken"),
        )
        .await;
    assert_eq!(again.0, FrameType::Error, "{}", again.1);
    assert_eq!(again.1["detail"]["reset_at_ms"], probe_at + 900_000);
    let evidence =
        crate::diagnostics_findings::Evidence::gather(harness.store(), probe_at, 60_000).unwrap();
    let diagnostics = crate::diagnostics_findings::report(&evidence, probe_at, 60_000);
    let finding = diagnostics["findings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|finding| finding["check"] == "credential_fences")
        .unwrap();
    assert!(
        finding["summary"]
            .as_str()
            .unwrap()
            .contains(&crate::account_selection::rfc3339(reset)),
        "{finding}"
    );
    assert!(
        finding["summary"].as_str().unwrap().contains("quota-held"),
        "{finding}"
    );
    assert!(
        !finding["hint"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("top up"),
        "{finding}"
    );
}

#[tokio::test]
async fn subscription_quota_hold_lifts_on_fresh_headroom_reading() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let reset = NOW as i64 + 3_600_000;
    oauth_weekly_reading_at(10.0, reset, NOW as i64);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let decision = selected.1["result"]["decision_id"]
        .as_str()
        .unwrap()
        .to_owned();
    oauth_weekly_reading_at(100.0, reset, NOW as i64);
    let reported = caller
        .call(
            "route.report",
            &json!({"decision_id":decision,"outcome":"refused","credential_id":"oauth:test","runner_code":"billing_exhausted"}),
        )
        .await;
    assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
    let blocked = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "before-headroom"),
        )
        .await;
    assert_eq!(blocked.1["code"], "account_pool_exhausted", "{}", blocked.1);
    oauth_weekly_reading_at(10.0, reset, NOW as i64 + 60_000);
    let _later = crate::InjectedClock::set(NOW + 60_000);
    let admitted = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "after-headroom"),
        )
        .await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    assert_credential(&admitted, "oauth:test");
}

#[tokio::test]
async fn credential_quota_reconciliation_requires_every_recorded_recovery_pool() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    let reset = NOW as i64 + 3_600_000;
    oauth_weekly_reading_at(10.0, reset, NOW as i64);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    oauth_weekly_reading_at(100.0, reset, NOW as i64);
    for reading in crate::ACCOUNT_QUOTA
        .get()
        .unwrap()
        .lock()
        .unwrap()
        .readings
        .readings
        .values_mut()
    {
        reading.entry["usage"]["secondary"] = json!({"usedPercent":100.0,"windowMinutes":300,
            "resetsAt":crate::account_selection::rfc3339(reset)});
    }
    let reported = caller
        .call(
            "route.report",
            &json!({"decision_id":selected.1["result"]["decision_id"],
        "outcome":"refused","credential_id":"oauth:test","runner_code":"billing_exhausted"}),
        )
        .await;
    assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
    let held = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "two-pool-held"),
        )
        .await;
    assert_eq!(held.1["code"], "account_pool_exhausted", "{}", held.1);
    assert!(harness
        .store()
        .account_read(|tx| tx.active_pool_fences("synthetic"))
        .unwrap()
        .iter()
        .any(|block| block.recovery_set.len() == 2));
    oauth_weekly_reading_at(0.0, reset, NOW as i64 + 60_000);
    let _later = crate::InjectedClock::set(NOW + 60_000);
    let partial = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "partial-pool-held"),
        )
        .await;
    assert_eq!(
        partial.1["code"], "account_pool_exhausted",
        "missing pool cannot heal a linked hold: {}",
        partial.1
    );
    assert!(harness
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .iter()
        .any(|f| f.credential_id == "oauth:test"));
    let probe_at = NOW + 16 * 60_000;
    oauth_weekly_reading_at(0.0, reset, probe_at as i64);
    let _probe_clock = crate::InjectedClock::set(probe_at);
    let rows = credentials.listed_credentials().to_vec();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "two-pool-probe".into(),
            credentials: rows,
            ..Default::default()
        },
        probe_at,
    );
    harness.set_credentials(credentials);
    let probe = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "missing-pool-probe"),
        )
        .await;
    assert_eq!(
        probe.0,
        FrameType::Response,
        "fresh partial headroom cannot suppress the probe: {}",
        probe.1
    );
}

#[tokio::test]
async fn pay_per_token_credential_without_reading_keeps_the_one_hour_probe() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    oauth_weekly_reading_at(10.0, NOW as i64 + 3_600_000, NOW as i64);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let caller = harness.bind(reserved(OWNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"codemode","requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let reported = caller
        .call(
            "route.report",
            &json!({
                "decision_id": selected.1["result"]["decision_id"], "outcome":"refused",
                "credential_id":"apikey:test", "runner_code":"billing_exhausted",
            }),
        )
        .await;
    assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
    let mut request = account_request("synthetic", "apikey-probe");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    let blocked = runner.call("route.account_for", &request).await;
    assert_eq!(blocked.0, FrameType::Error, "{}", blocked.1);
    assert_eq!(blocked.1["code"], "account_pool_exhausted", "{}", blocked.1);
    let _later = crate::InjectedClock::set(
        NOW + crate::credential_fences::FENCE_PROBE_RECHECK_MS as u64 + 1,
    );
    let admitted = runner.call("route.account_for", &request).await;
    assert_credential(&admitted, "apikey:test");
    assert_eq!(
        admitted.1["result"]["reason"]["code"], "credential_fence_probe",
        "{}",
        admitted.1
    );
}

#[tokio::test]
async fn subscription_hold_diagnostics_names_reset_without_a_top_up_hint() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    let reset = NOW as i64 + 7 * 24 * 60 * 60_000;
    oauth_weekly_reading_at(100.0, reset, NOW as i64);
    let shared = Arc::new(std::sync::Mutex::new(credentials));
    crate::credential_fences::record(
        harness.store(),
        &shared,
        &CredentialRefusal {
            provider_id: "synthetic".into(),
            credential_id: "oauth:test".into(),
            code: "billing_exhausted".into(),
        },
        NOW as i64,
    )
    .unwrap();
    let runner = harness.bind(reserved(RUNNER), None).await;
    let _ = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "diagnostic-hold"),
        )
        .await;
    let evidence =
        crate::diagnostics_findings::Evidence::gather(harness.store(), NOW as i64, 60_000).unwrap();
    let report = crate::diagnostics_findings::report(&evidence, NOW as i64, 60_000);
    let finding = report["findings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|finding| finding["check"] == "credential_fences")
        .unwrap();
    assert!(
        finding["summary"]
            .as_str()
            .unwrap()
            .contains(&crate::account_selection::rfc3339(reset)),
        "{finding}"
    );
    assert!(
        finding["hint"]
            .as_str()
            .unwrap()
            .contains(&crate::account_selection::rfc3339(reset)),
        "{finding}"
    );
    assert!(
        !finding["hint"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("top up"),
        "{finding}"
    );
    assert!(
        !finding["hint"].as_str().unwrap().contains("paid balance"),
        "{finding}"
    );
}

#[tokio::test]
async fn repeated_subscription_refusals_keep_reset_evidence_and_require_newer_headroom() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    let reset = NOW as i64 + 7_200_000;
    let shared = Arc::new(std::sync::Mutex::new(credentials));
    let refusal = CredentialRefusal {
        provider_id: "synthetic".into(),
        credential_id: "oauth:test".into(),
        code: "billing_exhausted".into(),
    };
    oauth_weekly_reading_at(100.0, reset, NOW as i64);
    crate::credential_fences::record(harness.store(), &shared, &refusal, NOW as i64).unwrap();
    let runner = harness.bind(reserved(RUNNER), None).await;
    let first = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "first-refusal"),
        )
        .await;
    assert_eq!(
        first.1["detail"]["reset_at_ms"],
        json!(NOW as i64 + 900_000),
        "{}",
        first.1
    );

    let _later = crate::InjectedClock::set(NOW + 60_000);
    crate::credential_fences::record(harness.store(), &shared, &refusal, NOW as i64 + 60_000)
        .unwrap();
    let second = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "second-refusal"),
        )
        .await;
    assert_eq!(
        second.1["detail"]["reset_at_ms"],
        json!(NOW as i64 + 60_000 + 900_000),
        "{}",
        second.1
    );
    oauth_weekly_reading_at(10.0, reset, NOW as i64 + 30_000);
    let older = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "older-headroom"),
        )
        .await;
    assert_eq!(older.0, FrameType::Error, "{}", older.1);
    assert_eq!(
        older.1["detail"]["reset_at_ms"],
        json!(NOW as i64 + 60_000 + 900_000),
        "{}",
        older.1
    );

    let _latest = crate::InjectedClock::set(NOW + 90_000);
    oauth_weekly_reading_at(10.0, reset, NOW as i64 + 90_000);
    let admitted = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "newer-headroom"),
        )
        .await;
    assert_credential(&admitted, "oauth:test");
    assert!(harness
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .is_empty());
    assert!(harness
        .store()
        .account_transaction(|tx| tx.active_pool_fences("synthetic"))
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn persisted_subscription_billing_fence_rechecks_without_expiring_at_its_reset() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    // Keep independent runner/vault facts fresh while exercising the reset's
    // extra quota check before the ordinary fifteen-minute recheck.
    let reset = NOW as i64 + 2 * 60_000;
    let shared = Arc::new(std::sync::Mutex::new(credentials));
    crate::credential_fences::record(
        harness.store(),
        &shared,
        &CredentialRefusal {
            provider_id: "synthetic".into(),
            credential_id: "oauth:test".into(),
            code: "billing_exhausted".into(),
        },
        NOW as i64,
    )
    .unwrap();
    oauth_weekly_reading_at(99.5, reset, NOW as i64);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let blocked = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "converted-hold"),
        )
        .await;
    assert_eq!(blocked.0, FrameType::Error, "{}", blocked.1);
    assert_eq!(
        blocked.1["detail"]["reset_at_ms"],
        json!(reset),
        "{}",
        blocked.1
    );
    let blocks = harness
        .store()
        .account_transaction(|tx| tx.active_pool_fences("synthetic"))
        .unwrap();
    assert_eq!(
        blocks.len(),
        1,
        "persisted billing refusal becomes a quota block"
    );

    let _later = crate::InjectedClock::set(reset as u64 + 1);
    // A newer refusal in a different input band must not veto the account's
    // extra reset check for ordinary small requests, or consume that band's trial.
    harness
        .store()
        .record_account_size_refusal(&striatum_store::AccountSizeRefusal {
            provider: "synthetic".into(),
            credential_id: "oauth:test".into(),
            model: "model".into(),
            size_class: 17,
            expires_at: reset + 3_600_000,
            observed_at: reset + 1,
        })
        .unwrap();
    // The reset permits a check, not deletion of quota evidence.
    let after = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "after-known-reset"),
        )
        .await;
    assert_eq!(after.0, FrameType::Response, "{}", after.1);
    let duplicate = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "second-reset-probe"),
        )
        .await;
    assert_eq!(
        duplicate.0,
        FrameType::Error,
        "a reset must admit only one probe: {}",
        duplicate.1
    );
    assert!(!harness
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .is_empty());
    assert!(
        harness
            .store()
            .fence_rechecks()
            .unwrap()
            .iter()
            .any(|origin| origin.kind == "size" && origin.last_claimed_at.is_none()),
        "the unrelated size fence keeps its own probe"
    );
    assert!(!harness
        .store()
        .account_transaction(|tx| tx.active_pool_fences("synthetic"))
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn quota_route_probe_claim_is_shared_by_credentials_on_the_same_account() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    let mut rows = credentials.listed_credentials().to_vec();
    let mut alias = rows
        .iter()
        .find(|row| row.id == "oauth:test")
        .unwrap()
        .clone();
    alias.id = "oauth:alias".into();
    rows.push(alias);
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "same-account-aliases".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    harness
        .store()
        .record_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "synthetic".into(),
            credential_id: "oauth:test".into(),
            model: "model".into(),
            until_ms: 0,
            decision_id: "old-refusal".into(),
            recorded_at: NOW as i64 - 900_000,
        })
        .unwrap();
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut request = account_request("synthetic", "alias-probe");
    request["candidates"] = json!({"view_digest":"alias-only","accounts":[{"account_id":"oauth-account","credential_id":"oauth:alias"}]});
    assert_credential(
        &runner.call("route.account_for", &request).await,
        "oauth:alias",
    );
    request["admission_id"] = json!("second-alias-probe");
    request["conversation_key"] = json!("second-alias-probe");
    let blocked = runner.call("route.account_for", &request).await;
    assert_eq!(
        blocked.0,
        FrameType::Error,
        "a sibling credential must not avoid the account's atomic claim: {}",
        blocked.1
    );
    let _next = crate::InjectedClock::set(NOW + 1);
    oauth_weekly_reading_at(0.0, NOW as i64 + 86_400_000, NOW as i64 + 1);
    request["admission_id"] = json!("headroom-alias");
    assert_credential(
        &runner.call("route.account_for", &request).await,
        "oauth:alias",
    );
    assert!(harness
        .store()
        .account_route_fences(NOW as i64 + 1)
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn a_billing_refusal_converted_without_a_reset_keeps_its_fifteen_minute_check_when_readings_disappear(
) {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    crate::credential_fences::record(
        harness.store(),
        &Arc::new(std::sync::Mutex::new(credentials)),
        &CredentialRefusal {
            provider_id: "synthetic".into(),
            credential_id: "oauth:test".into(),
            code: "billing_exhausted".into(),
        },
        NOW as i64,
    )
    .unwrap();
    let mut quota = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_provider_reply(&mut quota,"test-source",&serde_json::to_vec(&json!({
        "result":[{"provider":"test-source","apiProvider":"synthetic","account":"oauth-account","source":"vault",
            "fetchedAt":crate::account_selection::rfc3339(NOW as i64),"usage":{"primary":{"usedPercent":99.5,"windowMinutes":10080}}}],
        "completeProviders":["test-source"]})).unwrap(),NOW as i64).unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = quota;
    let runner = harness.bind(reserved(RUNNER), None).await;
    assert_eq!(
        runner
            .call(
                "route.account_for",
                &account_request("synthetic", "near-wall-without-reset")
            )
            .await
            .0,
        FrameType::Error
    );
    assert!(harness
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .any(|origin| origin.kind == "credential" && origin.quota));
    let holds = harness
        .store()
        .account_read(|tx| tx.active_pool_fences("synthetic"))
        .unwrap();
    assert!(
        !holds.is_empty(),
        "quota holds retain their recovery pools even without a reset"
    );
    for hold in holds {
        let fence: striatum_core::accounts::Fence =
            serde_json::from_str(&hold.evidence.fence_json).unwrap();
        assert_eq!(
            fence.reset,
            striatum_core::accounts::ResetTime::Missing,
            "a recovery hold must not invent a fixed reset deadline"
        );
    }
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = Default::default();
    let _later = crate::InjectedClock::set(NOW + 900_000);
    assert_credential(
        &runner
            .call(
                "route.account_for",
                &account_request("synthetic", "missing-reading-probe"),
            )
            .await,
        "oauth:test",
    );
    assert!(harness
        .store()
        .fence_rechecks()
        .unwrap()
        .iter()
        .any(|origin| origin.kind == "credential" && origin.quota));
    assert_eq!(
        runner
            .call(
                "route.account_for",
                &account_request("synthetic", "second-missing-reading-probe")
            )
            .await
            .0,
        FrameType::Error
    );
}

#[tokio::test]
async fn run_and_reading_resets_do_not_postpone_checks_and_headroom_lifts_both() {
    for (run_offset, reading_offset) in [(3_600_000, 7_200_000), (7_200_000, 3_600_000)] {
        let _clock = crate::InjectedClock::set(NOW);
        let (harness, _) = setup("synthetic", false);
        let reading_reset = NOW as i64 + reading_offset;
        oauth_weekly_reading_at(10.0, reading_reset, NOW as i64);
        let caller = harness.bind(reserved(OWNER), None).await;
        let runner = harness.bind(reserved(RUNNER), None).await;
        let selected = caller
            .call(
                "route.select",
                &json!({"consumer":"mason","requirements":{"iq":30}}),
            )
            .await;
        assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
        oauth_weekly_reading_at(100.0, reading_reset, NOW as i64);
        let reported = caller.call("route.report", &json!({
            "decision_id":selected.1["result"]["decision_id"], "outcome":"failed", "credential_id":"oauth:test",
            "runner_code":"account_limit_exhausted", "reset_at_ms":NOW as i64 + run_offset,
        })).await;
        assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
        let blocked = runner
            .call(
                "route.account_for",
                &account_request("synthetic", "later-known-reset"),
            )
            .await;
        assert_eq!(blocked.1["code"], "account_pool_exhausted", "{}", blocked.1);
        assert_eq!(
            blocked.1["detail"]["reset_at_ms"],
            json!(NOW as i64 + 900_000),
            "{}",
            blocked.1
        );
        oauth_weekly_reading_at(10.0, reading_reset, NOW as i64 + 60_000);
        let _later = crate::InjectedClock::set(NOW + 60_000);
        let admitted = runner
            .call(
                "route.account_for",
                &account_request("synthetic", "headroom-after-run"),
            )
            .await;
        assert_credential(&admitted, "oauth:test");
        assert!(harness
            .store()
            .account_route_fences(NOW as i64 + 60_000)
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn client_attestation_denied_fences_the_serving_credential() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let reset = NOW as i64 + 7 * 24 * 60 * 60_000;
    oauth_weekly_reading_at(10.0, reset, NOW as i64);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let reported = caller.call("route.report", &json!({
        "decision_id":selected.1["result"]["decision_id"], "outcome":"refused", "credential_id":"oauth:test",
        "runner_code":"client_attestation_denied",
    })).await;
    assert_eq!(reported.0, FrameType::Response, "{}", reported.1);
    let fences = harness.store().credential_entitlement_fences().unwrap();
    assert_eq!(fences.len(), 1, "{fences:?}");
    assert_eq!(fences[0].code, "client_attestation_denied");
    let blocked = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "attestation-fenced"),
        )
        .await;
    assert_eq!(blocked.0, FrameType::Error, "{}", blocked.1);
    assert_eq!(blocked.1["code"], "account_pool_exhausted", "{}", blocked.1);

    let _newer = crate::InjectedClock::set(NOW + 60_000);
    oauth_weekly_reading_at(10.0, reset, NOW as i64 + 60_000);
    let still_fenced = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "newer-attestation-headroom"),
        )
        .await;
    assert_eq!(still_fenced.0, FrameType::Error, "{}", still_fenced.1);
    let fences = harness.store().credential_entitlement_fences().unwrap();
    assert_eq!(fences.len(), 1, "headroom does not establish entitlement");
    assert_eq!(fences[0].observed_at_ms, NOW as i64);

    let probe_at = NOW + crate::credential_fences::FENCE_PROBE_RECHECK_MS as u64 + 1;
    let _probe_time = crate::InjectedClock::set(probe_at);
    oauth_weekly_reading_at(10.0, reset, probe_at as i64);
    let request = account_request("synthetic", "attestation-probe");
    let admitted = runner.call("route.account_for", &request).await;
    assert_credential(&admitted, "oauth:test");
    assert_eq!(
        admitted.1["result"]["reason"]["code"], "credential_fence_probe",
        "{}",
        admitted.1
    );
    assert_eq!(
        runner.call("route.account_for", &request).await,
        admitted,
        "replay does not spend another probe"
    );
    let second = runner
        .call(
            "route.account_for",
            &account_request("synthetic", "second-attestation-probe"),
        )
        .await;
    assert_eq!(
        second.0,
        FrameType::Error,
        "only one probe is admitted: {}",
        second.1
    );
    assert_eq!(
        harness.store().credential_entitlement_fences().unwrap()[0].observed_at_ms,
        NOW as i64
    );
}

#[tokio::test]
async fn entitlement_refusals_never_convert_to_quota_blocks() {
    for code in ["ENTITLEMENT_ERROR", "client_attestation_denied"] {
        let _clock = crate::InjectedClock::set(NOW);
        let (harness, credentials) = setup("synthetic", false);
        let reset = NOW as i64 + 3_600_000;
        oauth_weekly_reading_at(100.0, reset, NOW as i64);
        let shared = Arc::new(std::sync::Mutex::new(credentials));
        crate::credential_fences::record(
            harness.store(),
            &shared,
            &CredentialRefusal {
                provider_id: "synthetic".into(),
                credential_id: "oauth:test".into(),
                code: code.into(),
            },
            NOW as i64,
        )
        .unwrap();
        let runner = harness.bind(reserved(RUNNER), None).await;
        let blocked = runner
            .call(
                "route.account_for",
                &account_request("synthetic", "entitlement-not-quota"),
            )
            .await;
        assert_eq!(blocked.0, FrameType::Error, "{}", blocked.1);
        assert_eq!(
            blocked.1["detail"]["reset_at_ms"],
            json!(NOW + 3_600_000),
            "entitlement names its probe window, not the quota reset: {}",
            blocked.1
        );
        assert!(
            harness
                .store()
                .account_transaction(|tx| tx.active_pool_fences("synthetic"))
                .unwrap()
                .is_empty(),
            "{code} is not quota exhaustion"
        );
        let _later = crate::InjectedClock::set(reset as u64 + 1);
        oauth_weekly_reading_at(10.0, reset + 3_600_000, reset + 1);
        let blocked = runner
            .call(
                "route.account_for",
                &account_request("synthetic", "entitlement-after-quota-reset"),
            )
            .await;
        assert_credential(&blocked, "oauth:test");
        assert_eq!(
            harness.store().credential_entitlement_fences().unwrap()[0].code,
            code
        );
        assert_eq!(
            runner
                .call(
                    "route.account_for",
                    &account_request("synthetic", "second-entitlement-probe")
                )
                .await
                .0,
            FrameType::Error,
            "headroom did not clear entitlement evidence; only one timed probe passed"
        );
    }
}

#[tokio::test]
async fn entitlement_finding_with_quota_reading_names_the_probe_and_hand_lift() {
    for used_percent in [10.0, 100.0] {
        let _clock = crate::InjectedClock::set(NOW);
        let (harness, credentials) = setup("synthetic", false);
        oauth_weekly_reading_at(used_percent, NOW as i64 + 3_600_000, NOW as i64);
        let shared = Arc::new(std::sync::Mutex::new(credentials));
        crate::credential_fences::record(
            harness.store(),
            &shared,
            &CredentialRefusal {
                provider_id: "synthetic".into(),
                credential_id: "oauth:test".into(),
                code: "client_attestation_denied".into(),
            },
            NOW as i64,
        )
        .unwrap();
        // Give diagnostics the same vault-to-account join used by selection.
        crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap().roster =
            Some(crate::account_quota::VaultRoster {
                view: "test-view".into(),
                fetched_at_ms: NOW as i64,
                identities: vec![crate::account_quota::RosterIdentity {
                    refresh_adapter: "test-source".into(),
                    account_id: "oauth-account".into(),
                    credential_ids: vec!["oauth:test".into()],
                    active_credential_ids: vec!["oauth:test".into()],
                    first_seen_ms: NOW as i64,
                    plan_tier: None,
                }],
                grants: vec![],
            });
        let evidence =
            crate::diagnostics_findings::Evidence::gather(harness.store(), NOW as i64, 60_000)
                .unwrap();
        let report = crate::diagnostics_findings::report(&evidence, NOW as i64, 60_000);
        let finding = report["findings"]
            .as_array()
            .unwrap()
            .iter()
            .find(|finding| finding["check"] == "credential_fences")
            .unwrap();
        let hint = finding["hint"].as_str().unwrap();
        assert!(hint.contains("every 1h"), "{hint}");
        assert!(
            hint.contains("route.clear_credential_fences {\"credentialId\":\"oauth:test\"}"),
            "{hint}"
        );
        assert!(!hint.contains("No timed probe"), "{hint}");
        assert!(
            !finding["summary"].as_str().unwrap().contains("quota-held"),
            "{finding}"
        );
    }
}

fn backoff_fixture() -> Arc<Harness> {
    let (harness, mut credentials) = setup("synthetic", false);
    let facts: Vec<_> = ["model", "second"]
        .into_iter()
        .map(|model| {
            let mut fact = crate::tests::fact("synthetic", model);
            fact.auth_methods = Some(vec!["oauth".into()]);
            fact.billing_plane = Some(BillingPlane {
                provider_id: "synthetic".into(),
                auth_method: "oauth".into(),
                kind: "subscription".into(),
                selection_reason: "test".into(),
                rates: [("rate.output".into(), BillingRate::Priced { rate: 1.0 })]
                    .into_iter()
                    .collect(),
            });
            fact
        })
        .collect();
    harness.install_facts(&facts);
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "two-accounts".into(),
            credentials: ["first", "other"]
                .into_iter()
                .map(|id| ListedCredential {
                    id: format!("oauth:{id}"),
                    account_id: Some(id.into()),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec!["synthetic".into()],
                    serves: vec!["synthetic".into()],
                    credential_type: "oauth".into(),
                    auth_method: Some("oauth".into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &facts, NOW as i64, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts),
    );
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"] = json!([
            {"module":"broca","via_vault":true},{"module":"thalamus-harness","via_vault":true},
            {"module":"thalamus-pi","via_vault":true},{"module":RUNNER,"via_vault":true}
        ]);
    });
    harness
}

fn chosen_account(id: &str, model: &str, account: &str) -> Value {
    json!({"provider":"synthetic","model":model,"admission_id":id,"conversation_key":id,
        "candidates":{"view_digest":"two-accounts", "accounts":[{"account_id":account,"credential_id":format!("oauth:{account}")}]},
        "hold_wait_max_s":0})
}

#[tokio::test]
async fn observe_backoff_fences_every_model_and_runner_and_survives_restart() {
    let _clock = crate::InjectedClock::set(NOW);
    for client in ["broca", "thalamus-harness", "thalamus-pi"] {
        let harness = backoff_fixture();
        let runner = harness.bind(reserved(client), None).await;
        assert_credential(
            &runner
                .call(
                    "route.account_for",
                    &chosen_account("admitted", "model", "first"),
                )
                .await,
            "oauth:first",
        );
        let missing = runner
            .call(
                "route.account_observe",
                &json!({"kind":"transient_backoff","event_id":"missing","admission_id":"admitted"}),
            )
            .await;
        assert_eq!(
            missing.1["code"], "invalid_request",
            "{client}: {}",
            missing.1
        );
        assert_eq!(missing.1["detail"]["field"], "retry_at_ms");
        let body = json!({"kind":"transient_backoff","event_id":"retry","admission_id":"admitted", "retry_at_ms":NOW+30_000,
            "served_account_id":"first", "provider_code":"overloaded"});
        let observed = runner.call("route.account_observe", &body).await;
        assert_eq!(observed.0, FrameType::Response, "{client}: {}", observed.1);
        let mut duplicate = body.clone();
        duplicate["retry_at_ms"] = json!(NOW + 90_000);
        assert_eq!(
            runner.call("route.account_observe", &duplicate).await.0,
            FrameType::Response
        );
        assert_eq!(
            harness
                .store()
                .account_read(|tx| tx.observation_backoffs(NOW as i64))
                .unwrap(),
            vec![("synthetic".into(), "first".into(), (NOW + 30_000) as i64)]
        );
        let other_runner = harness.bind(reserved(RUNNER), None).await;
        let blocked = other_runner
            .call(
                "route.account_for",
                &chosen_account("blocked", "second", "first"),
            )
            .await;
        assert_eq!(blocked.0, FrameType::Error, "{client}: {}", blocked.1);
        assert_credential(
            &other_runner
                .call(
                    "route.account_for",
                    &chosen_account("unaffected", "second", "other"),
                )
                .await,
            "oauth:other",
        );
        drop(runner);
        drop(other_runner);
        let harness = super::conformance_tests::upgrade_harness(harness);
        let runner = harness.bind(reserved(client), None).await;
        assert_eq!(
            runner
                .call(
                    "route.account_for",
                    &chosen_account("restart", "second", "first")
                )
                .await
                .0,
            FrameType::Error
        );
        let _later = crate::InjectedClock::set(NOW + 30_001);
        assert_credential(
            &runner
                .call(
                    "route.account_for",
                    &chosen_account("after-reset", "second", "first"),
                )
                .await,
            "oauth:first",
        );
    }
}

#[tokio::test]
async fn decision_owner_backoff_requires_the_admission_to_have_carried_its_decision() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = backoff_fixture();
    let owner = harness.bind(reserved(OWNER), None).await;
    let selected = owner
        .call(
            "route.select",
            &json!({"consumer":"mason","pin":"synthetic/model","requirements":{"iq":30}}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let decision = selected.1["result"]["decision_id"].clone();
    let runner_id = selected.1["result"]["runner"]["module"].as_str().unwrap();
    let runner = harness.bind(reserved(runner_id), None).await;
    let mut request = chosen_account("carried", "model", "first");
    request["decision_id"] = decision.clone();
    assert_credential(
        &runner.call("route.account_for", &request).await,
        "oauth:first",
    );
    assert_credential(
        &runner
            .call(
                "route.account_for",
                &chosen_account("bare", "model", "first"),
            )
            .await,
        "oauth:first",
    );
    let body = json!({"event_id":"owner-backoff","kind":"transient_backoff","decision_id":decision,
        "admission_id":"carried","retry_at_ms":NOW+u64::MAX/2});
    for patch in [
        json!({"admission_id":"bare"}),
        json!({"decision_id":"foreign"}),
        json!({"kind":"run_finished"}),
    ] {
        let mut denied = body.clone();
        for (key, value) in patch.as_object().unwrap() {
            denied[key] = value.clone();
        }
        if denied["kind"] == "run_finished" {
            denied.as_object_mut().unwrap().remove("retry_at_ms");
        }
        let reply = owner.call("route.account_observe", &denied).await;
        assert_eq!(reply.1["code"], "account_not_authorized", "{}", reply.1);
    }
    assert_eq!(
        owner.call("route.account_observe", &body).await.0,
        FrameType::Response
    );
    assert_eq!(
        harness
            .store()
            .account_read(|tx| tx.observation_backoffs(NOW as i64))
            .unwrap()[0]
            .2,
        (NOW + 60 * 60_000) as i64
    );
}

#[tokio::test]
async fn runner_backoff_can_name_a_served_account_without_an_admission() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = backoff_fixture();
    let runner = harness.bind(reserved(RUNNER), None).await;
    let reply = runner
        .call(
            "route.account_observe",
            &json!({"kind":"transient_backoff","event_id":"served-only",
        "served_account_id":"first","retry_at_ms":NOW+10_000}),
        )
        .await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(
        harness
            .store()
            .account_read(|tx| tx.observation_backoffs(NOW as i64))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        runner
            .call(
                "route.account_for",
                &chosen_account("blocked-served", "second", "first")
            )
            .await
            .0,
        FrameType::Error
    );
}

#[tokio::test]
async fn select_hold_threshold_is_independent_of_the_runner_wait_cap() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    exhaust_oauth_until(&harness, "synthetic", (NOW + 60_000) as i64);
    let owner = harness.bind(reserved(OWNER), None).await;
    let body = json!({"consumer":"mason","pin":"synthetic/model","requirements":{"iq":30},
        "hold_ok":true,"hold_wait_max_s":120});
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["accounts"]["max_hold"] = json!("120s");
        config["providers"]["synthetic"]["accounts"]["hold_when_reset_within"] = json!("30s");
    });
    let refused = owner.call("route.select", &body).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    harness.edit_config(|config| {
        config["providers"]["synthetic"]["accounts"]["hold_when_reset_within"] = json!("120s");
    });
    let held = owner.call("route.select", &body).await;
    assert_eq!(held.0, FrameType::Response, "{}", held.1);
    assert_eq!(held.1["result"]["pin_outcome"], "pinned");
}

fn codex_pool(used: [f64; 3]) -> Arc<Harness> {
    let (harness, mut credentials) = setup("openai", false);
    let mut luna = crate::tests::fact("openai", "gpt-6-luna");
    luna.auth_methods = Some(vec!["oauth".into()]);
    luna.billing_plane = Some(BillingPlane {
        provider_id: "openai".into(),
        auth_method: "oauth".into(),
        kind: "subscription".into(),
        selection_reason: "test subscription".into(),
        rates: [("rate.output".into(), BillingRate::Priced { rate: 1.0 })]
            .into_iter()
            .collect(),
    });
    let mut fallback = luna.clone();
    fallback.provider_id = "fallback".into();
    fallback.model_id = "adequate".into();
    fallback.auth_methods = Some(vec!["apikey".into()]);
    let plane = fallback.billing_plane.as_mut().unwrap();
    plane.provider_id = "fallback".into();
    plane.auth_method = "apikey".into();
    plane.kind = "upstream".into();
    plane
        .rates
        .insert("rate.output".into(), BillingRate::Priced { rate: 20.0 });
    let facts = vec![luna, fallback];
    harness.install_facts(&facts);
    for fact in &facts {
        catalog_fact(&harness, fact);
    }
    let mut rows: Vec<_> = (0..3)
        .map(|index| ListedCredential {
            id: format!("chatgpt:{index}"),
            account_id: Some(format!("codex-{index}")),
            state: "active".into(),
            categories: vec!["llm-provider".into()],
            provider_ids: vec!["openai".into()],
            serves: vec!["openai".into()],
            credential_type: "oauth".into(),
            auth_method: Some("oauth".into()),
            ..Default::default()
        })
        .collect();
    rows.push(ListedCredential {
        id: "apikey:fallback".into(),
        account_id: Some("fallback-account".into()),
        state: "active".into(),
        categories: vec!["llm-provider".into()],
        provider_ids: vec!["fallback".into()],
        serves: vec!["fallback".into()],
        credential_type: "apikey".into(),
        auth_method: Some("apikey".into()),
        ..Default::default()
    });
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "codex-pool".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &facts, NOW as i64, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts),
    );
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["consumers"]["sidekick"] = json!({"owner":OWNER,"mode":"demand"});
        config["runners"] = json!([{"module":RUNNER,"via_vault":true,"routes":["openai/gpt-6-luna","fallback/adequate"]}]);
    });
    let mut state = crate::account_quota::AccountQuotaState::default();
    // Codex usage here has a weekly primary window, not a five-hour
    // primary plus weekly secondary. Dates are shifted to the injected clock.
    let entries: Vec<_> = used.into_iter().enumerate().map(|(index, used)| json!({
        "provider":"codex", "apiProvider":"openai", "account":format!("codex-{index}"),
        "source":"vault", "fetchedAt":crate::account_selection::rfc3339(NOW as i64),
        "savedResets":{"availableCount":2}, "spend":[],
        "usage":{"primary":{"usedPercent":used,"windowMinutes":10080,
            "resetsAt":crate::account_selection::rfc3339(NOW as i64 + (index as i64 + 1) * 86_400_000)}}
    })).collect();
    crate::account_quota::apply_provider_reply(
        &mut state,
        "codex",
        &serde_json::to_vec(&json!({"result":entries,"completeProviders":["codex"]})).unwrap(),
        NOW as i64,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
    harness
}

async fn codex_selected_provider(used: [f64; 3], legacy: bool) -> String {
    let harness = codex_pool(used);
    let caller = harness.bind(reserved(OWNER), None).await;
    let mut request = json!({"consumer":"sidekick","requirements":{"iq":1,"eq":0}});
    if legacy {
        request["targetAgent"] = json!("explore");
        request["taskId"] = json!("codex-availability");
        request["executor"] = json!("broca");
        request["substrate"] = json!("broca");
    }
    let reply = caller.call("route.select", &request).await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    let result = &reply.1["result"];
    if legacy {
        // The last account walls the pool only once routing's 1-point reserve
        // leaves it nothing spendable; the Broca floor alone no longer does.
        if used[2] >= 99.0 {
            assert_eq!(
                result["selected"]["routeKey"], "fallback/adequate",
                "{}",
                reply.1
            );
        }
        let rows = harness
            .store()
            .get_decisions_for_task("codex-availability")
            .unwrap();
        let signals: Value =
            serde_json::from_str(rows.last().unwrap().quota_signal_json.as_deref().unwrap())
                .unwrap();
        let luna = signals["signals"]
            .as_array()
            .unwrap()
            .iter()
            .find(|signal| signal["capacityKey"] == "openai/gpt-6-luna");
        if used[2] >= 99.0 {
            assert_eq!(
                luna.unwrap()["exhaustedUntil"],
                json!((NOW + 86_400_000) as f64)
            );
        } else {
            assert!(luna.is_none_or(|signal| signal["exhaustedUntil"].is_null()));
        }
        result["selected"]["routeKey"]
            .as_str()
            .expect(&format!("{}", reply.1))
            .split('/')
            .next()
            .unwrap()
            .into()
    } else {
        result["route"]["provider"].as_str().unwrap().into()
    }
}

#[tokio::test]
async fn codex_three_exhausted_accounts_select_an_adequate_fallback() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false] {
        assert_eq!(
            codex_selected_provider([100.0, 100.0, 100.0], legacy).await,
            "fallback"
        );
    }
}

#[tokio::test]
async fn codex_accounts_below_the_broca_floor_keep_luna() {
    // Every account below the Broca floor's 5 spendable points, but the 95%
    // and 96% accounts still have quota above the reserve: sidekicks keep
    // running on them instead of losing the pool.
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false] {
        assert_eq!(
            codex_selected_provider([100.0, 95.0, 96.0], legacy).await,
            "openai"
        );
    }
}

#[tokio::test]
async fn codex_one_account_with_headroom_keeps_luna() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false] {
        assert_eq!(
            codex_selected_provider([100.0, 100.0, 10.0], legacy).await,
            "openai"
        );
    }
}

#[tokio::test]
async fn codex_reserve_limited_refusal_carries_earliest_reset() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = codex_pool([100.0, 100.0, 99.0]);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let reply = runner.call("route.account_for", &json!({
        "provider":"openai", "model":"gpt-6-luna", "conversation_key":"codex-cold", "admission_id":"codex-cold"
    })).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(
        reply.1["code"], "account_pool_reserve_limited",
        "{}",
        reply.1
    );
    assert_eq!(
        reply.1["detail"]["reset_at_ms"],
        NOW + 900_000,
        "{}",
        reply.1
    );
}

#[tokio::test]
async fn codex_reserve_held_last_account_is_unavailable_to_sidekicks() {
    let _clock = crate::InjectedClock::set(NOW);
    for legacy in [false] {
        assert_eq!(
            codex_selected_provider([100.0, 100.0, 99.0], legacy).await,
            "fallback"
        );
    }
}

#[tokio::test]
async fn codex_runtime_wall_on_last_headroom_account_reaches_selection() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = codex_pool([100.0, 100.0, 10.0]);
    harness
        .store()
        .record_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "openai".into(),
            model: "gpt-6-luna".into(),
            credential_id: "chatgpt:2".into(),
            until_ms: NOW as i64 + 600_000,
            recorded_at: NOW as i64,
            decision_id: "previous".into(),
        })
        .unwrap();
    let caller = harness.bind(reserved(OWNER), None).await;
    let reply = caller
        .call(
            "route.select",
            &json!({
                "consumer":"sidekick",
                "requirements":{"iq":1,"eq":0}
            }),
        )
        .await;
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(
        reply.1["result"]["route"]["provider"], "fallback",
        "{}",
        reply.1
    );
}

fn credit_xai_fixture() -> (Arc<Harness>, BrocaCredentials) {
    let (harness, mut credentials) = setup("xai", false);
    let mut rows = credentials.listed_credentials().to_vec();
    rows.retain(|row| row.id == "oauth:test");
    rows[0].id = "oauth:xai".into();
    rows[0].account_id = Some("xai-account".into());
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "xai-credit".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials.clone());
    credit_xai_reading(Some(0.0), NOW as i64);
    (harness, credentials)
}

fn credit_xai_reading(used: Option<f64>, at: i64) {
    let mut state = crate::account_quota::AccountQuotaState::default();
    if let Some(used) = used {
        crate::account_quota::apply_provider_reply(&mut state, "grok", &serde_json::to_vec(&json!({
            "result":[{"provider":"grok","apiProvider":"xai","account":"xai-account",
                "credentialRef":"oauth:xai","source":"vault","fetchedAt":crate::account_selection::rfc3339(at),
                "usage":{"primary":{"usedPercent":used,"windowMinutes":10080}}}],
            "completeProviders":["grok"]
        })).unwrap(), at).unwrap();
    }
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
}

#[tokio::test]
async fn xai_credit_window_is_a_billing_wall_until_one_probe_and_matching_success() {
    for (used, retry) in [
        (Some(0.0), None),
        (Some(100.0), None),
        (None, None),
        (Some(0.0), Some(45_000)),
    ] {
        let _clock = crate::InjectedClock::set(NOW);
        let (harness, mut credentials) = credit_xai_fixture();
        let owner = harness.bind(reserved(OWNER), None).await;
        let selected = owner
            .call(
                "route.select",
                &json!({"consumer":"mason","requirements":{"iq":30},"pin":"xai/model"}),
            )
            .await;
        assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
        assert_eq!(
            selected.1["result"]["route"]["provider"], "xai",
            "{}",
            selected.1
        );
        assert_eq!(
            selected.1["result"]["route"]["model"], "model",
            "{}",
            selected.1
        );
        let runner = harness
            .bind(
                reserved(selected.1["result"]["runner"]["module"].as_str().unwrap()),
                None,
            )
            .await;
        let mut request = account_request("xai", "credit-send");
        request["decision_id"] = selected.1["result"]["decision_id"].clone();
        assert_credential(
            &runner.call("route.account_for", &request).await,
            "oauth:xai",
        );

        let receipt = NOW + 600_000;
        let _refusal = crate::InjectedClock::set(receipt);
        credit_xai_reading(used, receipt as i64);
        let mut refusal = json!({"event_id":"credit-refusal","kind":"account_limit_exhausted",
            "admission_id":"credit-send","served_account_id":"xai-account","credential_id":"oauth:xai",
            "limit_window":"credit","observed_at_ms":receipt});
        if let Some(retry) = retry {
            refusal["retry_at_ms"] = json!(receipt + retry);
        }
        let observed = runner.call("route.account_observe", &refusal).await;
        assert_eq!(observed.0, FrameType::Response, "{}", observed.1);
        let deadline = receipt + retry.unwrap_or(3_600_000);
        let fences = harness
            .store()
            .non_quota_account_route_fences(receipt as i64)
            .unwrap();
        assert_eq!(
            fences.len(),
            1,
            "credit is billing even with exhausted or missing quota: {observed:?}"
        );
        assert_eq!(fences[0].until_ms, deadline as i64);
        assert!(harness
            .store()
            .account_route_fences(receipt as i64)
            .unwrap()
            .is_empty());
        assert!(harness
            .store()
            .account_read(|tx| tx.active_pool_fences("xai"))
            .unwrap()
            .is_empty());

        let _fresh = crate::InjectedClock::set(receipt + 1);
        credit_xai_reading(Some(0.0), receipt as i64 + 1);
        let blocked = runner
            .call(
                "route.account_for",
                &account_request("xai", "credit-still-held"),
            )
            .await;
        assert_eq!(
            blocked.1["code"], "account_pool_exhausted",
            "fresh zero quota cannot repay credit: {}",
            blocked.1
        );
        assert_eq!(blocked.1["detail"]["reset_at_ms"], deadline);
        let _before = crate::InjectedClock::set(deadline - 1);
        assert_eq!(
            runner
                .call(
                    "route.account_for",
                    &account_request("xai", "credit-before-probe")
                )
                .await
                .0,
            FrameType::Error
        );

        let _due = crate::InjectedClock::set(deadline);
        credit_xai_reading(Some(0.0), deadline as i64);
        let rows = credentials.listed_credentials().to_vec();
        credentials.apply_listing(
            &ScopedCredentialListing {
                view: "xai-credit".into(),
                credentials: rows,
                ..Default::default()
            },
            deadline,
        );
        harness.set_credentials(credentials.clone());
        let probe = owner
            .call(
                "route.select",
                &json!({"consumer":"mason","requirements":{"iq":30},"pin":"xai/model"}),
            )
            .await;
        assert_eq!(probe.0, FrameType::Response, "{}", probe.1);
        let reserved = runner
            .call(
                "route.account_for",
                &account_request("xai", "credit-reserved-elsewhere"),
            )
            .await;
        assert_eq!(
            reserved.0,
            FrameType::Error,
            "the selected probe is reserved: {}",
            reserved.1
        );
        assert_eq!(
            reserved.1["code"], "account_pool_exhausted",
            "a probe reservation is a timed fence, not a missing runner candidate: {}",
            reserved.1
        );
        let reserved_until = reserved.1["detail"]["reset_at_ms"].as_u64().unwrap();
        assert!(
            (deadline + 1..=deadline + 3_600_000).contains(&reserved_until),
            "{}",
            reserved.1
        );
        let mut admitted = account_request("xai", "credit-probe");
        admitted["decision_id"] = probe.1["result"]["decision_id"].clone();
        let other_probe = account_request("xai", "credit-other-probe");
        let (won, lost) = tokio::join!(
            runner.call("route.account_for", &admitted),
            runner.call("route.account_for", &other_probe)
        );
        assert_credential(&won, "oauth:xai");
        assert_eq!(lost.0, FrameType::Error, "only one probe: {}", lost.1);
        let retry_at = lost.1["detail"]["reset_at_ms"].as_u64().unwrap();
        assert!(
            (deadline + 1..=deadline + 3_600_000).contains(&retry_at),
            "{}",
            lost.1
        );
        let after_claim = runner
            .call(
                "route.account_for",
                &account_request("xai", "credit-after-claim"),
            )
            .await;
        assert_eq!(after_claim.1["detail"]["reset_at_ms"], deadline + 3_600_000);
        assert_eq!(
            harness
                .store()
                .non_quota_account_route_fences(deadline as i64)
                .unwrap()
                .len(),
            1,
            "a probe permission is not recovery"
        );
        let completed = owner
            .call(
                "route.report",
                &json!({"decision_id":probe.1["result"]["decision_id"],
            "outcome":"completed","credential_id":"oauth:xai"}),
            )
            .await;
        assert_eq!(completed.0, FrameType::Response, "{}", completed.1);
        assert!(harness
            .store()
            .non_quota_account_route_fences(deadline as i64)
            .unwrap()
            .is_empty());
        assert_credential(
            &runner
                .call(
                    "route.account_for",
                    &account_request("xai", "credit-recovered"),
                )
                .await,
            "oauth:xai",
        );
    }
}

/// One `account_for` call made straight to the handler on the calling
/// thread, with a context loaded the way the wire handler loads it.
fn admit_directly(
    harness: &Harness,
    principal: &str,
    request: Value,
    now: u64,
) -> crate::account_selection::OpReply {
    let config = harness.operator_config();
    let policy = config.allocator_policy(request["provider"].as_str().unwrap());
    let ctx = crate::account_selection::Ctx {
        store: harness.store(),
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: crate::config_overrides::AccountSelectionFlags { enabled: true },
        policy: &policy,
        now_ms: now as i64,
        classes: Some(
            crate::account_selection::ClassContext::load(
                harness.store(),
                &harness.credentials(),
                config,
                crate::ACCOUNT_QUOTA.get(),
                now as i64,
            )
            .unwrap(),
        ),
    };
    crate::account_selection::handle(
        crate::account_selection::ACCOUNT_FOR_OP,
        &serde_json::to_vec(
            &json!({"method":crate::account_selection::ACCOUNT_FOR_OP,"params":request}),
        )
        .unwrap(),
        principal,
        &ctx,
    )
}

/// Selection owns the durable probe before admission. Other admissions must
/// get a timed wait both before and after its owner's admission, never a
/// sign-in refusal merely because the probe's account is excluded.
#[tokio::test]
async fn a_decision_owned_probe_answers_other_admissions_with_a_timed_wait() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = credit_xai_fixture();
    let owner = harness.bind(reserved(OWNER), None).await;
    let select = json!({"consumer":"mason","requirements":{"iq":30},"pin":"xai/model"});
    let selected = owner.call("route.select", &select).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let runner = harness
        .bind(
            reserved(selected.1["result"]["runner"]["module"].as_str().unwrap()),
            None,
        )
        .await;
    let mut request = account_request("xai", "race-send");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    assert_credential(
        &runner.call("route.account_for", &request).await,
        "oauth:xai",
    );

    let receipt = NOW + 600_000;
    let _refusal = crate::InjectedClock::set(receipt);
    credit_xai_reading(Some(0.0), receipt as i64);
    let refusal = json!({"event_id":"race-refusal","kind":"account_limit_exhausted",
        "admission_id":"race-send","served_account_id":"xai-account","credential_id":"oauth:xai",
        "limit_window":"credit","observed_at_ms":receipt});
    let observed = runner.call("route.account_observe", &refusal).await;
    assert_eq!(observed.0, FrameType::Response, "{}", observed.1);

    // An hour after the credit refusal its route fence is due for one
    // probe, and selection durably claims it for its decision.
    let deadline = receipt + 3_600_000;
    let _due = crate::InjectedClock::set(deadline);
    credit_xai_reading(Some(0.0), deadline as i64);
    let rows = credentials.listed_credentials().to_vec();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "xai-credit".into(),
            credentials: rows,
            ..Default::default()
        },
        deadline,
    );
    harness.set_credentials(credentials.clone());
    let probe = owner.call("route.select", &select).await;
    assert_eq!(probe.0, FrameType::Response, "{}", probe.1);
    let decision = probe.1["result"]["decision_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let principal = format!("reserved:{RUNNER}");
    let mut winner = account_request("xai", "race-probe-winner");
    winner["caller"] = json!({"runner":RUNNER});
    winner["model_decision_id"] = json!(decision);
    let mut loser = account_request("xai", "race-probe-loser");
    loser["caller"] = json!({"runner":RUNNER});

    for admitted in [false, true] {
        if admitted {
            // An unrelated admission also loses while the owner has prepared
            // its roster but has not yet committed account admission.
            let raced = Arc::new(std::sync::Mutex::new(None));
            let raced_hook = raced.clone();
            let harness_hook = harness.clone();
            let principal_hook = principal.clone();
            let mut contender = loser.clone();
            contender["admission_id"] = json!("during-owner-admission");
            after_roster_prepared("race-probe-winner", move || {
                *raced_hook.lock().unwrap() = Some(admit_directly(
                    &harness_hook,
                    &principal_hook,
                    contender,
                    deadline,
                ));
            });
            match admit_directly(&harness, &principal, winner.clone(), deadline) {
                crate::account_selection::OpReply::Result(result) => {
                    assert_eq!(result["account"]["credential_id"], "oauth:xai", "{result}")
                }
                other => panic!("the probe decision's admission was refused: {other:?}"),
            }
            assert!(matches!(raced.lock().unwrap().take(),
                Some(crate::account_selection::OpReply::Error { code, detail: Some(detail), .. })
                    if code == "account_pool_exhausted" && detail["reset_at_ms"].as_u64().is_some()));
        }
        loser["admission_id"] = json!(format!("other-admission-{admitted}"));
        match admit_directly(&harness, &principal, loser.clone(), deadline) {
            crate::account_selection::OpReply::Error { code, detail, .. } => {
                let detail = detail.unwrap_or_default();
                assert_eq!(
                    code, "account_pool_exhausted",
                    "losing the probe is a wait, never an authorization failure: {detail}"
                );
                let retry_at = detail["reset_at_ms"]
                    .as_u64()
                    .unwrap_or_else(|| panic!("the wait names when to retry: {detail}"));
                assert!(
                    (deadline + 1..=deadline + 3_600_000).contains(&retry_at),
                    "{detail}"
                );
            }
            other => panic!("a second probe was admitted: {other:?}"),
        }
    }
}

/// The roster of [`a_losing_racer_on_a_mixed_roster_gets_a_timed_wait`]:
/// the credit-metered xai account, and a second account whose pooled reading
/// shows it cannot reach the routed model.
fn mixed_reach_xai_fixture() -> (Arc<Harness>, BrocaCredentials, Vec<ListedCredential>) {
    let (harness, mut credentials) = setup("xai", false);
    let mut rows = credentials.listed_credentials().to_vec();
    rows.retain(|row| row.id == "oauth:test");
    let mut unreached = rows[0].clone();
    rows[0].id = "oauth:xai".into();
    rows[0].account_id = Some("xai-account".into());
    unreached.id = "oauth:pooled".into();
    unreached.account_id = Some("pooled-account".into());
    rows.push(unreached);
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "xai-credit".into(),
            credentials: rows.clone(),
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials.clone());
    mixed_reach_xai_reading(NOW as i64);
    (harness, credentials, rows)
}

/// The xai account's credit reading with headroom, and the second account's
/// reading, whose only window is a pool the routed model does not draw on.
fn mixed_reach_xai_reading(at: i64) {
    let mut state = crate::account_quota::AccountQuotaState::default();
    let fetched = crate::account_selection::rfc3339(at);
    crate::account_quota::apply_provider_reply(&mut state, "grok", &serde_json::to_vec(&json!({
        "result":[
            {"provider":"grok","apiProvider":"xai","account":"xai-account",
                "credentialRef":"oauth:xai","source":"vault","fetchedAt":fetched,
                "usage":{"primary":{"usedPercent":0.0,"windowMinutes":10080}}},
            {"provider":"grok","apiProvider":"xai","account":"pooled-account",
                "credentialRef":"oauth:pooled","source":"vault","fetchedAt":fetched,
                "usage":{"extraRateWindows":[{"id":"gemini-5h","window":{"usedPercent":0.0,"windowMinutes":300}}]}}],
        "completeProviders":["grok"]
    })).unwrap(), at).unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
}

/// A request that loses the probe of one account, while the only other
/// account is refused for a reason with no time (it cannot reach the
/// model), gets a timed wait for the probe it lost: never the other
/// account's untimed code, which would read as a missing login.
#[tokio::test]
async fn a_losing_racer_on_a_mixed_roster_gets_a_timed_wait() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials, rows) = mixed_reach_xai_fixture();
    let owner = harness.bind(reserved(OWNER), None).await;
    let select = json!({"consumer":"mason","requirements":{"iq":30},"pin":"xai/model"});
    let selected = owner.call("route.select", &select).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let runner = harness
        .bind(
            reserved(selected.1["result"]["runner"]["module"].as_str().unwrap()),
            None,
        )
        .await;
    let mut request = account_request("xai", "mixed-send");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    assert_credential(
        &runner.call("route.account_for", &request).await,
        "oauth:xai",
    );
    let receipt = NOW + 600_000;
    let _refusal = crate::InjectedClock::set(receipt);
    mixed_reach_xai_reading(receipt as i64);
    let refusal = json!({"event_id":"mixed-refusal","kind":"account_limit_exhausted",
        "admission_id":"mixed-send","served_account_id":"xai-account","credential_id":"oauth:xai",
        "limit_window":"credit","observed_at_ms":receipt});
    let observed = runner.call("route.account_observe", &refusal).await;
    assert_eq!(observed.0, FrameType::Response, "{}", observed.1);

    // An hour later one probe of the xai account's credit refusal is due, and
    // a selection decision claims it.
    let deadline = receipt + 3_600_000;
    let _due = crate::InjectedClock::set(deadline);
    mixed_reach_xai_reading(deadline as i64);
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "xai-credit".into(),
            credentials: rows,
            ..Default::default()
        },
        deadline,
    );
    harness.set_credentials(credentials.clone());
    let probe = owner.call("route.select", &select).await;
    assert_eq!(probe.0, FrameType::Response, "{}", probe.1);

    let mut loser = account_request("xai", "mixed-loser");
    loser["caller"] = json!({"runner":RUNNER});
    match admit_directly(&harness, &format!("reserved:{RUNNER}"), loser, deadline) {
        crate::account_selection::OpReply::Error { code, detail, .. } => {
            let detail = detail.unwrap_or_default();
            assert_eq!(
                code, "account_pool_exhausted",
                "losing the probe is a wait, never the other account's untimed refusal: {detail}"
            );
            let retry_at = detail["reset_at_ms"]
                .as_u64()
                .unwrap_or_else(|| panic!("the wait names when to retry: {detail}"));
            assert!(
                (deadline + 1..=deadline + 3_600_000).contains(&retry_at),
                "{detail}"
            );
            assert!(
                detail["retry_after_s"].as_u64().is_some_and(|s| s > 0),
                "{detail}"
            );
        }
        other => panic!("a second probe was admitted: {other:?}"),
    }
}

/// As [`a_losing_racer_on_a_mixed_roster_gets_a_timed_wait`], but the probe
/// is lost to a selection that holds it reserved while it scores, so the
/// emptied roster is explained before the allocator's refusal: that answer
/// is timed too.
#[tokio::test]
async fn a_racer_losing_a_reserved_probe_on_a_mixed_roster_gets_a_timed_wait() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials, rows) = mixed_reach_xai_fixture();
    let owner = harness.bind(reserved(OWNER), None).await;
    let select = json!({"consumer":"mason","requirements":{"iq":30},"pin":"xai/model"});
    let selected = owner.call("route.select", &select).await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let runner = harness
        .bind(
            reserved(selected.1["result"]["runner"]["module"].as_str().unwrap()),
            None,
        )
        .await;
    let mut request = account_request("xai", "reserved-send");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    assert_credential(
        &runner.call("route.account_for", &request).await,
        "oauth:xai",
    );
    let receipt = NOW + 600_000;
    let _refusal = crate::InjectedClock::set(receipt);
    mixed_reach_xai_reading(receipt as i64);
    let refusal = json!({"event_id":"reserved-refusal","kind":"account_limit_exhausted",
        "admission_id":"reserved-send","served_account_id":"xai-account","credential_id":"oauth:xai",
        "limit_window":"credit","observed_at_ms":receipt});
    let observed = runner.call("route.account_observe", &refusal).await;
    assert_eq!(observed.0, FrameType::Response, "{}", observed.1);

    // An hour later the probe is due, and a selection still scoring holds
    // it reserved.
    let deadline = receipt + 3_600_000;
    let _due = crate::InjectedClock::set(deadline);
    mixed_reach_xai_reading(deadline as i64);
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "xai-credit".into(),
            credentials: rows,
            ..Default::default()
        },
        deadline,
    );
    harness.set_credentials(credentials.clone());
    let mut scoring = crate::account_selection::ClassContext::load(
        harness.store(),
        &harness.credentials(),
        harness.operator_config(),
        crate::ACCOUNT_QUOTA.get(),
        deadline as i64,
    )
    .unwrap();
    scoring
        .reserve_selection_probes(harness.store(), &harness.credentials())
        .unwrap();

    let mut loser = account_request("xai", "reserved-loser");
    loser["caller"] = json!({"runner":RUNNER});
    match admit_directly(&harness, &format!("reserved:{RUNNER}"), loser, deadline) {
        crate::account_selection::OpReply::Error { code, detail, .. } => {
            let detail = detail.unwrap_or_default();
            assert_eq!(
                code, "account_pool_exhausted",
                "losing a reserved probe is a wait, never an untimed refusal: {detail}"
            );
            assert!(
                detail["reset_at_ms"]
                    .as_i64()
                    .is_some_and(|at| at > deadline as i64),
                "{detail}"
            );
        }
        other => panic!("a second probe was admitted: {other:?}"),
    }
    drop(scoring);
}

#[tokio::test]
async fn credit_window_is_not_an_undeclared_v1_report_extension() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = credit_xai_fixture();
    let owner = harness.bind(reserved(OWNER), None).await;
    let selected = owner
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30},"pin":"xai/model"}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let reply = owner
        .call(
            "route.report",
            &json!({"decision_id":selected.1["result"]["decision_id"],
        "outcome":"failed","runner_code":"account_limit_exhausted","credential_id":"oauth:xai",
        "limit_window":"credit"}),
        )
        .await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "invalid_request");
    assert!(harness
        .store()
        .non_quota_account_route_fences(NOW as i64)
        .unwrap()
        .is_empty());
}

fn stale_xai_fixture(used: f64) -> Arc<Harness> {
    let (harness, mut credentials) = setup("xai", false);
    let mut fact = crate::tests::fact("xai", "grok-4.7");
    fact.auth_methods = Some(vec!["oauth".into()]);
    fact.billing_plane = Some(BillingPlane {
        provider_id: "xai".into(),
        auth_method: "oauth".into(),
        kind: "subscription".into(),
        selection_reason: "subscription".into(),
        rates: [("rate.output".into(), BillingRate::Priced { rate: 1.0 })]
            .into_iter()
            .collect(),
    });
    harness.install_facts(&[fact.clone()]);
    catalog_fact(&harness, &fact);
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "xai-missing-vault-account-id".into(),
            credentials: vec![ListedCredential {
                id: "oauth:xai".into(),
                state: "active".into(),
                categories: vec!["llm-provider".into()],
                provider_ids: vec!["xai".into()],
                credential_type: "oauth".into(),
                auth_method: Some("oauth".into()),
                ..Default::default()
            }],
            ..Default::default()
        },
        NOW,
    );
    credentials.refresh_serving_methods(&Default::default(), &[fact.clone()], NOW as i64, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact]),
    );
    harness.set_credentials(credentials);
    let reported = NOW as i64 - 5 * 86_400_000;
    harness
        .store()
        .record_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "xai".into(),
            credential_id: "oauth:xai".into(),
            model: "grok-4.7".into(),
            until_ms: reported + 8 * 86_400_000,
            decision_id: "dec_916ab7900382".into(),
            recorded_at: reported,
        })
        .unwrap();
    harness
        .store()
        .record_credential_entitlement_fence(
            &CredentialRefusal {
                provider_id: "xai".into(),
                credential_id: "oauth:xai".into(),
                code: "billing_exhausted".into(),
            },
            Some("provider_ids=xai;plan_tier=;state=active"),
            NOW as i64 - 12 * 86_400_000,
        )
        .unwrap();
    let mut state = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_usage_reply(
        &mut state,
        &serde_json::to_vec(&json!({
            "result":[{"provider":"grok", "apiProvider":"xai", "credentialRef":"oauth:xai",
                "account":"819c167d-1ebc-4945-a7b9-7197a98a207e", "source":"vault",
                "fetchedAt":crate::account_selection::rfc3339(NOW as i64),
                "usage":{"primary":{"usedPercent":used,"windowMinutes":10080,
                    "resetsAt":crate::account_selection::rfc3339(NOW as i64 + 2 * 86_400_000)}}}],
            "completeProviders":["grok"]
        }))
        .unwrap(),
        NOW as i64,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
    harness
}

#[tokio::test]
async fn stale_xai_eight_day_fence_lifts_on_attested_fresh_headroom() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = stale_xai_fixture(0.0);
    let caller = harness.bind(reserved(OWNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"mason", "requirements":{"iq":30}, "pin":"xai/grok-4.7"}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    assert!(harness
        .store()
        .account_route_fences(NOW as i64)
        .unwrap()
        .is_empty());
    assert!(harness
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .is_empty());
    let runner = harness.bind(reserved(RUNNER), None).await;
    let admitted = runner.call("route.account_for", &json!({"provider":"xai","model":"grok-4.7",
        "conversation_key":"healthy-grok","admission_id":"healthy-grok", "decision_id":selected.1["result"]["decision_id"]})).await;
    assert_credential(&admitted, "oauth:xai");
    assert_eq!(
        admitted.1["result"]["account"]["account_id"],
        "819c167d-1ebc-4945-a7b9-7197a98a207e"
    );
}

#[tokio::test]
async fn stale_xai_guess_shortens_to_newer_reset_and_near_wall_stays_fenced() {
    for used in [100.0, 96.0] {
        let _clock = crate::InjectedClock::set(NOW);
        let harness = stale_xai_fixture(used);
        harness.edit_config(|config| {
            config["accounts"]["soft_quota_remaining_percent"] = json!(5.0);
        });
        let runner = harness.bind(reserved(RUNNER), None).await;
        let blocked = runner
            .call(
                "route.account_for",
                &json!({"provider":"xai","model":"grok-4.7",
            "conversation_key":"near-wall","admission_id":"near-wall"}),
            )
            .await;
        assert_eq!(blocked.0, FrameType::Error, "{used}: {}", blocked.1);
        let fences = harness.store().account_route_fences(NOW as i64).unwrap();
        assert_eq!(fences.len(), 1, "{used}: {fences:?}");
        assert_eq!(fences[0].until_ms, NOW as i64 + 2 * 86_400_000);
        assert_eq!(fences[0].recorded_at, NOW as i64 - 5 * 86_400_000);
        if used == 100.0 {
            assert_eq!(
                harness
                    .store()
                    .credential_entitlement_fences()
                    .unwrap()
                    .len(),
                1
            );
        }
    }
}

#[tokio::test]
async fn stale_api_billing_probe_passes_its_route_fence_once_even_with_spend_reading() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let old = NOW as i64 - crate::credential_fences::FENCE_PROBE_RECHECK_MS - 1;
    harness
        .store()
        .record_credential_entitlement_fence(
            &CredentialRefusal {
                provider_id: "synthetic".into(),
                credential_id: "apikey:test".into(),
                code: "billing_exhausted".into(),
            },
            None,
            old,
        )
        .unwrap();
    harness
        .store()
        .record_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "synthetic".into(),
            credential_id: "apikey:test".into(),
            model: "model".into(),
            until_ms: NOW as i64 + 8 * 86_400_000,
            recorded_at: old,
            decision_id: "billing-refusal".into(),
        })
        .unwrap();
    let mut state = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_usage_reply(&mut state, &serde_json::to_vec(&json!({
        "result":[{"provider":"platform", "apiProvider":"synthetic", "account":"apikey-account",
            "credentialRef":"apikey:test", "fetchedAt":crate::account_selection::rfc3339(NOW as i64),
            "usage":{}, "spend":[{"id":"credits","remaining":{"minor":20000,"exponent":2,"unit":"USD"}}]}]
    })).unwrap(), NOW as i64).unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"codemode", "requirements":{"iq":30},"pin":"synthetic/model"}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let mut request = account_request("synthetic", "due-api-probe");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    let admitted = runner.call("route.account_for", &request).await;
    assert_credential(&admitted, "apikey:test");
    assert_eq!(
        admitted.1["result"]["reason"]["code"],
        "credential_fence_probe"
    );
    request["admission_id"] = json!("second-api-probe");
    request["conversation_key"] = json!("second-api-probe");
    assert_eq!(
        runner.call("route.account_for", &request).await.0,
        FrameType::Error
    );
    assert_eq!(
        harness.store().credential_entitlement_fences().unwrap()[0].observed_at_ms,
        old
    );
}

#[tokio::test]
async fn account_writer_fresh_admit_is_one_command_and_probe_retry_skips_roster() {
    use striatum_store::AccountCommandKind::{Admit, Replay};
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let old = NOW as i64 - crate::credential_fences::FENCE_PROBE_RECHECK_MS - 1;
    harness
        .store()
        .record_credential_entitlement_fence(
            &CredentialRefusal {
                provider_id: "synthetic".into(),
                credential_id: "apikey:test".into(),
                code: "billing_exhausted".into(),
            },
            None,
            old,
        )
        .unwrap();
    harness
        .store()
        .record_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "synthetic".into(),
            credential_id: "apikey:test".into(),
            model: "model".into(),
            until_ms: NOW as i64 + 8 * 86_400_000,
            recorded_at: old,
            decision_id: "billing-refusal".into(),
        })
        .unwrap();
    let mut state = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_usage_reply(&mut state, &serde_json::to_vec(&json!({
        "result":[{"provider":"platform", "apiProvider":"synthetic", "account":"apikey-account",
            "credentialRef":"apikey:test", "fetchedAt":crate::account_selection::rfc3339(NOW as i64),
            "usage":{}, "spend":[{"id":"credits","remaining":{"minor":20000,"exponent":2,"unit":"USD"}}]}]
    })).unwrap(), NOW as i64).unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = state;
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"codemode", "requirements":{"iq":30},"pin":"synthetic/model"}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let mut request = account_request("synthetic", "writer-probe-replay");
    request["decision_id"] = selected.1["result"]["decision_id"].clone();
    let admits = harness.store().account_command_count_for_test(Admit);
    let replays = harness.store().account_command_count_for_test(Replay);
    let admitted = runner.call("route.account_for", &request).await;
    assert_credential(&admitted, "apikey:test");
    assert_eq!(
        harness.store().account_command_count_for_test(Admit) - admits,
        1
    );
    assert_eq!(
        harness.store().account_command_count_for_test(Replay),
        replays
    );
    let claims = format!("{:?}", harness.store().fence_rechecks().unwrap());
    let prepared = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sentinel = prepared.clone();
    after_roster_prepared("writer-probe-replay", move || {
        sentinel.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    let retried = runner.call("route.account_for", &request).await;
    assert_eq!(retried, admitted);
    assert!(!prepared.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!(
        harness.store().account_command_count_for_test(Admit) - admits,
        1
    );
    assert_eq!(
        harness.store().account_command_count_for_test(Replay) - replays,
        1
    );
    assert_eq!(
        format!("{:?}", harness.store().fence_rechecks().unwrap()),
        claims
    );
    request["admission_id"] = json!("writer-second-probe");
    request["conversation_key"] = json!("writer-second-probe");
    assert_eq!(
        runner.call("route.account_for", &request).await.0,
        FrameType::Error,
        "the first probe claim emptied the eligible roster"
    );
}

#[test]
fn s4b_unchanged_contexts_on_both_s0_fixtures_take_only_catalog_key_writer() {
    use super::request_cpu_bench as bench;
    let _clock = crate::InjectedClock::set(bench::FIXTURE_NOW);
    let counts = bench::FixtureCounts {
        subscription_rules: 10,
        exclusion_rules: 103,
        runner_route_rules: 0,
        catalog_facts: 836,
        listed_credentials: 39,
    };
    for fixture in [
        bench::production_fixture(),
        bench::representative_fixture(counts),
    ] {
        let store = fixture.store();
        let credentials = fixture.credentials();
        // Materialize the fixture's seeded repair clocks before measuring an
        // unchanged load. Only the catalogue writer-local key check is exempt.
        crate::account_selection::ClassContext::load(
            store,
            &credentials,
            fixture.operator_config(),
            crate::ACCOUNT_QUOTA.get(),
            bench::FIXTURE_NOW as i64,
        )
        .unwrap()
        .reserve_selection_probes(store, &credentials)
        .unwrap();
        let commits = store.writer_commit_count_for_test();
        let acquisitions = store.writer_acquisition_count_for_test();
        let mut context = crate::account_selection::ClassContext::load(
            store,
            &credentials,
            fixture.operator_config(),
            crate::ACCOUNT_QUOTA.get(),
            bench::FIXTURE_NOW as i64,
        )
        .unwrap();
        context
            .reserve_selection_probes(store, &credentials)
            .unwrap();
        assert_eq!(store.writer_commit_count_for_test(), commits);
        assert_eq!(store.writer_acquisition_count_for_test() - acquisitions, 1);
    }
}

#[test]
fn s4b_route_recovery_unchanged_does_not_acquire_writer() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    harness
        .store()
        .record_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "synthetic".into(),
            credential_id: "oauth:test".into(),
            model: "model".into(),
            until_ms: NOW as i64 + 3_600_000,
            decision_id: "report".into(),
            recorded_at: NOW as i64,
        })
        .unwrap();
    let quota = crate::account_quota::AccountQuotaState::default();
    let ids = credentials.quota_account_ids(&quota);
    // The recovery check reads the published account state; load it before
    // counting, so its one-time load barrier is not counted as the check's.
    crate::account_state::initialize(harness.store()).unwrap();
    let commits = harness.store().writer_commit_count_for_test();
    let acquisitions = harness.store().writer_acquisition_count_for_test();
    repair_route_fences(
        harness.store(),
        &ids,
        &quota,
        &harness.operator_config(),
        NOW as i64,
        || panic!("unchanged recovery must not proceed to the writer"),
    )
    .unwrap();
    assert_eq!(harness.store().writer_commit_count_for_test(), commits);
    assert_eq!(
        harness.store().writer_acquisition_count_for_test(),
        acquisitions
    );
}

#[test]
fn s4b_route_recovery_rechecks_newer_sibling_report_under_writer() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    let now = NOW as i64;
    let report = |model: &str, at| {
        harness
            .store()
            .record_account_route_fence(&striatum_store::AccountRouteFence {
                provider: "synthetic".into(),
                credential_id: "oauth:test".into(),
                model: model.into(),
                until_ms: now + 3_600_000,
                decision_id: model.into(),
                recorded_at: at,
            })
            .unwrap()
    };
    report("model", now - 100);
    oauth_weekly_reading_at(10.0, now + 3_600_000, now);
    let quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap().clone();
    let ids = credentials.quota_account_ids(&quota);
    repair_route_fences(
        harness.store(),
        &ids,
        &quota,
        &harness.operator_config(),
        now,
        || report("newer-sibling", now + 1_000),
    )
    .unwrap();
    assert_eq!(
        harness.store().account_route_fences(now).unwrap().len(),
        2,
        "newer evidence on a sibling route invalidates credential-wide recovery"
    );
    oauth_weekly_reading_at(10.0, now + 3_600_000, now + 10_000);
    let quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap().clone();
    let commits = harness.store().writer_commit_count_for_test();
    repair_route_fences(
        harness.store(),
        &ids,
        &quota,
        &harness.operator_config(),
        now + 10_000,
        || {},
    )
    .unwrap();
    assert!(harness
        .store()
        .account_route_fences(now)
        .unwrap()
        .is_empty());
    assert_eq!(harness.store().writer_commit_count_for_test() - commits, 1);
    let acquisitions = harness.store().writer_acquisition_count_for_test();
    repair_route_fences(
        harness.store(),
        &ids,
        &quota,
        &harness.operator_config(),
        now + 10_000,
        || {},
    )
    .unwrap();
    assert_eq!(harness.store().writer_commit_count_for_test() - commits, 1);
    assert_eq!(
        harness.store().writer_acquisition_count_for_test(),
        acquisitions
    );
}

fn s4b_load(harness: &Harness, now: i64) -> crate::account_selection::ClassContext {
    crate::account_selection::ClassContext::load(
        harness.store(),
        &harness.credentials(),
        harness.operator_config(),
        crate::ACCOUNT_QUOTA.get(),
        now,
    )
    .unwrap()
}

fn s4b_refusal(harness: &Harness, at: i64) {
    crate::credential_fences::record(
        harness.store(),
        &harness.credentials(),
        &CredentialRefusal {
            provider_id: "synthetic".into(),
            credential_id: "oauth:test".into(),
            code: "billing_exhausted".into(),
        },
        at,
    )
    .unwrap();
}

fn s4b_route(harness: &Harness, at: i64, until: i64) {
    harness
        .store()
        .record_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "synthetic".into(),
            credential_id: "oauth:test".into(),
            model: "model".into(),
            until_ms: until,
            decision_id: "fixed-report".into(),
            recorded_at: at,
        })
        .unwrap();
}

fn s4b_seed_trigger(
    harness: &Harness,
    credentials: &mut BrocaCredentials,
    trigger: &str,
    now: i64,
) -> u64 {
    let reset = now + 3_600_000;
    match trigger {
        "continuation" => {
            harness
                .store()
                .account_transaction(|tx| {
                    tx.record_continuation(
                        "test",
                        "expired-offer",
                        &striatum_store::V1Recorded {
                            body_json: "{}".into(),
                            answer_json: "{}".into(),
                        },
                        "run",
                        "admission",
                        "previous",
                        Some(("decision", "reservation", now)),
                    )
                })
                .unwrap();
            // The load reads the expired offer as released through the
            // state's `as_of` view and sends no command; the Tick or the
            // next command releases it durably (asserted by the caller).
            return 0;
        }
        "listing" => {
            let fingerprint = match credentials.listing_fingerprint("oauth:test") {
                crate::broca_credentials::ListingLookup::Listed(fingerprint) => fingerprint,
                _ => panic!("listed fixture"),
            };
            harness
                .store()
                .record_account_eligibility(&striatum_store::AccountEligibilityFact {
                    provider: "synthetic".into(),
                    credential_id: "oauth:test".into(),
                    model: "other".into(),
                    listing_fingerprint: Some(fingerprint.clone()),
                    code: "ineligible".into(),
                    observed_at: now - 100,
                })
                .unwrap();
            for (credential, fingerprint) in [
                ("oauth:test", Some(fingerprint.as_str())),
                ("apikey:test", None),
            ] {
                harness
                    .store()
                    .record_credential_entitlement_fence(
                        &CredentialRefusal {
                            provider_id: "synthetic".into(),
                            credential_id: credential.into(),
                            code: "ENTITLEMENT_ERROR".into(),
                        },
                        fingerprint,
                        now - 100,
                    )
                    .unwrap();
            }
            let mut rows = credentials.listed_credentials().to_vec();
            for row in &mut rows {
                row.plan_tier = Some(format!("changed-{now}"));
            }
            credentials.apply_listing(
                &ScopedCredentialListing {
                    view: format!("changed-{now}"),
                    credentials: rows,
                    ..Default::default()
                },
                now as u64,
            );
            harness.set_credentials(credentials.clone());
            // One Repair command holds fingerprint adoption or clear, the
            // quota-hold clear and eligibility reconciliation. It replaces
            // one transaction plus three autocommits for this listing.
            return 1;
        }
        "pool_deadline" => {
            let mut rows = credentials.listed_credentials().to_vec();
            rows.iter_mut()
                .find(|row| row.id == "apikey:test")
                .unwrap()
                .categories
                .push("media-provider".into());
            credentials.apply_listing(
                &ScopedCredentialListing {
                    view: "media-pool".into(),
                    credentials: rows,
                    ..Default::default()
                },
                now as u64,
            );
            harness.set_credentials(credentials.clone());
            // This expired pool source has no managed retry clock (managed=0).
            // Reading the pool must synchronize once to retire it; a managed
            // clock would keep the refusal available for a probe instead.
            harness.store().with_writer_for_test(|conn| conn.execute_batch(
                "INSERT INTO pool_key_state(provider,credential_id,updated_at) VALUES ('synthetic','oauth:test',0);
                 INSERT INTO block(kind,origin_key,provider,credential_id,evidence,evidence_at,quota,deadline,next_recheck_at,observed_at,source_order,source_present,managed)
                 VALUES ('pool_rate',json_array('synthetic','oauth:test'),'synthetic','oauth:test',json_array(1,NULL,0),0,0,1,1,0,1,1,0);"
            )).unwrap();
        }
        "size" => {
            harness
                .store()
                .record_account_size_refusal(&striatum_store::AccountSizeRefusal {
                    provider: "synthetic".into(),
                    credential_id: "oauth:test".into(),
                    model: "model".into(),
                    size_class: 17,
                    expires_at: reset,
                    observed_at: now - 100,
                })
                .unwrap();
            oauth_weekly_reading_at(10.0, reset, now);
        }
        "route_headroom" => {
            s4b_route(harness, now - 100, reset);
            oauth_weekly_reading_at(10.0, reset, now);
        }
        "route_reset" => {
            s4b_route(harness, now - 100, reset + 3_600_000);
            oauth_weekly_reading_at(100.0, reset, now);
        }
        "quota_create" | "quota_reset" | "quota_headroom" => {
            oauth_weekly_reading_at(100.0, reset + 3_600_000, now - 100);
            s4b_refusal(harness, now - 100);
            if trigger != "quota_create" {
                s4b_load(harness, now - 100);
            }
            oauth_weekly_reading_at(
                if trigger == "quota_headroom" {
                    10.0
                } else {
                    100.0
                },
                reset,
                now,
            );
        }
        "account" => {
            exhaust_oauth(harness, "synthetic");
            oauth_weekly_reading_at(10.0, reset, now);
            // The seeded account fence is for the five-hour pool, so recovery
            // must present that same window, not a different weekly pool.
            let mut quota = crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap();
            for reading in quota.readings.readings.values_mut() {
                reading.entry["usage"]["primary"]["windowMinutes"] = json!(300);
            }
        }
        _ => panic!("unknown trigger {trigger}"),
    }
    1
}

fn s4b_repair_rows(
    harness: &Harness,
) -> std::collections::BTreeMap<String, Vec<Vec<rusqlite::types::Value>>> {
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let archive =
        rusqlite::Connection::open(harness.store_path().with_file_name("history.db")).unwrap();
    [
        "account_continue",
        "account_eligibility",
        "credential_entitlement_fences",
        "account_size_refusal",
        "pool_key_state",
        "account_route_fence",
        "account_fence",
        "fence_recheck",
        "fence_retired_origin",
        "block",
        "probe_authorization",
        "success_proof",
        "retirement_tombstone",
    ]
    .into_iter()
    .map(|table| {
        let database = match table {
            "credential_entitlement_fences"
            | "account_size_refusal"
            | "account_route_fence"
            | "account_fence"
            | "fence_recheck"
            | "fence_retired_origin" => &archive,
            _ => &conn,
        };
        let mut query = database
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1,2,3"))
            .unwrap();
        let columns = query.column_count();
        let rows = query
            .query_map([], |row| {
                (0..columns).map(|column| row.get(column)).collect()
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<Vec<rusqlite::types::Value>>>>()
            .unwrap();
        (table.into(), rows)
    })
    .collect()
}

#[test]
fn s4b_each_context_repair_is_durable_and_not_repeated() {
    let _clock = crate::InjectedClock::set(NOW);
    for trigger in [
        "continuation",
        "pool_deadline",
        "listing",
        "size",
        "route_headroom",
        "route_reset",
        "quota_create",
        "quota_reset",
        "quota_headroom",
        "account",
    ] {
        let (harness, mut credentials) = setup("synthetic", false);
        let now = NOW as i64 + 10_000;
        let expected = s4b_seed_trigger(&harness, &mut credentials, trigger, now);
        let commits = harness.store().writer_commit_count_for_test();
        s4b_load(&harness, now);
        assert_eq!(
            harness.store().writer_commit_count_for_test() - commits,
            expected,
            "{trigger}: each affected repair commits once"
        );
        let rows = s4b_repair_rows(&harness);
        let acquisitions = harness.store().writer_acquisition_count_for_test();
        s4b_load(&harness, now);
        assert_eq!(
            s4b_repair_rows(&harness),
            rows,
            "{trigger}: repeated load changes no durable row"
        );
        assert_eq!(
            harness.store().writer_commit_count_for_test() - commits,
            expected,
            "{trigger}: no repeated commit"
        );
        assert_eq!(
            harness.store().writer_acquisition_count_for_test() - acquisitions,
            1,
            "{trigger}: only the catalog key check"
        );
        if trigger == "continuation" {
            // The Tick releases the offer durably, stamped at its own
            // deadline (`now`, the offer's `valid_until_ms`).
            crate::account_writer::tick(harness.store(), now + 5_000).unwrap();
            let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
            let released: Option<i64> = conn
                .query_row(
                    "SELECT released_at_ms FROM account_continue WHERE selection_id='expired-offer'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(released, Some(now), "{trigger}: released at its deadline");
        }
    }
}

#[derive(Debug, PartialEq)]
struct S4bReplayStep {
    context: Value,
    rows: std::collections::BTreeMap<String, Vec<Vec<rusqlite::types::Value>>>,
    pick: Value,
    admission: Value,
}

async fn s4b_replay(legacy: bool) -> Vec<S4bReplayStep> {
    let mut inputs = striatum_store::replay::Inputs::new(NOW as i64 + 10_000);
    let _replay = inputs.install();
    struct ResetLegacy;
    impl Drop for ResetLegacy {
        fn drop(&mut self) {
            LEGACY_CONTEXT_REPAIRS.store(false, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let _reset = ResetLegacy;
    LEGACY_CONTEXT_REPAIRS.store(legacy, std::sync::atomic::Ordering::SeqCst);
    let (harness, mut credentials) = setup("synthetic", false);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    // Fixture bootstrap has a process-local catalogue generation and a real
    // telemetry-window start. Compare every table changed by the replay, not
    // those untouched bootstrap rows; all action-generated values stay exact.
    let baseline = s4b_all_rows(&harness);
    let mut steps = Vec::new();
    for (index, trigger) in [
        "continuation",
        "pool_deadline",
        "listing",
        "listing",
        "size",
        "route_headroom",
        "route_reset",
        "quota_create",
        "quota_headroom",
        "quota_create",
        "quota_reset",
        "account",
        "size",
    ]
    .into_iter()
    .enumerate()
    {
        let now = NOW as i64 + 10_000 * (index as i64 + 1);
        inputs.now_ms = now;
        let _step_clock = inputs.install();
        s4b_seed_trigger(&harness, &mut credentials, trigger, now);
        let mut context = s4b_load(&harness, now);
        context
            .reserve_selection_probes(harness.store(), &harness.credentials())
            .unwrap();
        let context = json!({
            "facts": context.facts, "refusals": format!("{:?}", context.refusals), "fences": context.fences,
            "subscription": context.probe("synthetic", "model", false, false, None, "fixed-conversation"),
            "priced": context.probe("synthetic", "model", true, false, None, "fixed-conversation"),
        });
        let pick = caller
            .call(
                "route.select",
                &json!({"consumer":"mason","requirements":{"iq":30}}),
            )
            .await;
        let admission = runner
            .call(
                "route.account_for",
                &account_request("synthetic", &format!("replay-{index}")),
            )
            .await;
        let mut rows = s4b_all_rows(&harness);
        rows.retain(|table, rows| baseline.get(table) != Some(rows));
        steps.push(S4bReplayStep {
            context,
            rows,
            pick: json!({"type":format!("{:?}",pick.0), "body":pick.1}),
            admission: json!({"type":format!("{:?}",admission.0), "body":admission.1}),
        });
    }
    steps
}

#[tokio::test]
async fn s4b_trigger_sequences_match_pre_read_first_rows_context_picks_and_admissions() {
    let _clock = crate::InjectedClock::set(NOW + 10_000);
    // Replay inputs keep all generated identities and answers comparable;
    // nothing is masked, including admission answers and persisted claim owners.
    let before = s4b_replay(true).await;
    let after = s4b_replay(false).await;
    for (index, (before, after)) in before.iter().zip(&after).enumerate() {
        assert_eq!(before, after, "trigger sequence diverged at step {index}");
    }
}

#[tokio::test]
async fn s4b_selection_reserving_a_probe_keeps_its_commit_count() {
    let _clock = crate::InjectedClock::set(NOW);
    let mut counts = Vec::new();
    for legacy in [true, false] {
        let _replay = striatum_store::replay::Inputs::new(NOW as i64).install();
        let (harness, _) = setup("synthetic", false);
        crate::credential_fences::record(
            harness.store(),
            &harness.credentials(),
            &CredentialRefusal {
                provider_id: "synthetic".into(),
                credential_id: "oauth:test".into(),
                code: "ENTITLEMENT_ERROR".into(),
            },
            NOW as i64 - 7_200_000,
        )
        .unwrap();
        LEGACY_CONTEXT_REPAIRS.store(legacy, std::sync::atomic::Ordering::SeqCst);
        let caller = harness.bind(reserved(OWNER), None).await;
        let commits = harness.store().writer_commit_count_for_test();
        let selected = caller
            .call(
                "route.select",
                &json!({"consumer":"mason","requirements":{"iq":30}}),
            )
            .await;
        LEGACY_CONTEXT_REPAIRS.store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
        counts.push(harness.store().writer_commit_count_for_test() - commits);
        let runner = harness.bind(reserved(RUNNER), None).await;
        let reserved = runner
            .call(
                "route.account_for",
                &account_request("synthetic", "reserved-elsewhere"),
            )
            .await;
        assert_eq!(
            reserved.1["code"], "account_pool_exhausted",
            "selection must really reserve the only subscription probe: {}",
            reserved.1
        );
    }
    assert!(counts[0] > 0);
    assert_eq!(counts[0], counts[1]);
}

#[tokio::test]
async fn s4b_what_if_fresh_headroom_repairs_only_its_snapshot() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let now = NOW as i64;
    oauth_weekly_reading_at(100.0, now + 3_600_000, now - 100);
    s4b_route(&harness, now - 100, now + 3_600_000);
    let operator = harness.bind(json!({"kind":"direct"}), None).await;
    let request = json!({"consumer":"mason","iqs":[30]});
    let exhausted = operator.call("route.what_if", &request).await;
    assert_eq!(exhausted.0, FrameType::Response, "{}", exhausted.1);
    assert!(
        exhausted.1["result"]["results"][0]["pick"].is_null(),
        "{}",
        exhausted.1
    );
    oauth_weekly_reading_at(10.0, now + 3_600_000, now);
    let rows = s4b_repair_rows(&harness);
    let acquisitions = harness.store().writer_acquisition_count_for_test();
    let commits = harness.store().writer_commit_count_for_test();
    let answer = operator.call("route.what_if", &request).await;
    assert_eq!(answer.0, FrameType::Response, "{}", answer.1);
    assert!(
        answer.1["result"]["results"][0]["pick"].is_object(),
        "fresh headroom must actually recover the preview's account: {}",
        answer.1
    );
    // Only reading the catalog-change key takes a writer checkout: the writer's
    // count of model-fact writes, which says whether cached facts are current.
    // Catalog facts and observed speed aggregates use a reader; the warm
    // class-vector cache and account snapshot take no writer checkout, and the
    // dry run sends no repair command.
    assert_eq!(
        harness.store().writer_acquisition_count_for_test() - acquisitions,
        1
    );
    assert_eq!(harness.store().writer_commit_count_for_test(), commits);
    assert_eq!(s4b_repair_rows(&harness), rows);
    // A later live load must still perform its own durable recovery, just as a
    // control store that never evaluated the preview would have to do.
    let commits = harness.store().writer_commit_count_for_test();
    let context = s4b_load(&harness, now);
    assert_eq!(harness.store().writer_commit_count_for_test() - commits, 1);
    assert!(harness
        .store()
        .account_route_fences(now)
        .unwrap()
        .is_empty());
    assert!(context
        .probe("synthetic", "model", false, false, None, "live")
        .admission()
        .is_some());
}

#[test]
fn s4b_patched_headroom_snapshot_and_writable_control_repair_independently() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let now = NOW as i64;
    oauth_weekly_reading_at(10.0, now + 3_600_000, now);
    s4b_route(&harness, now - 100, now + 3_600_000);
    harness.edit_config(|config| config["accounts"]["soft_quota_remaining_percent"] = json!(95.0));
    let original = harness.operator_config();
    let before = s4b_load(&harness, now);
    assert!(before
        .probe("synthetic", "model", false, false, None, "live")
        .admission()
        .is_none());
    harness.edit_config(|config| config["accounts"]["soft_quota_remaining_percent"] = json!(5.0));
    let patched = harness.operator_config();
    harness.edit_config(|config| config["accounts"]["soft_quota_remaining_percent"] = json!(95.0));
    let rows = s4b_repair_rows(&harness);
    let acquisitions = harness.store().writer_acquisition_count_for_test();
    let snapshot = harness.store().private_account_copy_for_test(now).unwrap();
    let snapshot_commits = snapshot.writer_commit_count_for_test();
    let projected = crate::account_selection::ClassContext::load(
        &snapshot,
        &harness.credentials(),
        patched.clone(),
        crate::ACCOUNT_QUOTA.get(),
        now,
    )
    .unwrap();
    assert!(projected
        .probe("synthetic", "model", false, false, None, "live")
        .admission()
        .is_some());
    assert!(snapshot.writer_commit_count_for_test() > snapshot_commits);
    assert!(snapshot.account_route_fences(now).unwrap().is_empty());
    assert_eq!(
        harness.store().writer_acquisition_count_for_test(),
        acquisitions
    );
    assert_eq!(s4b_repair_rows(&harness), rows);
    let after = crate::account_selection::ClassContext::load(
        harness.store(),
        &harness.credentials(),
        original,
        crate::ACCOUNT_QUOTA.get(),
        now,
    )
    .unwrap();
    assert_eq!(
        before.probe("synthetic", "model", false, false, None, "live"),
        after.probe("synthetic", "model", false, false, None, "live")
    );
    assert_eq!(s4b_repair_rows(&harness), rows);
    let commits = harness.store().writer_commit_count_for_test();
    crate::account_selection::ClassContext::load(
        harness.store(),
        &harness.credentials(),
        patched,
        crate::ACCOUNT_QUOTA.get(),
        now,
    )
    .unwrap();
    assert_eq!(
        harness.store().writer_commit_count_for_test() - commits,
        1,
        "the same patch on a writable context really takes the repair path"
    );
    assert!(harness
        .store()
        .account_route_fences(now)
        .unwrap()
        .is_empty());
}

fn s4b_all_rows(
    harness: &Harness,
) -> std::collections::BTreeMap<String, Vec<Vec<rusqlite::types::Value>>> {
    let conn = rusqlite::Connection::open(harness.store_path()).unwrap();
    let tables = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    tables
        .into_iter()
        .map(|table| {
            let columns = conn
                .prepare(&format!("SELECT * FROM \"{table}\""))
                .unwrap()
                .column_count();
            let order = (1..=columns)
                .map(|column| column.to_string())
                .collect::<Vec<_>>()
                .join(",");
            let rows = conn
                .prepare(&format!("SELECT * FROM \"{table}\" ORDER BY {order}"))
                .unwrap()
                .query_map([], |row| {
                    (0..columns).map(|column| row.get(column)).collect()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<Vec<rusqlite::types::Value>>>>()
                .unwrap();
            (table, rows)
        })
        .collect()
}

/// What one selection step answers, for the tests below: a probe binding,
/// or a launch by a runner that can or cannot pin its credential.
fn selection_step(
    context: &crate::account_selection::ClassContext,
    harness: &Harness,
    step: &str,
) -> String {
    match step {
        "bind" => match context.bind_selected_probe(
            harness.store(),
            "synthetic",
            "model",
            false,
            "decision",
        ) {
            Ok(crate::account_selection::ProbeBinding::Bound(_)) => "Ok(Bound)".into(),
            Ok(crate::account_selection::ProbeBinding::Lost { retry_at }) => {
                format!("Ok(Lost {{ retry_at: {retry_at} }})")
            }
            Err(error) => format!("Err({error})"),
        },
        "launch with a pinning runner" => format!(
            "{:?}",
            context.claim_launch_credential(
                harness.store(),
                "synthetic",
                "model",
                false,
                true,
                None
            )
        ),
        _ => format!(
            "{:?}",
            context.claim_launch_credential(
                harness.store(),
                "synthetic",
                "model",
                false,
                false,
                None
            )
        ),
    }
}

const SELECTION_STEPS: [&str; 3] = ["bind", "launch with a pinning runner", "launch without one"];

/// A selection reads its account blocks once, before it binds its probe or
/// launches. A block recorded after that read, through the account writer,
/// must still stop the bind and both launch branches: they decide which
/// clocks apply, and whether anything needs claiming, from the current
/// state and the command's own fences, not from the selection's read.
#[test]
fn a_block_recorded_after_the_selection_read_stops_its_bind_and_launches() {
    let _clock = crate::InjectedClock::set(NOW);
    for step in SELECTION_STEPS {
        let (harness, _) = setup("synthetic", false);
        let mut context = s4b_load(&harness, NOW as i64);
        context
            .reserve_selection_probes(harness.store(), &harness.credentials())
            .unwrap();
        let chosen = context
            .probe("synthetic", "model", false, false, None, "chosen")
            .admission()
            .map(|admission| admission.credential_id.clone());
        assert_eq!(
            chosen.as_deref(),
            Some("oauth:test"),
            "{step}: the selection picks the login the block names"
        );
        exhaust_oauth(&harness, "synthetic");
        let answer = selection_step(&context, &harness, step);
        let claimed: Vec<_> = harness
            .store()
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .filter(|clock| clock.claim_owner.is_some())
            .collect();
        assert!(
            claimed.is_empty(),
            "{step} claimed past the new block: {claimed:?} ({answer})"
        );
        // Each step waits for the new block's probe: the time its clock
        // falls due. A launch claim answers `Result<Option<String>, _>`,
        // `Ok(Some(credential))` to pin and `Ok(None)` to go ahead unpinned,
        // so neither may appear here; a non-pinning runner gets the same
        // timed wait, not the untimed `no_eligible_account` refusal that is
        // only for a due probe.
        let clocks = harness.store().fence_rechecks().unwrap();
        let [block] = clocks.as_slice() else {
            panic!("{step}: one clock, the new block's: {clocks:?}");
        };
        let wait = format!("Lost {{ retry_at: {} }}", block.due_at());
        let expected = if step == "bind" {
            format!("Ok({wait})")
        } else {
            format!("Err({wait})")
        };
        assert_eq!(answer, expected, "{step} must wait for the new block");
    }
}

/// The bind and both launch claims decide which clocks apply from the
/// command's working state, and the claim names only those. Each first
/// checks the rows that chose them against SQL. Here a route clear is
/// written straight to SQL behind the account writer's back
/// (`write_unseen_for_test`), so the working state lacks it: the check finds
/// the difference, the command runs again on a fresh load, and it claims
/// nothing the clear lifted.
#[test]
fn bind_and_launch_claims_check_the_rows_that_chose_their_clocks() {
    let _clock = crate::InjectedClock::set(NOW);
    for step in SELECTION_STEPS {
        let (harness, _) = setup("synthetic", false);
        striatum_core::RouteStateStore::record_provider_cooldown(
            harness.store().as_ref(),
            "synthetic",
            "synthetic/model",
            "synthetic/model",
            &striatum_core::route_state_store::RouteStateUpdate {
                observed_at_ms: Some(NOW as i64 - 2 * 3_600_000),
                cooldown_until: Some((NOW + 3_600_000) as f64),
                last_error: Some("provider capacity".into()),
                ..Default::default()
            },
        )
        .unwrap();
        harness
            .store()
            .account_transaction(|tx| tx.sync_fence_rechecks(crate::account_selection::now_ms()))
            .unwrap();
        let mut context = s4b_load(&harness, NOW as i64);
        context
            .reserve_selection_probes(harness.store(), &harness.credentials())
            .unwrap();
        harness
            .store()
            .write_unseen_for_test(
                "UPDATE block SET cleared_models=json_array('model') WHERE kind='provider';",
            )
            .unwrap();
        let answer = selection_step(&context, &harness, step);
        let claimed: Vec<_> = harness
            .store()
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .filter(|clock| clock.claim_owner.is_some())
            .collect();
        assert!(
            claimed.is_empty(),
            "{step} claimed a clock the clear lifted: {claimed:?} ({answer})"
        );
    }
}

/// An account fence on `account` for `synthetic` until `reset_at_ms`,
/// recorded through the account writer as a broca limit.
fn fence_account_until(harness: &Harness, account: &str, reset_at_ms: i64) {
    let scope = FenceScope::General(GeneralWindow::FiveHour);
    let fence = Fence {
        account_id: account.into(),
        pool: Some(scope.into()),
        recovery_set: Vec::new(),
        observed_at_ms: NOW as i64,
        reset: ResetTime::At(reset_at_ms),
        retry_hint_ms: None,
        source: ConfirmationSource::BrocaAccountLimit,
    };
    harness
        .store()
        .account_transaction(|tx| {
            tx.insert_fence(&AccountFenceRow {
                fence_id: format!("class-fence-{account}-{reset_at_ms}"),
                provider: "synthetic".into(),
                account_id: account.into(),
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

/// A provider-capacity block on `synthetic` that is already due: its
/// observation is two hours old, although the raw cooldown runs another
/// hour. Its clock is not account-scoped, so a launch whose published read
/// sees only it takes the route-only branch.
fn due_provider_block(harness: &Harness) {
    striatum_core::RouteStateStore::record_provider_cooldown(
        harness.store().as_ref(),
        "synthetic",
        "synthetic/model",
        "synthetic/model",
        &striatum_core::route_state_store::RouteStateUpdate {
            observed_at_ms: Some(NOW as i64 - 2 * 3_600_000),
            cooldown_until: Some((NOW + 3_600_000) as f64),
            last_error: Some("provider capacity".into()),
            ..Default::default()
        },
    )
    .unwrap();
    harness
        .store()
        .account_transaction(|tx| tx.sync_fence_rechecks(crate::account_selection::now_ms()))
        .unwrap();
}

/// The selection context every test below starts from: read before any
/// block, with its probes reserved (none is due, so it holds none).
fn selection_read(harness: &Harness) -> crate::account_selection::ClassContext {
    let mut context = s4b_load(harness, NOW as i64);
    context
        .reserve_selection_probes(harness.store(), &harness.credentials())
        .unwrap();
    assert_eq!(
        context
            .probe("synthetic", "model", false, false, None, "chosen")
            .admission()
            .map(|admission| admission.credential_id.clone())
            .as_deref(),
        Some("oauth:test"),
        "the selection launches on the oauth login"
    );
    context
}

/// The selection chose the oauth login and read no block, so it needed no
/// credential pin. A block on the other login then lands before the Broca
/// launch claim. The claim decides on the current state, where an account
/// block now exists on the provider, so the launch target must pin the
/// login the claim returned: an unpinned target could be sent on the
/// blocked login.
#[test]
fn a_block_on_another_login_after_the_selection_read_keeps_the_launch_pinned() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let context = selection_read(&harness);
    fence_account_until(&harness, "apikey-account", NOW as i64 + 600_000);
    let launched = crate::select::runner_for_route(
        &Default::default(),
        &harness.credentials(),
        harness.store(),
        "session",
        "synthetic",
        "model",
        None,
        Some(&context),
        false,
        None,
    );
    let (runner, grant) = match launched {
        Ok(launched) => launched,
        Err(refusal) => panic!("the launch was refused: {refusal:?}"),
    };
    assert!(grant.is_none());
    assert_eq!(
        runner.pinned_credential_id().as_deref(),
        Some("oauth:test"),
        "the launch target pins the claimed login: {runner:?}"
    );
}

/// The selection held no probe reservation, because nothing was due when it
/// read. A block that is already due (a provider-capacity observation two
/// hours old) lands before the bind. A legacy Broca send makes no later
/// claim, so the bind must claim that probe for its decision; returning
/// Bound with the probe unclaimed would let another request take the
/// recovery slot while this send goes ahead.
#[test]
fn a_due_block_recorded_after_the_selection_read_is_claimed_by_its_bind() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let context = selection_read(&harness);
    due_provider_block(&harness);
    let clocks = harness.store().fence_rechecks().unwrap();
    assert!(
        clocks
            .iter()
            .any(|clock| clock.kind == "provider" && clock.due_at() <= NOW as i64),
        "the new block is due: {clocks:?}"
    );
    let answer = selection_step(&context, &harness, "bind");
    let owners: Vec<_> = harness
        .store()
        .fence_rechecks()
        .unwrap()
        .into_iter()
        .map(|clock| (clock.kind, clock.claim_owner))
        .collect();
    assert_eq!(answer, "Ok(Bound)");
    assert_eq!(
        owners,
        vec![("provider".to_owned(), Some("decision".to_owned()))]
    );
}

/// A launch by a runner that cannot pin its credential, with an account
/// block on the other login committed between the launch's read of the
/// published state and its command. The due provider block makes that read
/// choose the route-only branch, whose command sees the new block. That block does not apply to the login
/// this launch uses, so the launch goes ahead; it must not wait for an
/// unrelated account's probe.
#[test]
fn another_logins_block_after_the_launch_read_does_not_stop_a_non_pinning_launch() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let context = selection_read(&harness);
    due_provider_block(&harness);
    let shared = harness.clone();
    before_launch_command(&harness.credentials(), move |_| {
        fence_account_until(&shared, "apikey-account", NOW as i64 + 3_600_000);
    });
    let answer = selection_step(&context, &harness, "launch without one");
    assert!(
        harness
            .store()
            .fence_rechecks()
            .unwrap()
            .iter()
            .any(|clock| clock.account_id.as_deref() == Some("apikey-account")),
        "the block landed"
    );
    assert_eq!(answer, "Ok(None)");
}

/// A block on the login a non-pinning launch uses that is not due yet gives
/// the same timed wait whichever side of the launch's published read it
/// lands on: the untimed `no_eligible_account` answer is only for a due
/// probe, which this runner could never take. Before the read, the launch
/// takes the credential branch; after it, the route-only branch (the due
/// provider block is all the read sees) finds the block in its command.
#[test]
fn a_block_on_the_launch_login_gives_a_non_pinning_launch_one_timed_wait_either_side_of_its_read() {
    let _clock = crate::InjectedClock::set(NOW);
    let reset = NOW as i64 + 3_600_000;
    let mut answers = Vec::new();
    for before_read in [true, false] {
        let (harness, _) = setup("synthetic", false);
        let context = selection_read(&harness);
        due_provider_block(&harness);
        if before_read {
            fence_account_until(&harness, "oauth-account", reset);
        } else {
            let shared = harness.clone();
            before_launch_command(&harness.credentials(), move |_| {
                fence_account_until(&shared, "oauth-account", reset);
            });
        }
        let answer = selection_step(&context, &harness, "launch without one");
        // The wait is the block's own probe time: its quota clock's next check.
        let clocks = harness.store().fence_rechecks().unwrap();
        let block = clocks
            .iter()
            .find(|clock| clock.account_id.as_deref() == Some("oauth-account"))
            .expect("the block has a clock");
        assert!(
            block.quota && block.next_recheck_at > NOW as i64,
            "{block:?}"
        );
        assert_eq!(
            answer,
            format!("Err(Lost {{ retry_at: {} }})", block.next_recheck_at),
            "block recorded before the launch's read: {before_read}"
        );
        answers.push(answer);
    }
    assert_eq!(answers[0], answers[1]);
}

/// A transient retry block the selection read is let pass to admission
/// (`transient_overload_rotates_a_fresh_model_pin_but_never_walls_the_account_pool`).
/// One recorded after the selection read is a timed wait at the bind,
/// until the retry time, and nothing more: no account is walled, and a
/// selection that reads it afterwards binds.
#[test]
fn a_transient_block_recorded_after_the_selection_read_is_a_timed_wait_not_a_pool_wall() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let context = selection_read(&harness);
    crate::record_cooldown_update(
        harness.store(),
        &serde_json::from_value(json!({
            "routeKey":"synthetic/model", "capacityKey":"synthetic/model",
            "credentialId":"oauth:test", "wallClass":"transient", "cooldownUntil":NOW + 5_000,
            "lastError":"{\"class\":\"transient\",\"provider_code\":\"server_is_overloaded\"}"
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        selection_step(&context, &harness, "bind"),
        format!("Ok(Lost {{ retry_at: {} }})", NOW + 5_000)
    );
    let conn =
        rusqlite::Connection::open(harness.store_path().with_file_name("history.db")).unwrap();
    let walls: i64 = conn
        .query_row(
            "SELECT count(*) FROM account_fence WHERE cleared_at IS NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(walls, 0, "no account is walled");
    let claimed: Vec<_> = harness
        .store()
        .fence_rechecks()
        .unwrap()
        .into_iter()
        .filter(|clock| clock.claim_owner.is_some())
        .collect();
    assert!(claimed.is_empty(), "{claimed:?}");
    let mut later = s4b_load(&harness, NOW as i64);
    later
        .reserve_selection_probes(harness.store(), &harness.credentials())
        .unwrap();
    assert_eq!(selection_step(&later, &harness, "bind"), "Ok(Bound)");
}
