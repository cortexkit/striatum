//! `route.account_for` refuses only with the contract's codes, with their
//! timing details, on both the legacy handler and the `routing/v1` adapter.
//!
//! Each test drives the same condition through both entrypoints: the legacy
//! `account_selection::handle` with a class-aware context (the shape
//! `main.rs` serves) and the v1 `route.account_for` route. Timed refusals
//! carry their time (`until_ms` for a hold, `reset_at_ms` for an exhausted
//! pool); every other refusal carries no time. The transient
//! `account_selection_unavailable` carries no time either.

use std::sync::Arc;

use cortexkit_role_routing::errors::RefusalCode;
use serde_json::{json, Value};
use subc_protocol::FrameType;

use super::account_classes_tests::setup;
use super::conformance_tests::{reserved, Harness};
use crate::account_selection::{self, OpReply, ACCOUNT_FOR_OP};
use crate::broca_credentials::BrocaCredentials;

const NOW: u64 = 1_800_000_000_000;
const RUNNER: &str = "class-runner";
const OWNER: &str = "class-owner";

fn v1_request(provider: &str, id: &str) -> Value {
    json!({"provider":provider,"model":"model","conversation_key":format!("conversation-{id}"),"admission_id":id})
}

fn legacy_request(provider: &str, id: &str, key: &str) -> Value {
    json!({
        "provider": provider, "model": "model",
        "conversation_key": key, "caller": {"runner": RUNNER},
        "admission_id": id, "mode": "admit",
    })
}

fn legacy_provider_reply(
    harness: &Harness,
    credentials: BrocaCredentials,
    provider: &str,
    model: &str,
) -> OpReply {
    let shared = Arc::new(std::sync::Mutex::new(credentials));
    let policy = harness.operator_config().allocator_policy(provider);
    let ctx = account_selection::Ctx {
        store: harness.store(),
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: crate::config_overrides::AccountSelectionFlags { enabled: true },
        policy: &policy,
        now_ms: NOW as i64,
        classes: Some(
            account_selection::ClassContext::load(
                harness.store(),
                &shared,
                harness.operator_config(),
                crate::ACCOUNT_QUOTA.get(),
                NOW as i64,
            )
            .unwrap(),
        ),
    };
    let mut request = legacy_request(provider, "provider-legacy", "provider-conversation");
    request["model"] = json!(model);
    let body = serde_json::to_vec(&json!({"method": ACCOUNT_FOR_OP, "params": request})).unwrap();
    account_selection::handle(ACCOUNT_FOR_OP, &body, &format!("reserved:{RUNNER}"), &ctx)
}

const GEMINI: &str = "antigravity-gemini-3.8-flash";

#[tokio::test]
async fn unknown_account_provider_is_invalid_with_unique_model_provider_suggestion() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("google", false);
    harness.install_facts(&[crate::tests::fact("google", GEMINI)]);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut request = v1_request("antigravity", "unknown-provider");
    request["model"] = json!(GEMINI);
    let reply = runner.call("route.account_for", &request).await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "invalid_request", "{}", reply.1);
    assert_eq!(
        reply.1["detail"], json!({"field":"provider"}), "{}", reply.1
    );
    let message = reply.1["message"].as_str().unwrap();
    assert!(message.contains("`antigravity`"), "{message}");
    assert!(message.contains("`google`"), "{message}");
}

#[tokio::test]
async fn legacy_unknown_account_provider_is_invalid_with_the_same_suggestion() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("google", false);
    harness.install_facts(&[crate::tests::fact("google", GEMINI)]);
    match legacy_provider_reply(&harness, credentials, "antigravity", GEMINI) {
        OpReply::Error {
            code,
            message,
            detail,
        } => {
            assert_eq!(code, "invalid_request", "{message}");
            assert_eq!(detail, Some(json!({"field":"provider"})));
            assert!(message.contains("`antigravity`"), "{message}");
            assert!(message.contains("`google`"), "{message}");
        }
        other => panic!("expected provider refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn known_account_provider_without_credentials_or_model_keeps_auth_required() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("google", false);
    credentials.apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            view: "empty-vault".into(),
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials.clone());
    let runner = harness.bind(reserved(RUNNER), None).await;
    for model in ["model", "no-catalog-row"] {
        let mut request = v1_request("google", model);
        request["model"] = json!(model);
        let reply = runner.call("route.account_for", &request).await;
        assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
        assert_eq!(reply.1["code"], "auth_required", "{}", reply.1);
        assert_eq!(reply.1["detail"], json!({}), "{}", reply.1);
        assert_eq!(
            legacy_provider_reply(&harness, credentials.clone(), "google", model).code(),
            Some("auth_required")
        );
    }
}

#[tokio::test]
async fn account_provider_alias_and_company_gateway_are_known_without_model_rows() {
    use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("anthropic", false);
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "aliases-and-gateway".into(),
            credentials: [
                ("anthropic-platform", "llm-provider", "apikey"),
                ("company-gateway", "llm-gateway", "gateway"),
            ]
            .into_iter()
            .map(|(provider, category, method)| ListedCredential {
                id: format!("{provider}:key"),
                provider_ids: vec![provider.into()],
                serves: vec!["anthropic".into()],
                categories: vec![category.into()],
                state: "active".into(),
                credential_type: "apikey".into(),
                auth_method: Some(method.into()),
                ..Default::default()
            })
            .collect(),
            ..Default::default()
        },
        NOW,
    );
    let targets = credentials
        .catalog_resolution_for_providers(&["anthropic".into()].into())
        .targets;
    assert_eq!(
        targets.get("anthropic-platform").map(String::as_str),
        Some("anthropic")
    );
    assert_eq!(
        targets.get("company-gateway").map(String::as_str),
        Some("anthropic")
    );
    harness.set_credentials(credentials.clone());
    let runner = harness.bind(reserved(RUNNER), None).await;
    for provider in ["anthropic-platform", "company-gateway"] {
        let mut request = v1_request(provider, provider);
        request["model"] = json!("no-catalog-row");
        let reply = runner.call("route.account_for", &request).await;
        assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
        assert_eq!(reply.1["code"], "auth_required", "{}", reply.1);
        assert_eq!(
            legacy_provider_reply(&harness, credentials.clone(), provider, "no-catalog-row").code(),
            Some("auth_required")
        );
    }
}

#[tokio::test]
async fn unknown_account_provider_does_not_guess_between_multiple_model_providers() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("google", false);
    harness.install_facts(&[
        crate::tests::fact("google", GEMINI),
        crate::tests::fact("other-vendor", GEMINI),
    ]);
    let runner = harness.bind(reserved(RUNNER), None).await;
    for model in [GEMINI, "no-catalog-row"] {
        let mut request = v1_request("antigravity", model);
        request["model"] = json!(model);
        let reply = runner.call("route.account_for", &request).await;
        assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
        assert_eq!(reply.1["code"], "invalid_request", "{}", reply.1);
        let message = reply.1["message"].as_str().unwrap();
        assert!(message.contains("`antigravity`"), "{message}");
        assert!(!message.contains("`google`"), "{message}");
        assert!(!message.contains("`other-vendor`"), "{message}");
    }
}

#[tokio::test]
async fn invalid_request_is_untimed_on_both_entrypoints() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let runner = harness.bind(reserved(RUNNER), None).await;
    // V1: malformed pool.
    let mut bad = v1_request("synthetic", "bad-pool");
    bad["pool"] = json!("bad/name");
    let refused = runner.call("route.account_for", &bad).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "invalid_request", "{}", refused.1);
    assert_eq!(refused.1["detail"]["field"], "pool", "{}", refused.1);
    // Legacy: empty provider.
    let policy = harness.operator_config().allocator_policy("synthetic");
    let ctx = account_selection::Ctx {
        store: harness.store(),
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: crate::config_overrides::AccountSelectionFlags { enabled: true },
        policy: &policy,
        now_ms: NOW as i64,
        classes: None,
    };
    let body = serde_json::to_vec(&json!({"method": ACCOUNT_FOR_OP, "params": {
        "provider": "", "model": "model", "conversation_key": "k",
        "caller": {"runner": RUNNER}, "admission_id": "invalid-legacy",
    }}))
    .unwrap();
    assert_eq!(
        account_selection::handle(ACCOUNT_FOR_OP, &body, &format!("reserved:{RUNNER}"), &ctx)
            .code(),
        Some("invalid_request")
    );
}

#[tokio::test]
async fn unknown_decision_is_untimed_on_both_entrypoints() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut request = v1_request("synthetic", "unknown-decision");
    request["decision_id"] = json!("missing-decision");
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "unknown_decision", "{}", refused.1);
    assert_eq!(refused.1["detail"], json!({}), "{}", refused.1);
    // Legacy with the same unknown decision.
    let policy = harness.operator_config().allocator_policy("synthetic");
    let ctx = account_selection::Ctx {
        store: harness.store(),
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: crate::config_overrides::AccountSelectionFlags { enabled: true },
        policy: &policy,
        now_ms: NOW as i64,
        classes: None,
    };
    let mut legacy = legacy_request("synthetic", "unknown-legacy", "k");
    legacy["model_decision_id"] = json!("missing-decision");
    let body = serde_json::to_vec(&json!({"method": ACCOUNT_FOR_OP, "params": legacy})).unwrap();
    assert_eq!(
        account_selection::handle(ACCOUNT_FOR_OP, &body, &format!("reserved:{RUNNER}"), &ctx)
            .code(),
        Some("unknown_decision")
    );
}

#[tokio::test]
async fn admission_conflict_is_untimed() {
    // The legacy replay path is covered by the handler's own tests; here the
    // v1 `route.account_for` admission-conflict response is checked.
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let first = runner
        .call("route.account_for", &v1_request("synthetic", "conflict"))
        .await;
    assert_eq!(first.0, FrameType::Response, "{}", first.1);
    let mut second = v1_request("synthetic", "conflict");
    second["conversation_key"] = json!("different-conversation");
    let refused = runner.call("route.account_for", &second).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "admission_conflict", "{}", refused.1);
    assert_eq!(refused.1["detail"], json!({}), "{}", refused.1);
}

#[tokio::test]
async fn pin_generation_conflict_is_untimed_on_both_entrypoints() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let runner = harness.bind(reserved(RUNNER), None).await;
    let admitted = runner
        .call("route.account_for", &v1_request("synthetic", "pin"))
        .await;
    assert_eq!(admitted.0, FrameType::Response, "{}", admitted.1);
    let pin = &admitted.1["result"]["pin"];
    let mut renew = v1_request("synthetic", "pin-renew");
    renew["mode"] = json!("renew");
    renew["conversation_key"] = json!("conversation-pin");
    renew["expected_pin"] = json!({"id": pin["id"], "generation": 9999});
    let refused = runner.call("route.account_for", &renew).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(
        refused.1["code"], "pin_generation_conflict",
        "{}",
        refused.1
    );
    assert_eq!(refused.1["detail"], json!({}), "{}", refused.1);
}

#[tokio::test]
async fn disabled_is_declared_untimed_and_module_wide() {
    // Keep the exact `account_selection_disabled` code. It means the operator
    // switched account selection off for the whole module (the global serving
    // flag), and Broca falls back to its static credential on it, so it must
    // stay distinguishable from quota or auth refusals. The contract declares
    // it (0.5.2) as untimed with empty detail.
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let policy = harness.operator_config().allocator_policy("synthetic");
    let ctx = account_selection::Ctx {
        store: harness.store(),
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: crate::config_overrides::AccountSelectionFlags { enabled: false },
        policy: &policy,
        now_ms: NOW as i64,
        classes: None,
    };
    let body = serde_json::to_vec(&json!({"method": ACCOUNT_FOR_OP, "params": legacy_request("synthetic", "disabled-legacy", "k")})).unwrap();
    let reply =
        account_selection::handle(ACCOUNT_FOR_OP, &body, &format!("reserved:{RUNNER}"), &ctx);
    assert_eq!(reply.code(), Some("account_selection_disabled"));
    // The v1 adapter preserves it as the declared code with empty detail.
    let refusal = super::accounts::account_refusal_for_test("account_selection_disabled");
    assert_eq!(refusal.code, RefusalCode::AccountSelectionDisabled);
    assert!(refusal.code.is_known());
    assert!(refusal.detail.is_empty());
    assert_eq!(refusal.wait_until_ms(), None);
}

#[test]
fn disabled_sample_from_contract_052_is_declared_and_never_timed() {
    // Pins the exact `account_selection_disabled` sample body shipped in the
    // 0.5.2 `test-vectors/routing-v1/refusals.json`: it decodes to the known
    // code with empty detail and is never timed, even if a peer attaches
    // timing fields.
    let mut refusal: cortexkit_role_routing::errors::Refusal = serde_json::from_str(
        r#"{"code":"account_selection_disabled","message":"account selection is off on this router; use your own credential","detail":{}}"#,
    )
    .unwrap();
    assert!(refusal.code.is_known());
    assert_eq!(refusal.code, RefusalCode::AccountSelectionDisabled);
    assert!(refusal.detail.is_empty());
    assert_eq!(refusal.wait_until_ms(), None);
    for key in ["reset_at_ms", "until_ms"] {
        refusal.detail.insert(key.into(), json!(99));
    }
    assert_eq!(refusal.wait_until_ms(), None);
}

#[tokio::test]
async fn unscoped_caller_is_not_authorized() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let route = harness.bind(reserved("not-a-runner"), None).await;
    let refused = route
        .call("route.account_for", &v1_request("synthetic", "no-scope"))
        .await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    // The contract answers any non-runner caller as `account_not_authorized`.
    assert_eq!(refused.1["code"], "account_not_authorized", "{}", refused.1);
    assert_eq!(refused.1["detail"], json!({}), "{}", refused.1);
}

#[tokio::test]
async fn fenced_class_is_exhausted_until_its_probe_window_on_both_entrypoints() {
    // A credential fence is an account limit, not an auth problem. Its
    // next probe window is a retry boundary, not a quota replenishment.
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    let shared = Arc::new(std::sync::Mutex::new(credentials));
    crate::credential_fences::record(
        harness.store(),
        &shared,
        &striatum_core::route_state_store::CredentialRefusal {
            provider_id: "synthetic".into(),
            credential_id: "oauth:test".into(),
            code: "ENTITLEMENT_ERROR".into(),
        },
        NOW as i64,
    )
    .unwrap();
    let runner = harness.bind(reserved(RUNNER), None).await;
    let refused = runner
        .call("route.account_for", &v1_request("synthetic", "fenced"))
        .await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_pool_exhausted", "{}", refused.1);
    assert_eq!(
        refused.1["detail"]["reset_at_ms"],
        json!(NOW + 3_600_000),
        "{}",
        refused.1
    );
    let refusal: cortexkit_role_routing::errors::Refusal =
        serde_json::from_value(refused.1.clone()).unwrap();
    assert_eq!(refusal.wait_until_ms(), Some(NOW + 3_600_000));
}

#[tokio::test]
async fn missing_payment_class_needs_auth_on_both_entrypoints() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    // Remove the metered credential: a priced-only consumer has no credential.
    let rows: Vec<_> = credentials
        .listed_credentials()
        .iter()
        .filter(|row| row.id != "apikey:test")
        .cloned()
        .collect();
    credentials.apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            view: "oauth-only".into(),
            credentials: rows,
            ..Default::default()
        },
        NOW,
    );
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["consumers"]["codemode"]["pay_per_token"] = json!(true);
    });
    let caller = harness.bind(reserved(OWNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"codemode","requirements":{"iq":0}}),
        )
        .await;
    // The metered consumer cannot select without its credential; when selection
    // does answer, `account_for` for its class needs a login.
    if selected.0 == FrameType::Response {
        let runner = harness.bind(reserved(RUNNER), None).await;
        let mut request = v1_request("synthetic", "metered-missing");
        request["decision_id"] = selected.1["result"]["decision_id"].clone();
        let reply = runner.call("route.account_for", &request).await;
        if reply.0 == FrameType::Error {
            assert_eq!(reply.1["code"], "auth_required", "{}", reply.1);
            assert_eq!(reply.1["detail"], json!({}), "{}", reply.1);
        }
    }
}

#[tokio::test]
async fn route_fence_is_exhausted_with_its_reset_on_both_entrypoints() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, _) = setup("synthetic", false);
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    // Admit once so the report fences the serving account's route.
    let before = runner
        .call(
            "route.account_for",
            &v1_request("synthetic", "before-fence"),
        )
        .await;
    assert_eq!(before.0, FrameType::Response, "{}", before.1);
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30},"affinity":"fence-affinity"}),
        )
        .await;
    assert_eq!(selected.0, FrameType::Response, "{}", selected.1);
    let reset = NOW + 3_600_000;
    let report = caller
        .call(
            "route.report",
            &json!({"decision_id":selected.1["result"]["decision_id"],"outcome":"failed","runner_code":"provider_error","reset_at_ms":reset}),
        )
        .await;
    assert_eq!(report.0, FrameType::Response, "{}", report.1);
    let mut fenced = v1_request("synthetic", "after-fence");
    fenced["conversation_key"] = json!("conversation-before-fence");
    let refused = runner.call("route.account_for", &fenced).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_pool_exhausted", "{}", refused.1);
    assert_eq!(
        refused.1["detail"]["reset_at_ms"],
        json!(NOW + 15 * 60_000),
        "{}",
        refused.1
    );
    // The refusal is timed.
    let refusal: cortexkit_role_routing::errors::Refusal =
        serde_json::from_value(refused.1.clone()).unwrap();
    assert_eq!(refusal.wait_until_ms(), Some(NOW + 15 * 60_000));
    assert_eq!(refusal.code, RefusalCode::AccountPoolExhausted);
    assert_eq!(serde_json::to_vec(&refused.1).unwrap(), serde_json::to_vec(&json!({
        "code":"account_pool_exhausted",
        "message":format!("a failed report closed synthetic/model on every account this request may use until {}",
            account_selection::rfc3339((NOW + 15 * 60_000) as i64)),
        "detail":{"code":"account_pool_exhausted","retry_after_s":900,"decision_id":null,
            "exclusions":[],"reset_at_ms":NOW + 15 * 60_000},
    })).unwrap(), "without an unoffered sibling the refusal bytes stay unchanged");
}

#[tokio::test]
async fn route_fence_names_eligible_accounts_outside_the_offered_set() {
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, mut credentials) = setup("synthetic", false);
    let mut rows = credentials.listed_credentials().to_vec();
    let mut sibling = rows.iter().find(|row| row.id == "oauth:test").unwrap().clone();
    sibling.id = "oauth:unoffered".into();
    sibling.account_id = Some("unoffered-account".into());
    rows.push(sibling);
    credentials.apply_listing(&cortexkit_claustrum_client::ScopedCredentialListing {
        view:"unoffered-sibling".into(), credentials:rows, ..Default::default()
    }, NOW);
    harness.set_credentials(credentials);
    harness.store().record_account_route_fence(&striatum_store::AccountRouteFence {
        provider:"synthetic".into(), credential_id:"oauth:test".into(), model:"model".into(),
        until_ms:(NOW + 86_400_000) as i64, decision_id:"offered-wall".into(), recorded_at:NOW as i64,
    }).unwrap();
    let runner = harness.bind(reserved(RUNNER), None).await;
    let mut request = v1_request("synthetic", "offered-wall");
    request["candidates"] = json!({"view_digest":"offered-only", "accounts":[
        {"account_id":"oauth-account","credential_id":"oauth:test"}
    ]});
    crate::take_operator_warnings();
    let refused = runner.call("route.account_for", &request).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_pool_exhausted");
    assert_eq!(refused.1["detail"]["reset_at_ms"], NOW + 900_000,
        "the retry still belongs to the offered account");
    let diagnostic = json!({"count":1,"credential_ids":["oauth:unoffered"]});
    assert_eq!(refused.1["detail"]["eligible_accounts_not_offered"], diagnostic);
    let refusal: cortexkit_role_routing::errors::Refusal = serde_json::from_value(refused.1).unwrap();
    let typed: cortexkit_role_routing::errors::ResetDetail = refusal.detail_as().unwrap();
    assert_eq!(typed.reset_at_ms, Some(NOW + 900_000));
    assert_eq!(refusal.wait_until_ms(), Some(NOW + 900_000));
    let lines = crate::take_operator_warnings();
    let fields: Value = serde_json::from_str(lines.iter().find_map(|line|
        line.strip_prefix("routing refused an account call: ")).expect("refusal log")).unwrap();
    assert_eq!(fields["eligible_accounts_not_offered"], diagnostic);
    assert_eq!(fields["offered_candidate_credential_ids"], json!(["oauth:test"]));
    assert_eq!(fields["reason"], "route_fence");
}

#[test]
fn contract_codes_map_with_contract_detail() {
    // Quota pool refusals carry known resets, including policy reserves.
    // Unavailability can also carry a short retry delay and candidate reasons.
    let hold = super::accounts::account_refusal_for_test_detail(
        "account_hold",
        Some(&json!({"retry_after_s": 30})),
    );
    assert_eq!(hold.code, RefusalCode::AccountHold);
    assert!(hold.wait_until_ms().is_some());
    let exhausted = super::accounts::account_refusal_for_test_detail(
        "account_pool_exhausted",
        Some(&json!({"reset_at_ms": NOW + 1, "retry_after_s": 5})),
    );
    assert_eq!(exhausted.code, RefusalCode::AccountPoolExhausted);
    assert_eq!(exhausted.wait_until_ms(), Some(NOW + 1));
    let reserve = super::accounts::account_refusal_for_test_detail(
        "account_pool_reserve_limited",
        Some(&json!({"reset_at_ms":NOW + 2,"retry_after_s":5})),
    );
    assert_eq!(reserve.detail["reset_at_ms"], json!(NOW + 2));
    // `invalid_request` names its field. With no allocator detail supplied,
    // the remaining codes below still have no detail or wait.
    let invalid = super::accounts::account_refusal_for_test("invalid_request");
    assert!(invalid.wait_until_ms().is_none());
    assert_eq!(
        invalid.detail.get("field").and_then(|field| field.as_str()),
        Some("params")
    );
    for code in [
        "unknown_decision",
        "account_not_authorized",
        "auth_required",
        "account_quota_unknown",
        "account_pool_reserve_limited",
        "account_selection_disabled",
        "account_selection_unavailable",
        "admission_conflict",
        "pin_generation_conflict",
    ] {
        let refusal = super::accounts::account_refusal_for_test(code);
        assert!(refusal.wait_until_ms().is_none(), "{code}");
        assert!(refusal.detail.is_empty(), "{code}");
    }
}

#[test]
fn undeclared_legacy_codes_become_unavailable() {
    // `account_selection_disabled` is intentionally excluded: the contract
    // declares it (0.5.2), so it keeps its own code instead of mapping here.
    // `account_selection_unsupported` is excluded too: with no cause
    // attached it means no credential of the right kind served the request,
    // so the backstop maps it to the untimed `auth_required`, never a
    // transient retry.
    for code in [
        "store_error",
        "internal_error",
        "no_eligible_account",
        "unscoped_caller",
    ] {
        let refusal = super::accounts::account_refusal_for_test(code);
        assert_eq!(
            refusal.code,
            RefusalCode::AccountSelectionUnavailable,
            "{code}"
        );
        assert!(refusal.detail.is_empty(), "{code}");
    }
    let unsupported = super::accounts::account_refusal_for_test("account_selection_unsupported");
    assert_eq!(unsupported.code, RefusalCode::AuthRequired);
    assert!(unsupported.detail.is_empty());
    assert_eq!(unsupported.wait_until_ms(), None);
}

#[tokio::test]
async fn class_aware_legacy_matches_v1_for_empty_and_fenced() {
    let _clock = crate::InjectedClock::set(NOW);
    // A credential fence names its next probe window on both entrypoints.
    {
        let (harness, credentials) = setup("synthetic", false);
        let shared = Arc::new(std::sync::Mutex::new(credentials));
        crate::credential_fences::record(
            harness.store(),
            &shared,
            &striatum_core::route_state_store::CredentialRefusal {
                provider_id: "synthetic".into(),
                credential_id: "oauth:test".into(),
                code: "ENTITLEMENT_ERROR".into(),
            },
            NOW as i64,
        )
        .unwrap();
        let runner = harness.bind(reserved(RUNNER), None).await;
        let refused = runner
            .call(
                "route.account_for",
                &v1_request("synthetic", "legacy-fenced"),
            )
            .await;
        assert_eq!(refused.1["code"], "account_pool_exhausted", "{}", refused.1);
        assert_eq!(
            refused.1["detail"]["reset_at_ms"],
            json!(NOW + 3_600_000),
            "{}",
            refused.1
        );
        let policy = harness.operator_config().allocator_policy("synthetic");
        let config = harness.operator_config();
        let classes = account_selection::ClassContext::load(
            harness.store(),
            &shared,
            config,
            crate::ACCOUNT_QUOTA.get(),
            NOW as i64,
        )
        .unwrap();
        let ctx = account_selection::Ctx {
            store: harness.store(),
            accounts: crate::ACCOUNT_QUOTA.get(),
            flags: crate::config_overrides::AccountSelectionFlags { enabled: true },
            policy: &policy,
            now_ms: NOW as i64,
            classes: Some(classes),
        };
        let body = serde_json::to_vec(&json!({"method": ACCOUNT_FOR_OP, "params": legacy_request("synthetic", "legacy-fenced", "conversation-legacy-fenced")})).unwrap();
        assert_eq!(
            account_selection::handle(ACCOUNT_FOR_OP, &body, &format!("reserved:{RUNNER}"), &ctx)
                .code(),
            Some("account_pool_exhausted"),
            "legacy class-aware fenced"
        );
    }
    // An unknown provider is a caller error on both entrypoints, not a login.
    {
        let (harness, credentials) = setup("synthetic", false);
        let shared = Arc::new(std::sync::Mutex::new(credentials));
        let runner = harness.bind(reserved(RUNNER), None).await;
        let refused = runner
            .call(
                "route.account_for",
                &v1_request("unknown-vendor", "legacy-unknown"),
            )
            .await;
        assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
        assert_eq!(refused.1["code"], "invalid_request", "{}", refused.1);
        assert_eq!(refused.1["detail"], json!({"field":"provider"}));
        let policy = harness.operator_config().allocator_policy("unknown-vendor");
        let config = harness.operator_config();
        let classes = account_selection::ClassContext::load(
            harness.store(),
            &shared,
            config,
            crate::ACCOUNT_QUOTA.get(),
            NOW as i64,
        )
        .unwrap();
        let ctx = account_selection::Ctx {
            store: harness.store(),
            accounts: crate::ACCOUNT_QUOTA.get(),
            flags: crate::config_overrides::AccountSelectionFlags { enabled: true },
            policy: &policy,
            now_ms: NOW as i64,
            classes: Some(classes),
        };
        let mut params = legacy_request("unknown-vendor", "legacy-unknown", "k");
        params["model"] = json!("model");
        let body =
            serde_json::to_vec(&json!({"method": ACCOUNT_FOR_OP, "params": params})).unwrap();
        assert_eq!(
            account_selection::handle(ACCOUNT_FOR_OP, &body, &format!("reserved:{RUNNER}"), &ctx)
                .code(),
            Some("invalid_request"),
            "legacy class-aware unknown provider"
        );
    }
}

#[tokio::test]
async fn policy_excluded_is_not_authorized_on_both_entrypoints() {
    // Operator policy for this request (size band) filters the class, so both
    // entrypoints refuse `account_not_authorized`.
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    let shared = Arc::new(std::sync::Mutex::new(credentials));
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let select = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30},"pin":"synthetic/model"}),
        )
        .await;
    assert_eq!(select.0, FrameType::Response, "{}", select.1);
    let report = caller
        .call(
            "route.report",
            &json!({"decision_id":select.1["result"]["decision_id"],"outcome":"refused","credential_id":"oauth:test","runner_code":"size_shaped_429","request_input_tokens":169_000,"reset_at_ms":NOW+600_000}),
        )
        .await;
    assert_eq!(report.0, FrameType::Response, "{}", report.1);
    let mut large = v1_request("synthetic", "policy-large");
    large["initial_input_bytes"] = json!(169_000 * 4);
    let refused = runner.call("route.account_for", &large).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_not_authorized", "{}", refused.1);
    assert_eq!(refused.1["detail"], json!({}), "{}", refused.1);
    let policy = harness.operator_config().allocator_policy("synthetic");
    let config = harness.operator_config();
    let classes = account_selection::ClassContext::load(
        harness.store(),
        &shared,
        config,
        crate::ACCOUNT_QUOTA.get(),
        NOW as i64,
    )
    .unwrap();
    let ctx = account_selection::Ctx {
        store: harness.store(),
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: crate::config_overrides::AccountSelectionFlags { enabled: true },
        policy: &policy,
        now_ms: NOW as i64,
        classes: Some(classes),
    };
    let mut legacy = legacy_request("synthetic", "policy-legacy", "policy-key");
    legacy["initial_input_bytes"] = json!(169_000 * 4);
    let body = serde_json::to_vec(&json!({"method": ACCOUNT_FOR_OP, "params": legacy})).unwrap();
    assert_eq!(
        account_selection::handle(ACCOUNT_FOR_OP, &body, &format!("reserved:{RUNNER}"), &ctx)
            .code(),
        Some("account_not_authorized"),
        "legacy class-aware size policy"
    );
}

#[tokio::test]
async fn mixed_fence_and_policy_is_exhausted_on_both_entrypoints() {
    // When causes mix, the most recoverable wins: a fence anywhere in the
    // class makes the empty roster an account limit, even when other
    // credentials are policy-filtered.
    let _clock = crate::InjectedClock::set(NOW);
    let (harness, credentials) = setup("synthetic", false);
    let shared = Arc::new(std::sync::Mutex::new(credentials));
    let caller = harness.bind(reserved(OWNER), None).await;
    let runner = harness.bind(reserved(RUNNER), None).await;
    let select = caller
        .call(
            "route.select",
            &json!({"consumer":"mason","requirements":{"iq":30},"pin":"synthetic/model"}),
        )
        .await;
    assert_eq!(select.0, FrameType::Response, "{}", select.1);
    let report = caller
        .call(
            "route.report",
            &json!({"decision_id":select.1["result"]["decision_id"],"outcome":"refused","credential_id":"oauth:test","runner_code":"size_shaped_429","request_input_tokens":169_000,"reset_at_ms":NOW+600_000}),
        )
        .await;
    assert_eq!(report.0, FrameType::Response, "{}", report.1);
    crate::credential_fences::record(
        harness.store(),
        &shared,
        &striatum_core::route_state_store::CredentialRefusal {
            provider_id: "synthetic".into(),
            credential_id: "oauth:test".into(),
            code: "ENTITLEMENT_ERROR".into(),
        },
        NOW as i64,
    )
    .unwrap();
    let mut large = v1_request("synthetic", "mixed-large");
    large["initial_input_bytes"] = json!(169_000 * 4);
    let refused = runner.call("route.account_for", &large).await;
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "account_pool_exhausted", "{}", refused.1);
    assert_eq!(
        refused.1["detail"]["reset_at_ms"],
        json!(NOW + 3_600_000),
        "{}",
        refused.1
    );
    let policy = harness.operator_config().allocator_policy("synthetic");
    let config = harness.operator_config();
    let classes = account_selection::ClassContext::load(
        harness.store(),
        &shared,
        config,
        crate::ACCOUNT_QUOTA.get(),
        NOW as i64,
    )
    .unwrap();
    let ctx = account_selection::Ctx {
        store: harness.store(),
        accounts: crate::ACCOUNT_QUOTA.get(),
        flags: crate::config_overrides::AccountSelectionFlags { enabled: true },
        policy: &policy,
        now_ms: NOW as i64,
        classes: Some(classes),
    };
    let mut legacy = legacy_request("synthetic", "mixed-legacy", "mixed-key");
    legacy["initial_input_bytes"] = json!(169_000 * 4);
    let body = serde_json::to_vec(&json!({"method": ACCOUNT_FOR_OP, "params": legacy})).unwrap();
    assert_eq!(
        account_selection::handle(ACCOUNT_FOR_OP, &body, &format!("reserved:{RUNNER}"), &ctx)
            .code(),
        Some("account_pool_exhausted"),
        "legacy class-aware mixed fence wins"
    );
}
