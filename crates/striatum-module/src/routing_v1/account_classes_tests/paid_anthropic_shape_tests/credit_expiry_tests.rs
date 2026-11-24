use super::*;
use crate::account_quota::{self, AccountQuotaState};
use crate::account_selection::rfc3339;
use sha2::{Digest, Sha256};

const VIEW: &str = "paid-anthropic-vault";
const ORGS: [&str; 2] = ["org-first", "org-second"];
const CONSOLE_ORGS: [&str; 2] = [
    "00000000-0000-4000-8000-000000000001",
    "00000000-0000-4000-8000-000000000002",
];

fn credit(remaining: i64, expires_in_ms: i64) -> Value {
    json!({
        "id": format!("credit-{expires_in_ms}"), "label": "API credit (Ultra)",
        "remaining": {"minor": remaining, "exponent": 2, "unit": "USD"},
        "total": {"minor": 20000, "exponent": 2, "unit": "USD"},
        "expiresAt": rfc3339(NOW as i64 + expires_in_ms),
        "funding": "subscription", "basis": "stated",
    })
}

fn console_reading(org: &str, credential_ref: &str, spend: Vec<Value>) -> Value {
    json!({
        "provider": "claude-console",
        "account": org,
        "accountInfo": {"orgName": "Example Org"},
        "credentialRef": credential_ref,
        "source": "vault",
        "fetchedAt": rfc3339(NOW as i64),
        "usage": {},
        "spend": spend,
    })
}

fn install_readings(links: [Option<&str>; 2], spend: [Option<Vec<Value>>; 2]) {
    let rows = ORGS
        .into_iter()
        .zip(spend)
        .filter_map(|(org, spend)| {
            Some(
                json!({"provider": "claude-console", "apiProvider": "anthropic", "account": org,
                "fetchedAt": rfc3339(NOW as i64), "spend": spend?}),
            )
        })
        .collect();
    install_quota(links, &[("claude-console", rows)]);
}

fn install_quota(links: [Option<&str>; 2], readings: &[(&str, Vec<Value>)]) {
    let mut quota = AccountQuotaState::default();
    account_quota::apply_roster_listing(
        &mut quota,
        &ScopedCredentialListing {
            view: VIEW.into(),
            credentials: KEYS
                .into_iter()
                .zip(links)
                .map(|(id, org)| ListedCredential {
                    id: id.into(),
                    auth_method: Some("apikey".into()),
                    serves: vec!["anthropic".into()],
                    org_id: org.map(str::to_owned),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        NOW as i64,
    );
    for (provider, rows) in readings {
        account_quota::apply_provider_reply(
            &mut quota,
            provider,
            &serde_json::to_vec(&json!({"result":rows})).unwrap(),
            NOW as i64,
        )
        .unwrap();
    }
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = quota;
}

fn request(id: &str) -> Value {
    let mut request = credential_only_request(id);
    offer_key_account_ids(&mut request);
    request
}

async fn refuse_first_key(broca: &super::super::super::conformance_tests::Route) {
    let admitted = result(
        broca
            .call("route.account_for", &single_key_request("spent-key"))
            .await,
    );
    assert_eq!(admitted["account"]["credential_id"], KEYS[0]);
    let refusal = observe_ok(
        broca,
        &json!({"event_id":"spent-credit", "kind":"credential_refused",
            "admission_id":"spent-key", "provider_code":"billing_exhausted"}),
    )
    .await;
    assert_eq!(
        refusal["effects"],
        json!([json!({
            "credential_id": KEYS[0], "effect": "credential_refused", "provider": POOL
        })
        .to_string()])
    );
    observe_ok(
        broca,
        &json!({"event_id":"spent-key-finished", "kind":"run_finished",
            "admission_id":"spent-key"}),
    )
    .await;
}

fn credit_fence_harness(direct_catalog: bool) -> Arc<Harness> {
    paid_anthropic_harness_with_catalog(
        ScopedCredentialListing {
            view: VIEW.into(),
            ..Default::default()
        },
        direct_catalog,
    )
}

#[tokio::test]
async fn unchanged_console_credit_does_not_admit_a_billing_probe() {
    for direct_catalog in [false, true] {
        let _clock = crate::InjectedClock::set(NOW);
        let harness = credit_fence_harness(direct_catalog);
        install_readings(
            [Some(ORGS[0]), None],
            [Some(vec![credit(80, 7_200_000)]), None],
        );
        let broca = harness.bind(reserved("broca"), None).await;
        refuse_first_key(&broca).await;

        let _later = crate::InjectedClock::set(NOW + 3_600_001);
        let held = broca
            .call("route.account_for", &single_key_request("no-probe"))
            .await;
        assert_eq!(held.0, FrameType::Error, "unchanged credit: {}", held.1);
        assert_eq!(held.1["code"], "account_pool_exhausted");
        assert!(held.1["detail"]["reset_at_ms"].is_null(), "{}", held.1);
        let fences = harness.store().credential_entitlement_fences().unwrap();
        assert_eq!(fences.len(), 1);
        assert_eq!(fences[0].console_credit_micros_at_refusal, Some(800_000));

        let _expired = crate::InjectedClock::set(NOW + 7_200_000);
        let held = broca
            .call("route.account_for", &single_key_request("after-expiry"))
            .await;
        assert_eq!(held.0, FrameType::Error, "expired credit: {}", held.1);
        assert_eq!(held.1["code"], "account_pool_exhausted");
    }
}

#[tokio::test]
async fn new_console_credit_lifts_billing_fence() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let links = [Some(ORGS[0]), None];
    install_readings(
        links,
        [
            Some(vec![credit(30, 7_200_000), credit(50, 7_200_001)]),
            None,
        ],
    );
    let broca = harness.bind(reserved("broca"), None).await;
    refuse_first_key(&broca).await;

    // The total is still $0.80 despite a change to the individual pools.
    install_readings(links, [Some(vec![credit(80, 7_200_000)]), None]);
    let still_held = broca
        .call("route.account_for", &single_key_request("same-total"))
        .await;
    assert_eq!(still_held.0, FrameType::Error, "{}", still_held.1);

    install_readings(links, [Some(vec![credit(5000, 7_200_000)]), None]);
    let admitted = result(
        broca
            .call("route.account_for", &single_key_request("new-credit"))
            .await,
    );
    assert_eq!(admitted["account"]["credential_id"], KEYS[0]);
    assert_ne!(admitted["reason"]["code"], "credential_fence_probe");
    assert!(harness
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .iter()
        .all(|fence| fence.provider_id != POOL || fence.credential_id != KEYS[0]));
}

#[tokio::test]
async fn billing_refusal_without_console_credit_reading_keeps_hourly_probe() {
    for (direct_catalog, link, spend) in [
        (false, None, Some(vec![credit(80, 7_200_000)])),
        (true, None, Some(vec![credit(80, 7_200_000)])),
        (true, Some(ORGS[0]), None),
    ] {
        let _clock = crate::InjectedClock::set(NOW);
        let harness = credit_fence_harness(direct_catalog);
        install_readings([link, None], [spend, None]);
        let broca = harness.bind(reserved("broca"), None).await;
        refuse_first_key(&broca).await;

        let _before = crate::InjectedClock::set(NOW + 3_599_999);
        let held = broca
            .call("route.account_for", &single_key_request("before-probe"))
            .await;
        assert_eq!(held.0, FrameType::Error, "{}", held.1);
        let (why, reset_at) = if direct_catalog {
            ("credential_refused", json!(NOW + 3_600_000))
        } else {
            ("pool_backoff", Value::Null)
        };
        assert_eq!(
            held.1["detail"]["exclusions"][0]["reason"], why,
            "{}",
            held.1
        );
        assert_eq!(held.1["detail"]["reset_at_ms"], reset_at, "{}", held.1);

        let _due = crate::InjectedClock::set(NOW + 3_600_000);
        let probe = result(
            broca
                .call("route.account_for", &single_key_request("probe"))
                .await,
        );
        assert_eq!(probe["account"]["credential_id"], KEYS[0]);
        assert_eq!(probe["reason"]["code"], "credential_fence_probe");
    }
}

#[tokio::test]
async fn billing_fence_uses_an_org_reading_without_a_provider_name_match() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let org = CONSOLE_ORGS[0];
    let mut reading = console_reading(org, "cookie:example", vec![credit(80, 7_200_000)]);
    reading["provider"] = json!("other-spend-feed");
    install_quota([Some(org), None], &[("other-spend-feed", vec![reading])]);
    let broca = harness.bind(reserved("broca"), None).await;
    refuse_first_key(&broca).await;

    let _later = crate::InjectedClock::set(NOW + 3_600_001);
    let held = broca
        .call(
            "route.account_for",
            &single_key_request("no-named-feed-probe"),
        )
        .await;
    assert_eq!(held.0, FrameType::Error, "{}", held.1);
    assert_eq!(held.1["code"], "account_pool_exhausted");
}

#[tokio::test]
async fn ambiguous_org_readings_keep_the_hourly_billing_probe() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let org = CONSOLE_ORGS[0];
    let mut other = console_reading(org, "cookie:example-2", vec![]);
    other["provider"] = json!("other-spend-feed");
    install_quota(
        [Some(org), None],
        &[
            (
                "claude-console",
                vec![console_reading(
                    org,
                    "cookie:example-1",
                    vec![credit(80, 7_200_000)],
                )],
            ),
            ("other-spend-feed", vec![other]),
        ],
    );
    let broca = harness.bind(reserved("broca"), None).await;
    refuse_first_key(&broca).await;

    let _later = crate::InjectedClock::set(NOW + 3_600_001);
    let probe = result(
        broca
            .call(
                "route.account_for",
                &single_key_request("ambiguous-org-probe"),
            )
            .await,
    );
    assert_eq!(probe["account"]["credential_id"], KEYS[0]);
    assert_eq!(probe["reason"]["code"], "credential_fence_probe");
}

#[tokio::test]
async fn soonest_positive_console_credit_wins_and_is_explained() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    install_readings(
        ORGS.map(Some),
        [
            Some(vec![credit(20000, 240_000)]),
            Some(vec![
                credit(20000, 360_000),
                credit(20000, -60_000),
                credit(0, 60_000),
                credit(19956, 120_000),
            ]),
        ],
    );
    let owner = harness.bind(reserved(OWNER), None).await;
    let broca = harness.bind(reserved("broca"), None).await;
    let selected = result(
        owner
            .call(
                "route.select",
                &json!({
                    "consumer":"codemode", "pin":format!("{POOL}/{MODEL}"), "requirements":{"iq":0}
                }),
            )
            .await,
    );
    let mut first = request("early-credit");
    first["decision_id"] = selected["decision_id"].clone();
    let admitted = result(broca.call("route.account_for", &first).await);
    assert_eq!(admitted["account"]["credential_id"], KEYS[1]);

    let expected_credit = json!({
        "expires_at_ms":NOW + 120_000,
        "remaining":{"minor":19956, "exponent":2, "unit":"USD"}
    });
    let stored = harness
        .store()
        .v1_bound_account_decision(
            selected["decision_id"].as_str().unwrap(),
            admitted["pin"]["id"].as_str().unwrap(),
            "early-credit",
        )
        .unwrap()
        .unwrap();
    let stored: Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(stored["explanation"]["credit_expiry"], expected_credit);

    let explanation = result(
        owner
            .call(
                "route.explain",
                &json!({
                    "decision_id":selected["decision_id"]
                }),
            )
            .await,
    );
    assert_eq!(
        explanation["serving"]["credential_id"], KEYS[1],
        "{explanation}"
    );
    assert_eq!(
        explanation["credit_expiry"], expected_credit,
        "{explanation}"
    );

    let mut next = first;
    next["admission_id"] = json!("keep-credit-key");
    let retained = result(broca.call("route.account_for", &next).await);
    assert_eq!(retained["pin"]["id"], admitted["pin"]["id"]);
    let explanation = result(
        owner
            .call(
                "route.explain",
                &json!({"decision_id":selected["decision_id"]}),
            )
            .await,
    );
    assert_eq!(explanation["serving"]["credential_id"], KEYS[1]);
    assert!(
        explanation.get("credit_expiry").is_none(),
        "retaining a pin did not rank credits again: {explanation}"
    );
}

fn install_legacy_billing_wall(harness: &Harness, observed_at: i64) {
    harness
        .store()
        .record_credential_entitlement_fence(
            &CredentialRefusal {
                provider_id: POOL.into(),
                credential_id: KEYS[0].into(),
                code: "billing_exhausted".into(),
            },
            None,
            observed_at,
        )
        .unwrap();
    harness
        .store()
        .account_transaction(|tx| {
            assert!(!tx.classify_credential_quota(POOL, KEYS[0], false)?);
            let wall = &tx.fence_rechecks()?[0];
            assert!(wall.classified && !wall.quota);
            assert_eq!(wall.console_credit_micros_at_refusal, None);
            assert_eq!(wall.next_recheck_at, observed_at + 3_600_000);
            assert!(wall.last_claimed_at.is_none());
            Ok(())
        })
        .unwrap();
}

fn install_early_and_late_credit() {
    install_readings(
        ORGS.map(Some),
        [
            Some(vec![credit(20_000, 7_200_000)]),
            Some(vec![credit(10_000, 10_800_000)]),
        ],
    );
}

#[tokio::test]
async fn legacy_billing_probe_can_win_earlier_credit_and_heal() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    install_legacy_billing_wall(&harness, NOW as i64 - 3_600_001);
    install_early_and_late_credit();
    let broca = harness.bind(reserved("broca"), None).await;
    let admit = result(
        broca
            .call("route.account_for", &request("probe-credit"))
            .await,
    );
    assert_eq!(
        admit["account"]["credential_id"], KEYS[0],
        "the due earlier-credit probe should beat the healthy later-expiry key: {admit}"
    );
    assert_eq!(admit["reason"]["code"], "credential_fence_probe");
    let decision = harness
        .store()
        .account_decision(admit["reason"]["decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    let decision: Value = serde_json::from_str(&decision.decision_json).unwrap();
    assert_eq!(
        decision["explanation"]["credit_expiry"]["expires_at_ms"],
        NOW + 7_200_000
    );
    harness
        .store()
        .account_read(|tx| {
            let clocks = tx.fence_rechecks()?;
            assert_eq!(clocks.len(), 1);
            assert_eq!(clocks[0].last_claimed_at, Some(NOW as i64));
            assert_eq!(clocks[0].next_recheck_at, NOW as i64 + 3_600_000);
            assert_eq!(tx.probe_authorization_owners()?.len(), 1);
            Ok(())
        })
        .unwrap();

    observe_ok(&broca, &success_observation("probe-credit", true)).await;
    let ordinary = result(
        broca
            .call("route.account_for", &request("healed-credit"))
            .await,
    );
    assert_eq!(ordinary["account"]["credential_id"], KEYS[0]);
    assert_eq!(ordinary["reason"]["code"], "cold_scored");
    assert!(harness
        .store()
        .credential_entitlement_fences()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn legacy_billing_credit_opens_one_probe_before_due() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    install_legacy_billing_wall(&harness, NOW as i64 - 1_000);
    install_early_and_late_credit();
    let broca = harness.bind(reserved("broca"), None).await;
    let other = harness.bind(reserved("broca"), None).await;
    let (left, right) = (
        request("early-credit-probe"),
        request("racing-credit-probe"),
    );
    let (left, right) = tokio::join!(
        broca.call("route.account_for", &left),
        other.call("route.account_for", &right),
    );
    let mut picks = [result(left), result(right)];
    picks.sort_by_key(|pick| pick["account"]["credential_id"] != KEYS[0]);
    assert_eq!(picks[0]["account"]["credential_id"], KEYS[0], "{picks:?}");
    assert_eq!(picks[0]["reason"]["code"], "credential_fence_probe");
    assert_eq!(picks[1]["account"]["credential_id"], KEYS[1], "{picks:?}");
    assert_eq!(picks[1]["reason"]["code"], "cold_scored");
    harness
        .store()
        .account_read(|tx| {
            assert_eq!(tx.probe_authorization_owners()?.len(), 1);
            Ok(())
        })
        .unwrap();
}

#[tokio::test]
async fn withdrawn_credit_leaves_the_original_recheck_until_a_probe_is_claimed() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let observed = NOW - 1_000;
    install_legacy_billing_wall(&harness, observed as i64);
    install_early_and_late_credit();
    let broca = harness.bind(reserved("broca"), None).await;
    let mut healthy_only = single_key_request("credit-not-offered");
    healthy_only["candidates"]["accounts"][0] =
        json!({"account_id": KEYS[1], "credential_id": KEYS[1]});
    let healthy = result(broca.call("route.account_for", &healthy_only).await);
    assert_eq!(healthy["account"]["credential_id"], KEYS[1]);
    harness
        .store()
        .account_read(|tx| {
            let wall = &tx.fence_rechecks()?[0];
            assert_eq!(wall.next_recheck_at, observed as i64 + 3_600_000);
            assert!(wall.last_claimed_at.is_none());
            assert!(tx.probe_authorization_owners()?.is_empty());
            Ok(())
        })
        .unwrap();

    install_readings(
        ORGS.map(Some),
        [None, Some(vec![credit(10_000, 10_800_000)])],
    );
    let reply = broca
        .call("route.account_for", &single_key_request("credit-withdrawn"))
        .await;
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["detail"]["reset_at_ms"], observed + 3_600_000);

    let _due = crate::InjectedClock::set(observed + 3_600_000);
    let due = result(
        broca
            .call("route.account_for", &single_key_request("ordinary-recheck"))
            .await,
    );
    assert_eq!(due["reason"]["code"], "credential_fence_probe");
}

#[tokio::test]
async fn repeated_legacy_billing_refusal_at_flat_credit_is_not_expedited() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    install_legacy_billing_wall(&harness, NOW as i64 - 3_600_001);
    install_early_and_late_credit();
    let broca = harness.bind(reserved("broca"), None).await;
    let admitted = result(
        broca
            .call("route.account_for", &request("repeat-billing"))
            .await,
    );
    assert_eq!(admitted["account"]["credential_id"], KEYS[0]);
    assert_eq!(admitted["reason"]["code"], "credential_fence_probe");
    let _after = crate::InjectedClock::set(NOW + 1);
    observe_ok(
        &broca,
        &json!({"event_id":"repeat-billing-refusal", "kind":"credential_refused",
            "admission_id":"repeat-billing", "provider_code":"billing_exhausted"}),
    )
    .await;
    let fences = harness.store().credential_entitlement_fences().unwrap();
    assert_eq!(fences.len(), 1);
    assert_eq!(
        fences[0].console_credit_micros_at_refusal,
        Some(200_000_000)
    );
    let healthy = result(
        broca
            .call("route.account_for", &request("flat-credit"))
            .await,
    );
    assert_eq!(healthy["account"]["credential_id"], KEYS[1]);
    let only_refused = broca
        .call("route.account_for", &single_key_request("flat-refused"))
        .await;
    assert_eq!(only_refused.0, FrameType::Error, "{}", only_refused.1);
}

#[tokio::test]
async fn keys_without_live_console_credit_fall_back() {
    let _clock = crate::InjectedClock::set(NOW);
    for (label, link, spend) in [
        ("exhausted", Some(ORGS[0]), Some(vec![credit(0, 60_000)])),
        ("expired", Some(ORGS[0]), Some(vec![credit(20000, -1_000)])),
        ("no-reading", Some(ORGS[0]), None),
        ("unlinked", None, Some(vec![credit(20000, 60_000)])),
    ] {
        let harness = paid_anthropic_harness();
        install_readings(
            [link, Some(ORGS[1])],
            [spend, Some(vec![credit(100, 120_000)])],
        );
        let broca = harness.bind(reserved("broca"), None).await;
        let admitted = result(broca.call("route.account_for", &request(label)).await);
        assert_eq!(admitted["account"]["credential_id"], KEYS[1], "{label}");
    }
}

#[tokio::test]
async fn console_credit_links_by_org_without_api_provider() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    install_quota(
        CONSOLE_ORGS.map(Some),
        &[(
            "claude-console",
            vec![
                console_reading(
                    CONSOLE_ORGS[0],
                    "cookie:example-1",
                    vec![credit(15833, 240_000)],
                ),
                console_reading(
                    CONSOLE_ORGS[1],
                    "cookie:example-2",
                    vec![credit(15833, 120_000)],
                ),
            ],
        )],
    );
    let broca = harness.bind(reserved("broca"), None).await;
    let admitted = result(broca.call("route.account_for", &request("org-only")).await);
    assert_eq!(admitted["account"]["credential_id"], KEYS[1]);
}

const RESTORED_KEYS: [&str; 2] = ["apikey:anthropic:example-a", "apikey:anthropic:example-b"];
const RESTORED_VIEW: &str = "example-routing-listing";

fn caller_bearing_admission_after_restore(org_ids: [Option<&str>; 2]) -> (Value, String) {
    use crate::account_selection::{ClassContext, Ctx, OpReply, ACCOUNT_FOR_OP};
    use std::sync::Mutex;

    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let store = harness.store();
    let listing = ScopedCredentialListing {
        view: RESTORED_VIEW.into(),
        credentials: RESTORED_KEYS
            .into_iter()
            .zip(org_ids)
            .map(|(key, org)| ListedCredential {
                id: key.into(),
                org_id: org.map(str::to_owned),
                state: "active".into(),
                categories: vec!["llm-provider".into()],
                provider_ids: vec![POOL.into()],
                serves: vec!["anthropic".into()],
                credential_type: "apikey".into(),
                auth_method: Some("apikey".into()),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let mut previous = AccountQuotaState::default();
    let row = account_quota::apply_roster_listing(&mut previous, &listing, NOW as i64).unwrap();
    crate::account_writer::put_account_vault_roster(store, &row).unwrap();
    let readings = CONSOLE_ORGS
        .into_iter()
        .zip([300_000, 120_000])
        .enumerate()
        .map(|(index, (org, expires_in_ms))| {
            let mut pool = credit(1_234, expires_in_ms);
            pool["basis"] = json!("reported");
            console_reading(
                org,
                &format!("cookie:platform.example:org-{index}"),
                vec![pool],
            )
        })
        .collect::<Vec<_>>();
    let (rows, completeness) = account_quota::apply_usage_reply(
        &mut previous,
        &serde_json::to_vec(&json!({"result": readings})).unwrap(),
        NOW as i64,
    )
    .unwrap();
    store.put_account_quota_poll(&rows, &completeness).unwrap();

    let accounts = Arc::new(Mutex::new(account_quota::restore_account_state(store)));
    let credentials = harness.credentials();
    account_quota::restore_credential_listing(store, &mut credentials.lock().unwrap());
    let config = harness.operator_config();
    let policy = config.allocator_policy(POOL);
    let ctx = Ctx {
        store,
        accounts: Some(&accounts),
        flags: config.account_flags(),
        policy: &policy,
        now_ms: NOW as i64,
        classes: Some(
            ClassContext::load(store, &credentials, config, Some(&accounts), NOW as i64).unwrap(),
        ),
    };
    let body = serde_json::to_vec(&json!({"method": ACCOUNT_FOR_OP, "params": {
        "provider": POOL, "model": MODEL, "mode": "admit",
        "caller": {"runner": "broca"},
        "conversation_key": "example-first-conversation", "admission_id": "example-first-admit",
        "candidates": {"view_digest": "example-runner-listing", "accounts":
            RESTORED_KEYS.map(|key| json!({"account_id": key, "credential_id": key}))},
    }}))
    .unwrap();
    assert!(!crate::routing_v1::claims(&body));
    let admitted = match crate::account_selection::handle_with_call(
        ACCOUNT_FOR_OP,
        &body,
        "reserved:broca",
        &ctx,
        None,
    ) {
        OpReply::Result(value) => value,
        OpReply::Error { code, message, .. } => panic!("{code}: {message}"),
    };
    let stored = store
        .account_decision(admitted["reason"]["decision_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    (admitted, stored.decision_json)
}

#[test]
fn caller_bearing_admission_spends_earliest_restored_console_credit() {
    let (admitted, stored) = caller_bearing_admission_after_restore(CONSOLE_ORGS.map(Some));
    let decision: Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(
        decision["explanation"]["roster"]["view_digest"],
        RESTORED_VIEW
    );
    let accounts = decision["explanation"]["accounts"].as_array().unwrap();
    assert_eq!(accounts.len(), RESTORED_KEYS.len());
    for account in accounts {
        assert_eq!(account["exclusions"], json!([{"reason": "quota_missing"}]));
    }
    assert_eq!(admitted["account"]["credential_id"], RESTORED_KEYS[1]);
    assert_eq!(
        decision["explanation"]["credit_expiry"],
        json!({
            "expires_at_ms": NOW + 120_000,
            "remaining": {"minor": 1_234, "exponent": 2, "unit": "USD"},
        })
    );
}

#[test]
fn caller_bearing_admission_without_org_links_keeps_its_decision() {
    let (admitted, stored) = caller_bearing_admission_after_restore([None; 2]);
    assert_eq!(admitted["account"]["credential_id"], RESTORED_KEYS[0]);
    // This digest was captured from the legacy handler before it applied organization credit.
    assert_eq!(
        format!("{:x}", Sha256::digest(&stored)),
        "abc42fa1bce8d98ade5a6b81771512b247a79b6aab1e5844b331800a216fbb41",
        "{stored}"
    );
}

#[tokio::test]
async fn console_credit_is_unknown_when_current_readings_share_an_org() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let org = CONSOLE_ORGS[1];
    // Polls keep one reading per provider and account. Use two providers so
    // both readings remain current, even though one offers no spend credit.
    let mut other = console_reading(org, "cookie:example-2", vec![]);
    other["provider"] = json!("other-spend-feed");
    install_quota(
        CONSOLE_ORGS.map(Some),
        &[
            (
                "claude-console",
                vec![console_reading(
                    org,
                    "cookie:example-1",
                    vec![credit(15833, 120_000)],
                )],
            ),
            ("other-spend-feed", vec![other]),
        ],
    );
    let broca = harness.bind(reserved("broca"), None).await;
    let admitted = result(
        broca
            .call("route.account_for", &request("ambiguous-org"))
            .await,
    );
    assert_eq!(admitted["account"]["credential_id"], KEYS[0]);
}

#[tokio::test]
async fn console_credit_ties_and_unlinked_pool_keep_existing_order() {
    let _clock = crate::InjectedClock::set(NOW);
    for (label, links) in [("no-links", [None; 2]), ("tie", ORGS.map(Some))] {
        let harness = paid_anthropic_harness();
        // The second key offers much more credit; amount is not a tie-breaker.
        install_readings(
            links,
            [
                Some(vec![credit(10, 120_000)]),
                Some(vec![credit(20000, 120_000)]),
            ],
        );
        let broca = harness.bind(reserved("broca"), None).await;
        let admitted = result(broca.call("route.account_for", &request(label)).await);
        assert_eq!(admitted["account"]["credential_id"], KEYS[0], "{label}");
    }
}

#[tokio::test]
async fn console_credit_does_not_move_existing_affinity() {
    let _clock = crate::InjectedClock::set(NOW);
    let harness = paid_anthropic_harness();
    let broca = harness.bind(reserved("broca"), None).await;
    let first = request("initial");
    let initial = result(broca.call("route.account_for", &first).await);
    assert_eq!(initial["account"]["credential_id"], KEYS[0]);

    install_readings(
        ORGS.map(Some),
        [
            Some(vec![credit(20000, 240_000)]),
            Some(vec![credit(20000, 120_000)]),
        ],
    );
    let mut second = request("sticky");
    second["conversation_key"] = first["conversation_key"].clone();
    let sticky = result(broca.call("route.account_for", &second).await);
    assert_eq!(sticky["account"]["credential_id"], KEYS[0]);
    assert_eq!(sticky["pin"]["id"], initial["pin"]["id"]);
}

#[tokio::test]
async fn learned_vault_org_links_console_credit_without_changing_key_identity() {
    const EXPORTED: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/claustrum-list-scoped-v2/list-scoped-v2.json"
    ));
    const ORG: &str = "c57dea1d-dfac-4ad7-9dee-123456789abc";
    const LINKED_KEY: &str = "apikey:anthropic:work";

    let _clock = crate::InjectedClock::set(NOW);
    assert_eq!(
        format!("{:x}", Sha256::digest(EXPORTED)),
        "4ff7e45c2839f71873c1ca12ffe5ea3e15d35897e3610bb884a26a6f0329b691"
    );
    let vectors: Value = serde_json::from_slice(EXPORTED).unwrap();
    let reply = &vectors["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["case"] == "learned_org")
        .unwrap()["reply"];
    let listing = cortexkit_claustrum_client::decode_list_scoped_reply(reply.clone()).unwrap();
    assert!(listing.undecodable_credentials.is_empty());
    assert_eq!(listing.credentials.len(), 1);
    assert_eq!(listing.credentials[0].id, LINKED_KEY);
    assert_eq!(listing.credentials[0].account_id, None);
    assert_eq!(listing.credentials[0].org_id.as_deref(), Some(ORG));
    let harness = paid_anthropic_harness_with_listing(listing.clone());

    let mut quota = AccountQuotaState::default();
    let row = account_quota::apply_roster_listing(&mut quota, &listing, NOW as i64).unwrap();
    let persisted: Value = serde_json::from_str(&row.roster_json).unwrap();
    assert_eq!(persisted["listing"]["credentials"][0]["org_id"], ORG);
    assert_eq!(
        quota.credential_identities[LINKED_KEY].account_id,
        LINKED_KEY
    );
    account_quota::apply_provider_reply(
        &mut quota,
        "claude-console",
        &serde_json::to_vec(&json!({
            "result": [{"provider":"claude-console", "apiProvider": "anthropic", "account": ORG,
                "fetchedAt":rfc3339(NOW as i64), "spend":[credit(19956, 120_000)]}]
        }))
        .unwrap(),
        NOW as i64,
    )
    .unwrap();
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = quota;

    let broca = harness.bind(reserved("broca"), None).await;
    let mut request = request("learned-console-org");
    request["candidates"] = json!({"view_digest": listing.view, "accounts": [
        {"credential_id": KEYS[0], "account_id": KEYS[0]},
        {"credential_id":LINKED_KEY, "account_id": LINKED_KEY},
    ]});
    let admitted = result(broca.call("route.account_for", &request).await);
    assert_eq!(admitted["account"]["credential_id"], LINKED_KEY);
    assert_eq!(admitted["account"]["account_id"], LINKED_KEY);
}
