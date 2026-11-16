//! A quota route fence on an Antigravity model is held and cleared by the
//! Insula pool that model draws on, never by another pool of the same account.
//!
//! The rows reproduce two Google sign-ins serving Gemini 4 Carbon. One account
//! has spent its third-party (Claude and GPT) weekly pool but has Gemini
//! headroom; the other has spent its Gemini weekly pool. Both carry a quota
//! route fence on Carbon.

use std::sync::Arc;

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
use serde_json::{json, Value};
use striatum_core::accounts::Outcome;
use striatum_core::billing::{BillingPlane, BillingRate};
use striatum_core::fence_lifetimes::QUOTA_FENCE_RECHECK_MS;

use super::conformance_tests::{reserved, Harness};
use crate::broca_credentials::BrocaCredentials;

const NOW: i64 = 1_794_828_400_000;
const CARBON: &str = "antigravity-gemini-4-carbon";
const OPEN: &str = "antigravity:google";
const OPEN_ACCOUNT: &str = "open-account@example.com";
const SPENT: &str = "antigravity:google:grace";
const SPENT_ACCOUNT: &str = "spent-account@example.com";
/// The open account's third-party weekly reset; it says nothing about Gemini.
const THIRD_PARTY_RESET: i64 = 1_795_037_010_000;
const OPEN_GEMINI_RESET: i64 = 1_795_244_492_000;
const SPENT_GEMINI_RESET: i64 = 1_795_216_704_000;
const RUNNER: &str = "carbon-runner";
const OWNER: &str = "carbon-owner";

fn setup() -> Arc<Harness> {
    let harness = Harness::new("route-fence-pools");
    let mut fact = crate::tests::fact("google", CARBON);
    fact.auth_methods = Some(vec!["oauth".into()]);
    fact.billing_plane = Some(BillingPlane {
        provider_id: "google".into(),
        auth_method: "oauth".into(),
        kind: "subscription".into(),
        selection_reason: "test subscription plane".into(),
        rates: [("rate.output".into(), BillingRate::Priced { rate: 1.0 })]
            .into_iter()
            .collect(),
    });
    harness.install_facts(&[fact.clone()]);
    super::account_classes_tests::catalog_fact(&harness, &fact);
    let mut credentials = BrocaCredentials::default();
    credentials.apply_listing(
        &ScopedCredentialListing {
            view: "two-google-sign-ins".into(),
            credentials: [(OPEN, OPEN_ACCOUNT), (SPENT, SPENT_ACCOUNT)]
                .into_iter()
                .map(|(id, account)| ListedCredential {
                    id: id.into(),
                    account_id: Some(account.into()),
                    state: "active".into(),
                    categories: vec!["llm-provider".into()],
                    provider_ids: vec!["google".into()],
                    serves: vec!["google".into()],
                    credential_type: "oauth".into(),
                    auth_method: Some("oauth".into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
        NOW as u64,
    );
    credentials.refresh_serving_methods(&Default::default(), &[fact.clone()], NOW, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&[fact]),
    );
    credentials.publish_plan_prices(&json!({"prices":[]}));
    harness.set_credentials(credentials);
    harness.edit_config(|config| {
        config["runners"] = json!([{"module":RUNNER,"via_vault":true}]);
        config["consumers"] = json!({"athena":{"owner":OWNER,"mode":"demand"}});
    });
    harness
}

fn window(id: &str, used: f64, minutes: i64, reset: i64) -> Value {
    json!({"id": id, "window": {"usedPercent": used, "windowMinutes": minutes,
        "resetsAt": crate::account_selection::rfc3339(reset)}})
}

/// The open account's windows as Insula reads them, with its Gemini weekly
/// pool at `gemini_weekly` percent used, resetting at `gemini_reset`.
fn open_windows(gemini_weekly: f64, gemini_reset: i64) -> Vec<Value> {
    vec![
        window("gemini-weekly", gemini_weekly, 10080, gemini_reset),
        window("gemini-5h", 2.79, 300, NOW + 16_600_000),
        window("3p-weekly", 100.0, 10080, THIRD_PARTY_RESET),
    ]
}

fn spent_windows() -> Vec<Value> {
    vec![
        window("gemini-weekly", 100.0, 10080, SPENT_GEMINI_RESET),
        window("3p-weekly", 100.0, 10080, 1_795_176_704_000),
    ]
}

/// Readings of both accounts, fetched at `fetched_at`. The state is the
/// test's own, so tests running in parallel cannot replace it.
fn readings(
    fetched_at: i64,
    open: Vec<Value>,
    spent: Vec<Value>,
) -> crate::account_quota::SharedAccountQuota {
    let entry = |account: &str, windows: Vec<Value>| {
        json!({"provider": "antigravity", "apiProvider": "google", "account": account,
            "source": "oauth", "fetchedAt": crate::account_selection::rfc3339(fetched_at),
            "usage": {"extraRateWindows": windows}})
    };
    let body = json!({"result": [entry(OPEN_ACCOUNT, open), entry(SPENT_ACCOUNT, spent)],
        "completeProviders": ["antigravity"]});
    let mut state = crate::account_quota::AccountQuotaState::default();
    crate::account_quota::apply_provider_reply(
        &mut state,
        "antigravity",
        &serde_json::to_vec(&body).unwrap(),
        fetched_at,
    )
    .unwrap();
    Arc::new(std::sync::Mutex::new(state))
}

/// Insula's latest readings of both accounts as exported, fetched at
/// `fetched_at`: the open account's Gemini pool at 12.23% and its third-party
/// pool spent; the other account's Gemini and third-party pools both spent.
fn read_accounts(fetched_at: i64) -> crate::account_quota::SharedAccountQuota {
    readings(
        fetched_at,
        open_windows(12.23, OPEN_GEMINI_RESET),
        spent_windows(),
    )
}

/// A Carbon quota refusal on `credential`, first recorded at `first` and
/// reported again at `last`, carrying `until` as the provider's reset.
fn fence_carbon(harness: &Harness, credential: &str, first: i64, last: i64, until: i64) {
    for at in [first, last] {
        harness
            .store()
            .record_account_route_fence(&striatum_store::AccountRouteFence {
                provider: "google".into(),
                credential_id: credential.into(),
                model: CARBON.into(),
                until_ms: until,
                decision_id: format!("report-{credential}-{at}"),
                recorded_at: at,
            })
            .unwrap();
    }
}

fn load(
    harness: &Harness,
    quota: &crate::account_quota::SharedAccountQuota,
    now: i64,
) -> crate::account_selection::ClassContext {
    crate::account_selection::ClassContext::load(
        harness.store(),
        &harness.credentials(),
        harness.operator_config(),
        Some(quota),
        now,
    )
    .unwrap()
}

/// The account a Carbon request is admitted to, or `None` when refused.
fn carbon_account(context: &crate::account_selection::ClassContext) -> Option<String> {
    match context
        .probe("google", CARBON, false, false, None, "carbon-conversation")
        .outcome
    {
        Outcome::Admit(admission) => Some(admission.account_id),
        _ => None,
    }
}

fn fenced_credentials(harness: &Harness, now: i64) -> Vec<String> {
    harness
        .store()
        .account_route_fences(now)
        .unwrap()
        .into_iter()
        .filter(|fence| fence.model == CARBON)
        .map(|fence| fence.credential_id)
        .collect()
}

/// The published account state equals a fresh load of the store's rows.
fn assert_memory_matches_store(harness: &Harness, step: &str) {
    crate::account_state::assert_published_equals_fresh_load(harness.store(), step);
}

/// Rule: any fresh reading under 100% on the hold's own pool clears it at
/// once, whatever its provider reset says and however full other pools are.
#[test]
fn gemini_headroom_clears_a_carbon_fence_that_a_spent_third_party_pool_never_held() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    for gemini_weekly in [12.23, 99.5] {
        let harness = setup();
        // The live rows: the open account's fence was last reported three
        // days ago; the spent account's fence was reported this morning.
        fence_carbon(
            &harness,
            OPEN,
            1_794_459_863_824,
            1_794_579_994_214,
            THIRD_PARTY_RESET,
        );
        fence_carbon(
            &harness,
            SPENT,
            1_794_819_352_720,
            1_794_819_352_720,
            SPENT_GEMINI_RESET,
        );
        let quota = readings(
            NOW - 5_000,
            open_windows(gemini_weekly, OPEN_GEMINI_RESET),
            spent_windows(),
        );
        assert_memory_matches_store(&harness, "seeded");
        let context = load(&harness, &quota, NOW);
        assert_memory_matches_store(&harness, "after the reading was applied");
        assert_eq!(
            fenced_credentials(&harness, NOW),
            vec![SPENT.to_owned()],
            "Gemini at {gemini_weekly}%: the reading clears the open account's Carbon \
             fence long before its stored reset; the spent Gemini pool keeps the other"
        );
        if gemini_weekly < 50.0 {
            assert_eq!(carbon_account(&context).as_deref(), Some(OPEN_ACCOUNT));
        }
    }
}

/// Whether the open account's Carbon recheck has come due at `NOW`. Its fence
/// was reported 20 minutes ago, so one recheck interval has passed. The other
/// account's Carbon fence was reported a minute ago and is not due, so a due
/// recheck admits the open account as the probe, and a held one leaves
/// Carbon with no account.
fn open_recheck_due(gemini_weekly: f64, gemini_reset: i64, fetched_at: i64) -> bool {
    let harness = setup();
    let reported = NOW - 20 * 60_000;
    fence_carbon(&harness, OPEN, reported, reported, THIRD_PARTY_RESET);
    fence_carbon(
        &harness,
        SPENT,
        NOW - 60_000,
        NOW - 60_000,
        SPENT_GEMINI_RESET,
    );
    let quota = readings(
        fetched_at,
        open_windows(gemini_weekly, gemini_reset),
        spent_windows(),
    );
    let context = load(&harness, &quota, NOW);
    assert_memory_matches_store(&harness, "after the recheck was judged");
    assert_eq!(
        fenced_credentials(&harness, NOW),
        vec![OPEN.to_owned(), SPENT.to_owned()],
        "a spent own pool never clears a fence"
    );
    carbon_account(&context).as_deref() == Some(OPEN_ACCOUNT)
}

/// A current reading showing the fence's own pool spent is evidence the pool
/// is still spent, so it holds the 15-minute probe back.
#[test]
fn a_current_spent_own_pool_reading_holds_the_probe_back() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    assert!(!open_recheck_due(
        100.0,
        OPEN_GEMINI_RESET,
        NOW - 5 * 60_000
    ));
}

/// A reading that stopped arriving never holds the probe: once the spent
/// reading is no longer fresh (older than routing's 20-minute band), the
/// probe is due again.
#[test]
fn a_spent_own_pool_reading_no_longer_fresh_lets_the_probe_run() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    assert!(open_recheck_due(
        100.0,
        OPEN_GEMINI_RESET,
        NOW - 21 * 60_000
    ));
}

/// A spent reading whose own reset has passed says nothing about the pool
/// now: the probe is due without waiting for the next reading.
#[test]
fn a_spent_own_pool_reading_past_its_reset_lets_the_probe_run() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    assert!(open_recheck_due(100.0, NOW - 60_000, NOW - 5 * 60_000));
}

#[test]
fn a_spent_third_party_pool_never_postpones_the_carbon_recheck() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let harness = setup();
    // Both refusals arrive after the latest reading, so the reading cannot
    // clear them; only the quota recheck can.
    let reported = NOW - QUOTA_FENCE_RECHECK_MS - 60_000;
    let quota = read_accounts(reported - 30_000);
    fence_carbon(&harness, OPEN, reported, reported, THIRD_PARTY_RESET);
    fence_carbon(&harness, SPENT, reported, reported, SPENT_GEMINI_RESET);
    let before = load(&harness, &quota, reported + 60_000);
    assert_eq!(
        carbon_account(&before),
        None,
        "inside the recheck interval the fences hold"
    );
    let due = load(&harness, &quota, NOW);
    assert_memory_matches_store(&harness, "after the recheck came due");
    assert_eq!(
        carbon_account(&due).as_deref(),
        Some(OPEN_ACCOUNT),
        "the open account's Gemini pool has headroom, so its Carbon recheck is due"
    );
    assert_eq!(
        fenced_credentials(&harness, NOW),
        vec![OPEN.to_owned(), SPENT.to_owned()],
        "a due recheck admits a probe; it clears nothing by itself"
    );
    let probes: Vec<_> = harness
        .store()
        .fence_rechecks()
        .unwrap()
        .into_iter()
        .filter(|origin| origin.kind == "account_route")
        .collect();
    assert_eq!(probes.len(), 2, "{probes:?}");
}

/// Runners may send without account admission, so the selected decision must
/// own the recovery probe: selecting Carbon claims the open account's overdue
/// recheck for that decision, and the spent account's recheck stays unclaimed.
/// The probe's success then clears the fence, long before its stored reset.
#[tokio::test]
async fn selecting_carbon_claims_the_due_recheck_and_its_success_clears_the_fence() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let harness = setup();
    let reported = NOW - QUOTA_FENCE_RECHECK_MS - 60_000;
    let quota = read_accounts(reported - 30_000);
    fence_carbon(&harness, OPEN, reported, reported, THIRD_PARTY_RESET);
    fence_carbon(&harness, SPENT, reported, reported, SPENT_GEMINI_RESET);
    // Selection reads the process-wide reading state.
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = quota.lock().unwrap().clone();
    let caller = harness.bind(reserved(OWNER), None).await;
    let selected = caller
        .call(
            "route.select",
            &json!({"consumer":"athena","requirements":{"iq":30},"pin":format!("google/{CARBON}")}),
        )
        .await;
    assert_eq!(
        selected.0,
        subc_protocol::FrameType::Response,
        "{}",
        selected.1
    );
    let decision = selected.1["result"]["decision_id"].as_str().unwrap();
    assert_memory_matches_store(&harness, "after selection claimed the probe");
    let rechecks = harness.store().fence_rechecks().unwrap();
    let claim = |credential: &str| {
        rechecks
            .iter()
            .find(|origin| {
                origin.kind == "account_route"
                    && origin.credential_id.as_deref() == Some(credential)
            })
            .map(|origin| (origin.claim_owner.clone(), origin.last_claimed_at))
            .unwrap()
    };
    assert_eq!(claim(OPEN), (Some(decision.to_owned()), Some(NOW)));
    assert_eq!(claim(SPENT), (None, None));
    let reported = caller
        .call(
            "route.report",
            &json!({"decision_id":decision,"outcome":"completed"}),
        )
        .await;
    assert_eq!(
        reported.0,
        subc_protocol::FrameType::Response,
        "{}",
        reported.1
    );
    assert_memory_matches_store(&harness, "after the probe succeeded");
    assert_eq!(
        fenced_credentials(&harness, NOW),
        vec![SPENT.to_owned()],
        "the successful probe clears its own fence and no other"
    );
}
