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

mod allocator_tests;

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
    setup_models(&[CARBON])
}

/// Two Google sign-ins serving every model in `models` through Antigravity.
fn setup_models(models: &[&str]) -> Arc<Harness> {
    let harness = Harness::new("route-fence-pools");
    let facts: Vec<_> = models
        .iter()
        .map(|model| {
            let mut fact = crate::tests::fact("google", model);
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
            fact
        })
        .collect();
    harness.install_facts(&facts);
    for fact in &facts {
        super::account_classes_tests::catalog_fact(&harness, fact);
    }
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
    credentials.refresh_serving_methods(&Default::default(), &facts, NOW, &[]);
    credentials.publish_billing_catalog(
        crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(&facts),
    );
    credentials.publish_plan_prices(&json!({"prices":[]}));
    super::conformance_tests::publish_fixture_models(&mut credentials, RUNNER, &facts, true);
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

const CLAUDE: &str = "antigravity-claude-sonnet-5-5";
/// Gemini 4 Argon draws on the Gemini pool and, on paid Pro accounts, on its
/// own Argon pool too.
const ARGON: &str = "antigravity-gemini-4-argon";
const THIRD_PARTY_SPENT_RESET: i64 = 1_795_176_704_000;

/// The open account with every pool spent, so only the other account can
/// serve either pool in these tests.
fn open_spent_windows() -> Vec<Value> {
    vec![
        window("gemini-weekly", 100.0, 10080, OPEN_GEMINI_RESET),
        window("3p-weekly", 100.0, 10080, THIRD_PARTY_RESET),
    ]
}

/// The other account's windows with its Gemini and third-party weekly pools
/// at these percentages used.
fn grace_windows(gemini: f64, third_party: f64) -> Vec<Value> {
    vec![
        window("gemini-weekly", gemini, 10080, SPENT_GEMINI_RESET),
        window("3p-weekly", third_party, 10080, THIRD_PARTY_SPENT_RESET),
    ]
}

/// Make `quota` the readings the routing ops read.
fn publish_readings(quota: &crate::account_quota::SharedAccountQuota) {
    *crate::ACCOUNT_QUOTA.get().unwrap().lock().unwrap() = quota.lock().unwrap().clone();
}

/// Lift every standing block the readings in `quota` prove recovered, as
/// the account loop does, with the vault roster the loop reads them through.
fn refresh_blocks(harness: &Harness, quota: &crate::account_quota::SharedAccountQuota, now: i64) {
    let mut state = quota.lock().unwrap().clone();
    let row = |id: &str, account: &str| ListedCredential {
        id: id.into(),
        account_id: Some(account.into()),
        state: "active".into(),
        categories: vec!["llm-provider".into()],
        credential_type: "oauth".into(),
        refresh_adapter: Some("antigravity".into()),
        record_version: 1,
        operations: vec!["list".into()],
        ..Default::default()
    };
    crate::account_quota::apply_roster_listing(
        &mut state,
        &ScopedCredentialListing {
            view: "two-google-sign-ins".into(),
            credentials: vec![row(OPEN, OPEN_ACCOUNT), row(SPENT, SPENT_ACCOUNT)],
            ..Default::default()
        },
        now,
    );
    crate::account_selection::block_recovery::refresh_all_blocks(
        harness.store(),
        &state,
        &harness.operator_config(),
        now,
    )
    .unwrap();
}

/// The account a request for `model` is admitted to, or `None` when refused.
fn model_account(context: &crate::account_selection::ClassContext, model: &str) -> Option<String> {
    match context
        .probe(
            "google",
            model,
            false,
            false,
            None,
            "pool-block-conversation",
        )
        .outcome
    {
        Outcome::Admit(admission) => Some(admission.account_id),
        _ => None,
    }
}

/// The standing account blocks on the Google provider, as their pool keys.
fn account_blocks(harness: &Harness) -> Vec<Value> {
    harness
        .store()
        .account_read(|tx| tx.active_fences("google"))
        .unwrap()
        .into_iter()
        .map(|row| serde_json::from_str::<Value>(&row.scope_json).unwrap())
        .collect()
}

/// Admit `model` on the other account and have Broca report a limit
/// refusal for it that names no pool, as Broca's refusal on Carbon did.
/// `at_refusal` are the readings in force when the refusal arrives.
async fn refuse_on_grace(
    harness: &Arc<Harness>,
    model: &str,
    at_refusal: crate::account_quota::SharedAccountQuota,
) -> Value {
    // The other account still had room when the request was admitted.
    publish_readings(&readings(
        NOW - 60_000,
        open_spent_windows(),
        grace_windows(90.0, 90.0),
    ));
    let broca = harness.bind(reserved(RUNNER), None).await;
    let admission = format!("grace-{model}");
    let admitted = broca
        .call(
            "route.account_for",
            &json!({
                "provider":"google","model":model,"mode":"admit",
                "conversation_key":format!("conversation-{model}"),"admission_id":admission,
                "candidates":{"view_digest":"two-google-sign-ins","accounts":[
                    {"credential_id":SPENT,"account_id":SPENT_ACCOUNT}]}
            }),
        )
        .await;
    assert_eq!(
        admitted.0,
        subc_protocol::FrameType::Response,
        "{}",
        admitted.1
    );
    assert_eq!(
        admitted.1["result"]["account"]["account_id"], SPENT_ACCOUNT,
        "{}",
        admitted.1
    );
    publish_readings(&at_refusal);
    let observed = broca
        .call(
            "route.account_observe",
            &json!({
                "event_id":format!("limit-{model}"),"kind":"account_limit_exhausted",
                "admission_id":admission,"resets_at_ms":SPENT_GEMINI_RESET,
                "resets_at_source":"body","observed_at_ms":NOW
            }),
        )
        .await;
    assert_eq!(
        observed.0,
        subc_protocol::FrameType::Response,
        "{}",
        observed.1
    );
    assert_memory_matches_store(harness, "after the refusal was recorded");
    observed.1["result"]["observation"].clone()
}

/// Broca's refusal on Carbon named no pool. The block it leaves belongs to
/// Carbon's Gemini pool: a Claude route on the third-party pool is admitted
/// once that pool recovers, while Carbon stays walled until Gemini does.
#[tokio::test]
async fn a_carbon_limit_refusal_walls_only_the_gemini_pool() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let harness = setup_models(&[CARBON, CLAUDE]);
    refuse_on_grace(
        &harness,
        CARBON,
        readings(
            NOW - 30_000,
            open_spent_windows(),
            grace_windows(100.0, 100.0),
        ),
    )
    .await;
    assert_eq!(
        account_blocks(&harness),
        vec![json!({"window_minutes":null,"scope":{"kind":"every_model"},"name":"gemini"})],
        "the block is keyed to the refused model's pool"
    );

    // Both pools spent: both models walled.
    let both_spent = readings(
        NOW + 60_000,
        open_spent_windows(),
        grace_windows(100.0, 100.0),
    );
    let context = load(&harness, &both_spent, NOW + 90_000);
    assert_memory_matches_store(&harness, "both pools spent");
    assert_eq!(model_account(&context, CARBON), None);
    assert_eq!(model_account(&context, CLAUDE), None);

    // The third-party pool resets first: Claude is admitted on the account,
    // and Carbon stays walled by its spent Gemini pool.
    let third_party_back = readings(
        NOW + 120_000,
        open_spent_windows(),
        grace_windows(100.0, 3.0),
    );
    refresh_blocks(&harness, &third_party_back, NOW + 150_000);
    let context = load(&harness, &third_party_back, NOW + 150_000);
    assert_memory_matches_store(&harness, "third-party pool recovered");
    assert_eq!(account_blocks(&harness).len(), 1, "Gemini is still spent");
    assert_eq!(
        model_account(&context, CLAUDE).as_deref(),
        Some(SPENT_ACCOUNT)
    );
    assert_eq!(model_account(&context, CARBON), None);

    // The Gemini pool reads under 100%: the block clears and Carbon is back.
    let gemini_back = readings(
        NOW + 180_000,
        open_spent_windows(),
        grace_windows(99.5, 3.0),
    );
    refresh_blocks(&harness, &gemini_back, NOW + 210_000);
    assert_memory_matches_store(&harness, "Gemini pool recovered");
    assert_eq!(account_blocks(&harness), Vec::<Value>::new());
    let gemini_open = readings(
        NOW + 240_000,
        open_spent_windows(),
        grace_windows(10.0, 3.0),
    );
    let context = load(&harness, &gemini_open, NOW + 240_000);
    assert_eq!(
        model_account(&context, CARBON).as_deref(),
        Some(SPENT_ACCOUNT)
    );
}

/// With no fresh reading when the refusal arrives, the refused model alone
/// names its pool.
#[tokio::test]
async fn a_refusal_without_a_fresh_reading_takes_the_pool_of_the_refused_model() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let harness = setup_models(&[CARBON, CLAUDE]);
    refuse_on_grace(
        &harness,
        CLAUDE,
        readings(
            NOW - 30 * 60_000,
            open_spent_windows(),
            grace_windows(100.0, 100.0),
        ),
    )
    .await;
    assert_eq!(
        account_blocks(&harness),
        vec![json!({"window_minutes":null,"scope":{"kind":"every_model"},"name":"3p"})]
    );
}

/// A refused model that draws on two pools cannot be keyed to one, so its
/// block stays whole-account, with no fixed hold: with no reading newer than
/// the refusal, the 15-minute recheck admits a probe on the account.
#[tokio::test]
async fn a_refusal_on_a_model_drawing_on_two_pools_blocks_the_account_and_is_probed() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    let harness = setup_models(&[CARBON, CLAUDE, ARGON]);
    harness.edit_config(|config| {
        config["quota_model_pools"] = json!([{"model_prefix": ARGON, "pool": "gemini-argon"}]);
    });
    let pro = |gemini: f64, third_party: f64| {
        let mut windows = grace_windows(gemini, third_party);
        windows.push(window(
            "gemini-argon-wtu-weekly",
            gemini,
            10080,
            SPENT_GEMINI_RESET,
        ));
        windows
    };
    refuse_on_grace(
        &harness,
        ARGON,
        readings(NOW - 30_000, open_spent_windows(), pro(100.0, 100.0)),
    )
    .await;
    assert_eq!(account_blocks(&harness), vec![Value::Null]);
    // Every pool has room again but the last reading is older than the
    // refusal, so it proves nothing: only the recheck can open the account.
    let older = readings(NOW - 10_000, open_spent_windows(), pro(5.0, 5.0));
    let inside = load(&harness, &older, NOW + 5 * 60_000);
    assert_memory_matches_store(&harness, "inside the recheck interval");
    assert_eq!(
        model_account(&inside, CLAUDE),
        None,
        "the whole account is walled"
    );
    assert_eq!(model_account(&inside, CARBON), None);
    let due = load(&harness, &older, NOW + QUOTA_FENCE_RECHECK_MS + 60_000);
    assert_memory_matches_store(&harness, "after the recheck came due");
    assert_eq!(
        model_account(&due, CLAUDE).as_deref(),
        Some(SPENT_ACCOUNT),
        "one recheck later the account is probed"
    );
    assert_eq!(account_blocks(&harness), vec![Value::Null]);
}

/// Whether, 16 minutes after Broca's Carbon refusal on the other account,
/// that account's Carbon recheck has come due, given a reading of its spent
/// Gemini pool fetched at `fetched_at` with that pool resetting at
/// `gemini_reset`. A due recheck admits the account as Carbon's probe.
async fn grace_carbon_probe_due(fetched_at: i64, gemini_reset: i64) -> bool {
    let harness = setup_models(&[CARBON, CLAUDE]);
    refuse_on_grace(
        &harness,
        CARBON,
        readings(
            NOW - 30_000,
            open_spent_windows(),
            grace_windows(100.0, 100.0),
        ),
    )
    .await;
    let spent = readings(
        fetched_at,
        open_spent_windows(),
        vec![
            window("gemini-weekly", 100.0, 10080, gemini_reset),
            window("3p-weekly", 100.0, 10080, THIRD_PARTY_SPENT_RESET),
        ],
    );
    let context = load(&harness, &spent, NOW + 16 * 60_000);
    assert_memory_matches_store(&harness, "after the recheck was judged");
    assert_eq!(
        account_blocks(&harness).len(),
        1,
        "a spent pool never clears"
    );
    model_account(&context, CARBON).as_deref() == Some(SPENT_ACCOUNT)
}

/// The pool block's recheck follows the pooled route rules: a fresh reading
/// of its own spent pool holds it; a reading out of the freshness band, or
/// one whose reset has passed, lets the probe run.
#[tokio::test]
async fn a_pool_block_recheck_is_held_only_by_a_fresh_spent_reading_of_its_pool() {
    let _clock = crate::InjectedClock::set(NOW as u64);
    assert!(
        !grace_carbon_probe_due(NOW + 11 * 60_000, SPENT_GEMINI_RESET).await,
        "a five-minute-old spent reading holds the probe"
    );
    assert!(
        grace_carbon_probe_due(NOW - 5 * 60_000, SPENT_GEMINI_RESET).await,
        "a 21-minute-old reading no longer holds it"
    );
    assert!(
        grace_carbon_probe_due(NOW + 11 * 60_000, NOW + 10 * 60_000).await,
        "a spent reading past its reset no longer holds it"
    );
}
