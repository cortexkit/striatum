//! Account-operation tests exercise the wire replies and their store effects together.

use super::*;

/// The model-specific quota pools the config converter writes.
fn shipped_model_pools() -> Vec<striatum_core::quota_extract::ModelPool> {
    serde_json::from_value(crate::config_convert::defaults()["quota_model_pools"].clone())
        .expect("converter quota model pools")
}

/// Default account tuning with the converter's model-specific quota pools.
fn default_account_policy() -> striatum_core::accounts::AllocatorPolicy {
    striatum_core::accounts::AllocatorPolicy {
        model_pools: shipped_model_pools(),
        ..Default::default()
    }
}

use crate::account_quota::{apply_provider_reply, apply_roster_listing};
use cortexkit_claustrum_client::{ListedCredential, ListedGrant, ScopedCredentialListing};
use std::sync::{Arc, Mutex};
use striatum_store::dev_descriptor;

/// The fixed test clock, 2026-09-29T12:00:00Z.
const NOW: i64 = 1_790_683_200_000;
const ACCOUNT_A: &str = "00000000-0000-4000-8000-00000000000a";
const ACCOUNT_B: &str = "00000000-0000-4000-8000-00000000000b";
/// Insula reports it; the vault does not list it.
const INSULA_ONLY: &str = "00000000-0000-4000-8000-00000000000c";
/// The vault lists it; Insula has never reported it.
const VAULT_ONLY: &str = "00000000-0000-4000-8000-00000000000d";
const BROCA: &str = "reserved:broca";

static NEXT_RIG: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    dir: std::path::PathBuf,
    store: Option<SqliteRouteStateStore>,
    accounts: SharedAccountQuota,
    policy: AllocatorPolicy,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.store = None;
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn open_store(dir: &std::path::Path) -> SqliteRouteStateStore {
    let path = dir.join("store.db").to_string_lossy().into_owned();
    SqliteRouteStateStore::open(&dev_descriptor("striatum", path)).expect("open store")
}

fn claude_entry(account: &str) -> Value {
    json!({
        "provider": "claude",
        "apiProvider": "anthropic",
        "account": account,
        "accountInfo": { "email": format!("{account}@example.test") },
        "source": "vault",
        "fetchedAt": "2026-09-29T11:59:00Z",
        "usage": {
            "primary": { "usedPercent": 10.0, "windowMinutes": 300, "resetsAt": "2026-09-29T14:00:00Z" },
            "secondary": { "usedPercent": 40.0, "windowMinutes": 10080 },
            "extraRateWindows": [
                { "id": "Fable", "title": "7 Day (Fable)", "window": { "usedPercent": 5.0, "windowMinutes": 10080 } }
            ]
        }
    })
}

#[test]
fn live_display_name_scope_adds_to_weekly_only_for_fable() {
    use striatum_core::accounts::eligibility::evaluate_quota;
    let mut entry = claude_entry("live-scope");
    entry["usage"]["tertiary"] = Value::Null;
    let policy = default_account_policy();
    let evaluate = |entry: &Value, model| {
        let reading = quota_reading_for_model(entry, "live-scope", None, &[]).unwrap();
        evaluate_quota(Some(&reading), model, CallerKind::Plugin, NOW, &policy)
    };
    for model in [
        "claude-fable-5",
        "claude-fable-5[1m]",
        "claude-fable-5-1",
        "claude-fable-5-1[1m]",
        "claude-opus-5-5",
        "claude-opus-5-5[1m]",
        "claude-mythos-5-1",
        "claude-mythos-5-1[1m]",
    ] {
        let eval = evaluate(&entry, model);
        assert!(eval.eligible(), "{model}: {eval:?}");
        assert!(eval
            .window_weights
            .iter()
            .any(|window| window.window == ApplicableWindow::SevenDay));
        let scoped: Vec<_> = eval
            .window_weights
            .iter()
            .filter(|window| window.window == ApplicableWindow::FamilyScope)
            .collect();
        let fable = model.starts_with("claude-fable-");
        assert_eq!(scoped.len(), usize::from(fable), "{model}");
        assert_eq!(
            eval.window_weights.len(),
            if fable { 3 } else { 2 },
            "{model}"
        );

        let mut exhausted = entry.clone();
        exhausted["usage"]["extraRateWindows"][0]["window"]["usedPercent"] = json!(100.0);
        assert_eq!(evaluate(&exhausted, model).eligible(), !fable, "{model}");
        exhausted["usage"]["secondary"]["usedPercent"] = json!(100.0);
        assert!(
            !evaluate(&exhausted, model).eligible(),
            "{model}: weekly must always apply"
        );
    }
}

#[test]
fn future_model_id_scope_applies_without_a_family_table() {
    use striatum_core::accounts::eligibility::evaluate_quota;
    let mut entry = claude_entry("future-scope");
    entry["usage"]["extraRateWindows"][0]["id"] = json!("claude-fable-5");
    entry["usage"]["extraRateWindows"][0]["window"]["usedPercent"] = json!(100.0);
    let reading = quota_reading_for_model(&entry, "future-scope", None, &[]).unwrap();
    let eval = evaluate_quota(
        Some(&reading),
        "claude-fable-5-1[1m]",
        CallerKind::Plugin,
        NOW,
        &default_account_policy(),
    );
    assert!(
        eval.reserve.iter().any(|reason| matches!(
            reason,
            striatum_core::accounts::ExclusionReason::ScopeExhausted { .. }
        )),
        "future id must gate the pick: {eval:?}"
    );
}

#[test]
fn literal_null_scope_id_never_uses_the_title_and_warns_once_per_account() {
    use striatum_core::accounts::eligibility::evaluate_quota;
    let account = "null-scope-warning-account";
    let mut entry = claude_entry(account);
    entry["usage"]["extraRateWindows"][0]["id"] = Value::Null;
    entry["usage"]["extraRateWindows"][0]["window"]["usedPercent"] = json!(100.0);
    for title in ["Fable", "7 Day (Fable)"] {
        entry["usage"]["extraRateWindows"][0]["title"] = json!(title);
        let reading = quota_reading_for_model(&entry, account, None, &[]).unwrap();
        let eval = evaluate_quota(
            Some(&reading),
            "claude-fable-5",
            CallerKind::Plugin,
            NOW,
            &default_account_policy(),
        );
        assert!(eval.eligible(), "{eval:?}");
        assert_eq!(eval.window_weights.len(), 2);
    }
    assert!(!claim_unattributable_scope_warning(account));
    let other = "another-null-scope-warning-account";
    assert!(claim_unattributable_scope_warning(other));
    assert!(!claim_unattributable_scope_warning(other));
    entry["usage"]["extraRateWindows"][0]["id"] = json!("unrelated");
    let reading = quota_reading_for_model(&entry, account, None, &[]).unwrap();
    assert!(evaluate_quota(
        Some(&reading),
        "claude-fable-5",
        CallerKind::Plugin,
        NOW,
        &default_account_policy()
    )
    .eligible());
}

#[test]
fn last_good_is_admissible_until_thirty_minutes_but_not_after_a_reset() {
    use striatum_core::accounts::eligibility::evaluate_quota;
    use striatum_core::accounts::ExclusionReason;
    let mut state = AccountQuotaState::default();
    let mut good = claude_entry(ACCOUNT_A);
    good["fetchedAt"] = json!("2026-09-29T12:00:00Z");
    let body = json!({"result": [good], "completeProviders": ["claude"]});
    apply_provider_reply(
        &mut state,
        "claude",
        &serde_json::to_vec(&body).unwrap(),
        NOW,
    )
    .unwrap();
    let body = json!({"result": [{"provider": "claude", "apiProvider": "anthropic", "account": ACCOUNT_A,
            "errorClass": "upstream_failed", "error": "restart"}], "completeProviders": ["claude"]});
    apply_provider_reply(
        &mut state,
        "claude",
        &serde_json::to_vec(&body).unwrap(),
        NOW + 27 * 60_000,
    )
    .unwrap();
    let entry = &state.readings.readings.values().next().unwrap().entry;
    let reading = quota_reading_for_model(entry, "acct-a", None, &[]).unwrap();
    let evaluate = |reading: &QuotaReading, now| {
        evaluate_quota(
            Some(reading),
            "claude-opus-5-5",
            CallerKind::Plugin,
            now,
            &AllocatorPolicy::default(),
        )
    };
    let kept = evaluate(&reading, NOW + 27 * 60_000);
    assert!(kept.eligible(), "{:?}", kept.unknown);
    assert_eq!(reading.five_hour.as_ref().unwrap().used_percent, 10.0);
    let expired = evaluate(&reading, NOW + 30 * 60_000);
    assert!(expired
        .unknown
        .iter()
        .any(|reason| matches!(reason, ExclusionReason::AgeExcluded { .. })));

    let mut past_reset = entry.clone();
    past_reset["usage"]["primary"]["resetsAt"] = json!("2026-09-29T12:01:00Z");
    let reading = quota_reading_for_model(&past_reset, "acct-a", None, &[]).unwrap();
    let expired_window = evaluate(&reading, NOW + 2 * 60_000);
    assert!(!expired_window.eligible());
    assert!(expired_window
        .unknown
        .iter()
        .any(|reason| matches!(reason, ExclusionReason::WindowResetElapsed { .. })));
}

fn listing() -> ScopedCredentialListing {
    let row = |id: &str, account: &str| ListedCredential {
        id: id.into(),
        categories: vec!["anthropic-native".into()],
        credential_type: "oauth".into(),
        refresh_adapter: Some("anthropic".into()),
        state: "active".into(),
        record_version: 1,
        operations: vec!["list".into()],
        account_id: Some(account.into()),
        email: Some(format!("{account}@example.test")),
        ..Default::default()
    };
    ScopedCredentialListing {
        credentials: vec![
            row("oauth:anthropic", ACCOUNT_A),
            row("oauth:anthropic:ysample", ACCOUNT_B),
            row("oauth:anthropic:dsample", VAULT_ONLY),
        ],
        grant_tuples: vec![ListedGrant {
            selector_kind: "category".into(),
            selector: "anthropic-native".into(),
            operation: "list".into(),
        }],
        view: "vault-view-1".into(),
        ..Default::default()
    }
}

fn fixture(label: &str) -> Fixture {
    let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
    let dir = cortexkit_test_support::scratch_root().join(format!(
        "prefrontal-account-selection-module-{label}-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let mut state = AccountQuotaState::default();
    // Listed 20 minutes ago, so the vault-only identity is overdue for its
    // first reading.
    apply_roster_listing(&mut state, &listing(), NOW - 20 * 60_000);
    let reply = serde_json::to_vec(&json!({
        "result": [claude_entry(ACCOUNT_A), claude_entry(ACCOUNT_B), claude_entry(INSULA_ONLY)],
        "completeProviders": ["claude"],
    }))
    .unwrap();
    apply_provider_reply(&mut state, "claude", &reply, NOW - 30_000).unwrap();
    Fixture {
        store: Some(open_store(&dir)),
        dir,
        accounts: Arc::new(Mutex::new(state)),
        policy: default_account_policy(),
    }
}

impl Fixture {
    fn store(&self) -> &SqliteRouteStateStore {
        self.store.as_ref().unwrap()
    }

    fn reopen(&mut self) {
        self.store = None;
        self.store = Some(open_store(&self.dir));
    }

    fn ctx(&self, enabled: bool, now_ms: i64) -> Ctx<'_> {
        Ctx {
            store: self.store(),
            accounts: Some(&self.accounts),
            flags: AccountSelectionFlags { enabled },
            policy: &self.policy,
            now_ms,
            classes: None,
        }
    }

    fn call(&self, ctx: &Ctx<'_>, method: &str, principal: &str, params: Value) -> OpReply {
        let body = serde_json::to_vec(&json!({ "method": method, "params": params })).unwrap();
        handle(method, &body, principal, ctx)
    }

    fn serve_rows(&self) -> Vec<AccountDecisionRecord> {
        self.store()
            .account_decisions(DECISION_MODE_SERVE, 1000)
            .unwrap()
    }
}

fn admit_params(admission_id: &str, key: &str, runner: &str) -> Value {
    json!({
        "provider": "anthropic",
        "model": "claude-opus-5-5",
        "affinity_model": "claude-opus-5-5",
        "conversation_key": key,
        "caller": { "runner": runner, "harness": "prefrontal-core" },
        "admission_id": admission_id,
        "mode": "admit",
        "expected_pin": null,
        "initial_input_bytes": 12000,
        "initial_input_estimated": false,
        "model_decision_id": null,
        "candidates": null,
        "requested_account": null,
        "hold_wait_max_s": 900
    })
}

fn affinity_grace_fixture(label: &str) -> Fixture {
    let rig = fixture(label);
    crate::account_writer::execute(
        rig.store(),
        striatum_store::AccountCommandKind::Repair,
        |tx| {
            let salt = tx.salt(NOW, || "grace-test-salt".into())?;
            let hash = conversation_hash(&salt, BROCA, "grace-conversation");
            for (hash, pin, generation, expires_at) in [
                (hash, "grace-pin", 7, NOW - 30 * 60_000),
                (
                    "dead-conversation".into(),
                    "dead-pin",
                    1,
                    NOW - 2 * 60 * 60_000,
                ),
            ] {
                tx.insert_pin(&AccountPinRow {
                    pin_id: pin.into(),
                    principal: BROCA.into(),
                    conversation_hash: hash.clone(),
                    provider: "anthropic".into(),
                    pin_generation: generation,
                    affinity_model: "claude-opus-5-5".into(),
                    account_id: ACCOUNT_A.into(),
                    credential_id: "oauth:anthropic".into(),
                    pin_mode: "conversation_lease".into(),
                    previous_pin_id: None,
                    decision_id: "history".into(),
                    created_at: NOW - AFFINITY_IDLE_MS,
                })?;
                tx.put_affinity(
                    &AccountAffinityRow {
                        principal: BROCA.into(),
                        conversation_hash: hash,
                        provider: "anthropic".into(),
                        affinity_model: "claude-opus-5-5".into(),
                        pin_id: pin.into(),
                        pin_generation: generation,
                        account_id: ACCOUNT_A.into(),
                        credential_id: "oauth:anthropic".into(),
                        credential_binding_revision: 1,
                        policy_version: 2,
                        initial_demand_bytes: 12_000,
                        created_at: expires_at - AFFINITY_IDLE_MS,
                        last_used_at: expires_at - AFFINITY_IDLE_MS,
                        expires_at,
                        lease_valid_until: Some(NOW - 1),
                    },
                    None,
                )?;
            }
            Ok(())
        },
    )
    .unwrap();
    let report = crate::retention::run_retention_pass(
        rig.store(),
        &Default::default(),
        NOW,
        &crate::retention::PassBudget {
            pause: std::time::Duration::ZERO,
            ..Default::default()
        },
    );
    assert_eq!(
        report.account_affinities, 1,
        "only the conversation beyond the grace hour goes"
    );
    rig
}

#[test]
fn affinity_retention_grace_preserves_expected_pin_compare_and_set() {
    let rig = affinity_grace_fixture("grace-cas");
    let mut params = admit_params("renew", "grace-conversation", "anthropic-auth");
    params["mode"] = json!("renew");
    params["expected_pin"] = json!({"id":"grace-pin","generation":6});
    let reply = rig.call(&rig.ctx(true, NOW), ACCOUNT_FOR_OP, BROCA, params);
    assert_eq!(reply.code(), Some("pin_generation_conflict"));
    let OpReply::Error { detail, .. } = reply else {
        unreachable!()
    };
    assert_eq!(
        detail.unwrap()["current_pin"],
        json!({"id":"grace-pin","generation":7,
            "account_id":ACCOUNT_A,"credential_id":"oauth:anthropic"})
    );
}

#[test]
fn affinity_retention_grace_preserves_generation_when_a_bound_service_tier_moves() {
    let rig = affinity_grace_fixture("grace-tier");
    let ctx = rig.ctx(true, NOW);
    let mut request = admit_params("tier", "grace-conversation", "anthropic-auth");
    request["candidates"] = json!({"view_digest":"tier-view","accounts":[{"account_id":ACCOUNT_B,"credential_id":"oauth:anthropic:ysample"}]});
    let mut params = parse_account_for(&request).unwrap();
    params.service_tier_chosen = Some(cortexkit_role_routing::service_tier::ServiceTier::Fast);
    let mut roster = roster_for_model(&ctx, "anthropic", Some("claude-opus-5-5"));
    roster
        .roster
        .as_mut()
        .unwrap()
        .accounts
        .retain(|account| account.account_id != ACCOUNT_A);
    let reply =
        crate::account_writer::execute(rig.store(), striatum_store::AccountCommandKind::Repair, {
            let command = ctx.command_context();
            move |tx| {
                serve(
                    tx,
                    &command,
                    BROCA,
                    &params,
                    "anthropic",
                    &roster,
                    "tier-fingerprint",
                )
            }
        })
        .unwrap();
    let reply = result(reply);
    assert_eq!(reply["service_tier_chosen"], "fast");
    assert_eq!(reply["account"]["account_id"], ACCOUNT_B);
    assert_eq!(reply["pin"]["generation"], 8);
}

#[test]
fn affinity_retention_grace_still_suppresses_import_preference() {
    let rig = affinity_grace_fixture("grace-import");
    let mut params = admit_params("import", "grace-conversation", "anthropic-auth");
    params["preferred_existing_account"] = json!({"account_id":ACCOUNT_B});
    let reply = result(rig.call(&rig.ctx(true, NOW), ACCOUNT_FOR_OP, BROCA, params));
    assert_eq!(
        reply["pin"]["generation"], 8,
        "expired affinity still supplies the previous generation"
    );
    assert_ne!(reply["reason"]["code"], "imported_affinity");
    let decision: Value =
        serde_json::from_str(&rig.serve_rows().last().unwrap().decision_json).unwrap();
    assert!(decision["explanation"]["import"].is_null());
}

fn result(reply: OpReply) -> Value {
    match reply {
        OpReply::Result(value) => value,
        OpReply::Error { code, message, .. } => {
            panic!("expected a result, got {code}: {message}")
        }
    }
}

#[test]
fn established_affinity_prepared_before_a_transient_claim_cannot_double_send() {
    let rig = fixture("established-transient-interleaving");
    let mut request = admit_params("first", "established-transient", "broca");
    request["candidates"] = json!({"view_digest":"runner-view","accounts":[{
            "account_id":ACCOUNT_A,"credential_id":"oauth:anthropic"}]});
    let first = result(rig.call(
        &rig.ctx(true, NOW - 1000),
        ACCOUNT_FOR_OP,
        BROCA,
        request.clone(),
    ));
    result(rig.call(
        &rig.ctx(true, NOW - 999),
        ACCOUNT_OBSERVE_OP,
        BROCA,
        json!({"event_id":"started","caller":{"runner":"broca"},"kind":"admission_started",
                "admission_id":"first","run_id":"run-first"}),
    ));
    rig.store()
        .record_transient_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "anthropic".into(),
            credential_id: "oauth:anthropic".into(),
            model: "claude-opus-5-5".into(),
            until_ms: NOW,
            recorded_at: NOW - 100,
            decision_id: "refusal".into(),
        })
        .unwrap();
    let shared = std::sync::Arc::new(std::sync::Mutex::new(
        crate::broca_credentials::BrocaCredentials::default(),
    ));
    shared.lock().unwrap().apply_listing(&listing(), NOW as u64);
    let classes = ClassContext::load(
        rig.store(),
        &shared,
        Default::default(),
        Some(&rig.accounts),
        NOW,
    )
    .unwrap();
    let mut ctx = rig.ctx(true, NOW);
    ctx.classes = Some(classes);
    let roster = roster_for_model(&rig.ctx(true, NOW), "anthropic", Some("claude-opus-5-5"));
    let origins = rig.store().fence_rechecks().unwrap();
    assert!(origins
        .iter()
        .any(|o| o.kind == "transient_route" && o.due_at() == NOW));
    assert!(rig
        .store()
        .account_transaction(|tx| tx.grant_fence_rechecks(&origins, NOW, Some("winner")))
        .unwrap());
    request["admission_id"] = json!("distinct-second");
    request["expected_pin"] = first["pin"].clone();
    let parsed = parse_account_for(&request).unwrap();
    let reply = rig
        .store()
        .account_transaction(|tx| {
            serve(
                &mut crate::account_state::AccountCommand::new(tx, None),
                &ctx.command_context(),
                BROCA,
                &parsed,
                "anthropic",
                &roster,
                "second-fingerprint",
            )
        })
        .unwrap();
    match reply {
            OpReply::Error { code, detail, .. } => {
                assert_eq!(code, "account_hold");
                let held = detail.unwrap();
                assert_eq!(held["account"]["account_id"], ACCOUNT_A);
                assert_eq!(held["reset_at"], rfc3339(NOW+3_600_000));
                assert_eq!(held["pin"], json!({"id":first["pin"]["id"],"generation":first["pin"]["generation"]}));
            }
            other => panic!("the unchanged started affinity must hold after another request claims its due origin: {other:?}"),
        }
    assert!(
        rig.store()
            .account_read(|tx| tx.admission(BROCA, "distinct-second"))
            .unwrap()
            .is_none(),
        "the losing interleaving cannot commit a second admission"
    );
}

#[test]
fn established_affinity_consumes_its_decisions_transient_probe_once() {
    use striatum_core::{DecisionInput, RouteStateStore};

    for class_aware in [true, false] {
        let rig = fixture("established-owned-transient");
        let mut fact = crate::tests::fact("anthropic", "claude-opus-5-5");
        fact.auth_methods = Some(vec!["oauth".into()]);
        rig.store()
            .upsert_fact(&fact, striatum_core::FactSource::Override)
            .unwrap();
        let mut request = admit_params("first", "established-owned", "broca");
        request["candidates"] = json!({"view_digest":"runner-view","accounts":[{
                "account_id":ACCOUNT_A,"credential_id":"oauth:anthropic"}]});
        let first = result(rig.call(
            &rig.ctx(true, NOW - 1000),
            ACCOUNT_FOR_OP,
            BROCA,
            request.clone(),
        ));
        result(rig.call(
            &rig.ctx(true, NOW - 999),
            ACCOUNT_OBSERVE_OP,
            BROCA,
            json!({"event_id":"started","caller":{"runner":"broca"},"kind":"admission_started",
                    "admission_id":"first","run_id":"run-first"}),
        ));
        rig.store()
            .record_transient_account_route_fence(&striatum_store::AccountRouteFence {
                provider: "anthropic".into(),
                credential_id: "oauth:anthropic".into(),
                model: "claude-opus-5-5".into(),
                until_ms: NOW,
                recorded_at: NOW - 100,
                decision_id: "refusal".into(),
            })
            .unwrap();
        let input = DecisionInput {
            selected_route_key: Some("anthropic/claude-opus-5-5".into()),
            selected_provider_id: Some("anthropic".into()),
            selected_model_id: Some("claude-opus-5-5".into()),
            ..Default::default()
        };
        let decision = rig.store().record_decision(&input).unwrap();
        let other_decision = rig.store().record_decision(&input).unwrap();
        let origins = rig.store().fence_rechecks().unwrap();
        assert_eq!(origins.len(), 1);
        assert!(rig
            .store()
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, NOW, Some(&decision)))
            .unwrap());
        let shared = Arc::new(Mutex::new(
            crate::broca_credentials::BrocaCredentials::api_test_credentials(&[fact]),
        ));
        let mut live_listing = listing();
        for row in &mut live_listing.credentials {
            row.categories.push("llm-provider".into());
            row.provider_ids = vec!["anthropic".into()];
            row.auth_method = Some("oauth".into());
        }
        shared
            .lock()
            .unwrap()
            .apply_listing(&live_listing, NOW as u64);
        let mut ctx = rig.ctx(true, NOW + 1);
        if class_aware {
            ctx.classes = Some(
                ClassContext::load(
                    rig.store(),
                    &shared,
                    Default::default(),
                    Some(&rig.accounts),
                    NOW + 1,
                )
                .unwrap(),
            );
        }
        request["expected_pin"] = first["pin"].clone();
        request["admission_id"] = json!("other-owner");
        request["model_decision_id"] = json!(other_decision);
        assert_eq!(
            rig.call(&ctx, ACCOUNT_FOR_OP, BROCA, request.clone())
                .code(),
            Some("account_hold"),
        );

        request["model_decision_id"] = json!(decision);
        request["admission_id"] = json!("owned-probe");
        let admitted = result(rig.call(&ctx, ACCOUNT_FOR_OP, BROCA, request.clone()));
        assert_eq!(admitted["account"]["account_id"], ACCOUNT_A);
        assert_eq!(admitted["pin"]["id"], first["pin"]["id"]);
        assert_eq!(admitted["pin"]["generation"], first["pin"]["generation"]);
        assert!(rig.store().fence_rechecks().unwrap()[0]
            .claim_owner
            .is_none());
        request["admission_id"] = json!("owned-probe-again");
        assert_eq!(
            rig.call(&ctx, ACCOUNT_FOR_OP, BROCA, request).code(),
            Some("account_hold"),
        );
        for id in ["other-owner", "owned-probe-again"] {
            assert!(rig
                .store()
                .account_read(|tx| tx.admission(BROCA, id))
                .unwrap()
                .is_none());
        }
        assert!(rig
            .store()
            .account_read(|tx| tx.admission(BROCA, "owned-probe"))
            .unwrap()
            .is_some());
    }
}

#[test]
fn partial_reading_cannot_heal_route_rows_paired_with_account_recovery_pools() {
    let rig = fixture("partial-route-recovery");
    let shared = std::sync::Arc::new(std::sync::Mutex::new(
        crate::broca_credentials::BrocaCredentials::default(),
    ));
    shared.lock().unwrap().apply_listing(&listing(), NOW as u64);
    let entry = claude_entry(ACCOUNT_A);
    let reading = quota_reading_for_model(&entry, ACCOUNT_A, None, &[]).unwrap();
    let recovery_set = recovery_set_from(Some(&reading), NOW, &rig.policy);
    assert!(recovery_set.len() >= 2);
    let fence = Fence {
        account_id: ACCOUNT_A.into(),
        pool: None,
        recovery_set: recovery_set.clone(),
        observed_at_ms: NOW,
        reset: ResetTime::Missing,
        retry_hint_ms: None,
        source: striatum_core::accounts::ConfirmationSource::BrocaAccountLimit,
    };
    rig.store()
        .account_transaction(|tx| {
            tx.insert_pool_fence(&AccountPoolFenceRow {
                evidence: AccountFenceRow {
                    fence_id: "paired".into(),
                    provider: "anthropic".into(),
                    account_id: ACCOUNT_A.into(),
                    scope_json: "null".into(),
                    fence_json: wire(&fence).to_string(),
                    source: wire_str(&fence.source),
                    observed_at: NOW,
                    principal: BROCA.into(),
                    admission_id: None,
                    observation_id: None,
                    created_at: NOW,
                    cleared_at: None,
                    cleared_reason: None,
                },
                pool_key: None,
                recovery_set,
            })
        })
        .unwrap();
    rig.store()
        .record_account_route_fence(&striatum_store::AccountRouteFence {
            provider: "anthropic".into(),
            credential_id: "oauth:anthropic".into(),
            model: "claude-opus-5-5".into(),
            until_ms: NOW + 3_600_000,
            recorded_at: NOW,
            decision_id: "route-refusal".into(),
        })
        .unwrap();
    let mut partial = entry.clone();
    partial["fetchedAt"] = json!(rfc3339(NOW + 60_000));
    partial["usage"]["secondary"] = Value::Null;
    partial["usage"]["extraRateWindows"] = json!([]);
    let read = |entry| {
        account_quota::apply_usage_reply(
            &mut rig.accounts.lock().unwrap(),
            &serde_json::to_vec(&json!({"result":[entry]})).unwrap(),
            NOW + 60_000,
        )
        .unwrap()
    };
    read(partial);
    ClassContext::load(
        rig.store(),
        &shared,
        Default::default(),
        Some(&rig.accounts),
        NOW + 60_000,
    )
    .unwrap();
    assert_eq!(
        rig.store()
            .account_route_fences(NOW + 60_000)
            .unwrap()
            .len(),
        1,
        "a partial healthy reading cannot delete the paired route evidence"
    );
    let mut complete = entry;
    complete["fetchedAt"] = json!(rfc3339(NOW + 120_000));
    read(complete);
    ClassContext::load(
        rig.store(),
        &shared,
        Default::default(),
        Some(&rig.accounts),
        NOW + 120_000,
    )
    .unwrap();
    assert!(rig
        .store()
        .account_route_fences(NOW + 120_000)
        .unwrap()
        .is_empty());
}

#[test]
fn a_probe_claimed_after_roster_preparation_returns_its_fence_not_missing_candidates() {
    let fixture = fixture("claim-after-roster");
    let ctx = fixture.ctx(true, NOW);
    let mut roster = roster_for_model(&ctx, "anthropic", Some("claude-opus-5-5"));
    let accounts = &mut roster.roster.as_mut().unwrap().accounts;
    accounts.retain(|account| account.account_id == ACCOUNT_A);
    assert_eq!(accounts.len(), 1);
    accounts[0].quota = None;
    fixture
        .store()
        .account_transaction(|tx| {
            tx.record_non_quota_route_fence(&striatum_store::AccountRouteFence {
                provider: "anthropic".into(),
                credential_id: "oauth:anthropic".into(),
                model: "claude-opus-5-5".into(),
                until_ms: NOW,
                recorded_at: NOW - 100,
                decision_id: "refusal".into(),
            })
        })
        .unwrap();
    let due = fixture.store().fence_rechecks().unwrap();
    assert_eq!(due.len(), 1);
    assert!(fixture
        .store()
        .account_transaction(|tx| tx.grant_fence_rechecks(&due, NOW, Some("winning-decision")))
        .unwrap());
    let mut request = admit_params("loser", "claim-after-roster", "broca");
    request["candidates"] = json!({"view_digest":"same-runner-list","accounts":[{"account_id":ACCOUNT_A,"credential_id":"oauth:anthropic"}]});
    let parsed = parse_account_for(&request).unwrap();
    let reply = fixture
        .store()
        .account_transaction(|tx| {
            serve(
                &mut crate::account_state::AccountCommand::new(tx, None),
                &ctx.command_context(),
                "runner",
                &parsed,
                "anthropic",
                &roster,
                "loser-fingerprint",
            )
        })
        .unwrap();
    match reply {
        OpReply::Error { code, detail, .. } => {
            assert_eq!(
                code, "account_pool_exhausted",
                "a lost claim must retain its fence cause"
            );
            assert_eq!(detail.unwrap()["reset_at_ms"], NOW + 3_600_000);
        }
        other => panic!("a second probe was admitted: {other:?}"),
    }
}

#[test]
fn account_exhaustion_fences_only_its_account_and_opus_seats_with_a_93_percent_sibling() {
    use striatum_core::{RouteStateStore, RouteStateUpdate, RouteWallClass};
    let fixture = fixture("account-wall-versus-seat");
    let ctx = fixture.ctx(true, NOW);
    let first = result(fixture.call(
        &ctx,
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("wall-1", "broca:wall-1", "broca"),
    ));
    let account = first["account"]["account_id"].as_str().unwrap();
    let sibling = if account == ACCOUNT_A {
        ACCOUNT_B
    } else {
        ACCOUNT_A
    };
    for reading in fixture
        .accounts
        .lock()
        .unwrap()
        .readings
        .readings
        .values_mut()
        .filter(|reading| reading.account_id == account)
    {
        reading.entry["usage"]["secondary"]["usedPercent"] = json!(100.0);
    }
    let observed = result(fixture.call(&ctx,ACCOUNT_OBSERVE_OP,BROCA,json!({
            "event_id":"exhausted-account","caller":{"runner":"broca"},"kind":"account_limit_exhausted",
            "admission_id":"wall-1","limit_window":"seven_day","resets_at_ms":NOW+77*3_600_000,
            "resets_at_source":"header","observed_at_ms":NOW+1000,
        })));
    assert_eq!(
        observed["observation"]["effects"][0]["fence"]["account_id"],
        account
    );
    assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 1);

    // A caller retaining the old model-cooldown protocol cannot fan this
    // account's dated exhaustion out to the whole Opus pool.
    let route = "anthropic/claude-opus-5-5";
    fixture
        .store()
        .update_route_state(
            route,
            route,
            &RouteStateUpdate {
                cooldown_until: Some((NOW + 77 * 3_600_000) as f64),
                quota_exhausted_until: Some((NOW + 77 * 3_600_000) as f64),
                wall_class: Some(RouteWallClass::Quota),
                last_error: Some(
                    json!({"provider_code":"account_limit_exhausted","account_ref":account})
                        .to_string(),
                ),
                ..Default::default()
            },
        )
        .unwrap();
    let model_rows: i64 = rusqlite::Connection::open(fixture.dir.join("store.db"))
        .unwrap()
        .query_row(
            "SELECT count(*) FROM model_route_state WHERE route_key=?1",
            [route],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        model_rows, 0,
        "account evidence lives in account_fence, not model_route_state"
    );

    let mut exhausted = claude_entry(account);
    exhausted["usage"]["secondary"]["usedPercent"] = json!(100);
    exhausted["usage"]["secondary"]["resetsAt"] = json!("2026-10-02T17:00:00Z");
    let mut healthy = claude_entry(sibling);
    healthy["usage"]["secondary"]["usedPercent"] = json!(93);
    healthy["usage"]["secondary"]["resetsAt"] = json!("2026-10-03T12:00:00Z");
    apply_provider_reply(
        &mut fixture.accounts.lock().unwrap(),
        "claude",
        &serde_json::to_vec(
            &json!({"result":[exhausted.clone(),healthy.clone()],"completeProviders":["claude"]}),
        )
        .unwrap(),
        NOW + 1000,
    )
    .unwrap();
    let next = result(fixture.call(
        &ctx,
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("wall-2", "broca:wall-2", "broca"),
    ));
    assert_eq!(
        next["account"]["account_id"], sibling,
        "93% still leaves spendable quota for Broca"
    );

    // Production seating builds the same account-wise overlay before shared
    // panel admission. One 100% account cannot wall the 93% sibling.
    let mut fact = striatum_core::model_facts_seed()[0].clone();
    fact.provider_id = "anthropic".into();
    fact.model_id = "claude-opus-5-5".into();
    fact.variant = None;
    let facts = vec![fact];
    let mut snapshot = fixture.store().build_snapshot(NOW as f64).unwrap();
    let windows: Vec<striatum_core::ProviderUsage> =
        serde_json::from_value(json!([exhausted, healthy])).unwrap();
    striatum_core::merge_quota_overlay(&mut snapshot, &windows, &facts, &shipped_model_pools());
    assert_eq!(snapshot.by_capacity[route].quota_exhausted_until, None);
    let seat = |snapshot| {
        striatum_core::select_panel(striatum_core::SelectPanelInput {
            package_tokens: None,
            panel_spec: serde_json::from_value(
                json!({"size":1,"iqFloor":90,"class":"SPEC","diversity":"none"}),
            )
            .unwrap(),
            selection: striatum_core::PanelSelection::Roster,
            roster: serde_json::from_value(json!([{"model":route}])).unwrap(),
            facts: facts.clone(),
            runtime_snapshot: Some(snapshot),
            exclude_route_keys: vec![],
            operator_exclude_keys: vec![],
            required_capabilities: None,
            available_models: [route.to_string()].into_iter().collect(),
            availability_substrate: striatum_core::AvailabilitySubstrate::Broca,
            availability_label: crate::legacy_wire::availability_label(
                striatum_core::AvailabilitySubstrate::Broca,
            )
            .to_string(),
            is_subscription_route: None,
        })
        .unwrap()
    };
    assert_eq!(seat(snapshot.clone()).members[0].route_key, route);
    // The all-exhausted control proves seating still honors a real pool wall.
    healthy["usage"]["secondary"]["usedPercent"] = json!(100);
    let windows: Vec<striatum_core::ProviderUsage> =
        serde_json::from_value(json!([exhausted, healthy])).unwrap();
    striatum_core::merge_quota_overlay(&mut snapshot, &windows, &facts, &shipped_model_pools());
    assert!(seat(snapshot).members.is_empty());
}

fn provider_fixture(label: &str, adapter: &str, entries: Vec<Value>) -> Fixture {
    let rig = fixture(label);
    let mut state = AccountQuotaState::default();
    let mut vault = listing();
    vault.credentials.truncate(2);
    for row in &mut vault.credentials {
        row.refresh_adapter = Some(adapter.into());
        row.id = format!("oauth:{adapter}:{}", row.account_id.as_deref().unwrap());
    }
    apply_roster_listing(&mut state, &vault, NOW);
    account_quota::apply_usage_reply(
        &mut state,
        &serde_json::to_vec(&json!({"result": entries})).unwrap(),
        NOW,
    )
    .unwrap();
    *rig.accounts.lock().unwrap() = state;
    rig
}

fn reported_entry(
    provider: &str,
    vendor: Value,
    account: &str,
    kind: &str,
    minutes: i64,
    used: f64,
) -> Value {
    json!({
        "provider": provider, "apiProvider": vendor, "account": account,
        "source": "oauth", "fetchedAt": "2026-09-29T11:59:00Z",
        "usage": {"primary": {"windowKind": kind, "windowMinutes": minutes, "usedPercent": used}}
    })
}

#[test]
fn codex_vendor_joins_two_openai_accounts_and_selects_headroom() {
    let rig = provider_fixture(
        "codex",
        "openai",
        vec![
            reported_entry("codex", json!("openai"), ACCOUNT_A, "weekly", 10080, 100.0),
            reported_entry("codex", json!("openai"), ACCOUNT_B, "weekly", 10080, 10.0),
        ],
    );
    let built = allocator_roster(&rig.accounts.lock().unwrap(), "openai", NOW);
    assert_eq!(built.roster.unwrap().accounts.len(), 2);
    let mut params = admit_params("codex-admit", "codex-conversation", "broca");
    params["provider"] = json!("openai");
    params["model"] = json!("gpt-6");
    params["affinity_model"] = json!("gpt-6");
    let admitted = result(rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params));
    assert_eq!(admitted["account"]["account_id"], ACCOUNT_B);
    assert_eq!(admitted["account"]["provider"], "openai");
    assert_eq!(admitted["account"]["refresh_adapter"], "openai");
}

#[test]
fn antigravity_vendor_and_model_pool_select_the_joined_account() {
    // This positive vector requires a published vendor. The live producer
    // currently omits it; the live-shape vector below pins that refusal.
    let entry = |account: &str, used: f64| {
        json!({
            "provider": "antigravity", "apiProvider": "google", "account": account,
            "source": "oauth", "fetchedAt": "2026-09-29T11:59:00Z",
            "usage": {"extraRateWindows": [
                {"id": "gemini-5h", "windowKind": "five_hour", "window": {"usedPercent": used, "windowMinutes": 300}},
                {"id": "3p-weekly", "windowKind": "weekly", "window": {"usedPercent": 100.0, "windowMinutes": 10080}}
            ]}
        })
    };
    let rig = provider_fixture(
        "antigravity",
        "antigravity",
        vec![entry(ACCOUNT_A, 100.0), entry(ACCOUNT_B, 10.0)],
    );
    let state = rig.accounts.lock().unwrap();
    let report = account_quota::build_report(
        &state.readings,
        state.roster.as_ref(),
        &state.roster_poll,
        NOW,
    );
    assert_eq!(report.accounts.len(), 2);
    assert_eq!(
        report.accounts[0].routing_provider.as_deref(),
        Some("google")
    );
    assert!(report
        .accounts
        .iter()
        .all(|account| account.vault.joined && account.cold_input_usable));
    assert!(report.alarms.is_empty());
    drop(state);
    let mut params = admit_params("ag-admit", "ag-conversation", "broca");
    params["provider"] = json!("google");
    params["model"] = json!("antigravity-gemini-3.8-flash");
    params["affinity_model"] = params["model"].clone();
    let admitted = result(rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params));
    assert_eq!(admitted["account"]["account_id"], ACCOUNT_B);
    assert_eq!(admitted["account"]["refresh_adapter"], "antigravity");
}

#[test]
fn antigravity_pro_argon_weekly_limit_gates_argon_only() {
    // A paid Pro account publishes its separate Gemini 4 Argon weekly limit
    // as `gemini-argon-wtu-weekly`; Ultra and free accounts publish none.
    let entry = |account: &str, gemini_weekly: f64, argon: Option<f64>| {
        let mut windows = vec![
            json!({"id": "gemini-5h", "windowKind": "five_hour", "window": {"usedPercent": 10.0, "windowMinutes": 300}}),
            json!({"id": "gemini-weekly", "windowKind": "weekly", "window": {"usedPercent": gemini_weekly, "windowMinutes": 10080}}),
        ];
        if let Some(used) = argon {
            windows.push(json!({"id": "gemini-argon-wtu-weekly", "windowKind": "weekly", "window": {"usedPercent": used, "windowMinutes": 10080}}));
        }
        json!({
            "provider": "antigravity", "apiProvider": "google", "account": account,
            "source": "oauth", "fetchedAt": "2026-09-29T11:59:00Z",
            "usage": {"extraRateWindows": windows}
        })
    };
    let admit = |rig: &Fixture, id: &str, model: &str| {
        let mut params = admit_params(id, id, "broca");
        params["provider"] = json!("google");
        params["model"] = json!(model);
        params["affinity_model"] = json!(model);
        rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params)
    };
    const ARGON: &str = "antigravity-gemini-4-argon";
    const GEMINI: &str = "antigravity-gemini-3.8-flash";

    // Both accounts are Pro with the Argon limit spent: Argon is refused,
    // while another Gemini route is still admitted on the same accounts.
    let pro = provider_fixture(
        "argon-pro",
        "antigravity",
        vec![
            entry(ACCOUNT_A, 20.0, Some(100.0)),
            entry(ACCOUNT_B, 30.0, Some(100.0)),
        ],
    );
    let refused = admit(&pro, "argon-pro", ARGON);
    assert!(
        refused.code().is_some(),
        "a spent Argon weekly limit refuses Argon: {refused:?}"
    );
    assert!(preflight_model(&pro.ctx(true, NOW), "google", ARGON).is_err());
    let admitted = result(admit(&pro, "gemini-pro", GEMINI));
    assert_eq!(admitted["account"]["account_id"], ACCOUNT_A);
    let state = pro.accounts.lock().unwrap();
    let report = account_quota::build_report(
        &state.readings,
        state.roster.as_ref(),
        &state.roster_poll,
        NOW,
    );
    assert!(
        report.alarms.is_empty(),
        "the Argon window is a known pool, not an unmapped scope: {:?}",
        report.alarms
    );
    drop(state);

    // Account A has no Argon window (Ultra): its Gemini windows alone admit
    // Argon, beside a Pro account whose Argon limit is spent.
    let mixed = provider_fixture(
        "argon-mixed",
        "antigravity",
        vec![
            entry(ACCOUNT_A, 20.0, None),
            entry(ACCOUNT_B, 10.0, Some(100.0)),
        ],
    );
    let admitted = result(admit(&mixed, "argon-mixed", ARGON));
    assert_eq!(admitted["account"]["account_id"], ACCOUNT_A);

    // The same account's Gemini windows still gate Argon: with its Gemini
    // weekly pool spent, Argon has no account left.
    let ultra_spent = provider_fixture(
        "argon-ultra-spent",
        "antigravity",
        vec![
            entry(ACCOUNT_A, 100.0, None),
            entry(ACCOUNT_B, 10.0, Some(100.0)),
        ],
    );
    assert!(admit(&ultra_spent, "argon-ultra-spent", ARGON)
        .code()
        .is_some());
}

#[test]
fn vendorless_reading_is_unmapped_and_never_borrows_a_siblings_vendor() {
    let mut missing = reported_entry("codex", Value::Null, ACCOUNT_A, "weekly", 10080, 0.0);
    missing.as_object_mut().unwrap().remove("apiProvider");
    let rig = provider_fixture(
        "vendorless",
        "openai",
        vec![
            missing,
            reported_entry("codex", json!("openai"), ACCOUNT_B, "weekly", 10080, 10.0),
        ],
    );
    let state = rig.accounts.lock().unwrap();
    let report = account_quota::build_report(
        &state.readings,
        state.roster.as_ref(),
        &state.roster_poll,
        NOW,
    );
    let unmapped = report
        .accounts
        .iter()
        .find(|account| account.account_id == ACCOUNT_A)
        .unwrap();
    assert_eq!(unmapped.routing_provider, None);
    assert!(!unmapped.cold_input_usable);
    assert!(report
        .information
        .iter()
        .any(|alarm| alarm.contains("unmapped") && alarm.contains(ACCOUNT_A)));
    assert!(report.alarms.is_empty());
    let roster = allocator_roster(&state, "openai", NOW).roster.unwrap();
    assert_eq!(roster.accounts.len(), 1);
    assert_eq!(roster.accounts[0].account_id, ACCOUNT_B);
    drop(state);
    let mut params = admit_params("vendorless-admit", "vendorless-conversation", "broca");
    params["provider"] = json!("openai");
    params["model"] = json!("gpt-6");
    params["affinity_model"] = json!("gpt-6");
    assert_eq!(
        result(rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params))["account"]
            ["account_id"],
        ACCOUNT_B
    );
}

#[test]
fn arbitrary_vendor_uses_monthly_and_session_windows_and_unknown_vendor_is_typed() {
    let mut spent = reported_entry(
        "new-feed",
        json!("new-vendor"),
        ACCOUNT_A,
        "monthly",
        43200,
        100.0,
    );
    spent["usage"]["secondary"] =
        json!({"windowKind": "session", "windowMinutes": 60, "usedPercent": 0.0});
    let rig = provider_fixture(
        "new-vendor",
        "unrelated-adapter",
        vec![
            spent,
            reported_entry(
                "new-feed",
                json!("new-vendor"),
                ACCOUNT_B,
                "session",
                60,
                10.0,
            ),
        ],
    );
    let mut params = admit_params("new-admit", "new-conversation", "broca");
    params["provider"] = json!("new-vendor");
    params["model"] = json!("new-model");
    params["affinity_model"] = json!("new-model");
    let admitted = result(rig.call(
        &rig.ctx(true, NOW),
        "route.account_for",
        BROCA,
        params.clone(),
    ));
    assert_eq!(admitted["account"]["account_id"], ACCOUNT_B);
    assert_eq!(admitted["account"]["refresh_adapter"], "unrelated-adapter");
    params["provider"] = json!("untracked");
    assert_eq!(
        rig.call(
            &rig.ctx(true, NOW),
            "route.account_for",
            BROCA,
            params.clone()
        )
        .code(),
        Some("admission_conflict")
    );
    params["admission_id"] = json!("unknown-provider-admit");
    // No credential in the vault serves an untracked provider: the
    // runner needs a login it does not have (`auth_required`, untimed),
    // not a transient retry.
    assert_eq!(
        rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params)
            .code(),
        Some("auth_required")
    );
}

#[test]
fn account_id_join_does_not_require_one_vendor_per_refresh_adapter() {
    let rig = provider_fixture(
        "shared-adapter",
        "shared-adapter",
        vec![
            reported_entry("feed-a", json!("vendor-a"), ACCOUNT_A, "session", 60, 10.0),
            reported_entry(
                "feed-b",
                json!("vendor-b"),
                ACCOUNT_B,
                "monthly",
                43200,
                10.0,
            ),
        ],
    );
    let state = rig.accounts.lock().unwrap();
    let a = allocator_roster(&state, "vendor-a", NOW).roster.unwrap();
    let b = allocator_roster(&state, "vendor-b", NOW).roster.unwrap();
    assert_eq!(a.accounts.len(), 1);
    assert_eq!(a.accounts[0].account_id, ACCOUNT_A);
    assert_eq!(b.accounts.len(), 1);
    assert_eq!(b.accounts[0].account_id, ACCOUNT_B);
}

/// Sanitized from the 2026-10-03 usage.get capture: both Antigravity lanes
/// omit apiProvider; the vault lane reports 5h + weekly pools while the
/// OAuth lane reports weekly pools only. Identity and dates are test-local.
fn live_antigravity_shapes() -> Vec<Value> {
    vec![
        json!({
            "provider": "antigravity", "apiProvider": null, "account": ACCOUNT_A,
            "source": "vault", "fetchedAt": "2026-09-29T11:59:00Z",
            "usage": {"extraRateWindows": [
                {"id": "gemini-5h", "title": "Gemini Models 5h", "window": {"usedPercent": 3.85, "windowKind": "five_hour", "windowMinutes": 300}},
                {"id": "gemini-weekly", "title": "Gemini Models weekly", "window": {"usedPercent": 52.81, "windowKind": "weekly", "windowMinutes": 10080}},
                {"id": "3p-5h", "title": "Claude and GPT models 5h", "window": {"usedPercent": 0.0, "windowKind": "five_hour", "windowMinutes": 300}},
                {"id": "3p-weekly", "title": "Claude and GPT models weekly", "window": {"usedPercent": 0.0, "windowKind": "weekly", "windowMinutes": 10080}}
            ]}
        }),
        json!({
            "provider": "antigravity", "apiProvider": null, "account": ACCOUNT_B,
            "source": "oauth", "fetchedAt": "2026-09-29T11:59:00Z",
            "usage": {"extraRateWindows": [
                {"id": "gemini-weekly", "title": "Gemini Models weekly", "window": {"usedPercent": 100.0, "windowKind": "weekly", "windowMinutes": 10080}},
                {"id": "3p-weekly", "title": "Claude and GPT models weekly", "window": {"usedPercent": 0.0, "windowKind": "weekly", "windowMinutes": 10080}}
            ]}
        }),
    ]
}

#[test]
fn live_google_shapes_keep_vendorless_antigravity_informational_and_unselectable() {
    let mut entries = live_antigravity_shapes();
    // The live Gemini CLI row has a vendor but no account or usage because
    // its credential is absent. It is not a tracked Google account.
    entries.push(json!({"provider": "gemini", "apiProvider": "google", "errorClass": "credential_absent", "usage": null}));
    let rig = provider_fixture("live-google", "antigravity", entries);
    let state = rig.accounts.lock().unwrap();
    let report = account_quota::build_report(
        &state.readings,
        state.roster.as_ref(),
        &state.roster_poll,
        NOW,
    );
    assert_eq!(report.accounts.len(), 2);
    assert!(report
        .accounts
        .iter()
        .all(|account| account.routing_provider.is_none()
            && account.vault.joined
            && !account.cold_input_usable));
    assert_eq!(report.information.len(), 2);
    assert!(report
        .information
        .iter()
        .all(|information| information.contains("unmapped entry: no apiProvider")));
    assert!(
        report.alarms.is_empty(),
        "a missing producer vendor must not page through a roster gap: {:?}",
        report.alarms
    );
    assert!(report.gaps.insula_only.is_empty());
    assert!(report.gaps.vault_only.is_empty());
    drop(state);
    let mut params = admit_params("live-google", "live-google", "broca");
    params["provider"] = json!("google");
    params["model"] = json!("antigravity-gemini-3.8-flash");
    params["affinity_model"] = params["model"].clone();
    // The only google credential is absent and the vendorless lanes are
    // unmapped: no credential of the right kind serves this model, so
    // `auth_required` (untimed), not a transient retry.
    assert_eq!(
        rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params)
            .code(),
        Some("auth_required")
    );
}

#[test]
fn gemini_cli_account_cannot_serve_antigravity_even_with_sticky_affinity() {
    let mut entries = live_antigravity_shapes();
    // The captured Gemini row is credential_absent and unkeyed. Model a
    // future authorized CLI account with headroom to exercise the vendor
    // collision, rather than passing only because today's account is absent.
    let mut gemini = reported_entry("gemini", json!("google"), INSULA_ONLY, "session", 1440, 0.0);
    gemini["usage"]["extraRateWindows"] = json!([{"id": "gemini-2.5-flash", "title": "gemini-2.5-flash", "window": {"usedPercent": 0.0, "windowMinutes": 1440}}]);
    entries.push(gemini);
    let rig = provider_fixture("gemini-cli-collision", "antigravity", entries);
    let mut vault = listing();
    vault.credentials.truncate(1);
    let row = &mut vault.credentials[0];
    row.account_id = Some(INSULA_ONLY.into());
    row.refresh_adapter = Some("gemini-cli".into());
    row.id = "oauth:gemini-cli".into();
    let gemini_roster = account_quota::build_roster(&vault, None, NOW);
    rig.accounts
        .lock()
        .unwrap()
        .roster
        .as_mut()
        .unwrap()
        .identities
        .extend(gemini_roster.identities);
    let ctx = rig.ctx(true, NOW);
    let mut params = admit_params("gemini-cli-initial", "google-product-collision", "broca");
    params["provider"] = json!("google");
    params["model"] = json!("gemini-2.5-flash");
    params["affinity_model"] = json!("antigravity-gemini-3.8-flash");
    let initial = result(rig.call(&ctx, "route.account_for", BROCA, params.clone()));
    assert_eq!(
        initial["account"]["account_id"], INSULA_ONLY,
        "the control CLI route can use its own account"
    );
    params["admission_id"] = json!("antigravity-sticky");
    params["model"] = params["affinity_model"].clone();
    // The gemini-cli account cannot serve antigravity models: its pool
    // reach covers only gemini-2.5-flash, so no credential of the right
    // kind exists (`auth_required`, untimed), even with sticky affinity.
    assert_eq!(
        rig.call(&ctx, "route.account_for", BROCA, params.clone())
            .code(),
        Some("auth_required")
    );
    params["admission_id"] = json!("antigravity-cold");
    params["conversation_key"] = json!("antigravity-cold");
    assert_eq!(
        rig.call(&ctx, "route.account_for", BROCA, params).code(),
        Some("auth_required")
    );
    let mut unknown = admit_params("unknown-pool", "unknown-pool", "broca");
    unknown["provider"] = json!("google");
    unknown["model"] = json!("antigravity-new-family-model");
    unknown["affinity_model"] = unknown["model"].clone();
    // Same kind mismatch for a model no account's pools name.
    assert_eq!(
        rig.call(&ctx, "route.account_for", BROCA, unknown).code(),
        Some("auth_required")
    );
}

fn status(fixture: &Fixture, ctx: &Ctx<'_>, account: &str) -> Value {
    result(fixture.call(
        ctx,
        ACCOUNT_STATUS_OP,
        BROCA,
        json!({ "provider": "anthropic", "account_id": account }),
    ))
}

fn assert_status_vector(fixture: &Fixture, ctx: &Ctx<'_>, source: &str) {
    let vector: Value = serde_json::from_str(source).unwrap();
    let request = &vector["request"];
    let reply = result(fixture.call(
        ctx,
        request["method"].as_str().unwrap(),
        BROCA,
        request["params"].clone(),
    ));
    assert_eq!(json!({ "result": reply }), vector["reply"]);
}

#[test]
fn account_status_codex_uses_openai_vendor_and_reported_weekly_window() {
    let mut fixture = provider_fixture(
        "status-codex",
        "openai",
        vec![reported_entry(
            "codex",
            json!("openai"),
            ACCOUNT_A,
            "weekly",
            10080,
            10.0,
        )],
    );
    fixture.policy.reserve_five_hour = 95.0;
    fixture.policy.reserve_seven_day = 8.0;
    for (used, warm_ok, reason) in [
        (10.0, true, json!({ "code": "headroom" })),
        (
            92.0,
            false,
            json!({ "code": "within_reserve", "window": "weekly" }),
        ),
    ] {
        fixture
            .accounts
            .lock()
            .unwrap()
            .readings
            .readings
            .get_mut(&("codex".into(), ACCOUNT_A.into()))
            .unwrap()
            .entry["usage"]["primary"]["usedPercent"] = json!(used);
        let reply = result(fixture.call(
            &fixture.ctx(true, NOW),
            ACCOUNT_STATUS_OP,
            BROCA,
            json!({ "provider": "openai", "account_id": ACCOUNT_A }),
        ));
        assert_eq!(reply["provider"], "openai");
        assert_eq!(reply["account_id"], ACCOUNT_A);
        assert_eq!(reply["observed_at"], "2026-09-29T11:59:00Z");
        assert_eq!(reply["age_band"], "full");
        assert_eq!(reply["warm_ok"], warm_ok);
        assert_eq!(reply["reason"], reason);
        assert_eq!(
            reply["windows"],
            json!([{ "kind": "weekly", "used_percent": used,
                         "resets_at": null, "window_minutes": 10080 }])
        );
    }
}

#[test]
fn account_status_antigravity_uses_pools_not_aggregate_windows() {
    let mut fixture = provider_fixture(
        "status-antigravity",
        "antigravity",
        vec![json!({
            "provider": "antigravity", "apiProvider": "google", "account": ACCOUNT_A,
            "source": "oauth", "fetchedAt": "2026-09-29T11:59:00Z",
            "usage": {
                "primary": { "windowKind": "weekly", "windowMinutes": 10080, "usedPercent": 100.0 },
                "extraRateWindows": [
                    { "id": "gemini-5h", "window": { "usedPercent": 90.0 } },
                    { "id": "3p-weekly", "window": { "usedPercent": 10.0 } }
                ]
            }
        })],
    );
    fixture.policy.reserve_five_hour = 10.0;
    fixture.policy.reserve_seven_day = 1.0;
    let reply = result(fixture.call(
        &fixture.ctx(true, NOW),
        ACCOUNT_STATUS_OP,
        BROCA,
        json!({ "provider": "google", "account_id": ACCOUNT_A }),
    ));
    assert_eq!(reply["warm_ok"], false);
    assert_eq!(
        reply["reason"],
        json!({ "code": "within_reserve", "window": "gemini-5h" })
    );
    assert_eq!(
        reply["windows"],
        json!([
            { "kind": "gemini-5h", "used_percent": 90.0,
              "resets_at": null, "window_minutes": 300 },
            { "kind": "3p-weekly", "used_percent": 10.0,
              "resets_at": null, "window_minutes": 10080 },
        ])
    );
}

#[test]
fn account_status_fresh_headroom_allows_warming() {
    let fixture = fixture("status-headroom");
    assert_status_vector(
        &fixture,
        &fixture.ctx(true, NOW),
        include_str!("../../../../test-vectors/route-account-status-v1/warm_ok.json"),
    );
    let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
    assert_eq!(reply["warm_ok"], true);
    assert_eq!(reply["reason"], json!({ "code": "headroom" }));
    assert_eq!(reply["provider"], "anthropic");
    assert_eq!(reply["account_id"], ACCOUNT_A);
    assert_eq!(reply["observed_at"], "2026-09-29T11:59:00Z");
    assert_eq!(reply["age_band"], "full");
    assert_eq!(
        reply["windows"],
        json!([
            { "kind": "five_hour", "used_percent": 10.0,
              "resets_at": "2026-09-29T14:00:00Z", "window_minutes": 300 },
            { "kind": "weekly", "used_percent": 40.0,
              "resets_at": null, "window_minutes": 10080 },
            { "kind": "Fable", "used_percent": 5.0,
              "resets_at": null, "window_minutes": 10080 },
        ])
    );
}

#[test]
fn account_status_within_reserve_names_the_window() {
    let mut fixture = fixture("status-reserve");
    fixture
        .accounts
        .lock()
        .unwrap()
        .readings
        .readings
        .get_mut(&("claude".into(), ACCOUNT_A.into()))
        .unwrap()
        .entry["usage"]["primary"]["usedPercent"] = json!(99.5);
    assert_status_vector(
        &fixture,
        &fixture.ctx(true, NOW),
        include_str!("../../../../test-vectors/route-account-status-v1/within_reserve.json"),
    );
    fixture.policy.reserve_five_hour = 12.0;
    fixture.policy.reserve_seven_day = 8.0;
    fixture.policy.reserve_scoped = 3.0;
    for (slot, kind, used) in [
        ("primary", "five_hour", 88.0),
        ("secondary", "weekly", 93.0),
        ("scoped", "Fable", 97.0),
        ("reported", "producer_monthly", 93.0),
        ("reported_extra", "producer_pool", 93.0),
    ] {
        let mut entry = claude_entry(ACCOUNT_A);
        if slot == "scoped" {
            entry["usage"]["extraRateWindows"][0]["window"]["usedPercent"] = json!(used);
        } else if slot == "reported" {
            entry["usage"]["tertiary"] = json!({
                "windowKind": kind, "usedPercent": used, "windowMinutes": 43200,
            });
        } else if slot == "reported_extra" {
            entry["usage"]["extraRateWindows"]
                .as_array_mut()
                .unwrap()
                .push(json!({
                    "id": "unknown_scope", "windowKind": kind,
                    "window": { "usedPercent": used, "windowMinutes": 300 },
                }));
        } else {
            entry["usage"][slot]["usedPercent"] = json!(used);
        }
        fixture
            .accounts
            .lock()
            .unwrap()
            .readings
            .readings
            .get_mut(&("claude".into(), ACCOUNT_A.into()))
            .unwrap()
            .entry = entry;
        let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
        assert_eq!(reply["warm_ok"], false, "{kind}");
        assert_eq!(
            reply["reason"],
            json!({ "code": "within_reserve", "window": kind })
        );
    }
}

#[test]
fn account_status_preserves_producer_window_identity_not_slot_names() {
    let fixture = fixture("status-identity");
    let mut entry = claude_entry(ACCOUNT_A);
    let five_hour = entry["usage"]["primary"].clone();
    entry["usage"]["primary"] = entry["usage"]["secondary"].clone();
    entry["usage"]["secondary"] = five_hour;
    entry["usage"]["primary"]["windowKind"] = json!("producer_weekly");
    entry["usage"]["extraRateWindows"][0]["windowKind"] = json!("weekly");
    fixture
        .accounts
        .lock()
        .unwrap()
        .readings
        .readings
        .get_mut(&("claude".into(), ACCOUNT_A.into()))
        .unwrap()
        .entry = entry;
    let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
    assert_eq!(reply["warm_ok"], true);
    assert_eq!(reply["windows"][0]["kind"], "producer_weekly");
    assert_eq!(reply["windows"][1]["kind"], "five_hour");
    assert_eq!(reply["windows"][2]["kind"], "weekly");
}

#[test]
fn account_status_stale_reading_stops_warming() {
    let fixture = fixture("status-stale");
    // The reading was observed a minute before NOW: exactly the allocator's
    // excluded boundary, not a separate warming timeout.
    let ctx = fixture.ctx(true, NOW - 60_000 + fixture.policy.excluded_band_ms);
    assert_status_vector(
        &fixture,
        &ctx,
        include_str!("../../../../test-vectors/route-account-status-v1/reading_stale.json"),
    );
    let reply = status(&fixture, &ctx, ACCOUNT_A);
    assert_eq!(reply["warm_ok"], false);
    assert_eq!(reply["reason"], json!({ "code": "reading_stale" }));
    assert_eq!(reply["age_band"], "excluded");
    assert_eq!(reply["windows"].as_array().unwrap().len(), 3);
}

#[test]
fn account_status_uses_cold_placement_freshness_rules() {
    let mut fixture = fixture("status-freshness");
    fixture.policy.full_band_ms = 2 * 60_000;
    fixture.policy.excluded_band_ms = 10 * 60_000;
    let discounted = fixture.ctx(true, NOW + 60_000);
    let reply = status(&fixture, &discounted, ACCOUNT_A);
    assert_eq!(reply["age_band"], "discounted");
    assert_eq!(reply["warm_ok"], true);
    for (field, value, expected) in [
        ("errorClass", json!("upstream_failed"), false),
        ("error", json!("poll failed"), false),
        ("stale", json!({ "class": "unknown" }), false),
        ("stale", json!({ "class": "upstream_failed" }), true),
        (
            "fetchedAt",
            json!(rfc3339(NOW + fixture.policy.future_skew_ms + 1000)),
            false,
        ),
        ("fetchedAt", Value::Null, false),
    ] {
        let mut entry = claude_entry(ACCOUNT_A);
        entry[field] = value;
        fixture
            .accounts
            .lock()
            .unwrap()
            .readings
            .readings
            .get_mut(&("claude".into(), ACCOUNT_A.into()))
            .unwrap()
            .entry = entry;
        let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
        assert_eq!(reply["warm_ok"], expected, "{field}");
        assert_eq!(
            reply["reason"]["code"],
            if expected {
                "headroom"
            } else {
                "reading_stale"
            }
        );
    }
}

#[test]
fn account_status_unknown_account_is_not_a_warming_target() {
    let fixture = fixture("status-unknown");
    for account in ["unknown", INSULA_ONLY] {
        let reply = status(&fixture, &fixture.ctx(true, NOW), account);
        assert_eq!(reply["warm_ok"], false);
        assert_eq!(reply["reason"], json!({ "code": "account_unknown" }));
        assert_eq!(reply["observed_at"], Value::Null);
        assert_eq!(reply["windows"], json!([]));
    }
}

#[test]
fn account_status_missing_or_invalid_windows_stop_warming() {
    let fixture = fixture("status-missing");
    let reply = status(&fixture, &fixture.ctx(true, NOW), VAULT_ONLY);
    assert_eq!(reply["warm_ok"], false);
    assert_eq!(reply["reason"]["code"], "reading_missing");
    for usage in [
        Value::Null,
        json!({}),
        json!({ "primary": { "usedPercent": 10.0 } }),
        json!({ "primary": { "usedPercent": -1.0, "windowMinutes": 300 },
                    "secondary": { "usedPercent": 10.0, "windowMinutes": 10080 } }),
    ] {
        let mut entry = claude_entry(ACCOUNT_A);
        entry["usage"] = usage;
        fixture
            .accounts
            .lock()
            .unwrap()
            .readings
            .readings
            .get_mut(&("claude".into(), ACCOUNT_A.into()))
            .unwrap()
            .entry = entry;
        let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
        assert_eq!(reply["warm_ok"], false);
        assert_eq!(reply["reason"]["code"], "reading_missing");
    }
    fixture
        .accounts
        .lock()
        .unwrap()
        .readings
        .readings
        .get_mut(&("claude".into(), ACCOUNT_A.into()))
        .unwrap()
        .tombstoned_at_ms = Some(NOW);
    let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
    assert_eq!(reply["warm_ok"], false);
    assert_eq!(reply["reason"]["code"], "reading_missing");
}

#[test]
fn account_status_disabled_never_allows_warming() {
    let fixture = fixture("status-disabled");
    let reply = status(&fixture, &fixture.ctx(false, NOW), ACCOUNT_A);
    assert_eq!(reply["warm_ok"], false);
    assert_eq!(
        reply["reason"],
        json!({ "code": "account_selection_disabled" })
    );
}

#[test]
fn account_status_writes_nothing_whether_serving_is_on_or_off() {
    let fixture = fixture("status-no-writes");
    // data_version on a separate connection changes for any committed write
    // on the store connection, including tables outside account serving.
    let observer = rusqlite::Connection::open(fixture.dir.join("store.db")).unwrap();
    let version = || {
        observer
            .query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0))
            .unwrap()
    };
    let before = version();
    for enabled in [true, false] {
        let ctx = fixture.ctx(enabled, NOW);
        for account in [ACCOUNT_A, VAULT_ONLY, "unknown"] {
            status(&fixture, &ctx, account);
        }
    }
    assert_eq!(
        version(),
        before,
        "a quota read must not commit any store writes"
    );
    assert_eq!(fixture.store().account_serving_counts().unwrap().total(), 0);
    assert!(fixture.serve_rows().is_empty());
}

#[test]
fn pinned_model_account_preflight_refuses_all_walls_but_admits_a_healthy_sibling() {
    let fixture = fixture("pin-preflight");
    let mut a = claude_entry(ACCOUNT_A);
    let mut b = claude_entry(ACCOUNT_B);
    a["usage"]["primary"]["usedPercent"] = json!(100.0);
    b["usage"]["primary"]["usedPercent"] = json!(100.0);
    let apply = |b: &Value| {
        let mut vault_only = a.clone();
        vault_only["account"] = json!(VAULT_ONLY);
        let bytes =
            serde_json::to_vec(&json!({"result":[a,b,vault_only],"completeProviders":["claude"]}))
                .unwrap();
        apply_provider_reply(&mut fixture.accounts.lock().unwrap(), "claude", &bytes, NOW).unwrap();
    };
    apply(&b);
    let ctx = fixture.ctx(true, NOW);
    let (code, message) = preflight_model(&ctx, "anthropic", "claude-opus-5-5").unwrap_err();
    assert_eq!(code, "account_pool_reserve_limited");
    assert!(message.contains("until 2026-09-29T14:00:00Z"), "{message}");
    b["usage"]["primary"]["usedPercent"] = json!(10.0);
    apply(&b);
    assert!(preflight_model(&ctx, "anthropic", "claude-opus-5-5").is_ok());
    assert!(
        fixture.serve_rows().is_empty(),
        "preflight must not commit a runner admission"
    );
}

#[test]
fn every_account_op_refuses_with_account_selection_disabled_and_writes_nothing() {
    let fixture = fixture("disabled");
    let ctx = fixture.ctx(false, NOW);
    let calls = [
        (
            ACCOUNT_FOR_OP,
            admit_params("adm-1", "consult:c:s:1", "broca"),
        ),
        (
            ACCOUNT_OBSERVE_OP,
            json!({ "event_id": "e1", "caller": { "runner": "broca" }, "kind": "admission_started",
                        "admission_id": "adm-1", "run_id": "run-1" }),
        ),
        (ACCOUNT_RELEASE_OP, json!({ "admission_id": "adm-1" })),
    ];
    for (method, params) in calls {
        let reply = fixture.call(&ctx, method, BROCA, params);
        assert_eq!(
            reply.code(),
            Some(DISABLED_CODE),
            "{method} must refuse while disabled"
        );
        let OpReply::Error { detail, .. } = &reply else {
            unreachable!()
        };
        assert_eq!(detail.as_ref().unwrap()["code"], DISABLED_CODE);
    }
    // Malformed params are refused the same way: nothing is even parsed.
    let reply = handle(
        ACCOUNT_FOR_OP,
        b"{\"method\":\"route.account_for\"}",
        BROCA,
        &ctx,
    );
    assert_eq!(reply.code(), Some(DISABLED_CODE));
    assert_eq!(fixture.store().account_serving_counts().unwrap().total(), 0);
    assert!(fixture.serve_rows().is_empty());
}

#[test]
fn a_roster_gap_is_reported_in_both_directions_without_emails() {
    let fixture = fixture("gap");
    let build = {
        let guard = fixture.accounts.lock().unwrap();
        allocator_roster(&guard, "anthropic", NOW)
    };
    let roster = build.roster.as_ref().expect("vault roster");
    let ids: Vec<&str> = roster
        .accounts
        .iter()
        .map(|view| view.account_id.as_str())
        .collect();
    assert_eq!(ids, vec![ACCOUNT_A, ACCOUNT_B, VAULT_ONLY]);
    assert!(
        !ids.contains(&INSULA_ONLY),
        "an account the vault does not list is never a candidate"
    );
    let unread = roster
        .accounts
        .iter()
        .find(|view| view.account_id == VAULT_ONLY)
        .unwrap();
    assert!(unread.quota.is_none());
    let gaps = &build.context["gaps"];
    assert_eq!(gaps["insula_only"][0]["account_id"], INSULA_ONLY);
    assert_eq!(gaps["vault_only"][0]["account_id"], VAULT_ONLY);
    assert_eq!(gaps["vault_only"][0]["first_reading_overdue"], true);
    let alarms = build.context["alarms"].as_array().unwrap();
    assert!(alarms
        .iter()
        .any(|alarm| alarm.as_str().unwrap().contains(VAULT_ONLY)));
    assert!(
        !build.context.to_string().contains("@example.test"),
        "emails stay hidden"
    );

    // The served decision's record carries the same gaps.
    let ctx = fixture.ctx(true, NOW);
    result(fixture.call(
        &ctx,
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("adm-gap", "k-gap", "broca"),
    ));
    let row = &fixture.serve_rows()[0];
    let context: Value = serde_json::from_str(row.context_json.as_deref().unwrap()).unwrap();
    assert_eq!(
        context["roster"]["gaps"]["insula_only"][0]["account_id"],
        INSULA_ONLY
    );

    // A roster past 24 hours is still passed through, marked, so existing
    // affinity continues while new cold work is refused.
    let old = {
        let guard = fixture.accounts.lock().unwrap();
        allocator_roster(&guard, "anthropic", NOW + 24 * 60 * 60_000)
    };
    assert_eq!(old.context["past_max_age"], true);
    let ctx = fixture.ctx(true, NOW + 24 * 60 * 60_000);
    let reply = fixture.call(
        &ctx,
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("adm-old", "k-old", "broca"),
    );
    assert_eq!(reply.code(), Some("account_selection_unavailable"));
}

#[test]
fn different_candidates_replay_the_stored_decision_without_charging() {
    let fixture = fixture("replay-candidates");
    let ctx = fixture.ctx(true, NOW);
    let mut params = admit_params("adm-retry", "broca:retry", "broca");
    params["candidates"] = json!({
        "view_digest": "vault-before",
        "accounts": [{ "account_id": ACCOUNT_A, "credential_id": "oauth:anthropic" }]
    });
    let reply = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params.clone()));
    assert_eq!(reply["account"]["account_id"], ACCOUNT_A);
    let before = fixture
        .store()
        .account_transaction(|tx| tx.admission(BROCA, "adm-retry"))
        .unwrap()
        .unwrap();
    assert!(before.charged);
    let counts = fixture.store().account_serving_counts().unwrap().total();
    params["candidates"] = json!({
        "view_digest": "vault-after",
        "accounts": [{ "account_id": ACCOUNT_B, "credential_id": "oauth:anthropic:ysample" }]
    });
    let replayed = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params));
    assert_eq!(
        replayed, reply,
        "replay keeps even an account absent from candidates"
    );
    let after = fixture
        .store()
        .account_transaction(|tx| tx.admission(BROCA, "adm-retry"))
        .unwrap()
        .unwrap();
    assert_eq!(after, before, "replay must not change the charged demand");
    assert_eq!(
        fixture.store().account_serving_counts().unwrap().total(),
        counts
    );
    assert_eq!(
        fixture.serve_rows().len(),
        1,
        "replay creates no new decision"
    );
}

#[test]
fn admission_fingerprint_is_independent_of_json_key_order() {
    let forward: Value = serde_json::from_str(r#"{
            "provider":"anthropic", "model":"claude-opus-5-5",
            "conversation_key":"broca:retry", "caller":{"runner":"broca","harness":"test"},
            "admission_id":"adm-retry", "requested_account":{"account_id":"account-a","credential_id":"cred-a"}
        }"#).unwrap();
    let reversed: Value = serde_json::from_str(
        r#"{
            "requested_account":{"credential_id":"cred-a","account_id":"account-a"},
            "admission_id":"adm-retry", "caller":{"harness":"test","runner":"broca"},
            "conversation_key":"broca:retry", "model":"claude-opus-5-5", "provider":"anthropic"
        }"#,
    )
    .unwrap();
    assert_eq!(
        fingerprint(&parse_account_for(&forward).unwrap()),
        fingerprint(&parse_account_for(&reversed).unwrap())
    );
}

#[test]
fn admission_fingerprint_ignores_advisory_inputs_but_covers_identity() {
    let params = admit_params("adm-identity", "broca:identity", "broca");
    let baseline = fingerprint(&parse_account_for(&params).unwrap());
    for (field, value) in [
        ("initial_input_bytes", json!(99)),
        ("initial_input_estimated", json!(true)),
        ("model_decision_id", json!("new-decision")),
        ("hold_wait_max_s", json!(10)),
        ("caller", json!({"runner":"BROCA", "harness":"new-harness"})),
        ("vault_digest", json!("new-view")),
        ("quota_hint", json!(123)),
        ("timestamp", json!(NOW + 1)),
    ] {
        let mut changed = params.clone();
        changed[field] = value;
        assert_eq!(
            fingerprint(&parse_account_for(&changed).unwrap()),
            baseline,
            "{field}"
        );
    }
    for (field, value) in [
        ("provider", json!("openai")),
        ("model", json!("claude-fable-5-5")),
        ("affinity_model", json!("claude-fable-5-5")),
        ("conversation_key", json!("broca:other")),
        ("caller", json!({"runner":"plugin"})),
        ("expected_pin", json!({"id":"pin", "generation":1})),
        ("requested_account", json!({"account_id":"other"})),
        (
            "preferred_existing_account",
            json!({"credential_id":"other"}),
        ),
    ] {
        let mut changed = params.clone();
        changed[field] = value;
        assert_ne!(
            fingerprint(&parse_account_for(&changed).unwrap()),
            baseline,
            "{field}"
        );
    }
    let mut renew = params;
    renew["mode"] = json!("renew");
    renew["expected_pin"] = json!({"id":"pin", "generation":1});
    let renewing = fingerprint(&parse_account_for(&renew).unwrap());
    renew["mode"] = json!("reassign");
    assert_ne!(fingerprint(&parse_account_for(&renew).unwrap()), renewing);
}

#[test]
fn a_served_pin_survives_a_reopen_and_renewal_compare_and_sets_on_it() {
    let mut fixture = fixture("serve");
    let ctx = fixture.ctx(true, NOW);
    let params = admit_params("adm-1", "opencode:host:session-1", "anthropic-auth");
    let reply = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params.clone()));
    assert_eq!(
        reply["admission"],
        json!({ "id": "adm-1", "state": "reserved" })
    );
    assert_eq!(reply["account"]["provider"], "anthropic");
    assert_eq!(reply["account"]["refresh_adapter"], "anthropic");
    assert_eq!(reply["account"]["method"], "oauth");
    assert_eq!(reply["pin"]["generation"], 1);
    assert_eq!(reply["pin"]["mode"], "conversation_lease");
    assert_eq!(
        reply["pin"]["admission_valid_until"],
        "2026-09-29T12:01:00Z"
    );
    assert_eq!(reply["pin"]["lease_valid_until"], "2026-09-29T12:02:00Z");
    assert_eq!(reply["pin"]["affinity_expires_at"], "2026-10-06T12:00:00Z");
    assert_eq!(reply["reason"]["code"], "cold_scored");
    assert_eq!(reply["reason"]["policy_version"], 2);
    assert_eq!(reply["reason"]["quota_fetched_at"], "2026-09-29T11:59:00Z");
    assert_eq!(reply["reason"]["quota_age_band"], "full");
    let pin_id = reply["pin"]["id"].as_str().unwrap().to_string();
    let account = reply["account"]["account_id"].as_str().unwrap().to_string();

    // A transport retry gets the committed decision back; reusing the id
    // with another model changes admission identity and is a conflict.
    let again = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params.clone()));
    assert_eq!(again, reply);
    let mut changed = params.clone();
    changed["model"] = json!("claude-fable-5-5");
    let conflict = fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, changed);
    assert_eq!(conflict.code(), Some("admission_conflict"));

    fixture.reopen();
    let later = NOW + 30_000;
    let ctx = fixture.ctx(true, later);
    let mut renew = admit_params("adm-2", "opencode:host:session-1", "anthropic-auth");
    renew["mode"] = json!("renew");
    renew["expected_pin"] = json!({ "id": pin_id, "generation": 1 });
    let renewed = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, renew.clone()));
    assert_eq!(
        renewed["pin"]["id"],
        json!(pin_id),
        "the pin survived the reopen"
    );
    assert_eq!(renewed["pin"]["generation"], 1);
    assert_eq!(renewed["account"]["account_id"], json!(account));
    assert_eq!(renewed["reason"]["code"], "sticky_hit");
    let renewal = fixture
        .store()
        .account_transaction(|tx| tx.admission(BROCA, "adm-2"))
        .unwrap()
        .unwrap();
    assert!(!renewal.charged, "a renewal charges no demand");

    // A renewal naming an older generation gets the current pin back and
    // changes nothing.
    let mut stale = renew.clone();
    stale["admission_id"] = json!("adm-3");
    stale["expected_pin"] = json!({ "id": pin_id, "generation": 0 });
    let refused = fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, stale);
    assert_eq!(refused.code(), Some("pin_generation_conflict"));
    let OpReply::Error { detail, .. } = refused else {
        unreachable!()
    };
    assert_eq!(detail.unwrap()["current_pin"]["id"], json!(pin_id));
    assert!(fixture
        .store()
        .account_transaction(|tx| tx.admission(BROCA, "adm-3"))
        .unwrap()
        .is_none());

    // Another principal's conversation with the same key is its own.
    let other = result(fixture.call(
        &ctx,
        ACCOUNT_FOR_OP,
        "reserved:other",
        admit_params("adm-1", "opencode:host:session-1", "anthropic-auth"),
    ));
    assert_ne!(other["pin"]["id"], json!(pin_id));
}

#[test]
fn attempts_refresh_only_named_admission_and_its_started_reservations() {
    let mut fixture = fixture("last-observation");
    result(fixture.call(
        &fixture.ctx(true, NOW),
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("original", "broca:observed-run", "broca"),
    ));
    let observe = |kind: &str, event: &str| {
        json!({
            "event_id": event, "caller": {"runner": "broca"}, "kind": kind,
            "admission_id": "original", "run_id": "run", "observed_at_ms": NOW - 999_999
        })
    };
    let row = |fixture: &Fixture, id: &str| {
        fixture
            .store()
            .account_transaction(|tx| tx.admission(BROCA, id))
            .unwrap()
            .unwrap()
    };
    result(fixture.call(
        &fixture.ctx(true, NOW - 100),
        ACCOUNT_OBSERVE_OP,
        BROCA,
        observe("attempt", "before-start"),
    ));
    assert_eq!(row(&fixture, "original").last_observed_at, Some(NOW - 100));
    result(fixture.call(
        &fixture.ctx(true, NOW),
        ACCOUNT_OBSERVE_OP,
        BROCA,
        observe("admission_started", "start"),
    ));
    assert_eq!(row(&fixture, "original").last_observed_at, Some(NOW));
    let original = row(&fixture, "original");
    let other_account = if original.account_id == ACCOUNT_A {
        ACCOUNT_B
    } else {
        ACCOUNT_A
    };
    crate::account_writer::repair(fixture.store(), move |tx| {
        for id in ["moved", "older-move", "unused", "other-admission"] {
            let mut reservation = original.clone();
            reservation.admission_id = id.into();
            reservation.state = "reserved".into();
            reservation.run_id = None;
            reservation.started_at = None;
            reservation.last_observed_at = None;
            reservation.account_id = other_account.into();
            tx.insert_admission(&reservation)?;
            if id == "other-admission" {
                tx.mark_admission_started(BROCA, id, Some("run"), NOW, false)?;
            } else {
                tx.record_continuation(
                    BROCA,
                    id,
                    &striatum_store::V1Recorded {
                        body_json: "{}".into(),
                        answer_json: "{}".into(),
                    },
                    "run",
                    "original",
                    id,
                    Some((
                        id,
                        id,
                        if id == "unused" {
                            NOW + 50
                        } else {
                            NOW + 60_000
                        },
                    )),
                )?;
                if id == "older-move" {
                    tx.use_continuation(id, NOW + 10)?;
                }
            }
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(row(&fixture, "older-move").last_observed_at, Some(NOW + 10));
    let mut attempt = observe("attempt", "moved-attempt");
    attempt["served_account_id"] = json!(other_account);
    let reply = result(fixture.call(
        &fixture.ctx(true, NOW + 100),
        ACCOUNT_OBSERVE_OP,
        BROCA,
        attempt.clone(),
    ));
    assert_eq!(reply["observation"]["bound"], false);
    assert!(reply["observation"]["effects"]
        .as_array()
        .unwrap()
        .iter()
        .any(|effect| effect["effect"] == "served_account_mismatch"));
    for id in ["original", "moved", "older-move"] {
        assert_eq!(row(&fixture, id).last_observed_at, Some(NOW + 100), "{id}");
    }
    assert_eq!(row(&fixture, "unused").last_observed_at, None);
    assert_eq!(row(&fixture, "other-admission").last_observed_at, Some(NOW));
    // A replay and a caller's own timestamp cannot grant more liveness.
    let duplicate = result(fixture.call(
        &fixture.ctx(true, NOW + 500),
        ACCOUNT_OBSERVE_OP,
        BROCA,
        attempt.clone(),
    ));
    assert_eq!(duplicate["observation"]["duplicate"], true);
    assert_eq!(row(&fixture, "moved").last_observed_at, Some(NOW + 100));
    fixture.reopen();
    attempt["event_id"] = json!("after-restart");
    let later = NOW + 2 * 3_600_000;
    result(fixture.call(
        &fixture.ctx(true, later),
        ACCOUNT_OBSERVE_OP,
        BROCA,
        attempt,
    ));
    for id in ["original", "moved", "older-move"] {
        assert_eq!(row(&fixture, id).last_observed_at, Some(later), "{id}");
        let in_memory = crate::account_writer::repair(fixture.store(), move |tx| {
            Ok(tx.open_admission(BROCA, id)?.unwrap().last_observed_at)
        })
        .unwrap();
        assert_eq!(in_memory, Some(later));
    }
    assert_eq!(row(&fixture, "other-admission").last_observed_at, Some(NOW));
    // The working copy must not publish a time from a failed command.
    let failed: RouteStateResult<()> = crate::account_writer::repair(fixture.store(), move |tx| {
        let original = tx.admission(BROCA, "original")?.unwrap();
        tx.observe_admission(&original, later + 100)?;
        Err(striatum_core::route_state_store::RouteStateStoreError(
            "rollback".into(),
        ))
    });
    assert!(failed.is_err());
    assert_eq!(row(&fixture, "moved").last_observed_at, Some(later));
    let in_memory = crate::account_writer::repair(fixture.store(), |tx| {
        Ok(tx.open_admission(BROCA, "moved")?.unwrap().last_observed_at)
    })
    .unwrap();
    assert_eq!(in_memory, Some(later));
}

#[test]
fn observations_and_releases_act_only_on_the_principals_own_admission() {
    let fixture = fixture("observe");
    let ctx = fixture.ctx(true, NOW);
    let admitted = result(fixture.call(
        &ctx,
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("adm-1", "broca:run-1", "broca"),
    ));
    let account = admitted["account"]["account_id"].clone();
    let exhausted = |event: &str| {
        json!({
            "event_id": event,
            "caller": { "runner": "broca" },
            "kind": "account_limit_exhausted",
            "admission_id": "adm-1",
            "limit_window": "seven_day",
            "resets_at_ms": NOW + 3_600_000,
            "resets_at_source": "header",
            "observed_at_ms": NOW + 1_000,
        })
    };
    // Another principal naming Broca's admission changes nothing shared.
    let foreign = result(fixture.call(
        &ctx,
        ACCOUNT_OBSERVE_OP,
        "reserved:plugin",
        exhausted("e-1"),
    ));
    assert_eq!(foreign["observation"]["bound"], false);
    assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 0);
    let foreign_release = result(fixture.call(
        &ctx,
        ACCOUNT_RELEASE_OP,
        "reserved:plugin",
        json!({ "admission_id": "adm-1" }),
    ));
    assert_eq!(foreign_release["release"]["released"], false);

    let bound = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("e-2")));
    assert_eq!(bound["observation"]["bound"], true);
    assert_eq!(bound["observation"]["effects"][0]["effect"], "fence");
    assert_eq!(
        bound["observation"]["effects"][0]["fence"]["account_id"],
        account
    );
    // The same event again is recognized and changes nothing.
    let repeat = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("e-2")));
    assert_eq!(repeat["observation"]["duplicate"], true);
    assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 0);
    assert_eq!(
        fixture
            .store()
            .non_quota_account_route_fences(NOW)
            .unwrap()
            .len(),
        1,
        "headroom contradicts this refusal, so only its non-quota route fence stands"
    );

    // A new cold admission now avoids the fenced account.
    let next = result(fixture.call(
        &ctx,
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("adm-2", "broca:run-2", "broca"),
    ));
    assert_ne!(next["account"]["account_id"], account);

    // Release happens once, and never after the run started.
    let first = result(fixture.call(
        &ctx,
        ACCOUNT_RELEASE_OP,
        BROCA,
        json!({ "admission_id": "adm-2" }),
    ));
    assert_eq!(first["release"]["released"], true);
    assert_eq!(first["release"]["state"], "released");
    let second = result(fixture.call(
        &ctx,
        ACCOUNT_RELEASE_OP,
        BROCA,
        json!({ "admission_id": "adm-2" }),
    ));
    assert_eq!(second["release"]["released"], false);
    let late = fixture.ctx(true, NOW + 120_000);
    let started = result(fixture.call(
        &late,
        ACCOUNT_OBSERVE_OP,
        BROCA,
        json!({ "event_id": "s-1", "caller": { "runner": "broca" }, "kind": "admission_started",
                    "admission_id": "adm-1", "run_id": "run-1" }),
    ));
    assert_eq!(
        started["observation"]["effects"][0],
        json!({ "effect": "admission_started", "late": true })
    );
    let after_start = result(fixture.call(
        &late,
        ACCOUNT_RELEASE_OP,
        BROCA,
        json!({ "admission_id": "adm-1" }),
    ));
    assert_eq!(after_start["release"]["released"], false);
    assert_eq!(after_start["release"]["state"], "started");
}

#[test]
fn header_observations_bind_only_to_the_admitted_account() {
    for kind in ["attempt", "run_finished"] {
        let fixture = fixture("header-observation-account");
        let ctx = fixture.ctx(true, NOW);
        let admitted = result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-usage", "broca:usage", "broca"),
        ));
        let account = admitted["account"]["account_id"].as_str().unwrap();
        let other = if account == ACCOUNT_A {
            ACCOUNT_B
        } else {
            ACCOUNT_A
        };
        for (event, served, bound) in [
            ("mismatch", Some(other), false),
            ("matching", Some(account), true),
            ("unreported", None, true),
        ] {
            let headers = BTreeMap::from([
                (
                    "anthropic-ratelimit-unified-5h-status".to_string(),
                    "limited".to_string(),
                ),
                (
                    "anthropic-ratelimit-unified-fallback-percentage".to_string(),
                    "0.12345".to_string(),
                ),
            ]);
            let mut params = json!({
                "event_id": event,
                "caller": { "runner": "broca" },
                "kind": kind,
                "admission_id": "adm-usage",
                "observed_at_ms": NOW - 1_000,
                "headers": headers,
            });
            if let Some(served) = served {
                params["served_account_id"] = json!(served);
            }
            let body = serde_json::to_vec(&json!({
                "method": ACCOUNT_OBSERVE_OP,
                "params": params,
            }))
            .unwrap();
            let recorded = result(handle(ACCOUNT_OBSERVE_OP, &body, BROCA, &ctx));
            assert_eq!(recorded["observation"]["bound"], bound, "{kind}: {event}");
            if !bound {
                assert_eq!(
                    recorded["observation"]["effects"][0]["effect"],
                    "served_account_mismatch"
                );
            }
        }
    }
}

#[test]
fn broca_raw_headers_preserve_unknowns_and_confirm_only_raw_exhaustion() {
    let fixture = fixture("raw-headers");
    let ctx = fixture.ctx(true, NOW);
    result(fixture.call(
        &ctx,
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("adm-raw", "broca:raw", "broca"),
    ));
    let reading = |event: &str, utilization: &str| {
        json!({
            "event_id": event,
            "caller": { "runner": "broca" },
            "kind": "attempt",
            "admission_id": "adm-raw",
            "upstream_origin": "upstream",
            "observed_at_ms": NOW,
            "view_digest": "credential-read-view",
            "headers": {
                "anthropic-ratelimit-unified-5h-utilization": utilization,
                "anthropic-ratelimit-unified-5h-reset": ((NOW + 3_600_000) / 1000).to_string(),
                "anthropic-ratelimit-unified-fallback-percentage": "0.12345"
            }
        })
    };
    let below = reading("lineage:1", "0.995");
    let reply = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, below.clone()));
    assert_eq!(
        reply["observation"]["effects"][0]["effect"],
        "not_confirmed"
    );
    assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 0);
    let stored = fixture
        .store()
        .account_transaction(|tx| tx.observation_by_event(BROCA, "broca", "lineage:1"))
        .unwrap()
        .unwrap();
    let stored: Value = serde_json::from_str(&stored.observation_json).unwrap();
    assert_eq!(stored, below);
    assert_eq!(
        stored["headers"]["anthropic-ratelimit-unified-fallback-percentage"],
        "0.12345"
    );
    let full = reading("lineage:2", "1.0");
    let reply = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, full.clone()));
    assert_eq!(reply["observation"]["effects"][0]["effect"], "fence");
    assert_eq!(
        reply["observation"]["effects"][0]["fence"]["reset"]["at_ms"],
        json!(NOW + 3_600_000)
    );
    let repeat = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, full));
    assert_eq!(repeat["observation"]["duplicate"], true);
    assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 1);
}

/// A backup may restore the observation and decision logs empty. A runner
/// re-sending an event routing applied before the backup gets the first
/// report's answer back and the event is not applied again; a new event
/// is applied as usual.
#[test]
fn a_re_sent_event_is_not_applied_again_after_the_history_logs_are_restored_empty() {
    let mut fixture = fixture("restored-observations");
    let ctx = fixture.ctx(true, NOW);
    result(fixture.call(
        &ctx,
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("adm-restored", "broca:restored", "broca"),
    ));
    let exhausted = |event: &str| {
        json!({
            "event_id": event,
            "caller": { "runner": "broca" },
            "kind": "attempt",
            "admission_id": "adm-restored",
            "upstream_origin": "upstream",
            "observed_at_ms": NOW,
            "view_digest": "credential-read-view",
            "headers": {
                "anthropic-ratelimit-unified-5h-utilization": "1.0",
                "anthropic-ratelimit-unified-5h-reset": ((NOW + 3_600_000) / 1000).to_string()
            }
        })
    };
    let first = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("restored:1")));
    assert_eq!(first["observation"]["duplicate"], false);
    assert_eq!(first["observation"]["effects"][0]["effect"], "fence");
    assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 1);
    drop(ctx);

    fixture.store = None;
    {
        let connection = rusqlite::Connection::open(fixture.dir.join("store.db")).unwrap();
        connection
            .execute_batch("DELETE FROM account_observation; DELETE FROM account_decision;")
            .unwrap();
    }
    fixture.reopen();
    let ctx = fixture.ctx(true, NOW);
    assert_eq!(
        fixture
            .store()
            .account_serving_counts()
            .unwrap()
            .observations,
        0
    );

    let repeat = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("restored:1")));
    assert_eq!(repeat["observation"]["duplicate"], true);
    assert_eq!(repeat["observation"]["id"], first["observation"]["id"]);
    assert_eq!(
        repeat["observation"]["bound"],
        first["observation"]["bound"]
    );
    assert_eq!(
        repeat["observation"]["effects"],
        first["observation"]["effects"]
    );
    let counts = fixture.store().account_serving_counts().unwrap();
    assert_eq!((counts.observations, counts.fences), (0, 1));

    let next = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("restored:2")));
    assert_eq!(next["observation"]["duplicate"], false);
    assert_eq!(
        fixture
            .store()
            .account_serving_counts()
            .unwrap()
            .observations,
        1
    );
}

#[test]
fn broca_transport_failure_without_headers_records_no_quota_evidence() {
    let fixture = fixture("no-headers");
    let ctx = fixture.ctx(true, NOW);
    result(fixture.call(
        &ctx,
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("adm-transport", "broca:transport", "broca"),
    ));
    let params = json!({ "event_id": "lineage:3", "caller": { "runner": "broca" },
            "kind": "attempt", "admission_id": "adm-transport",
            "upstream_origin": "upstream", "terminal_reason": "transport_failure" });
    let reply = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, params.clone()));
    assert_eq!(reply["observation"]["effects"], json!([]));
    assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 0);
    let stored = fixture
        .store()
        .account_transaction(|tx| tx.observation_by_event(BROCA, "broca", "lineage:3"))
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&stored.observation_json).unwrap(),
        params
    );
}

#[test]
fn broca_run_finished_ends_lease_and_later_release_is_noop() {
    let fixture = fixture("finish-lease");
    let ctx = fixture.ctx(true, NOW);
    result(fixture.call(
        &ctx,
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("adm-finish", "broca:finish", "broca"),
    ));
    result(fixture.call(
        &ctx,
        ACCOUNT_OBSERVE_OP,
        BROCA,
        json!({
            "event_id": "lineage:4", "caller": { "runner": "broca" },
            "kind": "admission_started", "admission_id": "adm-finish", "run_id": "run-finish"
        }),
    ));
    let finish_time = NOW + 10_000;
    let ctx = fixture.ctx(true, finish_time);
    let finish = json!({ "event_id": "lineage:5", "caller": { "runner": "broca" },
            "kind": "run_finished", "admission_id": "adm-finish", "run_id": "run-finish",
            "terminal_reason": "success", "upstream_origin": "upstream", "headers": {
                "anthropic-ratelimit-unified-7d-utilization": "1.0"
            } });
    let reply = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, finish.clone()));
    assert_eq!(reply["observation"]["effects"][1]["lease_ended"], true);
    assert_eq!(reply["observation"]["effects"][2]["effect"], "fence");
    let lease_until = || {
        fixture
            .store()
            .account_transaction(|tx| {
                let row = tx.admission(BROCA, "adm-finish")?.unwrap();
                Ok(tx
                    .affinity(BROCA, &row.conversation_hash, &row.provider)?
                    .unwrap()
                    .lease_valid_until)
            })
            .unwrap()
    };
    assert_eq!(lease_until(), Some(finish_time));
    let reply = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, finish.clone()));
    assert_eq!(reply["observation"]["duplicate"], true);
    // A different admission can renew the same affinity after run end.
    let renewed_until = NOW + 180_000;
    fixture
        .store()
        .account_transaction(|tx| {
            let row = tx.admission(BROCA, "adm-finish")?.unwrap();
            let mut affinity = tx
                .affinity(BROCA, &row.conversation_hash, &row.provider)?
                .unwrap();
            affinity.lease_valid_until = Some(renewed_until);
            let expected = (affinity.pin_id.clone(), affinity.pin_generation);
            assert!(tx.put_affinity(&affinity, Some((&expected.0, expected.1)))?);
            Ok(())
        })
        .unwrap();
    let later = fixture.ctx(true, finish_time + 1000);
    let mut new_finish = finish;
    new_finish["event_id"] = json!("lineage:6");
    new_finish.as_object_mut().unwrap().remove("headers");
    let reply = result(fixture.call(&later, ACCOUNT_OBSERVE_OP, BROCA, new_finish));
    assert_eq!(
        reply["observation"]["effects"],
        json!([{ "effect": "none", "state": "finished" }])
    );
    let reply = result(fixture.call(
        &later,
        ACCOUNT_RELEASE_OP,
        BROCA,
        json!({ "admission_id": "adm-finish" }),
    ));
    assert_eq!(reply["release"]["released"], false);
    assert_eq!(reply["release"]["lease_ended"], false);
    assert_eq!(lease_until(), Some(renewed_until));
    assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 1);
}

#[test]
fn rfc3339_formats_whole_seconds_in_utc() {
    assert_eq!(rfc3339(NOW), "2026-09-29T12:00:00Z");
    assert_eq!(
        rfc3339(NOW + 7 * 24 * 3_600_000 + 999),
        "2026-10-06T12:00:00Z"
    );
}

/// The runner lists only the account `first` admitted, so no sibling can
/// stand in while that account is blocked.
fn only_on(first: &Value, admission_id: &str, key: &str) -> Value {
    let mut params = admit_params(admission_id, key, "broca");
    params["candidates"] = json!({
        "view_digest": "runner-view",
        "accounts": [{
            "account_id": first["account"]["account_id"],
            "credential_id": first["account"]["credential_id"],
        }],
    });
    params
}

fn exhaust(
    fixture: &Fixture,
    event_id: &str,
    admission_id: &str,
    at: i64,
    resets_at_ms: Value,
    resets_at_source: Value,
) -> Value {
    // Quota recovery tests need a refusal consistent with its reading.
    // Contradictory headroom is exercised separately as a non-quota wall.
    if let Some(admission) = fixture
        .store()
        .account_read(|tx| tx.admission(BROCA, admission_id))
        .unwrap()
    {
        fn exhausted_windows(value: &mut Value) {
            match value {
                Value::Object(fields) => {
                    for (name, value) in fields {
                        if name == "usedPercent" {
                            *value = json!(100.0);
                        } else {
                            exhausted_windows(value);
                        }
                    }
                }
                Value::Array(values) => {
                    for value in values {
                        exhausted_windows(value);
                    }
                }
                _ => {}
            }
        }
        for reading in fixture
            .accounts
            .lock()
            .unwrap()
            .readings
            .readings
            .values_mut()
            .filter(|reading| reading.account_id == admission.account_id)
        {
            exhausted_windows(&mut reading.entry);
        }
    }
    result(fixture.call(
        &fixture.ctx(true, at),
        ACCOUNT_OBSERVE_OP,
        BROCA,
        json!({
            "event_id": event_id, "caller": {"runner": "broca"},
            "kind": "account_limit_exhausted", "admission_id": admission_id,
            "resets_at_ms": resets_at_ms, "resets_at_source": resets_at_source,
            "observed_at_ms": at,
        }),
    ))
}

#[test]
fn a_whole_account_reset_schedules_one_probe_without_expiring_evidence() {
    let fixture = fixture("trial-run");
    let first = result(fixture.call(
        &fixture.ctx(true, NOW),
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("trial-1", "broca:trial-1", "broca"),
    ));
    // A ChatGPT-shaped exhaustion: no window, a header reset in 10 min.
    let reset = NOW + 10 * 60_000;
    let observed = exhaust(
        &fixture,
        "trial-e1",
        "trial-1",
        NOW + 1000,
        json!(reset),
        json!("header"),
    );
    let fence = &observed["observation"]["effects"][0]["fence"];
    for reading in fixture
        .accounts
        .lock()
        .unwrap()
        .readings
        .readings
        .values_mut()
    {
        reading.entry["fetchedAt"] = json!(rfc3339(NOW - 3_600_000));
    }
    assert_eq!(
        fence["pool"],
        Value::Null,
        "names no pool: the whole account"
    );
    let at = |now: i64, id: &str| {
        fixture.call(
            &fixture.ctx(true, now),
            ACCOUNT_FOR_OP,
            BROCA,
            only_on(&first, id, &format!("broca:{id}")),
        )
    };
    assert_eq!(
        at(NOW + 1001, "trial-before-reset").code(),
        Some("account_hold")
    );
    assert!(
        at(reset - 1, "trial-2").code().is_some(),
        "blocked until the extra reset check"
    );
    let trial = result(at(reset, "trial-3"));
    assert!(
        at(reset, "concurrent-trial").code().is_some(),
        "the reset admits only one probe"
    );
    assert!(
        !fixture
            .store
            .as_ref()
            .unwrap()
            .account_read(|tx| tx.active_fences("anthropic"))
            .unwrap()
            .is_empty(),
        "a probe does not delete quota evidence"
    );
    assert_eq!(
        trial["account"]["account_id"],
        first["account"]["account_id"]
    );
    // The trial run's exhaustion blocks the account again.
    exhaust(
        &fixture,
        "trial-e2",
        "trial-3",
        reset + 2000,
        Value::Null,
        Value::Null,
    );
    assert!(at(reset + 3000, "trial-4").code().is_some());
}

#[test]
fn quota_without_readings_rechecks_at_fifteen_minutes_even_with_a_later_hint() {
    let confirmed = NOW + 1000;
    for (label, resets_at, source, retry_at) in [
        (
            "missing-reset",
            Value::Null,
            Value::Null,
            confirmed + 15 * 60_000,
        ),
        (
            "hint",
            json!(confirmed + 30 * 60_000),
            json!("retry_after"),
            confirmed + 15 * 60_000,
        ),
    ] {
        let fixture = fixture(label);
        let key = format!("conversation-{label}");
        let first = result(fixture.call(
            &fixture.ctx(true, NOW),
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params(&format!("{label}-1"), &key, "broca"),
        ));
        exhaust(
            &fixture,
            &format!("{label}-e"),
            &format!("{label}-1"),
            confirmed,
            resets_at,
            source,
        );
        for reading in fixture
            .accounts
            .lock()
            .unwrap()
            .readings
            .readings
            .values_mut()
        {
            reading.entry["fetchedAt"] = json!(rfc3339(NOW - 3_600_000));
        }
        let at = |now: i64, id: String| {
            fixture.call(
                &fixture.ctx(true, now),
                ACCOUNT_FOR_OP,
                BROCA,
                only_on(&first, &id, &key),
            )
        };
        assert!(
            at(retry_at - 1000, format!("{label}-2")).code().is_some(),
            "{label}"
        );
        let retried = result(at(retry_at, format!("{label}-3")));
        assert!(at(retry_at, format!("{label}-concurrent")).code().is_some());
        assert_eq!(
            retried["account"]["account_id"], first["account"]["account_id"],
            "{label}"
        );
    }
}

/// An Antigravity-shaped reading of one account, observed at `fetched_at`.
fn pooled_entry(account: &str, fetched_at: &str, windows: &[(&str, f64, i64)]) -> Value {
    let windows: Vec<Value> = windows
            .iter()
            .map(|(id, used, minutes)| {
                json!({"id": id, "window": {"usedPercent": used, "windowMinutes": minutes}})
            })
            .collect();
    json!({
        "provider": "antigravity", "apiProvider": "google", "account": account,
        "source": "oauth", "fetchedAt": fetched_at,
        "usage": {"extraRateWindows": windows}
    })
}

/// A refusal that names no pool on a pooled (Antigravity) account blocks
/// the pool the refused model draws on. A spent pool the model never draws
/// on neither holds nor lifts that block; a fresh reading of its own pool
/// with headroom lifts it.
#[test]
fn a_no_pool_refusal_on_a_pooled_account_blocks_and_lifts_by_the_refused_models_pool() {
    let both = [("gemini-5h", 10.0, 300), ("3p-weekly", 20.0, 10080)];
    let rig = provider_fixture(
        "recovery-set",
        "antigravity",
        vec![pooled_entry(ACCOUNT_A, "2026-09-29T11:59:00Z", &both)],
    );
    let mut params = admit_params("pool-1", "pool-conversation", "broca");
    params["provider"] = json!("google");
    params["model"] = json!("antigravity-gemini-3.8-flash");
    params["affinity_model"] = params["model"].clone();
    let first = result(rig.call(&rig.ctx(true, NOW), ACCOUNT_FOR_OP, BROCA, params.clone()));
    assert_eq!(first["account"]["account_id"], ACCOUNT_A);
    let observed = exhaust(
        &rig,
        "pool-e",
        "pool-1",
        NOW + 1000,
        Value::Null,
        Value::Null,
    );
    let fence = &observed["observation"]["effects"][0]["fence"];
    assert_eq!(
        fence["pool"],
        json!({"window_minutes": null, "scope": {"kind": "every_model"}, "name": "gemini"}),
        "the refused model's pool: {fence}"
    );
    assert!(
        fence["recovery_set"].as_array().is_none_or(Vec::is_empty),
        "{fence}"
    );
    let read = |rig: &Fixture, fetched_at: &str, windows: &[(&str, f64, i64)]| {
        let body = json!({"result": [pooled_entry(ACCOUNT_A, fetched_at, windows)]});
        account_quota::apply_usage_reply(
            &mut rig.accounts.lock().unwrap(),
            &serde_json::to_vec(&body).unwrap(),
            NOW,
        )
        .unwrap();
    };
    let admit = |rig: &Fixture, now: i64, id: &str| {
        let mut retry = params.clone();
        retry["admission_id"] = json!(id);
        retry["candidates"] = json!({
            "view_digest": "runner-view",
            "accounts": [{
                "account_id": first["account"]["account_id"],
                "credential_id": first["account"]["credential_id"],
            }],
        });
        rig.call(&rig.ctx(true, now), ACCOUNT_FOR_OP, BROCA, retry)
    };
    // The refused model's own pool is still spent.
    read(
        &rig,
        "2026-09-29T12:02:00Z",
        &[("gemini-5h", 100.0, 300), ("3p-weekly", 20.0, 10080)],
    );
    assert!(admit(&rig, NOW + 3 * 60_000, "pool-2").code().is_some());
    // Its own pool has headroom while the other pool is spent: the block
    // lifts.
    read(
        &rig,
        "2026-09-29T12:04:00Z",
        &[("gemini-5h", 10.0, 300), ("3p-weekly", 100.0, 10080)],
    );
    let lifted = result(admit(&rig, NOW + 5 * 60_000, "pool-3"));
    assert_eq!(lifted["account"]["account_id"], ACCOUNT_A);
}

#[test]
fn a_spent_pool_reading_out_of_the_freshness_band_does_not_suppress_its_probe() {
    let rig = provider_fixture(
        "partial-reading-probe",
        "antigravity",
        vec![pooled_entry(
            ACCOUNT_A,
            "2026-09-29T11:59:00Z",
            &[("gemini-5h", 10.0, 300), ("3p-weekly", 20.0, 10080)],
        )],
    );
    let mut params = admit_params("partial-first", "partial-conversation", "broca");
    params["provider"] = json!("google");
    params["model"] = json!("antigravity-gemini-3.8-flash");
    params["affinity_model"] = params["model"].clone();
    let first = result(rig.call(&rig.ctx(true, NOW), ACCOUNT_FOR_OP, BROCA, params.clone()));
    exhaust(
        &rig,
        "partial-e",
        "partial-first",
        NOW + 1_000,
        Value::Null,
        Value::Null,
    );
    // A newer reading shows the block's own pool spent. Once that reading
    // ages out of the freshness band it no longer holds the due probe:
    // a hold never waits on readings that stopped arriving.
    let partial = pooled_entry(
        ACCOUNT_A,
        "2026-09-29T12:15:00Z",
        &[("gemini-5h", 100.0, 300), ("3p-weekly", 20.0, 10080)],
    );
    account_quota::apply_usage_reply(
        &mut rig.accounts.lock().unwrap(),
        &serde_json::to_vec(&json!({"result":[partial]})).unwrap(),
        NOW + 15 * 60_000,
    )
    .unwrap();
    params["admission_id"] = json!("partial-probe");
    params["candidates"] = json!({"view_digest":"runner-view","accounts":[{
            "account_id":first["account"]["account_id"],"credential_id":first["account"]["credential_id"]}]});
    let probe = rig.call(
        &rig.ctx(true, NOW + 36 * 60_000),
        ACCOUNT_FOR_OP,
        BROCA,
        params.clone(),
    );
    assert!(
        probe.code().is_none(),
        "a spent reading out of the freshness band must not disable the due probe: {probe:?}"
    );
    params["admission_id"] = json!("partial-second");
    assert!(rig
        .call(
            &rig.ctx(true, NOW + 36 * 60_000),
            ACCOUNT_FOR_OP,
            BROCA,
            params
        )
        .code()
        .is_some());
}

#[test]
fn a_malformed_pool_name_is_refused_naming_the_member_and_an_unknown_one_is_accepted() {
    let fixture = fixture("pool-names");
    let ctx = fixture.ctx(true, NOW);
    let mut unknown = admit_params("pool-name-1", "broca:pool-name-1", "broca");
    unknown["pool"] = json!("never-seen.pool_1");
    result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, unknown));
    for bad in [
        String::new(),
        "a".repeat(65),
        "Upper".into(),
        "has space".into(),
    ] {
        let mut params = admit_params("pool-name-2", "broca:pool-name-2", "broca");
        params["pool"] = json!(bad);
        let OpReply::Error { code, detail, .. } = fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params)
        else {
            panic!("{bad:?} accepted");
        };
        assert_eq!(code, "invalid_request");
        assert_eq!(detail.unwrap()["field"], "pool");
        let OpReply::Error { code, detail, .. } = fixture.call(
            &ctx,
            ACCOUNT_OBSERVE_OP,
            BROCA,
            json!({"event_id": "bad-pool", "caller": {"runner": "broca"},
                       "kind": "account_limit_exhausted", "pool": bad}),
        ) else {
            panic!("{bad:?} accepted on observe");
        };
        assert_eq!(code, "invalid_request");
        assert_eq!(detail.unwrap()["field"], "pool");
    }
}

/// A fence stored before blocks were keyed by pool keeps its effect: the
/// stored row's scope decides its pool key, and its evidence still decodes.
#[test]
fn a_fence_stored_before_pool_keys_blocks_by_its_migrated_key() {
    // Every fixture lists the same accounts; this one only names one.
    let seed = fixture("legacy-fence");
    let first = result(seed.call(
        &seed.ctx(true, NOW),
        ACCOUNT_FOR_OP,
        BROCA,
        admit_params("legacy-1", "broca:legacy-1", "broca"),
    ));
    let account = first["account"]["account_id"].as_str().unwrap().to_string();
    for (id, scope) in [
        ("legacy-family", json!({"kind": "family", "value": "opus"})),
        (
            "legacy-unknown",
            json!({"kind": "unknown_scope", "value": "opus"}),
        ),
    ] {
        let rig = fixture(id);
        rig.store()
            .account_transaction(|tx| {
                tx.insert_fence(&AccountFenceRow {
                    fence_id: id.into(),
                    provider: "anthropic".into(),
                    account_id: account.clone(),
                    scope_json: scope.to_string(),
                    fence_json: json!({
                        "account_id": account, "scope": scope,
                        "observed_at_ms": NOW, "reset": {"kind": "missing"},
                        "retry_hint_ms": null, "source": "broca_account_limit",
                    })
                    .to_string(),
                    source: "broca_account_limit".into(),
                    observed_at: NOW,
                    principal: BROCA.into(),
                    admission_id: None,
                    observation_id: None,
                    created_at: NOW,
                    cleared_at: None,
                    cleared_reason: None,
                })
            })
            .unwrap();
        let ctx = rig.ctx(true, NOW + 1000);
        let opus = rig.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            only_on(&first, "opus", "broca:opus"),
        );
        assert!(opus.code().is_some(), "{id}: blocks its family");
        let mut sonnet = only_on(&first, "sonnet", "broca:sonnet");
        sonnet["model"] = json!("claude-sonnet-5");
        sonnet["affinity_model"] = json!("claude-sonnet-5");
        let sonnet = result(rig.call(&ctx, ACCOUNT_FOR_OP, BROCA, sonnet));
        assert_eq!(sonnet["account"]["account_id"], account.as_str(), "{id}");
    }
}

/// Demand is read per roster account from the commit time the allocator still
/// charges: the quota reading time minus the delay allowed for admissions to
/// appear in that reading. Read all history when the account has no reading,
/// may use paid credits, or the strategy counts admissions of any age. Accounts
/// outside the roster are not read.
#[test]
fn demand_reads_each_roster_account_only_from_the_admissions_that_can_count() {
    use striatum_core::accounts::AllocationContext;
    let fixture = fixture("demand-floor");
    let store = fixture.store();
    let sticky = AllocatorPolicy::default();
    let observed = NOW - 30_000;
    let floor = observed - sticky.reflection_lag_ms;
    let admissions = [
        ("reflected", ACCOUNT_A, floor - 1),
        ("at-floor", ACCOUNT_A, floor),
        ("pending", ACCOUNT_A, floor + 1),
        ("unread", ACCOUNT_B, floor - 3_600_000),
        ("outside-roster", INSULA_ONLY, NOW),
    ];
    store
        .account_transaction(|tx| {
            tx.insert_pin(&AccountPinRow {
                pin_id: "pin-1".into(),
                principal: BROCA.into(),
                conversation_hash: "conversation".into(),
                provider: "anthropic".into(),
                pin_generation: 1,
                affinity_model: "claude-opus-5-5".into(),
                account_id: ACCOUNT_A.into(),
                credential_id: "oauth:anthropic".into(),
                pin_mode: "run_frozen".into(),
                previous_pin_id: None,
                decision_id: "acd-1".into(),
                created_at: floor - 3_600_000,
            })?;
            for (admission_id, account_id, committed_at) in admissions {
                tx.insert_admission(&AccountAdmissionRow {
                    principal: BROCA.into(),
                    admission_id: admission_id.into(),
                    request_fingerprint: "fp".into(),
                    request_mode: "admit".into(),
                    decision_id: "acd-1".into(),
                    conversation_hash: "conversation".into(),
                    provider: "anthropic".into(),
                    model: "claude-opus-5-5".into(),
                    account_id: account_id.into(),
                    credential_id: "oauth:anthropic".into(),
                    pin_id: "pin-1".into(),
                    pin_generation: 1,
                    state: "finished".into(),
                    charged: true,
                    request_bytes: 1_000,
                    request_bytes_estimated: false,
                    committed_at,
                    admission_valid_until: committed_at + 60_000,
                    lease_valid_until: committed_at + 120_000,
                    reply_json: "{}".into(),
                    run_id: None,
                    started_at: None,
                    started_late: false,
                    released_at: None,
                    finished_at: Some(committed_at + 1),
                    last_observed_at: None,
                })?;
            }
            Ok(())
        })
        .unwrap();
    let view = |account_id: &str, observed_at_ms: Option<i64>| AccountView {
        account_id: account_id.into(),
        routing_provider: "anthropic".into(),
        refresh_adapter: "anthropic".into(),
        credentials: Vec::new(),
        model_reach: ModelReach::All,
        quota: observed_at_ms.map(|observed_at_ms| QuotaReading {
            observed_at_ms: Some(observed_at_ms),
            error_class: None,
            stale: None,
            five_hour: None,
            seven_day: None,
            general_windows: None,
            scoped: ScopedList::Missing,
            pool_windows: None,
        }),
    };
    let roster = Roster {
        view_digest: "vault-view-1".into(),
        fetched_at_ms: NOW,
        accounts: vec![view(ACCOUNT_A, Some(observed)), view(ACCOUNT_B, None)],
    };
    let loaded = |policy: &AllocatorPolicy, context: &AllocationContext| {
        store
            .account_transaction(|tx| load_demand(tx, "anthropic", Some(&roster), policy, context))
            .unwrap()
            .into_iter()
            .map(|demand| (demand.account_id, demand.committed_at_ms))
            .collect::<Vec<_>>()
    };
    let row = |account_id: &str, committed_at: i64| (account_id.to_string(), committed_at);
    let credits = |spendable: bool| AllocationContext {
        credits: BTreeMap::from([(
            ACCOUNT_A.to_string(),
            striatum_core::accounts::CreditReading {
                observed_at_ms: NOW,
                spendable,
                prepaid: false,
            },
        )]),
        ..AllocationContext::default()
    };

    // A reading bounds its account; an account without one is read whole.
    // A credit reading the allocator cannot spend changes nothing.
    let bounded = vec![row(ACCOUNT_A, floor + 1), row(ACCOUNT_B, floor - 3_600_000)];
    assert_eq!(loaded(&sticky, &AllocationContext::default()), bounded);
    assert_eq!(loaded(&sticky, &credits(false)), bounded);
    let no_credits = AllocatorPolicy {
        paid_credits: false,
        ..AllocatorPolicy::default()
    };
    assert_eq!(loaded(&no_credits, &credits(true)), bounded);
    // An account that may go on paid credits is scored from the earlier of
    // its quota and credit readings: a newer credit reading changes nothing,
    // an older one lowers the floor to the credit reading's.
    assert_eq!(loaded(&sticky, &credits(true)), bounded);
    let older_credit = AllocationContext {
        credits: BTreeMap::from([(
            ACCOUNT_A.to_string(),
            striatum_core::accounts::CreditReading {
                observed_at_ms: observed - 1,
                spendable: true,
                prepaid: false,
            },
        )]),
        ..AllocationContext::default()
    };
    assert_eq!(
        loaded(&sticky, &older_credit),
        vec![
            row(ACCOUNT_A, floor),
            row(ACCOUNT_A, floor + 1),
            row(ACCOUNT_B, floor - 3_600_000),
        ]
    );
    // Counting strategies charge admissions of any age.
    let every_roster_admission = vec![
        row(ACCOUNT_A, floor - 1),
        row(ACCOUNT_A, floor),
        row(ACCOUNT_A, floor + 1),
        row(ACCOUNT_B, floor - 3_600_000),
    ];
    for strategy in [AccountStrategy::RoundRobin, AccountStrategy::Hybrid] {
        let policy = AllocatorPolicy {
            strategy,
            ..AllocatorPolicy::default()
        };
        assert_eq!(
            loaded(&policy, &AllocationContext::default()),
            every_roster_admission,
            "{strategy:?}"
        );
    }
}
