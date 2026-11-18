//! Quota refresh and health tests exercise the authority of fresh and retained quota evidence together.

use super::*;

#[test]
fn quota_refresh_logs_only_unrecognized_funding_once_per_distinct_set() {
    let known: Vec<ProviderUsage> = serde_json::from_value(json!([{
            "provider": "deepseek",
            "spend": [
                { "id": "purchased", "label": "Purchased credits", "funding": "purchased", "basis": "reported" },
                { "id": "unknown", "label": "Prepaid credits", "funding": "unknown", "basis": "reported" }
            ]
        }]))
        .unwrap();
    let first_unrecognized_refresh: Vec<ProviderUsage> = serde_json::from_value(json!([{
            "provider": "provider-a",
            "spend": [{ "id": "future", "label": "Future", "funding": "issuer_credit_v2", "basis": "reported" }]
        }]))
        .unwrap();
    let repeated_unrecognized_refresh: Vec<ProviderUsage> = serde_json::from_value(json!([
            {
                "provider": "provider-a",
                "spend": [{ "id": "future", "label": "Future", "funding": "issuer_credit_v2", "basis": "reported" }]
            },
            {
                "provider": "provider-b",
                "spend": [{ "id": "future", "label": "Future", "funding": "issuer_credit_v2", "basis": "reported" }]
            }
        ]))
        .unwrap();
    let different_unrecognized_refresh: Vec<ProviderUsage> = serde_json::from_value(json!([{
            "provider": "provider-a",
            "spend": [{ "id": "future", "label": "Future", "funding": "issuer_credit_v3", "basis": "reported" }]
        }]))
        .unwrap();
    let mut reported = QuotaRefreshWarnings::default();
    let mut emitted = Vec::new();

    log_quota_refresh_warnings_with(&known, &mut reported, |warning| {
        emitted.push(warning.to_string())
    });
    assert!(emitted.is_empty(), "known wire values must not log");
    log_quota_refresh_warnings_with(&first_unrecognized_refresh, &mut reported, |warning| {
        emitted.push(warning.to_string())
    });
    assert_eq!(emitted, [
            "[striatum] quota refresh ignored unsupported spend funding values: {\"issuer_credit_v2\"}"
        ]);
    // A second refresh with the same value set, even on another provider,
    // produces no log line.
    log_quota_refresh_warnings_with(&repeated_unrecognized_refresh, &mut reported, |warning| {
        emitted.push(warning.to_string())
    });
    assert_eq!(emitted.len(), 1);
    log_quota_refresh_warnings_with(&different_unrecognized_refresh, &mut reported, |warning| {
        emitted.push(warning.to_string())
    });
    assert_eq!(emitted.len(), 2);
}

#[test]
fn walled_with_funds_warning_logs_once_until_the_shape_clears() {
    let walled_reading = |provider: &str, grant_minor| {
        usage(json!({
            "provider": provider,
            "spend": [
                {
                    "id": "grant",
                    "label": "Granted balance",
                    "funding": "granted",
                    "remaining": { "minor": grant_minor, "exponent": 2, "unit": "USD" },
                    "basis": "reported"
                },
                {
                    "id": "purchased",
                    "label": "Purchased credits",
                    "funding": "purchased",
                    "remaining": { "minor": 250, "exponent": 2, "unit": "USD" },
                    "basis": "reported"
                }
            ]
        }))
    };
    let deepseek_wall = walled_reading("deepseek", 0);
    let deepseek_clear = walled_reading("deepseek", 100);
    let openai_wall = walled_reading("openai", 0);
    let openai_clear = walled_reading("openai", 100);
    let both_walls = [deepseek_wall.clone(), openai_wall.clone()];
    let mut warnings = QuotaRefreshWarnings::default();
    let mut emitted = Vec::new();

    log_quota_refresh_warnings_with(&both_walls, &mut warnings, |warning| {
        emitted.push(warning.to_string())
    });
    assert_eq!(emitted.len(), 2);
    assert!(emitted
        .iter()
        .any(|line| line.contains("provider deepseek walled")));
    assert!(emitted
        .iter()
        .any(|line| line.contains("provider openai walled")));

    log_quota_refresh_warnings_with(&both_walls, &mut warnings, |warning| {
        emitted.push(warning.to_string())
    });
    assert_eq!(emitted.len(), 2, "a repeated wall shape must be silent");

    let deepseek_cleared = [deepseek_clear, openai_wall.clone()];
    log_quota_refresh_warnings_with(&deepseek_cleared, &mut warnings, |warning| {
        emitted.push(warning.to_string())
    });
    assert_eq!(
        emitted.len(),
        2,
        "clearing one provider's shape does not warn"
    );

    log_quota_refresh_warnings_with(&both_walls, &mut warnings, |warning| {
        emitted.push(warning.to_string())
    });
    assert_eq!(emitted.len(), 3, "the returned DeepSeek wall warns again");
    assert!(emitted[2].contains("provider deepseek walled"));

    let openai_cleared = [deepseek_wall, openai_clear];
    log_quota_refresh_warnings_with(&openai_cleared, &mut warnings, |warning| {
        emitted.push(warning.to_string())
    });
    assert_eq!(emitted.len(), 3);
    log_quota_refresh_warnings_with(&both_walls, &mut warnings, |warning| {
        emitted.push(warning.to_string())
    });
    assert_eq!(emitted.len(), 4, "the returned OpenAI wall warns again");
    assert!(emitted[3].contains("provider openai walled"));
}

/// The quota poll loop writes healthy usage entries to the reading log.
/// Exercise its writer directly: a healthy entry creates provider/window
/// rows, while an entry containing an error creates none.
#[test]
fn quota_poll_records_healthy_entries_into_the_reading_log() {
    let rig = Rig::new("reading-log");
    let store = rig.open_store();
    let healthy: ProviderUsage = serde_json::from_value(serde_json::json!({
        "provider": "codex",
        "apiProvider": "openai",
        "account": "team-a",
        "fetchedAt": "2030-01-01T00:00:00Z",
        "savedResets": { "availableCount": 2 },
        "usage": {
            "primary": { "usedPercent": 20.0, "rawUsedPercent": 25.0,
                         "resetsAt": "2030-01-01T05:00:00Z", "windowMinutes": 300 },
            "secondary": { "usedPercent": 40.0, "rawUsedPercent": 45.0,
                           "resetsAt": "2030-01-08T00:00:00Z", "windowMinutes": 10080 }
        }
    }))
    .unwrap();
    let degraded: ProviderUsage = serde_json::from_value(serde_json::json!({
        "provider": "claude",
        "apiProvider": "anthropic",
        "account": "seat",
        "error": "upstream failed",
        "spend": []
    }))
    .unwrap();

    record_quota_readings(&store, &[healthy, degraded], 1_893_456_000_000);

    let rows = store.get_quota_readings("openai/team-a").unwrap();
    assert_eq!(
        rows.len(),
        2,
        "both windows of the healthy entry are recorded"
    );
    assert_eq!(rows[0].observed_at, 1_893_456_000_000);
    assert_eq!(rows[0].raw_used_percent, Some(25.0));
    assert!(store
        .get_quota_readings("anthropic/seat")
        .unwrap()
        .is_empty());
}

#[test]
fn absent_provider_window_is_retained_until_a_new_window_arrives() {
    let mut cached = vec![
        usage(serde_json::json!({
            "provider": "codex",
            "usage": { "primary": { "usedPercent": 100.0, "resetsAt": "2030-01-01T00:00:00Z" } }
        })),
        usage(serde_json::json!({
            "provider": "grok",
            "usage": { "primary": { "usedPercent": 10.0, "resetsAt": "2030-01-01T00:00:00Z" } }
        })),
    ];
    let fresh = vec![usage(serde_json::json!({
        "provider": "grok",
        "usage": { "primary": { "usedPercent": 40.0, "resetsAt": "2031-01-01T00:00:00Z" } }
    }))];

    let mut retained_degraded_since_ms = HashMap::new();
    merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

    let codex = cached
        .iter()
        .find(|window| window.provider == "codex")
        .expect("missing provider should be retained");
    assert_eq!(
        codex
            .usage
            .as_ref()
            .and_then(|usage| usage.primary.as_ref())
            .map(|window| window.used_percent),
        Some(100.0)
    );
    let grok = cached
        .iter()
        .find(|window| window.provider == "grok")
        .expect("present provider should be replaced");
    assert_eq!(
        grok.usage
            .as_ref()
            .and_then(|usage| usage.primary.as_ref())
            .map(|window| window.used_percent),
        Some(40.0)
    );
}

#[test]
fn degraded_refresh_does_not_replace_a_healthy_cached_window() {
    let mut cached = vec![usage(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 97.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    let fresh = vec![usage(serde_json::json!({
        "provider": "codex",
        "error": "quota source timeout"
    }))];
    let mut retained_degraded_since_ms = HashMap::new();

    merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

    assert_eq!(cached.len(), 1);
    assert_eq!(cached[0].provider, "codex");
    assert!(
        cached[0].error.is_none(),
        "healthy cached entry is retained"
    );
    assert_eq!(
        cached[0]
            .usage
            .as_ref()
            .and_then(|usage| usage.primary.as_ref())
            .map(|window| window.used_percent),
        Some(97.0)
    );
    assert!(retained_degraded_since_ms.contains_key(&quota_cache_key(&cached[0])));
}

#[test]
fn retained_degraded_quota_window_expires_despite_recurring_degraded_and_unrelated_successful_refreshes(
) {
    let first_reading_at_ms = 1_000;
    let first_degraded_at_ms = first_reading_at_ms + 1;
    let repeated_degraded_at_ms = first_degraded_at_ms + QUOTA_STALE_AFTER_MS / 2;
    let unrelated_refresh_at_ms = first_degraded_at_ms + QUOTA_STALE_AFTER_MS + 1;
    let mut cache = QuotaCache::default();

    apply_quota_refresh(
        &mut cache,
        vec![usage(serde_json::json!({
            "provider": "codex",
            "usage": { "primary": { "usedPercent": 97.0, "resetsAt": "2030-01-01T00:00:00Z" } }
        }))],
        first_reading_at_ms,
    );
    apply_quota_refresh(
        &mut cache,
        vec![usage(serde_json::json!({
            "provider": "codex",
            "error": "quota source timeout"
        }))],
        first_degraded_at_ms,
    );
    apply_quota_refresh(
        &mut cache,
        vec![usage(serde_json::json!({
            "provider": "codex",
            "error": "quota source timeout"
        }))],
        repeated_degraded_at_ms,
    );

    let codex_key = quota_cache_key(&cache.windows[0]);
    assert_eq!(
        cache.windows[0].error, None,
        "the last healthy window is retained temporarily"
    );
    assert_eq!(
        cache.retained_degraded_since_ms.get(&codex_key),
        Some(&first_degraded_at_ms),
        "repeated failures must not restart the retention clock"
    );

    apply_quota_refresh(
        &mut cache,
        vec![usage(serde_json::json!({
            "provider": "grok",
            "usage": { "primary": { "usedPercent": 40.0, "resetsAt": "2031-01-01T00:00:00Z" } }
        }))],
        unrelated_refresh_at_ms,
    );

    assert!(
        cache
            .windows
            .iter()
            .all(|window| window.provider != "codex"),
        "another account's fresh reading must not keep codex's degraded window alive"
    );
    assert!(cache.retained_degraded_since_ms.is_empty());
    assert!(quota_health(&cache, unrelated_refresh_at_ms).0.is_none());

    apply_quota_refresh(
        &mut cache,
        vec![usage(serde_json::json!({
            "provider": "codex",
            "usage": { "primary": { "usedPercent": 12.0, "resetsAt": "2031-01-01T00:00:00Z" } }
        }))],
        unrelated_refresh_at_ms + 1,
    );

    let recovered = cache
        .windows
        .iter()
        .find(|window| window.provider == "codex")
        .expect("a fresh healthy reading restores the account");
    assert!(recovered.error.is_none());
    assert_eq!(
        recovered
            .usage
            .as_ref()
            .and_then(|usage| usage.primary.as_ref())
            .map(|window| window.used_percent),
        Some(12.0)
    );
    assert!(cache.retained_degraded_since_ms.is_empty());
    assert!(quota_health(&cache, unrelated_refresh_at_ms + 1)
        .0
        .is_none());
}

#[test]
fn credential_unusable_class_drops_retention_like_absent() {
    // A revoked OAuth grant (Insula maps Google's `invalid_grant`, which
    // RFC 6749 makes permanent, to this class) never self-recovers;
    // retaining the last healthy window would keep routing to a dead
    // account. Removing the class from the dead-credential set must make
    // this test fail.
    let mut cached = vec![usage(serde_json::json!({
        "provider": "gemini",
        "usage": { "primary": { "usedPercent": 12.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    let fresh = vec![usage(serde_json::json!({
        "provider": "gemini",
        "error": "invalid_grant: token revoked",
        "errorClass": "credential_unusable"
    }))];
    let mut retained_degraded_since_ms = HashMap::new();

    merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

    assert_eq!(cached.len(), 1);
    assert!(
        cached[0].error.is_some(),
        "degraded credential_unusable entry replaces the stale healthy window"
    );
    assert_eq!(
        cached[0].error_class.as_deref(),
        Some("credential_unusable")
    );
    assert!(
        retained_degraded_since_ms.is_empty(),
        "no retention record for a revoked account"
    );
}

#[test]
fn no_quota_reported_class_drops_retention_and_takes_the_degraded_entry() {
    // no_quota_reported is an ended plan (the gateway affirms success but
    // reports no plan block), so like credential_absent below it is a true
    // state, not trouble: retaining a last-healthy window for it
    // manufactures a provider that does not exist on this host. Classless
    // degraded entries (from older Insula builds) and every other class
    // keep the retention path above.
    let mut cached = vec![usage(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 97.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    let fresh = vec![usage(serde_json::json!({
        "provider": "codex",
        "error": "qwen-cloud: the account has no token plan (gateway affirmed success and reported no plan block)",
        "errorClass": "no_quota_reported"
    }))];
    let mut retained_degraded_since_ms = HashMap::new();

    merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

    assert_eq!(cached.len(), 1);
    assert!(
        cached[0].error.is_some(),
        "degraded no_quota_reported entry replaces the stale healthy window"
    );
    assert_eq!(cached[0].error_class.as_deref(), Some("no_quota_reported"));
    assert!(
        retained_degraded_since_ms.is_empty(),
        "no retention record for a provider that does not exist here"
    );
}

#[test]
fn credential_absent_class_drops_retention_and_takes_the_degraded_entry() {
    // credential_absent is steady state (nobody configured the credential),
    // not trouble: retaining a last-healthy window for it manufactures a
    // provider that does not exist on this host. Classless degraded entries
    // (from older Insula builds) and every other class keep the retention
    // path above.
    let mut cached = vec![usage(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 97.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    let fresh = vec![usage(serde_json::json!({
        "provider": "codex",
        "error": "no credential configured",
        "errorClass": "credential_absent"
    }))];
    let mut retained_degraded_since_ms = HashMap::new();

    merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

    assert_eq!(cached.len(), 1);
    assert!(
        cached[0].error.is_some(),
        "degraded credential_absent entry replaces the stale healthy window"
    );
    assert_eq!(cached[0].error_class.as_deref(), Some("credential_absent"));
    assert!(
        retained_degraded_since_ms.is_empty(),
        "no retention record for a provider that does not exist here"
    );
}

#[test]
fn unknown_error_class_still_retains_like_any_degraded_entry() {
    // The class list grows producer-side (String by contract, not enum): an
    // unrecognized class must behave exactly like a classless degraded entry,
    // never disappear and never join a known bucket.
    let mut cached = vec![usage(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 55.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    let fresh = vec![usage(serde_json::json!({
        "provider": "codex",
        "error": "novel failure",
        "errorClass": "some_future_class"
    }))];
    let mut retained_degraded_since_ms = HashMap::new();

    merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

    assert_eq!(cached.len(), 1);
    assert!(
        cached[0].error.is_none(),
        "unknown class keeps the last-healthy retention path"
    );
}

#[test]
fn empty_usage_refresh_does_not_replace_a_healthy_cached_window() {
    let mut cached = vec![usage(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 88.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }))];
    let fresh = vec![usage(serde_json::json!({
        "provider": "codex",
        "usage": {}
    }))];
    let mut retained_degraded_since_ms = HashMap::new();

    merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

    assert_eq!(
        cached[0]
            .usage
            .as_ref()
            .and_then(|usage| usage.primary.as_ref())
            .map(|window| window.used_percent),
        Some(88.0)
    );
    assert!(retained_degraded_since_ms.contains_key(&quota_cache_key(&cached[0])));
}

#[test]
fn pool_only_refresh_is_retained_as_a_live_quota_signal() {
    let mut cache = QuotaCache::default();
    let pool_only = usage(serde_json::json!({
        "provider": "balance-wire",
        "apiProvider": "balance-provider",
        "usage": {},
        "spend": [{
            "id": "subscription",
            "label": "Subscription balance",
            "funding": "subscription",
            "remaining": { "minor": 20, "exponent": 2, "unit": "USD" },
            "total": { "minor": 100, "exponent": 2, "unit": "USD" },
            "basis": "reported",
            "spendable": true
        }]
    }));

    apply_quota_refresh(&mut cache, vec![pool_only], 10);

    assert_eq!(cache.windows.len(), 1);
    assert_eq!(
        cache.windows[0].provider, "balance-wire",
        "pool-only entries are retained as live quota observations"
    );
}

#[test]
fn failed_quota_refresh_retains_last_healthy_windows() {
    let mut cache = QuotaCache::default();
    let healthy = usage(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 24.0, "resetsAt": "2030-01-01T00:00:00Z" } }
    }));
    apply_quota_refresh(&mut cache, vec![healthy], 10);
    let retained = cache.windows.clone();

    record_quota_refresh_failure(&mut cache, "usage.get", "daemon restarted");

    assert_eq!(cache.windows, retained);
    assert!(!cache.available);
    assert_eq!(
        cache.last_error.as_deref(),
        Some("usage.get: daemon restarted")
    );
}

fn codex_window(resets_at_ms: u64) -> ProviderUsage {
    usage(json!({
        "provider": "codex",
        "apiProvider": "openai",
        "usage": { "primary": {
            "usedPercent": 0.0,
            "resetsAt": rfc3339_utc(resets_at_ms),
            "windowMinutes": 300
        }}
    }))
}

#[test]
fn first_poll_after_restart_replaces_the_restored_snapshot_wholesale() {
    let rig = Rig::new("quota-snapshot-first-poll");
    let store = rig.open_store();
    let observed_at_ms = now_ms();
    persist_quota_snapshot(
        &store,
        &[codex_window(observed_at_ms + 75 * 60_000)],
        observed_at_ms - 60_000,
    );
    let mut cache = restore_quota_cache(&store);
    assert_eq!(cache.windows.len(), 1);

    let claude = usage(json!({
        "provider": "claude",
        "usage": { "primary": { "usedPercent": 10.0 } }
    }));
    apply_quota_refresh(&mut cache, vec![claude.clone()], observed_at_ms);

    assert_eq!(
        cache.windows,
        vec![claude],
        "the restored codex window is not retained past the first fresh poll"
    );
    assert_eq!(cache.restored_from_ms, None);
    assert_eq!(cache.refreshed_at_ms, observed_at_ms);
}

/// A cache whose only reading is a codex window spent to 100% (walling
/// openai until its reset three hours out), last refreshed `age_ms` ago and
/// failing since, the way the refresh loop leaves it after a failed poll.
fn walled_codex_cache_aged(age_ms: u64) -> QuotaCache {
    let now = now_ms();
    let observed_at_ms = now - age_ms;
    let mut cache = QuotaCache::default();
    apply_quota_refresh(
        &mut cache,
        vec![usage(json!({
            "provider": "codex",
            "apiProvider": "openai",
            "usage": { "primary": {
                "usedPercent": 100.0,
                "resetsAt": rfc3339_utc(now + 3 * 60 * 60_000),
                "windowMinutes": 300
            }}
        }))],
        observed_at_ms,
    );
    record_quota_refresh_failure(&mut cache, "usage.get", "json: missing field `remaining`");
    cache
}

pub(super) async fn health_report_for(cache: QuotaCache) -> ModuleControlResponse {
    health_report_for_with_credentials(cache, &mapped_broca_credentials("anthropic")).await
}

/// Replays captured live data through the actual account report and control
/// health dispatcher, without deploying a module or changing a live store.
#[tokio::test]
#[ignore = "requires PREFRONTAL_ROUTING_USAGE_CAPTURE and PREFRONTAL_ROUTING_VAULT_ROSTER_CAPTURE"]
async fn captured_insula_feed_reports_unmapped_information_without_degrading_health() {
    let capture =
        std::env::var("PREFRONTAL_ROUTING_USAGE_CAPTURE").expect("required usage capture path");
    let roster_capture = std::env::var("PREFRONTAL_ROUTING_VAULT_ROSTER_CAPTURE")
        .expect("required roster capture path");
    let bytes = std::fs::read(capture).unwrap();
    let raw: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let mut state = account_quota::AccountQuotaState::default();
    let roster: account_quota::VaultRoster =
        serde_json::from_slice(&std::fs::read(roster_capture).unwrap()).unwrap();
    state.roster_poll = account_quota::RosterPoll::Listed {
        at_ms: roster.fetched_at_ms,
    };
    state.roster = Some(roster);
    account_quota::apply_usage_reply(&mut state, &bytes, now_ms() as i64).unwrap();
    let report = account_quota::build_report(
        &state.readings,
        state.roster.as_ref(),
        &state.roster_poll,
        now_ms() as i64,
    );
    let unmapped = report
        .accounts
        .iter()
        .filter(|account| account.routing_provider.is_none())
        .count();
    assert!(unmapped > 0, "capture must exercise absent vendor metadata");
    assert_eq!(report.information.len(), unmapped);
    assert!(report
        .alarms
        .iter()
        .all(|alarm| !alarm.contains("unmapped entry")));
    let entries = quota_consumer::decode_usage_result(&raw["result"]).unwrap();
    let mut cache = QuotaCache::default();
    apply_quota_refresh(&mut cache, entries, now_ms());
    let health = health_report_for(cache).await;
    let ModuleControlResponse::HealthCheck { status, detail, .. } = &health else {
        panic!("health reply");
    };
    assert_eq!(*status, HealthStatus::Ok);
    assert!(detail.is_none());
    let summary = json!({
        "health": health,
        "accountQuota": {
            "accounts": report.accounts.len(), "unmappedInformationCount": report.information.len(),
            "unmappedProviders": report.accounts.iter().filter(|account| account.routing_provider.is_none()).map(|account| account.provider.clone()).collect::<Vec<_>>(),
            "alarms": report.alarms,
            "vaultOnlyGapCount": report.gaps.vault_only.len(), "insulaOnlyGapCount": report.gaps.insula_only.len()
        }
    });
    println!("{}", serde_json::to_string_pretty(&summary).unwrap());
}

#[tokio::test]
async fn health_check_reports_degraded_naming_the_stale_quota_age() {
    let ModuleControlResponse::HealthCheck { status, detail, .. } =
        health_report_for(walled_codex_cache_aged(QUOTA_STALE_AFTER_MS - 60_000)).await
    else {
        panic!("health.check answers with a health report");
    };
    assert_eq!(status, HealthStatus::Ok, "inside the bound: {detail:?}");

    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = health_report_for(walled_codex_cache_aged(7 * 60 * 60_000 + 5 * 60_000)).await
    else {
        panic!("health.check answers with a health report");
    };
    assert_eq!(status, HealthStatus::Degraded);
    let detail = detail.expect("a degraded report says why");
    assert!(
        detail.contains("quota snapshot stale: last successful refresh 7h05m ago")
            && detail.contains("missing field `remaining`"),
        "{detail}"
    );
    assert_eq!(metrics.unwrap()["quota"]["stale"], true);

    // No reading yet (boot before the first poll) is not staleness.
    let ModuleControlResponse::HealthCheck { status, .. } =
        health_report_for(QuotaCache::default()).await
    else {
        panic!("health.check answers with a health report");
    };
    assert_eq!(status, HealthStatus::Ok);
}

#[tokio::test]
async fn health_check_reports_unmapped_credentials_without_degrading_and_joins_stale_quota_details()
{
    let broca_credentials = empty_broca_credentials();
    broca_credentials.lock().unwrap().apply_listing(
        &cortexkit_claustrum_client::ScopedCredentialListing {
            credentials: vec![
                cortexkit_claustrum_client::ListedCredential {
                    id: "fixture-mapped".into(),
                    categories: vec!["llm-provider".into()],
                    state: "active".into(),
                    provider_ids: vec!["anthropic".into()],
                    ..Default::default()
                },
                cortexkit_claustrum_client::ListedCredential {
                    id: "fixture-unmapped".into(),
                    categories: vec!["llm-provider".into()],
                    state: "active".into(),
                    ..Default::default()
                },
            ],
            view: "unmapped-view".into(),
            ..Default::default()
        },
        now_ms(),
    );
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = health_report_for_with_credentials(
        walled_codex_cache_aged(QUOTA_STALE_AFTER_MS + 60_000),
        &broca_credentials,
    )
    .await
    else {
        panic!("health.check answers with a health report");
    };
    assert_eq!(status, HealthStatus::Degraded);
    let detail = detail.unwrap();
    assert!(
        detail.starts_with("quota snapshot stale: last successful refresh 16m ago"),
        "{detail}"
    );
    assert!(detail.contains("missing field `remaining`"), "{detail}");
    let metrics = metrics.unwrap();
    assert_eq!(metrics["quota"]["stale"], true);
    assert_eq!(metrics["brocaReachability"]["unmappedCredentials"], 1);
    assert_eq!(
        metrics["brocaReachability"]["unmappedCredentialIds"],
        json!(["fixture-unmapped"])
    );

    // The unmapped credential remains visible without degrading Broca health.
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = health_report_for_with_credentials(QuotaCache::default(), &broca_credentials).await
    else {
        panic!("health.check answers with a health report");
    };
    assert_eq!(status, HealthStatus::Ok);
    assert_eq!(detail, None);
    assert_eq!(
        metrics.unwrap()["brocaReachability"]["unmappedCredentialIds"],
        json!(["fixture-unmapped"])
    );

    // The quota remains stale independently of the informational unmapped row.
    let ModuleControlResponse::HealthCheck { status, detail, .. } =
        health_report_for_with_credentials(
            walled_codex_cache_aged(QUOTA_STALE_AFTER_MS + 60_000),
            &mapped_broca_credentials("anthropic"),
        )
        .await
    else {
        panic!("health.check answers with a health report");
    };
    assert_eq!(status, HealthStatus::Degraded);
    assert!(detail.unwrap().starts_with("quota snapshot stale:"));

    let ModuleControlResponse::HealthCheck { status, detail, .. } =
        health_report_for(QuotaCache::default()).await
    else {
        panic!("health.check answers with a health report");
    };
    assert_eq!(status, HealthStatus::Ok);
    assert_eq!(detail, None);
}

/// Point a config file's mtime at a fixed second so a live-edit test does not
/// depend on the filesystem clock's resolution.

#[test]
fn healthy_refresh_replaces_a_degraded_cached_window_and_clears_retention() {
    let mut cached = vec![usage(serde_json::json!({
        "provider": "codex",
        "error": "previous timeout"
    }))];
    let fresh = vec![usage(serde_json::json!({
        "provider": "codex",
        "usage": { "primary": { "usedPercent": 12.0, "resetsAt": "2031-01-01T00:00:00Z" } }
    }))];
    let mut retained_degraded_since_ms = HashMap::from([(quota_cache_key(&cached[0]), 0)]);

    merge_quota_windows(&mut cached, fresh, &mut retained_degraded_since_ms, 0);

    assert_eq!(cached.len(), 1);
    assert!(cached[0].error.is_none());
    assert_eq!(
        cached[0]
            .usage
            .as_ref()
            .and_then(|usage| usage.primary.as_ref())
            .map(|window| window.used_percent),
        Some(12.0)
    );
    assert!(!retained_degraded_since_ms.contains_key(&quota_cache_key(&cached[0])));
}
