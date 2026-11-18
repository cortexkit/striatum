//! Provider quota snapshots and their persistence.
//! Refresh, stale-read rules, and health reporting use the same view of retained evidence.

use super::*;

/// Age past which the cached quota snapshot no longer steers selection. The
/// backstop runs every ten minutes, so a snapshot this old has missed a backstop
/// refresh. Quota windows move within that time:
/// saved resets (credits that reset effective utilization) can be spent,
/// five-hour windows roll over, and an exhausted quota reading from an hour ago
/// is no evidence of exhaustion now. Past this bound selection neither adjusts
/// ranking pressure nor blocks routes based on quota; each decision says quota
/// is unknown, and health reports degraded with the snapshot's age.
pub(super) const QUOTA_STALE_AFTER_MS: u64 = 15 * 60_000;

/// Latest quota snapshot, shared between the background refresh task (writer) and
/// `route.select` handlers (readers). The lock is NEVER held across an `.await`:
/// the refresh task fetches outside the lock then takes it briefly to merge; a
/// selection takes it only to clone the snapshot out.
#[derive(Clone, Default)]
pub(super) struct QuotaCache {
    pub(super) windows: Vec<ProviderUsage>,
    pub(super) refreshed_at_ms: u64,
    pub(super) available: bool,
    /// Last connect/fetch error (observability: surfaced via route.quota_status).
    pub(super) last_error: Option<String>,
    /// First degraded time for each provider/account whose healthy window is retained.
    pub(super) retained_degraded_since_ms: HashMap<String, u64>,
    /// Set when `windows` came from the snapshot persisted before the last
    /// restart rather than from a poll made by this process: the Unix-millisecond
    /// poll time at which that snapshot was accepted. Cleared by the first
    /// successful poll, which replaces the restored windows wholesale.
    pub(super) restored_from_ms: Option<u64>,
    pub(super) consumer: quota_events::ConsumerHealth,
    pub(super) snapshot_persistence: quota_events::SnapshotPersistence,
}

pub(super) type SharedQuotaCache = Arc<Mutex<QuotaCache>>;

/// The cached snapshot's age when it is past [`QUOTA_STALE_AFTER_MS`]; `None`
/// while it is fresh or when there has never been a reading to age.
pub(super) fn stale_quota_age_ms(refreshed_at_ms: u64, now_ms: u64) -> Option<u64> {
    if refreshed_at_ms == 0 {
        return None;
    }
    let age_ms = now_ms.saturating_sub(refreshed_at_ms);
    (age_ms > QUOTA_STALE_AFTER_MS).then_some(age_ms)
}

/// Human-readable age for health details and decision notes, e.g. "7h05m".
pub(super) fn render_age(age_ms: u64) -> String {
    let minutes = age_ms / 60_000;
    if minutes >= 60 {
        format!("{}h{:02}m", minutes / 60, minutes % 60)
    } else {
        format!("{minutes}m")
    }
}

/// The explanation line a decision carries when its quota snapshot is stale.
pub(super) fn stale_quota_note(age_ms: u64, last_error: Option<&str>) -> String {
    let mut note = format!(
        "quota signal: stale (last successful refresh {} ago, bound {}); quota pressure and walls ignored, every provider ranked as quota-unknown",
        render_age(age_ms),
        render_age(QUOTA_STALE_AFTER_MS)
    );
    if let Some(error) = last_error {
        note.push_str(&format!("; last refresh error: {error}"));
    }
    note
}

/// The routing module's health from its quota snapshot: degraded, naming the
/// age and the last refresh error, once the snapshot is stale.
pub(super) fn quota_health(cache: &QuotaCache, now_ms: u64) -> (Option<String>, serde_json::Value) {
    let age_ms = (cache.refreshed_at_ms > 0).then(|| now_ms.saturating_sub(cache.refreshed_at_ms));
    let stale_age = stale_quota_age_ms(cache.refreshed_at_ms, now_ms);
    let metrics = json!({
        "available": cache.available,
        "refreshedAtMs": cache.refreshed_at_ms,
        "ageMs": age_ms,
        "staleAfterMs": QUOTA_STALE_AFTER_MS,
        "stale": stale_age.is_some(),
        "lastError": cache.last_error,
        "consumer": cache.consumer,
    });
    let detail = stale_age.map(|age_ms| {
        let mut detail = format!(
            "quota snapshot stale: last successful refresh {} ago (bound {}); selection treats quota as unknown",
            render_age(age_ms),
            render_age(QUOTA_STALE_AFTER_MS)
        );
        if let Some(error) = &cache.last_error {
            detail.push_str(&format!("; last refresh error: {error}"));
        }
        detail
    });
    (detail, metrics)
}

/// The per-account quota state, set once when the store opens. It is a process
/// global rather than a handler argument because only the read-only
/// `route.account_quota_status` operation reads it; `None` until boot restores it.
pub(super) static ACCOUNT_QUOTA: std::sync::OnceLock<account_quota::SharedAccountQuota> =
    std::sync::OnceLock::new();

pub(super) fn quota_cache_key(window: &ProviderUsage) -> String {
    format!(
        "{}\0{}",
        window.provider,
        window.account.as_deref().unwrap_or("")
    )
}

pub(super) fn merge_quota_windows(
    existing: &mut Vec<ProviderUsage>,
    fresh: Vec<ProviderUsage>,
    retained_degraded_since_ms: &mut HashMap<String, u64>,
    now_ms: u64,
) {
    // Successful events for other accounts advance the cache-wide refresh time,
    // so expire retained windows against the first degraded reading for each key.
    let expired_keys: HashSet<String> = retained_degraded_since_ms
        .iter()
        .filter_map(|(key, first_degraded_at_ms)| {
            (now_ms.saturating_sub(*first_degraded_at_ms) > QUOTA_STALE_AFTER_MS)
                .then(|| key.clone())
        })
        .collect();
    if !expired_keys.is_empty() {
        existing.retain(|window| !expired_keys.contains(&quota_cache_key(window)));
        for key in expired_keys {
            retained_degraded_since_ms.remove(&key);
        }
    }

    let mut fresh_by_key: std::collections::HashMap<String, ProviderUsage> = fresh
        .into_iter()
        .map(|window| (quota_cache_key(&window), window))
        .collect();
    let mut merged = Vec::with_capacity(existing.len() + fresh_by_key.len());
    for window in existing.drain(..) {
        let key = quota_cache_key(&window);
        if let Some(replacement) = fresh_by_key.remove(&key) {
            // Three classes will not self-recover, so a retained "last healthy"
            // window is a fiction that can only mislead: credential_absent
            // (no credential is configured on this host), credential_unusable
            // (a revoked OAuth grant yields invalid_grant and needs a new
            // authorisation), and no_quota_reported (the gateway succeeds but
            // reports no plan, as when a subscription has ended). Retaining a
            // healthy window can route workers to a dead subscription. Drop it
            // for all three classes and take the degraded entry as the truth.
            // Every other class keeps
            // its last healthy window only until that key's degraded age reaches
            // the stale bound; refreshes for other keys cannot extend its lifetime.
            let dead_credential = replacement.error_class.as_deref().is_some_and(|class| {
                class == "credential_absent"
                    || class == "credential_unusable"
                    || class == "no_quota_reported"
            });
            if window.is_healthy() && !replacement.is_healthy() && !dead_credential {
                let first_degraded_at_ms = *retained_degraded_since_ms
                    .entry(key.clone())
                    .or_insert_with(|| {
                        let reason = replacement
                            .error
                            .as_deref()
                            .filter(|reason| !reason.is_empty())
                            .unwrap_or("no usable quota windows");
                        let provider = match replacement
                            .account
                            .as_deref()
                            .filter(|account| !account.is_empty())
                        {
                            Some(account) => format!("{} account {}", replacement.provider, account),
                            None => replacement.provider.clone(),
                        };
                        eprintln!(
                            "[striatum] retaining last healthy quota window for {provider} after degraded refresh: {reason}"
                        );
                        now_ms
                    });
                if now_ms.saturating_sub(first_degraded_at_ms) > QUOTA_STALE_AFTER_MS {
                    retained_degraded_since_ms.remove(&key);
                    merged.push(replacement);
                } else {
                    merged.push(window);
                }
            } else {
                retained_degraded_since_ms.remove(&key);
                merged.push(replacement);
            }
        } else {
            merged.push(window);
        }
    }
    for (key, window) in fresh_by_key {
        if window.is_healthy() {
            retained_degraded_since_ms.remove(&key);
        }
        merged.push(window);
    }
    *existing = merged;
}

#[derive(Default)]
pub(super) struct QuotaRefreshWarnings {
    reported_value_sets: BTreeSet<BTreeSet<String>>,
    providers_walled_with_funds: BTreeSet<String>,
}

impl QuotaRefreshWarnings {
    fn warnings_for(&mut self, fresh: &[ProviderUsage]) -> Vec<String> {
        let mut warnings = Vec::new();
        let unsupported: BTreeSet<String> = fresh
            .iter()
            .flat_map(|entry| entry.spend.iter())
            .filter(|pool| !pool.has_known_funding())
            .map(|pool| pool.funding.clone())
            .collect();
        if !unsupported.is_empty() && self.reported_value_sets.insert(unsupported.clone()) {
            warnings.push(format!(
                "[striatum] quota refresh ignored unsupported spend funding values: {unsupported:?}"
            ));
        }

        let mut wall_details_by_provider = BTreeMap::<String, BTreeSet<String>>::new();
        // A positive balance in non-spendable pools cannot prevent a wall when
        // every spend-authorized pool is empty. Name that policy when the
        // provider enters this state so the visible balance is not mistaken for
        // a quota bug; re-arm the warning only after the state clears.
        for entry in fresh {
            if entry.spend.is_empty() {
                continue;
            }
            let all_spendable_empty = entry
                .spend
                .iter()
                .filter(|pool| pool.has_supported_funding())
                .all(|pool| pool.is_known_empty());
            if !all_spendable_empty {
                continue;
            }
            let withheld: BTreeSet<String> = entry
                .spend
                .iter()
                .filter(|pool| !pool.has_supported_funding())
                .filter_map(|pool| {
                    pool.remaining
                        .as_ref()
                        .filter(|remaining| !remaining.is_non_positive())
                        .map(|remaining| format!("{} ({})", remaining.render(), pool.funding))
                })
                .collect();
            if !withheld.is_empty() {
                wall_details_by_provider
                    .entry(entry.provider.clone())
                    .or_default()
                    .extend(withheld);
            }
        }

        let currently_walled: BTreeSet<String> = wall_details_by_provider.keys().cloned().collect();
        for provider in currently_walled.difference(&self.providers_walled_with_funds) {
            let withheld = wall_details_by_provider
                .get(provider)
                .expect("active provider has a withheld-balance description");
            warnings.push(format!(
                "[striatum] provider {provider} walled on empty spendable pools while non-spendable pools hold {} - policy, not a quota bug (only granted/subscription funding may spend)",
                withheld.iter().cloned().collect::<Vec<_>>().join(", ")
            ));
        }
        self.providers_walled_with_funds = currently_walled;
        warnings
    }
}

pub(super) fn log_quota_refresh_warnings_with(
    fresh: &[ProviderUsage],
    reported: &mut QuotaRefreshWarnings,
    mut emit: impl FnMut(&str),
) {
    for warning in reported.warnings_for(fresh) {
        emit(&warning);
    }
}

fn log_quota_refresh_warnings(fresh: &[ProviderUsage]) {
    static REPORTED: OnceLock<Mutex<QuotaRefreshWarnings>> = OnceLock::new();
    let mut reported = REPORTED
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    log_quota_refresh_warnings_with(fresh, &mut reported, |warning| eprintln!("{warning}"));
}

#[cfg(test)]
pub(super) fn record_quota_readings(
    store: &SqliteRouteStateStore,
    fresh: &[ProviderUsage],
    observed_at_ms: u64,
) {
    if let Err(error) = write_quota_readings(store, fresh, observed_at_ms) {
        tracing::warn!(error, "quota reading log: sample not recorded");
    }
}

/// Append healthy entries to the consumption estimate's reading log, resolving
/// providers and per-model capacity keys the same way as the quota overlay.
/// Return write failures so the delivery adapter leaves the event unacknowledged.
fn write_quota_readings(
    store: &SqliteRouteStateStore,
    fresh: &[ProviderUsage],
    observed_at_ms: u64,
) -> Result<(), String> {
    let rows = quota_reading_rows(store, fresh, observed_at_ms)?;
    store
        .append_quota_readings(&rows)
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) fn quota_reading_rows(
    store: &SqliteRouteStateStore,
    fresh: &[ProviderUsage],
    observed_at_ms: u64,
) -> Result<Vec<striatum_core::route_state_store::QuotaReadingLogRow>, String> {
    let facts = store.get_facts().map_err(|e| e.to_string())?;
    let observed_at = i64::try_from(observed_at_ms).map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    for entry in fresh {
        let Some(join) = striatum_core::quota::quota_route_join(entry, &facts) else {
            continue;
        };
        let capacity_keys = striatum_core::quota::quota_capacity_keys(entry, &facts);
        rows.extend(striatum_core::quota_poll::reading_log_rows_for_poll(
            entry,
            join.provider_id,
            &capacity_keys,
            observed_at,
        ));
    }
    Ok(rows)
}

pub(super) fn apply_quota_refresh(
    cache: &mut QuotaCache,
    fresh: Vec<ProviderUsage>,
    observed_at_ms: u64,
) {
    // The quota producer can add funding labels over time. Unrecognized labels are
    // logged once per distinct set; a provider wall caused by empty spendable
    // balances and positive non-spendable balances is logged when it begins, then
    // again only if it clears and returns.
    log_quota_refresh_warnings(&fresh);
    // A restored snapshot only bridges the gap between boot and the first poll.
    // That poll starts from an empty cache, exactly as it did before snapshots
    // were persisted, so nothing restored outlives the first fresh reading.
    if cache.restored_from_ms.take().is_some() {
        cache.windows.clear();
        cache.retained_degraded_since_ms.clear();
    }
    merge_quota_windows(
        &mut cache.windows,
        fresh,
        &mut cache.retained_degraded_since_ms,
        observed_at_ms,
    );
    cache.refreshed_at_ms = observed_at_ms;
    cache.available = true;
    cache.last_error = None;
}

/// Test helper for writing a snapshot without a live delivery cursor.
#[cfg(test)]
pub(super) fn persist_quota_snapshot(
    store: &SqliteRouteStateStore,
    windows: &[ProviderUsage],
    observed_at_ms: u64,
) {
    if let Err(error) = write_quota_snapshot(store, windows, observed_at_ms) {
        tracing::warn!(error, "quota snapshot not persisted");
    }
}

pub(super) fn write_quota_snapshot(
    store: &SqliteRouteStateStore,
    windows: &[ProviderUsage],
    observed_at_ms: u64,
) -> Result<(), String> {
    let observed_at = i64::try_from(observed_at_ms).map_err(|e| e.to_string())?;
    serde_json::to_string(windows)
        .map_err(|error| error.to_string())
        .and_then(|json| {
            store
                .put_quota_snapshot(&json, observed_at)
                .map_err(|error| error.to_string())
        })
}

/// Build the boot-time quota cache from the snapshot the previous process
/// persisted. The restored windows are treated exactly like windows retained
/// through a live outage: the overlay recomputes pace against the current clock
/// and ignores any window whose reset has already passed, so no separate age
/// cutoff applies. `available` stays false because this process has not reached
/// insula yet. A missing or unreadable snapshot yields an empty cache.
pub(super) fn restore_quota_cache(store: &SqliteRouteStateStore) -> QuotaCache {
    let persisted = match store.quota_snapshot() {
        Ok(Some(persisted)) => persisted,
        Ok(None) => return QuotaCache::default(),
        Err(error) => {
            tracing::warn!(error = %error, "quota snapshot unreadable; starting with no reading");
            return QuotaCache::default();
        }
    };
    let windows: Vec<ProviderUsage> = match serde_json::from_str(&persisted.windows_json) {
        Ok(windows) => windows,
        Err(error) => {
            tracing::warn!(error = %error, "quota snapshot undecodable; starting with no reading");
            return QuotaCache::default();
        }
    };
    let observed_at_ms = u64::try_from(persisted.observed_at).unwrap_or(0);
    QuotaCache {
        windows,
        refreshed_at_ms: observed_at_ms,
        available: false,
        last_error: None,
        retained_degraded_since_ms: HashMap::new(),
        restored_from_ms: Some(observed_at_ms),
        consumer: Default::default(),
        snapshot_persistence: Default::default(),
    }
}

pub(super) fn record_quota_refresh_failure(
    cache: &mut QuotaCache,
    phase: &str,
    error: impl fmt::Display,
) {
    cache.available = false;
    let error = format!("{phase}: {error}");
    // The loop retries every few seconds, so print only when the reason
    // changes. This failure used to go to debug tracing only, which left a
    // refresh failing on every retry for hours with nothing in the stderr log.
    if cache.last_error.as_deref() != Some(error.as_str()) {
        eprintln!(
            "[striatum] quota refresh failed; selection keeps the snapshot from refreshedAtMs {} until a refresh succeeds or it goes stale: {error}",
            cache.refreshed_at_ms
        );
    }
    cache.last_error = Some(error);
}
