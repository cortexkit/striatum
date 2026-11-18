//! Stored account blocks and evidence-driven recovery.
//! Quota refresh can reconcile these records without admitting a new request.

use super::*;

/// The confirming evidence stored with a block. Its pool key and recovery
/// set are stored in their own columns, so blocks confirmed before blocks
/// were keyed by pool decode the same way.
#[derive(Deserialize)]
struct StoredBlockEvidence {
    pub(super) account_id: String,
    pub(super) observed_at_ms: EpochMs,
    pub(super) reset: ResetTime,
    #[serde(default)]
    pub(super) retry_hint_ms: Option<EpochMs>,
    pub(super) source: ConfirmationSource,
}

/// Every standing block on `provider`, with its stored row id.
pub(super) fn load_blocks(
    tx: &(impl BlockReads + ?Sized),
    provider: &str,
) -> RouteStateResult<Vec<(String, Fence)>> {
    // The pool-key columns are read only when a block stands. A provider with
    // none reads just the base columns, which every schema version has: the
    // upgrade tests serve admissions on a store opened before the pool-key
    // migration.
    if tx.active_fences(provider)?.is_empty() {
        return Ok(Vec::new());
    }
    Ok(tx
        .active_pool_fences(provider)?
        .into_iter()
        .filter_map(|row| {
            match serde_json::from_str::<StoredBlockEvidence>(&row.evidence.fence_json) {
                Ok(evidence) => Some((
                    row.evidence.fence_id,
                    Fence {
                        account_id: evidence.account_id,
                        pool: row.pool_key,
                        recovery_set: row.recovery_set,
                        observed_at_ms: evidence.observed_at_ms.min(row.evidence.created_at),
                        reset: evidence.reset,
                        retry_hint_ms: evidence.retry_hint_ms,
                        source: evidence.source,
                    },
                )),
                Err(error) => {
                    tracing::warn!(fence = %row.evidence.fence_id, error = %error, "stored fence undecodable; ignored");
                    None
                }
            }
        })
        .collect())
}

pub(super) fn load_fences(
    tx: &(impl BlockReads + ?Sized),
    provider: &str,
) -> RouteStateResult<Vec<Fence>> {
    Ok(load_blocks(tx, provider)?
        .into_iter()
        .map(|(_, fence)| fence)
        .collect())
}

pub(super) fn quota_reading_binds_origin(
    origin: &striatum_store::FenceRecheck,
    account: &str,
    fences: &[Fence],
    reading: Option<&QuotaReading>,
    now: i64,
    policy: &AllocatorPolicy,
) -> bool {
    let relevant: Vec<_> = fences
        .iter()
        .filter(|fence| {
            fence.account_id == account
                && (origin.kind != "account" || fence.observed_at_ms == origin.evidence_at)
        })
        .collect();
    let whole_account = Fence {
        account_id: account.into(),
        pool: None,
        recovery_set: Vec::new(),
        observed_at_ms: origin.evidence_at,
        reset: ResetTime::Missing,
        retry_hint_ms: None,
        source: striatum_core::accounts::ConfirmationSource::BrocaAccountLimit,
    };
    let relevant = if relevant.is_empty() {
        vec![&whole_account]
    } else {
        relevant
    };
    relevant
        .iter()
        .any(|fence| striatum_core::accounts::pools::binding_pressure(fence, reading, now, policy))
        || relevant
            .iter()
            .all(|fence| block_cleared(fence, reading, now, policy).is_some())
}

/// Bring the standing blocks on `provider` up to date with the accounts'
/// latest readings: clear blocks proved recovered by newer headroom, and add
/// newly reported pools to each whole-account recovery set. A reset or retry
/// check never deletes quota evidence. Clearing a recovered block keeps a
/// later partial reading from bringing it back.
pub(super) fn refresh_blocks(
    tx: &AccountCommand<'_>,
    provider: &str,
    accounts: &[AccountView],
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> RouteStateResult<()> {
    for (fence_id, fence) in load_blocks(tx, provider)? {
        let reading = accounts
            .iter()
            .find(|view| view.account_id == fence.account_id)
            .and_then(|view| view.quota.as_ref());
        if let Some(end) = block_cleared(&fence, reading, now, policy) {
            tx.clear_fence(&fence_id, now, &wire_str(&end))?;
        } else if let Some(set) = grown_recovery_set(&fence, reading, now, policy) {
            tx.set_fence_recovery_set(&fence_id, &set)?;
        }
    }
    Ok(())
}

/// Refresh the standing blocks of every provider against the current
/// readings. The account loop calls this so that readings arriving between
/// admissions still lift blocks and grow recovery sets.
pub(crate) fn refresh_all_blocks(
    store: &SqliteRouteStateStore,
    state: &AccountQuotaState,
    config: &crate::config_overrides::LiveConfig,
    now: EpochMs,
) -> RouteStateResult<()> {
    let state = state.to_owned();
    let config = config.to_owned();
    crate::account_writer::repair(store, move |tx| {
        let state = &state;

        tx.release_expired_continuations(now)?;
        for provider in tx.active_fence_providers()? {
            let accounts = allocator_roster_for_model(state, &provider, None, now, &[])
                .roster
                .map(|roster| roster.accounts)
                .unwrap_or_default();
            refresh_blocks(
                tx,
                &provider,
                &accounts,
                now,
                &config.allocator_policy(&provider),
            )?;
        }
        Ok(())
    })
}

/// Whether one route is judged by its own quota pools rather than by the
/// whole account: the account's reading has Antigravity pool windows, or the
/// route is one that needs a pool. Readings that meter the whole account
/// (aggregate windows only) keep the whole-account rules.
pub(super) fn route_is_pooled(
    usage: Option<&striatum_core::quota::Usage>,
    provider: &str,
    model: &str,
) -> bool {
    usage.is_some_and(|usage| {
        usage
            .extra_rate_windows
            .iter()
            .flatten()
            .any(striatum_core::quota_extract::is_account_pool_window)
    }) || striatum_core::quota_extract::requires_account_pool(&format!("{provider}/{model}"))
}

/// The decoded usage of a stored reading, if it decodes.
pub(super) fn reading_usage(entry: &Value) -> Option<striatum_core::quota::Usage> {
    serde_json::from_value::<AccountEntry>(entry.clone())
        .ok()
        .and_then(|entry| entry.usage)
}

/// A pool window under 100% used: evidence its pool can serve again.
fn pool_window_open(window: &RateWindow) -> bool {
    window.used_percent.is_finite() && (0.0..100.0).contains(&window.used_percent)
}

/// The pool a block is keyed to by name, with its window length when the
/// key states one.
fn named_block_pool(fence: &Fence) -> Option<(&str, Option<u32>)> {
    let key = fence.pool.as_ref()?;
    Some((key.name.as_deref()?, key.window_minutes))
}

/// Whether a block keyed to a named pool is lifted by a fresh reading taken
/// after it that shows every window of that pool under 100% used. Pools the
/// block is not keyed to neither hold nor lift it.
fn named_pool_block_open(
    fence: &Fence,
    reading: Option<&QuotaReading>,
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> bool {
    let Some((name, minutes)) = named_block_pool(fence) else {
        return false;
    };
    let Some(reading) = reading.filter(|reading| {
        reading
            .observed_at_ms
            .is_some_and(|observed| observed > fence.observed_at_ms)
            && striatum_core::accounts::fresh_for_recovery(reading, now, policy)
    }) else {
        return false;
    };
    let mut windows = reading
        .pool_windows
        .iter()
        .flatten()
        .filter(|window| {
            window.pool.as_deref() == Some(name)
                && minutes.is_none_or(|minutes| window.window_minutes == minutes)
        })
        .peekable();
    windows.peek().is_some()
        && windows.all(|window| {
            window.used_percent.is_finite() && (0.0..100.0).contains(&window.used_percent)
        })
}

/// When a standing block stops restricting its account: [`block_end`], or a
/// fresh reading showing the block's own named pool under 100% used.
pub(super) fn block_cleared(
    fence: &Fence,
    reading: Option<&QuotaReading>,
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> Option<striatum_core::accounts::BlockEnd> {
    block_end(fence, reading, now, policy).or_else(|| {
        named_pool_block_open(fence, reading, now, policy)
            .then_some(striatum_core::accounts::BlockEnd::Reading)
    })
}

/// Whether the account's latest fresh reading shows the named pool of one of
/// these blocks spent: a window of that pool at or above 100% used whose
/// reset has not passed. `None` when some block is not keyed to a named
/// pool, or the reading has no pool windows; those blocks keep the
/// whole-account recheck rules.
pub(super) fn named_pool_blocks_spent(
    fences: &[&Fence],
    quota: &AccountQuotaState,
    account: &str,
    now: EpochMs,
) -> Option<bool> {
    if fences.is_empty() {
        return None;
    }
    let pools = fences
        .iter()
        .map(|fence| named_block_pool(fence))
        .collect::<Option<Vec<_>>>()?;
    let windows = crate::credential_fences::fresh_pool_windows(quota, account, now);
    if windows.is_empty() {
        return None;
    }
    Some(pools.iter().any(|(name, minutes)| {
        windows.iter().any(|(pool, window)| {
            pool == name
                && minutes.is_none_or(|minutes| window.window_minutes == Some(i64::from(minutes)))
                && pool_window_spent(window, now)
        })
    }))
}

/// A pool window at or above 100% used whose reset has not yet passed.
fn pool_window_spent(window: &RateWindow, now_ms: i64) -> bool {
    window.used_percent.is_finite()
        && window.used_percent >= 100.0
        && window
            .resets_at
            .as_deref()
            .and_then(account_quota::parse_rfc3339_ms)
            .is_none_or(|reset| reset > now_ms)
}

/// Whether current evidence shows one pooled route's own pool spent, which
/// keeps its quota recheck from coming due. The reading must be the account's
/// latest and fresh (routing's one freshness band, `EXCLUDED_BAND_MS`), and
/// the spent window's own reset must not have passed. Once the reading ages
/// out of that band, or the reset passes with no newer reading, the 15-minute
/// recheck runs again: a quota hold never waits on readings that stopped
/// arriving. When routing reads no pool for the model, nothing holds the
/// recheck back.
pub(super) fn route_pool_spent(
    quota: &AccountQuotaState,
    account: &str,
    provider: &str,
    model: &str,
    model_pools: &[striatum_core::quota_extract::ModelPool],
    now_ms: i64,
) -> bool {
    crate::credential_fences::fresh_route_windows(
        quota,
        account,
        &format!("{provider}/{model}"),
        model_pools,
        now_ms,
    )
    .is_some_and(|(_, windows)| {
        windows
            .iter()
            .any(|window| pool_window_spent(window, now_ms))
    })
}

/// The latest reset of a fresh exhausted window that speaks for one quota
/// route fence. A pooled route takes only its own pools' resets; a spent pool
/// the model does not draw on never moves its check time. A reset only
/// schedules an extra check; it never holds the fence.
pub(super) fn route_exhausted_reset(
    quota: &AccountQuotaState,
    account_ids: &crate::broca_credentials::CredentialAccounts,
    fence: &striatum_store::AccountRouteFence,
    config: &crate::config_overrides::LiveConfig,
    now_ms: i64,
) -> Option<i64> {
    let account =
        crate::credential_fences::credential_account_id(account_ids, &fence.credential_id)?;
    let policy = config.allocator_policy(&fence.provider);
    let usage = crate::credential_fences::latest_fresh_entry(quota, &account, now_ms)
        .and_then(|entry| entry.usage);
    if !route_is_pooled(usage.as_ref(), &fence.provider, &fence.model) {
        return crate::credential_fences::exhausted_quota_reset_for_account(
            quota, &account, now_ms, &policy,
        );
    }
    let (_, windows) = crate::credential_fences::fresh_route_windows(
        quota,
        &account,
        &format!("{}/{}", fence.provider, fence.model),
        config.quota_model_pools_ref(),
        now_ms,
    )?;
    windows
        .iter()
        .filter(|window| pool_window_spent(window, now_ms))
        .filter_map(|window| {
            window
                .resets_at
                .as_deref()
                .and_then(account_quota::parse_rfc3339_ms)
        })
        .max()
}

/// What a newer reading proves about one quota route fence.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RouteRecovery {
    /// Nothing: the fence stands.
    Unproven,
    /// The whole account recovered: every route fence of the credential
    /// reported before the reading may clear.
    Account,
    /// The pools this route draws on recovered: only this route clears,
    /// since the credential's other routes may draw on other pools.
    Route,
}

/// Judge one quota route fence against its account's latest reading. Only a
/// reading newer than `newest_report` (the credential's latest refusal on any
/// route) can prove recovery. `blocks` are the account's standing pool blocks.
#[allow(clippy::too_many_arguments)]
fn route_fence_recovery(
    fence: &striatum_store::AccountRouteFence,
    account: &str,
    newest_report: i64,
    latest: &account_quota::StoredReading,
    blocks: &[AccountPoolFenceRow],
    quota: &AccountQuotaState,
    config: &crate::config_overrides::LiveConfig,
    now_ms: i64,
) -> RouteRecovery {
    let policy = config.allocator_policy(&fence.provider);
    if route_is_pooled(
        reading_usage(&latest.entry).as_ref(),
        &fence.provider,
        &fence.model,
    ) {
        // A pooled route clears as soon as a fresh reading taken after the
        // refusal shows every pool its model draws on under 100% used. Pools
        // the model never draws on neither hold nor clear it, and when the
        // reading names no pool for the model nothing clears by reading: the
        // 15-minute recheck decides. Standing account blocks keep their own
        // rules and still restrict admission on their own.
        let recovered = crate::credential_fences::fresh_route_windows(
            quota,
            account,
            &format!("{}/{}", fence.provider, fence.model),
            config.quota_model_pools_ref(),
            now_ms,
        )
        .is_some_and(|(fetched, windows)| {
            fetched > newest_report && !windows.is_empty() && windows.iter().all(pool_window_open)
        });
        return if recovered {
            RouteRecovery::Route
        } else {
            RouteRecovery::Unproven
        };
    }
    // A credential-wide clear requires newer evidence than every report and
    // recovery of every retained pool, not just the route being scored.
    let reading = quota_reading_for_model(&latest.entry, account, None, &[]);
    let recovered = if blocks.is_empty() {
        crate::credential_fences::fresh_headroom_after_for_account(
            quota,
            account,
            newest_report,
            now_ms,
            &policy,
        )
    } else {
        blocks.iter().all(|block| {
            serde_json::from_str::<Fence>(&block.evidence.fence_json).is_ok_and(|mut evidence| {
                evidence.pool = block.pool_key.clone();
                evidence.recovery_set = block.recovery_set.clone();
                evidence.observed_at_ms = evidence.observed_at_ms.max(newest_report);
                block_end(&evidence, reading.as_ref(), now_ms, &policy).is_some()
            })
        })
    };
    if recovered {
        RouteRecovery::Account
    } else {
        RouteRecovery::Unproven
    }
}

/// Each recovered quota route fence as (provider, credential, model, reading
/// time). The model is `None` when the whole account recovered, so every
/// route of the credential reported before the reading clears.
pub(super) fn route_fence_recoveries(
    tx: &(impl BlockReads + ?Sized),
    account_ids: &crate::broca_credentials::CredentialAccounts,
    quota: &AccountQuotaState,
    config: &crate::config_overrides::LiveConfig,
    now_ms: i64,
) -> RouteStateResult<Vec<(String, String, Option<String>, i64)>> {
    let latest_reports = tx.route_fence_latest_reports()?;
    let mut recoveries = Vec::new();
    for fence in tx.report_route_fences(now_ms)? {
        let Some(account) =
            crate::credential_fences::credential_account_id(account_ids, &fence.credential_id)
        else {
            continue;
        };
        let Some(newest_report) = latest_reports.get(&fence.credential_id).copied() else {
            continue;
        };
        let Some(latest) = crate::credential_fences::latest_reading_for_account(quota, &account)
        else {
            continue;
        };
        let Some(at) = latest
            .entry
            .get("fetchedAt")
            .and_then(Value::as_str)
            .and_then(account_quota::parse_rfc3339_ms)
            .filter(|at| *at > newest_report)
        else {
            continue;
        };
        let blocks: Vec<_> = tx
            .active_pool_fences(&fence.provider)?
            .into_iter()
            .filter(|block| block.evidence.account_id == account)
            .collect();
        match route_fence_recovery(
            &fence,
            &account,
            newest_report,
            latest,
            &blocks,
            quota,
            config,
            now_ms,
        ) {
            RouteRecovery::Unproven => {}
            RouteRecovery::Account => {
                recoveries.push((fence.provider, fence.credential_id, None, at));
            }
            RouteRecovery::Route => {
                recoveries.push((fence.provider, fence.credential_id, Some(fence.model), at));
            }
        }
    }
    Ok(recoveries)
}

/// Clear what `route_fence_recoveries` found recovered, in the writer command
/// that re-read it.
fn clear_recovered_route(
    tx: &AccountTx<'_>,
    provider: &str,
    credential: &str,
    model: Option<&str>,
    at: i64,
) -> RouteStateResult<()> {
    match model {
        Some(model) => tx.clear_account_route_fence_before(provider, credential, model, at),
        None => tx.clear_account_route_fences_before(provider, credential, at),
    }
    .map(|_| ())
}

pub(crate) fn repair_route_fences(
    store: &SqliteRouteStateStore,
    account_ids: &crate::broca_credentials::CredentialAccounts,
    quota: &AccountQuotaState,
    config: &crate::config_overrides::LiveConfig,
    now_ms: i64,
    after_read: impl FnOnce(),
) -> Result<(), String> {
    #[cfg(test)]
    if LEGACY_CONTEXT_REPAIRS.load(Ordering::SeqCst) {
        return legacy_route_fence_repair(store, account_ids, quota, config, now_ms);
    }
    let recoveries = crate::account_state::published(store, now_ms)
        .and_then(|blocks| route_fence_recoveries(&blocks, account_ids, quota, config, now_ms))
        .map_err(|error| error.to_string())?;
    if recoveries.is_empty() {
        return Ok(());
    }
    after_read();
    let account_ids = account_ids.to_owned();
    let quota = quota.to_owned();
    let config = config.to_owned();
    crate::account_writer::repair(store, move |tx| {
        let account_ids = &account_ids;
        let quota = &quota;
        let config = &config;

        // Re-read reports and pool requirements in the SQLite IMMEDIATE
        // transaction, which takes the write lock up front. A newer report can
        // invalidate the earlier read snapshot's recovery entirely.
        for (provider, credential, model, at) in
            route_fence_recoveries(&*tx, account_ids, quota, config, now_ms)?
        {
            clear_recovered_route(tx, &provider, &credential, model.as_deref(), at)?;
        }
        Ok(())
    })
    .map_err(|error| error.to_string())
}

// Independent pre-read-first recovery path used only by the differential test.
#[cfg(test)]
fn legacy_route_fence_repair(
    store: &SqliteRouteStateStore,
    account_ids: &crate::broca_credentials::CredentialAccounts,
    quota: &AccountQuotaState,
    config: &crate::config_overrides::LiveConfig,
    now_ms: i64,
) -> Result<(), String> {
    let routes = store
        .account_route_fences(now_ms)
        .map_err(|e| e.to_string())?;
    if routes.is_empty() {
        return Ok(());
    }
    let reports = store
        .account_route_fence_latest_reports()
        .map_err(|e| e.to_string())?;
    store
        .account_transaction(|tx| {
            for fence in &routes {
                let Some(account) = crate::credential_fences::credential_account_id(
                    account_ids,
                    &fence.credential_id,
                ) else {
                    continue;
                };
                let Some(report) = reports.get(&fence.credential_id).copied() else {
                    continue;
                };
                let Some(latest) =
                    crate::credential_fences::latest_reading_for_account(quota, &account)
                else {
                    continue;
                };
                let blocks: Vec<_> = tx
                    .active_pool_fences(&fence.provider)?
                    .into_iter()
                    .filter(|block| block.evidence.account_id == account)
                    .collect();
                let recovery = route_fence_recovery(
                    fence, &account, report, latest, &blocks, quota, config, now_ms,
                );
                let Some(at) = (recovery != RouteRecovery::Unproven)
                    .then(|| {
                        latest
                            .entry
                            .get("fetchedAt")
                            .and_then(Value::as_str)
                            .and_then(account_quota::parse_rfc3339_ms)
                    })
                    .flatten()
                else {
                    continue;
                };
                let model = (recovery == RouteRecovery::Route).then_some(fence.model.as_str());
                clear_recovered_route(tx, &fence.provider, &fence.credential_id, model, at)?;
            }
            Ok(())
        })
        .map_err(|e| e.to_string())
}

/// Reconcile both newly reported and persisted credential refusals. Once an
/// exhausted subscription reading supplies a reset for a billing refusal,
/// store an ordinary pool block so the hold survives stale readings and uses
/// normal block recovery.
/// The credential row remains as refusal evidence and also excludes paid-credit
/// placement: a billing refusal must not be bypassed by switching payment class.
pub(crate) fn refresh_credential_quota_holds(
    store: &SqliteRouteStateStore,
    credentials: &crate::broca_credentials::CredentialAccounts,
    quota: &AccountQuotaState,
    config: &crate::config_overrides::LiveConfig,
    refusals: Vec<striatum_core::route_state_store::CredentialEntitlementFence>,
    route_fences: &[striatum_store::AccountRouteFence],
    now: EpochMs,
) -> Result<Vec<striatum_core::route_state_store::CredentialEntitlementFence>, String> {
    // With no refusal evidence there is nothing to reconcile; ordinary reads
    // must not reserve SQLite's writer just to iterate an empty list.
    if refusals.is_empty() {
        return Ok(refusals);
    }
    let captured_refusals = refusals.clone();
    let mut changed = false;
    let kept = crate::account_state::published(store, now)
        .and_then(|blocks| {
            reconcile_credential_quota_holds(
                &blocks,
                credentials,
                quota,
                config,
                refusals,
                route_fences,
                now,
                false,
                &mut changed,
            )
        })
        .map_err(|error| error.to_string())?;
    if !changed {
        return Ok(kept);
    }
    // The read pass is only a hint to acquire the writer. Re-read evidence and
    // holds inside it, so a newer refusal or an admission's reservation of a
    // fenced credential's next probe cannot be overwritten by the earlier read.
    let credentials = credentials.to_owned();
    let quota = quota.to_owned();
    let config = config.to_owned();
    let route_fences = route_fences.to_owned();
    crate::account_writer::repair(store, move |tx| {
        let credentials = &credentials;
        let quota = &quota;
        let config = &config;

        let current_refusals = tx.report_credential_refusals()?;
        let mut current_routes = tx.report_route_fences(now)?;
        // Context loading may already have cleared the route fence using
        // the same fresh reading. That fence still proves the refusal was
        // quota-related, so headroom can heal the credential refusal too.
        // Reuse that evidence only if the refusal itself has not changed.
        current_routes.extend(
            route_fences
                .iter()
                .filter(|route| {
                    current_refusals.iter().any(|refusal| {
                        refusal.provider_id == route.provider
                            && refusal.credential_id == route.credential_id
                            && captured_refusals.contains(refusal)
                    })
                })
                .cloned(),
        );
        refresh_credential_quota_holds_in(
            tx,
            credentials,
            quota,
            config,
            current_refusals,
            &current_routes,
            now,
        )
    })
    .map_err(|error| error.to_string())
}

/// Reconcile both newly reported and persisted credential refusals. Once an
/// exhausted subscription reading supplies a reset for a billing refusal,
/// store an ordinary pool block so the hold survives stale readings and uses
/// normal block recovery.
/// The credential row remains as refusal evidence and also excludes paid-credit
/// placement: a billing refusal must not be bypassed by switching payment class.
pub(crate) fn refresh_credential_quota_holds_in(
    tx: &AccountCommand<'_>,
    credentials: &crate::broca_credentials::CredentialAccounts,
    quota: &AccountQuotaState,
    config: &crate::config_overrides::LiveConfig,
    refusals: Vec<striatum_core::route_state_store::CredentialEntitlementFence>,
    route_fences: &[striatum_store::AccountRouteFence],
    now: EpochMs,
) -> RouteStateResult<Vec<striatum_core::route_state_store::CredentialEntitlementFence>> {
    reconcile_credential_quota_holds(
        tx,
        credentials,
        quota,
        config,
        refusals,
        route_fences,
        now,
        true,
        &mut false,
    )
}

pub(super) fn reconcile_credential_quota_holds(
    tx: &(impl BlockReads + ?Sized),
    credentials: &crate::broca_credentials::CredentialAccounts,
    quota: &AccountQuotaState,
    config: &crate::config_overrides::LiveConfig,
    refusals: Vec<striatum_core::route_state_store::CredentialEntitlementFence>,
    route_fences: &[striatum_store::AccountRouteFence],
    now: EpochMs,
    write: bool,
    changed: &mut bool,
) -> RouteStateResult<Vec<striatum_core::route_state_store::CredentialEntitlementFence>> {
    let prior_clocks = tx.fence_rechecks()?;
    let mut kept = Vec::new();
    for refusal in refusals {
        if !crate::credential_fences::is_billing_code(&refusal.code) {
            // Quota utilization cannot establish entitlement. Remove quota
            // blocks left by older reconciliation, but retain the refusal so
            // a timed probe can discover a provider-side reversal.
            let prefix = crate::credential_fences::quota_hold_prefix(&refusal);
            for block in tx.active_pool_fences(&refusal.provider_id)? {
                if block.evidence.fence_id.starts_with(&prefix) {
                    *changed = true;
                    if write {
                        tx.repair(
                            striatum_store::DryRunRepair::ClearHeld(
                                block.evidence.clone(),
                                "entitlement_not_quota".into(),
                            ),
                            now,
                        )?;
                    }
                }
            }
            kept.push(refusal);
            continue;
        }
        let Some(account) =
            crate::credential_fences::credential_account_id(credentials, &refusal.credential_id)
        else {
            kept.push(refusal);
            continue;
        };
        let prefix = crate::credential_fences::quota_hold_prefix(&refusal);
        let policy = config.allocator_policy(&refusal.provider_id);
        let prior_hold = tx
            .active_pool_fences(&refusal.provider_id)?
            .iter()
            .any(|block| block.evidence.fence_id.starts_with(&prefix));
        let headroom = crate::credential_fences::fresh_headroom_after_for_account(
            quota,
            &account,
            i64::MIN,
            now,
            &policy,
        );
        let reading_at = crate::credential_fences::latest_reading_for_account(quota, &account)
            .and_then(|reading| reading.entry.get("fetchedAt").and_then(Value::as_str))
            .and_then(account_quota::parse_rfc3339_ms);
        let contradicted_at_receipt =
            headroom && reading_at.is_some_and(|at| at <= refusal.observed_at_ms);
        let prior_quota = prior_hold
            || prior_clocks.iter().any(|origin| {
                origin.kind == "credential"
                    && origin.quota
                    && origin.provider == refusal.provider_id
                    && origin.credential_id.as_deref() == Some(&refusal.credential_id)
                    && origin.evidence_at == refusal.observed_at_ms
            })
            || route_fences.iter().any(|route| {
                route.provider == refusal.provider_id
                    && route.credential_id == refusal.credential_id
            });
        let quota_interpretation = !contradicted_at_receipt
            && (prior_quota
                || (crate::credential_fences::has_quota_signal_for_account(quota, &account)
                    && !headroom));
        let prior_classified = prior_clocks.iter().find(|origin| {
            origin.kind == "credential"
                && origin.classified
                && origin.provider == refusal.provider_id
                && origin.credential_id.as_deref() == Some(&refusal.credential_id)
                && origin.evidence_at == refusal.observed_at_ms
        });
        let (classified, needs_write) = if let Some(origin) = prior_classified {
            (origin.quota, false)
        } else {
            tx.credential_quota_classification(
                &refusal.provider_id,
                &refusal.credential_id,
                quota_interpretation,
            )?
        };
        *changed |= needs_write;
        let classified = if write && needs_write {
            tx.repair(
                striatum_store::DryRunRepair::Classify(
                    refusal.provider_id.clone(),
                    refusal.credential_id.clone(),
                    quota_interpretation,
                ),
                now,
            )?
        } else {
            classified
        };
        if !classified {
            kept.push(refusal);
            continue;
        }
        if !quota_interpretation {
            kept.push(refusal);
            continue;
        }
        let reading = crate::credential_fences::latest_reading_for_account(quota, &account)
            .and_then(|reading| quota_reading_for_model(&reading.entry, &account, None, &[]));
        let reading_reset = crate::credential_fences::exhausted_quota_reset_for_account(
            quota, &account, now, &policy,
        );
        let lifted = (|| -> RouteStateResult<bool> {
            let blocks: Vec<_> = tx
                .active_pool_fences(&refusal.provider_id)?
                .into_iter()
                .filter(|block| block.evidence.fence_id.starts_with(&prefix))
                .collect();
            let prior = blocks
                .iter()
                .filter_map(|block| {
                    let mut fence =
                        serde_json::from_str::<Fence>(&block.evidence.fence_json).ok()?;
                    fence.recovery_set = block.recovery_set.clone();
                    Some(fence)
                })
                .max_by_key(|fence| match fence.reset {
                    ResetTime::At(reset) => reset,
                    _ => 0,
                });
            let newer_reading = reading
                .as_ref()
                .and_then(|reading| reading.observed_at_ms)
                .is_some_and(|fetched| fetched > refusal.observed_at_ms);
            let all_recovered = if blocks.is_empty() {
                crate::credential_fences::fresh_headroom_after_for_account(
                    quota,
                    &account,
                    refusal.observed_at_ms,
                    now,
                    &policy,
                )
            } else {
                blocks.iter().all(|block| {
                    serde_json::from_str::<Fence>(&block.evidence.fence_json).is_ok_and(
                        |mut fence| {
                            fence.pool = block.pool_key.clone();
                            fence.recovery_set = block.recovery_set.clone();
                            fence.observed_at_ms = fence.observed_at_ms.max(refusal.observed_at_ms);
                            block_end(&fence, reading.as_ref(), now, &policy).is_some()
                        },
                    )
                })
            };
            if all_recovered {
                for block in &blocks {
                    *changed = true;
                    if write {
                        tx.repair(
                            striatum_store::DryRunRepair::ClearFence(
                                block.evidence.fence_id.clone(),
                                "fresh_headroom".into(),
                            ),
                            now,
                        )?;
                    }
                }
                return Ok(true);
            }
            let reset = reading_reset.filter(|_| newer_reading).or_else(|| {
                reading_reset
                    .into_iter()
                    .chain(prior.iter().filter_map(|fence| match fence.reset {
                        ResetTime::At(reset) if reset > refusal.observed_at_ms => Some(reset),
                        _ => None,
                    }))
                    .max()
            });
            let mut reset = reset;
            // Contemporaneous reports can name different windows. Newer
            // quota evidence supersedes an older report's guessed horizon.
            for route in route_fences.iter().filter(|route| {
                route.provider == refusal.provider_id
                    && crate::credential_fences::credential_account_id(
                        credentials,
                        &route.credential_id,
                    )
                    .as_deref()
                        == Some(account.as_str())
            }) {
                if !reading
                    .as_ref()
                    .and_then(|reading| reading.observed_at_ms)
                    .is_some_and(|fetched| fetched > route.recorded_at)
                {
                    if route.until_ms > refusal.observed_at_ms {
                        reset =
                            Some(reset.map_or(route.until_ms, |reset| reset.max(route.until_ms)));
                    }
                }
            }
            let mut fence = prior.unwrap_or_else(|| Fence {
                account_id: account.clone(),
                pool: None,
                recovery_set: recovery_set_from(reading.as_ref(), now, &policy),
                observed_at_ms: refusal.observed_at_ms,
                reset: reset.map(ResetTime::At).unwrap_or(ResetTime::Missing),
                retry_hint_ms: None,
                source: striatum_core::accounts::ConfirmationSource::BrocaAccountLimit,
            });
            fence.reset = reset.map(ResetTime::At).unwrap_or(ResetTime::Missing);
            fence.observed_at_ms = fence.observed_at_ms.max(refusal.observed_at_ms);
            for key in blocks.iter().flat_map(|block| &block.recovery_set) {
                if !fence.recovery_set.contains(key) {
                    fence.recovery_set.push(key.clone());
                }
            }
            if let Some(end) = block_end(&fence, reading.as_ref(), now, &policy) {
                for block in &blocks {
                    *changed = true;
                    if write {
                        tx.repair(
                            striatum_store::DryRunRepair::ClearFence(
                                block.evidence.fence_id.clone(),
                                wire_str(&end),
                            ),
                            now,
                        )?;
                    }
                }
                return Ok(true);
            }
            if !blocks.iter().any(|block| {
                serde_json::from_str::<Fence>(&block.evidence.fence_json).is_ok_and(|fence| {
                    fence.reset == reset.map(ResetTime::At).unwrap_or(ResetTime::Missing)
                        && fence.observed_at_ms == refusal.observed_at_ms
                })
            }) {
                *changed = true;
                if write {
                    for block in &blocks {
                        tx.repair(
                            striatum_store::DryRunRepair::ClearFence(
                                block.evidence.fence_id.clone(),
                                "quota_evidence_updated".into(),
                            ),
                            now,
                        )?;
                    }
                    tx.repair(
                        striatum_store::DryRunRepair::InsertFence(AccountPoolFenceRow {
                            evidence: AccountFenceRow {
                                fence_id: format!("{prefix}{}", new_id("hold")),
                                provider: refusal.provider_id.clone(),
                                account_id: account.clone(),
                                scope_json: wire(&fence.pool).to_string(),
                                fence_json: wire(&fence).to_string(),
                                source: wire_str(&fence.source),
                                observed_at: refusal.observed_at_ms,
                                principal: "routing".into(),
                                admission_id: None,
                                observation_id: None,
                                created_at: now,
                                cleared_at: None,
                                cleared_reason: None,
                            },
                            pool_key: None,
                            recovery_set: fence.recovery_set,
                        }),
                        now,
                    )?;
                }
            }
            Ok(false)
        })()?;
        if lifted {
            *changed = true;
            if write {
                tx.repair(
                    striatum_store::DryRunRepair::ClearCredential(
                        refusal.provider_id.clone(),
                        refusal.credential_id.clone(),
                    ),
                    now,
                )?;
            }
        } else {
            kept.push(refusal);
        }
    }
    Ok(kept)
}
