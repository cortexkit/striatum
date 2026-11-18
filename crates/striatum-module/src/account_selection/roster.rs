//! Allocator rosters and quota readings.
//! The vault-to-quota join is also used for billing, independently of admitting an account.

use super::*;

// ---------------------------------------------------------------------------
// Roster: vault identities joined with Insula readings
// ---------------------------------------------------------------------------

/// The allocator's roster for one provider plus what routing knows about its
/// gaps, for the explanation.
#[derive(Debug, Clone, PartialEq)]
pub struct RosterBuild {
    /// `None` when routing has never listed the vault (and has no persisted
    /// listing): new cold admissions are then unavailable.
    pub roster: Option<Roster>,
    /// Roster facts for the record: the listing's digest and age, whether it
    /// is past the 24-hour limit, the join gaps in both directions and the
    /// alarms. Emails are left out.
    pub context: Value,
}

pub(super) fn is_default_record(credential_id: &str) -> bool {
    credential_id.split(':').count() == 2
}

fn reset_time(text: Option<&str>) -> ResetTime {
    match text {
        None => ResetTime::Missing,
        Some(text) => {
            account_quota::parse_rfc3339_ms(text).map_or(ResetTime::Unparsable, ResetTime::At)
        }
    }
}

pub(super) fn quota_window(window: &RateWindow) -> Option<QuotaWindow> {
    let minutes = u32::try_from(window.window_minutes?).ok()?;
    Some(QuotaWindow {
        used_percent: window.used_percent,
        // Insula's `rawUsedPercent` is the poll's percent before the account's
        // saved (banked) resets are applied, not the unrounded utilization an
        // upstream response header carries. Only that header value may
        // confirm exhaustion, so a poll reading carries none.
        raw_utilization: None,
        resets_at: reset_time(window.resets_at.as_deref()),
        window_minutes: minutes,
        window_kind: window.window_kind.clone(),
        source: ObservationSource::Poll,
    })
}

/// One stored Insula entry as the allocator's reading. Keep every reported
/// general period and its duration; scoped windows keep their producer id and
/// length. Pooled windows are limited to the requested model's pools, while
/// `pool_windows` keeps every pool of the whole reading for block recovery.
pub(super) fn quota_reading_for_model(
    entry: &Value,
    account_id: &str,
    capacity_key: Option<&str>,
    model_pools: &[striatum_core::quota_extract::ModelPool],
) -> Option<QuotaReading> {
    let entry: AccountEntry = serde_json::from_value(entry.clone()).ok()?;
    quota_reading_from_entry(&entry, account_id, capacity_key, model_pools)
}

pub(super) fn quota_reading_from_entry(
    entry: &AccountEntry,
    account_id: &str,
    capacity_key: Option<&str>,
    model_pools: &[striatum_core::quota_extract::ModelPool],
) -> Option<QuotaReading> {
    let usage = entry.usage.as_ref();
    let reported = usage
        .map(|usage| {
            striatum_core::quota_extract::account_rate_windows(usage, capacity_key, model_pools)
        })
        .unwrap_or_default();
    let general_windows: Vec<_> = reported
        .iter()
        .filter_map(|(_, window)| quota_window(window))
        .collect();
    let invalid_topology = reported.len() != general_windows.len()
        || general_windows
            .iter()
            .any(|window| window.window_minutes == 0);
    let general = |minutes: u32| {
        general_windows
            .iter()
            .find(|window| window.window_minutes == minutes)
            .cloned()
    };
    let scoped = match usage.and_then(|usage| usage.extra_rate_windows.as_ref()) {
        None => ScopedList::Missing,
        Some(windows) => ScopedList::Listed(
            windows
                .iter()
                .filter(|extra| {
                    !striatum_core::quota_extract::is_account_pool_window(extra)
                })
                .filter_map(|extra| {
                    let window = extra.window.as_ref()?;
                    if extra.id.as_deref().is_none_or(str::is_empty)
                        && claim_unattributable_scope_warning(account_id)
                    {
                        crate::warn_operator(format!(
                            "[striatum] account {account_id}: ignored unattributable extraRateWindows window without an id; general quota windows still apply"
                        ));
                    }
                    Some(ScopedWindow {
                        scope_id: extra.id.clone().unwrap_or_default(),
                        window_minutes: window
                            .window_minutes
                            .and_then(|minutes| u32::try_from(minutes).ok())
                            .unwrap_or(0),
                        used_percent: window.used_percent,
                        resets_at: reset_time(window.resets_at.as_deref()),
                    })
                })
                .collect(),
        ),
    };
    Some(QuotaReading {
        observed_at_ms: entry
            .fetched_at
            .as_deref()
            .and_then(account_quota::parse_rfc3339_ms),
        // An error without a class is still a degraded entry.
        error_class: entry
            .error_class
            .clone()
            .or_else(|| invalid_topology.then(|| "invalid_general_window".into()))
            .or_else(|| {
                entry
                    .error
                    .as_ref()
                    .map(|_| "unclassified_error".to_string())
            }),
        stale: entry.stale.as_ref().map(|stale| StaleDisclosure {
            since_ms: stale
                .since
                .as_deref()
                .and_then(account_quota::parse_rfc3339_ms),
            class: stale.class.clone(),
        }),
        five_hour: general(GeneralWindow::FiveHour.window_minutes()),
        seven_day: general(GeneralWindow::SevenDay.window_minutes()),
        general_windows: Some(general_windows),
        scoped,
        pool_windows: Some(
            usage
                .map(striatum_core::quota_extract::named_account_windows)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(pool, window)| {
                    Some(PoolWindowReading {
                        pool: pool.map(str::to_string),
                        window_minutes: u32::try_from(window.window_minutes?)
                            .ok()
                            .filter(|minutes| *minutes > 0)?,
                        used_percent: window.used_percent,
                    })
                })
                .collect(),
        ),
    })
}

/// Bad producer metadata can be encountered on every selection. Warn once per
/// account for the process lifetime rather than flooding the operator log.
pub(super) fn claim_unattributable_scope_warning(account_id: &str) -> bool {
    static WARNED: std::sync::OnceLock<std::sync::Mutex<BTreeSet<String>>> =
        std::sync::OnceLock::new();
    WARNED
        .get_or_init(Default::default)
        .lock()
        .expect("unattributable scope warning lock")
        .insert(account_id.to_string())
}

/// Join the vault roster with Insula readings for one routing provider.
///
/// Candidates are the vault's identities: an Insula account the vault does not
/// list is a gap, never a candidate. A listed identity with no reading, or a
/// tombstoned one, is a candidate with no reading (unknown for cold work, and
/// after 15 minutes an alarm). A record whose vault state is not `active`
/// needs re-authorization. The 24-hour roster limit is the allocator's: the
/// roster is passed through with its listing time and the allocator refuses
/// new cold work once it is too old, while existing affinity continues.
#[cfg(test)]
pub fn allocator_roster(state: &AccountQuotaState, provider: &str, now_ms: i64) -> RosterBuild {
    allocator_roster_for_model(state, provider, None, now_ms, &[])
}

pub(super) fn allocator_roster_for_model(
    state: &AccountQuotaState,
    provider: &str,
    model: Option<&str>,
    now_ms: i64,
    model_pools: &[striatum_core::quota_extract::ModelPool],
) -> RosterBuild {
    // Learn adapter/vendor relationships only from account-id joins. An adapter
    // is usable for unread siblings only when all its mapped readings agree.
    let mut vendors: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    if let Some(vault) = &state.roster {
        for reading in state.readings.readings.values() {
            if let (Some(identity), Some(vendor)) = (
                account_quota::identity_for_reading(vault, reading),
                reading.routing_provider(),
            ) {
                vendors
                    .entry(&identity.refresh_adapter)
                    .or_default()
                    .insert(vendor);
            }
        }
    }
    let adapters: BTreeSet<_> = vendors
        .into_iter()
        .filter(|(_, vendors)| vendors.len() == 1 && vendors.contains(provider))
        .map(|(adapter, _)| adapter)
        .collect();
    let report = account_quota::build_report(
        &state.readings,
        state.roster.as_ref(),
        &state.roster_poll,
        now_ms,
    );
    let insula_only: Vec<Value> = report
        .gaps
        .insula_only
        .iter()
        .filter(|gap| {
            state
                .readings
                .readings
                .get(&(gap.provider.clone(), gap.account_id.clone()))
                .is_some_and(|reading| reading.routing_provider() == Some(provider))
        })
        .map(|gap| json!({ "account_id": gap.account_id, "reason": gap.reason }))
        .collect();
    let vault_only: Vec<Value> = report
        .gaps
        .vault_only
        .iter()
        .filter(|gap| adapters.contains(gap.refresh_adapter.as_str()))
        .map(|gap| {
            json!({
                "account_id": gap.account_id,
                "credential_ids": gap.credential_ids,
                "first_seen_ms": gap.first_seen_ms,
                "first_reading_overdue": gap.first_reading_overdue,
            })
        })
        .collect();
    let roster = state.roster.as_ref().map(|vault| Roster {
        view_digest: vault.view.clone(),
        fetched_at_ms: vault.fetched_at_ms,
        accounts: vault
            .identities
            .iter()
            .filter(|identity| {
                let matching: Vec<_> = state
                    .readings
                    .readings
                    .values()
                    .filter(|reading| reading.account_id == identity.account_id)
                    .collect();
                if matching.is_empty() {
                    return adapters.contains(identity.refresh_adapter.as_str());
                }
                // A vendorless, ambiguous or conflicting reading cannot borrow
                // a sibling's vendor, including through sticky affinity.
                matching.iter().all(|reading| {
                    reading.routing_provider() == Some(provider)
                        && account_quota::identity_for_reading(vault, reading) == Some(*identity)
                })
            })
            .map(|identity| {
                let reading = state
                    .readings
                    .readings
                    .values()
                    .filter(|reading| {
                        reading.account_id == identity.account_id
                            && reading.routing_provider() == Some(provider)
                    })
                    .filter(|reading| reading.tombstoned_at_ms.is_none())
                    .max_by_key(|reading| {
                        (
                            reading
                                .entry
                                .get("fetchedAt")
                                .and_then(Value::as_str)
                                .and_then(account_quota::parse_rfc3339_ms),
                            &reading.provider,
                        )
                    });
                let capacity_key = model.map(|model| format!("{provider}/{model}"));
                let usage = reading
                    .and_then(|reading| {
                        serde_json::from_value::<AccountEntry>(reading.entry.clone()).ok()
                    })
                    .and_then(|entry| entry.usage);
                let pool_route = capacity_key
                    .as_deref()
                    .is_some_and(|key| striatum_core::quota_extract::requires_account_pool(key));
                let pooled_reading = usage
                    .as_ref()
                    .and_then(|usage| usage.extra_rate_windows.as_ref())
                    .into_iter()
                    .flatten()
                    .any(striatum_core::quota_extract::is_account_pool_window);
                // Product/pool reach is a hard filter, not merely unknown quota:
                // sticky affinity and overrides must not bypass it either.
                let model_reach = if model.is_some() && (pool_route || pooled_reading) {
                    let reaches = usage.as_ref().is_some_and(|usage| {
                        !striatum_core::quota_extract::account_rate_windows(
                            usage,
                            capacity_key.as_deref(),
                            model_pools,
                        )
                        .is_empty()
                    });
                    ModelReach::Only(if reaches {
                        vec![model.unwrap().to_string()]
                    } else {
                        Vec::new()
                    })
                } else {
                    ModelReach::All
                };
                AccountView {
                    account_id: identity.account_id.clone(),
                    routing_provider: provider.to_string(),
                    refresh_adapter: identity.refresh_adapter.clone(),
                    credentials: identity
                        .credential_ids
                        .iter()
                        .map(|credential_id| CredentialRecord {
                            credential_id: credential_id.clone(),
                            default_record: is_default_record(credential_id),
                            needs_reauth: !identity.active_credential_ids.contains(credential_id),
                        })
                        .collect(),
                    model_reach,
                    quota: reading.and_then(|reading| {
                        quota_reading_for_model(
                            &reading.entry,
                            &reading.account_id,
                            capacity_key.as_deref(),
                            model_pools,
                        )
                    }),
                }
            })
            .collect(),
    });
    let context = json!({
        "provider": provider,
        "tracked": roster.as_ref().is_some_and(|roster| !roster.accounts.is_empty()),
        "view_digest": state.roster.as_ref().map(|vault| vault.view.clone()),
        "fetched_at_ms": state.roster.as_ref().map(|vault| vault.fetched_at_ms),
        "past_max_age": state.roster.as_ref().is_some_and(|vault| {
            now_ms - vault.fetched_at_ms >= account_quota::ROSTER_MAX_AGE_MS
        }),
        "poll": report.roster.poll,
        "account_ids": roster
            .as_ref()
            .map(|roster| roster.accounts.iter().map(|view| view.account_id.clone()).collect::<Vec<_>>())
            .unwrap_or_default(),
        "gaps": { "insula_only": insula_only, "vault_only": vault_only },
        "alarms": report.alarms,
    });
    RosterBuild { roster, context }
}

pub(super) fn roster_for(ctx: &Ctx<'_>, provider: &str) -> RosterBuild {
    roster_for_model(ctx, provider, None)
}

pub(super) fn roster_for_model(ctx: &Ctx<'_>, provider: &str, model: Option<&str>) -> RosterBuild {
    if let (Some(classes), Some(model)) = (ctx.classes.as_ref(), model) {
        return classes.roster_for(provider, model, false);
    }
    match ctx.accounts.map(|state| state.lock()) {
        Some(Ok(guard)) => {
            allocator_roster_for_model(&guard, provider, model, ctx.now_ms, &ctx.policy.model_pools)
        }
        Some(Err(_)) => RosterBuild {
            roster: None,
            context: json!({ "provider": provider, "error": "account state lock poisoned" }),
        },
        None => RosterBuild {
            roster: None,
            context: json!({ "provider": provider, "error": "account state not restored yet" }),
        },
    }
}

/// Pricing uses the same roster as account_for, not the subscription policy or
/// a guessed credential id. Called by the poller, never during model selection.
pub(crate) fn billing_roster(
    state: &AccountQuotaState,
    provider: &str,
    model: &str,
    now_ms: i64,
    model_pools: &[striatum_core::quota_extract::ModelPool],
) -> Vec<AccountView> {
    allocator_roster_for_model(state, provider, Some(model), now_ms, model_pools)
        .roster
        .map(|roster| roster.accounts)
        .unwrap_or_default()
}
