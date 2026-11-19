//! Credential refusal fences.
//!
//! When a provider permanently refuses one vault credential, core reports the
//! provider, the credential and the provider's code with the run's route outcome.
//! Two refusals are reported: an entitlement or authorization refusal such as
//! `ENTITLEMENT_ERROR` (the account has no plan for the provider's models), and an
//! empty paid balance (`billing_exhausted`). Routing stores the (provider,
//! credential) pair and stops choosing that provider through that credential: a
//! route whose every serving credential is fenced is rejected as
//! `credential_unentitled` or `credential_billing_exhausted`, and a route that
//! another credential still serves is launched pinned to that other credential.
//! Explicit pins require an eligible credential too.
//!
//! A fence ends three ways. A change to the credential's vault listing (provider
//! ids, plan tier or state) ends it at once, as today. A fence older than
//! [`FENCE_PROBE_RECHECK_MS`] admits a single probe request on that credential:
//! an atomic claim advances a separate retry clock so no second probe follows within the
//! interval, a successful outcome ends the fence, and a billing or entitlement
//! failure renews it for another interval. An operator lifts every fence on a
//! named credential at once, for use right after topping it up.
//!
//! Billing refusals consistent with a subscription's quota become quota holds:
//! fresh newer headroom lifts them, and without a fresh reading they permit one
//! probe every fifteen minutes. Reset dates only schedule additional checks.
//! Fresh headroom contradicting the refusal keeps it non-quota instead; later
//! quota refreshes cannot immediately reopen a credit or entitlement wall.
//! Entitlement refusals always keep the timed probe: quota utilization says
//! nothing about entitlement, so a reading cannot lift or convert that fence.

use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::HashSet;

use striatum_core::route_state_store::{CredentialEntitlementFence, CredentialRefusal};
use striatum_core::{
    RouteStateResult, REASON_CREDENTIAL_BILLING_EXHAUSTED, REASON_CREDENTIAL_UNENTITLED,
};
use striatum_store::SqliteRouteStateStore;

use crate::broca_credentials::{ListingLookup, SharedBrocaCredentials};

/// How long a billing or entitlement fence holds before routing admits one
/// probe request on the fenced credential: at most one hour. Topping up a balance
/// does not change the credential's vault listing, so without a recheck the
/// fence would hold until the listing changed for an unrelated reason.
///
/// Billing fences use this interval only without a quota signal (see
/// [`probe_allowed`]). A subscription's billing hold uses its quota reset and
/// headroom readings instead; entitlement refusals always keep timed probes.
#[cfg(test)]
pub(crate) const FENCE_PROBE_RECHECK_MS: i64 = CREDENTIAL_REFUSED_REPROBE_MS;

/// A non-quota refused credential is blocked for at most one hour. A pooled API key gets this
/// block in the key pool. Any other credential gets routing's credential
/// refusal fence, which lets one probe request through after that interval.
/// This interval is routing's documented credential-refusal re-probe horizon.
pub(crate) const CREDENTIAL_REFUSED_REPROBE_MS: i64 =
    striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS;

/// A window reading at or above this used percent counts as exhausted for
/// turning a billing-class refusal into a quota hold. The
/// one-point margin catches a producer that reports 99.x for a full window
/// while keeping the hold to genuinely full windows.
pub(crate) const QUOTA_HOLD_USED_THRESHOLD: f64 = 99.0;

/// Whether a timed probe may be admitted for a fence of this age.
///
/// A pay-per-token credential (for example an API key out of credit) has no
/// Insula quota reading: topping its balance up changes no vault listing and
/// publishes no reading, so without a timed recheck the fence would hold
/// until the listing changed for an unrelated reason. A subscription's billing
/// refusal uses its quota reset and fresh headroom readings instead of blind
/// probes. An entitlement refusal is independent of quota utilization: only a
/// real request can discover a provider-side reversal without a listing change
/// or an operator lift, so it keeps the one-hour probe even with quota readings.
#[cfg(test)]
pub(crate) fn probe_allowed(code: &str, has_quota_signal: bool, age_ms: i64) -> bool {
    (!is_billing_code(code) || !has_quota_signal) && age_ms > FENCE_PROBE_RECHECK_MS
}

/// The reset a quota hold on `credential_id` runs until, when its latest
/// fresh reading shows a window at its limit or inside the allocator reserve with a future
/// reset. Returns the latest such reset, so a run-reported reset and a
/// reading reset combine by taking the later. `None` when the credential has
/// no reading, the latest reading is not fresh, or no exhausted window names
/// a future reset: the refusal is not a quota hold.
pub(crate) fn exhausted_quota_reset(
    quota: &crate::account_quota::AccountQuotaState,
    credentials: &crate::broca_credentials::CredentialAccounts,
    credential_id: &str,
    now_ms: i64,
    policy: &striatum_core::accounts::AllocatorPolicy,
) -> Option<i64> {
    let account_id = credential_account_id(credentials, credential_id)?;
    exhausted_quota_reset_for_account(quota, &account_id, now_ms, policy)
}

/// Latest fresh exhausted reset for one vault account, by account id.
/// Fresh means the reading's producer date is inside the full (under 5 minutes)
/// or discounted (under 20 minutes) age band defined in `account_quota`, with
/// no entry error and no unrecognized stale class. A window at the hold threshold
/// or inside the allocator reserve holds until its reset; the latest reset wins.
pub(crate) fn exhausted_quota_reset_for_account(
    quota: &crate::account_quota::AccountQuotaState,
    account_id: &str,
    now_ms: i64,
    policy: &striatum_core::accounts::AllocatorPolicy,
) -> Option<i64> {
    let entry = latest_fresh_entry(quota, account_id, now_ms)?;
    let usage = entry.usage.as_ref()?;
    let mut resets = Vec::new();
    for window in [
        usage.primary.as_ref(),
        usage.secondary.as_ref(),
        usage.tertiary.as_ref(),
    ]
    .into_iter()
    .flatten()
    .chain(
        usage
            .extra_rate_windows
            .iter()
            .flatten()
            .filter_map(|extra| extra.window.as_ref()),
    ) {
        if window.used_percent.is_finite() && !window_has_headroom(window, policy) {
            if let Some(reset) = window
                .resets_at
                .as_deref()
                .and_then(crate::account_quota::parse_rfc3339_ms)
                .filter(|reset| *reset > now_ms)
            {
                resets.push(reset);
            }
        }
    }
    resets.into_iter().max()
}

/// The windows of `account_id`'s latest fresh reading that meter one routed
/// capacity, with the time the reading was fetched. On a pooled reading these
/// are only the pools the capacity draws on (for an Antigravity Gemini model,
/// `gemini-5h` and `gemini-weekly`, never the Claude and GPT `3p` pool).
pub(crate) fn fresh_route_windows(
    quota: &crate::account_quota::AccountQuotaState,
    account_id: &str,
    capacity_key: &str,
    model_pools: &[striatum_core::quota_extract::ModelPool],
    now_ms: i64,
) -> Option<(i64, Vec<striatum_core::quota::RateWindow>)> {
    let entry = latest_fresh_entry(quota, account_id, now_ms)?;
    let fetched = entry
        .fetched_at
        .as_deref()
        .and_then(crate::account_quota::parse_rfc3339_ms)?;
    let usage = entry.usage.as_ref()?;
    Some((
        fetched,
        striatum_core::quota_extract::account_rate_windows(usage, Some(capacity_key), model_pools)
            .into_iter()
            .map(|(_, window)| window)
            .collect(),
    ))
}

/// The Antigravity pool windows of `account_id`'s latest fresh reading, each
/// with the pool it meters. Empty when that reading meters the whole account
/// rather than named pools.
pub(crate) fn fresh_pool_windows(
    quota: &crate::account_quota::AccountQuotaState,
    account_id: &str,
    now_ms: i64,
) -> Vec<(&'static str, striatum_core::quota::RateWindow)> {
    latest_fresh_entry(quota, account_id, now_ms)
        .and_then(|entry| entry.usage)
        .map(|usage| {
            striatum_core::quota_extract::named_account_windows(&usage)
                .into_iter()
                .filter_map(|(pool, window)| Some((pool?, window)))
                .collect()
        })
        .unwrap_or_default()
}

/// Whether a fresh reading with headroom arrived after `fence_observed_ms`
/// for `account_id`: every window has capacity above its allocator reserve
/// and below the hold threshold. A quota hold
/// lifts on such a reading. An older reading cannot heal a newer refusal.
/// Entitlement fences must not use quota readings for recovery.
pub(crate) fn fresh_headroom_after_for_account(
    quota: &crate::account_quota::AccountQuotaState,
    account_id: &str,
    fence_observed_ms: i64,
    now_ms: i64,
    policy: &striatum_core::accounts::AllocatorPolicy,
) -> bool {
    let Some(entry) = latest_fresh_entry(quota, account_id, now_ms) else {
        return false;
    };
    let fetched = entry
        .fetched_at
        .as_deref()
        .and_then(crate::account_quota::parse_rfc3339_ms);
    if fetched.is_none_or(|fetched| fetched <= fence_observed_ms) {
        return false;
    }
    let Some(usage) = entry.usage.as_ref() else {
        return false;
    };
    let mut saw_window = false;
    for window in [
        usage.primary.as_ref(),
        usage.secondary.as_ref(),
        usage.tertiary.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        saw_window = true;
        if !window_has_headroom(window, policy) {
            return false;
        }
    }
    if let Some(extras) = usage.extra_rate_windows.as_ref() {
        for extra in extras {
            if let Some(window) = extra.window.as_ref() {
                saw_window = true;
                if !window_has_headroom(window, policy) {
                    return false;
                }
            }
        }
    }
    saw_window
}

/// A typed quota refusal contradicted by current headroom is not a measured
/// quota wall. Keep it as a non-quota refusal until a request proves recovery;
/// otherwise every telemetry refresh would immediately reopen a billing wall.
pub(crate) fn quota_refusal_contradicted(
    quota: &crate::account_quota::AccountQuotaState,
    credentials: &crate::broca_credentials::BrocaCredentials,
    credential: &str,
    now: i64,
    policy: &striatum_core::accounts::AllocatorPolicy,
) -> bool {
    let accounts = credentials.quota_account_ids(quota);
    credential_account_id(&accounts, credential).is_some_and(|account| {
        fresh_headroom_after_for_account(quota, &account, i64::MIN, now, policy)
    })
}

/// A reading from before a binding window's advertised reset cannot suppress
/// that reset's extra check. Another still-current binding window (including a
/// near-wall weekly reserve) continues to restrict the account.
pub(crate) fn current_quota_reading(
    quota: &crate::account_quota::AccountQuotaState,
    account: &str,
    now: i64,
    policy: &striatum_core::accounts::AllocatorPolicy,
) -> bool {
    let Some(entry) = latest_fresh_entry(quota, account, now) else {
        return false;
    };
    let Some(usage) = entry.usage.as_ref() else {
        return false;
    };
    let fetched = entry
        .fetched_at
        .as_deref()
        .and_then(crate::account_quota::parse_rfc3339_ms)
        .unwrap_or(i64::MIN);
    let windows = [
        usage.primary.as_ref(),
        usage.secondary.as_ref(),
        usage.tertiary.as_ref(),
    ]
    .into_iter()
    .flatten()
    .chain(
        usage
            .extra_rate_windows
            .iter()
            .flatten()
            .filter_map(|extra| extra.window.as_ref()),
    );
    let mut valid = false;
    let mut binding = false;
    for window in windows.filter(|window| {
        window.used_percent.is_finite() && (0.0..=100.0).contains(&window.used_percent)
    }) {
        valid = true;
        if !window_has_headroom(window, policy) {
            binding = true;
            if !window
                .resets_at
                .as_deref()
                .and_then(crate::account_quota::parse_rfc3339_ms)
                .is_some_and(|reset| reset <= now && fetched < reset)
            {
                return true;
            }
        }
    }
    valid && !binding
}

fn window_has_headroom(
    window: &striatum_core::quota::RateWindow,
    policy: &striatum_core::accounts::AllocatorPolicy,
) -> bool {
    let reserve = if window.window_minutes == Some(300) {
        policy.reserve_five_hour
    } else {
        policy.reserve_seven_day
    }
    .max(policy.soft_quota_remaining_percent.unwrap_or(0.0));
    window.used_percent.is_finite()
        && (0.0..QUOTA_HOLD_USED_THRESHOLD).contains(&window.used_percent)
        && 100.0 - window.used_percent > reserve
}

pub(crate) fn latest_reading_for_account<'a>(
    quota: &'a crate::account_quota::AccountQuotaState,
    account_id: &str,
) -> Option<&'a crate::account_quota::StoredReading> {
    quota
        .readings
        .readings
        .values()
        .filter(|reading| reading.account_id == account_id && reading.tombstoned_at_ms.is_none())
        .max_by_key(|reading| {
            reading
                .entry
                .get("fetchedAt")
                .and_then(serde_json::Value::as_str)
                .and_then(crate::account_quota::parse_rfc3339_ms)
        })
}

pub(crate) fn latest_fresh_entry(
    quota: &crate::account_quota::AccountQuotaState,
    account_id: &str,
    now_ms: i64,
) -> Option<crate::account_quota::AccountEntry> {
    let reading = latest_reading_for_account(quota, account_id)?;
    let entry: crate::account_quota::AccountEntry =
        serde_json::from_value(reading.entry.clone()).ok()?;
    if entry.error_class.is_some() || entry.error.is_some() {
        return None;
    }
    if let Some(stale) = entry
        .stale
        .as_ref()
        .and_then(|stale| stale.class.as_deref())
    {
        if stale != "upstream_failed" {
            return None;
        }
    }
    let fetched = entry
        .fetched_at
        .as_deref()
        .and_then(crate::account_quota::parse_rfc3339_ms)?;
    let age = now_ms.saturating_sub(fetched);
    // Fresh means routing would still place cold work on the reading's age:
    // inside the full (under `FULL_BAND_MS`) or discounted (under
    // `EXCLUDED_BAND_MS`) band from `account_quota`, not excluded and not
    // clock-invalid.
    if age < -crate::account_quota::FUTURE_SKEW_MS || age >= crate::account_quota::EXCLUDED_BAND_MS
    {
        return None;
    }
    Some(entry)
}

pub(crate) fn credential_account_id(
    accounts: &crate::broca_credentials::CredentialAccounts,
    credential_id: &str,
) -> Option<String> {
    accounts
        .get(credential_id)
        .map(|account| account.clone().unwrap_or_else(|| credential_id.to_owned()))
}

/// Stable link from refusal evidence to its ordinary account pool blocks.
/// Keep the credential evidence for diagnostics and paid-credit exclusion;
/// pool blocks supply the durable reset and normal headroom recovery rules.
pub(crate) fn quota_hold_prefix(fence: &CredentialEntitlementFence) -> String {
    format!(
        "credential-quota:{}:",
        serde_json::json!([fence.provider_id, fence.credential_id])
    )
}

pub(crate) fn clear_quota_holds(
    store: &SqliteRouteStateStore,
    fence: &CredentialEntitlementFence,
    reason: &str,
) -> Result<(), String> {
    let fence = fence.clone();
    let reason = reason.to_owned();
    let now = crate::account_selection::now_ms();
    crate::account_writer::repair(store, move |tx| {
        clear_quota_holds_in(tx, &fence, &reason, now)
    })
    .map_err(|error| error.to_string())
}

fn clear_quota_holds_in(
    tx: &crate::account_state::AccountCommand<'_>,
    fence: &CredentialEntitlementFence,
    reason: &str,
    now: i64,
) -> RouteStateResult<()> {
    let prefix = quota_hold_prefix(fence);
    for block in tx.active_pool_fences(&fence.provider_id)? {
        if block.evidence.fence_id.starts_with(&prefix) {
            tx.clear_held_fence(&block.evidence, now, reason)?;
        }
    }
    Ok(())
}

/// Lift a successful media probe's fence without leaving its observation transaction.
pub(crate) fn lift_after_success_in(
    tx: &crate::account_state::AccountCommand<'_>,
    provider: &str,
    credential: &str,
    now: i64,
    admitted_at: i64,
) -> RouteStateResult<bool> {
    if let Some(fence) = tx.credential_entitlement_fence(provider, credential)? {
        if fence.observed_at_ms >= admitted_at {
            return Ok(false);
        }
        if tx.active_pool_fences(provider)?.iter().any(|block| {
            block
                .evidence
                .fence_id
                .starts_with(&quota_hold_prefix(&fence))
        }) {
            return Ok(false);
        }
        clear_quota_holds_in(tx, &fence, "run_succeeded", now)?;
        tx.clear_held_entitlement_fence(&fence)?;
        return Ok(true);
    }
    Ok(false)
}

/// The vault account for `credential_id` from the quota state's roster, when
/// the roster lists it. Used by diagnostics, which has no vault view.
pub(crate) fn roster_account_id(
    quota: &crate::account_quota::AccountQuotaState,
    credential_id: &str,
) -> Option<String> {
    quota.roster.as_ref().and_then(|roster| {
        roster
            .identities
            .iter()
            .find(|identity| identity.credential_ids.iter().any(|id| id == credential_id))
            .map(|identity| identity.account_id.clone())
    })
}

/// Whether `account_id` has any non-tombstoned reading with quota windows.
pub(crate) fn has_quota_signal_for_account(
    quota: &crate::account_quota::AccountQuotaState,
    account_id: &str,
) -> bool {
    quota.readings.readings.values().any(|reading| {
        reading.account_id == account_id
            && reading.tombstoned_at_ms.is_none()
            && reading.entry.get("usage").is_some_and(|usage| {
                ["primary", "secondary", "tertiary"]
                    .iter()
                    .any(|slot| usage.get(slot).is_some_and(serde_json::Value::is_object))
                    || usage
                        .get("extraRateWindows")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|extras| {
                            extras.iter().any(|extra| {
                                extra
                                    .get("window")
                                    .is_some_and(serde_json::Value::is_object)
                            })
                        })
            })
    })
}

/// Store a reported refusal against the credential's current vault listing.
pub(crate) fn record(
    store: &SqliteRouteStateStore,
    credentials: &SharedBrocaCredentials,
    refusal: &CredentialRefusal,
    observed_at_ms: i64,
) -> Result<(), String> {
    let fingerprint = credentials.lock().ok().and_then(|guard| {
        match guard.listing_fingerprint(&refusal.credential_id) {
            ListingLookup::Listed(fingerprint) => Some(fingerprint),
            ListingLookup::Unknown | ListingLookup::Absent => None,
        }
    });
    crate::account_writer::record_credential_entitlement_fence(
        store,
        refusal,
        fingerprint.as_deref(),
        observed_at_ms,
    )
    .map_err(|e| e.to_string())?;
    eprintln!(
        "[striatum] credential entitlement fence recorded provider={} credential={} code={}",
        refusal.provider_id, refusal.credential_id, refusal.code
    );
    Ok(())
}

#[cfg(test)]
thread_local! {
    static BEFORE_LISTING_REPAIR: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn before_listing_repair_for_test(hook: impl FnOnce() + 'static) {
    BEFORE_LISTING_REPAIR.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

/// Read listing repairs first, then commit all changes for that listing in one
/// command. Eligibility and credential fingerprints describe the same listing,
/// so publishing them together avoids a second commit on the request path.
pub(crate) fn live_with_eligibility(
    store: &SqliteRouteStateStore,
    credentials: &SharedBrocaCredentials,
    reconcile_eligibility: bool,
) -> Result<Vec<CredentialEntitlementFence>, String> {
    let blocks = crate::account_state::published(store, crate::account_selection::now_ms())
        .map_err(|e| e.to_string())?;
    let fences = blocks
        .report_credential_refusals()
        .map_err(|e| e.to_string())?;
    let (lookups, listings) = match credentials.lock() {
        Ok(guard) => {
            let lookups: Vec<_> = fences
                .iter()
                .map(|fence| {
                    if fence.code == striatum_core::route_state_store::SIGNED_OUT_FENCE_CODE {
                        let inactive = guard
                            .listed_credentials()
                            .iter()
                            .any(|row| row.id == fence.credential_id && row.state != "active");
                        (guard.signed_out_fingerprint(&fence.credential_id), inactive)
                    } else {
                        (guard.listing_fingerprint(&fence.credential_id), false)
                    }
                })
                .collect();
            let listings =
                (reconcile_eligibility && guard.listing_version().is_some()).then(|| {
                    guard
                        .listed_credentials()
                        .iter()
                        .filter_map(|row| match guard.listing_fingerprint(&row.id) {
                            ListingLookup::Listed(fingerprint) => {
                                Some((row.id.clone(), fingerprint))
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                });
            (lookups, listings)
        }
        Err(_) => return Ok(fences),
    };
    let eligibility_changed = if let Some(listings) = &listings {
        let fingerprints: BTreeMap<_, _> = listings
            .iter()
            .map(|(id, fp)| (id.as_str(), fp.as_str()))
            .collect();
        blocks
            .account_eligibility()
            .map_err(|e| e.to_string())?
            .iter()
            .any(|fact| {
                fact.listing_fingerprint.is_none()
                    || fact.listing_fingerprint.as_deref()
                        != fingerprints.get(fact.credential_id.as_str()).copied()
            })
    } else {
        false
    };
    drop(blocks);
    enum ListingRepair {
        SignedOut(CredentialEntitlementFence, String),
        Adopt(String, String),
        Clear(CredentialEntitlementFence),
    }
    let mut repairs = Vec::new();
    let mut live = Vec::with_capacity(fences.len());
    for (mut fence, (lookup, inactive)) in fences.into_iter().zip(lookups) {
        let current = match lookup {
            ListingLookup::Unknown => {
                live.push(fence);
                continue;
            }
            ListingLookup::Absent => None,
            ListingLookup::Listed(fingerprint) => Some(fingerprint),
        };
        if inactive {
            if current != fence.listing_fingerprint {
                repairs.push(ListingRepair::SignedOut(
                    fence.clone(),
                    current.clone().unwrap(),
                ));
                fence.listing_fingerprint = current;
            }
            live.push(fence);
            continue;
        }
        match (fence.listing_fingerprint.as_deref(), current.as_deref()) {
            (Some(recorded), Some(current)) if recorded == current => live.push(fence),
            (None, Some(current)) => {
                repairs.push(ListingRepair::Adopt(
                    fence.credential_id.clone(),
                    current.to_owned(),
                ));
                fence.listing_fingerprint = Some(current.to_owned());
                live.push(fence);
            }
            _ => repairs.push(ListingRepair::Clear(fence)),
        }
    }
    if !repairs.is_empty() || eligibility_changed {
        #[cfg(test)]
        if let Some(hook) = BEFORE_LISTING_REPAIR.with(|slot| slot.borrow_mut().take()) {
            hook();
        }
        let now = crate::account_selection::now_ms();
        crate::account_writer::repair(store, move |tx| {
            for repair in &repairs {
                match repair {
                    ListingRepair::SignedOut(fence, fingerprint) => {
                        tx.update_signed_out_fingerprint(&fence, &fingerprint)?
                    }
                    ListingRepair::Adopt(credential, fingerprint) => {
                        tx.adopt_credential_entitlement_fingerprint(&credential, &fingerprint)?
                    }
                    ListingRepair::Clear(fence) => {
                        clear_quota_holds_in(tx, &fence, "vault_listing_changed", now)?;
                        tx.clear_credential_entitlement_fences(
                            &fence.credential_id,
                            fence.listing_fingerprint.as_deref(),
                        )?;
                    }
                }
            }
            if let Some(listings) = &listings {
                tx.reconcile_account_eligibility(listings)?;
            }
            Ok(())
        })
        .map_err(|e| e.to_string())?;
    }
    Ok(live)
}

/// Fences that survive the current vault listing. Unknown listings leave them
/// unchanged; a changed listing durably lifts them before this read returns.
pub(crate) fn live(
    store: &SqliteRouteStateStore,
    credentials: &SharedBrocaCredentials,
) -> Result<Vec<CredentialEntitlementFence>, String> {
    live_with_eligibility(store, credentials, false)
}

/// The explanation line a probed pick carries in its decision.
pub(crate) fn probe_note(provider_id: &str, credential_id: &str) -> String {
    format!(
        "fence probe: {provider_id} via {credential_id} is due for one scheduled recheck; refusal evidence remains until recovery",
    )
}

/// End every fence naming `credential_id`, as an operator does right after
/// topping it up. Returns the entitlement fences that were lifted, ordered by provider.
pub(crate) fn lift_for_operator(
    store: &SqliteRouteStateStore,
    credential_id: &str,
) -> Result<Vec<CredentialEntitlementFence>, String> {
    let route_fences =
        crate::account_writer::clear_all_account_route_fences_for_credential(store, credential_id)
            .map_err(|e| e.to_string())?;
    if route_fences > 0 {
        eprintln!(
            "[striatum] account route fences lifted credential={credential_id} count={route_fences}: operator lift after top-up"
        );
    }
    let lifted = crate::account_writer::clear_credential_entitlement_fences_for_credential(
        store,
        credential_id,
    )
    .map_err(|e| e.to_string())?;
    for fence in &lifted {
        clear_quota_holds(store, fence, "operator_lift")?;
        eprintln!(
            "[striatum] credential entitlement fence lifted provider={} credential={}: operator lift after top-up",
            fence.provider_id, fence.credential_id
        );
    }
    Ok(lifted)
}

/// The fenced credentials that make `provider_id`'s route unselectable: every
/// serving identity is fenced. Empty when the provider has no fence or another
/// credential still serves it. With no known serving identity (no fresh vault
/// listing), nothing shows that another credential serves the provider, so
/// every fenced credential of the provider is named.
pub(crate) fn unentitled(
    fences: &[CredentialEntitlementFence],
    provider_id: &str,
    identities: &[(String, Option<String>)],
) -> Vec<String> {
    let fenced: Vec<&str> = fences
        .iter()
        .filter(|fence| fence.provider_id == provider_id)
        .map(|fence| fence.credential_id.as_str())
        .collect();
    if fenced.is_empty() {
        return Vec::new();
    }
    if identities.is_empty() {
        return fenced.into_iter().map(str::to_string).collect();
    }
    if identities
        .iter()
        .all(|(credential, _)| fenced.contains(&credential.as_str()))
    {
        return identities
            .iter()
            .map(|(credential, _)| credential.clone())
            .collect();
    }
    Vec::new()
}

/// The serving identities a launch on `provider_id` may still use.
#[cfg(test)]
pub(crate) fn entitled<'a>(
    fences: &[CredentialEntitlementFence],
    provider_id: &str,
    identities: &'a [(String, Option<String>)],
) -> Vec<&'a (String, Option<String>)> {
    let fenced: HashSet<&str> = fences
        .iter()
        .filter(|fence| fence.provider_id == provider_id)
        .map(|fence| fence.credential_id.as_str())
        .collect();
    identities
        .iter()
        .filter(|(credential, _)| !fenced.contains(credential.as_str()))
        .collect()
}

/// The rejection reason for a route whose serving credentials `refused` are all
/// fenced: `credential_billing_exhausted` when every one of them ran out of
/// balance, `no_eligible_account` for sign-in loss alone, otherwise
/// `credential_unentitled`. Each fence's reason follows the
/// provider code it was reported with; a code routing does not know reads as
/// an entitlement refusal, since it was reported as a permanent refusal of the
/// credential.
pub(crate) fn rejection_reason(
    fences: &[CredentialEntitlementFence],
    provider_id: &str,
    refused: &[String],
) -> &'static str {
    // Sign-in loss is an eligibility problem, not a revoked key or missing
    // provider entitlement. Do not label it as a credential refusal in findings.
    if !refused.is_empty()
        && refused.iter().all(|credential| {
            fences.iter().any(|fence| {
                fence.provider_id == provider_id
                    && &fence.credential_id == credential
                    && fence.code == striatum_core::route_state_store::SIGNED_OUT_FENCE_CODE
            })
        })
    {
        return "no_eligible_account";
    }
    let all_billing = refused.iter().all(|credential| {
        fences.iter().any(|fence| {
            fence.provider_id == provider_id
                && &fence.credential_id == credential
                && is_billing_code(&fence.code)
        })
    });
    if all_billing {
        REASON_CREDENTIAL_BILLING_EXHAUSTED
    } else {
        REASON_CREDENTIAL_UNENTITLED
    }
}

/// Broca's provider codes for a credential out of paid balance: `billing_exhausted`,
/// `insufficient_quota` from older Broca releases, and `credit_insufficient` (a
/// balance too small for the request, OpenRouter's 402).
pub(crate) fn is_billing_code(code: &str) -> bool {
    matches!(
        code,
        "billing_exhausted" | "insufficient_quota" | "credit_insufficient"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_signals_disable_only_billing_probes() {
        for (code, probes_with_quota) in [
            ("billing_exhausted", false),
            ("insufficient_quota", false),
            ("credit_insufficient", false),
            ("ENTITLEMENT_ERROR", true),
            ("client_attestation_denied", true),
        ] {
            for has_signal in [false, true] {
                assert!(
                    !probe_allowed(code, has_signal, FENCE_PROBE_RECHECK_MS),
                    "{code}, signal={has_signal}"
                );
                assert_eq!(
                    probe_allowed(code, has_signal, FENCE_PROBE_RECHECK_MS + 1),
                    if has_signal { probes_with_quota } else { true },
                    "{code}, signal={has_signal}"
                );
            }
        }
    }

    fn fence(provider: &str, credential: &str) -> CredentialEntitlementFence {
        CredentialEntitlementFence {
            provider_id: provider.into(),
            credential_id: credential.into(),
            code: "ENTITLEMENT_ERROR".into(),
            listing_fingerprint: Some("fp".into()),
            observed_at_ms: 1,
        }
    }

    #[test]
    fn a_route_is_unentitled_only_when_every_serving_credential_is_fenced() {
        let fences = vec![fence("cline-pass", "apikey:cline-pass")];
        let sole = vec![("apikey:cline-pass".to_string(), None)];
        assert_eq!(
            unentitled(&fences, "cline-pass", &sole),
            vec!["apikey:cline-pass".to_string()]
        );
        let two = vec![
            ("apikey:cline-pass".to_string(), None),
            ("apikey:cline-pass:team".to_string(), None),
        ];
        assert!(unentitled(&fences, "cline-pass", &two).is_empty());
        assert_eq!(
            entitled(&fences, "cline-pass", &two)
                .into_iter()
                .map(|(credential, _)| credential.as_str())
                .collect::<Vec<_>>(),
            vec!["apikey:cline-pass:team"]
        );
        // Another provider served by the same credential is not fenced.
        assert!(unentitled(&fences, "other", &sole).is_empty());
        // No fresh roster: nothing shows another credential serves the provider.
        assert_eq!(
            unentitled(&fences, "cline-pass", &[]),
            vec!["apikey:cline-pass".to_string()]
        );
    }

    #[test]
    fn the_rejection_reason_follows_the_refusal_codes() {
        let mut billing = fence("p", "apikey:p:a");
        billing.code = "billing_exhausted".into();
        let entitlement = fence("p", "apikey:p:b");
        let a = vec!["apikey:p:a".to_string()];
        let both = vec!["apikey:p:a".to_string(), "apikey:p:b".to_string()];
        let fences = vec![billing, entitlement];
        assert_eq!(
            rejection_reason(&fences, "p", &a),
            REASON_CREDENTIAL_BILLING_EXHAUSTED
        );
        assert_eq!(
            rejection_reason(&fences, "p", &both),
            REASON_CREDENTIAL_UNENTITLED
        );
        assert_eq!(
            rejection_reason(&fences, "p", &["apikey:p:b".to_string()]),
            REASON_CREDENTIAL_UNENTITLED
        );
    }
}
