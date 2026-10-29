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
//! the probe stamps the fence fresh so no second probe follows within the
//! interval, a successful outcome ends the fence, and a billing or entitlement
//! failure renews it for another interval. An operator lifts every fence on a
//! named credential at once, for use right after topping it up.
//!
//! A subscription is a quota: its Insula reading already names the exhausted
//! window and its reset, and a fresh reading with headroom lifts the hold.
//! A billing- or entitlement-class refusal for such a credential is therefore
//! a quota hold until that reset, never a billing fence with a timer. See
//! [`probe_allowed`]: only a credential with no quota reading at all (a
//! pay-per-token credential) may probe.

use std::collections::HashSet;

use prefrontal_routing_core::route_state_store::{CredentialEntitlementFence, CredentialRefusal};
use prefrontal_routing_core::{
    CredentialRefusalMark, ModelFact, ModelRouteStateSnapshot, REASON_CREDENTIAL_BILLING_EXHAUSTED,
    REASON_CREDENTIAL_UNENTITLED,
};
use prefrontal_routing_store::SqliteRouteStateStore;

use crate::broca_credentials::{ListingLookup, SharedBrocaCredentials};

/// How long a billing or entitlement fence holds before routing admits one
/// probe request on the fenced credential: six hours. Topping up a balance
/// does not change the credential's vault listing, so without a recheck the
/// fence would hold until the listing changed for an unrelated reason.
///
/// This interval applies only to credentials with no quota signal (see
/// [`probe_allowed`]). A subscription credential always has an Insula reading
/// for its account, and that reading already carries the reset and lifts on
/// headroom, so a timer would only spend a real request known to fail.
pub(crate) const FENCE_PROBE_RECHECK_MS: i64 = 6 * 60 * 60 * 1000;

/// A window reading at or above this used percent counts as exhausted for
/// turning a billing- or entitlement-class refusal into a quota hold. The
/// one-point margin catches a producer that reports 99.x for a full window
/// while keeping the hold to genuinely full windows.
pub(crate) const QUOTA_HOLD_USED_THRESHOLD: f64 = 99.0;

/// Whether a timed probe may be admitted for a fence of this age.
///
/// A pay-per-token credential (for example an API key out of credit) has no
/// Insula quota reading: topping its balance up changes no vault listing and
/// publishes no reading, so without a timed recheck the fence would hold
/// until the listing changed for an unrelated reason. A subscription
/// credential always has a quota reading for its account, and that reading
/// already says when the window resets and lifts the hold on headroom: a
/// timed probe would spend a real request that is known to fail. So only a
/// fence whose credential has no quota signal may probe, and only once it is
/// older than [`FENCE_PROBE_RECHECK_MS`].
pub(crate) fn probe_allowed(has_quota_signal: bool, age_ms: i64) -> bool {
    !has_quota_signal && age_ms > FENCE_PROBE_RECHECK_MS
}

/// Whether routing holds any quota reading for `credential_id`'s account.
/// The join is by vault account id only, never by provider name: Insula's
/// provider spelling (for example the usage feed) and routing's provider id
/// (the catalog vendor) need not match. Any non-tombstoned reading counts as
/// a signal, even a stale one: a subscription account keeps its signal while
/// its reading ages, and a pay-per-token credential never gains one.
pub(crate) fn has_quota_signal(
    quota: &crate::account_quota::AccountQuotaState,
    credentials: &crate::broca_credentials::BrocaCredentials,
    credential_id: &str,
) -> bool {
    let Some(account_id) = credential_account_id(credentials, credential_id) else {
        return false;
    };
    has_quota_signal_for_account(quota, &account_id)
}

/// The reset a quota hold on `credential_id` runs until, when its latest
/// fresh reading shows an exhausted window (at or near 100%) with a future
/// reset. Returns the latest such reset, so a run-reported reset and a
/// reading reset combine by taking the later. `None` when the credential has
/// no reading, the latest reading is not fresh, or no exhausted window names
/// a future reset: the refusal is not a quota hold.
pub(crate) fn exhausted_quota_reset(
    quota: &crate::account_quota::AccountQuotaState,
    credentials: &crate::broca_credentials::BrocaCredentials,
    credential_id: &str,
    now_ms: i64,
) -> Option<i64> {
    let account_id = credential_account_id(credentials, credential_id)?;
    exhausted_quota_reset_for_account(quota, &account_id, now_ms)
}

/// Latest fresh exhausted reset for one vault account, by account id.
/// Fresh means the reading's producer date is inside the full (under 5 minutes)
/// or discounted (under 20 minutes) age band defined in `account_quota`, with
/// no entry error and no unrecognized stale class. Exhausted means at or above
/// [`QUOTA_HOLD_USED_THRESHOLD`] with a future reset; the latest such reset
/// wins.
pub(crate) fn exhausted_quota_reset_for_account(
    quota: &crate::account_quota::AccountQuotaState,
    account_id: &str,
    now_ms: i64,
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
        if window.used_percent.is_finite() && window.used_percent >= QUOTA_HOLD_USED_THRESHOLD {
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

/// Whether a fresh reading with headroom arrived after `fence_observed_ms`
/// for `account_id`: no window at or above the hold threshold. A quota hold
/// lifts on such a reading; an entitlement fence made while the reading
/// already showed headroom stays, because the later reading is not newer
/// than the fence.
pub(crate) fn fresh_headroom_after_for_account(
    quota: &crate::account_quota::AccountQuotaState,
    account_id: &str,
    fence_observed_ms: i64,
    now_ms: i64,
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
        if !window.used_percent.is_finite() || window.used_percent >= QUOTA_HOLD_USED_THRESHOLD {
            return false;
        }
    }
    if let Some(extras) = usage.extra_rate_windows.as_ref() {
        for extra in extras {
            if let Some(window) = extra.window.as_ref() {
                saw_window = true;
                if !window.used_percent.is_finite()
                    || window.used_percent >= QUOTA_HOLD_USED_THRESHOLD
                {
                    return false;
                }
            }
        }
    }
    saw_window
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

fn latest_fresh_entry(
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
    credentials: &crate::broca_credentials::BrocaCredentials,
    credential_id: &str,
) -> Option<String> {
    credentials
        .listed_credentials()
        .iter()
        .find(|row| row.id == credential_id)
        .map(|row| row.account_id.clone().unwrap_or_else(|| row.id.clone()))
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

fn clear_quota_holds(
    store: &SqliteRouteStateStore,
    fence: &CredentialEntitlementFence,
    reason: &str,
) -> Result<(), String> {
    let prefix = quota_hold_prefix(fence);
    store
        .account_transaction(|tx| {
            for block in tx.active_pool_fences(&fence.provider_id)? {
                if block.evidence.fence_id.starts_with(&prefix) {
                    tx.clear_fence(
                        &block.evidence.fence_id,
                        crate::account_selection::now_ms(),
                        reason,
                    )?;
                }
            }
            Ok(())
        })
        .map_err(|error| error.to_string())
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

/// Whether `account_id` has any non-tombstoned quota reading.
pub(crate) fn has_quota_signal_for_account(
    quota: &crate::account_quota::AccountQuotaState,
    account_id: &str,
) -> bool {
    quota
        .readings
        .readings
        .values()
        .any(|reading| reading.account_id == account_id && reading.tombstoned_at_ms.is_none())
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
    store
        .record_credential_entitlement_fence(refusal, fingerprint.as_deref(), observed_at_ms)
        .map_err(|e| e.to_string())?;
    eprintln!(
        "[prefrontal-routing] credential entitlement fence recorded provider={} credential={} code={}",
        refusal.provider_id, refusal.credential_id, refusal.code
    );
    Ok(())
}

/// The fences still in force. A fence whose credential's listing changed since
/// the refusal is deleted here; one recorded before routing knew the listing
/// takes the first listing it is seen against. With no listing read yet,
/// every fence stays.
pub(crate) fn live(
    store: &SqliteRouteStateStore,
    credentials: &SharedBrocaCredentials,
) -> Result<Vec<CredentialEntitlementFence>, String> {
    let fences = store
        .credential_entitlement_fences()
        .map_err(|e| e.to_string())?;
    if fences.is_empty() {
        return Ok(fences);
    }
    let lookups: Vec<ListingLookup> = match credentials.lock() {
        Ok(guard) => fences
            .iter()
            .map(|fence| guard.listing_fingerprint(&fence.credential_id))
            .collect(),
        Err(_) => return Ok(fences),
    };
    let mut live = Vec::with_capacity(fences.len());
    for (mut fence, lookup) in fences.into_iter().zip(lookups) {
        let current = match lookup {
            ListingLookup::Unknown => {
                live.push(fence);
                continue;
            }
            ListingLookup::Absent => None,
            ListingLookup::Listed(fingerprint) => Some(fingerprint),
        };
        match (fence.listing_fingerprint.as_deref(), current.as_deref()) {
            (Some(recorded), Some(current)) if recorded == current => live.push(fence),
            (None, Some(current)) => {
                store
                    .adopt_credential_entitlement_fingerprint(&fence.credential_id, current)
                    .map_err(|e| e.to_string())?;
                fence.listing_fingerprint = Some(current.to_string());
                live.push(fence);
            }
            // The listing changed (or no longer names the credential): lift.
            _ => {
                clear_quota_holds(store, &fence, "vault_listing_changed")?;
                store
                    .clear_credential_entitlement_fences(
                        &fence.credential_id,
                        fence.listing_fingerprint.as_deref(),
                    )
                    .map_err(|e| e.to_string())?;
                eprintln!(
                    "[prefrontal-routing] credential entitlement fence lifted provider={} credential={}: its vault listing changed",
                    fence.provider_id, fence.credential_id
                );
            }
        }
    }
    Ok(live)
}

/// A fence whose refusal is older than the recheck interval: the next
/// account admission may reserve one probe request on it and stamp it fresh.
fn is_stale(fence: &CredentialEntitlementFence, now_ms: i64) -> bool {
    probe_allowed(false, now_ms.saturating_sub(fence.observed_at_ms))
}

/// Split live fences into fresh restrictions and potential account-admission
/// probes. Every fence counts as billing or entitlement: the reason
/// a route refusal carries is one of the two, so age alone decides. This
/// age-only split is kept for callers with no quota view. Account selection
/// checks [`probe_allowed`] with the credential's quota signal instead.
pub(crate) fn split(
    fences: &[CredentialEntitlementFence],
    now_ms: i64,
) -> (
    Vec<CredentialEntitlementFence>,
    Vec<CredentialEntitlementFence>,
) {
    let mut enforcing = Vec::new();
    let mut stale = Vec::new();
    for fence in fences {
        if is_stale(fence, now_ms) {
            stale.push(fence.clone());
        } else {
            enforcing.push(fence.clone());
        }
    }
    (enforcing, stale)
}

/// The explanation line a probed pick carries in its decision.
pub(crate) fn probe_note(provider_id: &str, credential_id: &str) -> String {
    format!(
        "fence probe: {provider_id} via {credential_id} is past the 6h entitlement recheck, admitting one probe request",
    )
}

/// The stale-fenced credentials a probe of `provider_id` may launch on: every
/// serving identity is fenced, but at least one only by a stale fence, so the
/// route is selectable only as a probe. Empty when the route was never fenced
/// or is still fenced by a fresh refusal.
pub(crate) fn probe_credentials(
    full: &[CredentialEntitlementFence],
    enforcing: &[CredentialEntitlementFence],
    provider_id: &str,
    identities: &[(String, Option<String>)],
) -> Vec<String> {
    if unentitled(full, provider_id, identities).is_empty() {
        return Vec::new();
    }
    let fresh: HashSet<&str> = enforcing
        .iter()
        .filter(|fence| fence.provider_id == provider_id)
        .map(|fence| fence.credential_id.as_str())
        .collect();
    identities
        .iter()
        .filter(|(credential, _)| !fresh.contains(credential.as_str()))
        .map(|(credential, _)| credential.clone())
        .collect()
}

/// End the fence on the (provider, credential) pair a run just succeeded on.
/// True when a fence was lifted.
pub(crate) fn lift_after_success(
    store: &SqliteRouteStateStore,
    provider_id: &str,
    credential_id: &str,
) -> Result<bool, String> {
    for fence in store
        .credential_entitlement_fences()
        .map_err(|error| error.to_string())?
        .iter()
        .filter(|fence| fence.provider_id == provider_id && fence.credential_id == credential_id)
    {
        clear_quota_holds(store, fence, "run_succeeded")?;
    }
    let lifted = store
        .clear_credential_entitlement_fence(provider_id, credential_id)
        .map_err(|e| e.to_string())?;
    if lifted > 0 {
        eprintln!(
            "[prefrontal-routing] credential entitlement fence lifted provider={provider_id} credential={credential_id}: the probe run succeeded",
        );
    }
    Ok(lifted > 0)
}

/// End every fence naming `credential_id`, as an operator does right after
/// topping it up. Returns the entitlement fences that were lifted, ordered by provider.
pub(crate) fn lift_for_operator(
    store: &SqliteRouteStateStore,
    credential_id: &str,
) -> Result<Vec<CredentialEntitlementFence>, String> {
    let route_fences = store
        .clear_account_route_fences_for_credential(credential_id)
        .map_err(|e| e.to_string())?;
    if route_fences > 0 {
        eprintln!(
            "[prefrontal-routing] account route fences lifted credential={credential_id} count={route_fences}: operator lift after top-up"
        );
    }
    let lifted = store
        .clear_credential_entitlement_fences_for_credential(credential_id)
        .map_err(|e| e.to_string())?;
    for fence in &lifted {
        clear_quota_holds(store, fence, "operator_lift")?;
        eprintln!(
            "[prefrontal-routing] credential entitlement fence lifted provider={} credential={}: operator lift after top-up",
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

/// Mark every catalog route whose serving credentials are all fenced, so
/// admission rejects it as `credential_unentitled`. The mark is set on the
/// model's capacity key, which covers every variant of the model.
pub(crate) fn apply(
    snapshot: &mut ModelRouteStateSnapshot,
    facts: &[ModelFact],
    fences: &[CredentialEntitlementFence],
    credentials: &SharedBrocaCredentials,
) {
    if fences.is_empty() {
        return;
    }
    let Ok(guard) = credentials.lock() else {
        return;
    };
    for fact in facts {
        if !fences
            .iter()
            .any(|fence| fence.provider_id == fact.provider_id)
        {
            continue;
        }
        let refused = unentitled(fences, &fact.provider_id, &guard.serving_identities(fact));
        if refused.is_empty() {
            continue;
        }
        snapshot
            .by_capacity
            .entry(prefrontal_routing_core::make_capacity_key(
                &fact.provider_id,
                &fact.model_id,
            ))
            .or_default()
            .credential_refusal = Some(CredentialRefusalMark {
            reason: rejection_reason(fences, &fact.provider_id, &refused),
            credential_ids: refused,
        });
    }
}

/// The rejection reason for a route whose serving credentials `refused` are all
/// fenced: `credential_billing_exhausted` when every one of them ran out of
/// balance, otherwise `credential_unentitled`. Each fence's reason follows the
/// provider code it was reported with; a code routing does not know reads as
/// an entitlement refusal, since it was reported as a permanent refusal of the
/// credential.
pub(crate) fn rejection_reason(
    fences: &[CredentialEntitlementFence],
    provider_id: &str,
    refused: &[String],
) -> &'static str {
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
