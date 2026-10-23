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
//! Explicit pins are not fenced.
//!
//! Waiting fixes neither refusal, so a fence has no expiry. It ends when the
//! credential's vault listing (provider ids, plan tier or state) no longer matches
//! the listing the refusal was recorded against, because changing the listing is
//! what an operator does to fix it.

use std::collections::HashSet;

use prefrontal_routing_core::route_state_store::{CredentialEntitlementFence, CredentialRefusal};
use prefrontal_routing_core::{
    CredentialRefusalMark, ModelFact, ModelRouteStateSnapshot, REASON_CREDENTIAL_BILLING_EXHAUSTED,
    REASON_CREDENTIAL_UNENTITLED,
};
use prefrontal_routing_store::SqliteRouteStateStore;

use crate::broca_credentials::{ListingLookup, SharedBrocaCredentials};

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
fn is_billing_code(code: &str) -> bool {
    matches!(
        code,
        "billing_exhausted" | "insufficient_quota" | "credit_insufficient"
    )
}

/// Drop every credential mark from a snapshot an explicit pin is judged on:
/// a pin keeps the behaviour it had before credential fences existed.
pub(crate) fn clear(snapshot: &mut ModelRouteStateSnapshot) {
    for state in snapshot
        .by_route
        .values_mut()
        .chain(snapshot.by_capacity.values_mut())
    {
        state.credential_refusal = None;
    }
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
