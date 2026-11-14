//! Persist the vault's non-secret wire metadata, not resolved credentials.

use cortexkit_claustrum_client::{decode_list_scoped_reply, ScopedCredentialListing};
use serde_json::{json, Value};

// The client listing types do not implement serde. Keep the published wire shape
// so restoration uses the client's row validation rather than a second decoder.
pub(super) fn encode(listing: &ScopedCredentialListing) -> Value {
    json!({
        "view": listing.view,
        "credentials": listing.credentials.iter().map(|row| json!({
            "id": row.id, "categories": row.categories, "type": row.credential_type,
            "serves": row.serves, "provider_ids": row.provider_ids,
            "auth_method": row.auth_method, "refresh_adapter": row.refresh_adapter,
            "state": row.state, "record_version": row.record_version,
            "operations": row.operations, "account_id": row.account_id,
            "email": row.email, "org_name": row.org_name, "plan_tier": row.plan_tier,
            "plan_tier_source": row.plan_tier_source,
        })).collect::<Vec<_>>(),
        "grant_tuples": listing.grant_tuples.iter().map(|grant| json!({
            "selector_kind": grant.selector_kind, "selector": grant.selector,
            "operation": grant.operation,
        })).collect::<Vec<_>>(),
        "undecodable_credentials": listing.undecodable_credentials,
        "undecodable_grants": listing.undecodable_grants,
    })
}

pub(crate) fn restore_credential_listing(
    store: &striatum_store::SqliteRouteStateStore,
    credentials: &mut crate::BrocaCredentials,
) {
    let row = match store.account_vault_roster() {
        Ok(Some(row)) => row,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(%error, "persisted credential listing unreadable");
            return;
        }
    };
    let Ok(roster) = serde_json::from_str::<Value>(&row.roster_json) else {
        return;
    };
    // Identity-only snapshots from older routers cannot establish API-key membership.
    let Some(value) = roster.get("listing") else {
        return;
    };
    match decode_list_scoped_reply(json!({"result": value})) {
        Ok(mut listing) if listing.view == row.view => {
            // Corrupt persisted rows must not turn a partial decode into an
            // authoritative empty roster and a false sign-in refusal.
            if !listing.undecodable_credentials.is_empty() || !listing.undecodable_grants.is_empty()
            {
                tracing::warn!(
                    "persisted credential listing has malformed rows; awaiting a live listing"
                );
                return;
            }
            for (key, errors) in [
                (
                    "undecodable_credentials",
                    &mut listing.undecodable_credentials,
                ),
                ("undecodable_grants", &mut listing.undecodable_grants),
            ] {
                errors.extend(
                    value[key]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(str::to_owned),
                );
            }
            credentials.apply_listing(&listing, row.fetched_at.max(0) as u64);
        }
        _ => tracing::warn!("persisted credential listing invalid; awaiting a live listing"),
    }
}
