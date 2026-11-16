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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_upgrade_preserves_listing_wire_bytes() {
        // These are the persisted metadata fields read by routing before it serves
        // requests. A client upgrade must not silently discard or normalize them.
        let wire = json!({
            "view": "c2NvcGVkLXZpZXc=",
            "credentials": [{
                "id": "claude-login", "categories": ["llm-provider", "llm-gateway"],
                "type": "oauth", "serves": ["bearer"], "provider_ids": ["anthropic"],
                "auth_method": "subscription", "refresh_adapter": "claude",
                "state": "needs_reauth", "record_version": 42,
                "operations": ["get", "list"], "account_id": "account-1",
                "email": "person@example.test", "org_name": "Example",
                "plan_tier": "max", "plan_tier_source": "provider"
            }, {
                "id": "gateway-key", "categories": ["llm-gateway"],
                "type": "api_key", "serves": ["api_key"], "provider_ids": ["company"],
                "auth_method": null, "refresh_adapter": null,
                "state": "active", "record_version": 1, "operations": ["get"],
                "account_id": null, "email": null, "org_name": null,
                "plan_tier": null, "plan_tier_source": null
            }],
            "grant_tuples": [{"selector_kind": "category", "selector": "llm-provider", "operation": "list"},
                {"selector_kind": "category", "selector": "llm-gateway", "operation": "list"}],
            "undecodable_credentials": [], "undecodable_grants": []
        });
        let decoded = decode_list_scoped_reply(json!({"result": wire})).unwrap();
        assert!(
            decoded.undecodable_credentials.is_empty(),
            "client rejected stored credential metadata: {:?}",
            decoded.undecodable_credentials
        );
        assert!(
            decoded.undecodable_grants.is_empty(),
            "client rejected stored grants: {:?}",
            decoded.undecodable_grants
        );
        assert_eq!(
            serde_json::to_vec(&encode(&decoded)).unwrap(),
            serde_json::to_vec(&wire).unwrap(),
            "claustrum client changed routing's persisted list_scoped bytes"
        );
        let restored = decode_list_scoped_reply(json!({"result": encode(&decoded)})).unwrap();
        assert_eq!(encode(&restored), wire);
    }
}
