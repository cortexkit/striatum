//! Persist the vault's non-secret wire metadata, not resolved credentials.

use cortexkit_claustrum_client::{decode_list_scoped_reply, ScopedCredentialListing};
use serde_json::{json, Value};

pub(super) fn restore_credential_identities(
    row: &striatum_store::PersistedVaultRoster,
) -> std::collections::BTreeMap<String, super::CredentialIdentity> {
    let Ok(roster) = serde_json::from_str::<Value>(&row.roster_json) else {
        return Default::default();
    };
    let Some(listing) = roster.get("listing") else {
        return Default::default();
    };
    match decode_list_scoped_reply(json!({"result": listing})) {
        Ok(listing)
            if listing.view == row.view
                && listing.undecodable_credentials.is_empty()
                && listing.undecodable_grants.is_empty() =>
        {
            super::credential_identities(&listing)
        }
        _ => Default::default(),
    }
}

// The client listing types do not implement serde. Keep the published wire shape
// so restoration uses the client's row validation rather than a second decoder.
pub(super) fn encode(listing: &ScopedCredentialListing) -> Value {
    json!({
        "view": listing.view,
        "credentials": listing.credentials.iter().map(|row| {
            let mut value = json!({
                "id": row.id, "categories": row.categories, "type": row.credential_type,
                "serves": row.serves, "provider_ids": row.provider_ids,
                "auth_method": row.auth_method, "refresh_adapter": row.refresh_adapter,
                "state": row.state, "record_version": row.record_version,
                "operations": row.operations, "account_id": row.account_id,
                "email": row.email, "org_name": row.org_name, "plan_tier": row.plan_tier,
                "plan_tier_source": row.plan_tier_source,
            });
            if let Some(condition) = &row.condition {
                value["condition"] = condition_json(condition);
            }
            value
        }).collect::<Vec<_>>(),
        "grant_tuples": listing.grant_tuples.iter().map(|grant| json!({
            "selector_kind": grant.selector_kind, "selector": grant.selector,
            "operation": grant.operation,
        })).collect::<Vec<_>>(),
        "undecodable_credentials": listing.undecodable_credentials,
        "undecodable_grants": listing.undecodable_grants,
    })
}

pub(crate) fn condition_json(
    condition: &cortexkit_claustrum_client::ListedCredentialCondition,
) -> Value {
    json!({"reason": condition.reason, "since_ms": condition.since_ms})
}

pub(super) fn persisted_view(row: striatum_store::PersistedVaultRoster) -> Option<String> {
    let roster: Value = serde_json::from_str(&row.roster_json).ok()?;
    // An older router may have saved this digest without its conditions. Let
    // the first live read replace that snapshot, even when its digest matches.
    (roster["listing_includes_conditions"] == true && roster["listing"].is_object())
        .then_some(row.view)
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
    fn login_condition_producer_replies_decode_and_preserve_the_condition() {
        // Claustrum's list_scoped replies, not its connector or peer rows.
        let vectors: Value = serde_json::from_str(include_str!(
            "../../fixtures/claustrum-list-scoped-conditions.json"
        ))
        .unwrap();
        for (case, tier) in [
            ("listing_condition", None),
            ("listing_condition_with_plan_tier", Some("pro")),
            ("listing_unconditioned", None),
        ] {
            let decoded = decode_list_scoped_reply(vectors[case].clone()).unwrap();
            assert!(decoded.undecodable_credentials.is_empty(), "{case}");
            assert_eq!(decoded.credentials.len(), 1, "{case}");
            let row = &decoded.credentials[0];
            assert_eq!(row.id, "oauth:antigravity:condition-fixture");
            assert_eq!(row.plan_tier.as_deref(), tier, "{case}");
            assert_eq!(
                encode(&decoded)["credentials"][0].get("condition"),
                vectors[case]["result"]["credentials"][0].get("condition"),
                "{case}"
            );
        }
    }

    #[test]
    fn login_condition_first_live_listing_replaces_an_old_snapshot_with_the_same_view() {
        let listing = ScopedCredentialListing {
            view: "same-digest".into(),
            ..Default::default()
        };
        let row = super::super::apply_roster_listing(&mut Default::default(), &listing, 1).unwrap();
        let mut old_row = row.clone();
        let mut old_wire: Value = serde_json::from_str(&row.roster_json).unwrap();
        old_wire
            .as_object_mut()
            .unwrap()
            .remove("listing_includes_conditions");
        old_row.roster_json = old_wire.to_string();
        assert_eq!(persisted_view(row).as_deref(), Some("same-digest"));
        assert_eq!(persisted_view(old_row), None);
    }

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
