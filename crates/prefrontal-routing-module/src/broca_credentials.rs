//! Broca reachability from Claustrum's non-secret credential metadata.
//!
//! `serves` names model vendors; only the operator's `provider_ids` mapping
//! names Fusiform providers. Older vaults omit the mapping and upgraded vaults
//! initially emit empty mappings, so either shape requires a catalog fallback.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use prefrontal_ckcred_client::ScopedCredentialListing;
use prefrontal_routing_core::{make_route_key, ModelFact};
use subc_protocol::session::{HealthReport, HealthStatus};

/// The same maximum age as the quota snapshot: a failed poll may keep the last
/// good answer briefly, but cannot assert reachability indefinitely.
const MAX_AGE_MS: u64 = 15 * 60 * 1000;

#[derive(Debug, Clone, Default)]
pub struct BrocaCredentials {
    view: Option<String>,
    reached_at_ms: Option<u64>,
    providers: HashSet<String>,
    active: usize,
    unmapped: usize,
    unreadable: usize,
    last_error: Option<String>,
}

pub type SharedBrocaCredentials = Arc<Mutex<BrocaCredentials>>;

impl BrocaCredentials {
    /// Every successful poll renews freshness. Only a changed view rebuilds the
    /// join, even if tokens refreshed or the previous call failed.
    pub fn apply_listing(&mut self, listing: &ScopedCredentialListing, at_ms: u64) -> bool {
        self.reached_at_ms = Some(at_ms);
        self.last_error = None;
        if self.view.as_deref() == Some(&listing.view) {
            return false;
        }
        self.view = Some(listing.view.clone());
        self.providers.clear();
        self.active = 0;
        self.unmapped = 0;
        self.unreadable = listing.undecodable_credentials.len();
        for credential in &listing.credentials {
            if credential.state != "active"
                || !credential
                    .categories
                    .iter()
                    .any(|category| category == "llm-provider")
            {
                continue;
            }
            self.active += 1;
            if credential.provider_ids.is_empty() {
                self.unmapped += 1;
            }
            self.providers
                .extend(credential.provider_ids.iter().cloned());
        }
        true
    }

    pub fn note_failure(&mut self, error: impl ToString) {
        self.last_error = Some(error.to_string());
    }

    fn fallback_reason(&self, now_ms: u64) -> Option<String> {
        let Some(reached_at_ms) = self.reached_at_ms else {
            return Some("broca reachability unknown: credential listing unavailable".into());
        };
        if now_ms.saturating_sub(reached_at_ms) > MAX_AGE_MS {
            return Some(
                "broca reachability unknown: credential listing stale (over 15 minutes)".into(),
            );
        }
        if self.unmapped > 0 {
            return Some(format!(
                "broca reachability: {} of {} credentials unmapped",
                self.unmapped, self.active
            ));
        }
        None
    }

    pub fn health_report(&self, now_ms: u64) -> HealthReport {
        let mut report = HealthReport::ok();
        report.detail = self.fallback_reason(now_ms).or_else(|| {
            (self.unreadable > 0).then(|| {
                format!(
                    "broca reachability: {} unreadable credential rows skipped",
                    self.unreadable
                )
            })
        });
        if report.detail.is_some() {
            report.status = HealthStatus::Degraded;
        }
        report.metrics = Some(serde_json::json!({
            "activeCredentials": self.active,
            "unmappedCredentials": self.unmapped,
            "unreadableCredentials": self.unreadable,
            "lastReachedAtMs": self.reached_at_ms,
            "lastError": self.last_error,
            "view": self.view,
        }));
        report
    }

    /// Rows arrive already filtered for catalog withdrawals. An incomplete or
    /// stale mapping widens to that served catalog, never to the caller's host
    /// list. With complete mappings the exact provider-id join is exclusive.
    pub fn served_routes(&self, facts: &[ModelFact], now_ms: u64) -> (Vec<String>, Option<String>) {
        let fallback = self.fallback_reason(now_ms);
        let routes = facts
            .iter()
            .filter(|fact| fallback.is_some() || self.providers.contains(&fact.provider_id))
            .map(|fact| make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()))
            .collect();
        (routes, fallback)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prefrontal_ckcred_client::{
        CkcredCredentialResolver, CredentialGetTarget, CredentialResolver, CredentialResolverError,
        CredentialResolverFuture, ListedCredential,
    };
    use serde_json::{json, Value};

    struct FixtureTarget(Value);

    impl CredentialGetTarget for FixtureTarget {
        fn credential_get<'a>(
            &'a self,
            _: &'a str,
            _: Option<u64>,
            _: bool,
        ) -> CredentialResolverFuture<'a, Result<Value, CredentialResolverError>> {
            panic!("reachability must never request secret material")
        }

        fn credential_list_scoped(
            &self,
        ) -> CredentialResolverFuture<'_, Result<Value, CredentialResolverError>> {
            Box::pin(async { Ok(self.0.clone()) })
        }
    }

    async fn decode(reply: Value) -> ScopedCredentialListing {
        CkcredCredentialResolver::with_target(Arc::new(FixtureTarget(reply)), 0)
            .list_scoped()
            .await
            .expect("fixture decodes through the live client")
    }

    fn fixture() -> Value {
        serde_json::from_str(include_str!("../fixtures/claustrum-list-scoped.json")).unwrap()
    }

    fn credential(id: &str, providers: &[&str]) -> ListedCredential {
        ListedCredential {
            id: id.into(),
            categories: vec!["llm-provider".into()],
            state: "active".into(),
            provider_ids: providers.iter().map(|s| (*s).into()).collect(),
            // The credential's vendor list names a different provider than its
            // operator-set provider ids, to prove the join reads only the ids.
            serves: vec!["uncredentialed".into()],
            ..Default::default()
        }
    }

    fn listing(credentials: Vec<ListedCredential>) -> ScopedCredentialListing {
        ScopedCredentialListing {
            credentials,
            view: "first-view".into(),
            ..Default::default()
        }
    }

    fn facts() -> Vec<ModelFact> {
        vec![
            crate::tests::fact("anthropic", "m1"),
            crate::tests::fact("uncredentialed", "m2"),
        ]
    }

    #[tokio::test]
    async fn producer_fixture_list_only_and_all_auth_method_rows_decode() {
        let fixture = fixture();
        assert_eq!(
            fixture["source"]["commit"],
            "04fe143076fd95b2e8abbf3817a507e3a6f81141"
        );
        let list_only = decode(fixture["list_only_reply"].clone()).await;
        assert!(list_only.undecodable_credentials.is_empty());
        assert_eq!(
            list_only.view,
            "Oj5KK0yt7FvfNQByawwRHX8Omd0BYj6GEu2L1haq4AA="
        );
        assert_eq!(list_only.grant_tuples[0].operation, "list");
        let row = &list_only.credentials[0];
        assert_eq!(row.id, "oauth:anthropic");
        assert_eq!(row.credential_type, "oauth");
        assert_eq!(row.state, "active");
        assert_eq!(row.categories, ["llm-provider"]);
        assert_eq!(row.operations, ["list"]);
        assert_eq!(row.provider_ids, ["anthropic"]);
        assert_eq!(row.serves, ["anthropic"]);
        assert_eq!(
            row.account_id.as_deref(),
            Some("00000000-0000-4000-8000-000000000000")
        );
        assert_eq!(row.email.as_deref(), Some("consumer@example.invalid"));
        assert_eq!(row.org_name.as_deref(), Some("Example Org"));
        assert_eq!(row.refresh_adapter.as_deref(), Some("anthropic"));
        assert_eq!(row.record_version, 232);

        let all = decode(fixture["reply"].clone()).await;
        assert!(all.undecodable_credentials.is_empty());
        assert_eq!(all.credentials.len(), 5);
        assert_eq!(
            all.credentials
                .iter()
                .map(|row| row.auth_method.as_deref())
                .collect::<Vec<_>>(),
            [
                Some("antigravity"),
                Some("apikey"),
                Some("chatgpt"),
                None,
                Some("oauth")
            ]
        );
        assert_eq!(all.credentials[0].provider_ids, ["google-antigravity"]);
        assert_eq!(all.credentials[0].serves, ["google", "anthropic", "openai"]);
        assert_eq!(all.credentials[1].provider_ids, ["openrouter"]);
        assert_eq!(all.credentials[1].refresh_adapter, None);
        assert_eq!(all.credentials[1].account_id, None);
        assert_eq!(all.credentials[1].org_name, None);
        assert!(all.credentials[2].provider_ids.is_empty());
        assert_eq!(
            all.credentials[4].provider_ids,
            ["anthropic", "claude-code"]
        );
    }

    #[test]
    fn mapped_credentials_join_exact_provider_ids_for_routes_and_panels() {
        let mut cache = BrocaCredentials::default();
        let mut inactive = credential("revoked", &["uncredentialed"]);
        inactive.state = "needs_reauth".into();
        let mut unrelated = credential("not-an-llm", &[]);
        unrelated.categories.clear();
        let mut inactive_unmapped = credential("inactive-unmapped", &[]);
        inactive_unmapped.state = "revoked".into();
        cache.apply_listing(
            &listing(vec![
                credential("mapped", &["anthropic"]),
                inactive,
                unrelated,
                inactive_unmapped,
            ]),
            crate::now_ms(),
        );
        assert_eq!(
            cache.health_report(crate::now_ms()).status,
            HealthStatus::Ok
        );
        assert!(cache.health_report(crate::now_ms()).detail.is_none());
        let shared = Arc::new(Mutex::new(cache));
        let facts = facts();
        let selection = crate::select_availability(
            Some("broca"),
            vec!["uncredentialed/m2".into()],
            &facts,
            &shared,
        );
        assert_eq!(selection.models, ["anthropic/m1"]);
        assert!(!selection.note.unwrap().contains("unknown"));
        let (substrate, available) = crate::panel_availability(
            Some("broca"),
            HashSet::from(["uncredentialed/m2".into()]),
            &facts,
            &shared,
        );
        assert_eq!(
            substrate,
            prefrontal_routing_core::AvailabilitySubstrate::Broca
        );
        assert_eq!(available, HashSet::from(["anthropic/m1".into()]));
    }

    #[tokio::test]
    async fn unmapped_active_credentials_keep_catalog_fallback_and_specific_degraded_line() {
        for missing in [false, true] {
            let mut reply = fixture()["list_only_reply"].clone();
            let mut unmapped = reply["result"]["credentials"][0].clone();
            unmapped["id"] = json!("legacy-or-unmapped");
            if missing {
                unmapped.as_object_mut().unwrap().remove("provider_ids");
                unmapped.as_object_mut().unwrap().remove("auth_method");
            } else {
                unmapped["provider_ids"] = json!([]);
            }
            reply["result"]["credentials"]
                .as_array_mut()
                .unwrap()
                .push(unmapped);
            let listing = decode(reply).await;
            assert_eq!(listing.credentials.len(), 2);
            let mut cache = BrocaCredentials::default();
            cache.apply_listing(&listing, crate::now_ms());
            let health = cache.health_report(crate::now_ms());
            assert_eq!(health.status, HealthStatus::Degraded);
            assert_eq!(
                health.detail.as_deref(),
                Some("broca reachability: 1 of 2 credentials unmapped")
            );
            let shared = Arc::new(Mutex::new(cache));
            let facts = facts();
            let selection = crate::select_availability(Some("broca"), Vec::new(), &facts, &shared);
            assert_eq!(selection.models, ["anthropic/m1", "uncredentialed/m2"]);
            assert!(selection
                .note
                .unwrap()
                .contains("broca reachability: 1 of 2 credentials unmapped"));
            let (substrate, available) =
                crate::panel_availability(Some("broca"), HashSet::new(), &facts, &shared);
            assert_eq!(
                substrate,
                prefrontal_routing_core::AvailabilitySubstrate::BrocaServedCatalog
            );
            assert_eq!(available.len(), 2);

            // Operator-set provider IDs determine which catalog providers Broca
            // has credentials for. Mapping the last credential makes that set
            // complete, so health reports ok.
            let mut mapped = listing;
            mapped.view = "mapped-view".into();
            mapped.credentials[1].provider_ids = vec!["anthropic".into()];
            let mut cache = shared.lock().unwrap();
            cache.apply_listing(&mapped, crate::now_ms());
            assert_eq!(
                cache.health_report(crate::now_ms()).status,
                HealthStatus::Ok
            );
            assert!(cache.health_report(crate::now_ms()).detail.is_none());
            assert_eq!(
                cache.served_routes(&facts, crate::now_ms()).0,
                ["anthropic/m1"]
            );
        }
    }

    #[test]
    fn failed_listing_retains_join_for_fifteen_minutes_then_falls_back() {
        let mut cache = BrocaCredentials::default();
        cache.apply_listing(&listing(vec![credential("mapped", &["anthropic"])]), 1_000);
        cache.note_failure("daemon unavailable");
        assert_eq!(
            cache.served_routes(&facts(), 1_000 + MAX_AGE_MS).0,
            ["anthropic/m1"]
        );
        assert_eq!(
            cache.health_report(1_000 + MAX_AGE_MS).status,
            HealthStatus::Ok
        );
        let (routes, reason) = cache.served_routes(&facts(), 1_001 + MAX_AGE_MS);
        assert_eq!(routes, ["anthropic/m1", "uncredentialed/m2"]);
        assert!(reason.unwrap().contains("stale (over 15 minutes)"));
        assert_eq!(
            cache.health_report(1_001 + MAX_AGE_MS).status,
            HealthStatus::Degraded
        );
    }

    #[test]
    fn unchanged_view_skips_join_rebuild_but_renews_freshness() {
        let mut cache = BrocaCredentials::default();
        let original = listing(vec![credential("mapped", &["anthropic"])]);
        assert!(cache.apply_listing(&original, 0));
        cache.note_failure("temporary error");
        // The vault's hash of the listing decides whether anything changed. With
        // the same digest the cache keeps its provider set, even if a reply row differs.
        let mut unchanged = original.clone();
        unchanged.credentials.clear();
        assert!(!cache.apply_listing(&unchanged, MAX_AGE_MS + 1));
        assert_eq!(
            cache.served_routes(&facts(), MAX_AGE_MS + 2).0,
            ["anthropic/m1"]
        );
        assert_eq!(cache.last_error, None);
        unchanged.view = "new-view".into();
        assert!(cache.apply_listing(&unchanged, MAX_AGE_MS + 3));
        assert!(cache.served_routes(&facts(), MAX_AGE_MS + 4).0.is_empty());
        assert_eq!(cache.health_report(MAX_AGE_MS + 4).status, HealthStatus::Ok);
    }

    #[tokio::test]
    async fn unreadable_row_degrades_only_itself_and_keeps_healthy_join() {
        let mut reply = fixture()["list_only_reply"].clone();
        let mut bad = reply["result"]["credentials"][0].clone();
        bad["id"] = json!("bad-provider-ids");
        bad["provider_ids"] = json!([42]);
        reply["result"]["credentials"]
            .as_array_mut()
            .unwrap()
            .push(bad);
        let listing = decode(reply).await;
        assert_eq!(listing.credentials.len(), 1);
        assert_eq!(
            listing.undecodable_credentials,
            ["bad-provider-ids: provider_ids contains a non-string"]
        );
        let mut cache = BrocaCredentials::default();
        cache.apply_listing(&listing, 0);
        assert_eq!(cache.served_routes(&facts(), 1).0, ["anthropic/m1"]);
        let health = cache.health_report(1);
        assert_eq!(health.status, HealthStatus::Degraded);
        assert_eq!(
            health.detail.as_deref(),
            Some("broca reachability: 1 unreadable credential rows skipped")
        );
    }
}
