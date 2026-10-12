//! Broca reachability from Claustrum's non-secret credential metadata.
//!
//! `serves` names model vendors; only the operator's `provider_ids` mapping
//! names Fusiform providers. Older vaults omit the mapping and upgraded vaults
//! initially emit empty mappings, so either shape requires a catalog fallback.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::{Arc, Mutex},
};

use prefrontal_ckcred_client::{ListedCredential, ScopedCredentialListing};
use prefrontal_routing_core::{make_route_key, ModelFact};
use subc_protocol::session::{HealthReport, HealthStatus};

/// The same maximum age as the quota snapshot: a failed poll may keep the last
/// good answer briefly, but cannot assert reachability indefinitely.
const MAX_AGE_MS: u64 = 15 * 60 * 1000;

/// No credential maps an explicit-only pool provider, or the listing is unknown.
pub(crate) const EXPLICIT_PROVIDER_CREDENTIAL_UNRESOLVED: &str =
    "explicit_provider_credential_unresolved";

#[derive(Debug, Clone, Default)]
pub struct BrocaCredentials {
    view: Option<String>,
    reached_at_ms: Option<u64>,
    providers: HashSet<String>,
    active: usize,
    unmapped: usize,
    unmapped_ids: Vec<String>,
    unreadable: usize,
    last_error: Option<String>,
    listed: Vec<ListedCredential>,
    serving: BTreeMap<String, ServingMethod>,
    pub(crate) billing_catalog: crate::fusiform_catalog::PlaneCatalog,
    /// The operator's `model_routing.score_alias` as of the last refresh. An
    /// aliased provider has no Fusiform plane of its own: its serving method is
    /// read from the credentials mapped to the alias, and its rates from the
    /// target provider's catalog under that method.
    score_alias: std::collections::BTreeMap<String, String>,
    #[cfg(test)]
    pub(crate) legacy_pricing_baseline: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServingMethod {
    pub auth_method: Option<String>,
    pub reason: String,
}

pub(crate) fn credential_method(adapter: Option<&str>, credential_type: &str) -> String {
    match adapter {
        Some("openai" | "chatgpt") => "chatgpt",
        Some("antigravity") => "antigravity",
        Some("anthropic") => "oauth",
        _ => credential_type,
    }
    .to_string()
}

fn choose_method(roster: BTreeSet<String>, listed: BTreeSet<String>) -> ServingMethod {
    let (methods, source) = if roster.is_empty() {
        (listed, "single listed method")
    } else {
        (roster, "roster")
    };
    ServingMethod {
        auth_method: (methods.len() == 1).then(|| methods.iter().next().unwrap().clone()),
        reason: if methods.len() == 1 {
            source.into()
        } else {
            format!(
                "ambiguous ({source}: {})",
                methods.into_iter().collect::<Vec<_>>().join(", ")
            )
        },
    }
}

pub type SharedBrocaCredentials = Arc<Mutex<BrocaCredentials>>;

impl BrocaCredentials {
    #[cfg(test)]
    pub(crate) fn api_test_credentials(facts: &[ModelFact]) -> Self {
        let mut credentials = Self::default();
        credentials.serving = facts
            .iter()
            .map(|fact| {
                (
                    format!("{}/{}", fact.provider_id, fact.model_id),
                    ServingMethod {
                        auth_method: Some("apikey".into()),
                        reason: "single listed method".into(),
                    },
                )
            })
            .collect();
        credentials.billing_catalog =
            crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(facts);
        credentials
    }

    /// Every successful poll renews freshness. Only a changed view rebuilds the
    /// join, even if tokens refreshed or the previous call failed.
    pub fn apply_listing(&mut self, listing: &ScopedCredentialListing, at_ms: u64) -> bool {
        self.reached_at_ms = Some(at_ms);
        self.last_error = None;
        if self.view.as_deref() == Some(&listing.view) {
            return false;
        }
        self.view = Some(listing.view.clone());
        self.listed = listing.credentials.clone();
        self.providers.clear();
        self.active = 0;
        self.unmapped = 0;
        self.unmapped_ids.clear();
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
                self.unmapped_ids.push(credential.id.clone());
            }
            self.providers
                .extend(credential.provider_ids.iter().cloned());
        }
        true
    }

    /// Replace the score aliases used for serving methods, Fusiform plane keys
    /// and pricing. Takes effect for serving methods on the next refresh.
    pub(crate) fn set_score_alias(&mut self, aliases: std::collections::BTreeMap<String, String>) {
        self.score_alias = aliases;
    }

    pub(crate) fn refresh_serving_methods(
        &mut self,
        state: &crate::account_quota::AccountQuotaState,
        facts: &[ModelFact],
        at_ms: i64,
    ) {
        // Aliased providers have no stored rows; serve them the borrowed rows
        // selection will see so their credential method is known.
        let mut facts = facts.to_vec();
        prefrontal_routing_core::score_alias::append_alias_rows(&mut facts, &self.score_alias);
        self.serving = facts
            .iter()
            .map(|fact| {
                let roster = crate::account_selection::billing_roster(
                    state,
                    &fact.provider_id,
                    &fact.model_id,
                    at_ms,
                );
                let methods = roster
                    .iter()
                    .flat_map(|account| {
                        account.credentials.iter().map(|record| {
                            let credential_type = self
                                .listed
                                .iter()
                                .find(|row| row.id == record.credential_id)
                                .map_or("oauth", |row| row.credential_type.as_str());
                            credential_method(Some(&account.refresh_adapter), credential_type)
                        })
                    })
                    .collect();
                let listed = self
                    .listed
                    .iter()
                    .filter(|row| {
                        row.state == "active"
                            && row
                                .categories
                                .iter()
                                .any(|category| category == "llm-provider")
                            && row.provider_ids.contains(&fact.provider_id)
                    })
                    .map(|row| {
                        credential_method(row.refresh_adapter.as_deref(), &row.credential_type)
                    })
                    .collect();
                (
                    format!("{}/{}", fact.provider_id, fact.model_id),
                    choose_method(methods, listed),
                )
            })
            .collect();
    }

    pub(crate) fn plane_keys(&self) -> BTreeSet<(String, String)> {
        self.serving
            .iter()
            .filter_map(|(key, method)| {
                // An aliased provider is priced on its target's plane, the only
                // one Fusiform knows, under the alias's own credential method.
                let provider = key.split_once('/')?.0;
                let provider =
                    prefrontal_routing_core::score_alias::alias_target(&self.score_alias, provider)
                        .unwrap_or(provider);
                Some((provider.to_string(), method.auth_method.clone()?))
            })
            .collect()
    }

    /// Only cached data is read here; a selection cannot trigger a catalog call.
    pub(crate) fn price_facts(&self, facts: &mut [ModelFact]) {
        // The ignored live replay can run the pre-plane scorer on the same
        // copied database. This switch does not exist in production builds.
        #[cfg(test)]
        if self.legacy_pricing_baseline {
            return;
        }
        for fact in facts {
            let key = format!("{}/{}", fact.provider_id, fact.model_id);
            let method = self
                .serving
                .get(&key)
                .cloned()
                .unwrap_or_else(|| choose_method(BTreeSet::new(), BTreeSet::new()));
            let catalog_key =
                prefrontal_routing_core::score_alias::catalog_key(&self.score_alias, &key);
            fact.billing_plane = Some(self.billing_catalog.for_route(&catalog_key, &method));
        }
    }

    #[cfg(test)]
    pub(crate) fn mark_copy_listing_gap(&mut self) {
        for method in self.serving.values_mut() {
            if method.reason == "ambiguous (single listed method: )" {
                method.reason =
                    "serving method not verified live: no listing in a store copy".into();
            }
        }
    }

    /// The key pool of a routing provider that exists only as vault credential
    /// mappings: every llm-provider API key whose `provider_ids` names it, in
    /// any lifecycle state (a key the vault marks unusable is reported as such,
    /// not dropped). The workspace is the row's `account_id`. An unknown or stale
    /// listing, or no mapped credential, refuses by name: guessing a key would
    /// send on the wrong account or on the subscription logins.
    pub(crate) fn pool_keys(
        &self,
        provider: &str,
        now_ms: u64,
    ) -> Result<Vec<prefrontal_routing_core::key_pool::PoolKey>, (String, String)> {
        let unresolved = |detail: &str| {
            Err((
                EXPLICIT_PROVIDER_CREDENTIAL_UNRESOLVED.to_string(),
                format!("no vault credential resolves provider {provider}: {detail}"),
            ))
        };
        let Some(reached_at_ms) = self.reached_at_ms else {
            return unresolved("the credential listing has not been read");
        };
        if now_ms.saturating_sub(reached_at_ms) > MAX_AGE_MS {
            return unresolved("the credential listing is over 15 minutes old");
        }
        let keys: Vec<_> = self
            .listed
            .iter()
            .filter(|row| {
                row.categories
                    .iter()
                    .any(|category| category == "llm-provider")
                    && row.provider_ids.iter().any(|id| id == provider)
                    // A key pool holds API keys only. A login can share a label
                    // with a key (`oauth:anthropic:ysample` beside
                    // `apikey:anthropic:ysample`); even a mis-mapped login must
                    // never be pinned as a pool key.
                    && row.id.starts_with("apikey:")
            })
            .map(|row| {
                prefrontal_routing_core::key_pool::PoolKey::new(
                    &row.id,
                    row.account_id.as_deref(),
                    &row.state,
                )
            })
            .collect();
        if keys.is_empty() {
            return unresolved("no llm-provider credential lists it in provider_ids");
        }
        Ok(keys)
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
        if self.providers.is_empty() {
            return Some(
                "broca reachability unknown: no credentials map to catalog providers".into(),
            );
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
            "unmappedCredentialIds": self.unmapped_ids,
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

    #[test]
    fn serving_method_prefers_roster_and_marks_mixed_rosters_ambiguous() {
        let listed = BTreeSet::from(["apikey".into(), "chatgpt".into()]);
        let choice = choose_method(BTreeSet::from(["chatgpt".into()]), listed.clone());
        assert_eq!(choice.auth_method.as_deref(), Some("chatgpt"));
        assert_eq!(choice.reason, "roster");
        let mixed = choose_method(
            BTreeSet::from(["oauth".into(), "antigravity".into()]),
            listed,
        );
        assert_eq!(mixed.auth_method, None);
        assert!(mixed.reason.starts_with("ambiguous (roster:"));
    }

    #[test]
    fn empty_roster_uses_only_a_single_distinct_listed_method() {
        let single = choose_method(BTreeSet::new(), BTreeSet::from(["apikey".into()]));
        assert_eq!(single.auth_method.as_deref(), Some("apikey"));
        assert_eq!(single.reason, "single listed method");
        let mixed = choose_method(
            BTreeSet::new(),
            BTreeSet::from(["apikey".into(), "oauth".into()]),
        );
        assert_eq!(mixed.auth_method, None);
        assert!(mixed.reason.starts_with("ambiguous"));
    }

    #[test]
    fn methods_come_from_metadata_never_credential_ids() {
        assert_eq!(credential_method(Some("openai"), "oauth"), "chatgpt");
        assert_eq!(credential_method(Some("anthropic"), "oauth"), "oauth");
        assert_eq!(
            credential_method(Some("antigravity"), "oauth"),
            "antigravity"
        );
        assert_eq!(credential_method(None, "apikey"), "apikey");
        let mut row = credential("chatgpt:openai:misleading", &["openai"]);
        row.credential_type = "apikey".into();
        let mut credentials = BrocaCredentials::default();
        credentials.apply_listing(&listing(vec![row]), 1);
        credentials.refresh_serving_methods(
            &crate::account_quota::AccountQuotaState::default(),
            &[crate::tests::fact("openai", "example")],
            1,
        );
        assert_eq!(
            credentials.serving["openai/example"].auth_method.as_deref(),
            Some("apikey")
        );
    }

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
    async fn unmapped_active_credentials_are_reported_without_widening_mapped_routes() {
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
            assert_eq!(health.status, HealthStatus::Ok);
            assert_eq!(health.detail, None);
            assert_eq!(
                health.metrics.as_ref().unwrap()["unmappedCredentialIds"],
                json!(["legacy-or-unmapped"])
            );
            let shared = Arc::new(Mutex::new(cache));
            let facts = facts();
            let selection = crate::select_availability(Some("broca"), Vec::new(), &facts, &shared);
            assert_eq!(selection.models, ["anthropic/m1"]);
            assert!(!selection.note.unwrap().contains("unmapped"));
            let (substrate, available) =
                crate::panel_availability(Some("broca"), HashSet::new(), &facts, &shared);
            assert_eq!(
                substrate,
                prefrontal_routing_core::AvailabilitySubstrate::Broca
            );
            assert_eq!(available, HashSet::from(["anthropic/m1".into()]));

            // The unmapped row remains visible, but it cannot widen the routes
            // past providers whose IDs are explicitly mapped.
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
    fn unmapped_credentials_are_informational_when_other_providers_are_mapped() {
        let mut credentials = (0..19)
            .map(|index| credential(&format!("mapped-{index}"), &["anthropic"]))
            .collect::<Vec<_>>();
        credentials.push(credential("apikey:typesafe", &[]));
        credentials.push(credential("oauth:cursor", &[]));

        let mut cache = BrocaCredentials::default();
        cache.apply_listing(&listing(credentials), crate::now_ms());
        let report = cache.health_report(crate::now_ms());
        assert_eq!(report.status, HealthStatus::Ok);
        assert_eq!(report.detail, None);
        let metrics = report.metrics.unwrap();
        assert_eq!(metrics["activeCredentials"], 21);
        assert_eq!(metrics["unmappedCredentials"], 2);
        assert_eq!(
            metrics["unmappedCredentialIds"],
            serde_json::json!(["apikey:typesafe", "oauth:cursor"])
        );
        assert_eq!(
            cache.served_routes(&facts(), crate::now_ms()).0,
            ["anthropic/m1"]
        );
    }

    #[test]
    fn no_mapped_credentials_degrades_health_and_falls_back_to_catalog() {
        let mut cache = BrocaCredentials::default();
        cache.apply_listing(
            &listing(vec![
                credential("apikey:typesafe", &[]),
                credential("oauth:cursor", &[]),
            ]),
            crate::now_ms(),
        );

        let report = cache.health_report(crate::now_ms());
        assert_eq!(report.status, HealthStatus::Degraded);
        assert_eq!(
            report.detail.as_deref(),
            Some("broca reachability unknown: no credentials map to catalog providers")
        );
        assert_eq!(
            cache.served_routes(&facts(), crate::now_ms()).0,
            ["anthropic/m1", "uncredentialed/m2"]
        );
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
        assert_eq!(
            cache.health_report(1_001 + MAX_AGE_MS).detail.as_deref(),
            Some("broca reachability unknown: credential listing stale (over 15 minutes)")
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
        assert_eq!(
            cache.served_routes(&facts(), MAX_AGE_MS + 4).0,
            ["anthropic/m1", "uncredentialed/m2"]
        );
        assert_eq!(
            cache.health_report(MAX_AGE_MS + 4).status,
            HealthStatus::Degraded
        );
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
