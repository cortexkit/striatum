//! Broca reachability from Claustrum's non-secret credential metadata.
//!
//! `provider_ids` supplies explicit provider membership. When that mapping is
//! empty, exactly one catalog provider named in `serves` supplies membership.
//! A mapped provider with no catalog rows (`anthropic-platform`) borrows the
//! rows of the one catalog provider its credentials' `serves` names; see
//! [`BrocaCredentials::catalog_resolution`].

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::{Arc, Mutex},
};

use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
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
    media_list_granted: bool,
    catalog_providers: BTreeSet<String>,
    pub(crate) runner_declarations: BTreeMap<String, crate::routing_v1::runners::RunnerDeclaration>,
    pub(crate) runner_service_tiers:
        BTreeMap<String, cortexkit_role_llm_runner::service_tier::ServiceTiers>,
    pub(crate) runner_routed_accounts: BTreeMap<String, Vec<String>>,
    pub(crate) classifier_models: BTreeMap<String, Vec<crate::routing_v1::classifier::ServedModel>>,
    pub(crate) plan_prices: serde_json::Value,
    pub(crate) wire_apis: BTreeMap<String, String>,
    serving: BTreeMap<String, ServingMethod>,
    pub(crate) billing_catalog: crate::fusiform_catalog::PlaneCatalog,
    subscription_plan_providers: Option<HashSet<String>>,
    billing_loaded: Arc<tokio::sync::Notify>,
    pub(crate) catalog_snapshot: Option<Arc<crate::fusiform_catalog::CatalogSnapshot>>,
    pub(crate) catalog_wake: Arc<tokio::sync::Notify>,
    pub(crate) connection_file_path: Option<std::path::PathBuf>,
    /// The providers with no catalog rows that resolved to a catalog provider
    /// at the last [`Self::refresh_serving_methods`], each with its catalog
    /// provider. Such a provider has no Fusiform plane of its own: its serving
    /// method is read from the credentials mapped to it, and its rates from the
    /// catalog provider's plane under that method.
    priced_targets: BTreeMap<String, String>,
    #[cfg(test)]
    pub(crate) legacy_pricing_baseline: bool,
    #[cfg(test)]
    pub(crate) engine_clock_offset_ms: u64,
    #[cfg(test)]
    pub(crate) engine_test_facts: BTreeMap<(String, String), (Option<f64>, Option<f64>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServingMethod {
    pub auth_method: Option<String>,
    pub reason: String,
}

/// Explicit provider mappings win. Without them, infer membership only when
/// exactly one provider named in `serves` is present in the catalog.
pub(crate) fn credential_providers(
    row: &ListedCredential,
    catalog: &BTreeSet<String>,
) -> Vec<String> {
    if !row.provider_ids.is_empty() {
        return row.provider_ids.clone();
    }
    let vendors: BTreeSet<_> = row
        .serves
        .iter()
        .filter(|vendor| catalog.contains(*vendor))
        .collect();
    if vendors.len() == 1 {
        vendors.into_iter().cloned().collect()
    } else {
        Vec::new()
    }
}

pub(crate) fn credential_serves_fact(
    row: &ListedCredential,
    fact: &ModelFact,
    catalog: &BTreeSet<String>,
) -> bool {
    row.state == "active"
        && row
            .categories
            .iter()
            .any(|category| category == "llm-provider")
        && credential_providers(row, catalog).contains(&fact.provider_id)
        && fact.auth_methods.as_ref().is_none_or(|allowed| {
            row.auth_method
                .as_ref()
                .is_some_and(|method| allowed.contains(method))
        })
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

/// What routing's last vault listing says about one credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ListingLookup {
    /// Routing has not read a listing yet, so nothing can be compared.
    Unknown,
    /// The listing no longer names the credential.
    Absent,
    /// The credential's fingerprint (see [`listing_fingerprint_of`]).
    Listed(String),
}

/// `provider_ids=<sorted ids>;plan_tier=<tier>;state=<state>`. Token refreshes
/// and record versions are left out: they do not change what the provider
/// entitles the credential to.
pub(crate) fn listing_fingerprint_of(row: &ListedCredential) -> String {
    let mut provider_ids = row.provider_ids.clone();
    provider_ids.sort();
    provider_ids.dedup();
    format!(
        "provider_ids={};plan_tier={};state={}",
        provider_ids.join(","),
        row.plan_tier.as_deref().unwrap_or(""),
        row.state
    )
}

/// Wait for the first complete plane publication, not merely a catalog connection.
/// Restored account identities can resolve a serving method before its rates arrive.
pub(crate) async fn wait_for_billing_catalog(
    credentials: &SharedBrocaCredentials,
    bound: std::time::Duration,
) -> bool {
    let Ok(loaded) = credentials
        .lock()
        .map(|guard| Arc::clone(&guard.billing_loaded))
    else {
        return false;
    };
    tokio::time::timeout(bound, async {
        loop {
            let notified = loaded.notified();
            tokio::pin!(notified);
            // Register before checking readiness so publication cannot be missed.
            notified.as_mut().enable();
            if credentials
                .lock()
                .is_ok_and(|guard| guard.billing_catalog.is_loaded())
            {
                return;
            }
            notified.await;
        }
    })
    .await
    .is_ok()
}

impl BrocaCredentials {
    pub(crate) fn listing_is_fresh(&self, now_ms: u64) -> bool {
        self.view.is_some()
            && self
                .reached_at_ms
                .is_some_and(|at| now_ms.saturating_sub(at) <= MAX_AGE_MS)
    }

    pub(crate) fn wire_api(&self, fact: &ModelFact, auth_method: &str) -> Option<&str> {
        let provider = self
            .priced_targets
            .get(&fact.provider_id)
            .unwrap_or(&fact.provider_id);
        let model = format!("{provider}/{}", fact.model_id);
        self.billing_catalog
            .wire_api(&model, auth_method)
            .unwrap_or_else(|| self.wire_apis.get(&model).map(String::as_str))
    }
    /// The vault's listed facts, without deriving a credential kind from its id
    /// or refresh adapter. Selection uses these facts to classify account coverage.
    pub(crate) fn listed_credentials(&self) -> &[ListedCredential] {
        &self.listed
    }

    /// Exact API-key vault ids grouped by the providers they serve, using the same
    /// metadata mapping as route admission. Credential names are not provider ids.
    pub(crate) fn api_key_credentials_by_provider(
        &self,
        now_ms: u64,
    ) -> BTreeMap<String, BTreeSet<String>> {
        let mut mapped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        if !self.listing_is_fresh(now_ms) {
            return mapped;
        }
        for row in self.listed.iter().filter(|row| {
            row.state == "active"
                && row.auth_method.as_deref() == Some("apikey")
                && row
                    .categories
                    .iter()
                    .any(|category| category == "llm-provider")
        }) {
            for provider in credential_providers(row, &self.catalog_providers) {
                mapped.entry(provider).or_default().insert(row.id.clone());
            }
        }
        mapped
    }

    pub(crate) fn listing_version(&self) -> Option<(&str, u64)> {
        Some((self.view.as_deref()?, self.reached_at_ms?))
    }

    /// Reprices `fact` on the selected account class's method. `pricing` is the
    /// selection's configuration, read as in [`Self::price_facts`]: a configured
    /// subscription multiplier keeps list rates on an uncovered plan.
    pub(crate) fn price_for_method(
        &self,
        fact: &mut ModelFact,
        auth_method: &str,
        pricing: &prefrontal_routing_core::RoutePricing,
    ) {
        let key = format!("{}/{}", fact.provider_id, fact.model_id);
        let catalog_key =
            prefrontal_routing_core::score_alias::catalog_key(&self.priced_targets, &key);
        let multiplier_configured = pricing.subscription_multiplier(&key).is_some();
        fact.billing_plane = Some(self.billing_catalog.for_route(
            &catalog_key,
            &ServingMethod {
                auth_method: Some(auth_method.into()),
                reason: "selected account class".into(),
            },
            multiplier_configured,
        ));
    }

    /// The vault listing of one credential, reduced to the fields that could
    /// change whether a provider entitles it: the provider ids it is mapped to,
    /// its plan tier and its state. `Unknown` until routing has read a listing.
    pub(crate) fn listing_fingerprint(&self, credential_id: &str) -> ListingLookup {
        if self.view.is_none() {
            return ListingLookup::Unknown;
        }
        self.listed
            .iter()
            .find(|row| row.id == credential_id)
            .map_or(ListingLookup::Absent, |row| {
                ListingLookup::Listed(listing_fingerprint_of(row))
            })
    }

    /// The credentials a launch on `fact` may run on, in listing order: the
    /// serving identities provider-reset and entitlement fences are judged
    /// against, and the only credentials routing may pin a launch to.
    ///
    /// Only credentials of the route's serving method count: the method
    /// pricing and Broca's account selection use (for `openai`, the ChatGPT
    /// logins of the billing roster). A provider can also have metered API
    /// keys mapped to it (`apikey:openai`); those are not how the route is
    /// served, so a fence on one subscription login must move the launch to
    /// another login, never to a key billed per token. When the serving method
    /// is ambiguous, API keys are left out for the same reason. A provider
    /// served by API keys alone keeps them (its serving method is `apikey`).
    pub(crate) fn serving_identities(&self, fact: &ModelFact) -> Vec<(String, Option<String>)> {
        if self
            .reached_at_ms
            .is_none_or(|at| crate::now_ms().saturating_sub(at) > MAX_AGE_MS)
        {
            return Vec::new();
        }
        let serving = self.serving_method(fact).auth_method;
        self.listed
            .iter()
            .filter(|row| credential_serves_fact(row, fact, &self.catalog_providers))
            .filter(|row| match serving.as_deref() {
                Some(serving) => row.auth_method.as_deref() == Some(serving),
                None => row.auth_method.as_deref() != Some("apikey"),
            })
            .map(|row| (row.id.clone(), row.account_id.clone()))
            .collect()
    }

    /// The method a route is served and priced under: the one the last
    /// refresh resolved, or before any refresh the single method of the
    /// credentials listed for it (the same fallback pricing uses).
    fn serving_method(&self, fact: &ModelFact) -> ServingMethod {
        let key = format!("{}/{}", fact.provider_id, fact.model_id);
        self.serving.get(&key).cloned().unwrap_or_else(|| {
            choose_method(
                BTreeSet::new(),
                self.listed
                    .iter()
                    .filter(|row| credential_serves_fact(row, fact, &self.catalog_providers))
                    .filter_map(|row| row.auth_method.clone())
                    .collect(),
            )
        })
    }
    pub(crate) fn publish_billing_catalog(
        &mut self,
        catalog: crate::fusiform_catalog::PlaneCatalog,
    ) {
        self.billing_catalog = catalog;
        self.billing_loaded.notify_waiters();
    }

    /// Every published plan counts, including unpriced or refused plan rows.
    /// Unknown membership must not be confused with a successfully empty list.
    pub(crate) fn publish_plan_prices(&mut self, prices: &serde_json::Value) {
        self.plan_prices = prices.clone();
        self.subscription_plan_providers = prices
            .get("prices")
            .and_then(serde_json::Value::as_array)
            .and_then(|rows| {
                rows.iter()
                    .map(|row| {
                        row.get("provider_id")?
                            .as_str()
                            .filter(|id| !id.is_empty())
                            .map(str::to_owned)
                    })
                    .collect()
            });
    }

    pub(crate) fn subscription_plan_providers(&self) -> Option<HashSet<String>> {
        let mut providers = self.subscription_plan_providers.clone()?;
        providers.extend(self.billing_catalog.subscription_plan_providers());
        Some(providers)
    }

    #[cfg(test)]
    pub(crate) fn api_test_credentials(facts: &[ModelFact]) -> Self {
        let mut credentials = Self::default();
        if !facts.is_empty() {
            credentials.apply_listing(
                &ScopedCredentialListing {
                    view: "test-catalog-credentials".into(),
                    credentials: facts
                        .iter()
                        .map(|fact| {
                            let method = fact
                                .billing_plane
                                .as_ref()
                                .map(|plane| plane.auth_method.clone())
                                .or_else(|| {
                                    fact.auth_methods
                                        .as_ref()
                                        .and_then(|methods| methods.first().cloned())
                                })
                                .unwrap_or_else(|| "apikey".into());
                            (
                                (fact.provider_id.clone(), method.clone()),
                                ListedCredential {
                                    id: format!("catalog-credential-{}-{method}", fact.provider_id),
                                    account_id: Some(format!(
                                        "catalog-account-{}-{method}",
                                        fact.provider_id
                                    )),
                                    state: "active".into(),
                                    categories: vec!["llm-provider".into()],
                                    provider_ids: vec![fact.provider_id.clone()],
                                    credential_type: if method == "apikey" {
                                        "apikey"
                                    } else {
                                        "oauth"
                                    }
                                    .into(),
                                    auth_method: Some(method),
                                    ..Default::default()
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>()
                        .into_values()
                        .collect(),
                    ..Default::default()
                },
                crate::now_ms(),
            );
        }
        credentials.serving = facts
            .iter()
            .map(|fact| {
                (
                    format!("{}/{}", fact.provider_id, fact.model_id),
                    ServingMethod {
                        auth_method: Some(
                            fact.billing_plane
                                .as_ref()
                                .map_or("apikey", |plane| plane.auth_method.as_str())
                                .into(),
                        ),
                        reason: "single listed method".into(),
                    },
                )
            })
            .collect();
        credentials.billing_catalog =
            crate::fusiform_catalog::PlaneCatalog::ordinary_test_catalog(facts);
        credentials.publish_plan_prices(&serde_json::json!({"prices": []}));
        credentials
    }

    /// Every successful poll renews freshness. Only a changed view rebuilds the
    /// join, even if tokens refreshed or the previous call failed.
    pub fn apply_listing(&mut self, listing: &ScopedCredentialListing, at_ms: u64) -> bool {
        self.reached_at_ms = Some(at_ms);
        self.last_error = None;
        self.media_list_granted = listing.grant_tuples.iter().any(|grant| {
            grant.selector_kind == "category"
                && grant.selector == "media-provider"
                && grant.operation == "list"
        });
        if self.view.as_deref() == Some(&listing.view) {
            return false;
        }
        self.view = Some(listing.view.clone());
        self.listed = listing.credentials.clone();
        self.serving.clear();
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
            let providers = credential_providers(credential, &self.catalog_providers);
            if providers.is_empty() {
                self.unmapped += 1;
                self.unmapped_ids.push(credential.id.clone());
            }
            self.providers.extend(providers);
        }
        self.catalog_wake.notify_one();
        true
    }

    pub(crate) fn media_listing_ready(&self, at_ms: u64) -> bool {
        self.media_list_granted
            && self.last_error.is_none()
            && self
                .reached_at_ms
                .is_some_and(|at| at_ms.saturating_sub(at) <= MAX_AGE_MS)
    }

    pub(crate) fn mapped_providers(&self) -> HashSet<String> {
        self.providers.clone()
    }

    /// Only metered consumers learn vault payment methods in admission-only
    /// copies. Other consumers keep the catalog facts, as do availability and billing.
    pub(crate) fn admission_facts(
        &self,
        facts: &[ModelFact],
        consumer: Option<crate::select_policy::Consumer>,
        now_ms: u64,
    ) -> Vec<ModelFact> {
        if !crate::select_policy::requires_metered(consumer)
            || self.fallback_reason(now_ms).is_some()
        {
            return facts.to_vec();
        }
        let mut methods_by_provider: std::collections::HashMap<_, BTreeSet<_>> =
            std::collections::HashMap::new();
        for row in &self.listed {
            if row.state != "active" || !row.categories.iter().any(|value| value == "llm-provider")
            {
                continue;
            }
            let Some(method) = row.auth_method.as_ref().filter(|method| !method.is_empty()) else {
                continue;
            };
            for provider in &row.provider_ids {
                methods_by_provider
                    .entry(provider)
                    .or_default()
                    .insert(method.clone());
            }
        }
        facts
            .iter()
            .cloned()
            .map(|mut fact| {
                if let Some(methods) = methods_by_provider.get(&fact.provider_id) {
                    let auth = fact.auth_methods.get_or_insert_with(Vec::new);
                    for method in methods {
                        if !auth.contains(method) {
                            auth.push(method.clone());
                        }
                    }
                }
                fact
            })
            .collect()
    }

    /// Resolve every provider the listing maps credentials to but that has no
    /// rows in `facts` (the stored catalog rows, before any borrowed rows are
    /// appended): it borrows the rows of the one catalog provider that a
    /// `serves` vendor of its active credentials names. With no such vendor,
    /// or more than one, the provider stays unresolved and gets no rows, so it
    /// has no scores and no prices and is never selected.
    pub(crate) fn catalog_resolution(
        &self,
        facts: &[ModelFact],
    ) -> prefrontal_routing_core::score_alias::CatalogResolution {
        let catalog: BTreeSet<String> = facts.iter().map(|fact| fact.provider_id.clone()).collect();
        self.catalog_resolution_for_providers(&catalog)
    }

    pub(crate) fn catalog_resolution_for_providers(
        &self,
        catalog: &BTreeSet<String>,
    ) -> prefrontal_routing_core::score_alias::CatalogResolution {
        prefrontal_routing_core::score_alias::resolve_catalog_targets(
            catalog,
            self.listed
                .iter()
                .filter(|row| {
                    row.state == "active"
                        && row
                            .categories
                            .iter()
                            .any(|category| category == "llm-provider")
                })
                .map(
                    |row| prefrontal_routing_core::score_alias::CredentialVendors {
                        provider_ids: &row.provider_ids,
                        serves: &row.serves,
                    },
                ),
        )
    }

    /// [`Self::catalog_resolution`]'s map from each resolved provider to the
    /// catalog provider whose rows it borrows.
    pub(crate) fn catalog_targets(&self, facts: &[ModelFact]) -> BTreeMap<String, String> {
        self.catalog_resolution(facts).targets
    }

    pub(crate) fn refresh_serving_methods(
        &mut self,
        state: &crate::account_quota::AccountQuotaState,
        facts: &[ModelFact],
        at_ms: i64,
        model_pools: &[prefrontal_routing_core::quota_extract::ModelPool],
    ) {
        self.catalog_providers = facts.iter().map(|fact| fact.provider_id.clone()).collect();
        self.providers.clear();
        self.unmapped_ids.clear();
        for row in self.listed.iter().filter(|row| {
            row.state == "active"
                && row
                    .categories
                    .iter()
                    .any(|category| category == "llm-provider")
        }) {
            let providers = credential_providers(row, &self.catalog_providers);
            if providers.is_empty() {
                self.unmapped_ids.push(row.id.clone());
            }
            self.providers.extend(providers);
        }
        self.unmapped = self.unmapped_ids.len();
        // Resolved providers have no stored rows; serve them the borrowed rows
        // selection will see so their credential method is known.
        self.priced_targets = self.catalog_targets(facts);
        let mut facts = facts.to_vec();
        prefrontal_routing_core::score_alias::append_alias_rows(&mut facts, &self.priced_targets);
        self.serving = facts
            .iter()
            .map(|fact| {
                let roster = crate::account_selection::billing_roster(
                    state,
                    &fact.provider_id,
                    &fact.model_id,
                    at_ms,
                    model_pools,
                );
                let methods = roster
                    .iter()
                    .flat_map(|account| {
                        account.credentials.iter().filter_map(|record| {
                            let listed = self
                                .listed
                                .iter()
                                .find(|row| row.id == record.credential_id);
                            // A quota identity cannot supply an absent vault auth method.
                            listed
                                .filter(|row| {
                                    credential_serves_fact(row, fact, &self.catalog_providers)
                                })
                                .and_then(|row| row.auth_method.clone())
                        })
                    })
                    .collect();
                let listed = self
                    .listed
                    .iter()
                    .filter(|row| credential_serves_fact(row, fact, &self.catalog_providers))
                    .filter_map(|row| row.auth_method.clone())
                    .collect();
                (
                    format!("{}/{}", fact.provider_id, fact.model_id),
                    choose_method(methods, listed),
                )
            })
            .collect();
    }

    pub(crate) fn plane_keys(&self) -> BTreeSet<(String, String)> {
        let mut keys: BTreeSet<_> = self
            .serving
            .iter()
            .filter_map(|(key, method)| {
                // A resolved provider is priced on its catalog provider's plane,
                // the only one Fusiform knows, under its own credential method.
                let provider = key.split_once('/')?.0;
                let provider = prefrontal_routing_core::score_alias::alias_target(
                    &self.priced_targets,
                    provider,
                )
                .unwrap_or(provider);
                Some((provider.to_string(), method.auth_method.clone()?))
            })
            .collect();
        // A newly mapped provider may have no local model row yet. Its login is
        // still enough to request the plane before the first placement arrives.
        for row in &self.listed {
            if row.state == "active"
                && row
                    .categories
                    .iter()
                    .any(|category| category == "llm-provider")
            {
                for provider in &row.provider_ids {
                    if self
                        .serving
                        .keys()
                        .any(|key| key.split_once('/').is_some_and(|(id, _)| id == provider))
                    {
                        continue;
                    }
                    let provider = prefrontal_routing_core::score_alias::alias_target(
                        &self.priced_targets,
                        provider,
                    )
                    .unwrap_or(provider);
                    if let Some(method) = &row.auth_method {
                        keys.insert((provider.to_string(), method.clone()));
                    }
                }
            }
        }
        keys
    }

    /// Only cached data is read here; a selection cannot trigger a catalog call.
    /// `pricing` is the selection's own configuration: a route whose provider
    /// has an operator-configured subscription multiplier keeps the catalog's
    /// base list rates when Fusiform does not cover its plan (see
    /// [`crate::fusiform_catalog::PlaneCatalog::for_route`]).
    pub(crate) fn price_facts(
        &self,
        facts: &mut [ModelFact],
        pricing: &prefrontal_routing_core::RoutePricing,
    ) {
        // The ignored live replay can run the pre-plane scorer on the same
        // copied database. This switch does not exist in production builds.
        #[cfg(test)]
        if self.legacy_pricing_baseline {
            return;
        }
        for fact in facts {
            let key = format!("{}/{}", fact.provider_id, fact.model_id);
            let method = self.serving_method(fact);
            let catalog_key =
                prefrontal_routing_core::score_alias::catalog_key(&self.priced_targets, &key);
            let multiplier_configured = pricing.subscription_multiplier(&key).is_some();
            fact.billing_plane = Some(self.billing_catalog.for_route(
                &catalog_key,
                &method,
                multiplier_configured,
            ));
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
        // The legacy shapes judge reach for the Broca runner from this listing.
        let runner = crate::legacy_wire::BROCA;
        let Some(reached_at_ms) = self.reached_at_ms else {
            return Some(format!(
                "{runner} reachability unknown: credential listing unavailable"
            ));
        };
        if now_ms.saturating_sub(reached_at_ms) > MAX_AGE_MS {
            return Some(format!(
                "{runner} reachability unknown: credential listing stale (over 15 minutes)"
            ));
        }
        if self.providers.is_empty() {
            return Some(format!(
                "{runner} reachability unknown: no credentials map to catalog providers"
            ));
        }
        None
    }

    pub fn health_report(&self, now_ms: u64) -> HealthReport {
        let mut report = HealthReport::ok();
        report.detail = self.fallback_reason(now_ms).or_else(|| {
            (self.unreadable > 0).then(|| {
                format!(
                    "{} reachability: {} unreadable credential rows skipped",
                    crate::legacy_wire::BROCA,
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

    pub(crate) fn statically_served_routes(&self, facts: &[ModelFact]) -> HashSet<String> {
        let catalog = facts.iter().map(|fact| fact.provider_id.clone()).collect();
        facts
            .iter()
            .filter(|fact| {
                self.listed
                    .iter()
                    .any(|row| credential_serves_fact(row, fact, &catalog))
            })
            .map(|fact| make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()))
            .collect()
    }

    /// Rows arrive already filtered for catalog withdrawals. When the vault
    /// listing is unknown, stale, or has no mapped providers, unrestricted models
    /// fall back to that served catalog, never the caller's host list. A fresh
    /// listing also resolves membership through uniquely matching `serves` vendors.
    /// Restricted models cannot use the catalog fallback: an active credential
    /// in a fresh listing must join the provider and satisfy the login allowlist.
    pub fn served_routes(&self, facts: &[ModelFact], now_ms: u64) -> (Vec<String>, Option<String>) {
        let fallback = self.fallback_reason(now_ms);
        let catalog = facts.iter().map(|fact| fact.provider_id.clone()).collect();
        let routes = facts
            .iter()
            .filter(|fact| {
                (fallback.is_some() && fact.auth_methods.is_none())
                    || (self.listing_is_fresh(now_ms)
                        && self
                            .listed
                            .iter()
                            .any(|row| credential_serves_fact(row, fact, &catalog)))
            })
            .map(|fact| make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()))
            .collect();
        (routes, fallback)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn api_key_provider_mapping_uses_active_metadata_and_expires() {
        let mut explicit = credential("opaque-key", &["opencode-go", "another-provider"]);
        explicit.auth_method = Some("apikey".into());
        let mut inferred = credential("unrelated-name", &[]);
        inferred.auth_method = Some("apikey".into());
        inferred.serves = vec!["kimi-for-coding".into()];
        let mut inactive = explicit.clone();
        inactive.id = "inactive".into();
        inactive.state = "disabled".into();
        let mut other_category = explicit.clone();
        other_category.id = "other-category".into();
        other_category.categories = vec!["storage".into()];
        let mut oauth = explicit.clone();
        oauth.id = "oauth-record".into();
        oauth.auth_method = Some("oauth".into());
        let mut unmapped = inferred.clone();
        unmapped.id = "unmapped".into();
        unmapped.serves = vec!["unknown".into()];
        let mut cache = BrocaCredentials::default();
        cache.catalog_providers = ["kimi-for-coding".into()].into();
        cache.apply_listing(
            &listing(vec![
                explicit,
                inferred,
                inactive,
                other_category,
                oauth,
                unmapped,
            ]),
            100,
        );
        assert_eq!(
            cache.api_key_credentials_by_provider(100),
            BTreeMap::from([
                ("opencode-go".into(), ["opaque-key".into()].into()),
                ("another-provider".into(), ["opaque-key".into()].into()),
                ("kimi-for-coding".into(), ["unrelated-name".into()].into()),
            ])
        );
        assert!(cache
            .api_key_credentials_by_provider(100 + MAX_AGE_MS + 1)
            .is_empty());
    }

    #[test]
    fn listed_credentials_preserve_the_vault_type_without_adapter_inference() {
        let mut credentials = super::BrocaCredentials::default();
        credentials.apply_listing(
            &cortexkit_claustrum_client::ScopedCredentialListing {
                view: "type-facts".into(),
                credentials: vec![cortexkit_claustrum_client::ListedCredential {
                    id: "opaque-id".into(),
                    credential_type: "apikey".into(),
                    refresh_adapter: Some("chatgpt".into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            1,
        );
        assert_eq!(
            credentials.listed_credentials()[0].credential_type,
            "apikey"
        );
        assert_eq!(credentials.listed_credentials()[0].id, "opaque-id");
    }

    use super::*;
    use cortexkit_claustrum_client::{
        ClaustrumCredentialResolver, CredentialGetTarget, CredentialResolver,
        CredentialResolverError, CredentialResolverFuture, ListedCredential,
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
        let mut row = credential("chatgpt:openai:misleading", &["openai"]);
        row.credential_type = "apikey".into();
        row.auth_method = Some("apikey".into());
        row.refresh_adapter = Some("chatgpt".into());
        let mut credentials = BrocaCredentials::default();
        credentials.apply_listing(&listing(vec![row]), 1);
        credentials.refresh_serving_methods(
            &crate::account_quota::AccountQuotaState::default(),
            &[crate::tests::fact("openai", "example")],
            1,
            &[],
        );
        assert_eq!(
            credentials.serving["openai/example"].auth_method.as_deref(),
            Some("apikey")
        );
    }

    fn metered_fact() -> ModelFact {
        let mut fact = crate::tests::fact("mapped", "metered");
        fact.billing_plane = Some(prefrontal_routing_core::billing::BillingPlane {
            provider_id: "mapped".into(),
            auth_method: "apikey".into(),
            kind: "upstream".into(),
            selection_reason: "test credential".into(),
            rates: std::collections::BTreeMap::from([(
                "rate.output".into(),
                prefrontal_routing_core::billing::BillingRate::Priced { rate: 0.75 },
            )]),
        });
        fact
    }

    fn admission_refusals(
        cache: &BrocaCredentials,
        facts: &[ModelFact],
        consumer: crate::select_policy::Consumer,
    ) -> std::collections::HashMap<String, String> {
        crate::select_policy::candidate_rejections(
            &cache.admission_facts(facts, Some(consumer), 1),
            Some(consumer),
            None,
            cache.subscription_plan_providers().as_ref(),
        )
    }

    #[test]
    fn active_apikey_mapping_admits_metered_routes_without_changing_catalog_policy() {
        let consumer = Some(crate::select_policy::Consumer::Codemode);
        let mut row = credential("oauth:misleading", &["mapped"]);
        row.credential_type = "apikey".into();
        row.auth_method = Some("apikey".into());
        let mut cache = BrocaCredentials::default();
        cache.apply_listing(&listing(vec![row]), 1);
        cache.publish_plan_prices(&json!({"prices": []}));
        let mut facts = vec![metered_fact(), metered_fact()];
        facts[1].model_id = "other".into();
        facts[1].auth_methods = Some(vec!["oauth".into()]);
        for consumer in [
            crate::select_policy::Consumer::Codemode,
            crate::select_policy::Consumer::Flow,
        ] {
            assert!(admission_refusals(&cache, &facts, consumer).is_empty());
        }
        let enriched = cache.admission_facts(&facts, consumer, 1);
        assert_eq!(enriched[0].auth_methods, Some(vec!["apikey".into()]));
        assert_eq!(
            enriched[1].auth_methods,
            Some(vec!["oauth".into(), "apikey".into()])
        );
        assert_eq!(facts[0].auth_methods, None);
        assert_eq!(facts[1].auth_methods, Some(vec!["oauth".into()]));
        assert!(!cache
            .served_routes(&facts, 1)
            .0
            .contains(&"mapped/other".into()));
        assert_eq!(cache.admission_facts(&enriched, consumer, 1), enriched);
        assert_eq!(
            cache.admission_facts(&facts, consumer, MAX_AGE_MS + 2),
            facts
        );
        cache.apply_listing(
            &ScopedCredentialListing {
                view: "removed".into(),
                ..Default::default()
            },
            2,
        );
        assert_eq!(cache.admission_facts(&facts, consumer, 2), facts);
    }

    #[test]
    fn oauth_and_chatgpt_credentials_do_not_admit_metered_consumers() {
        for (kind, method) in [
            ("oauth", "oauth"),
            ("chatgpt", "chatgpt"),
            ("apikey", "chatgpt"),
            ("apikey", "oauth"),
        ] {
            let mut row = credential("apikey:misleading", &["mapped"]);
            row.credential_type = kind.into();
            row.auth_method = Some(method.into());
            let mut cache = BrocaCredentials::default();
            cache.apply_listing(&listing(vec![row]), 1);
            cache.publish_plan_prices(&json!({"prices": []}));
            for consumer in [
                crate::select_policy::Consumer::Codemode,
                crate::select_policy::Consumer::Flow,
            ] {
                assert!(
                    admission_refusals(&cache, &[metered_fact()], consumer)
                        .contains_key("mapped/metered"),
                    "{kind} / {method}"
                );
            }
        }
    }

    #[test]
    fn inactive_unmapped_and_unknown_credentials_do_not_supply_apikey_auth() {
        let consumer = Some(crate::select_policy::Consumer::Codemode);
        let mut inactive = credential("apikey:inactive", &["mapped"]);
        inactive.credential_type = "apikey".into();
        inactive.state = "dead".into();
        let mut unmapped = credential("apikey:mapped", &["elsewhere"]);
        unmapped.credential_type = "apikey".into();
        let unknown = credential("apikey:unknown", &["mapped"]);
        let mut other_category = inactive.clone();
        other_category.state = "active".into();
        other_category.categories = vec!["web-search".into()];
        let mut cache = BrocaCredentials::default();
        cache.apply_listing(
            &listing(vec![inactive, unmapped, unknown, other_category]),
            1,
        );
        cache.publish_plan_prices(&json!({"prices": []}));
        let facts = [metered_fact()];
        assert_eq!(
            cache.admission_facts(&facts, consumer, 1)[0].auth_methods,
            None
        );
        assert!(
            admission_refusals(&cache, &facts, crate::select_policy::Consumer::Codemode)
                .contains_key("mapped/metered")
        );
        cache.apply_listing(
            &ScopedCredentialListing {
                view: "removed".into(),
                ..Default::default()
            },
            2,
        );
        assert_eq!(
            cache.admission_facts(&facts, consumer, 2)[0].auth_methods,
            None
        );
    }

    #[test]
    fn ordinary_consumers_keep_catalog_admission_facts_with_or_without_vault_apikeys() {
        use crate::select_policy::{candidate_rejections, Consumer};
        let mut facts = vec![metered_fact(), metered_fact(), metered_fact()];
        for (fact, provider) in facts
            .iter_mut()
            .zip(["openai", "openrouter", "fireworks-ai"])
        {
            fact.provider_id = provider.into();
            fact.billing_plane.as_mut().unwrap().provider_id = provider.into();
        }
        facts[1].auth_methods = Some(vec!["oauth".into()]);
        facts[2].auth_methods = Some(vec![]);
        let mut row = credential("opaque", &["openai", "openrouter", "fireworks-ai"]);
        row.credential_type = "apikey".into();
        let mut cache = BrocaCredentials::default();
        cache.apply_listing(&listing(vec![row]), 1);
        let absent = BrocaCredentials::default();
        let plans = HashSet::new();
        let allowed_providers = ["openai".into()];
        for consumer in [
            Some(Consumer::Mason),
            Some(Consumer::Sidekick),
            Some(Consumer::Athena),
            Some(Consumer::Core),
            Some(Consumer::Lab),
            None,
        ] {
            let with_vault = cache.admission_facts(&facts, consumer, 1);
            let without_vault = absent.admission_facts(&facts, consumer, 1);
            assert_eq!(with_vault, facts, "{consumer:?}");
            assert_eq!(with_vault, without_vault, "{consumer:?}");
            for allowed in [None, Some(allowed_providers.as_slice())] {
                assert_eq!(
                    candidate_rejections(&with_vault, consumer, allowed, Some(&plans)),
                    candidate_rejections(&without_vault, consumer, allowed, Some(&plans)),
                    "{consumer:?}"
                );
            }
        }
    }

    #[test]
    fn vault_apikey_does_not_bypass_plan_membership_or_output_price_requirements() {
        let mut row = credential("opaque", &["mapped"]);
        row.credential_type = "apikey".into();
        row.auth_method = Some("apikey".into());
        let mut cache = BrocaCredentials::default();
        cache.apply_listing(&listing(vec![row]), 1);
        let fact = metered_fact();
        for plans in [json!({"prices": [{"provider_id": "mapped"}]}), json!({})] {
            cache.publish_plan_prices(&plans);
            assert!(admission_refusals(
                &cache,
                &[fact.clone()],
                crate::select_policy::Consumer::Codemode
            )
            .contains_key("mapped/metered"));
        }
        cache.publish_plan_prices(&json!({"prices": []}));
        let mut unpriced = fact;
        unpriced.billing_plane.as_mut().unwrap().rates.clear();
        assert!(admission_refusals(
            &cache,
            &[unpriced],
            crate::select_policy::Consumer::Codemode
        )
        .contains_key("mapped/metered"));
    }

    /// The credentials a launch may be pinned to are those of the route's
    /// serving method. `openai` is served through its ChatGPT logins (the
    /// roster's method); its metered API keys, listed first, are never among
    /// them, whether the method came from the roster or is ambiguous. A
    /// provider served by API keys alone keeps its keys.
    #[test]
    fn serving_identities_are_the_serving_methods_credentials_only() {
        let row = |id: &str, provider: &str| {
            let login = id.starts_with("chatgpt:");
            let mut row = credential(id, &[provider]);
            row.credential_type = if login { "oauth" } else { "apikey" }.into();
            row.auth_method = Some(if login { "chatgpt" } else { "apikey" }.into());
            row.refresh_adapter = login.then(|| "openai".into());
            row
        };
        let mut cache = BrocaCredentials::default();
        cache.apply_listing(
            &listing(vec![
                row("apikey:openai", "openai"),
                row("apikey:openai:astro", "openai"),
                row("chatgpt:openai", "openai"),
                row("chatgpt:openai:live", "openai"),
                row("apikey:metered", "metered"),
            ]),
            crate::now_ms(),
        );
        let sol = crate::tests::fact("openai", "gpt-6.1-sol");
        let ids = |cache: &BrocaCredentials, fact: &ModelFact| {
            cache
                .serving_identities(fact)
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>()
        };
        let logins = ["chatgpt:openai", "chatgpt:openai:live"];
        // No roster yet: the listed methods are mixed, so the method is
        // ambiguous, and an API key is still never a serving identity.
        assert_eq!(ids(&cache, &sol), logins);
        // The roster resolves the method to chatgpt, as it did live.
        cache.serving.insert(
            "openai/gpt-6.1-sol".into(),
            ServingMethod {
                auth_method: Some("chatgpt".into()),
                reason: "roster".into(),
            },
        );
        assert_eq!(ids(&cache, &sol), logins);
        assert_eq!(
            ids(&cache, &crate::tests::fact("metered", "m")),
            ["apikey:metered"]
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
        ClaustrumCredentialResolver::with_target(Arc::new(FixtureTarget(reply)), 0)
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
        for id in ["apikey:typesafe", "oauth:cursor"] {
            let mut row = credential(id, &[]);
            row.serves.clear();
            credentials.push(row);
        }

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
