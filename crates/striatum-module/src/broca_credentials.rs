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
use striatum_core::{make_route_key, ModelFact};
use subc_protocol::session::{HealthReport, HealthStatus};

/// The same maximum age as the quota snapshot: a failed poll may keep the last
/// good answer briefly, but cannot assert reachability indefinitely.
const MAX_AGE_MS: u64 = 15 * 60 * 1000;

/// No credential maps an explicit-only pool provider, or the listing is unknown.
pub(crate) const EXPLICIT_PROVIDER_CREDENTIAL_UNRESOLVED: &str =
    "explicit_provider_credential_unresolved";

pub(crate) type CredentialAccounts = BTreeMap<String, Option<String>>;

/// Provider membership and documented `login_methods` coverage from one complete
/// fact snapshot. Curated auth restrictions describe only their own row. Build
/// before filtering candidates: a model outside a panel or pick can still
/// establish that its provider limits a sign-in to documented models.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AuthCatalog {
    pub providers: BTreeSet<String>,
    signins: BTreeMap<String, BTreeSet<String>>,
}

impl<'a> FromIterator<&'a ModelFact> for AuthCatalog {
    fn from_iter<T: IntoIterator<Item = &'a ModelFact>>(facts: T) -> Self {
        let mut catalog = Self::default();
        for fact in facts {
            catalog.providers.insert(fact.provider_id.clone());
            for method in fact
                .auth_methods
                .iter()
                .filter(|_| fact.login_methods_documented)
                .flatten()
                .filter(|method| *method != "apikey")
            {
                catalog
                    .signins
                    .entry(fact.provider_id.clone())
                    .or_default()
                    .insert(method.clone());
            }
        }
        catalog
    }
}

impl std::ops::Deref for AuthCatalog {
    type Target = BTreeSet<String>;
    fn deref(&self) -> &Self::Target {
        &self.providers
    }
}

impl AuthCatalog {
    pub(crate) fn inherit_aliases(&mut self, targets: &BTreeMap<String, String>) {
        for (alias, target) in targets {
            if let Some(methods) = self.signins.get(target).cloned() {
                self.signins.insert(alias.clone(), methods);
            }
        }
    }

    fn allows(&self, fact: &ModelFact, method: Option<&str>) -> bool {
        if method.is_some_and(|method| {
            method != "apikey"
                && self
                    .signins
                    .get(&fact.provider_id)
                    .is_some_and(|methods| methods.contains(method))
        }) && !fact.login_methods_documented
        {
            return false;
        }
        fact.auth_methods.as_ref().is_none_or(|allowed| {
            method.is_some_and(|method| allowed.iter().any(|value| value == method))
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct BrocaCredentials {
    inner: Arc<CredentialData>,
}

impl BrocaCredentials {
    pub(crate) fn set_watch_confirmed(&mut self, view: Option<&str>) {
        let confirmed = view.is_some() && self.inner.view.as_deref() == view;
        if self.inner.watch_confirmed != confirmed {
            Arc::make_mut(&mut self.inner).watch_confirmed = confirmed;
        }
    }

    pub fn apply_listing(&mut self, listing: &ScopedCredentialListing, at_ms: u64) -> bool {
        std::ops::DerefMut::deref_mut(self).apply_listing(listing, at_ms)
    }

    pub(crate) fn publish_billing_catalog(
        &mut self,
        catalog: crate::fusiform_catalog::PlaneCatalog,
    ) {
        std::ops::DerefMut::deref_mut(self).publish_billing_catalog(catalog);
    }

    /// Retain an immutable request view without copying catalog pages. Pollers
    /// copy on write, so a request never holds the credentials lock across I/O.
    pub(crate) fn snapshot(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    #[cfg(test)]
    pub(crate) fn api_test_credentials(facts: &[ModelFact]) -> Self {
        Self {
            inner: Arc::new(CredentialData::api_test_credentials(facts)),
        }
    }
}

impl std::ops::Deref for BrocaCredentials {
    type Target = CredentialData;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for BrocaCredentials {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let data = Arc::make_mut(&mut self.inner);
        if let Some(current) = data.serving_revision.current.take() {
            data.serving_revision.previous = Some(current);
        }
        data
    }
}

#[derive(Clone, Default)]
struct ServingRevision {
    previous: Option<Arc<ServingInputs>>,
    current: std::sync::OnceLock<Arc<ServingInputs>>,
}

impl std::fmt::Debug for ServingRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Populating a derived cache is not a change to the credential snapshot.
        f.write_str("ServingRevision")
    }
}

#[derive(Debug)]
struct ServingInputs {
    generation: u64,
    view: Option<String>,
    listed: Vec<ListedCredential>,
    declarations: BTreeMap<String, crate::routing_v1::runners::RunnerDeclaration>,
    declarations_seen: BTreeSet<String>,
    retained_declarations: BTreeSet<String>,
    billing: crate::fusiform_catalog::PlaneCatalog,
    wire_apis: BTreeMap<String, String>,
    wire_api_models: BTreeSet<String>,
    priced_targets: BTreeMap<String, String>,
    routed_accounts: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
struct ServingRefreshInputs {
    catalog: striatum_store::CatalogFingerprintKey,
    view: Option<String>,
    listed: Vec<ListedCredential>,
    declarations: BTreeMap<String, crate::routing_v1::runners::RunnerDeclaration>,
    billing: crate::fusiform_catalog::PlaneCatalog,
    wire_apis: BTreeMap<String, String>,
    identities: Option<Vec<(String, String, Vec<String>)>>,
    provider_links: Vec<(String, Option<String>)>,
}

impl ServingRefreshInputs {
    fn read(
        credentials: &CredentialData,
        state: &crate::account_quota::AccountQuotaState,
        catalog: striatum_store::CatalogFingerprintKey,
    ) -> Self {
        // Keep refresh_serving_methods' structural dependencies in this one key.
        // Adding an input to that join requires adding it here too. The account
        // roster supplies credential IDs through identity/account/provider joins; quota
        // values, model reach, poll times and current time do not filter those IDs.
        Self {
            catalog,
            view: credentials.view.clone(),
            listed: credentials.listed.clone(),
            declarations: credentials.runner_declarations.clone(),
            billing: credentials.billing_catalog.clone(),
            wire_apis: credentials.wire_apis.clone(),
            identities: state.roster.as_ref().map(|roster| {
                roster
                    .identities
                    .iter()
                    .map(|identity| {
                        (
                            identity.refresh_adapter.clone(),
                            identity.account_id.clone(),
                            identity.credential_ids.clone(),
                        )
                    })
                    .collect()
            }),
            provider_links: state
                .readings
                .readings
                .values()
                .map(|reading| {
                    (
                        reading.account_id.clone(),
                        reading.routing_provider().map(str::to_owned),
                    )
                })
                .collect(),
        }
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Default)]
pub struct CredentialData {
    serving_revision: ServingRevision,
    serving_refresh_inputs: Option<ServingRefreshInputs>,
    #[cfg(test)]
    serving_refresh_builds: usize,
    view: Option<String>,
    reached_at_ms: Option<u64>,
    // A held watch confirms the applied digest without extending the age limit
    // after disconnect. Never restore this evidence from a saved listing.
    watch_confirmed: bool,
    providers: HashSet<String>,
    active: usize,
    unmapped: usize,
    unmapped_ids: Vec<String>,
    unreadable: usize,
    last_error: Option<String>,
    listed: Vec<ListedCredential>,
    media_list_granted: bool,
    gateway_grant_warned: bool,
    catalog_providers: AuthCatalog,
    pub(crate) runner_declarations: BTreeMap<String, crate::routing_v1::runners::RunnerDeclaration>,
    // Process-lifetime marks prevent an expired declaration from becoming a
    // legacy runner. After routing restarts, an absent configured runner is
    // treated as legacy until it first declares again.
    pub(crate) runner_declarations_seen: BTreeSet<String>,
    pub(crate) runner_retained_declarations: BTreeSet<String>,
    pub(crate) first_party_runners:
        BTreeMap<String, crate::routing_v1::runners::enrollment::Identity>,
    pub(crate) runner_catalog_wake: Arc<tokio::sync::Notify>,
    pub(crate) runner_service_tiers:
        BTreeMap<String, cortexkit_role_llm_runner::service_tier::ServiceTiers>,
    pub(crate) runner_routed_accounts: BTreeMap<String, Vec<String>>,
    /// Successful describes, including explicit withdrawals of tier support.
    /// Shared timestamps let admission-time refreshes renew the poller's view.
    pub(crate) runner_describe_observed: BTreeMap<String, i64>,
    pub(crate) runner_describe_logs: crate::account_refusal_log::LogLimiter,
    pub(crate) classifier_models: BTreeMap<String, Vec<crate::routing_v1::classifier::ServedModel>>,
    pub(crate) plan_prices: serde_json::Value,
    pub(crate) concurrency_caps: crate::concurrency_caps::CapInputs,
    pub(crate) wire_apis: BTreeMap<String, String>,
    /// Endpoint presence from served `wire.api`; `wire_apis` stores `wire.family`.
    pub(crate) wire_api_models: BTreeSet<String>,
    serving: BTreeMap<String, ServingMethod>,
    pub(crate) billing_catalog: crate::fusiform_catalog::PlaneCatalog,
    subscription_plan_providers: Option<HashSet<String>>,
    billing_loaded: Arc<tokio::sync::Notify>,
    listing_loaded: Arc<(Mutex<bool>, std::sync::Condvar)>,
    pub(crate) catalog_snapshot: Option<Arc<crate::fusiform_catalog::CatalogSnapshot>>,
    pub(crate) catalog_wake: Arc<tokio::sync::Notify>,
    pub(crate) catalog_rebind_wake: Arc<tokio::sync::Notify>,
    pub(crate) connection_file_path: Option<std::path::PathBuf>,
    /// One on-demand account-input fetch shared by concurrent admissions.
    pub(crate) admission_inputs: Arc<tokio::sync::Mutex<()>>,
    pub(crate) admission_describes: crate::routing_v1::runners::AdmissionDescribes,
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
    catalog: &AuthCatalog,
    credentials: &CredentialData,
) -> bool {
    row.state == "active"
        && row
            .categories
            .iter()
            .any(|category| matches!(category.as_str(), "llm-provider" | "llm-gateway"))
        && credential_providers(row, catalog).contains(&fact.provider_id)
        && catalog.allows(fact, row.auth_method.as_deref())
        && credentials.method_serves_fact(fact, row.auth_method.as_deref())
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

/// Called on the route handler's blocking pool, before taking a request snapshot.
/// A fresh store gets the same 1.5 s startup allowance as selection's catalog.
/// Persisted listings are restored before serving, so only a fresh store (or an
/// older store without a full listing) whose vault is unreachable should time out.
pub(crate) fn wait_for_first_listing(credentials: &SharedBrocaCredentials) -> bool {
    let Ok(loaded) = credentials
        .lock()
        .map(|guard| Arc::clone(&guard.listing_loaded))
    else {
        // Let context loading report a poisoned cache, not a missing listing.
        return true;
    };
    let ready = loaded
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (ready, _) = loaded
        .1
        .wait_timeout_while(ready, std::time::Duration::from_millis(1500), |ready| {
            !*ready
        })
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *ready
}

pub(crate) fn listing_not_ready(params: &serde_json::Value) -> crate::account_selection::OpReply {
    // Keep the 3-second startup retry interval and candidate explanation, so
    // this earlier check does not change an existing caller's retry behavior.
    let exclusions: Vec<_> = params["candidates"]["accounts"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|candidate| {
            serde_json::json!({"account_id": candidate["account_id"],
            "reason": "routing just started and hasn't read the credential list yet"})
        })
        .collect();
    crate::account_selection::OpReply::typed(
        "account_selection_unavailable",
        "Routing is awaiting its first credential listing; retry shortly.",
        serde_json::json!({"code": "account_selection_unavailable", "retry_after_s": 3,
            "retry_after_ms": 3000,
            "cause": "credential_listing_not_loaded",
            "decision_id": params["decision_id"], "exclusions": exclusions}),
    )
}

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

impl CredentialData {
    /// Compare published values, not poll times or repeated publication of equal
    /// values. Each copy-on-write snapshot retains its own revision even when a
    /// newer snapshot is published.
    pub(crate) fn serving_generation(&self) -> u64 {
        self.serving_revision
            .current
            .get_or_init(|| {
                if let Some(old) = self.serving_revision.previous.as_ref().filter(|old| {
                    old.view == self.view
                        && old.listed == self.listed
                        && old.declarations == self.runner_declarations
                        && old.declarations_seen == self.runner_declarations_seen
                        && old.retained_declarations == self.runner_retained_declarations
                        && old.billing == self.billing_catalog
                        && old.wire_apis == self.wire_apis
                        && old.wire_api_models == self.wire_api_models
                        && old.priced_targets == self.priced_targets
                        && old.routed_accounts == self.runner_routed_accounts
                }) {
                    return Arc::clone(old);
                }
                static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
                Arc::new(ServingInputs {
                    generation: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    view: self.view.clone(),
                    listed: self.listed.clone(),
                    declarations: self.runner_declarations.clone(),
                    declarations_seen: self.runner_declarations_seen.clone(),
                    retained_declarations: self.runner_retained_declarations.clone(),
                    billing: self.billing_catalog.clone(),
                    wire_apis: self.wire_apis.clone(),
                    wire_api_models: self.wire_api_models.clone(),
                    priced_targets: self.priced_targets.clone(),
                    routed_accounts: self.runner_routed_accounts.clone(),
                })
            })
            .generation
    }
    /// Known wire families require a runner's wire/auth pair. Missing wire or
    /// declaration snapshots retain provider/allowlist reachability so discovery
    /// gaps do not withdraw rows. Endpoint requirements remain runner-specific:
    /// a runner's `requires_wire_api` is checked when that runner is selected.
    fn method_serves_fact(&self, fact: &ModelFact, method: Option<&str>) -> bool {
        if self.runner_declarations.is_empty() {
            return true;
        }
        let Some(wire) = self.wire_api(fact, method.unwrap_or("")) else {
            return true;
        };
        self.runner_declarations.values().any(|runner| {
            runner
                .pairs
                .iter()
                .any(|pair| pair.wire_api == wire && Some(pair.auth_method.as_str()) == method)
        })
    }

    pub(crate) fn listing_is_fresh(&self, now_ms: u64) -> bool {
        self.view.is_some()
            && (self.watch_confirmed
                || self
                    .reached_at_ms
                    .is_some_and(|at| now_ms.saturating_sub(at) <= MAX_AGE_MS))
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

    pub(crate) fn runner_allows_provider(
        &self,
        fact: &ModelFact,
        row: &ListedCredential,
        explicit_providers: &[String],
    ) -> bool {
        // A gateway owns its route and account pool, but borrows catalog rows
        // from the unique provider its vault `serves` resolves to. Runner limits
        // constrain those rows, not the gateway's name. An unresolved gateway
        // cannot satisfy a provider limit, even when its own id is listed.
        let provider = if row.auth_method.as_deref() == Some("gateway") {
            self.priced_targets.get(&fact.provider_id)
        } else {
            Some(&fact.provider_id)
        };
        provider.is_some_and(|provider| explicit_providers.contains(provider))
    }

    pub(crate) fn row_serves_wire_api(&self, fact: &ModelFact, auth_method: &str) -> bool {
        let provider = self
            .priced_targets
            .get(&fact.provider_id)
            .unwrap_or(&fact.provider_id);
        let model = format!("{provider}/{}", fact.model_id);
        self.billing_catalog
            .row_serves_wire_api(&model, auth_method)
            .unwrap_or_else(|| self.wire_api_models.contains(&model))
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
                    .any(|category| matches!(category.as_str(), "llm-provider" | "llm-gateway"))
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
        pricing: &striatum_core::RoutePricing,
    ) {
        let key = format!("{}/{}", fact.provider_id, fact.model_id);
        let catalog_key = striatum_core::score_alias::catalog_key(&self.priced_targets, &key);
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

    /// A new sign-in can replace a token without changing plan or provider
    /// entitlement, so sign-in evidence also watches the vault record version.
    pub(crate) fn signed_out_fingerprint(&self, credential_id: &str) -> ListingLookup {
        if self.view.is_none() {
            return ListingLookup::Unknown;
        }
        self.listed
            .iter()
            .find(|row| row.id == credential_id)
            .map_or(ListingLookup::Absent, |row| {
                ListingLookup::Listed(
                    serde_json::json!([listing_fingerprint_of(row), row.record_version])
                        .to_string(),
                )
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
        if !self.listing_is_fresh(crate::now_ms()) {
            return Vec::new();
        }
        let serving = self
            .serving_method(fact, &self.catalog_providers)
            .auth_method;
        self.listed
            .iter()
            .filter(|row| credential_serves_fact(row, fact, &self.catalog_providers, self))
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
    fn serving_method(&self, fact: &ModelFact, catalog: &AuthCatalog) -> ServingMethod {
        let key = format!("{}/{}", fact.provider_id, fact.model_id);
        self.serving
            .get(&key)
            .filter(|serving| {
                match serving.auth_method.as_deref() {
                    Some(method) => catalog.allows(fact, Some(method)),
                    // Removing the provider's last listing for a sign-in lets that
                    // sign-in reach unlisted models again. Recompute unresolved
                    // methods when coverage changes before the next roster refresh.
                    None => {
                        self.catalog_providers.signins.get(&fact.provider_id)
                            == catalog.signins.get(&fact.provider_id)
                    }
                }
            })
            .cloned()
            .unwrap_or_else(|| {
                choose_method(
                    BTreeSet::new(),
                    self.listed
                        .iter()
                        .filter(|row| credential_serves_fact(row, fact, catalog, self))
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
        self.concurrency_caps
            .publish_catalog(prices, crate::now_ms() as i64);
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
        *self.listing_loaded.0.lock().unwrap() = true;
        self.listing_loaded.1.notify_all();
        if !self.gateway_grant_warned
            && !listing.grant_tuples.iter().any(|grant| {
                grant.selector_kind == "category"
                    && grant.selector == "llm-gateway"
                    && grant.operation == "list"
            })
        {
            // list_scoped returns the granted view without category parameters;
            // an absent gateway grant must not discard the other credentials.
            tracing::warn!(
                "reserved:striatum needs list on category llm-gateway to discover company gateways; continuing with the scoped credential listing"
            );
            self.gateway_grant_warned = true;
        }
        self.media_list_granted = listing.grant_tuples.iter().any(|grant| {
            grant.selector_kind == "category"
                && grant.selector == "media-provider"
                && grant.operation == "list"
        });
        if self.view.as_deref() == Some(&listing.view) {
            return false;
        }
        self.watch_confirmed = false;
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
                    .any(|category| matches!(category.as_str(), "llm-provider" | "llm-gateway"))
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
        self.media_list_granted && self.last_error.is_none() && self.listing_is_fresh(at_ms)
    }

    pub(crate) fn mapped_providers(&self) -> HashSet<String> {
        self.providers.clone()
    }

    /// Resolve only missing vault account ids from unambiguous, live producer
    /// linkages under the credential's providers. Never modify the vault view.
    pub(crate) fn quota_account_ids(
        &self,
        quota: &crate::account_quota::AccountQuotaState,
    ) -> CredentialAccounts {
        self.listed
            .iter()
            .map(|row| {
                if let Some(account) = &row.account_id {
                    return (row.id.clone(), Some(account.clone()));
                }
                let providers = credential_providers(row, &self.catalog_providers);
                let accounts: BTreeSet<_> = quota
                    .readings
                    .readings
                    .values()
                    .filter(|reading| {
                        reading.tombstoned_at_ms.is_none()
                            && reading
                                .entry
                                .get("credentialRef")
                                .and_then(serde_json::Value::as_str)
                                == Some(row.id.as_str())
                            && reading.routing_provider().is_some_and(|provider| {
                                providers.iter().any(|listed| listed == provider)
                            })
                    })
                    .map(|reading| reading.account_id.clone())
                    .collect();
                let account = if accounts.len() == 1 {
                    accounts.into_iter().next()
                } else {
                    None
                };
                (row.id.clone(), account)
            })
            .collect()
    }

    pub(crate) fn credential_conditions(
        &self,
        provider: &str,
        account_id: &str,
        quota: Option<&crate::account_quota::AccountQuotaState>,
    ) -> Vec<serde_json::Value> {
        let mut rows = self
            .listed
            .iter()
            .filter_map(|row| Some((row, row.condition.as_ref()?)))
            .filter(|(row, _)| {
                credential_providers(row, &self.catalog_providers)
                    .iter()
                    .any(|id| id == provider)
            })
            .peekable();
        if rows.peek().is_none() {
            return Vec::new();
        }
        let joined = quota.map(|quota| self.quota_account_ids(quota));
        let mut entries = BTreeMap::new();
        for (row, condition) in rows {
            let id = row
                .account_id
                .as_deref()
                .or_else(|| joined.as_ref()?.get(&row.id)?.as_deref())
                .unwrap_or(&row.id);
            if id == account_id {
                entries.insert(
                    &row.id,
                    serde_json::json!({
                        "credential_id": row.id,
                        "condition": crate::account_quota::condition_json(condition),
                    }),
                );
            }
        }
        entries.into_values().collect()
    }

    /// Only metered consumers learn vault payment methods in admission-only
    /// copies. Other consumers keep the catalog facts, as do availability and billing.
    pub(crate) fn admission_facts(
        &self,
        facts: &[ModelFact],
        priced_per_token: bool,
        now_ms: u64,
    ) -> Vec<ModelFact> {
        if !priced_per_token || self.fallback_reason(now_ms).is_some() {
            return facts.to_vec();
        }
        let mut methods_by_provider: std::collections::HashMap<_, BTreeSet<_>> =
            std::collections::HashMap::new();
        for row in &self.listed {
            if row.state != "active"
                || !row
                    .categories
                    .iter()
                    .any(|value| matches!(value.as_str(), "llm-provider" | "llm-gateway"))
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
    ) -> striatum_core::score_alias::CatalogResolution {
        let catalog: BTreeSet<String> = facts.iter().map(|fact| fact.provider_id.clone()).collect();
        self.catalog_resolution_for_providers(&catalog)
    }

    pub(crate) fn catalog_resolution_for_providers(
        &self,
        catalog: &BTreeSet<String>,
    ) -> striatum_core::score_alias::CatalogResolution {
        striatum_core::score_alias::resolve_catalog_targets(
            catalog,
            self.listed
                .iter()
                .filter(|row| {
                    row.state == "active"
                        && row.categories.iter().any(|category| {
                            matches!(category.as_str(), "llm-provider" | "llm-gateway")
                        })
                })
                .map(|row| striatum_core::score_alias::CredentialVendors {
                    provider_ids: &row.provider_ids,
                    serves: &row.serves,
                }),
        )
    }

    /// [`Self::catalog_resolution`]'s map from each resolved provider to the
    /// catalog provider whose rows it borrows.
    pub(crate) fn catalog_targets(&self, facts: &[ModelFact]) -> BTreeMap<String, String> {
        self.catalog_resolution(facts).targets
    }

    pub(crate) fn refresh_serving_methods_if_changed(
        &mut self,
        state: &crate::account_quota::AccountQuotaState,
        catalog: &striatum_store::CatalogFactsSnapshot,
        at_ms: i64,
        model_pools: &[striatum_core::quota_extract::ModelPool],
    ) {
        let inputs = ServingRefreshInputs::read(self, state, catalog.generation);
        if self.serving_refresh_inputs.as_ref() == Some(&inputs) {
            // Which credentials and accounts can serve each model is unchanged,
            // but concurrency caps read live quota values, so republish them.
            self.concurrency_caps.publish_roster(state);
            return;
        }
        self.refresh_serving_methods(state, &catalog.facts, at_ms, model_pools);
        self.serving_refresh_inputs = Some(inputs);
    }

    pub(crate) fn refresh_serving_methods(
        &mut self,
        state: &crate::account_quota::AccountQuotaState,
        facts: &[ModelFact],
        at_ms: i64,
        model_pools: &[striatum_core::quota_extract::ModelPool],
    ) {
        self.serving_refresh_inputs = None;
        #[cfg(test)]
        {
            self.serving_refresh_builds += 1;
        }
        self.concurrency_caps.publish_roster(state);
        self.catalog_providers = facts.iter().collect();
        self.providers.clear();
        self.unmapped_ids.clear();
        for row in self.listed.iter().filter(|row| {
            row.state == "active"
                && row
                    .categories
                    .iter()
                    .any(|category| matches!(category.as_str(), "llm-provider" | "llm-gateway"))
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
        striatum_core::score_alias::append_alias_rows(&mut facts, &self.priced_targets);
        let mut catalog: AuthCatalog = facts.iter().collect();
        catalog.providers = self.catalog_providers.providers.clone();
        self.catalog_providers = catalog;
        self.serving = facts
            .iter()
            .map(|fact| {
                let roster = crate::account_selection::roster::billing_roster(
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
                                    credential_serves_fact(row, fact, &self.catalog_providers, self)
                                })
                                .and_then(|row| row.auth_method.clone())
                        })
                    })
                    .collect();
                let listed = self
                    .listed
                    .iter()
                    .filter(|row| credential_serves_fact(row, fact, &self.catalog_providers, self))
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
                let provider =
                    striatum_core::score_alias::alias_target(&self.priced_targets, provider)
                        .unwrap_or(provider);
                Some((provider.to_string(), method.auth_method.clone()?))
            })
            .collect();
        // Selecting a subscription account can resolve a method that is
        // ambiguous across a model's listed credentials. Request those planes
        // even when another method already serves a model on the same provider.
        for row in &self.listed {
            if row.state == "active"
                && row
                    .categories
                    .iter()
                    .any(|category| matches!(category.as_str(), "llm-provider" | "llm-gateway"))
            {
                for provider in &row.provider_ids {
                    if row
                        .auth_method
                        .as_deref()
                        .is_some_and(striatum_core::billing::is_api_metered_auth_method)
                        && self
                            .serving
                            .keys()
                            .any(|key| key.split_once('/').is_some_and(|(id, _)| id == provider))
                    {
                        continue;
                    }
                    let provider =
                        striatum_core::score_alias::alias_target(&self.priced_targets, provider)
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
        pricing: &striatum_core::RoutePricing,
    ) {
        // The ignored live replay can run the pre-plane scorer on the same
        // copied database. This switch does not exist in production builds.
        #[cfg(test)]
        if self.legacy_pricing_baseline {
            return;
        }
        let catalog = facts.iter().collect();
        for fact in facts {
            let key = format!("{}/{}", fact.provider_id, fact.model_id);
            let method = self.serving_method(fact, &catalog);
            let catalog_key = striatum_core::score_alias::catalog_key(&self.priced_targets, &key);
            let multiplier_configured = pricing.subscription_multiplier(&key).is_some();
            fact.billing_plane = Some(self.billing_catalog.for_route(
                &catalog_key,
                &method,
                multiplier_configured,
            ));
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
    ) -> Result<Vec<striatum_core::key_pool::PoolKey>, (String, String)> {
        let unresolved = |detail: &str| {
            Err((
                EXPLICIT_PROVIDER_CREDENTIAL_UNRESOLVED.to_string(),
                format!("no vault credential resolves provider {provider}: {detail}"),
            ))
        };
        if self.reached_at_ms.is_none() {
            return unresolved("the credential listing has not been read");
        }
        if !self.listing_is_fresh(now_ms) {
            return unresolved("the credential listing is over 15 minutes old");
        }
        let keys: Vec<_> = self
            .listed
            .iter()
            .filter(|row| {
                row.categories
                    .iter()
                    .any(|category| matches!(category.as_str(), "llm-provider" | "llm-gateway"))
                    && row.provider_ids.iter().any(|id| id == provider)
                    // A key pool holds API keys only. A login can share a label
                    // with a key (`oauth:anthropic:ysample` beside
                    // `apikey:anthropic:ysample`); even a mis-mapped login must
                    // never be pinned as a pool key.
                    && row.id.starts_with("apikey:")
            })
            .map(|row| {
                striatum_core::key_pool::PoolKey::new(
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
        if self.reached_at_ms.is_none() {
            return Some(format!(
                "{runner} reachability unknown: credential listing unavailable"
            ));
        }
        if !self.listing_is_fresh(now_ms) {
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
        let catalog = facts.iter().collect();
        facts
            .iter()
            .filter(|fact| {
                self.listed
                    .iter()
                    .any(|row| credential_serves_fact(row, fact, &catalog, self))
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
        let catalog = facts.iter().collect();
        let routes = facts
            .iter()
            .filter(|fact| {
                (fallback.is_some() && fact.auth_methods.is_none())
                    || (self.listing_is_fresh(now_ms)
                        && self
                            .listed
                            .iter()
                            .any(|row| credential_serves_fact(row, fact, &catalog, self)))
            })
            .map(|fact| make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref()))
            .collect();
        (routes, fallback)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn quiet_confirmed_watch_keeps_every_listing_age_gate_open() {
        let mut cache = BrocaCredentials::default();
        let mut login = credential("oauth:anthropic:login", &["anthropic"]);
        login.auth_method = Some("claude-code".into());
        let mut key = credential("apikey:metered:key", &["metered"]);
        key.auth_method = Some("apikey".into());
        key.credential_type = "apikey".into();
        let mut rows = listing(vec![login, key]);
        rows.grant_tuples
            .push(cortexkit_claustrum_client::ListedGrant {
                selector_kind: "category".into(),
                selector: "media-provider".into(),
                operation: "list".into(),
            });
        let now = crate::now_ms();
        cache.apply_listing(&rows, now - MAX_AGE_MS - 1);
        assert!(!cache.listing_is_fresh(now));
        cache.set_watch_confirmed(Some("unapplied-view"));
        assert!(!cache.listing_is_fresh(now));
        cache.set_watch_confirmed(Some(&rows.view));
        assert!(cache.listing_is_fresh(now));
        assert!(!cache.api_key_credentials_by_provider(now).is_empty());
        assert!(!cache
            .serving_identities(&crate::tests::fact("anthropic", "sonnet"))
            .is_empty());
        assert!(cache.media_listing_ready(now));
        assert!(cache.pool_keys("metered", now).is_ok());
        assert!(cache.fallback_reason(now).is_none());
        cache.set_watch_confirmed(None);
        assert!(!cache.listing_is_fresh(now));
        assert!(cache.api_key_credentials_by_provider(now).is_empty());
        assert!(cache
            .serving_identities(&crate::tests::fact("anthropic", "sonnet"))
            .is_empty());
        assert!(!cache.media_listing_ready(now));
        assert!(cache.pool_keys("metered", now).is_err());
        assert!(cache.fallback_reason(now).is_some());
    }

    #[test]
    fn positive_listing_keeps_a_still_allowed_roster_method() {
        let mut credentials = super::BrocaCredentials::default();
        let mut fact = crate::tests::fact("vendor", "listed");
        credentials.catalog_providers = [&fact].into_iter().collect();
        credentials.serving.insert(
            "vendor/listed".into(),
            super::ServingMethod {
                auth_method: Some("custom-signin".into()),
                reason: "roster".into(),
            },
        );
        fact.auth_methods = Some(vec!["apikey".into(), "custom-signin".into()]);
        fact.login_methods_documented = true;
        let catalog = [&fact].into_iter().collect();
        assert_eq!(
            credentials.serving_method(&fact, &catalog).reason,
            "roster",
            "new positive coverage must not discard a valid quota roster choice"
        );

        let mut other = crate::tests::fact("other-vendor", "listed");
        other.auth_methods = Some(vec!["another-signin".into()]);
        other.login_methods_documented = true;
        let catalog = [&fact, &other].into_iter().collect();
        assert_eq!(
            credentials.serving_method(&fact, &catalog).reason,
            "roster",
            "a different provider's coverage must not affect this model's pricing"
        );
    }

    #[test]
    fn serving_generation_compares_values_and_preserves_old_snapshots() {
        let mut credentials = super::BrocaCredentials::api_test_credentials(&facts());
        let original = credentials.snapshot();
        let initial = credentials.serving_generation();
        assert_eq!(initial, original.serving_generation());
        let listing = cortexkit_claustrum_client::ScopedCredentialListing {
            view: credentials.view.clone().unwrap(),
            credentials: credentials.listed_credentials().to_vec(),
            ..Default::default()
        };
        credentials.apply_listing(&listing, 999);
        assert_eq!(
            initial,
            credentials.serving_generation(),
            "reached_at_ms is not serving data"
        );
        let catalog = credentials.billing_catalog.clone();
        credentials.publish_billing_catalog(catalog);
        assert_eq!(initial, credentials.serving_generation());
        let declarations = credentials.runner_declarations.clone();
        credentials.runner_declarations = declarations;
        assert_eq!(initial, credentials.serving_generation());
        credentials.refresh_serving_methods(&Default::default(), &facts(), 999, &[]);
        let refreshed = credentials.serving_generation();
        credentials.refresh_serving_methods(&Default::default(), &facts(), 1000, &[]);
        assert_eq!(refreshed, credentials.serving_generation());
        let mut previous = refreshed;
        macro_rules! changes {
            ($change:expr) => {{
                $change;
                let next = credentials.serving_generation();
                assert_ne!(previous, next);
                previous = next;
            }};
        }
        changes!(credentials.view = Some("different-view".into()));
        changes!(credentials.listed[0].state = "disabled".into());
        changes!(credentials
            .wire_apis
            .insert("provider/model".into(), "messages".into()));
        changes!(credentials.wire_api_models.insert("provider/model".into()));
        changes!(credentials
            .priced_targets
            .insert("pool".into(), "provider".into()));
        changes!(credentials
            .runner_routed_accounts
            .insert("runner".into(), vec!["oauth".into()]));
        changes!(credentials.billing_catalog = Default::default());
        let declaration = crate::routing_v1::runners::parse_declaration(&serde_json::json!({
            "runner": {"pairs":[{"wire_api":"messages","auth_method":"oauth"}],"follows_account_choice":true}
        })).unwrap().unwrap();
        changes!(credentials
            .runner_declarations
            .insert("generation-runner".into(), declaration));
        assert_ne!(previous, initial);
        assert_eq!(
            original.serving_generation(),
            initial,
            "an old request keeps its own generation"
        );
    }

    fn serving_refresh_case(
        change: impl FnOnce(
            &striatum_store::SqliteRouteStateStore,
            &mut BrocaCredentials,
            &mut crate::account_quota::AccountQuotaState,
        ),
        rebuilds: usize,
    ) {
        use crate::account_quota::{RosterIdentity, StoredReading, VaultRoster};
        use crate::routing_v1::request_cpu_bench as bench;
        let fixture = bench::representative_fixture(bench::FixtureCounts {
            subscription_rules: 1,
            exclusion_rules: 1,
            runner_route_rules: 0,
            catalog_facts: 3,
            listed_credentials: 2,
        });
        let mut credentials = fixture.credentials().lock().unwrap().snapshot();
        let mut state = crate::account_quota::AccountQuotaState {
            roster: Some(VaultRoster {
                view: "roster".into(),
                fetched_at_ms: 1,
                grants: vec![],
                identities: vec![RosterIdentity {
                    refresh_adapter: "adapter".into(),
                    account_id: "account".into(),
                    credential_ids: vec![credentials.listed[0].id.clone()],
                    active_credential_ids: vec![],
                    first_seen_ms: 1,
                    plan_tier: None,
                }],
            }),
            ..Default::default()
        };
        state.readings.readings.insert(
            ("adapter".into(), "account".into()),
            StoredReading {
                provider: "adapter".into(),
                account_id: "account".into(),
                entry: serde_json::json!({"apiProvider":"cpu-provider-0"}),
                received_at_ms: 1,
                tombstoned_at_ms: None,
            },
        );
        let catalog = fixture.store().get_selection_catalog().unwrap();
        let initial = credentials.serving_refresh_builds;
        credentials.refresh_serving_methods_if_changed(&state, &catalog, 1, &[]);
        assert_eq!(credentials.serving_refresh_builds, initial + 1);
        change(fixture.store(), &mut credentials, &mut state);
        let catalog = fixture.store().get_selection_catalog().unwrap();
        credentials.refresh_serving_methods_if_changed(&state, &catalog, 100_000_000, &[]);
        assert_eq!(credentials.serving_refresh_builds, initial + rebuilds);
    }

    #[test]
    fn serving_refresh_unchanged_second_pass_does_not_rebuild() {
        serving_refresh_case(|_, _, _| {}, 1);
    }

    #[test]
    fn serving_refresh_catalog_generation_change_rebuilds() {
        serving_refresh_case(
            |store, _, _| {
                store
                    .with_writer_for_test(|connection| {
                        connection.execute(
                            "UPDATE model_facts SET auth_methods_json='[\"apikey\"]'",
                            [],
                        )
                    })
                    .unwrap();
            },
            2,
        );
    }

    #[test]
    fn serving_refresh_credential_listing_change_rebuilds() {
        serving_refresh_case(
            |_, credentials, _| {
                let mut rows = credentials.listed.clone();
                rows[0].state = "disabled".into();
                credentials.apply_listing(
                    &ScopedCredentialListing {
                        view: "changed-listing".into(),
                        credentials: rows,
                        ..Default::default()
                    },
                    2,
                );
            },
            2,
        );
    }

    #[test]
    fn serving_refresh_equal_listing_with_new_view_rebuilds() {
        serving_refresh_case(
            |_, credentials, _| {
                let rows = credentials.listed.clone();
                credentials.apply_listing(
                    &ScopedCredentialListing {
                        view: "new-view".into(),
                        credentials: rows,
                        ..Default::default()
                    },
                    2,
                );
            },
            2,
        );
    }

    #[test]
    fn serving_refresh_runner_declarations_change_rebuilds() {
        serving_refresh_case(
            |_, credentials, _| {
                let declaration = crate::routing_v1::runners::parse_declaration(&serde_json::json!({
                "runner":{"pairs":[{"wire_api":"messages","auth_method":"oauth"}],"follows_account_choice":true}
            })).unwrap().unwrap();
                credentials
                    .runner_declarations
                    .insert("changed-runner".into(), declaration);
            },
            2,
        );
    }

    #[test]
    fn serving_refresh_billing_catalog_change_rebuilds() {
        serving_refresh_case(
            |_, credentials, _| {
                assert_ne!(credentials.billing_catalog, Default::default());
                credentials.publish_billing_catalog(Default::default());
            },
            2,
        );
    }

    #[test]
    fn serving_refresh_wire_apis_change_rebuilds() {
        serving_refresh_case(
            |_, credentials, _| {
                credentials
                    .wire_apis
                    .insert("cpu-provider-0/model-0".into(), "messages".into());
            },
            2,
        );
    }

    #[test]
    fn serving_refresh_roster_identity_change_rebuilds() {
        serving_refresh_case(
            |_, _, state| {
                state.roster.as_mut().unwrap().identities[0]
                    .credential_ids
                    .push("another-credential".into());
            },
            2,
        );
    }

    #[test]
    fn serving_refresh_account_provider_link_change_rebuilds() {
        serving_refresh_case(
            |_, _, state| {
                state.readings.readings.values_mut().next().unwrap().entry["apiProvider"] =
                    serde_json::json!("another-provider");
            },
            2,
        );
    }

    #[test]
    fn serving_refresh_quota_value_only_change_does_not_rebuild() {
        serving_refresh_case(
            |_, _, state| {
                let reading = state.readings.readings.values_mut().next().unwrap();
                reading.entry["usage"] = serde_json::json!({"primary":{"used_percent":90}});
                reading.received_at_ms += 60_000;
                state.roster.as_mut().unwrap().fetched_at_ms += 60_000;
            },
            1,
        );
    }

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
        cache.catalog_providers.providers = ["kimi-for-coding".into()].into();
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
    fn plane_keys_include_selected_account_methods_when_provider_serving_is_ambiguous() {
        let mut credentials = BrocaCredentials::default();
        credentials.listed = ["apikey", "antigravity"]
            .into_iter()
            .map(|method| ListedCredential {
                id: format!("{method}:google"),
                state: "active".into(),
                categories: vec!["llm-provider".into()],
                provider_ids: vec!["google".into()],
                auth_method: Some(method.into()),
                ..Default::default()
            })
            .collect();
        credentials.serving = [
            (
                "google/gemini-4-carbon".into(),
                ServingMethod {
                    auth_method: Some("apikey".into()),
                    reason: "roster".into(),
                },
            ),
            (
                "google/antigravity-gemini-4-carbon".into(),
                choose_method(
                    BTreeSet::new(),
                    BTreeSet::from(["apikey".into(), "antigravity".into()]),
                ),
            ),
        ]
        .into();
        assert_eq!(
            credentials.plane_keys(),
            BTreeSet::from([
                ("google".into(), "apikey".into()),
                ("google".into(), "antigravity".into()),
            ])
        );
    }

    #[test]
    fn gateway_category_resolves_its_own_provider_to_the_anthropic_plane() {
        let mut row = credential("company-gateway", &["anthropic-gw-acme"]);
        row.categories = vec!["llm-gateway".into()];
        row.serves = vec!["anthropic".into()];
        row.auth_method = Some("gateway".into());
        let facts = vec![crate::tests::fact("anthropic", "claude")];
        let mut cache = BrocaCredentials::default();
        cache.apply_listing(&listing(vec![row.clone()]), 1);
        cache.refresh_serving_methods(&Default::default(), &facts, 1, &[]);
        assert_eq!(
            cache.catalog_targets(&facts),
            BTreeMap::from([("anthropic-gw-acme".into(), "anthropic".into())])
        );
        assert_eq!(
            cache.serving["anthropic-gw-acme/claude"]
                .auth_method
                .as_deref(),
            Some("gateway")
        );
        assert_eq!(
            cache.plane_keys(),
            BTreeSet::from([("anthropic".into(), "gateway".into())])
        );
        assert_eq!(cache.health_report(1).status, HealthStatus::Ok);
        assert_eq!(
            cache.health_report(1).metrics.unwrap()["activeCredentials"],
            1
        );

        for state in ["disabled", "revoked"] {
            row.state = state.into();
            cache.apply_listing(
                &ScopedCredentialListing {
                    view: state.into(),
                    credentials: vec![row.clone()],
                    ..Default::default()
                },
                2,
            );
            cache.refresh_serving_methods(&Default::default(), &facts, 2, &[]);
            assert!(cache.catalog_targets(&facts).is_empty());
            assert!(cache.plane_keys().is_empty());
        }
    }

    #[test]
    fn missing_gateway_grant_keeps_provider_listing_and_warns_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Warnings(Arc<AtomicUsize>);
        impl tracing::Subscriber for Warnings {
            fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
                *metadata.level() == tracing::Level::WARN
            }
            fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn event(&self, _: &tracing::Event<'_>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
        }
        let warnings = Arc::new(AtomicUsize::new(0));
        let _subscriber = tracing::subscriber::set_default(Warnings(warnings.clone()));
        let mut cache = BrocaCredentials::default();
        let provider_listing = listing(vec![credential("direct-key", &["anthropic"])]);
        let facts = vec![crate::tests::fact("anthropic", "claude")];
        assert!(cache.apply_listing(&provider_listing, 1));
        assert!(!cache.apply_listing(&provider_listing, 2));
        cache.refresh_serving_methods(&Default::default(), &facts, 2, &[]);
        assert_eq!(warnings.load(Ordering::SeqCst), 1);
        assert_eq!(cache.listed_credentials().len(), 1);
        assert_eq!(cache.health_report(2).status, HealthStatus::Ok);
        assert!(!cache
            .plane_keys()
            .iter()
            .any(|(_, method)| method == "gateway"));

        let mut granted = provider_listing;
        granted
            .grant_tuples
            .push(cortexkit_claustrum_client::ListedGrant {
                selector_kind: "category".into(),
                selector: "llm-gateway".into(),
                operation: "list".into(),
            });
        let mut granted_cache = BrocaCredentials::default();
        granted_cache.apply_listing(&granted, 3);
        assert_eq!(warnings.load(Ordering::SeqCst), 1);
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
        fact.billing_plane = Some(striatum_core::billing::BillingPlane {
            provider_id: "mapped".into(),
            auth_method: "apikey".into(),
            kind: "upstream".into(),
            selection_reason: "test credential".into(),
            rates: std::collections::BTreeMap::from([(
                "rate.output".into(),
                striatum_core::billing::BillingRate::Priced { rate: 0.75 },
            )]),
        });
        fact
    }

    fn admission_refusals(
        cache: &BrocaCredentials,
        facts: &[ModelFact],
        consumer: crate::select_policy::Consumer,
    ) -> std::collections::HashMap<String, String> {
        crate::select_policy::candidate_rejections_priced(
            &cache.admission_facts(facts, true, 1),
            Some(consumer),
            true,
            None,
            cache.subscription_plan_providers().as_ref(),
            None,
        )
    }

    #[test]
    fn active_apikey_mapping_admits_metered_routes_without_changing_catalog_policy() {
        let consumer = true;
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
        let consumer = true;
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
            let with_vault = cache.admission_facts(&facts, false, 1);
            let without_vault = absent.admission_facts(&facts, false, 1);
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

    #[test]
    fn quota_account_join_preserves_vault_ids_and_filters_producer_linkages() {
        let mut credentials = BrocaCredentials::default();
        let mut fixed = credential("fixed", &["anthropic"]);
        fixed.account_id = Some("vault-account".into());
        let mut inferred = credential("inferred", &[]);
        inferred.serves = vec!["anthropic".into()];
        credentials.apply_listing(
            &listing(vec![
                fixed,
                inferred,
                credential("joined", &["anthropic"]),
                credential("ambiguous", &["anthropic"]),
                credential("wrong", &["anthropic"]),
                credential("gone", &["anthropic"]),
            ]),
            1,
        );
        credentials.refresh_serving_methods(&Default::default(), &facts(), 1, &[]);
        let mut quota = crate::account_quota::AccountQuotaState::default();
        for (n, (id, account, provider, tombstoned)) in [
            ("fixed", "producer-account", "anthropic", false),
            ("inferred", "inferred-account", "anthropic", false),
            ("joined", "joined-account", "anthropic", false),
            ("joined", "joined-account", "anthropic", false),
            ("ambiguous", "a", "anthropic", false),
            ("ambiguous", "b", "anthropic", false),
            ("wrong", "wrong-account", "uncredentialed", false),
            ("gone", "gone-account", "anthropic", true),
        ]
        .into_iter()
        .enumerate()
        {
            quota.readings.readings.insert(
                (format!("source-{n}"), account.into()),
                crate::account_quota::StoredReading {
                    provider: format!("source-{n}"),
                    account_id: account.into(),
                    entry: serde_json::json!({"credentialRef":id,"apiProvider":provider}),
                    received_at_ms: 1,
                    tombstoned_at_ms: tombstoned.then_some(1),
                },
            );
        }
        let accounts = credentials.quota_account_ids(&quota);
        assert_eq!(accounts["fixed"].as_deref(), Some("vault-account"));
        assert_eq!(accounts["inferred"].as_deref(), Some("inferred-account"));
        assert_eq!(accounts["joined"].as_deref(), Some("joined-account"));
        for id in ["ambiguous", "wrong", "gone"] {
            assert!(accounts[id].is_none());
        }
        assert!(credentials
            .listed_credentials()
            .iter()
            .find(|r| r.id == "joined")
            .unwrap()
            .account_id
            .is_none());
    }

    #[test]
    fn request_snapshot_shares_catalog_and_is_immutable_across_refresh() {
        let mut credentials = BrocaCredentials::api_test_credentials(&facts());
        let snapshot = credentials.snapshot();
        assert!(Arc::ptr_eq(&credentials.inner, &snapshot.inner));
        credentials.apply_listing(&listing(vec![credential("new", &["anthropic"])]), 2);
        assert!(!Arc::ptr_eq(&credentials.inner, &snapshot.inner));
        assert_ne!(
            credentials.listed_credentials()[0].id,
            snapshot.listed_credentials()[0].id
        );
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
        assert_eq!(substrate, striatum_core::AvailabilitySubstrate::Broca);
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
            assert_eq!(substrate, striatum_core::AvailabilitySubstrate::Broca);
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
