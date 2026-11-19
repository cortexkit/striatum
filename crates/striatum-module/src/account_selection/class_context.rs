//! Request-local account classes and indexes.
//! Keep credential coverage and eligibility together so selection and allocation use the same snapshot.

use super::*;

#[derive(Default)]
pub(crate) struct LoginConditions {
    reasons: BTreeSet<String>,
    credential_ids: BTreeSet<String>,
}

impl LoginConditions {
    pub(crate) fn extend(&mut self, other: Self) {
        self.reasons.extend(other.reasons);
        self.credential_ids.extend(other.credential_ids);
    }

    pub(crate) fn detail(&self) -> Value {
        json!({
            "cause": "login_condition", "reasons": self.reasons, "credential_ids": self.credential_ids,
        })
    }
}

// Tests count eligibility calls rather than inferring avoided work from timing.
#[cfg(test)]
pub(crate) static SELECT_ELIGIBILITY_PROBES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// One immutable vault/config/catalog view shared by selection and allocation.
#[derive(Clone)]
pub(crate) struct ClassContext {
    pub(super) serving_index: std::sync::Arc<crate::serving_index::ServingIndex>,
    pub(super) indexes: RequestIndexes,
    // Clones share predicate-validated entries, never references into a clone's
    // credential vector. Every hit rechecks the resolver's effective inputs.
    class_memo: std::sync::Arc<std::sync::Mutex<ClassMemo>>,
    pub(super) rechecks: Vec<striatum_store::FenceRecheck>,
    pub(super) probe_authorizations: Vec<striatum_store::ProbeAuthorization>,
    pub(super) account_ids: crate::broca_credentials::CredentialAccounts,
    pub credentials: crate::broca_credentials::BrocaCredentials,
    pub config: std::sync::Arc<crate::config_overrides::LiveConfig>,
    pub facts: Vec<striatum_core::ModelFact>,
    pub refusals: Vec<striatum_core::route_state_store::CredentialEntitlementFence>,
    pub quota: AccountQuotaState,
    pub fences: BTreeMap<String, Vec<Fence>>,
    pub now_ms: i64,
    pub(super) eligibility: Vec<striatum_store::AccountEligibilityFact>,
    pub(super) size_refusals: Vec<striatum_store::AccountSizeRefusal>,
    pub(super) route_fences: Vec<striatum_store::AccountRouteFence>,
    transient_route_fences: Vec<striatum_store::AccountRouteFence>,
    pub(super) pool_walls: BTreeSet<(String, String)>,
    pub(super) catalog: crate::broca_credentials::AuthCatalog,
    pub(super) targets: BTreeMap<String, String>,
    pub(super) vendorless_quota: quota_route::VendorlessQuota,
    pub(super) tier_accounts: Option<BTreeMap<String, Vec<String>>>,
    pub(super) continuation_credential: Option<String>,
    pub(super) media_route: bool,
    pub(super) probe_scope:
        std::sync::Weak<std::sync::Mutex<crate::broca_credentials::BrocaCredentials>>,
    pub(super) probe_owner: Option<String>,
    selection_probes: Option<std::sync::Arc<probe_reservations::Selection>>,
}

pub(crate) enum ProbeBinding {
    Bound,
    Lost { retry_at: i64 },
}

/// The account recovery named by selection, including the stored block class.
#[derive(Clone, Debug)]
pub(crate) struct AccountBlock {
    pub reset_at_ms: Option<i64>,
    /// When to check again if another request is testing whether the account can serve traffic.
    pub test_check_at_ms: Option<i64>,
    pub lifetime: striatum_core::blocks::LifetimeClass,
    pub credential_code: Option<String>,
}

impl AccountBlock {
    pub(crate) fn retry_at_ms(&self) -> Option<i64> {
        self.reset_at_ms
            .into_iter()
            .chain(self.test_check_at_ms)
            .min()
    }
}

/// Why a legacy launch could not claim the probes its route needs.
// Keep the details in test failures when a legacy launch is unexpectedly refused.
#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum LaunchRefusal {
    /// Another request holds the probe, or it is not due yet: the caller
    /// may try another route, or retry at `retry_at`.
    Lost { retry_at: i64 },
    /// A refusal with no retry time, with its contract code.
    Refused { code: String, message: String },
}

#[cfg(test)]
impl LaunchRefusal {
    /// A store failure, including a writer batch whose state disagreed with
    /// SQL on both of its runs: the caller cannot tell whether any account
    /// is eligible.
    pub(crate) fn store(error: impl std::fmt::Display) -> Self {
        Self::Refused {
            code: "store_error".into(),
            message: error.to_string(),
        }
    }
}

type ClassMemo = std::collections::HashMap<
    String,
    std::collections::HashMap<String, [Option<ClassMemoEntry>; 4]>,
>;

#[derive(Clone, Copy, PartialEq, Eq)]
struct ClassRowInputs {
    serving: bool,
    covered: bool,
    metered: bool,
    usable: bool,
}

struct ClassMemoEntry {
    inputs: Vec<ClassRowInputs>,
    rows: Vec<usize>,
    subscription: bool,
    #[cfg(test)]
    misses: usize,
}

#[derive(Default)]
pub(super) struct RequestIndexes {
    blocks: std::sync::OnceLock<BlockIndex>,
    readings: std::sync::OnceLock<ReadingIndex>,
    rechecks: std::sync::OnceLock<RecheckIndex>,
    pub(super) facts: std::sync::OnceLock<fact_index::FactIndex>,
    probe_holds: std::sync::Mutex<std::collections::HashMap<ProbeHoldKey, i64>>,
}

/// Provider, credential and the context's probe owner.
type ProbeHoldKey = (String, String, Option<String>);

#[cfg(test)]
static INDEX_BUILDS: [std::sync::atomic::AtomicU64; 3] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 3];

#[cfg(test)]
pub(crate) fn take_index_build_counts() -> [u64; 3] {
    std::array::from_fn(|index| INDEX_BUILDS[index].swap(0, std::sync::atomic::Ordering::Relaxed))
}

#[cfg(test)]
fn count_index_build(index: usize) {
    INDEX_BUILDS[index].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

impl Clone for RequestIndexes {
    fn clone(&self) -> Self {
        // Clones share the structural ServingIndex, but not the dynamic cells:
        // each clone can reserve a different probe and read newer refusal and
        // fence evidence without changing the original context's answers.
        // Remembered probe holds must survive cloning: a hold can disappear
        // when another selection records a durable claim this snapshot cannot see.
        Self {
            probe_holds: std::sync::Mutex::new(self.probe_holds.lock().unwrap().clone()),
            ..Self::default()
        }
    }
}

#[derive(Default)]
struct BlockIndex {
    eligibility: BTreeMap<(String, String), Vec<usize>>,
    refusals: BTreeSet<(String, String)>,
}

struct ReadingRecord {
    reading: account_quota::StoredReading,
    decoded: Option<AccountEntry>,
    fetched: Option<i64>,
}

#[derive(Default)]
struct ReadingIndex {
    accounts: BTreeMap<String, Vec<ReadingRecord>>,
    quota_signal: BTreeSet<String>,
}

type RecheckScopes =
    BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<usize>>>>>;
type DueKey = (usize, String, String, i64, Option<String>, usize);

#[derive(Default)]
struct RecheckIndex {
    scopes: RecheckScopes,
    due: std::sync::Mutex<std::collections::HashMap<DueKey, bool>>,
}

impl ClassContext {
    pub(super) fn replace_rechecks(&mut self, rechecks: Vec<striatum_store::FenceRecheck>) {
        self.rechecks = rechecks;
        self.indexes.rechecks.take();
    }

    fn replace_fences(&mut self, fences: BTreeMap<String, Vec<Fence>>) {
        self.fences = fences;
        self.indexes.rechecks.take();
    }

    pub(super) fn replace_refusals(
        &mut self,
        refusals: Vec<striatum_core::route_state_store::CredentialEntitlementFence>,
    ) {
        self.refusals = refusals;
        self.indexes.blocks.take();
    }

    fn block_index(&self) -> &BlockIndex {
        self.indexes.blocks.get_or_init(|| {
            #[cfg(test)]
            count_index_build(0);
            let mut index = BlockIndex::default();
            for (i, fact) in self.eligibility.iter().enumerate() {
                index
                    .eligibility
                    .entry((fact.provider.clone(), fact.credential_id.clone()))
                    .or_default()
                    .push(i);
            }
            index.refusals.extend(
                self.refusals
                    .iter()
                    .map(|f| (f.provider_id.clone(), f.credential_id.clone())),
            );
            index
        })
    }

    fn reading_index(&self) -> &ReadingIndex {
        self.indexes.readings.get_or_init(|| {
            #[cfg(test)]
            count_index_build(1);
            let mut index = ReadingIndex::default();
            for reading in self
                .quota
                .readings
                .readings
                .values()
                .filter(|r| r.tombstoned_at_ms.is_none())
            {
                let decoded: Option<AccountEntry> =
                    serde_json::from_value(reading.entry.clone()).ok();
                let fetched = reading
                    .entry
                    .get("fetchedAt")
                    .and_then(Value::as_str)
                    .and_then(account_quota::parse_rfc3339_ms);
                index
                    .accounts
                    .entry(reading.account_id.clone())
                    .or_default()
                    .push(ReadingRecord {
                        reading: reading.clone(),
                        decoded,
                        fetched,
                    });
            }
            index.quota_signal.extend(
                index
                    .accounts
                    .keys()
                    .filter(|account| {
                        crate::credential_fences::has_quota_signal_for_account(&self.quota, account)
                    })
                    .cloned(),
            );
            index
        })
    }

    pub(super) fn indexed_origins<'a>(
        &'a self,
        provider: &str,
        account: &str,
        credential: &str,
        model: &str,
    ) -> impl Iterator<Item = &'a striatum_store::FenceRecheck> {
        let index = self.indexes.rechecks.get_or_init(|| {
            #[cfg(test)]
            count_index_build(2);
            let mut index = RecheckIndex::default();
            for (i, origin) in self.rechecks.iter().enumerate() {
                let mapped = origin
                    .credential_id
                    .as_deref()
                    .filter(|_| {
                        origin.quota
                            || matches!(
                                origin.kind.as_str(),
                                "account_route" | "non_quota_route" | "transient_route"
                            )
                    })
                    .and_then(|id| {
                        crate::credential_fences::credential_account_id(&self.account_ids, id)
                    });
                let (account, credential) = if let Some(account) = mapped {
                    (account, String::new())
                } else {
                    (
                        origin.account_id.clone().unwrap_or_default(),
                        origin.credential_id.clone().unwrap_or_default(),
                    )
                };
                index
                    .scopes
                    .entry(origin.provider.clone())
                    .or_default()
                    .entry(account)
                    .or_default()
                    .entry(credential)
                    .or_default()
                    .entry(origin.model.clone().unwrap_or_default())
                    .or_default()
                    .push(i);
            }
            index
        });
        let mut positions = Vec::new();
        for p in std::iter::once("").chain((!provider.is_empty()).then_some(provider)) {
            let Some(accounts) = index.scopes.get(p) else {
                continue;
            };
            for a in std::iter::once("").chain((!account.is_empty()).then_some(account)) {
                let Some(credentials) = accounts.get(a) else {
                    continue;
                };
                for c in std::iter::once("").chain((!credential.is_empty()).then_some(credential)) {
                    let Some(models) = credentials.get(c) else {
                        continue;
                    };
                    for m in std::iter::once("").chain((!model.is_empty()).then_some(model)) {
                        if let Some(rows) = models.get(m) {
                            positions.extend(rows.iter().copied());
                        }
                    }
                }
            }
        }
        positions.into_iter().map(|i| &self.rechecks[i])
    }

    fn indexed_origin_due(
        &self,
        origin: &striatum_store::FenceRecheck,
        account: &str,
        model: &str,
    ) -> bool {
        let key = (
            origin as *const _ as usize,
            account.to_owned(),
            model.to_owned(),
            self.now_ms,
            self.probe_owner.clone(),
            std::sync::Arc::as_ptr(&self.config) as usize,
        );
        let mut due = self
            .indexes
            .rechecks
            .get()
            .expect("origins indexed before due lookup")
            .due
            .lock()
            .unwrap();
        *due.entry(key)
            .or_insert_with(|| self.origin_due(origin, account, model))
    }

    /// When another owner's probe reservation blocks this credential.
    ///
    /// A reservation can vanish while a request is still running: its holder
    /// may claim the probe, which replaces the reservation with a stored claim
    /// this context's snapshot cannot see, or a selection may finish scoring
    /// and release it. A hold this context has seen therefore stays in force
    /// for the context's life. The empty-roster refusal then names the hold
    /// that emptied the roster, and its end as the retry time, instead of
    /// finding no cause and answering `account_not_authorized`. A remembered
    /// hold still ends at its expiry, as a live one would.
    pub(super) fn probe_reserved_until(&self, provider: &str, credential: &str) -> Option<i64> {
        let key = (
            provider.to_owned(),
            credential.to_owned(),
            self.probe_owner.clone(),
        );
        if let Some(until) = self
            .probe_holds()
            .get(&key)
            .filter(|until| **until > self.now_ms)
        {
            return Some(*until);
        }
        let until = probe_reservations::blocked_until(
            &self.probe_scope,
            provider,
            credential,
            self.probe_owner.as_deref(),
            self.now_ms,
        )?;
        self.probe_holds().insert(key, until);
        Some(until)
    }

    /// Record a hold that refused this context's probe claim, so a later
    /// empty-roster refusal in the same request still sees it.
    pub(super) fn note_probe_reserved(&self, provider: &str, credential: &str, until: i64) {
        self.probe_holds().insert(
            (
                provider.to_owned(),
                credential.to_owned(),
                self.probe_owner.clone(),
            ),
            until,
        );
    }

    pub(crate) fn remember_probe_holds(&self, other: &Self) {
        let holds = other.probe_holds().clone();
        self.probe_holds().extend(holds);
    }

    fn probe_holds(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::HashMap<ProbeHoldKey, i64>> {
        self.indexes
            .probe_holds
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
type ProbeBindHook = Box<dyn FnOnce(&SqliteRouteStateStore) + Send>;

/// Where a test hook runs: before a probe's durable binding, or in a launch
/// claim between its read of the published state and its writer command.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum HookPoint {
    ProbeBind,
    LaunchCommand,
}

#[cfg(test)]
static BEFORE_PROBE_BIND: std::sync::Mutex<Vec<(usize, HookPoint, ProbeBindHook)>> =
    std::sync::Mutex::new(Vec::new());

/// Run once after scoring chose a probe and before its durable binding.
#[cfg(test)]
pub(crate) fn before_probe_bind(
    credentials: &crate::broca_credentials::SharedBrocaCredentials,
    hook: impl FnOnce(&SqliteRouteStateStore) + Send + 'static,
) {
    BEFORE_PROBE_BIND.lock().unwrap().push((
        std::sync::Arc::as_ptr(credentials) as usize,
        HookPoint::ProbeBind,
        Box::new(hook),
    ));
}

/// Run once in a launch claim, after it read the published state to choose
/// its branch and before its writer command, so a test can commit a block
/// on the far side of that read.
#[cfg(test)]
pub(crate) fn before_launch_command(
    credentials: &crate::broca_credentials::SharedBrocaCredentials,
    hook: impl FnOnce(&SqliteRouteStateStore) + Send + 'static,
) {
    BEFORE_PROBE_BIND.lock().unwrap().push((
        std::sync::Arc::as_ptr(credentials) as usize,
        HookPoint::LaunchCommand,
        Box::new(hook),
    ));
}

#[cfg(test)]
fn run_before_probe_bind_hook(
    scope: &std::sync::Weak<std::sync::Mutex<crate::broca_credentials::BrocaCredentials>>,
    store: &SqliteRouteStateStore,
) {
    run_hook(scope, store, HookPoint::ProbeBind);
}

#[cfg(test)]
fn run_hook(
    scope: &std::sync::Weak<std::sync::Mutex<crate::broca_credentials::BrocaCredentials>>,
    store: &SqliteRouteStateStore,
    point: HookPoint,
) {
    let hook = {
        let mut hooks = BEFORE_PROBE_BIND.lock().unwrap();
        hooks
            .iter()
            .position(|(key, at, _)| *key == scope.as_ptr() as usize && *at == point)
            .map(|at| hooks.remove(at).2)
    };
    if let Some(hook) = hook {
        hook(store);
    }
}

// Replay tests compare old and new context repairs, including endpoint work
// dispatched to blocking threads, so the test switch must be shared.
#[cfg(test)]
pub(crate) static LEGACY_CONTEXT_REPAIRS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
thread_local! {
    pub(crate) static CONTEXT_LOADS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub(crate) struct RouteClass<'a> {
    pub rows: Vec<&'a cortexkit_claustrum_client::ListedCredential>,
    pub subscription: bool,
}

pub(crate) fn media_credential_serves(
    row: &cortexkit_claustrum_client::ListedCredential,
    provider: &str,
) -> bool {
    row.state == "active"
        && row
            .categories
            .iter()
            .any(|category| category == "media-provider")
        // The vault lists an API key with auth method `apikey`, the same
        // spelling the rest of routing's credential matching uses.
        && row.auth_method.as_deref() == Some("apikey")
        && row.provider_ids.iter().any(|id| id == provider)
}

/// Coverage is established before eligibility. A refusal cannot turn an OAuth
/// subscription into permission to spend on a metered key.
pub(crate) fn resolve_class<'a>(
    rows: &'a [cortexkit_claustrum_client::ListedCredential],
    fact: &striatum_core::ModelFact,
    catalog: &crate::broca_credentials::AuthCatalog,
    credentials: &crate::broca_credentials::CredentialData,
    specs: &(impl striatum_core::provider_model_spec::SpecResolver + ?Sized),
    borrowed: bool,
    priced_per_token: bool,
    metered_fallback: bool,
    usable: impl Fn(&cortexkit_claustrum_client::ListedCredential) -> bool,
) -> RouteClass<'a> {
    let key = format!("{}/{}", fact.provider_id, fact.model_id);
    let serving: Vec<_> = rows
        .iter()
        .filter(|row| {
            crate::broca_credentials::credential_serves_fact(row, fact, catalog, credentials)
        })
        .collect();
    let covered = |row: &&cortexkit_claustrum_client::ListedCredential| {
        !borrowed && specs.resolve(&key, row.credential_type == "oauth")
    };
    let has_subscription = serving.iter().any(covered);
    let can_fallback = metered_fallback
        && serving
            .iter()
            .filter(|row| !covered(row))
            .any(|row| usable(row));
    let subscription = !priced_per_token
        && has_subscription
        && (!can_fallback
            || serving
                .iter()
                .filter(|row| covered(row))
                .any(|row| usable(row)));
    RouteClass {
        rows: serving
            .into_iter()
            .filter(|row| {
                covered(row) == subscription
                    && (!priced_per_token
                        || row
                            .auth_method
                            .as_deref()
                            .is_some_and(striatum_core::billing::is_api_metered_auth_method))
            })
            .collect(),
        subscription,
    }
}

impl ClassContext {
    pub(super) fn account_id<'a>(
        &'a self,
        row: &'a cortexkit_claustrum_client::ListedCredential,
    ) -> &'a str {
        self.joined_account_id(&row.id).unwrap_or(&row.id)
    }

    pub(crate) fn joined_account_id(&self, credential: &str) -> Option<&str> {
        self.account_ids
            .get(credential)
            .and_then(|account| account.as_deref())
    }

    pub(crate) fn probe_accounts(&self) -> BTreeMap<String, String> {
        self.credentials
            .listed_credentials()
            .iter()
            .filter_map(|row| {
                crate::credential_fences::credential_account_id(&self.account_ids, &row.id)
                    .map(|account| (row.id.clone(), account))
            })
            .collect()
    }

    pub(crate) fn pool_recheck_hint(
        &self,
        providers: &[&str],
        model: &str,
        account: &str,
        credential: &str,
    ) -> Option<i64> {
        let origins: Vec<_> = self
            .rechecks
            .iter()
            .filter(|origin| {
                providers.iter().any(|provider| {
                    self.origin_applies(origin, provider, account, credential, model)
                }) && !self.origin_recovered(origin, account)
            })
            .collect();
        let mut quota_at =
            striatum_store::FenceRecheck::quota_group_at(origins.iter().copied(), None);
        if origins.iter().any(|origin| origin.quota) && quota_at <= self.now_ms {
            quota_at = origins
                .iter()
                .filter_map(|origin| origin.reset_check_at)
                .filter(|at| *at > self.now_ms)
                .chain(std::iter::once(self.now_ms.saturating_add(
                    striatum_core::fence_lifetimes::QUOTA_FENCE_RECHECK_MS,
                )))
                .min()
                .unwrap();
        }
        origins
            .iter()
            .filter(|origin| !origin.quota)
            .map(|origin| origin.due_at())
            .chain(std::iter::once(quota_at))
            .filter(|at| *at > self.now_ms)
            .max()
    }
    #[cfg(test)]
    pub(crate) fn fence_config(&self) -> &crate::config_overrides::LiveConfig {
        &self.config
    }
    pub(crate) fn pool_fences_allow(
        &self,
        providers: &[&str],
        model: &str,
        account: &str,
        credential: &str,
    ) -> bool {
        let origins: Vec<_> = self
            .rechecks
            .iter()
            .filter(|origin| {
                !matches!(origin.kind.as_str(), "pool_billing" | "pool_rate")
                    && providers.iter().any(|provider| {
                        self.origin_applies(origin, provider, account, credential, model)
                    })
            })
            .collect();
        let quota_due = striatum_store::FenceRecheck::quota_group_due(
            origins.iter().copied(),
            self.now_ms,
            None,
        );
        origins.iter().all(|origin| {
            if origin.quota {
                self.origin_recovered(origin, account)
                    || (quota_due
                        && !crate::credential_fences::current_quota_reading(
                            &self.quota,
                            account,
                            self.now_ms,
                            &self.config.allocator_policy(&origin.provider),
                        ))
            } else {
                self.origin_due(origin, account, model)
            }
        })
    }
    pub(crate) fn recovered_pool_origins(
        &self,
        provider: &str,
        model: &str,
        account: &str,
        credential: &str,
    ) -> Vec<striatum_store::RecoveredOrigin> {
        self.rechecks
            .iter()
            .filter(|origin| {
                self.origin_applies(origin, provider, account, credential, model)
                    && self.origin_recovered(origin, account)
            })
            .map(|origin| {
                (
                    origin.kind.clone(),
                    origin.origin_key.clone(),
                    origin.evidence.clone(),
                )
            })
            .collect()
    }
    /// A media admission uses only media vault metadata, even when its provider
    /// also serves language models. A neutral model fact lets the allocator
    /// look up quota readings by provider/model without inventing LLM scores
    /// or token prices for engine selection.
    pub(crate) fn for_engine(&mut self, provider: &str, model: &str) -> Result<(), String> {
        let fact = serde_json::from_value(json!({
            "providerID": provider, "modelID": model,
            "scores": {"elo": 0, "eq": 0, "speed": 0}, "confidence": "unknown"
        }))
        .map_err(|error| error.to_string())?;
        self.facts
            .retain(|fact| fact.provider_id != provider || fact.model_id != model);
        self.facts.push(fact);
        self.indexes.facts.take();
        self.media_route = true;
        Ok(())
    }

    /// Whether media account admission (`route.account_for`) would find a
    /// usable credential for this engine right now. Engine selection asks this
    /// so it skips a provider whose media keys are all blocked: refused by the
    /// provider (credential fence), under a pooled-key wall, or on an account
    /// with a route fence or transient backoff. A refused key that is due for
    /// its scheduled probe still counts as usable, so the provider can be retried.
    pub(crate) fn engine_has_eligible_credential(
        &mut self,
        provider: &str,
        model: &str,
    ) -> Result<bool, String> {
        self.for_engine(provider, model)?;
        if !self
            .credentials
            .media_listing_ready(self.now_ms.max(0) as u64)
        {
            return Ok(false);
        }
        let (mut roster, _) = self.roster_and_fences_for(provider, model, true);
        self.avoid_transient_accounts(&mut roster, provider, model);
        Ok(roster.roster.is_some_and(|roster| {
            roster
                .accounts
                .iter()
                .any(|account| !account.credentials.is_empty())
        }))
    }
    pub(crate) fn apply_configured_service_tiers(
        &mut self,
        facts: &mut [striatum_core::ModelFact],
        resolve: impl Fn(
            &striatum_core::ModelFact,
        ) -> Option<cortexkit_role_routing::service_tier::ServiceTier>,
        runners: &[crate::config_policy::RunnerPolicy],
        registry: &crate::routing_v1::runners::Registry,
        priced: bool,
    ) -> BTreeMap<String, cortexkit_role_routing::service_tier::ServiceTier> {
        use cortexkit_role_routing::service_tier::ServiceTier;
        let mut chosen = BTreeMap::new();
        let mut accounts = BTreeMap::new();
        for fact in facts {
            self.tier_accounts = None;
            let key = format!("{}/{}", fact.provider_id, fact.model_id);
            let requested = resolve(fact);
            let standard_accounts = self
                .class(fact, priced)
                .rows
                .iter()
                .map(|row| row.id.clone())
                .collect();
            accounts.insert(key.clone(), standard_accounts);
            let Some(requested) = requested else { continue };
            for tier in ServiceTier::ALL
                .iter()
                .rev()
                .copied()
                .filter(|tier| *tier <= requested)
            {
                self.tier_accounts = None;
                if tier.is_standard() {
                    chosen.insert(key.clone(), tier);
                    break;
                }
                self.apply_service_tier(
                    std::slice::from_mut(fact),
                    tier.as_str(),
                    runners,
                    registry,
                    priced,
                    None,
                );
                let eligible = self.service_tier_accounts(&fact.provider_id, &fact.model_id);
                if !eligible.is_empty() {
                    accounts.insert(key.clone(), eligible);
                    chosen.insert(key.clone(), tier);
                    break;
                }
            }
        }
        self.tier_accounts = (!accounts.is_empty()).then_some(accounts);
        chosen
    }

    pub(crate) fn apply_service_tier(
        &mut self,
        facts: &mut [striatum_core::ModelFact],
        tier: &str,
        runners: &[crate::config_policy::RunnerPolicy],
        registry: &crate::routing_v1::runners::Registry,
        priced: bool,
        sticky_credential: Option<&str>,
    ) -> Vec<Value> {
        let mut allowed = BTreeMap::new();
        let mut rejections = Vec::new();
        for fact in facts {
            let key = format!("{}/{}", fact.provider_id, fact.model_id);
            let class = self.class(fact, priced);
            let mut candidates = Vec::new();
            let reject =
                |check: &str, row: Option<&cortexkit_claustrum_client::ListedCredential>| {
                    json!({
                        "route": key,
                        "check": check,
                        "credential_id": row.map(|row| &row.id),
                        "wire_family": row.and_then(|row| row.auth_method.as_deref())
                            .and_then(|method| self.credentials.wire_api(fact, method)),
                    })
                };
            if crate::service_tiers::model_offers(&self.credentials, fact, tier) {
                if class.rows.is_empty() {
                    rejections.push(reject("account", None));
                }
                for row in class.rows {
                    if sticky_credential.is_some_and(|id| id != row.id)
                        || !self.usable_candidate(row, fact, true)
                    {
                        rejections.push(reject("account", Some(row)));
                        continue;
                    }
                    if !runners.iter().any(|runner| {
                        registry.runs(runner, &striatum_core::fact_to_route(fact).route_key, &key)
                            && registry.get(runner).is_some_and(|runner| {
                                crate::service_tiers::runner_supports(
                                    &self.credentials,
                                    runner,
                                    fact,
                                    row,
                                    tier,
                                )
                            })
                    }) {
                        rejections.push(reject("runner", Some(row)));
                        continue;
                    }
                    let account = self.account_id(row);
                    let credits = self
                        .quota
                        .readings
                        .readings
                        .values()
                        .filter(|reading| {
                            reading.account_id == account && reading.tombstoned_at_ms.is_none()
                        })
                        .max_by_key(|reading| reading.received_at_ms)
                        .is_some_and(|reading| {
                            crate::service_tiers::drawing_credits(&reading.entry)
                        });
                    if let Some(rate) = crate::service_tiers::plan_rate(
                        &self.credentials.plan_prices,
                        row,
                        &fact.provider_id,
                        tier,
                        credits,
                    ) {
                        candidates.push((row.id.clone(), rate));
                    } else {
                        rejections.push(reject("plan", Some(row)));
                    }
                }
            } else {
                rejections.push(reject("model", None));
            }
            // Accounts on one route share its standard prices. Keep only the
            // least expensive eligible draw, so later allocation cannot silently
            // replace the account cost used to rank that route.
            let lowest = candidates
                .iter()
                .map(|(_, rate)| *rate)
                .min_by(f64::total_cmp);
            let ids = candidates
                .into_iter()
                .filter(|(_, rate)| Some(*rate) == lowest)
                .map(|(id, _)| id)
                .collect();
            if let Some(rate) = lowest {
                crate::service_tiers::price_fact(
                    &self.credentials,
                    fact,
                    tier,
                    class.subscription,
                    rate,
                );
            }
            allowed.insert(key, ids);
        }
        self.tier_accounts = Some(allowed);
        rejections
    }

    pub(crate) fn service_tier_accounts(&self, provider: &str, model: &str) -> Vec<String> {
        self.tier_accounts
            .as_ref()
            .and_then(|accounts| accounts.get(&format!("{provider}/{model}")))
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn tier_runner_serves(
        &self,
        runner: &crate::routing_v1::runners::EffectiveRunner,
        fact: &striatum_core::ModelFact,
        tier: &str,
    ) -> bool {
        self.service_tier_accounts(&fact.provider_id, &fact.model_id)
            .iter()
            .any(|id| {
                self.credentials
                    .listed_credentials()
                    .iter()
                    .find(|row| &row.id == id)
                    .is_some_and(|row| {
                        crate::service_tiers::runner_supports(
                            &self.credentials,
                            runner,
                            fact,
                            row,
                            tier,
                        )
                    })
            })
    }

    pub(crate) fn load(
        store: &SqliteRouteStateStore,
        credentials: &crate::broca_credentials::SharedBrocaCredentials,
        config: std::sync::Arc<crate::config_overrides::LiveConfig>,
        accounts: Option<&SharedAccountQuota>,
        now_ms: i64,
    ) -> Result<Self, String> {
        let snapshot = store
            .get_facts_with_generation()
            .map_err(|error| error.to_string())?;
        Self::read_with_catalog(
            store,
            credentials,
            config,
            accounts,
            snapshot.facts.clone(),
            &snapshot,
            now_ms,
        )
    }

    #[cfg(test)]
    pub(crate) fn read(
        store: &SqliteRouteStateStore,
        credentials: &crate::broca_credentials::SharedBrocaCredentials,
        config: std::sync::Arc<crate::config_overrides::LiveConfig>,
        accounts: Option<&SharedAccountQuota>,
        facts: Vec<striatum_core::ModelFact>,
        now_ms: i64,
    ) -> Result<Self, String> {
        let snapshot = store
            .get_facts_with_generation()
            .map_err(|error| error.to_string())?;
        Self::read_with_catalog(
            store,
            credentials,
            config,
            accounts,
            facts,
            &snapshot,
            now_ms,
        )
    }

    pub(crate) fn read_with_catalog(
        store: &SqliteRouteStateStore,
        credentials: &crate::broca_credentials::SharedBrocaCredentials,
        config: std::sync::Arc<crate::config_overrides::LiveConfig>,
        accounts: Option<&SharedAccountQuota>,
        facts: Vec<striatum_core::ModelFact>,
        catalog_snapshot: &striatum_store::CatalogFactsSnapshot,
        now_ms: i64,
    ) -> Result<Self, String> {
        Self::read_for_sweep(
            store,
            credentials,
            config,
            accounts,
            facts,
            catalog_snapshot,
            now_ms,
            None,
        )
    }

    pub(crate) fn read_for_sweep(
        store: &SqliteRouteStateStore,
        credentials: &crate::broca_credentials::SharedBrocaCredentials,
        config: std::sync::Arc<crate::config_overrides::LiveConfig>,
        accounts: Option<&SharedAccountQuota>,
        mut facts: Vec<striatum_core::ModelFact>,
        catalog_snapshot: &striatum_store::CatalogFactsSnapshot,
        now_ms: i64,
        sweep: Option<&dry_run::Sweep>,
    ) -> Result<Self, String> {
        #[cfg(test)]
        if let Some(oracle) = sweep.and_then(|sweep| sweep.oracle.as_ref()) {
            return Self::read_with_catalog(
                oracle,
                credentials,
                config,
                accounts,
                facts,
                catalog_snapshot,
                now_ms,
            );
        }
        #[cfg(test)]
        CONTEXT_LOADS.with(|loads| loads.set(loads.get() + 1));
        // Expired admission probes and continuation offers read as released
        // through the published state's `as_of` view; the next command
        // releases them durably, so this read sends no release command. A
        // sweep's private copy applies the same releases to itself.
        if let Some(sweep) = sweep {
            sweep.expire(now_ms);
        }
        let refusals = match sweep {
            Some(sweep) => sweep.listing(credentials, now_ms)?,
            None => crate::credential_fences::live_with_eligibility(store, credentials, true)?,
        };
        let probe_scope = std::sync::Arc::downgrade(credentials);
        let credentials = credentials
            .lock()
            .map_err(|error| error.to_string())?
            .snapshot();
        let eligibility = dry_run::published(store, sweep, now_ms)
            .and_then(|blocks| blocks.account_eligibility())
            .map_err(|error| error.to_string())?;
        let mut size_refusals = dry_run::published(store, sweep, now_ms)
            .and_then(|blocks| blocks.account_size_refusals(now_ms))
            .map_err(|error| error.to_string())?;
        let mut catalog: crate::broca_credentials::AuthCatalog =
            catalog_snapshot.facts.iter().collect();
        catalog.providers = catalog_snapshot.providers.clone();
        let identity_facts: Vec<_> = facts
            .iter()
            .filter(|fact| catalog.contains(&fact.provider_id))
            .cloned()
            .collect();
        let targets = credentials.catalog_targets(&identity_facts);
        catalog.inherit_aliases(&targets);
        let media_providers: BTreeSet<_> = credentials
            .listed_credentials()
            .iter()
            .flat_map(|row| {
                row.provider_ids
                    .iter()
                    .filter(|provider| media_credential_serves(row, provider))
                    .cloned()
            })
            .collect();
        let mut pool_walls = BTreeSet::new();
        for provider in targets
            .keys()
            .chain(media_providers.iter())
            .collect::<BTreeSet<_>>()
        {
            let read_states = || match sweep {
                Some(sweep) => sweep.pool_states(provider, now_ms),
                None => crate::account_writer::pool_key_states(store, provider, now_ms),
            };
            #[cfg(test)]
            let read_states = || {
                if LEGACY_CONTEXT_REPAIRS.load(Ordering::SeqCst) {
                    store.pre_read_first_pool_key_states_for_test(provider, now_ms)
                } else {
                    read_states()
                }
            };
            for (credential, state) in read_states().map_err(|error| error.to_string())? {
                if state.walled_until_ms.is_some_and(|until| until > now_ms)
                    || state.cooldown_until_ms.is_some_and(|until| until > now_ms)
                {
                    pool_walls.insert((provider.clone(), credential));
                }
            }
        }
        striatum_core::score_alias::append_alias_rows(&mut facts, &targets);
        let quota = accounts
            .and_then(|accounts| accounts.lock().ok().map(|state| state.clone()))
            .unwrap_or_default();
        let vendorless_quota = quota_route::vendorless_quota(&quota, &catalog_snapshot.facts);
        let account_ids = credentials.quota_account_ids(&quota);
        let size_clocks = dry_run::published(store, sweep, now_ms)
            .and_then(|blocks| blocks.fence_rechecks())
            .map_err(|error| error.to_string())?;
        for fact in &size_refusals {
            if !size_clocks.iter().any(|origin| {
                origin.kind == "size"
                    && origin.provider == fact.provider
                    && origin.credential_id.as_deref() == Some(&fact.credential_id)
                    && origin.model.as_deref() == Some(&fact.model)
                    && origin.size_class == Some(fact.size_class as i64)
                    && origin.evidence_at == fact.observed_at
            }) {
                continue;
            }
            let recovered =
                crate::credential_fences::credential_account_id(&account_ids, &fact.credential_id)
                    .is_some_and(|account| {
                        crate::credential_fences::fresh_headroom_after_for_account(
                            &quota,
                            &account,
                            fact.observed_at,
                            now_ms,
                            &config.allocator_policy(&fact.provider),
                        )
                    });
            if recovered {
                // Compare evidence time so a concurrently reported refusal is
                // never erased by the older recovery snapshot.
                if let Some(sweep) = sweep {
                    sweep.clear_size(fact);
                } else {
                    crate::account_writer::clear_account_size_refusal(
                        store,
                        &fact.provider,
                        &fact.credential_id,
                        &fact.model,
                        fact.size_class,
                        fact.observed_at.saturating_add(1),
                    )
                    .map_err(|error| error.to_string())?;
                }
            }
        }
        // Each read below takes the state published after the commands
        // above, so it sees what they changed.
        size_refusals = dry_run::published(store, sweep, now_ms)
            .and_then(|blocks| blocks.account_size_refusals(now_ms))
            .map_err(|error| error.to_string())?;
        let blocks = dry_run::published(store, sweep, now_ms).map_err(|error| error.to_string())?;
        let mut route_fences = blocks
            .account_route_fences(now_ms)
            .map_err(|error| error.to_string())?;
        let recorded_route_fences = route_fences.clone();
        let latest_reports = blocks
            .route_fence_latest_reports()
            .map_err(|error| error.to_string())?;
        let transient_route_fences = blocks
            .transient_account_route_fences(now_ms)
            .map_err(|error| error.to_string())?;
        drop(blocks);
        if let Some(sweep) = sweep {
            for (provider, credential, model, at) in
                route_fence_recoveries(sweep, &account_ids, &quota, &config, now_ms)
                    .map_err(|e| e.to_string())?
            {
                match model {
                    Some(model) => sweep.clear_route(&provider, &credential, &model, at),
                    None => sweep.clear_routes(&provider, &credential, at),
                }
            }
        } else {
            repair_route_fences(store, &account_ids, &quota, &config, now_ms, || {})?;
        }
        route_fences = dry_run::published(store, sweep, now_ms)
            .and_then(|blocks| blocks.account_route_fences(now_ms))
            .map_err(|error| error.to_string())?;
        for fence in &mut route_fences {
            if let Some(reset) = route_exhausted_reset(&quota, &account_ids, fence, &config, now_ms)
            {
                let fetched = crate::credential_fences::credential_account_id(
                    &account_ids,
                    &fence.credential_id,
                )
                .and_then(|account| {
                    crate::credential_fences::latest_reading_for_account(&quota, &account)
                })
                .and_then(|reading| reading.entry.get("fetchedAt").and_then(Value::as_str))
                .and_then(account_quota::parse_rfc3339_ms);
                let newer = fetched
                    .zip(latest_reports.get(&fence.credential_id))
                    .is_some_and(|(fetched, reported)| fetched > *reported);
                let until = if newer {
                    reset
                } else {
                    reset.max(fence.until_ms)
                };
                if until != fence.until_ms {
                    if let Some(sweep) = sweep {
                        sweep.reconcile_route(fence, until);
                    } else {
                        crate::account_writer::reconcile_account_route_fence(
                            store,
                            fence,
                            until,
                            latest_reports[&fence.credential_id],
                        )
                        .map_err(|error| error.to_string())?;
                    }
                    fence.until_ms = until;
                }
            }
        }
        let refusals = if let Some(sweep) = sweep {
            reconcile_credential_quota_holds(
                sweep,
                &account_ids,
                &quota,
                &config,
                refusals,
                &recorded_route_fences,
                now_ms,
                true,
                &mut false,
            )
            .map_err(|e| e.to_string())?
        } else {
            refresh_credential_quota_holds(
                store,
                &account_ids,
                &quota,
                &config,
                refusals,
                &recorded_route_fences,
                now_ms,
            )?
        };
        let blocks = dry_run::published(store, sweep, now_ms).map_err(|error| error.to_string())?;
        let fences = facts
            .iter()
            .map(|fact| fact.provider_id.clone())
            .chain(media_providers.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|provider| Ok((provider.clone(), load_fences(&blocks, &provider)?)))
            .collect::<RouteStateResult<_>>()
            .map_err(|error| error.to_string())?;
        let rechecks = blocks.fence_rechecks().map_err(|error| error.to_string())?;
        let probe_authorizations = blocks
            .probe_authorizations_for_origins(&rechecks)
            .map_err(|error| error.to_string())?;
        drop(blocks);
        let mut context = Self {
            serving_index: crate::serving_index::ServingIndex::get(
                &config,
                &credentials,
                catalog_snapshot,
            ),
            indexes: Default::default(),
            class_memo: Default::default(),
            rechecks,
            probe_authorizations,
            account_ids,
            credentials,
            config,
            facts,
            refusals,
            quota,
            vendorless_quota,
            fences,
            now_ms,
            eligibility,
            size_refusals,
            route_fences,
            transient_route_fences,
            pool_walls,
            catalog,
            targets,
            tier_accounts: None,
            continuation_credential: None,
            media_route: false,
            probe_scope,
            probe_owner: None,
            selection_probes: None,
        };
        let recovered: Vec<_> = context
            .rechecks
            .iter()
            .filter(|origin| {
                origin.kind == "account"
                    && origin
                        .account_id
                        .as_ref()
                        .is_some_and(|account| context.origin_recovered(origin, account))
            })
            .filter_map(|origin| {
                serde_json::from_str::<Vec<String>>(&origin.origin_key)
                    .ok()
                    .and_then(|mut ids| ids.pop())
            })
            .collect();
        if !recovered.is_empty() {
            let fences = context.fences.clone();
            let fences = if let Some(sweep) = sweep {
                for id in &recovered {
                    sweep
                        .repair(
                            striatum_store::DryRunRepair::ClearFence(
                                id.clone(),
                                "fresh_headroom".into(),
                            ),
                            now_ms,
                        )
                        .map_err(|e| e.to_string())?;
                }
                let mut fences = fences;
                for (provider, fences) in &mut fences {
                    *fences = load_fences(sweep, provider).map_err(|e| e.to_string())?;
                }
                fences
            } else {
                crate::account_writer::repair(store, move |tx| {
                    for id in &recovered {
                        tx.clear_fence(id, now_ms, "fresh_headroom")?;
                    }
                    let mut fences = fences.clone();
                    for (provider, fences) in &mut fences {
                        *fences = load_fences(&*tx, provider)?;
                    }
                    Ok(fences)
                })
                .map_err(|error| error.to_string())?
            };
            context.replace_fences(fences);
            context.replace_rechecks(
                dry_run::published(store, sweep, now_ms)
                    .and_then(|blocks| blocks.fence_rechecks())
                    .map_err(|error| error.to_string())?,
            );
        }
        Ok(context)
    }

    pub(crate) fn reserve_selection_probes(
        &mut self,
        store: &SqliteRouteStateStore,
        credentials: &crate::broca_credentials::SharedBrocaCredentials,
    ) -> Result<(), String> {
        let mut probes = Vec::new();
        for row in self.credentials.listed_credentials() {
            for fact in &self.facts {
                if self.probe_eligible(&fact.provider_id, &fact.model_id, &row.id) {
                    probes.push((fact.provider_id.clone(), row.id.clone()));
                }
            }
        }
        if probes.is_empty() {
            return Ok(());
        }
        let (hold, blocked) = probe_reservations::Selection::new(credentials, probes, self.now_ms);
        self.probe_owner = Some(hold.owner.clone());
        self.selection_probes = Some(std::sync::Arc::new(hold));
        // A competing selection can bind and release its hold before scoring
        // first checks eligibility. Remember the hold while still using the
        // snapshot that predates its durable claim.
        for (provider, credential, until) in blocked {
            self.note_probe_reserved(&provider, &credential, until);
        }
        // A direct admission can claim a probe between the first store read
        // and reservation. Read after taking the hold so its renewed fence
        // cannot remain probe-eligible in this scoring snapshot.
        self.replace_refusals(refresh_credential_quota_holds(
            store,
            &self.account_ids,
            &self.quota,
            &self.config,
            crate::credential_fences::live(store, credentials)?,
            &self.route_fences,
            self.now_ms,
        )?);
        self.replace_rechecks(
            crate::account_state::published(store, self.now_ms)
                .and_then(|blocks| blocks.fence_rechecks())
                .map_err(|error| error.to_string())?,
        );
        Ok(())
    }

    /// Report when scoring lost its probe, leaving a timed hold for a
    /// fresh selection attempt. Store failures remain errors, not probe losses.
    pub(crate) fn bind_selected_probe(
        &self,
        store: &SqliteRouteStateStore,
        provider: &str,
        model: &str,
        priced: bool,
        decision: &str,
    ) -> RouteStateResult<ProbeBinding> {
        let Outcome::Admit(admission) = self
            .probe(provider, model, priced, false, None, "probe-reservation")
            .outcome
        else {
            if self.selection_probes.is_none() {
                return Ok(ProbeBinding::Bound);
            }
            return Ok(ProbeBinding::Lost {
                retry_at: self
                    .blocked_reset(provider, model, priced)
                    .flatten()
                    .unwrap_or(self.now_ms.saturating_add(120_000)),
            });
        };
        // A block recorded since this context was read must still be
        // claimed or waited for: ask the current published state, which
        // holds every committed block, what applies now that this context
        // did not see; only then is a command needed.
        // Only blocks this context has not seen: what it saw, the selection
        // already weighed (some, such as a transient retry, it lets pass
        // until admission).
        let applies_now = || -> RouteStateResult<Vec<striatum_store::FenceRecheck>> {
            let published = crate::account_state::published(store, self.now_ms)?;
            let fences = load_fences(&published, provider)?;
            Ok(published
                .fence_rechecks()?
                .into_iter()
                .filter(|origin| {
                    !self.rechecks.iter().any(|seen| {
                        seen.kind == origin.kind
                            && seen.origin_key == origin.origin_key
                            && seen.evidence == origin.evidence
                    })
                })
                .filter(|origin| {
                    self.origin_applies_with(
                        &fences,
                        origin,
                        provider,
                        &admission.account_id,
                        &admission.credential_id,
                        model,
                    ) && !self.origin_recovered(origin, &admission.account_id)
                })
                .collect())
        };
        let owner = match &self.selection_probes {
            Some(hold) => {
                if !self.probe_eligible(provider, model, &admission.credential_id)
                    && applies_now()?.is_empty()
                {
                    return Ok(ProbeBinding::Bound);
                }
                hold.owner.clone()
            }
            None => {
                // No probe reservation to bind; the admission claims any
                // due probe this context saw. A block it did not see that
                // is not due yet makes the selection wait for it instead of
                // going ahead. One that is already due is claimed for the
                // decision here: a legacy send has no later admission or
                // launch claim to take it, and would otherwise go ahead
                // without claiming that due probe.
                if self.probe_eligible(provider, model, &admission.credential_id) {
                    return Ok(ProbeBinding::Bound);
                }
                let unseen = applies_now()?;
                let waiting: Vec<_> = unseen
                    .iter()
                    .filter(|origin| origin.due_at() > self.now_ms)
                    .cloned()
                    .collect();
                if !waiting.is_empty() {
                    return Ok(ProbeBinding::Lost {
                        retry_at: lost_probe_retry_at(&waiting, None, self.now_ms),
                    });
                }
                if unseen.is_empty() {
                    return Ok(ProbeBinding::Bound);
                }
                // With no reservation owner, the decision claims directly.
                decision.to_owned()
            }
        };
        #[cfg(test)]
        run_before_probe_bind_hook(&self.probe_scope, store);
        let context = self.clone();
        let owned_provider = provider.to_owned();
        let owned_model = model.to_owned();
        let decision = decision.to_owned();
        let claimed_admission = admission.clone();
        let release_at = self.now_ms;
        let (claimed, retry_at) = crate::account_writer::repair(store, move |tx| {
            // What the selection read as released is released first.
            tx.release_expired(release_at)?;
            let provider = owned_provider.as_str();
            let model = owned_model.as_str();
            let admission = claimed_admission.clone();
            // The command's own fences, not the context's: a block
            // recorded after the context was read is claimed too.
            let fences = load_fences(&*tx, provider)?;
            let origins: Vec<_> = tx
                .fence_rechecks()?
                .into_iter()
                .filter(|origin| {
                    context.origin_applies_with(
                        &fences,
                        origin,
                        &provider,
                        &admission.account_id,
                        &admission.credential_id,
                        &model,
                    ) && !context.origin_recovered(origin, &admission.account_id)
                })
                .collect();
            // The block rows these origins were chosen from (and the rows
            // that ruled others out) are checked against SQL before the
            // claim, because the claim names only the chosen origins.
            tx.check_decision_inputs(&[provider, ""])?;
            let retry_at = origins.iter().map(|origin| origin.next_recheck_at).max();
            let claimed = tx.bind_decision_probe(
                &origins,
                &owner,
                &decision,
                &admission.account_id,
                &admission.credential_id,
                context.now_ms,
            )?;
            Ok((claimed, retry_at))
        })?;
        if !claimed {
            let retry_at = retry_at
                .unwrap_or(self.now_ms)
                .max(self.now_ms.saturating_add(1));
            self.note_probe_reserved(provider, &admission.credential_id, retry_at);
            return Ok(ProbeBinding::Lost { retry_at });
        }
        // A runner may send without a later account admission, so its
        // decision durably owns the probe's interval by now; the in-memory
        // reservation, if the selection took one, is no longer needed.
        if let Some(hold) = &self.selection_probes {
            hold.release_selected(provider, &admission.credential_id, self.now_ms);
        }
        Ok(ProbeBinding::Bound)
    }

    pub(crate) fn class(
        &self,
        fact: &striatum_core::ModelFact,
        priced_per_token: bool,
    ) -> RouteClass<'_> {
        let rows = self.credentials.listed_credentials();
        let entry = self.serving_index.fact(
            &self.config,
            &self.credentials,
            fact,
            &self.catalog,
            &self.targets,
        );
        let continuation = self
            .continuation_credential
            .as_deref()
            .map(|id| self.method(id));
        let allowed = |index: usize| {
            continuation.is_none_or(|method| {
                method != "unknown" && rows[index].auth_method.as_deref() == Some(method)
            })
        };
        let serving: Vec<_> = entry
            .selection
            .iter()
            .filter(|row| allowed(row.index))
            .collect();
        let fallback = self.config.metered_fallback(&fact.provider_id);
        let usable = |row: &crate::serving_index::ServingRow| {
            if !self.eligible(&rows[row.index], fact) {
                return false;
            }
            let siblings = serving
                .iter()
                .filter(|other| other.covered == row.covered)
                .map(|other| &rows[other.index])
                .filter(|other| self.eligible(other, fact))
                .map(|other| self.view(other, fact))
                .collect();
            matches!(
                striatum_core::accounts::decide_with_context(
                    &synthetic_request(&fact.provider_id, &fact.model_id, "class-preflight".into()),
                    &AllocatorState {
                        roster: Some(self.roster(siblings)), affinity: None,
                        fences: self.probe_fences(&fact.provider_id, &fact.model_id, self.fences.get(&fact.provider_id).cloned().unwrap_or_default()),
                        principal_exclusions: Vec::new(), admissions: Vec::new(), operator_override: None,
                        policy: self.config.allocator_policy_ref(&fact.provider_id).clone(),
                    }, self.now_ms, &self.allocation_context(&fact.provider_id, &fact.model_id),
                ).outcome,
                Outcome::Admit(admission) if admission.account_id == self.account_id(&rows[row.index])
            )
        };
        let mut inputs: Vec<_> = rows
            .iter()
            .map(|row| ClassRowInputs {
                serving: self.media_route && media_credential_serves(row, &fact.provider_id),
                covered: false,
                metered: false,
                usable: false,
            })
            .collect();
        if !self.media_route {
            for row in &serving {
                inputs[row.index] = ClassRowInputs {
                    serving: true,
                    covered: row.covered,
                    metered: row.metered,
                    usable: fallback && usable(row),
                };
            }
        }
        let slot = usize::from(priced_per_token) + 2 * usize::from(self.media_route);
        let mut memo = self.class_memo.lock().unwrap();
        let cached = memo
            .get(fact.provider_id.as_str())
            .and_then(|models| models.get(fact.model_id.as_str()))
            .and_then(|slots| slots[slot].as_ref());
        if let Some(entry) = cached.filter(|entry| entry.inputs == inputs) {
            return RouteClass {
                rows: entry.rows.iter().map(|&i| &rows[i]).collect(),
                subscription: entry.subscription,
            };
        }
        let can_fallback = fallback
            && inputs
                .iter()
                .any(|row| row.serving && !row.covered && row.usable);
        let subscription = !self.media_route
            && !priced_per_token
            && inputs.iter().any(|row| row.serving && row.covered)
            && (!can_fallback
                || inputs
                    .iter()
                    .any(|row| row.serving && row.covered && row.usable));
        let indices: Vec<_> = inputs
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                row.serving
                    && (self.media_route
                        || (row.covered == subscription && (!priced_per_token || row.metered)))
            })
            .map(|(i, _)| i)
            .collect();
        let resolved = RouteClass {
            rows: indices.iter().map(|&i| &rows[i]).collect(),
            subscription,
        };
        let entry = ClassMemoEntry {
            inputs,
            rows: indices,
            subscription,
            #[cfg(test)]
            misses: cached.map_or(1, |entry| entry.misses + 1),
        };
        memo.entry(fact.provider_id.clone())
            .or_default()
            .entry(fact.model_id.clone())
            .or_insert_with(|| std::array::from_fn(|_| None))[slot] = Some(entry);
        resolved
    }

    #[cfg(test)]
    pub(crate) fn revalidate_credential(&mut self, credential: &str) {
        self.continuation_credential = Some(credential.into());
    }

    pub(super) fn view(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &striatum_core::ModelFact,
    ) -> AccountView {
        let account_id = self.account_id(row).to_owned();
        let capacity = format!("{}/{}", fact.provider_id, fact.model_id);
        let model_pools = self.config.quota_model_pools();
        let reading = self
            .reading_index()
            .accounts
            .get(&account_id)
            .into_iter()
            .flatten()
            .filter(|reading| self.quota_serves(&reading.reading, row, fact, &capacity))
            .max_by_key(|reading| reading.fetched);
        let usage = reading
            .and_then(|reading| reading.decoded.as_ref())
            .and_then(|entry| entry.usage.as_ref());
        let pooled = usage
            .as_ref()
            .and_then(|usage| usage.extra_rate_windows.as_ref())
            .into_iter()
            .flatten()
            .any(striatum_core::quota_extract::is_account_pool_window);
        let model_reach = if pooled {
            ModelReach::Only(
                usage
                    .as_ref()
                    .filter(|usage| {
                        !striatum_core::quota_extract::account_rate_windows(
                            usage,
                            Some(&capacity),
                            &model_pools,
                        )
                        .is_empty()
                    })
                    .map(|_| fact.model_id.clone())
                    .into_iter()
                    .collect(),
            )
        } else {
            ModelReach::All
        };
        AccountView {
            account_id: account_id.clone(),
            routing_provider: fact.provider_id.clone(),
            refresh_adapter: row.refresh_adapter.clone().unwrap_or_default(),
            credentials: vec![CredentialRecord {
                credential_id: row.id.clone(),
                default_record: is_default_record(&row.id),
                needs_reauth: false,
            }],
            model_reach,
            quota: reading
                .filter(|_| {
                    !self
                        .indexed_origins(&fact.provider_id, &account_id, &row.id, &fact.model_id)
                        .any(|origin| {
                            origin.quota
                                && self.origin_applies(
                                    origin,
                                    &fact.provider_id,
                                    &account_id,
                                    &row.id,
                                    &fact.model_id,
                                )
                                && self.indexed_origin_due(origin, &account_id, &fact.model_id)
                        })
                })
                .filter(|_| self.reading_index().quota_signal.contains(&account_id))
                .and_then(|reading| {
                    quota_reading_from_entry(
                        reading.decoded.as_ref()?,
                        &reading.reading.account_id,
                        Some(&capacity),
                        &model_pools,
                    )
                }),
        }
    }

    pub(super) fn eligible(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &striatum_core::ModelFact,
    ) -> bool {
        row.condition.is_none() && self.eligible_without_login_condition(row, fact)
    }

    fn eligible_without_login_condition(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &striatum_core::ModelFact,
    ) -> bool {
        // Keep account quota in the allocator's roster, not its entitlement
        // filter. A scheduled probe temporarily bypasses that quota restriction;
        // admission must still claim the attempt before sending.
        !self
            .indexed_origins(
                &fact.provider_id,
                self.account_id(row),
                &row.id,
                &fact.model_id,
            )
            .any(|origin| {
                !striatum_core::blocks::eligible(
                    &origin.origin(),
                    || {
                        self.origin_applies(
                            origin,
                            &fact.provider_id,
                            self.account_id(row),
                            &row.id,
                            &fact.model_id,
                        )
                    },
                    || self.indexed_origin_due(origin, self.account_id(row), &fact.model_id),
                )
            })
            && (!self.probe_eligible(&fact.provider_id, &fact.model_id, &row.id)
                || self
                    .probe_reserved_until(&fact.provider_id, &row.id)
                    .is_none())
            && self.tier_accounts.as_ref().is_none_or(|accounts| {
                accounts
                    .get(&format!("{}/{}", fact.provider_id, fact.model_id))
                    .is_some_and(|ids| ids.contains(&row.id))
            })
            && !self
                .block_index()
                .eligibility
                .get(&(fact.provider_id.clone(), row.id.clone()))
                .into_iter()
                .flatten()
                .any(|&i| {
                    let fact_ = &self.eligibility[i];
                    fact_.model.is_empty() || fact_.model == fact.model_id
                })
            && (!self
                .block_index()
                .refusals
                .contains(&(fact.provider_id.clone(), row.id.clone()))
                || self.probe_eligible(&fact.provider_id, &fact.model_id, &row.id))
    }

    /// When a request may retry a credential whose one probe another
    /// selection holds reserved: the later of the hold's end and the next
    /// check of the credential's blocks that are not due. `None` when no
    /// reservation keeps this credential out.
    pub(super) fn reserved_probe_retry_at(
        &self,
        provider: &str,
        model: &str,
        row: &cortexkit_claustrum_client::ListedCredential,
    ) -> Option<i64> {
        let until = self
            .probe_eligible(provider, model, &row.id)
            .then(|| self.probe_reserved_until(provider, &row.id))
            .flatten()?;
        Some(
            self.rechecks
                .iter()
                .filter(|origin| {
                    self.origin_applies(origin, provider, self.account_id(row), &row.id, model)
                        && !self.origin_due(origin, self.account_id(row), model)
                })
                .map(|origin| self.recheck_hint(origin, self.account_id(row), model))
                .max()
                .map_or(until, |at| at.max(until)),
        )
    }

    /// The accounts a request may use that the class context left out of
    /// its roster because another selection holds their probe reserved, with
    /// when each may be retried.
    pub(super) fn reserved_probe_accounts(
        &self,
        provider: &str,
        model: &str,
        params: &AccountForParams,
    ) -> Vec<(String, i64)> {
        self.credentials
            .listed_credentials()
            .iter()
            .filter(|row| {
                params.candidates.as_ref().is_none_or(|candidates| {
                    candidates.accounts.iter().any(|candidate| {
                        candidate.credential_id == row.id
                            && candidate.account_id == self.account_id(row)
                    })
                })
            })
            .filter_map(|row| {
                self.reserved_probe_retry_at(provider, model, row)
                    .map(|until| (self.account_id(row).to_owned(), until))
            })
            .collect()
    }

    pub(super) fn probe_eligible(&self, provider: &str, model: &str, credential: &str) -> bool {
        let account =
            crate::credential_fences::credential_account_id(&self.account_ids, credential)
                .unwrap_or_else(|| credential.into());
        self.indexed_origins(provider, &account, credential, model)
            .any(|origin| {
                striatum_core::blocks::probe_eligible(
                    self.origin_applies(origin, provider, &account, credential, model),
                    || self.indexed_origin_due(origin, &account, model),
                )
            })
    }

    pub(super) fn origin_applies(
        &self,
        origin: &striatum_store::FenceRecheck,
        provider: &str,
        account: &str,
        credential: &str,
        model: &str,
    ) -> bool {
        let fences = self.fences.get(provider).map_or(&[][..], Vec::as_slice);
        self.origin_applies_with(fences, origin, provider, account, credential, model)
    }

    /// [`Self::origin_applies`] with the account fences given instead of the
    /// ones this context read. A writer command passes the fences it loaded
    /// from its own working state, so an account block recorded after this
    /// context was read still decides what the command claims.
    fn origin_applies_with(
        &self,
        fences: &[Fence],
        origin: &striatum_store::FenceRecheck,
        provider: &str,
        account: &str,
        credential: &str,
        model: &str,
    ) -> bool {
        striatum_core::blocks::origin_applies(
            &origin.origin(),
            || self.origin_targets(origin, provider, account, credential, model),
            || {
                Some(fences).is_some_and(|fences| {
                    fences.iter().any(|fence| {
                        fence.account_id == account
                            && fence.observed_at_ms == origin.evidence_at
                            && fence.applies_to(
                                model,
                                self.config
                                    .allocator_policy_ref(provider)
                                    .family_for_model(model),
                                request_pool(None, provider, model).as_deref(),
                            )
                    })
                })
            },
        )
    }

    pub(super) fn origin_targets(
        &self,
        origin: &striatum_store::FenceRecheck,
        provider: &str,
        account: &str,
        credential: &str,
        model: &str,
    ) -> bool {
        let named = origin
            .credential_id
            .as_deref()
            .filter(|id| {
                origin.quota
                    && crate::credential_fences::credential_account_id(&self.account_ids, id)
                        .as_deref()
                        == Some(account)
            })
            .unwrap_or(credential);
        origin.applies(provider, account, named, model)
    }

    pub(super) fn origin_due(
        &self,
        origin: &striatum_store::FenceRecheck,
        account: &str,
        model: &str,
    ) -> bool {
        if self.origin_recovered(origin, account) {
            return true;
        }
        let owned = self.probe_owner.is_some() && origin.claim_owner == self.probe_owner;
        (self.recheck_at(origin, account, model) <= self.now_ms || owned)
            && (!origin.quota || !self.origin_has_binding_reading(origin, account))
    }

    pub(super) fn origin_has_binding_reading(
        &self,
        origin: &striatum_store::FenceRecheck,
        account: &str,
    ) -> bool {
        // A quota route fence on a pooled reading is held back only by its
        // own pools: a spent pool the model never draws on must not stop its
        // recheck.
        if let Some(model) = origin
            .model
            .as_deref()
            .filter(|_| origin.kind == "account_route")
        {
            let usage = crate::credential_fences::latest_reading_for_account(&self.quota, account)
                .and_then(|reading| reading_usage(&reading.entry));
            if route_is_pooled(usage.as_ref(), &origin.provider, model) {
                return route_pool_spent(
                    &self.quota,
                    account,
                    &origin.provider,
                    model,
                    self.config.quota_model_pools_ref(),
                    self.now_ms,
                );
            }
        }
        // A block keyed to a named pool on a pooled reading is held back only
        // by its own pool being spent, as a pooled route fence is. Once the
        // reading ages out of the freshness band, or the spent window's reset
        // passes, the 15-minute recheck runs again.
        if origin.kind == "account" {
            let matching: Vec<_> = self
                .fences
                .get(&origin.provider)
                .into_iter()
                .flatten()
                .filter(|fence| {
                    fence.account_id == account && fence.observed_at_ms == origin.evidence_at
                })
                .collect();
            if let Some(spent) =
                named_pool_blocks_spent(&matching, &self.quota, account, self.now_ms)
            {
                return spent;
            }
        }
        let policy = self.config.allocator_policy(&origin.provider);
        if !crate::credential_fences::current_quota_reading(
            &self.quota,
            account,
            self.now_ms,
            &policy,
        ) {
            return false;
        }
        let reading = crate::credential_fences::latest_reading_for_account(&self.quota, account)
            .and_then(|reading| quota_reading_for_model(&reading.entry, account, None, &[]));
        quota_reading_binds_origin(
            origin,
            account,
            self.fences.get(&origin.provider).map_or(&[], Vec::as_slice),
            reading.as_ref(),
            self.now_ms,
            &policy,
        )
    }

    fn recheck_at(&self, origin: &striatum_store::FenceRecheck, account: &str, model: &str) -> i64 {
        if origin.quota {
            let related: Vec<_> = self
                .rechecks
                .iter()
                .filter(|other| {
                    other.quota
                        && self.origin_targets(other, &origin.provider, account, "", model)
                        && other.principal.is_none()
                        && other
                            .size_class
                            .is_none_or(|class| Some(class) == origin.size_class)
                        && (other.kind != "account"
                            || self.origin_applies(other, &origin.provider, account, "", model))
                })
                .collect();
            striatum_store::FenceRecheck::quota_group_at(related, self.probe_owner.as_deref())
        } else {
            origin.due_at()
        }
    }

    pub(super) fn recheck_hint(
        &self,
        origin: &striatum_store::FenceRecheck,
        account: &str,
        model: &str,
    ) -> i64 {
        let at = self.recheck_at(origin, account, model);
        if origin.quota && at <= self.now_ms {
            self.rechecks
                .iter()
                .filter(|other| {
                    other.quota && self.origin_applies(other, &origin.provider, account, "", model)
                })
                .filter_map(|other| other.reset_check_at)
                .filter(|reset| *reset > self.now_ms)
                .chain(std::iter::once(self.now_ms.saturating_add(
                    striatum_core::fence_lifetimes::QUOTA_FENCE_RECHECK_MS,
                )))
                .min()
                .unwrap()
        } else {
            at
        }
    }

    pub(super) fn origin_recovered(
        &self,
        origin: &striatum_store::FenceRecheck,
        account: &str,
    ) -> bool {
        if !origin.quota {
            return false;
        }
        let policy = self.config.allocator_policy(&origin.provider);
        if origin.kind == "account" {
            let reading =
                crate::credential_fences::latest_reading_for_account(&self.quota, account)
                    .and_then(|reading| {
                        quota_reading_for_model(&reading.entry, account, None, &[])
                    });
            let matching: Vec<_> = self
                .fences
                .get(&origin.provider)
                .into_iter()
                .flatten()
                .filter(|fence| {
                    fence.account_id == account && fence.observed_at_ms == origin.evidence_at
                })
                .collect();
            return !matching.is_empty()
                && matching.iter().all(|fence| {
                    block_cleared(fence, reading.as_ref(), self.now_ms, &policy).is_some()
                });
        }
        matches!(origin.kind.as_str(), "provider" | "route" | "size")
            && crate::credential_fences::fresh_headroom_after_for_account(
                &self.quota,
                account,
                origin.evidence_at,
                self.now_ms,
                &policy,
            )
    }

    fn probe_fences(&self, provider: &str, model: &str, fences: Vec<Fence>) -> Vec<Fence> {
        fences
            .into_iter()
            .filter(|fence| {
                !self.rechecks.iter().any(|origin| {
                    origin.kind == "account"
                        && origin.applies(provider, &fence.account_id, "", model)
                        && origin.evidence_at == fence.observed_at_ms
                        && self.origin_due(origin, &fence.account_id, model)
                })
            })
            .collect()
    }

    pub(super) fn quota_hold_reset(&self, provider: &str, credential: &str) -> Option<i64> {
        let account =
            crate::credential_fences::credential_account_id(&self.account_ids, credential)?;
        let stored = self
            .fences
            .get(provider)
            .into_iter()
            .flatten()
            .filter_map(|fence| {
                if fence.account_id == account && fence.pool.is_none() {
                    match fence.reset {
                        ResetTime::At(reset) if reset > self.now_ms => Some(reset),
                        _ => None,
                    }
                } else {
                    None
                }
            });
        stored
            .chain(crate::credential_fences::exhausted_quota_reset(
                &self.quota,
                &self.account_ids,
                credential,
                self.now_ms,
                &self.config.allocator_policy(provider),
            ))
            .max()
    }

    /// A stale refusal is a fallback, never a reason to probe while an
    /// unfenced eligible identity in the same payment class can serve.
    pub(super) fn eligible_rows<'a>(
        &self,
        rows: Vec<&'a cortexkit_claustrum_client::ListedCredential>,
        fact: &striatum_core::ModelFact,
    ) -> Vec<&'a cortexkit_claustrum_client::ListedCredential> {
        let rows: Vec<_> = rows
            .into_iter()
            .filter(|row| self.eligible(row, fact))
            .collect();
        rows
    }

    #[cfg(test)]
    pub(super) fn usable(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &striatum_core::ModelFact,
    ) -> bool {
        self.usable_candidate(row, fact, false)
    }

    fn usable_candidate(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &striatum_core::ModelFact,
        only: bool,
    ) -> bool {
        if !self.eligible(row, fact) {
            return false;
        }
        let specs = &self.config.compiled_routing_policy().subscription;
        let key = format!("{}/{}", fact.provider_id, fact.model_id);
        let covered = |row: &cortexkit_claustrum_client::ListedCredential| {
            !self.targets.contains_key(&fact.provider_id)
                && specs.resolve(&key, row.credential_type == "oauth")
        };
        let rows = self
            .credentials
            .listed_credentials()
            .iter()
            .filter(|other| {
                (!only || other.id == row.id)
                    && covered(other) == covered(row)
                    && self.eligible(other, fact)
                    && crate::broca_credentials::credential_serves_fact(
                        other,
                        fact,
                        &self.catalog,
                        &self.credentials,
                    )
            })
            .collect();
        let roster = self.roster(
            self.eligible_rows(rows, fact)
                .into_iter()
                .map(|other| self.view(other, fact))
                .collect(),
        );
        matches!(
            striatum_core::accounts::decide_with_context(
                &synthetic_request(&fact.provider_id, &fact.model_id, "class-preflight".into()),
                &AllocatorState {
                    roster: Some(roster),
                    affinity: None,
                    fences: self.probe_fences(&fact.provider_id,&fact.model_id,self.fences.get(&fact.provider_id).cloned().unwrap_or_default()),
                    principal_exclusions: Vec::new(),
                    admissions: Vec::new(),
                    operator_override: None,
                    policy: self.config.allocator_policy_ref(&fact.provider_id).clone(),
                },
                self.now_ms,
                &self.allocation_context(&fact.provider_id, &fact.model_id),
            )
            .outcome,
            Outcome::Admit(admission) if admission.account_id == self.account_id(row)
        )
    }

    pub(super) fn roster(&self, accounts: Vec<AccountView>) -> Roster {
        let (view, at) = self
            .credentials
            .listing_version()
            .unwrap_or(("unavailable", 0));
        Roster {
            view_digest: view.into(),
            fetched_at_ms: at as i64,
            accounts,
        }
    }

    pub(super) fn roster_for(
        &self,
        provider: &str,
        model: &str,
        priced_per_token: bool,
    ) -> RosterBuild {
        let mut roster = self
            .roster_and_fences_for(provider, model, priced_per_token)
            .0;
        self.avoid_transient_accounts(&mut roster, provider, model);
        roster
    }

    /// A new conversation has no cache to preserve. Prefer an open account,
    /// but a blocked credential must not hide an unblocked sibling credential.
    pub(super) fn avoid_transient_accounts(
        &self,
        roster: &mut RosterBuild,
        provider: &str,
        model: &str,
    ) {
        let accounts = self.accounts_fenced_by(&self.transient_route_fences, provider, model);
        let fenced = |account: &AccountView| {
            accounts.contains_key(&account.account_id)
                && account.credentials.iter().all(|credential| {
                    self.rechecks.iter().any(|origin| {
                        origin.kind == "transient_route"
                            && origin.applies(
                                provider,
                                &account.account_id,
                                &credential.credential_id,
                                model,
                            )
                            && !self.origin_due(origin, &account.account_id, model)
                    })
                })
        };
        if let Some(roster) = &mut roster.roster {
            if roster.accounts.iter().any(|account| !fenced(account)) {
                roster.accounts.retain(|account| !fenced(account));
            }
        }
    }

    /// The roster for one route and accounts temporarily excluded by its
    /// applicable evidence, each with the next permitted recheck time.
    pub(super) fn roster_and_fences_for(
        &self,
        provider: &str,
        model: &str,
        priced_per_token: bool,
    ) -> (RosterBuild, BTreeMap<String, i64>) {
        self.build_route_roster(provider, model, priced_per_token, false)
    }

    /// Admission applies the condition gate after the decision's tier, size,
    /// and offered-account restrictions, so it can explain what made it empty.
    pub(super) fn roster_before_login_conditions(
        &self,
        provider: &str,
        model: &str,
        priced_per_token: bool,
    ) -> (RosterBuild, BTreeMap<String, i64>) {
        self.build_route_roster(provider, model, priced_per_token, true)
    }

    fn build_route_roster(
        &self,
        provider: &str,
        model: &str,
        priced_per_token: bool,
        include_conditioned: bool,
    ) -> (RosterBuild, BTreeMap<String, i64>) {
        let fact = self.fact_for(provider, model);
        let fenced = self.route_fenced_accounts(provider, model);
        let mut skipped = BTreeMap::new();
        let mut accounts: BTreeMap<String, AccountView> = BTreeMap::new();
        if let Some(fact) = fact {
            for row in self
                .class(fact, priced_per_token)
                .rows
                .into_iter()
                .filter(|row| {
                    (include_conditioned || row.condition.is_none())
                        && self.eligible_without_login_condition(row, fact)
                })
            {
                let view = self.view(row, fact);
                if let Some(until) = fenced.get(&view.account_id) {
                    skipped.insert(view.account_id, *until);
                    continue;
                }
                if let Some(existing) = accounts.get_mut(&view.account_id) {
                    existing.credentials.extend(view.credentials);
                } else {
                    accounts.insert(view.account_id.clone(), view);
                }
            }
        }
        (
            RosterBuild {
                roster: Some(self.roster(accounts.into_values().collect())),
                // Recorded only when true, so stored decisions for subscription
                // rosters keep their earlier shape.
                context: if priced_per_token {
                    json!({"provider": provider, "account_classes": true, "priced_per_token": true})
                } else {
                    json!({"provider": provider, "account_classes": true})
                },
            },
            skipped,
        )
    }

    pub(super) fn filter_login_conditions(
        &self,
        build: &mut RosterBuild,
        offered: impl Fn(&str, &str) -> bool,
    ) -> Option<LoginConditions> {
        let marked: BTreeMap<_, _> = self
            .credentials
            .listed_credentials()
            .iter()
            .filter_map(|row| Some((row.id.as_str(), row.condition.as_ref()?)))
            .collect();
        if marked.is_empty() {
            return None;
        }
        let mut conditions = LoginConditions::default();
        let mut has_unmarked = false;
        let roster = build.roster.as_mut()?;
        for account in &mut roster.accounts {
            account.credentials.retain(|credential| {
                let id = &credential.credential_id;
                let condition = marked.get(id.as_str());
                if offered(&account.account_id, id) {
                    if let Some(condition) = condition {
                        conditions.credential_ids.insert(id.clone());
                        conditions.reasons.insert(condition.reason.clone());
                    } else {
                        has_unmarked = true;
                    }
                }
                condition.is_none()
            });
        }
        roster
            .accounts
            .retain(|account| !account.credentials.is_empty());
        (!has_unmarked && !conditions.credential_ids.is_empty()).then_some(conditions)
    }

    pub(crate) fn login_condition_detail(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
    ) -> Option<LoginConditions> {
        if !self
            .credentials
            .listed_credentials()
            .iter()
            .any(|row| row.condition.is_some())
        {
            return None;
        }
        let (mut roster, fenced) = self.roster_before_login_conditions(provider, model, priced);
        if !fenced.is_empty() {
            return None;
        }
        self.filter_login_conditions(&mut roster, |_, _| true)
    }

    /// Accounts closed by a shared quota route fence or an account-wide backoff
    /// observation, each with its deadline. Credential-scoped route backoffs
    /// reach admission so its atomic probe check can judge the exact credential.
    fn route_fenced_accounts(&self, provider: &str, model: &str) -> BTreeMap<String, i64> {
        // A refusal probe must reach admission to claim its single-request
        // reservation. A quota route fence must not hide that same probe;
        // allocator quota blocks and observation backoffs still apply.
        let mut fenced = BTreeMap::new();
        for origin in self
            .rechecks
            .iter()
            .filter(|origin| matches!(origin.kind.as_str(), "account_route" | "observation"))
        {
            let account = origin.account_id.clone().or_else(|| {
                origin.credential_id.as_ref().and_then(|id| {
                    crate::credential_fences::credential_account_id(&self.account_ids, id)
                })
            });
            let Some(account) = account else { continue };
            if origin.applies(
                provider,
                &account,
                origin.credential_id.as_deref().unwrap_or(""),
                model,
            ) && !self.origin_due(origin, &account, model)
            {
                let until = self
                    .recheck_hint(origin, &account, model)
                    .max(self.now_ms + 1);
                fenced
                    .entry(account)
                    .and_modify(|at: &mut i64| *at = (*at).max(until))
                    .or_insert(until);
            }
        }
        fenced
    }

    fn accounts_fenced_by(
        &self,
        fences: &[striatum_store::AccountRouteFence],
        provider: &str,
        model: &str,
    ) -> BTreeMap<String, i64> {
        let mut fenced: BTreeMap<String, i64> = BTreeMap::new();
        for fence in fences
            .iter()
            .filter(|fence| fence.provider == provider && fence.model == model)
        {
            let account = self
                .credentials
                .listed_credentials()
                .iter()
                .find(|row| row.id == fence.credential_id)
                .map(|row| self.account_id(row).to_owned())
                .unwrap_or_else(|| fence.credential_id.clone());
            if let Some(origin) = self.rechecks.iter().find(|origin| {
                origin.kind == "transient_route"
                    && origin.applies(provider, &account, &fence.credential_id, model)
            }) {
                if !self.origin_due(origin, &account, model) {
                    let until = fenced
                        .entry(account)
                        .or_insert(origin.due_at().max(self.now_ms + 1));
                    *until = (*until).max(origin.due_at().max(self.now_ms + 1));
                }
            }
        }
        fenced
    }

    fn route_fence_reset(&self, provider: &str, model: &str, priced: bool) -> Option<i64> {
        let (roster, fenced) = self.roster_and_fences_for(provider, model, priced);
        if roster
            .roster
            .as_ref()
            .is_none_or(|roster| roster.accounts.is_empty())
        {
            fenced.values().min().copied()
        } else {
            None
        }
    }

    pub(super) fn method(&self, credential: &str) -> &str {
        if self.media_route {
            return self
                .credentials
                .listed_credentials()
                .iter()
                .find(|row| row.id == credential)
                .map(|row| {
                    row.refresh_adapter
                        .as_deref()
                        .unwrap_or(&row.credential_type)
                })
                .unwrap_or("unknown");
        }
        self.credentials
            .listed_credentials()
            .iter()
            .find(|row| row.id == credential)
            .and_then(|row| row.auth_method.as_deref())
            .unwrap_or("unknown")
    }

    pub(crate) fn probe(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
        hold_ok: bool,
        wait: Option<u64>,
        conversation: &str,
    ) -> Decision {
        self.probe_with_policy(
            provider,
            model,
            priced,
            hold_ok,
            wait,
            conversation,
            self.config.allocator_policy_ref(provider).clone(),
        )
    }

    /// Probe each roster account independently, without an affinity lookup or a
    /// reservation. A blocked sibling must not prevent an otherwise eligible
    /// account from being returned.
    pub(crate) fn select_eligible_accounts(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
    ) -> Vec<String> {
        #[cfg(test)]
        SELECT_ELIGIBILITY_PROBES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let Some(roster) = self.roster_for(provider, model, priced).roster else {
            return Vec::new();
        };
        let request = synthetic_request(provider, model, "demand-preflight".into());
        let mut inputs = self.allocation_context(provider, model);
        inputs.hold_ok = Some(false);
        roster
            .accounts
            .iter()
            .filter_map(|account| {
                let mut only = roster.clone();
                only.accounts = vec![account.clone()];
                let decision = striatum_core::accounts::decide_with_context(
                    &request,
                    &AllocatorState {
                        roster: Some(only),
                        affinity: None,
                        fences: self.probe_fences(
                            provider,
                            model,
                            self.fences.get(provider).cloned().unwrap_or_default(),
                        ),
                        principal_exclusions: Vec::new(),
                        admissions: Vec::new(),
                        operator_override: None,
                        policy: self.config.allocator_policy_ref(provider).clone(),
                    },
                    self.now_ms,
                    &inputs,
                );
                matches!(decision.outcome, Outcome::Admit(_)).then(|| account.account_id.clone())
            })
            .collect()
    }

    /// Inspect the requesting payment class with the allocator's own filters.
    /// A new session has no account affinity and must leave the allocator's
    /// minimum quota headroom intact. Those floors make an account unavailable
    /// just as a confirmed wall does; model selection must not relax them.
    pub(crate) fn blocked_reset(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
    ) -> Option<Option<i64>> {
        self.blocked_account(provider, model, priced)
            .map(|block| block.reset_at_ms)
    }

    pub(crate) fn blocked_account(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
    ) -> Option<AccountBlock> {
        self.blocked_account_with_policy(
            provider,
            model,
            priced,
            self.config.allocator_policy_ref(provider),
        )
    }

    /// One route's recovery in a fresh [`recovery_view::RecoveryView`]. A caller
    /// reading several routes builds the view once and reads each from it.
    pub(crate) fn account_recovery_with_keep_free(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
        keep_free: f64,
    ) -> Option<AccountBlock> {
        self.recovery_view(keep_free)
            .account_recovery(provider, model, priced)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn probe_with_policy(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
        hold_ok: bool,
        wait: Option<u64>,
        conversation: &str,
        mut policy: AllocatorPolicy,
    ) -> Decision {
        let mut request = synthetic_request(provider, model, conversation.into());
        if let Some(wait) = wait {
            request.hold_wait_max_s = wait;
        }
        let mut inputs = self.allocation_context(provider, model);
        inputs.hold_ok = Some(hold_ok);
        // A continuation spends its existing prompt cache. Reuse affinity's
        // fence checks, retaining a healthy account even when quota readings are
        // missing or stale, rather than making a cold choice.
        let affinity = self.continuation_credential.as_ref().and_then(|id| {
            let row = self
                .credentials
                .listed_credentials()
                .iter()
                .find(|row| row.id == *id)?;
            policy.strategy = AccountStrategy::Sticky;
            Some(ConversationAffinity {
                pin_id: String::new(),
                account_id: self.account_id(row).to_owned(),
                credential_id: row.id.clone(),
                affinity_model: model.into(),
                pin_generation: 1,
                expires_at_ms: i64::MAX,
                lease_active: true,
            })
        });
        striatum_core::accounts::decide_with_context(
            &request,
            &AllocatorState {
                roster: self.roster_for(provider, model, priced).roster,
                affinity,
                fences: self.probe_fences(
                    provider,
                    model,
                    self.fences.get(provider).cloned().unwrap_or_default(),
                ),
                principal_exclusions: Vec::new(),
                admissions: Vec::new(),
                operator_override: None,
                policy,
            },
            self.now_ms,
            &inputs,
        )
    }

    pub(crate) fn refusal_mark(
        &self,
        fact: &striatum_core::ModelFact,
        priced: bool,
    ) -> Option<striatum_core::CredentialRefusalMark> {
        if !self.credentials.listed_credentials().iter().any(|row| {
            crate::broca_credentials::credential_serves_fact(
                row,
                fact,
                &self.catalog,
                &self.credentials,
            )
        }) {
            return None;
        }
        let class = self.class(fact, priced);
        if class.rows.iter().any(|row| self.eligible(row, fact)) {
            return None;
        }
        if let Some(detail) = self.login_condition_detail(&fact.provider_id, &fact.model_id, priced)
        {
            return Some(striatum_core::CredentialRefusalMark {
                reason: "login_condition",
                credential_ids: detail.credential_ids.into_iter().collect(),
            });
        }
        let refused: Vec<_> = class.rows.iter().map(|row| row.id.clone()).collect();
        let reason = if !refused.is_empty()
            && class.rows.iter().all(|row| {
                self.refusals.iter().any(|refusal| {
                    refusal.provider_id == fact.provider_id && refusal.credential_id == row.id
                })
            }) {
            crate::credential_fences::rejection_reason(&self.refusals, &fact.provider_id, &refused)
        } else {
            "no_eligible_account"
        };
        Some(striatum_core::CredentialRefusalMark {
            reason,
            credential_ids: refused,
        })
    }

    pub(crate) fn apply_eligibility(
        &self,
        snapshot: &mut striatum_core::ModelRouteStateSnapshot,
        facts: &[striatum_core::ModelFact],
        priced: bool,
        defer_pool_pin: bool,
    ) {
        for fact in facts {
            if defer_pool_pin && self.targets.contains_key(&fact.provider_id) {
                continue;
            }
            if let Some(mark) = self.refusal_mark(fact, priced) {
                snapshot
                    .by_capacity
                    .entry(striatum_core::make_capacity_key(
                        &fact.provider_id,
                        &fact.model_id,
                    ))
                    .or_default()
                    .credential_refusal = Some(mark);
            }
        }
    }

    /// Legacy selection needs request-local account walls because it does not run
    /// allocator preflight. V1 must keep these walls at account preflight: a
    /// capacity wall would hide an adequate route and lose `accounts_blocked`
    /// and the reset that lets the caller wait.
    pub(crate) fn apply_legacy_route_fences(
        &self,
        snapshot: &mut striatum_core::ModelRouteStateSnapshot,
        facts: &[striatum_core::ModelFact],
        priced: bool,
        defer_pool_pin: bool,
    ) {
        for fact in facts {
            if defer_pool_pin && self.targets.contains_key(&fact.provider_id) {
                continue;
            }
            if let Some(reset) = self
                .blocked_reset(&fact.provider_id, &fact.model_id, priced)
                .flatten()
                .or_else(|| self.route_fence_reset(&fact.provider_id, &fact.model_id, priced))
            {
                let state = snapshot
                    .by_capacity
                    .entry(striatum_core::make_capacity_key(
                        &fact.provider_id,
                        &fact.model_id,
                    ))
                    .or_default();
                state.quota_exhausted_until =
                    Some(state.quota_exhausted_until.unwrap_or(0.0).max(reset as f64));
                state.quota_pressure = Some(1.0);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn launch_credential(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
    ) -> Result<String, LaunchRefusal> {
        match self
            .probe(provider, model, priced, false, None, "launch-preflight")
            .outcome
        {
            Outcome::Admit(admission) => Ok(admission.credential_id),
            // Selection scored this route as admissible; refusing it now
            // means its evidence changed or another request took its probe
            // since. Return a retry time, as when another request
            // takes the probe first.
            _ => Err(LaunchRefusal::Lost {
                retry_at: self
                    .blocked_reset(provider, model, priced)
                    .flatten()
                    .unwrap_or(self.now_ms.saturating_add(120_000)),
            }),
        }
    }

    /// A legacy send has no later account admission. Claim its selected
    /// origins here, or fail closed when the runner cannot pin the credential
    /// needed for a due account-local probe. Route/provider probes need no
    /// pin. A probe that another request holds, or that is not due yet, is a
    /// timed refusal, so the caller can try another route or wait.
    ///
    /// On success the answer is the credential the launch must pin, or
    /// `None` when it needs no pin. That is decided on the state the claim
    /// decided on, never on this context's older read: a block recorded on
    /// another login since the read still requires the pin, so the send
    /// cannot land on the blocked login.
    #[cfg(test)]
    pub(crate) fn claim_launch_credential(
        &self,
        store: &SqliteRouteStateStore,
        provider: &str,
        model: &str,
        priced: bool,
        pinnable: bool,
        owner: Option<&str>,
    ) -> Result<Option<String>, LaunchRefusal> {
        // Whether anything needs claiming, and which branch claims it, is
        // decided on the current published state, which holds every block
        // committed since this context was read.
        let current = crate::account_state::published(store, self.now_ms)
            .and_then(|published| published.fence_rechecks())
            .map_err(LaunchRefusal::store)?;
        #[cfg(test)]
        run_hook(&self.probe_scope, store, HookPoint::LaunchCommand);
        let account_scoped = |origin: &striatum_store::FenceRecheck| {
            origin.provider == provider
                && origin.model.as_deref().is_none_or(|named| named == model)
                && (origin.account_id.is_some() || origin.credential_id.is_some())
                && origin.principal.is_none()
                && origin.size_class.is_none()
        };
        if !self.needs_credential_pin_with(&current, provider, model)
            && !current.iter().any(|origin| {
                origin.provider == provider
                    && origin.model.as_deref().is_none_or(|named| named == model)
            })
        {
            return Ok(None);
        }
        if !pinnable && !current.iter().any(account_scoped) {
            // The login this context would launch on. Only an account block
            // that applies to it stops this launch; another login's block
            // does not. Without one, any account block on the provider does.
            let launch = self
                .launch_credential(provider, model, priced)
                .ok()
                .map(|credential| {
                    let account = crate::credential_fences::credential_account_id(
                        &self.account_ids,
                        &credential,
                    )
                    .unwrap_or_else(|| credential.clone());
                    (account, credential)
                });
            let context = self.clone();
            let owned_provider = provider.to_owned();
            let owned_model = model.to_owned();
            let owned_owner = owner.map(str::to_owned);
            let release_at = context.now_ms;
            let granted = crate::account_writer::repair(store, move |tx| {
                tx.release_expired(release_at)?;
                let provider = owned_provider.as_str();
                let model = owned_model.as_str();
                let owner = owned_owner.as_deref();

                let all = tx.fence_rechecks()?;
                let fences = load_fences(&*tx, provider)?;
                tx.check_decision_inputs(&[provider, ""])?;
                // An account block recorded since the branch was chosen on
                // the login this launch uses: this runner cannot pin its
                // credential, so it waits for it.
                let scoped: Vec<_> = all
                    .iter()
                    .filter(|origin| {
                        origin.provider == provider
                            && origin.model.as_deref().is_none_or(|named| named == model)
                            && (origin.account_id.is_some() || origin.credential_id.is_some())
                            && origin.principal.is_none()
                            && origin.size_class.is_none()
                    })
                    .filter(|origin| match &launch {
                        Some((account, credential)) => {
                            context.origin_applies_with(
                                &fences, origin, provider, account, credential, model,
                            ) && !context.origin_recovered(origin, account)
                        }
                        None => true,
                    })
                    .cloned()
                    .collect();
                if !scoped.is_empty() {
                    return Ok(Some(lost_probe_retry_at(
                        &scoped,
                        owner.or(context.probe_owner.as_deref()),
                        context.now_ms,
                    )));
                }
                let origins: Vec<_> = all
                    .into_iter()
                    .filter(|origin| {
                        origin.account_id.is_none()
                            && origin.credential_id.is_none()
                            && origin.principal.is_none()
                            && origin.size_class.is_none()
                            && origin.applies(provider, "", "", model)
                    })
                    .collect();
                let granted = tx.grant_fence_rechecks(
                    &origins,
                    context.now_ms,
                    owner.or(context.probe_owner.as_deref()),
                )?;
                Ok((!granted).then(|| {
                    lost_probe_retry_at(
                        &origins,
                        owner.or(context.probe_owner.as_deref()),
                        context.now_ms,
                    )
                }))
            })
            .map_err(LaunchRefusal::store)?;
            return match granted {
                None => Ok(None),
                Some(retry_at) => Err(LaunchRefusal::Lost { retry_at }),
            };
        }
        let credential = self.launch_credential(provider, model, priced)?;
        let account =
            crate::credential_fences::credential_account_id(&self.account_ids, &credential)
                .unwrap_or_else(|| credential.clone());
        let context = self.clone();
        let owned_provider = provider.to_owned();
        let owned_model = model.to_owned();
        let owned_owner = owner.map(str::to_owned);
        let owned_credential = credential.clone();
        let release_at = context.now_ms;
        let claimed = crate::account_writer::repair(store, move |tx| {
            tx.release_expired(release_at)?;
            let provider = owned_provider.as_str();
            let model = owned_model.as_str();
            // The claim is made for this owner, or for the selection's probe
            // owner when it names none; a probe clock (`fence_recheck` row) that owner
            // already claimed counts as its own claim.
            let owner = owned_owner.as_deref().or(context.probe_owner.as_deref());
            let credential = owned_credential.clone();
            // The command's own fences, not the context's (see bind).
            let fences = load_fences(&*tx, provider)?;
            let all = tx.fence_rechecks()?;
            // Whether the launch must pin its credential, on the clocks this
            // command decides on.
            let pin = context.needs_credential_pin_with(&all, provider, model);
            let origins: Vec<_> = all
                .into_iter()
                .filter(|origin| {
                    context.origin_applies_with(
                        &fences,
                        origin,
                        provider,
                        &account,
                        &credential,
                        model,
                    ) && !context.origin_recovered(origin, &account)
                })
                .collect();
            tx.check_decision_inputs(&[provider, ""])?;
            let due = |origin: &striatum_store::FenceRecheck| {
                context.origin_due(origin, &account, model)
                    || owner.is_some() && origin.claim_owner.as_deref() == owner
            };
            // Use the store's current group of quota recheck clocks. `origin_due`
            // uses the earlier context snapshot and can treat a newly recorded
            // quota clock as due.
            let clock_due = |origin: &striatum_store::FenceRecheck| {
                if origin.quota {
                    striatum_store::FenceRecheck::quota_group_due(
                        origins.iter().filter(|other| other.quota),
                        context.now_ms,
                        owner,
                    )
                } else {
                    origin.due_at() <= context.now_ms
                        || owner.is_some() && origin.claim_owner.as_deref() == owner
                }
            };
            // Only a due account probe needs a runner that pins its
            // credential; a block that is not due yet is waited for, as by a
            // pinning runner.
            if !pinnable
                && origins.iter().any(|origin| {
                    (origin.account_id.is_some() || origin.credential_id.is_some())
                        && due(origin)
                        && clock_due(origin)
                })
            {
                return Ok(Err(LaunchRefusal::Refused {
                    code: "no_eligible_account".into(),
                    message: format!(
                        "{provider}/{model}: a probe requires a credential-pinning runner"
                    ),
                }));
            }
            let lost = || LaunchRefusal::Lost {
                retry_at: lost_probe_retry_at(&origins, owner, context.now_ms),
            };
            if !origins.iter().all(due) {
                return Ok(Err(lost()));
            }
            Ok(
                if tx.grant_fence_rechecks(&origins, context.now_ms, owner)? {
                    Ok(pin)
                } else {
                    Err(lost())
                },
            )
        })
        .map_err(LaunchRefusal::store)?;
        claimed.map(|pin| pin.then_some(credential))
    }

    /// Whether a launch of `provider`/`model` must pin its credential: an
    /// account-scoped clock among `rechecks` (the clocks a claim decides on,
    /// never only this context's older read), a continuation, or a refusal,
    /// eligibility fact, size refusal or route fence this context read.
    #[cfg(test)]
    fn needs_credential_pin_with(
        &self,
        rechecks: &[striatum_store::FenceRecheck],
        provider: &str,
        model: &str,
    ) -> bool {
        if rechecks.iter().any(|origin| {
            origin.provider == provider
                && origin.principal.is_none()
                && origin.size_class.is_none()
                && origin.model.as_deref().is_none_or(|named| named == model)
                && (origin.account_id.is_some() || origin.credential_id.is_some())
        }) {
            return true;
        }
        self.continuation_credential.is_some()
            || self
                .refusals
                .iter()
                .any(|fact| fact.provider_id == provider)
            || self.eligibility.iter().any(|fact| {
                fact.provider == provider && (fact.model.is_empty() || fact.model == model)
            })
            || self
                .size_refusals
                .iter()
                .any(|fact| fact.provider == provider && fact.model == model)
            || self.route_fences.iter().any(|fence| {
                fence.provider == provider && fence.model == model && fence.until_ms > self.now_ms
            })
    }

    pub(super) fn allocation_context(
        &self,
        provider: &str,
        model: &str,
    ) -> striatum_core::accounts::AllocationContext {
        let mut context = striatum_core::accounts::AllocationContext::default();
        for row in self.credentials.listed_credentials().iter().filter(|row| {
            self.indexed_origins(provider, self.account_id(row), &row.id, model)
                .any(|origin| {
                    self.origin_applies(origin, provider, self.account_id(row), &row.id, model)
                        && self.indexed_origin_due(origin, self.account_id(row), model)
                })
        }) {
            context.credential_probe_notes.insert(
                row.id.clone(),
                crate::credential_fences::probe_note(provider, &row.id),
            );
        }
        let class = self.fact_for(provider, model);
        if let Some(fact) = class {
            let serving = self.serving_index.fact(
                &self.config,
                &self.credentials,
                fact,
                &self.catalog,
                &self.targets,
            );
            for row in serving
                .selection
                .iter()
                .map(|row| &self.credentials.listed_credentials()[row.index])
            {
                let account = self.account_id(row);
                if let Some(reading) = self
                    .reading_index()
                    .accounts
                    .get(account)
                    .into_iter()
                    .flatten()
                    .max_by_key(|reading| reading.fetched)
                {
                    add_spend_inputs(&mut context, account, &reading.reading.entry);
                }
            }
        }
        context
    }

    pub(super) fn restrict_size(
        &self,
        roster: &mut RosterBuild,
        provider: &str,
        model: &str,
        tokens: u64,
    ) {
        let class = striatum_core::accounts::input_size_class(tokens);
        if let Some(roster) = &mut roster.roster {
            for view in &mut roster.accounts {
                view.credentials.retain(|credential| {
                    !self.size_refusals.iter().any(|fact| {
                        fact.provider == provider
                            && fact.model == model
                            && fact.credential_id == credential.credential_id
                            && fact.size_class == class
                            && self.rechecks.iter().any(|origin| {
                                matches!(origin.kind.as_str(), "size" | "non_quota_size")
                                    && origin.applies(
                                        provider,
                                        &view.account_id,
                                        &credential.credential_id,
                                        model,
                                    )
                                    && origin.size_class == Some(class as i64)
                                    && !self.origin_due(origin, &view.account_id, model)
                            })
                    })
                });
            }
            roster.accounts.retain(|view| !view.credentials.is_empty());
        }
    }
}

fn add_spend_inputs(
    context: &mut striatum_core::accounts::AllocationContext,
    account: &str,
    entry: &Value,
) {
    use striatum_core::accounts::{CreditReading, SpendReading};
    let Some(at) = entry
        .get("fetchedAt")
        .and_then(Value::as_str)
        .and_then(account_quota::parse_rfc3339_ms)
    else {
        return;
    };
    if entry
        .get("errorClass")
        .is_some_and(|value| !value.is_null())
        || entry.get("error").is_some_and(|value| !value.is_null())
        || entry.get("stale").is_some_and(|value| !value.is_null())
    {
        return;
    }
    for pool in entry
        .get("spend")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let amount = |key: &str| {
            pool.get(key)
                .and_then(|value| {
                    serde_json::from_value::<striatum_core::quota::SpendBalance>(value.clone()).ok()
                })
                .map(|balance| balance.minor as f64 * 10_f64.powi(-balance.exponent))
        };
        let pool_id = pool.get("id").and_then(Value::as_str);
        if matches!(pool_id, Some("extra_usage" | "credits")) {
            let Some(remaining) = amount("remaining").filter(|amount| amount.is_finite()) else {
                continue;
            };
            let spendable =
                pool.get("spendable").and_then(Value::as_bool) != Some(false) && remaining > 0.0;
            context.credits.insert(
                account.to_owned(),
                CreditReading {
                    observed_at_ms: at,
                    spendable,
                    prepaid: pool_id == Some("credits"),
                },
            );
        } else if let (Some(total), Some(remaining)) = (amount("total"), amount("remaining")) {
            context.spend.insert(
                account.to_owned(),
                SpendReading {
                    used: (total - remaining).max(0.0),
                    reset_at_ms: None,
                },
            );
        }
    }
}

#[cfg(test)]
mod spend_input_tests {
    use super::*;

    #[test]
    fn absent_or_failed_spend_readings_do_not_authorize_credits() {
        let at = 1_800_000_000_000;
        let entry = json!({"fetchedAt":rfc3339(at),"spend":[{"id":"credits","remaining":{"minor":100,"exponent":0,"unit":"credit"}}]});
        for unusable in [
            json!({}),
            json!({"fetchedAt":rfc3339(at)}),
            json!({"fetchedAt":rfc3339(at),"spend":[{"id":"credits","spendable":true}]}),
            json!({"fetchedAt":rfc3339(at),"spend":[{"id":"extra_usage","spendable":true}]}),
            {
                let mut failed = entry.clone();
                failed["errorClass"] = json!("upstream_failed");
                failed
            },
            {
                let mut stale = entry.clone();
                stale["stale"] = json!({"class":"upstream_failed"});
                stale
            },
        ] {
            let mut context = striatum_core::accounts::AllocationContext::default();
            add_spend_inputs(&mut context, "a", &unusable);
            assert!(context.credits.is_empty(), "{unusable}");
        }
        let mut context = striatum_core::accounts::AllocationContext::default();
        add_spend_inputs(&mut context, "a", &entry);
        assert!(context.credits["a"].spendable);
        assert!(context.credits["a"].prepaid, "the credits pool is prepaid");
        assert_eq!(context.credits["a"].observed_at_ms, at);
        let overage = json!({"fetchedAt":rfc3339(at),"spend":[{"id":"extra_usage","remaining":{"minor":100,"exponent":0,"unit":"usd"}}]});
        let mut context = striatum_core::accounts::AllocationContext::default();
        add_spend_inputs(&mut context, "a", &overage);
        assert!(context.credits["a"].spendable);
        assert!(
            !context.credits["a"].prepaid,
            "extra usage is metered overage"
        );
    }
}

#[cfg(test)]
mod compiled_class_tests;
#[cfg(test)]
mod serving_index_tests;
