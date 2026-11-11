//! Account selection over the wire: `route.account_for` and the runner reports
//! that follow it (`route.account_observe`, `route.account_release`).
//! `route.account_status` reads quota headroom for cache warming without
//! admissions, decisions, store access or polling.
//!
//! Specification: `docs/designs/account-selection.md` ("Proposed protocol",
//! "Affinity, persistence and freeze semantics", "Operator pins and rollout").
//! The allocator is the pure `prefrontal_routing_core::accounts::decide`;
//! this module builds its inputs (the roster joined from the vault listing and
//! Insula readings, and the stored affinity, fences, exclusions and demand),
//! commits its result in one store transaction and renders the reply.
//!
//! Account selection always serves. Every decision routing records is one it answered a
//! caller with (`mode = 'serve'`).

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_core::accounts::eligibility::{assess_reading, reserve_for};
use prefrontal_routing_core::accounts::evidence::{
    confirm_exhaustion, header_window, Carrier, ConfirmationOutcome, Derivation, EvidenceScope,
    ExhaustionObservation, FrameOrigin, ObservationKind, ResetSource, WindowEvidence,
};
use prefrontal_routing_core::accounts::{
    block_end, grown_recovery_set, recovery_set_from, validate_pool_name, AccountRef,
    AccountRequest, AccountStrategy, AccountView, AdmissionDemand, AdmissionState, AffinityOutcome,
    AllocatorPolicy, AllocatorState, ApplicableWindow, CallerKind, CandidateRef,
    ConfirmationSource, ConversationAffinity, ConversationKeySource, CredentialRecord, Decision,
    EpochMs, Fence, GeneralWindow, ModelFamily, ModelReach, ObservationSource, Outcome,
    PoolWindowReading, PrincipalExclusion, QuotaReading, QuotaWindow, RefusalCode, ResetTime,
    Roster, RunnerCandidates, ScopedList, ScopedWindow, StaleDisclosure,
};
use prefrontal_routing_core::quota::RateWindow;
use prefrontal_routing_core::{RouteStateResult, RouteStateStoreError};
use prefrontal_routing_store::{
    AccountAdmissionRow, AccountAffinityRow, AccountDecisionRecord, AccountExclusionRow,
    AccountFenceRow, AccountHoldRow, AccountObservationRow, AccountPinRow, AccountPoolFenceRow,
    AccountTx, SqliteRouteStateStore, DECISION_MODE_SERVE,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::account_quota::{self, AccountEntry, AccountQuotaState, SharedAccountQuota};
use crate::account_refusal_log::{because, checking};
use crate::config_overrides::AccountSelectionFlags;

/// Choose the account for one admission, renewal or reassignment.
pub const ACCOUNT_FOR_OP: &str = "route.account_for";
/// Read whether an account's quota has room for prompt-cache warming.
pub const ACCOUNT_STATUS_OP: &str = "route.account_status";
/// Report account-bound admission, dispatch and failure evidence.
pub const ACCOUNT_OBSERVE_OP: &str = "route.account_observe";
/// Release an admission that never started, or end a conversation's lease.
pub const ACCOUNT_RELEASE_OP: &str = "route.account_release";

/// The refusal every account operation answers while serving is off. This
/// code is intentionally preserved verbatim: it means the operator switched
/// account selection off for the whole module (the serving flag), and Broca
/// falls back to its static credential on it, so it must stay
/// distinguishable from quota or auth refusals. The contract declares it
/// (0.5.2) as untimed with empty detail, module-wide.
pub const DISABLED_CODE: &str = "account_selection_disabled";

/// A reservation that has not started within this long expires.
const ADMISSION_VALID_MS: i64 = 60_000;
/// How long a caller may dispatch on a pin without renewing it.
const LEASE_MS: i64 = 2 * 60_000;
/// Conversation affinity lasts this long idle.
const AFFINITY_IDLE_MS: i64 = 7 * 24 * 60 * 60_000;
/// Accepted use renews affinity at most this often.
const AFFINITY_TOUCH_MS: i64 = 60_000;
/// The longest hold a caller waits out when it does not say.
pub(crate) const DEFAULT_HOLD_WAIT_S: u64 = 900;
/// The principal a worker-launch preflight decides under. No caller can hold
/// it, so a preflight never reads or touches a real conversation.
const PREFLIGHT_PRINCIPAL: &str = "routing:preflight";

// ---------------------------------------------------------------------------
// Replies
// ---------------------------------------------------------------------------

/// What an account operation answers: a result, or a typed refusal whose
/// `detail` object repeats the refusal `code` and carries `retry_after_s`,
/// `decision_id`, the per-account `exclusions` and any fields specific to
/// that code (the error shape in the design's "Proposed protocol").
#[derive(Debug, Clone, PartialEq)]
pub enum OpReply {
    Result(Value),
    Error {
        code: String,
        message: String,
        detail: Option<Value>,
    },
}

impl OpReply {
    fn error(code: &str, message: impl Into<String>) -> Self {
        OpReply::Error {
            code: code.to_string(),
            message: message.into(),
            detail: None,
        }
    }

    fn typed(code: &str, message: impl Into<String>, detail: Value) -> Self {
        OpReply::Error {
            code: code.to_string(),
            message: message.into(),
            detail: Some(detail),
        }
    }

    #[cfg(test)]
    pub fn code(&self) -> Option<&str> {
        match self {
            OpReply::Result(_) => None,
            OpReply::Error { code, .. } => Some(code),
        }
    }
}

fn invalid(message: impl Into<String>) -> OpReply {
    OpReply::error("invalid_request", message)
}

/// Whether `method` is one of the account-selection operations.
pub fn handles(method: &str) -> bool {
    matches!(
        method,
        ACCOUNT_FOR_OP | ACCOUNT_STATUS_OP | ACCOUNT_OBSERVE_OP | ACCOUNT_RELEASE_OP
    )
}

/// The label a route's daemon-attested principal is recorded under. Only this
/// namespaces admissions, affinity and mutation authority; the request's
/// `caller` is audit metadata.
pub fn principal_label(principal: Option<&subc_protocol::Principal>) -> String {
    match principal {
        Some(subc_protocol::Principal::Reserved { module_id }) => format!("reserved:{module_id}"),
        Some(subc_protocol::Principal::Direct) => "direct".to_string(),
        Some(subc_protocol::Principal::Unverified) => "unverified".to_string(),
        None => "unattested".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// Everything one operation reads besides its request.
pub struct Ctx<'a> {
    pub store: &'a SqliteRouteStateStore,
    /// The live per-account quota state and vault roster; `None` before boot
    /// restored it.
    pub accounts: Option<&'a SharedAccountQuota>,
    pub flags: AccountSelectionFlags,
    pub policy: &'a AllocatorPolicy,
    pub now_ms: i64,
    pub classes: Option<ClassContext>,
}

impl Ctx<'_> {
    fn provider_policy(&self, provider: &str) -> AllocatorPolicy {
        self.classes.as_ref().map_or_else(
            || self.policy.clone(),
            |classes| classes.config.allocator_policy(provider),
        )
    }
}

/// One immutable vault/config/catalog view shared by selection and allocation.
#[derive(Clone)]
pub(crate) struct ClassContext {
    rechecks: Vec<prefrontal_routing_store::FenceRecheck>,
    pub credentials: crate::broca_credentials::BrocaCredentials,
    pub config: std::sync::Arc<crate::config_overrides::LiveConfig>,
    pub facts: Vec<prefrontal_routing_core::ModelFact>,
    pub refusals: Vec<prefrontal_routing_core::route_state_store::CredentialEntitlementFence>,
    pub quota: AccountQuotaState,
    pub fences: BTreeMap<String, Vec<Fence>>,
    pub now_ms: i64,
    eligibility: Vec<prefrontal_routing_store::AccountEligibilityFact>,
    size_refusals: Vec<prefrontal_routing_store::AccountSizeRefusal>,
    route_fences: Vec<prefrontal_routing_store::AccountRouteFence>,
    transient_route_fences: Vec<prefrontal_routing_store::AccountRouteFence>,
    pool_walls: BTreeSet<(String, String)>,
    catalog: BTreeSet<String>,
    targets: BTreeMap<String, String>,
    tier_accounts: Option<BTreeMap<String, Vec<String>>>,
    continuation_credential: Option<String>,
    media_route: bool,
    probe_scope: std::sync::Weak<std::sync::Mutex<crate::broca_credentials::BrocaCredentials>>,
    probe_owner: Option<String>,
    selection_probes: Option<std::sync::Arc<probe_reservations::Selection>>,
}

mod probe_reservations;

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
    fact: &prefrontal_routing_core::ModelFact,
    catalog: &BTreeSet<String>,
    specs: &[prefrontal_routing_core::provider_model_spec::ParsedSpec],
    borrowed: bool,
    priced_per_token: bool,
    metered_fallback: bool,
    usable: impl Fn(&cortexkit_claustrum_client::ListedCredential) -> bool,
) -> RouteClass<'a> {
    let key = format!("{}/{}", fact.provider_id, fact.model_id);
    let serving: Vec<_> = rows
        .iter()
        .filter(|row| crate::broca_credentials::credential_serves_fact(row, fact, catalog))
        .collect();
    let covered = |row: &&cortexkit_claustrum_client::ListedCredential| {
        !borrowed
            && prefrontal_routing_core::provider_model_spec::resolve_spec(
                specs,
                &key,
                row.credential_type == "oauth",
            )
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
                    && (!priced_per_token || row.auth_method.as_deref() == Some("apikey"))
            })
            .collect(),
        subscription,
    }
}

impl ClassContext {
    pub(crate) fn probe_accounts(&self) -> BTreeMap<String, String> {
        self.credentials
            .listed_credentials()
            .iter()
            .filter_map(|row| {
                crate::credential_fences::credential_account_id(&self.credentials, &row.id)
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
            prefrontal_routing_store::FenceRecheck::quota_group_at(origins.iter().copied(), None);
        if origins.iter().any(|origin| origin.quota) && quota_at <= self.now_ms {
            quota_at = origins
                .iter()
                .filter_map(|origin| origin.reset_check_at)
                .filter(|at| *at > self.now_ms)
                .chain(std::iter::once(self.now_ms.saturating_add(
                    prefrontal_routing_core::fence_lifetimes::QUOTA_FENCE_RECHECK_MS,
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
        let quota_due = prefrontal_routing_store::FenceRecheck::quota_group_due(
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
    ) -> Vec<(String, String)> {
        self.rechecks
            .iter()
            .filter(|origin| {
                self.origin_applies(origin, provider, account, credential, model)
                    && self.origin_recovered(origin, account)
            })
            .map(|origin| (origin.kind.clone(), origin.origin_key.clone()))
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
        facts: &mut [prefrontal_routing_core::ModelFact],
        resolve: impl Fn(
            &prefrontal_routing_core::ModelFact,
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
        facts: &mut [prefrontal_routing_core::ModelFact],
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
                        registry.runs(
                            &runner.module,
                            &prefrontal_routing_core::fact_to_route(fact).route_key,
                            &key,
                        ) && registry.get(&runner.module).is_some_and(|runner| {
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
                    let account = row.account_id.as_deref().unwrap_or(&row.id);
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
        fact: &prefrontal_routing_core::ModelFact,
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
        use prefrontal_routing_core::RouteStateStore;
        let facts = store.get_facts().map_err(|error| error.to_string())?;
        Self::read(store, credentials, config, accounts, facts, now_ms)
    }

    pub(crate) fn read(
        store: &SqliteRouteStateStore,
        credentials: &crate::broca_credentials::SharedBrocaCredentials,
        config: std::sync::Arc<crate::config_overrides::LiveConfig>,
        accounts: Option<&SharedAccountQuota>,
        mut facts: Vec<prefrontal_routing_core::ModelFact>,
        now_ms: i64,
    ) -> Result<Self, String> {
        #[cfg(test)]
        CONTEXT_LOADS.with(|loads| loads.set(loads.get() + 1));
        if store
            .account_read(|tx| tx.has_expired_continuations(now_ms))
            .map_err(|error| error.to_string())?
        {
            store
                .account_transaction(|tx| tx.release_expired_continuations(now_ms))
                .map_err(|error| error.to_string())?;
        }
        let refusals = crate::credential_fences::live(store, credentials)?;
        let probe_scope = std::sync::Arc::downgrade(credentials);
        let credentials = credentials
            .lock()
            .map_err(|error| error.to_string())?
            .clone();
        if credentials.listing_version().is_some() {
            let listings = credentials
                .listed_credentials()
                .iter()
                .filter_map(|row| match credentials.listing_fingerprint(&row.id) {
                    crate::broca_credentials::ListingLookup::Listed(fingerprint) => {
                        Some((row.id.clone(), fingerprint))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            store
                .reconcile_account_eligibility(&listings)
                .map_err(|error| error.to_string())?;
        }
        let eligibility = store
            .account_eligibility()
            .map_err(|error| error.to_string())?;
        let mut size_refusals = store
            .account_size_refusals(now_ms)
            .map_err(|error| error.to_string())?;
        let catalog = store
            .catalog_providers()
            .map_err(|error| error.to_string())?;
        let identity_facts: Vec<_> = facts
            .iter()
            .filter(|fact| catalog.contains(&fact.provider_id))
            .cloned()
            .collect();
        let targets = credentials.catalog_targets(&identity_facts);
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
            for (credential, state) in store
                .pool_key_states(provider, now_ms)
                .map_err(|error| error.to_string())?
            {
                if state.walled_until_ms.is_some_and(|until| until > now_ms)
                    || state.cooldown_until_ms.is_some_and(|until| until > now_ms)
                {
                    pool_walls.insert((provider.clone(), credential));
                }
            }
        }
        prefrontal_routing_core::score_alias::append_alias_rows(&mut facts, &targets);
        let quota = accounts
            .and_then(|accounts| accounts.lock().ok().map(|state| state.clone()))
            .unwrap_or_default();
        let credentials = credentials.with_quota_account_ids(&quota);
        let size_clocks = store.fence_rechecks().map_err(|error| error.to_string())?;
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
                crate::credential_fences::credential_account_id(&credentials, &fact.credential_id)
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
                store
                    .clear_account_size_refusal(
                        &fact.provider,
                        &fact.credential_id,
                        &fact.model,
                        fact.size_class,
                        fact.observed_at.saturating_add(1),
                    )
                    .map_err(|error| error.to_string())?;
            }
        }
        size_refusals = store
            .account_size_refusals(now_ms)
            .map_err(|error| error.to_string())?;
        let mut route_fences = store
            .account_route_fences(now_ms)
            .map_err(|error| error.to_string())?;
        let recorded_route_fences = route_fences.clone();
        let latest_reports = store
            .account_route_fence_latest_reports()
            .map_err(|error| error.to_string())?;
        let transient_route_fences = store
            .transient_account_route_fences(now_ms)
            .map_err(|error| error.to_string())?;
        let credentials_with_headroom: BTreeSet<_> = route_fences
            .iter()
            .filter_map(|fence| {
                let account = crate::credential_fences::credential_account_id(
                    &credentials,
                    &fence.credential_id,
                )?;
                // Clear all routes for a credential only after a reading newer
                // than every report on it; an older reading cannot heal a newer failure.
                let newest_report = *latest_reports.get(&fence.credential_id)?;
                crate::credential_fences::fresh_headroom_after_for_account(
                    &quota,
                    &account,
                    newest_report,
                    now_ms,
                    &config.allocator_policy(&fence.provider),
                )
                .then(|| {
                    let at = crate::credential_fences::latest_reading_for_account(&quota, &account)
                        .and_then(|reading| reading.entry.get("fetchedAt").and_then(Value::as_str))
                        .and_then(account_quota::parse_rfc3339_ms)?;
                    Some((fence.provider.clone(), fence.credential_id.clone(), at))
                })
                .flatten()
            })
            .collect();
        for (provider, credential, at) in &credentials_with_headroom {
            store
                .clear_account_route_fences_before(provider, credential, *at)
                .map_err(|error| error.to_string())?;
        }
        route_fences = store
            .account_route_fences(now_ms)
            .map_err(|error| error.to_string())?;
        for fence in &mut route_fences {
            if let Some(reset) = crate::credential_fences::exhausted_quota_reset(
                &quota,
                &credentials,
                &fence.credential_id,
                now_ms,
                &config.allocator_policy(&fence.provider),
            ) {
                let fetched = crate::credential_fences::credential_account_id(
                    &credentials,
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
                    store
                        .reconcile_account_route_fence(
                            fence,
                            until,
                            latest_reports[&fence.credential_id],
                        )
                        .map_err(|error| error.to_string())?;
                    fence.until_ms = until;
                }
            }
        }
        let refusals = refresh_credential_quota_holds(
            store,
            &credentials,
            &quota,
            &config,
            refusals,
            &recorded_route_fences,
            now_ms,
        )?;
        let fences = store
            .account_read(|tx| {
                facts
                    .iter()
                    .map(|fact| fact.provider_id.clone())
                    .chain(media_providers.iter().cloned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .map(|provider| Ok((provider.clone(), load_fences(tx, &provider)?)))
                    .collect()
            })
            .map_err(|error| error.to_string())?;
        let rechecks = store.fence_rechecks().map_err(|error| error.to_string())?;
        let mut context = Self {
            rechecks,
            credentials,
            config,
            facts,
            refusals,
            quota,
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
            store
                .account_transaction(|tx| {
                    for id in &recovered {
                        tx.clear_fence(id, now_ms, "fresh_headroom")?;
                    }
                    for (provider, fences) in &mut context.fences {
                        *fences = load_fences(tx, provider)?;
                    }
                    Ok(())
                })
                .map_err(|error| error.to_string())?;
            context.rechecks = store.fence_rechecks().map_err(|error| error.to_string())?;
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
        let hold = probe_reservations::Selection::new(credentials, probes, self.now_ms);
        self.probe_owner = Some(hold.owner.clone());
        self.selection_probes = Some(std::sync::Arc::new(hold));
        // A direct admission can claim a probe between the first store read
        // and reservation. Read after taking the hold so its renewed fence
        // cannot remain probe-eligible in this scoring snapshot.
        self.refusals = refresh_credential_quota_holds(
            store,
            &self.credentials,
            &self.quota,
            &self.config,
            crate::credential_fences::live(store, credentials)?,
            &self.route_fences,
            self.now_ms,
        )?;
        self.rechecks = store.fence_rechecks().map_err(|error| error.to_string())?;
        Ok(())
    }

    pub(crate) fn bind_selected_probe(
        &self,
        store: &SqliteRouteStateStore,
        provider: &str,
        model: &str,
        priced: bool,
        decision: &str,
    ) -> RouteStateResult<()> {
        let Some(hold) = &self.selection_probes else {
            return Ok(());
        };
        let Outcome::Admit(admission) = self
            .probe(provider, model, priced, false, None, "probe-reservation")
            .outcome
        else {
            return Ok(());
        };
        if self.probe_eligible(provider, model, &admission.credential_id) {
            hold.bind(provider, &admission.credential_id, decision, self.now_ms);
            store.bind_fence_probe_owner(&hold.owner, decision)?;
        }
        Ok(())
    }

    pub(crate) fn class(
        &self,
        fact: &prefrontal_routing_core::ModelFact,
        priced_per_token: bool,
    ) -> RouteClass<'_> {
        if self.media_route {
            return RouteClass {
                rows: self
                    .credentials
                    .listed_credentials()
                    .iter()
                    .filter(|row| media_credential_serves(row, &fact.provider_id))
                    .collect(),
                subscription: false,
            };
        }
        let specs = prefrontal_routing_core::provider_model_spec::parse_spec_list(
            &self.config.operator_policy().subscription,
        );
        let mut restricted = fact.clone();
        if let Some(credential) = &self.continuation_credential {
            let method = self.method(credential);
            restricted.auth_methods = Some(
                if method != "unknown"
                    && fact
                        .auth_methods
                        .as_ref()
                        .is_none_or(|allowed| allowed.iter().any(|allowed| allowed == method))
                {
                    vec![method.into()]
                } else {
                    Vec::new()
                },
            );
        }
        let fact = &restricted;
        resolve_class(
            self.credentials.listed_credentials(),
            fact,
            &self.catalog,
            &specs,
            self.targets.contains_key(&fact.provider_id),
            priced_per_token,
            self.config.metered_fallback(&fact.provider_id),
            |row| self.usable(row, fact),
        )
    }

    pub(crate) fn revalidate_credential(&mut self, credential: &str) {
        self.continuation_credential = Some(credential.into());
    }

    fn view(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &prefrontal_routing_core::ModelFact,
    ) -> AccountView {
        let account_id = row.account_id.clone().unwrap_or_else(|| row.id.clone());
        let capacity = format!("{}/{}", fact.provider_id, fact.model_id);
        let model_pools = self.config.quota_model_pools();
        let reading = self
            .quota
            .readings
            .readings
            .values()
            .filter(|reading| {
                reading.account_id == account_id
                    && reading.tombstoned_at_ms.is_none()
                    && reading.routing_provider().is_some_and(|vendor| {
                        vendor == fact.provider_id
                            || self
                                .targets
                                .get(&fact.provider_id)
                                .is_some_and(|target| target == vendor)
                    })
            })
            .max_by_key(|reading| {
                reading
                    .entry
                    .get("fetchedAt")
                    .and_then(Value::as_str)
                    .and_then(account_quota::parse_rfc3339_ms)
            });
        let usage = reading
            .and_then(|reading| serde_json::from_value::<AccountEntry>(reading.entry.clone()).ok())
            .and_then(|entry| entry.usage);
        let pooled = usage
            .as_ref()
            .and_then(|usage| usage.extra_rate_windows.as_ref())
            .into_iter()
            .flatten()
            .any(prefrontal_routing_core::quota_extract::is_account_pool_window);
        let model_reach = if pooled {
            ModelReach::Only(
                usage
                    .as_ref()
                    .filter(|usage| {
                        !prefrontal_routing_core::quota_extract::account_rate_windows(
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
                    !self.rechecks.iter().any(|origin| {
                        origin.quota
                            && self.origin_applies(
                                origin,
                                &fact.provider_id,
                                &account_id,
                                &row.id,
                                &fact.model_id,
                            )
                            && self.origin_due(origin, &account_id, &fact.model_id)
                    })
                })
                .filter(|_| {
                    crate::credential_fences::has_quota_signal_for_account(&self.quota, &account_id)
                })
                .and_then(|reading| {
                    quota_reading_for_model(
                        &reading.entry,
                        &reading.account_id,
                        Some(&capacity),
                        &model_pools,
                    )
                }),
        }
    }

    fn eligible(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &prefrontal_routing_core::ModelFact,
    ) -> bool {
        // Keep account quota in the allocator's roster, not its entitlement
        // filter. A scheduled probe temporarily bypasses that quota restriction;
        // admission must still claim the attempt before sending.
        !self.rechecks.iter().any(|origin| {
            !matches!(
                origin.kind.as_str(),
                "account"
                    | "account_route"
                    | "non_quota_route"
                    | "transient_route"
                    | "observation"
                    | "route"
                    | "provider"
                    | "failure"
            ) && self.origin_applies(
                origin,
                &fact.provider_id,
                row.account_id.as_deref().unwrap_or(&row.id),
                &row.id,
                &fact.model_id,
            ) && !self.origin_due(
                origin,
                row.account_id.as_deref().unwrap_or(&row.id),
                &fact.model_id,
            )
        }) && (!self.probe_eligible(&fact.provider_id, &fact.model_id, &row.id)
            || probe_reservations::blocked_until(
                &self.probe_scope,
                &fact.provider_id,
                &row.id,
                self.probe_owner.as_deref(),
                self.now_ms,
            )
            .is_none())
            && self.tier_accounts.as_ref().is_none_or(|accounts| {
                accounts
                    .get(&format!("{}/{}", fact.provider_id, fact.model_id))
                    .is_some_and(|ids| ids.contains(&row.id))
            })
            && !self.eligibility.iter().any(|fact_| {
                fact_.provider == fact.provider_id
                    && fact_.credential_id == row.id
                    && (fact_.model.is_empty() || fact_.model == fact.model_id)
            })
            && !self.refusals.iter().any(|fence| {
                fence.provider_id == fact.provider_id
                    && fence.credential_id == row.id
                    && !self.probe_eligible(&fact.provider_id, &fact.model_id, &row.id)
            })
    }

    fn probe_eligible(&self, provider: &str, model: &str, credential: &str) -> bool {
        let account =
            crate::credential_fences::credential_account_id(&self.credentials, credential)
                .unwrap_or_else(|| credential.into());
        self.rechecks.iter().any(|origin| {
            self.origin_applies(origin, provider, &account, credential, model)
                && self.origin_due(origin, &account, model)
        })
    }

    fn origin_applies(
        &self,
        origin: &prefrontal_routing_store::FenceRecheck,
        provider: &str,
        account: &str,
        credential: &str,
        model: &str,
    ) -> bool {
        origin.principal.is_none()
            && origin.size_class.is_none()
            && self.origin_targets(origin, provider, account, credential, model)
            && (origin.kind != "account"
                || self.fences.get(provider).is_some_and(|fences| {
                    fences.iter().any(|fence| {
                        fence.account_id == account
                            && fence.observed_at_ms == origin.evidence_at
                            && fence.applies_to(
                                model,
                                self.config
                                    .allocator_policy(provider)
                                    .family_for_model(model),
                                request_pool(None, provider, model).as_deref(),
                            )
                    })
                }))
    }

    fn origin_targets(
        &self,
        origin: &prefrontal_routing_store::FenceRecheck,
        provider: &str,
        account: &str,
        credential: &str,
        model: &str,
    ) -> bool {
        let named = origin
            .credential_id
            .as_deref()
            .filter(|id| {
                (origin.quota
                    || matches!(
                        origin.kind.as_str(),
                        "account_route" | "non_quota_route" | "transient_route"
                    ))
                    && crate::credential_fences::credential_account_id(&self.credentials, id)
                        .as_deref()
                        == Some(account)
            })
            .unwrap_or(credential);
        origin.applies(provider, account, named, model)
    }

    fn origin_due(
        &self,
        origin: &prefrontal_routing_store::FenceRecheck,
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

    fn origin_has_binding_reading(
        &self,
        origin: &prefrontal_routing_store::FenceRecheck,
        account: &str,
    ) -> bool {
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

    fn recheck_at(
        &self,
        origin: &prefrontal_routing_store::FenceRecheck,
        account: &str,
        model: &str,
    ) -> i64 {
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
            prefrontal_routing_store::FenceRecheck::quota_group_at(
                related,
                self.probe_owner.as_deref(),
            )
        } else {
            origin.due_at()
        }
    }

    fn recheck_hint(
        &self,
        origin: &prefrontal_routing_store::FenceRecheck,
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
                    prefrontal_routing_core::fence_lifetimes::QUOTA_FENCE_RECHECK_MS,
                )))
                .min()
                .unwrap()
        } else {
            at
        }
    }

    fn origin_recovered(
        &self,
        origin: &prefrontal_routing_store::FenceRecheck,
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
                    block_end(fence, reading.as_ref(), self.now_ms, &policy).is_some()
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

    fn quota_hold_reset(&self, provider: &str, credential: &str) -> Option<i64> {
        let account =
            crate::credential_fences::credential_account_id(&self.credentials, credential)?;
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
                &self.credentials,
                credential,
                self.now_ms,
                &self.config.allocator_policy(provider),
            ))
            .max()
    }

    fn has_quota_signal(&self, provider: &str, credential: &str) -> bool {
        crate::credential_fences::has_quota_signal(&self.quota, &self.credentials, credential)
            || self.quota_hold_reset(provider, credential).is_some()
    }

    /// A stale refusal is a fallback, never a reason to probe while an
    /// unfenced eligible identity in the same payment class can serve.
    fn eligible_rows<'a>(
        &self,
        rows: Vec<&'a cortexkit_claustrum_client::ListedCredential>,
        fact: &prefrontal_routing_core::ModelFact,
    ) -> Vec<&'a cortexkit_claustrum_client::ListedCredential> {
        let rows: Vec<_> = rows
            .into_iter()
            .filter(|row| self.eligible(row, fact))
            .collect();
        rows
    }

    fn usable(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &prefrontal_routing_core::ModelFact,
    ) -> bool {
        self.usable_candidate(row, fact, false)
    }

    fn usable_candidate(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &prefrontal_routing_core::ModelFact,
        only: bool,
    ) -> bool {
        if !self.eligible(row, fact) {
            return false;
        }
        let specs = prefrontal_routing_core::provider_model_spec::parse_spec_list(
            &self.config.operator_policy().subscription,
        );
        let key = format!("{}/{}", fact.provider_id, fact.model_id);
        let covered = |row: &cortexkit_claustrum_client::ListedCredential| {
            !self.targets.contains_key(&fact.provider_id)
                && prefrontal_routing_core::provider_model_spec::resolve_spec(
                    &specs,
                    &key,
                    row.credential_type == "oauth",
                )
        };
        let rows = self
            .credentials
            .listed_credentials()
            .iter()
            .filter(|other| {
                (!only || other.id == row.id)
                    && covered(other) == covered(row)
                    && self.eligible(other, fact)
                    && crate::broca_credentials::credential_serves_fact(other, fact, &self.catalog)
            })
            .collect();
        let roster = self.roster(
            self.eligible_rows(rows, fact)
                .into_iter()
                .map(|other| self.view(other, fact))
                .collect(),
        );
        matches!(
            prefrontal_routing_core::accounts::decide_with_context(
                &synthetic_request(&fact.provider_id, &fact.model_id, "class-preflight".into()),
                &AllocatorState {
                    roster: Some(roster),
                    affinity: None,
                    fences: self.probe_fences(&fact.provider_id,&fact.model_id,self.fences.get(&fact.provider_id).cloned().unwrap_or_default()),
                    principal_exclusions: Vec::new(),
                    admissions: Vec::new(),
                    operator_override: None,
                    policy: self.config.allocator_policy(&fact.provider_id),
                },
                self.now_ms,
                &self.allocation_context(&fact.provider_id, &fact.model_id),
            )
            .outcome,
            Outcome::Admit(admission) if admission.account_id == row.account_id.as_deref().unwrap_or(&row.id)
        )
    }

    fn roster(&self, accounts: Vec<AccountView>) -> Roster {
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

    fn roster_for(&self, provider: &str, model: &str, priced_per_token: bool) -> RosterBuild {
        let mut roster = self
            .roster_and_fences_for(provider, model, priced_per_token)
            .0;
        self.avoid_transient_accounts(&mut roster, provider, model);
        roster
    }

    /// A new conversation has no cache to preserve. Prefer an open account,
    /// but keep the roster intact when retrying a transient is the only option.
    fn avoid_transient_accounts(&self, roster: &mut RosterBuild, provider: &str, model: &str) {
        let fenced = self.accounts_fenced_by(&self.transient_route_fences, provider, model);
        if let Some(roster) = &mut roster.roster {
            if roster
                .accounts
                .iter()
                .any(|account| !fenced.contains_key(&account.account_id))
            {
                roster
                    .accounts
                    .retain(|account| !fenced.contains_key(&account.account_id));
            }
        }
    }

    /// The roster for one route and accounts temporarily excluded by its
    /// applicable evidence, each with the next permitted recheck time.
    fn roster_and_fences_for(
        &self,
        provider: &str,
        model: &str,
        priced_per_token: bool,
    ) -> (RosterBuild, BTreeMap<String, i64>) {
        let fact = self
            .facts
            .iter()
            .find(|fact| fact.provider_id == provider && fact.model_id == model);
        let fenced = self.route_fenced_accounts(provider, model);
        let mut skipped = BTreeMap::new();
        let mut accounts: BTreeMap<String, AccountView> = BTreeMap::new();
        if let Some(fact) = fact {
            for row in self.eligible_rows(self.class(fact, priced_per_token).rows, fact) {
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

    /// Accounts closed by a report for this route or by an account-wide
    /// backoff observation, each with its deadline. Report fences name the
    /// credential that served; observations already name the whole account.
    fn route_fenced_accounts(&self, provider: &str, model: &str) -> BTreeMap<String, i64> {
        // A refusal probe must reach admission to claim its single-request
        // reservation. A quota route fence must not hide that same probe;
        // allocator quota blocks and observation backoffs still apply.
        let mut fenced = BTreeMap::new();
        for origin in self.rechecks.iter().filter(|origin| {
            matches!(
                origin.kind.as_str(),
                "account_route" | "non_quota_route" | "observation"
            )
        }) {
            let account = origin.account_id.clone().or_else(|| {
                origin.credential_id.as_ref().and_then(|id| {
                    crate::credential_fences::credential_account_id(&self.credentials, id)
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
        fences: &[prefrontal_routing_store::AccountRouteFence],
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
                .and_then(|row| row.account_id.clone())
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

    fn method(&self, credential: &str) -> &str {
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
            self.config.allocator_policy(provider),
        )
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
        // An empty roster may have lost accounts to route or credential
        // fences. Reuse admission's classification so only a dated account
        // limit, not missing credentials or policy exclusions, supplies a reset.
        let (roster, fenced) = self.roster_and_fences_for(provider, model, priced);
        if roster
            .roster
            .as_ref()
            .is_some_and(|roster| roster.accounts.is_empty())
        {
            if let Some(OpReply::Error { code, detail, .. }) =
                empty_roster_refusal(self, provider, model, priced, &fenced, self.now_ms, None)
            {
                if code == "account_pool_exhausted" {
                    return Some(detail.and_then(|detail| detail["reset_at_ms"].as_i64()));
                }
            }
        }
        let blocked = self.probe(provider, model, priced, false, None, "blocked-preflight");
        if !blocked.refusal().is_some_and(|refusal| {
            matches!(
                refusal.code,
                prefrontal_routing_core::accounts::RefusalCode::AccountPoolExhausted
                    | prefrontal_routing_core::accounts::RefusalCode::AccountPoolReserveLimited
            )
        }) {
            return None;
        }
        let reset = pool_recovery_reset(
            &blocked,
            roster.roster.as_ref(),
            self.fences.get(provider).map_or(&[], Vec::as_slice),
            &self.config.allocator_policy(provider),
            model,
            self.now_ms,
        );
        Some(reset.map(|reset| fenced.values().copied().fold(reset, i64::min)))
    }

    #[allow(clippy::too_many_arguments)]
    fn probe_with_policy(
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
                account_id: row.account_id.clone().unwrap_or_else(|| row.id.clone()),
                credential_id: row.id.clone(),
                affinity_model: model.into(),
                pin_generation: 1,
                expires_at_ms: i64::MAX,
                lease_active: true,
            })
        });
        prefrontal_routing_core::accounts::decide_with_context(
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
        fact: &prefrontal_routing_core::ModelFact,
        priced: bool,
    ) -> Option<prefrontal_routing_core::CredentialRefusalMark> {
        if !self
            .credentials
            .listed_credentials()
            .iter()
            .any(|row| crate::broca_credentials::credential_serves_fact(row, fact, &self.catalog))
        {
            return None;
        }
        let class = self.class(fact, priced);
        if class.rows.iter().any(|row| self.eligible(row, fact)) {
            return None;
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
        Some(prefrontal_routing_core::CredentialRefusalMark {
            reason,
            credential_ids: refused,
        })
    }

    pub(crate) fn apply_eligibility(
        &self,
        snapshot: &mut prefrontal_routing_core::ModelRouteStateSnapshot,
        facts: &[prefrontal_routing_core::ModelFact],
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
                    .entry(prefrontal_routing_core::make_capacity_key(
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
        snapshot: &mut prefrontal_routing_core::ModelRouteStateSnapshot,
        facts: &[prefrontal_routing_core::ModelFact],
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
                    .entry(prefrontal_routing_core::make_capacity_key(
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

    pub(crate) fn launch_credential(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
    ) -> Result<String, (String, String)> {
        match self
            .probe(provider, model, priced, false, None, "launch-preflight")
            .outcome
        {
            Outcome::Admit(admission) => Ok(admission.credential_id),
            _ => Err((
                "no_eligible_account".into(),
                format!("{provider}/{model}: no eligible account"),
            )),
        }
    }

    /// A legacy send has no later account admission. Claim its selected
    /// origins here, or fail closed when the runner cannot pin the credential
    /// needed for an account-local probe. Route/provider probes need no pin.
    pub(crate) fn claim_launch_credential(
        &self,
        store: &SqliteRouteStateStore,
        provider: &str,
        model: &str,
        priced: bool,
        pinnable: bool,
        owner: Option<&str>,
    ) -> Result<String, (String, String)> {
        if !self.needs_credential_pin(provider, model)
            && !self.rechecks.iter().any(|origin| {
                origin.provider == provider
                    && origin.model.as_deref().is_none_or(|named| named == model)
            })
        {
            return Ok(String::new());
        }
        if !pinnable
            && !self.rechecks.iter().any(|origin| {
                origin.provider == provider
                    && origin.model.as_deref().is_none_or(|named| named == model)
                    && (origin.account_id.is_some() || origin.credential_id.is_some())
                    && origin.principal.is_none()
                    && origin.size_class.is_none()
            })
        {
            let granted = store
                .account_transaction(|tx| {
                    let origins: Vec<_> = tx
                        .fence_rechecks()?
                        .into_iter()
                        .filter(|origin| {
                            origin.account_id.is_none()
                                && origin.credential_id.is_none()
                                && origin.principal.is_none()
                                && origin.size_class.is_none()
                                && origin.applies(provider, "", "", model)
                        })
                        .collect();
                    tx.grant_fence_rechecks(
                        &origins,
                        self.now_ms,
                        owner.or(self.probe_owner.as_deref()),
                    )
                })
                .map_err(|error| ("no_eligible_account".into(), error.to_string()))?;
            return if granted {
                Ok(String::new())
            } else {
                Err((
                    "no_eligible_account".into(),
                    "the route/provider probe was already claimed or is not due".into(),
                ))
            };
        }
        let credential = self.launch_credential(provider, model, priced)?;
        let account =
            crate::credential_fences::credential_account_id(&self.credentials, &credential)
                .unwrap_or_else(|| credential.clone());
        let claimed = store
            .account_transaction(|tx| {
                let origins: Vec<_> = tx
                    .fence_rechecks()?
                    .into_iter()
                    .filter(|origin| {
                        self.origin_applies(origin, provider, &account, &credential, model)
                            && !self.origin_recovered(origin, &account)
                    })
                    .collect();
                if !pinnable
                    && origins
                        .iter()
                        .any(|origin| origin.account_id.is_some() || origin.credential_id.is_some())
                {
                    return Ok(false);
                }
                if !origins
                    .iter()
                    .all(|origin| self.origin_due(origin, &account, model))
                {
                    return Ok(false);
                }
                tx.grant_fence_rechecks(
                    &origins,
                    self.now_ms,
                    owner.or(self.probe_owner.as_deref()),
                )
            })
            .map_err(|error| ("no_eligible_account".into(), error.to_string()))?;
        if claimed {
            Ok(credential)
        } else {
            Err(("no_eligible_account".into(),format!("{provider}/{model}: a probe is unavailable or requires a credential-pinning runner")))
        }
    }

    pub(crate) fn needs_credential_pin(&self, provider: &str, model: &str) -> bool {
        if self.rechecks.iter().any(|origin| {
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

    fn allocation_context(
        &self,
        provider: &str,
        model: &str,
    ) -> prefrontal_routing_core::accounts::AllocationContext {
        let mut context = prefrontal_routing_core::accounts::AllocationContext::default();
        for row in self.credentials.listed_credentials().iter().filter(|row| {
            self.rechecks.iter().any(|origin| {
                self.origin_applies(
                    origin,
                    provider,
                    row.account_id.as_deref().unwrap_or(&row.id),
                    &row.id,
                    model,
                ) && self.origin_due(origin, row.account_id.as_deref().unwrap_or(&row.id), model)
            })
        }) {
            context.credential_probe_notes.insert(
                row.id.clone(),
                crate::credential_fences::probe_note(provider, &row.id),
            );
        }
        let class = self
            .facts
            .iter()
            .find(|fact| fact.provider_id == provider && fact.model_id == model);
        if let Some(fact) = class {
            for row in self.credentials.listed_credentials().iter().filter(|row| {
                crate::broca_credentials::credential_serves_fact(row, fact, &self.catalog)
            }) {
                let account = row.account_id.as_deref().unwrap_or(&row.id);
                if let Some(reading) = self
                    .quota
                    .readings
                    .readings
                    .values()
                    .filter(|reading| {
                        reading.account_id == account && reading.tombstoned_at_ms.is_none()
                    })
                    .max_by_key(|reading| {
                        reading
                            .entry
                            .get("fetchedAt")
                            .and_then(Value::as_str)
                            .and_then(account_quota::parse_rfc3339_ms)
                    })
                {
                    add_spend_inputs(&mut context, account, &reading.entry);
                }
            }
        }
        context
    }

    fn restrict_size(&self, roster: &mut RosterBuild, provider: &str, model: &str, tokens: u64) {
        let class = prefrontal_routing_core::accounts::input_size_class(tokens);
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
    context: &mut prefrontal_routing_core::accounts::AllocationContext,
    account: &str,
    entry: &Value,
) {
    use prefrontal_routing_core::accounts::{CreditReading, SpendReading};
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
                    serde_json::from_value::<prefrontal_routing_core::quota::SpendBalance>(
                        value.clone(),
                    )
                    .ok()
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
            let mut context = prefrontal_routing_core::accounts::AllocationContext::default();
            add_spend_inputs(&mut context, "a", &unusable);
            assert!(context.credits.is_empty(), "{unusable}");
        }
        let mut context = prefrontal_routing_core::accounts::AllocationContext::default();
        add_spend_inputs(&mut context, "a", &entry);
        assert!(context.credits["a"].spendable);
        assert!(context.credits["a"].prepaid, "the credits pool is prepaid");
        assert_eq!(context.credits["a"].observed_at_ms, at);
        let overage = json!({"fetchedAt":rfc3339(at),"spend":[{"id":"extra_usage","remaining":{"minor":100,"exponent":0,"unit":"usd"}}]});
        let mut context = prefrontal_routing_core::accounts::AllocationContext::default();
        add_spend_inputs(&mut context, "a", &overage);
        assert!(context.credits["a"].spendable);
        assert!(
            !context.credits["a"].prepaid,
            "extra usage is metered overage"
        );
    }
}

fn store_error(error: RouteStateStoreError) -> OpReply {
    OpReply::typed(
        "account_selection_unavailable",
        format!(
            "routing could not read or commit account state: {}",
            error.0
        ),
        json!({
            "code": "account_selection_unavailable",
            "retry_after_s": null,
            "decision_id": null,
            "exclusions": [],
        }),
    )
}

// ---------------------------------------------------------------------------
// Time, ids and hashing
// ---------------------------------------------------------------------------

/// UTC RFC 3339 with whole seconds for a Unix-millisecond instant.
pub fn rfc3339(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (h, mnt, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mnt:02}:{s:02}Z")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A process-random value, so ids and the salt differ between processes that
/// start at the same instant.
fn process_entropy() -> u64 {
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u32(std::process::id());
    hasher.finish()
}

fn unique_digest(parts: &[&str]) -> String {
    #[cfg(test)]
    if let Some(inputs) = prefrontal_routing_store::replay::current() {
        let mut hasher = Sha256::new();
        hasher.update(inputs.next_id().as_bytes());
        for part in parts {
            hasher.update(part.as_bytes());
            hasher.update([0]);
        }
        return hex(&hasher.finalize());
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let mut hasher = Sha256::new();
    hasher.update(nanos.to_le_bytes());
    hasher.update(NEXT_ID.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    hasher.update(process_entropy().to_le_bytes());
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hex(&hasher.finalize())
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", &unique_digest(&[prefix])[..24])
}

fn fresh_salt() -> String {
    unique_digest(&["account-selection-salt"])
}

/// The stored form of a conversation key: salted and namespaced by principal,
/// so no row carries the caller's raw key and two principals never share one.
fn conversation_hash(salt: &str, principal: &str, conversation_key: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [salt, principal, conversation_key] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hex(&hasher.finalize())
}

/// Hash only admission identity, using a typed struct's fixed-order JSON encoding.
/// Each accepted request field is classified here:
/// - `provider`: in; selects the provider (normalized like selection).
/// - `model`: in; selects the model and its family.
/// - `affinity_model`: in; selects the affinity family, defaulting to `model`.
/// - `conversation_key`: in; selects the conversation's affinity.
/// - `caller.runner`: in as caller kind; Broca and plugins have different pin rules.
/// - `caller.harness`: out; descriptive runner metadata, not admission identity.
/// - `admission_id`: out; already the lookup key, namespaced by principal.
/// - `mode`: in; distinguishes admission, renewal and reassignment.
/// - `expected_pin`: in; identifies the pin/generation being renewed or reassigned.
/// - `initial_input_bytes`: out; advisory demand estimate may change on retry.
/// - `initial_input_estimated`: out; describes that advisory estimate.
/// - `model_decision_id`: out; provenance of model choice, not the chosen model.
/// - `candidates` (including `view_digest`, `accounts`, `account_id` and
///   `credential_id`): out; the runner's vault/roster view may change on retry.
/// - `requested_account` (`account_id`, `credential_id`): in as the effective
///   account reference; an explicit operator override changes the admission.
/// - `preferred_existing_account` (`account_id`, `credential_id`): in as the
///   effective reference; importing a local pin changes the requested affinity.
/// - `hold_wait_max_s`: out; advisory wait budget, not admission identity.
/// - `pool`: in when present; it decides which pool blocks apply. A body
///   without it hashes exactly as it did before the member existed.
/// - `service_tier`: in when present; changing the requested service tier is a
///   different admission. Omission preserves existing fingerprints.
/// Unknown fields are ignored by parsing, so quota hints, digests and timestamps
/// outside this schema cannot change identity either.
///
/// Replay returns the stored decision even if its account vanished from the new
/// candidates. The runner's credential read handles it like any revoked credential.
/// This encoding was introduced before production enabled account serving
/// (`account_selection.enabled`). With that flag off, handlers refuse admissions;
/// the former shadow mode recorded hypothetical decisions but created no admission rows.
/// Thus production has no whole-request fingerprints to migrate. This relies on
/// serving never having been enabled, not on the 60 s reservation start window:
/// expired admission rows remain stored and would still use their old hash.
fn fingerprint(params: &AccountForParams) -> String {
    #[derive(Serialize)]
    struct AdmissionIdentity<'a> {
        provider: String,
        model: String,
        affinity_model: String,
        conversation_key: &'a str,
        caller: CallerKind,
        mode: RequestMode,
        expected_pin: &'a Option<PinRef>,
        requested_account: Option<AccountRef>,
        preferred_existing_account: Option<AccountRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pool: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_conversation_key: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        service_tier: Option<cortexkit_role_routing::service_tier::ServiceTier>,
    }
    let identity = AdmissionIdentity {
        provider: normalize(&params.provider),
        model: normalize(&params.model),
        affinity_model: normalize(params.affinity_model()),
        conversation_key: &params.conversation_key,
        caller: params.caller_kind(),
        mode: params.mode,
        expected_pin: &params.expected_pin,
        requested_account: params
            .requested_account
            .as_ref()
            .and_then(AccountRefParams::to_ref),
        preferred_existing_account: params
            .preferred_existing_account
            .as_ref()
            .and_then(AccountRefParams::to_ref),
        pool: params.pool.as_deref(),
        parent_conversation_key: params.parent_conversation_key.as_deref(),
        service_tier: params.service_tier,
    };
    let encoded = serde_json::to_vec(&identity).expect("admission identity is serializable");
    hex(&Sha256::digest(&encoded))
}

fn normalize(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn wire<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn wire_str<T: Serialize>(value: &T) -> String {
    match wire(value) {
        Value::String(text) => text,
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Roster: vault identities joined with Insula readings
// ---------------------------------------------------------------------------

/// The allocator's roster for one provider plus what routing knows about its
/// gaps, for the explanation.
#[derive(Debug, Clone, PartialEq)]
pub struct RosterBuild {
    /// `None` when routing has never listed the vault (and has no persisted
    /// listing): new cold admissions are then unavailable.
    pub roster: Option<Roster>,
    /// Roster facts for the record: the listing's digest and age, whether it
    /// is past the 24-hour limit, the join gaps in both directions and the
    /// alarms. Emails are left out.
    pub context: Value,
}

fn is_default_record(credential_id: &str) -> bool {
    credential_id.split(':').count() == 2
}

fn reset_time(text: Option<&str>) -> ResetTime {
    match text {
        None => ResetTime::Missing,
        Some(text) => {
            account_quota::parse_rfc3339_ms(text).map_or(ResetTime::Unparsable, ResetTime::At)
        }
    }
}

fn quota_window(window: &RateWindow) -> Option<QuotaWindow> {
    let minutes = u32::try_from(window.window_minutes?).ok()?;
    Some(QuotaWindow {
        used_percent: window.used_percent,
        // Insula's `rawUsedPercent` is the poll's percent before the account's
        // saved (banked) resets are applied, not the unrounded utilization an
        // upstream response header carries. Only that header value may
        // confirm exhaustion, so a poll reading carries none.
        raw_utilization: None,
        resets_at: reset_time(window.resets_at.as_deref()),
        window_minutes: minutes,
        window_kind: window.window_kind.clone(),
        source: ObservationSource::Poll,
    })
}

/// One stored Insula entry as the allocator's reading. Keep every reported
/// general period and its duration; scoped windows keep their producer id and
/// length. Pooled windows are limited to the requested model's pools, while
/// `pool_windows` keeps every pool of the whole reading for block recovery.
fn quota_reading_for_model(
    entry: &Value,
    account_id: &str,
    capacity_key: Option<&str>,
    model_pools: &[prefrontal_routing_core::quota_extract::ModelPool],
) -> Option<QuotaReading> {
    let entry: AccountEntry = serde_json::from_value(entry.clone()).ok()?;
    let usage = entry.usage.as_ref();
    let reported = usage
        .map(|usage| {
            prefrontal_routing_core::quota_extract::account_rate_windows(
                usage,
                capacity_key,
                model_pools,
            )
        })
        .unwrap_or_default();
    let general_windows: Vec<_> = reported
        .iter()
        .filter_map(|(_, window)| quota_window(window))
        .collect();
    let invalid_topology = reported.len() != general_windows.len()
        || general_windows
            .iter()
            .any(|window| window.window_minutes == 0);
    let general = |minutes: u32| {
        general_windows
            .iter()
            .find(|window| window.window_minutes == minutes)
            .cloned()
    };
    let scoped = match usage.and_then(|usage| usage.extra_rate_windows.as_ref()) {
        None => ScopedList::Missing,
        Some(windows) => ScopedList::Listed(
            windows
                .iter()
                .filter(|extra| {
                    !prefrontal_routing_core::quota_extract::is_account_pool_window(extra)
                })
                .filter_map(|extra| {
                    let window = extra.window.as_ref()?;
                    if extra.id.as_deref().is_none_or(str::is_empty)
                        && claim_unattributable_scope_warning(account_id)
                    {
                        crate::warn_operator(format!(
                            "[prefrontal-routing] account {account_id}: ignored unattributable extraRateWindows window without an id; general quota windows still apply"
                        ));
                    }
                    Some(ScopedWindow {
                        scope_id: extra.id.clone().unwrap_or_default(),
                        window_minutes: window
                            .window_minutes
                            .and_then(|minutes| u32::try_from(minutes).ok())
                            .unwrap_or(0),
                        used_percent: window.used_percent,
                        resets_at: reset_time(window.resets_at.as_deref()),
                    })
                })
                .collect(),
        ),
    };
    Some(QuotaReading {
        observed_at_ms: entry
            .fetched_at
            .as_deref()
            .and_then(account_quota::parse_rfc3339_ms),
        // An error without a class is still a degraded entry.
        error_class: entry
            .error_class
            .clone()
            .or_else(|| invalid_topology.then(|| "invalid_general_window".into()))
            .or_else(|| {
                entry
                    .error
                    .as_ref()
                    .map(|_| "unclassified_error".to_string())
            }),
        stale: entry.stale.as_ref().map(|stale| StaleDisclosure {
            since_ms: stale
                .since
                .as_deref()
                .and_then(account_quota::parse_rfc3339_ms),
            class: stale.class.clone(),
        }),
        five_hour: general(GeneralWindow::FiveHour.window_minutes()),
        seven_day: general(GeneralWindow::SevenDay.window_minutes()),
        general_windows: Some(general_windows),
        scoped,
        pool_windows: Some(
            usage
                .map(prefrontal_routing_core::quota_extract::named_account_windows)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(pool, window)| {
                    Some(PoolWindowReading {
                        pool: pool.map(str::to_string),
                        window_minutes: u32::try_from(window.window_minutes?)
                            .ok()
                            .filter(|minutes| *minutes > 0)?,
                        used_percent: window.used_percent,
                    })
                })
                .collect(),
        ),
    })
}

/// Bad producer metadata can be encountered on every selection. Warn once per
/// account for the process lifetime rather than flooding the operator log.
fn claim_unattributable_scope_warning(account_id: &str) -> bool {
    static WARNED: std::sync::OnceLock<std::sync::Mutex<BTreeSet<String>>> =
        std::sync::OnceLock::new();
    WARNED
        .get_or_init(Default::default)
        .lock()
        .expect("unattributable scope warning lock")
        .insert(account_id.to_string())
}

/// Join the vault roster with Insula readings for one routing provider.
///
/// Candidates are the vault's identities: an Insula account the vault does not
/// list is a gap, never a candidate. A listed identity with no reading, or a
/// tombstoned one, is a candidate with no reading (unknown for cold work, and
/// after 15 minutes an alarm). A record whose vault state is not `active`
/// needs re-authorization. The 24-hour roster limit is the allocator's: the
/// roster is passed through with its listing time and the allocator refuses
/// new cold work once it is too old, while existing affinity continues.
#[cfg(test)]
pub fn allocator_roster(state: &AccountQuotaState, provider: &str, now_ms: i64) -> RosterBuild {
    allocator_roster_for_model(state, provider, None, now_ms, &[])
}

fn allocator_roster_for_model(
    state: &AccountQuotaState,
    provider: &str,
    model: Option<&str>,
    now_ms: i64,
    model_pools: &[prefrontal_routing_core::quota_extract::ModelPool],
) -> RosterBuild {
    // Learn adapter/vendor relationships only from account-id joins. An adapter
    // is usable for unread siblings only when all its mapped readings agree.
    let mut vendors: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    if let Some(vault) = &state.roster {
        for reading in state.readings.readings.values() {
            if let (Some(identity), Some(vendor)) = (
                account_quota::identity_for_reading(vault, reading),
                reading.routing_provider(),
            ) {
                vendors
                    .entry(&identity.refresh_adapter)
                    .or_default()
                    .insert(vendor);
            }
        }
    }
    let adapters: BTreeSet<_> = vendors
        .into_iter()
        .filter(|(_, vendors)| vendors.len() == 1 && vendors.contains(provider))
        .map(|(adapter, _)| adapter)
        .collect();
    let report = account_quota::build_report(
        &state.readings,
        state.roster.as_ref(),
        &state.roster_poll,
        now_ms,
    );
    let insula_only: Vec<Value> = report
        .gaps
        .insula_only
        .iter()
        .filter(|gap| {
            state
                .readings
                .readings
                .get(&(gap.provider.clone(), gap.account_id.clone()))
                .is_some_and(|reading| reading.routing_provider() == Some(provider))
        })
        .map(|gap| json!({ "account_id": gap.account_id, "reason": gap.reason }))
        .collect();
    let vault_only: Vec<Value> = report
        .gaps
        .vault_only
        .iter()
        .filter(|gap| adapters.contains(gap.refresh_adapter.as_str()))
        .map(|gap| {
            json!({
                "account_id": gap.account_id,
                "credential_ids": gap.credential_ids,
                "first_seen_ms": gap.first_seen_ms,
                "first_reading_overdue": gap.first_reading_overdue,
            })
        })
        .collect();
    let roster = state.roster.as_ref().map(|vault| Roster {
        view_digest: vault.view.clone(),
        fetched_at_ms: vault.fetched_at_ms,
        accounts: vault
            .identities
            .iter()
            .filter(|identity| {
                let matching: Vec<_> = state
                    .readings
                    .readings
                    .values()
                    .filter(|reading| reading.account_id == identity.account_id)
                    .collect();
                if matching.is_empty() {
                    return adapters.contains(identity.refresh_adapter.as_str());
                }
                // A vendorless, ambiguous or conflicting reading cannot borrow
                // a sibling's vendor, including through sticky affinity.
                matching.iter().all(|reading| {
                    reading.routing_provider() == Some(provider)
                        && account_quota::identity_for_reading(vault, reading) == Some(*identity)
                })
            })
            .map(|identity| {
                let reading = state
                    .readings
                    .readings
                    .values()
                    .filter(|reading| {
                        reading.account_id == identity.account_id
                            && reading.routing_provider() == Some(provider)
                    })
                    .filter(|reading| reading.tombstoned_at_ms.is_none())
                    .max_by_key(|reading| {
                        (
                            reading
                                .entry
                                .get("fetchedAt")
                                .and_then(Value::as_str)
                                .and_then(account_quota::parse_rfc3339_ms),
                            &reading.provider,
                        )
                    });
                let capacity_key = model.map(|model| format!("{provider}/{model}"));
                let usage = reading
                    .and_then(|reading| {
                        serde_json::from_value::<AccountEntry>(reading.entry.clone()).ok()
                    })
                    .and_then(|entry| entry.usage);
                let pool_route = capacity_key.as_deref().is_some_and(|key| {
                    prefrontal_routing_core::quota_extract::requires_account_pool(key)
                });
                let pooled_reading = usage
                    .as_ref()
                    .and_then(|usage| usage.extra_rate_windows.as_ref())
                    .into_iter()
                    .flatten()
                    .any(prefrontal_routing_core::quota_extract::is_account_pool_window);
                // Product/pool reach is a hard filter, not merely unknown quota:
                // sticky affinity and overrides must not bypass it either.
                let model_reach = if model.is_some() && (pool_route || pooled_reading) {
                    let reaches = usage.as_ref().is_some_and(|usage| {
                        !prefrontal_routing_core::quota_extract::account_rate_windows(
                            usage,
                            capacity_key.as_deref(),
                            model_pools,
                        )
                        .is_empty()
                    });
                    ModelReach::Only(if reaches {
                        vec![model.unwrap().to_string()]
                    } else {
                        Vec::new()
                    })
                } else {
                    ModelReach::All
                };
                AccountView {
                    account_id: identity.account_id.clone(),
                    routing_provider: provider.to_string(),
                    refresh_adapter: identity.refresh_adapter.clone(),
                    credentials: identity
                        .credential_ids
                        .iter()
                        .map(|credential_id| CredentialRecord {
                            credential_id: credential_id.clone(),
                            default_record: is_default_record(credential_id),
                            needs_reauth: !identity.active_credential_ids.contains(credential_id),
                        })
                        .collect(),
                    model_reach,
                    quota: reading.and_then(|reading| {
                        quota_reading_for_model(
                            &reading.entry,
                            &reading.account_id,
                            capacity_key.as_deref(),
                            model_pools,
                        )
                    }),
                }
            })
            .collect(),
    });
    let context = json!({
        "provider": provider,
        "tracked": roster.as_ref().is_some_and(|roster| !roster.accounts.is_empty()),
        "view_digest": state.roster.as_ref().map(|vault| vault.view.clone()),
        "fetched_at_ms": state.roster.as_ref().map(|vault| vault.fetched_at_ms),
        "past_max_age": state.roster.as_ref().is_some_and(|vault| {
            now_ms - vault.fetched_at_ms >= account_quota::ROSTER_MAX_AGE_MS
        }),
        "poll": report.roster.poll,
        "account_ids": roster
            .as_ref()
            .map(|roster| roster.accounts.iter().map(|view| view.account_id.clone()).collect::<Vec<_>>())
            .unwrap_or_default(),
        "gaps": { "insula_only": insula_only, "vault_only": vault_only },
        "alarms": report.alarms,
    });
    RosterBuild { roster, context }
}

fn roster_for(ctx: &Ctx<'_>, provider: &str) -> RosterBuild {
    roster_for_model(ctx, provider, None)
}

fn roster_for_model(ctx: &Ctx<'_>, provider: &str, model: Option<&str>) -> RosterBuild {
    if let (Some(classes), Some(model)) = (ctx.classes.as_ref(), model) {
        return classes.roster_for(provider, model, false);
    }
    match ctx.accounts.map(|state| state.lock()) {
        Some(Ok(guard)) => {
            allocator_roster_for_model(&guard, provider, model, ctx.now_ms, &ctx.policy.model_pools)
        }
        Some(Err(_)) => RosterBuild {
            roster: None,
            context: json!({ "provider": provider, "error": "account state lock poisoned" }),
        },
        None => RosterBuild {
            roster: None,
            context: json!({ "provider": provider, "error": "account state not restored yet" }),
        },
    }
}

/// Pricing uses the same roster as account_for, not the subscription policy or
/// a guessed credential id. Called by the poller, never during model selection.
pub(crate) fn billing_roster(
    state: &AccountQuotaState,
    provider: &str,
    model: &str,
    now_ms: i64,
    model_pools: &[prefrontal_routing_core::quota_extract::ModelPool],
) -> Vec<AccountView> {
    allocator_roster_for_model(state, provider, Some(model), now_ms, model_pools)
        .roster
        .map(|roster| roster.accounts)
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Stored rows as allocator inputs
// ---------------------------------------------------------------------------

fn conversation_affinity(row: &AccountAffinityRow, now_ms: i64) -> ConversationAffinity {
    ConversationAffinity {
        pin_id: row.pin_id.clone(),
        account_id: row.account_id.clone(),
        credential_id: row.credential_id.clone(),
        affinity_model: row.affinity_model.clone(),
        pin_generation: row.pin_generation,
        expires_at_ms: row.expires_at,
        lease_active: row.lease_valid_until.is_some_and(|until| until > now_ms),
    }
}

/// The confirming evidence stored with a block. Its pool key and recovery
/// set are stored in their own columns, so blocks confirmed before blocks
/// were keyed by pool decode the same way.
#[derive(Deserialize)]
struct StoredBlockEvidence {
    account_id: String,
    observed_at_ms: EpochMs,
    reset: ResetTime,
    #[serde(default)]
    retry_hint_ms: Option<EpochMs>,
    source: ConfirmationSource,
}

/// Every standing block on `provider`, with its stored row id.
fn load_blocks(tx: &AccountTx<'_>, provider: &str) -> RouteStateResult<Vec<(String, Fence)>> {
    // The pool-key columns are read only when a block stands. A provider with
    // none reads just the base columns, which every schema version has: the
    // upgrade tests serve admissions on a store opened before the pool-key
    // migration.
    if tx.active_fences(provider)?.is_empty() {
        return Ok(Vec::new());
    }
    Ok(tx
        .active_pool_fences(provider)?
        .into_iter()
        .filter_map(|row| {
            match serde_json::from_str::<StoredBlockEvidence>(&row.evidence.fence_json) {
                Ok(evidence) => Some((
                    row.evidence.fence_id,
                    Fence {
                        account_id: evidence.account_id,
                        pool: row.pool_key,
                        recovery_set: row.recovery_set,
                        observed_at_ms: evidence.observed_at_ms.min(row.evidence.created_at),
                        reset: evidence.reset,
                        retry_hint_ms: evidence.retry_hint_ms,
                        source: evidence.source,
                    },
                )),
                Err(error) => {
                    tracing::warn!(fence = %row.evidence.fence_id, error = %error, "stored fence undecodable; ignored");
                    None
                }
            }
        })
        .collect())
}

fn load_fences(tx: &AccountTx<'_>, provider: &str) -> RouteStateResult<Vec<Fence>> {
    Ok(load_blocks(tx, provider)?
        .into_iter()
        .map(|(_, fence)| fence)
        .collect())
}

fn quota_reading_binds_origin(
    origin: &prefrontal_routing_store::FenceRecheck,
    account: &str,
    fences: &[Fence],
    reading: Option<&QuotaReading>,
    now: i64,
    policy: &AllocatorPolicy,
) -> bool {
    let relevant: Vec<_> = fences
        .iter()
        .filter(|fence| {
            fence.account_id == account
                && (origin.kind != "account" || fence.observed_at_ms == origin.evidence_at)
        })
        .collect();
    let whole_account = Fence {
        account_id: account.into(),
        pool: None,
        recovery_set: Vec::new(),
        observed_at_ms: origin.evidence_at,
        reset: ResetTime::Missing,
        retry_hint_ms: None,
        source: prefrontal_routing_core::accounts::ConfirmationSource::BrocaAccountLimit,
    };
    let relevant = if relevant.is_empty() {
        vec![&whole_account]
    } else {
        relevant
    };
    relevant.iter().any(|fence| {
        prefrontal_routing_core::accounts::pools::binding_pressure(fence, reading, now, policy)
    }) || relevant
        .iter()
        .all(|fence| block_end(fence, reading, now, policy).is_some())
}

/// Bring the standing blocks on `provider` up to date with the accounts'
/// latest readings: clear blocks proved recovered by newer headroom, and add
/// newly reported pools to each whole-account recovery set. A reset or retry
/// check never deletes quota evidence. Clearing a recovered block keeps a
/// later partial reading from bringing it back.
fn refresh_blocks(
    tx: &AccountTx<'_>,
    provider: &str,
    accounts: &[AccountView],
    now: EpochMs,
    policy: &AllocatorPolicy,
) -> RouteStateResult<()> {
    for (fence_id, fence) in load_blocks(tx, provider)? {
        let reading = accounts
            .iter()
            .find(|view| view.account_id == fence.account_id)
            .and_then(|view| view.quota.as_ref());
        if let Some(end) = block_end(&fence, reading, now, policy) {
            tx.clear_fence(&fence_id, now, &wire_str(&end))?;
        } else if let Some(set) = grown_recovery_set(&fence, reading, now, policy) {
            tx.set_fence_recovery_set(&fence_id, &set)?;
        }
    }
    Ok(())
}

/// Refresh the standing blocks of every provider against the current
/// readings. The account loop calls this so that readings arriving between
/// admissions still lift blocks and grow recovery sets.
pub(crate) fn refresh_all_blocks(
    store: &SqliteRouteStateStore,
    state: &AccountQuotaState,
    config: &crate::config_overrides::LiveConfig,
    now: EpochMs,
) -> RouteStateResult<()> {
    store.account_transaction(|tx| {
        tx.release_expired_continuations(now)?;
        for provider in tx.active_fence_providers()? {
            let accounts = allocator_roster_for_model(state, &provider, None, now, &[])
                .roster
                .map(|roster| roster.accounts)
                .unwrap_or_default();
            refresh_blocks(
                tx,
                &provider,
                &accounts,
                now,
                &config.allocator_policy(&provider),
            )?;
        }
        Ok(())
    })
}

/// Reconcile both newly reported and persisted credential refusals. Once an
/// exhausted subscription reading supplies a reset for a billing refusal,
/// store an ordinary pool block so the hold survives stale readings and uses
/// normal block recovery.
/// The credential row remains as refusal evidence and also excludes paid-credit
/// placement: a billing refusal must not be bypassed by switching payment class.
pub(crate) fn refresh_credential_quota_holds(
    store: &SqliteRouteStateStore,
    credentials: &crate::broca_credentials::BrocaCredentials,
    quota: &AccountQuotaState,
    config: &crate::config_overrides::LiveConfig,
    refusals: Vec<prefrontal_routing_core::route_state_store::CredentialEntitlementFence>,
    route_fences: &[prefrontal_routing_store::AccountRouteFence],
    now: EpochMs,
) -> Result<Vec<prefrontal_routing_core::route_state_store::CredentialEntitlementFence>, String> {
    // With no refusal evidence there is nothing to reconcile; ordinary reads
    // must not reserve SQLite's writer just to iterate an empty list.
    if refusals.is_empty() {
        return Ok(refusals);
    }
    store
        .account_transaction(|tx| {
            refresh_credential_quota_holds_in(
                tx,
                credentials,
                quota,
                config,
                refusals,
                route_fences,
                now,
            )
        })
        .map_err(|error| error.to_string())
}

/// Reconcile both newly reported and persisted credential refusals. Once an
/// exhausted subscription reading supplies a reset for a billing refusal,
/// store an ordinary pool block so the hold survives stale readings and uses
/// normal block recovery.
/// The credential row remains as refusal evidence and also excludes paid-credit
/// placement: a billing refusal must not be bypassed by switching payment class.
pub(crate) fn refresh_credential_quota_holds_in(
    tx: &AccountTx<'_>,
    credentials: &crate::broca_credentials::BrocaCredentials,
    quota: &AccountQuotaState,
    config: &crate::config_overrides::LiveConfig,
    refusals: Vec<prefrontal_routing_core::route_state_store::CredentialEntitlementFence>,
    route_fences: &[prefrontal_routing_store::AccountRouteFence],
    now: EpochMs,
) -> RouteStateResult<Vec<prefrontal_routing_core::route_state_store::CredentialEntitlementFence>> {
    let credentials = &credentials.with_quota_account_ids(quota);
    let prior_clocks = tx.fence_rechecks()?;
    let mut kept = Vec::new();
    for refusal in refusals {
        if !crate::credential_fences::is_billing_code(&refusal.code) {
            // Quota utilization cannot establish entitlement. Remove quota
            // blocks left by older reconciliation, but retain the refusal so
            // a timed probe can discover a provider-side reversal.
            let prefix = crate::credential_fences::quota_hold_prefix(&refusal);
            for block in tx.active_pool_fences(&refusal.provider_id)? {
                if block.evidence.fence_id.starts_with(&prefix) {
                    tx.clear_fence(&block.evidence.fence_id, now, "entitlement_not_quota")?;
                }
            }
            kept.push(refusal);
            continue;
        }
        let Some(account) =
            crate::credential_fences::credential_account_id(credentials, &refusal.credential_id)
        else {
            kept.push(refusal);
            continue;
        };
        let prefix = crate::credential_fences::quota_hold_prefix(&refusal);
        let policy = config.allocator_policy(&refusal.provider_id);
        let prior_hold = tx
            .active_pool_fences(&refusal.provider_id)?
            .iter()
            .any(|block| block.evidence.fence_id.starts_with(&prefix));
        let headroom = crate::credential_fences::quota_refusal_contradicted(
            quota,
            credentials,
            &refusal.credential_id,
            now,
            &policy,
        );
        let reading_at = crate::credential_fences::latest_reading_for_account(quota, &account)
            .and_then(|reading| reading.entry.get("fetchedAt").and_then(Value::as_str))
            .and_then(account_quota::parse_rfc3339_ms);
        let contradicted_at_receipt =
            headroom && reading_at.is_some_and(|at| at <= refusal.observed_at_ms);
        let prior_quota = prior_hold
            || prior_clocks.iter().any(|origin| {
                origin.kind == "credential"
                    && origin.quota
                    && origin.provider == refusal.provider_id
                    && origin.credential_id.as_deref() == Some(&refusal.credential_id)
                    && origin.evidence_at == refusal.observed_at_ms
            })
            || route_fences.iter().any(|route| {
                route.provider == refusal.provider_id
                    && route.credential_id == refusal.credential_id
            });
        let quota_interpretation = !contradicted_at_receipt
            && (prior_quota
                || (crate::credential_fences::has_quota_signal_for_account(quota, &account)
                    && !headroom));
        if !tx.classify_credential_quota(
            &refusal.provider_id,
            &refusal.credential_id,
            quota_interpretation,
        )? {
            kept.push(refusal);
            continue;
        }
        if !quota_interpretation {
            kept.push(refusal);
            continue;
        }
        let reading = crate::credential_fences::latest_reading_for_account(quota, &account)
            .and_then(|reading| quota_reading_for_model(&reading.entry, &account, None, &[]));
        let reading_reset = crate::credential_fences::exhausted_quota_reset_for_account(
            quota, &account, now, &policy,
        );
        let lifted = (|| -> RouteStateResult<bool> {
            let blocks: Vec<_> = tx
                .active_pool_fences(&refusal.provider_id)?
                .into_iter()
                .filter(|block| block.evidence.fence_id.starts_with(&prefix))
                .collect();
            let prior = blocks
                .iter()
                .filter_map(|block| {
                    let mut fence =
                        serde_json::from_str::<Fence>(&block.evidence.fence_json).ok()?;
                    fence.recovery_set = block.recovery_set.clone();
                    Some(fence)
                })
                .max_by_key(|fence| match fence.reset {
                    ResetTime::At(reset) => reset,
                    _ => 0,
                });
            let newer_reading = reading
                .as_ref()
                .and_then(|reading| reading.observed_at_ms)
                .is_some_and(|fetched| fetched > refusal.observed_at_ms);
            let all_recovered = if blocks.is_empty() {
                crate::credential_fences::fresh_headroom_after_for_account(
                    quota,
                    &account,
                    refusal.observed_at_ms,
                    now,
                    &policy,
                )
            } else {
                blocks.iter().all(|block| {
                    serde_json::from_str::<Fence>(&block.evidence.fence_json).is_ok_and(
                        |mut fence| {
                            fence.pool = block.pool_key.clone();
                            fence.recovery_set = block.recovery_set.clone();
                            fence.observed_at_ms = fence.observed_at_ms.max(refusal.observed_at_ms);
                            block_end(&fence, reading.as_ref(), now, &policy).is_some()
                        },
                    )
                })
            };
            if all_recovered {
                for block in &blocks {
                    tx.clear_fence(&block.evidence.fence_id, now, "fresh_headroom")?;
                }
                return Ok(true);
            }
            let reset = reading_reset.filter(|_| newer_reading).or_else(|| {
                reading_reset
                    .into_iter()
                    .chain(prior.iter().filter_map(|fence| match fence.reset {
                        ResetTime::At(reset) if reset > refusal.observed_at_ms => Some(reset),
                        _ => None,
                    }))
                    .max()
            });
            let mut reset = reset;
            // Contemporaneous reports can name different windows. Newer
            // quota evidence supersedes an older report's guessed horizon.
            for route in route_fences.iter().filter(|route| {
                route.provider == refusal.provider_id
                    && crate::credential_fences::credential_account_id(
                        credentials,
                        &route.credential_id,
                    )
                    .as_deref()
                        == Some(account.as_str())
            }) {
                if !reading
                    .as_ref()
                    .and_then(|reading| reading.observed_at_ms)
                    .is_some_and(|fetched| fetched > route.recorded_at)
                {
                    if route.until_ms > refusal.observed_at_ms {
                        reset =
                            Some(reset.map_or(route.until_ms, |reset| reset.max(route.until_ms)));
                    }
                }
            }
            let mut fence = prior.unwrap_or_else(|| Fence {
                account_id: account.clone(),
                pool: None,
                recovery_set: recovery_set_from(reading.as_ref(), now, &policy),
                observed_at_ms: refusal.observed_at_ms,
                reset: reset.map(ResetTime::At).unwrap_or(ResetTime::Missing),
                retry_hint_ms: None,
                source: prefrontal_routing_core::accounts::ConfirmationSource::BrocaAccountLimit,
            });
            fence.reset = reset.map(ResetTime::At).unwrap_or(ResetTime::Missing);
            fence.observed_at_ms = fence.observed_at_ms.max(refusal.observed_at_ms);
            for key in blocks.iter().flat_map(|block| &block.recovery_set) {
                if !fence.recovery_set.contains(key) {
                    fence.recovery_set.push(key.clone());
                }
            }
            if let Some(end) = block_end(&fence, reading.as_ref(), now, &policy) {
                for block in &blocks {
                    tx.clear_fence(&block.evidence.fence_id, now, &wire_str(&end))?;
                }
                return Ok(true);
            }
            if !blocks.iter().any(|block| {
                serde_json::from_str::<Fence>(&block.evidence.fence_json).is_ok_and(|fence| {
                    fence.reset == reset.map(ResetTime::At).unwrap_or(ResetTime::Missing)
                        && fence.observed_at_ms == refusal.observed_at_ms
                })
            }) {
                for block in &blocks {
                    tx.clear_fence(&block.evidence.fence_id, now, "quota_evidence_updated")?;
                }
                tx.insert_pool_fence(&AccountPoolFenceRow {
                    evidence: AccountFenceRow {
                        fence_id: format!("{prefix}{}", new_id("hold")),
                        provider: refusal.provider_id.clone(),
                        account_id: account.clone(),
                        scope_json: wire(&fence.pool).to_string(),
                        fence_json: wire(&fence).to_string(),
                        source: wire_str(&fence.source),
                        observed_at: refusal.observed_at_ms,
                        principal: "routing".into(),
                        admission_id: None,
                        observation_id: None,
                        created_at: now,
                        cleared_at: None,
                        cleared_reason: None,
                    },
                    pool_key: None,
                    recovery_set: fence.recovery_set,
                })?;
            }
            Ok(false)
        })()?;
        if lifted {
            tx.clear_credential_entitlement_fence(&refusal.provider_id, &refusal.credential_id)?;
        } else {
            kept.push(refusal);
        }
    }
    Ok(kept)
}

/// The pool a request's model draws on: the one the caller names, or else
/// the pool the reading assigns that model's route, if any.
fn admission_probe_owner(principal: &str, admission: &str) -> String {
    json!(["account_admission", principal, admission]).to_string()
}

fn request_pool(named: Option<&str>, provider: &str, model: &str) -> Option<String> {
    named.map(str::to_string).or_else(|| {
        prefrontal_routing_core::quota_extract::antigravity_pool_for_capacity(&format!(
            "{}/{}",
            normalize(provider),
            normalize(model)
        ))
        .map(str::to_string)
    })
}

/// A malformed pool name is refused with the member it came from named.
fn invalid_pool(pool: Option<&str>) -> Option<OpReply> {
    let error = validate_pool_name(pool?).err()?;
    Some(OpReply::typed(
        "invalid_request",
        format!("`pool`: {error}"),
        json!({ "code": "invalid_request", "field": "pool" }),
    ))
}

/// The demand the allocator can charge to `roster`'s accounts. Each account's
/// admissions are read from its [`demand_floor`] on, so a decision never
/// loads history the allocator would ignore; admissions of accounts outside
/// the roster are never read, because the allocator only prices roster
/// accounts.
fn load_demand(
    tx: &AccountTx<'_>,
    provider: &str,
    roster: Option<&Roster>,
    policy: &AllocatorPolicy,
    context: &prefrontal_routing_core::accounts::AllocationContext,
) -> RouteStateResult<Vec<AdmissionDemand>> {
    // One read per account; a repeated account keeps its lowest floor.
    let mut floors: BTreeMap<&str, Option<EpochMs>> = BTreeMap::new();
    for view in roster.map_or(&[][..], |roster| &roster.accounts) {
        let floor = demand_floor(view, policy, context);
        floors
            .entry(view.account_id.as_str())
            .and_modify(|kept| *kept = kept.zip(floor).map(|(a, b)| a.min(b)))
            .or_insert(floor);
    }
    let mut demand = Vec::new();
    for (account_id, floor) in floors {
        demand.extend(
            tx.demand(provider, account_id, floor)?
                .into_iter()
                .filter_map(|row| {
                    let state: AdmissionState = serde_json::from_value(json!(row.state)).ok()?;
                    Some(AdmissionDemand {
                        account_id: row.account_id,
                        state,
                        committed_at_ms: row.committed_at,
                        request_bytes: row.request_bytes,
                    })
                }),
        );
    }
    Ok(demand)
}

/// The commit time an admission charged to `view` must be after to still
/// count in the allocator, or `None` when one of any age counts.
///
/// The allocator charges an account's non-released admissions committed
/// after its quota reading less `reflection_lag_ms`: an older one is already
/// reflected in the reading. With no reading (or no reading time) it charges
/// every one. It also charges every one to an account it places on paid
/// credits, because it then sets the reading aside. Credit placement needs
/// `paid_credits` and a spendable credit reading for the account; when both
/// hold, the whole history is kept, without repeating the allocator's further
/// conditions for credit mode. The round-robin and hybrid strategies order
/// accounts by how many admissions each holds, whatever their age, so they
/// need the whole history too.
fn demand_floor(
    view: &AccountView,
    policy: &AllocatorPolicy,
    context: &prefrontal_routing_core::accounts::AllocationContext,
) -> Option<EpochMs> {
    let may_use_credits = policy.paid_credits
        && context
            .credits
            .get(&view.account_id)
            .is_some_and(|credit| credit.spendable);
    if may_use_credits
        || matches!(
            policy.strategy,
            AccountStrategy::RoundRobin | AccountStrategy::Hybrid
        )
    {
        return None;
    }
    let observed_at = view.quota.as_ref()?.observed_at_ms?;
    Some(observed_at.saturating_sub(policy.reflection_lag_ms))
}

fn load_exclusions(
    tx: &AccountTx<'_>,
    principal: &str,
    admission_id: &str,
) -> RouteStateResult<Vec<PrincipalExclusion>> {
    Ok(tx
        .admission_exclusions(principal, admission_id)?
        .into_iter()
        .map(|row| PrincipalExclusion {
            principal: row.principal,
            credential_id: row.credential_id,
            runner_view_digest: row.runner_view_digest,
            roster_view_digest: row.roster_view_digest,
            recorded_at_ms: row.recorded_at,
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Decision records
// ---------------------------------------------------------------------------

fn family_name(family: ModelFamily) -> String {
    wire_str(&family)
}

fn refusal_code(code: RefusalCode) -> &'static str {
    match code {
        RefusalCode::AccountSelectionUnavailable => "account_selection_unavailable",
        RefusalCode::AccountSelectionUnsupported => "account_selection_unsupported",
        RefusalCode::AccountNotAuthorized => "account_not_authorized",
        RefusalCode::AccountReauthRequired => "auth_required",
        RefusalCode::AccountQuotaUnknown => "account_quota_unknown",
        RefusalCode::AccountPoolReserveLimited => "account_pool_reserve_limited",
        RefusalCode::AccountPoolExhausted => "account_pool_exhausted",
    }
}

/// `(outcome, code, account_id, credential_id)` for the decision log.
fn outcome_columns(decision: &Decision) -> (&'static str, String, Option<String>, Option<String>) {
    match &decision.outcome {
        Outcome::Admit(admission) => (
            "admit",
            wire_str(&admission.reason),
            Some(admission.account_id.clone()),
            Some(admission.credential_id.clone()),
        ),
        Outcome::Hold(hold) => (
            "hold",
            "account_hold".to_string(),
            Some(hold.account_id.clone()),
            Some(hold.credential_id.clone()),
        ),
        Outcome::Refuse(refusal) => ("refuse", refusal_code(refusal.code).to_string(), None, None),
    }
}

struct RecordInput<'a> {
    decision_id: &'a str,
    mode: &'a str,
    trigger: &'a str,
    principal: &'a str,
    admission_id: Option<&'a str>,
    request_mode: Option<&'a str>,
    decision: &'a Decision,
    context: Value,
    now_ms: i64,
}

fn decision_record(input: RecordInput<'_>) -> AccountDecisionRecord {
    let (outcome, code, account_id, credential_id) = outcome_columns(input.decision);
    let explanation = &input.decision.explanation;
    AccountDecisionRecord {
        decision_id: input.decision_id.to_string(),
        mode: input.mode.to_string(),
        trigger: input.trigger.to_string(),
        principal: input.principal.to_string(),
        admission_id: input.admission_id.map(str::to_owned),
        request_mode: input.request_mode.map(str::to_owned),
        provider: explanation.provider.clone(),
        model: explanation.model.clone(),
        family: family_name(explanation.family),
        outcome: outcome.to_string(),
        code,
        account_id,
        credential_id,
        policy_version: explanation.policy_version,
        decision_json: wire(input.decision).to_string(),
        context_json: Some(input.context.to_string()),
        created_at: input.now_ms,
    }
}

/// Per-account exclusions as a refusal reports them: one entry per account
/// and reason, `{"account_id", "reason", ...}`, with a reserve exclusion split
/// into one entry per window it names (`"window": "seven_day"`).
fn wire_exclusions(decision: &Decision) -> Vec<Value> {
    let mut out = Vec::new();
    for note in &decision.explanation.accounts {
        for reason in &note.exclusions {
            let Value::Object(mut fields) = wire(reason) else {
                continue;
            };
            if let Some(Value::Array(windows)) = fields.remove("windows") {
                for window in windows {
                    let mut entry = fields.clone();
                    entry.insert("account_id".into(), json!(note.account_id));
                    entry.insert("window".into(), window);
                    out.push(Value::Object(entry));
                }
                continue;
            }
            fields.insert("account_id".into(), json!(note.account_id));
            out.push(Value::Object(fields));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// route.account_for
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum RequestMode {
    #[default]
    Admit,
    Renew,
    Reassign,
}

impl RequestMode {
    fn as_str(self) -> &'static str {
        match self {
            RequestMode::Admit => "admit",
            RequestMode::Renew => "renew",
            RequestMode::Reassign => "reassign",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct CallerParams {
    runner: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct PinRef {
    id: String,
    generation: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct CandidateParams {
    account_id: String,
    credential_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct CandidatesParams {
    view_digest: String,
    #[serde(default)]
    accounts: Vec<CandidateParams>,
}

#[derive(Debug, Clone, Deserialize)]
struct AccountRefParams {
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    credential_id: Option<String>,
}

impl AccountRefParams {
    fn to_ref(&self) -> Option<AccountRef> {
        match (&self.account_id, &self.credential_id) {
            (Some(account_id), _) => Some(AccountRef::AccountId(account_id.clone())),
            (None, Some(credential_id)) => Some(AccountRef::CredentialId(credential_id.clone())),
            (None, None) => None,
        }
    }
}

/// The parameters `route.account_for` accepts. Field names are the wire
/// names, snake_case like the rest of the account-selection schema.
#[derive(Debug, Clone, Deserialize)]
struct AccountForParams {
    #[serde(default)]
    service_tier: Option<cortexkit_role_routing::service_tier::ServiceTier>,
    #[serde(skip)]
    service_tier_chosen: Option<cortexkit_role_routing::service_tier::ServiceTier>,
    #[serde(skip)]
    service_tier_bound: bool,
    #[serde(default)]
    parent_conversation_key: Option<String>,
    #[serde(default)]
    hold_ok: Option<bool>,
    #[serde(default)]
    request_input_tokens: Option<u64>,
    provider: String,
    model: String,
    #[serde(default)]
    affinity_model: Option<String>,
    conversation_key: String,
    caller: CallerParams,
    admission_id: String,
    #[serde(default)]
    mode: RequestMode,
    #[serde(default)]
    expected_pin: Option<PinRef>,
    #[serde(default)]
    initial_input_bytes: u64,
    #[serde(default)]
    initial_input_estimated: bool,
    /// Identifies the selection whose stored payment class constrains this
    /// admission, regardless of later edits to the consumer's pay_per_token flag.
    #[serde(default)]
    model_decision_id: Option<String>,
    /// Broca's caller-bearing requests name the same binding `decision_id`;
    /// it fills `model_decision_id` when that is absent. Not a serde alias:
    /// the v1 wrapper sends `model_decision_id` beside a continuation's own
    /// `decision_id`, and an alias would refuse that body as a duplicate.
    #[serde(default)]
    decision_id: Option<String>,
    #[serde(default)]
    candidates: Option<CandidatesParams>,
    #[serde(default)]
    requested_account: Option<AccountRefParams>,
    /// A plugin's one-time import of its existing local pin.
    #[serde(default)]
    preferred_existing_account: Option<AccountRefParams>,
    #[serde(default)]
    hold_wait_max_s: Option<u64>,
    /// The quota pool the requested model draws on, as the runner names it;
    /// absent means the account is one pool.
    #[serde(default)]
    pool: Option<String>,
}

impl AccountForParams {
    fn validate(&self) -> Result<(), OpReply> {
        if self.admission_id.trim().is_empty() {
            return Err(because(
                "admission_id_required",
                invalid("`admission_id` is required and must not be empty"),
            ));
        }
        if self.conversation_key.trim().is_empty() {
            return Err(because(
                "conversation_key_required",
                invalid("`conversation_key` must not be empty"),
            ));
        }
        if self.provider.trim().is_empty() || self.model.trim().is_empty() {
            return Err(because(
                "provider_model_required",
                invalid("`provider` and `model` must not be empty"),
            ));
        }
        if let Some(reply) = invalid_pool(self.pool.as_deref()) {
            return Err(because("pool_name", reply));
        }
        if self.mode != RequestMode::Admit && self.expected_pin.is_none() {
            checking("expected_pin_required");
            return Err(invalid(format!(
                "mode `{}` requires `expected_pin` as {{id, generation}}",
                self.mode.as_str()
            )));
        }
        Ok(())
    }

    fn caller_kind(&self) -> CallerKind {
        if self
            .caller
            .runner
            .eq_ignore_ascii_case(crate::legacy_wire::BROCA)
        {
            CallerKind::Broca
        } else {
            CallerKind::Plugin
        }
    }

    fn affinity_model(&self) -> &str {
        self.affinity_model.as_deref().unwrap_or(&self.model)
    }

    /// The allocator's request. `import_allowed` is true only when routing
    /// holds no affinity for this conversation, the one time an import hint
    /// may be considered.
    fn request(&self, principal: &str, import_allowed: bool) -> AccountRequest {
        AccountRequest {
            principal: principal.to_string(),
            provider: self.provider.clone(),
            model: self.model.clone(),
            affinity_model: self.affinity_model().to_string(),
            conversation_key: self.conversation_key.clone(),
            // Broca falls back to `broca:<run_id>` when a direct caller sent no
            // conversation key; such a run has no affinity across sends.
            conversation_key_source: if self
                .conversation_key
                .starts_with(crate::legacy_wire::BROCA_RUN_KEY_PREFIX)
            {
                ConversationKeySource::BrocaRunFallback
            } else {
                ConversationKeySource::Caller
            },
            caller: self.caller_kind(),
            // A model other than the affinity model was reached through
            // internal safety recovery, which is not direct work.
            via_safety_recovery: normalize(&self.model) != normalize(self.affinity_model()),
            initial_input_bytes: self.initial_input_bytes,
            initial_input_estimated: self.initial_input_estimated,
            candidates: self.candidates.as_ref().map(|candidates| RunnerCandidates {
                view_digest: candidates.view_digest.clone(),
                accounts: candidates
                    .accounts
                    .iter()
                    .map(|candidate| CandidateRef {
                        account_id: candidate.account_id.clone(),
                        credential_id: candidate.credential_id.clone(),
                    })
                    .collect(),
            }),
            requested_account: self
                .requested_account
                .as_ref()
                .and_then(AccountRefParams::to_ref),
            preferred_existing_account: if import_allowed {
                self.preferred_existing_account
                    .as_ref()
                    .and_then(AccountRefParams::to_ref)
            } else {
                None
            },
            hold_wait_max_s: self.hold_wait_max_s.unwrap_or(DEFAULT_HOLD_WAIT_S),
            pool: request_pool(self.pool.as_deref(), &self.provider, &self.model),
        }
    }
}

fn decode_params(body: &[u8]) -> Result<Value, OpReply> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|error| invalid(format!("request body is not JSON: {error}")))?;
    Ok(value.get("params").cloned().unwrap_or(Value::Null))
}

fn parse_account_for(params: &Value) -> Result<AccountForParams, OpReply> {
    let mut parsed: AccountForParams = serde_json::from_value(params.clone())
        .map_err(|error| invalid(format!("invalid route.account_for params: {error}")))?;
    if parsed.model_decision_id.is_none() {
        parsed.model_decision_id = parsed.decision_id.take();
    }
    parsed.validate()?;
    Ok(parsed)
}

/// Handle one account-selection operation. `body` is the whole request
/// (`{"method", "params"}`); `principal` is the route's attested principal.
pub fn handle(method: &str, body: &[u8], principal: &str, ctx: &Ctx<'_>) -> OpReply {
    checking("account_selection_enabled");
    // This query bypasses the serving transactions and answers even while
    // serving is off. A warming check never reserves or charges quota.
    if method == ACCOUNT_STATUS_OP {
        return match decode_params(body) {
            Ok(params) => account_status(&params, ctx),
            Err(reply) => reply,
        };
    }
    if !ctx.flags.enabled {
        return OpReply::typed(
            DISABLED_CODE,
            format!("{method} is not served: account selection is disabled in routing policy"),
            json!({ "code": DISABLED_CODE }),
        );
    }
    checking("request_decode");
    let params = match decode_params(body) {
        Ok(params) => params,
        Err(reply) => return reply,
    };
    match method {
        ACCOUNT_FOR_OP => account_for(&params, principal, ctx),
        ACCOUNT_OBSERVE_OP => observe(&params, principal, ctx),
        ACCOUNT_RELEASE_OP => release(&params, principal, ctx),
        other => OpReply::error("unknown_method", format!("unknown method '{other}'")),
    }
}

// ---------------------------------------------------------------------------
// route.account_status
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct AccountStatusParams {
    provider: String,
    account_id: String,
}

/// A closed vocabulary; only reserve refusals carry a window identity.
#[derive(Serialize)]
#[serde(tag = "code", rename_all = "snake_case")]
enum WarmReason {
    Headroom,
    WithinReserve { window: String },
    ReadingStale,
    ReadingMissing,
    AccountUnknown,
    AccountSelectionDisabled,
}

#[derive(Serialize)]
struct StatusWindow {
    kind: String,
    used_percent: f64,
    resets_at: Option<String>,
    window_minutes: Option<i64>,
    #[serde(skip)]
    reserve_window: ApplicableWindow,
}

fn status_window(
    window: &RateWindow,
    reported_kind: Option<&str>,
    reserve_window: Option<ApplicableWindow>,
) -> StatusWindow {
    let general = if window.window_minutes
        == Some(i64::from(GeneralWindow::FiveHour.window_minutes()))
    {
        Some(("five_hour", ApplicableWindow::FiveHour))
    } else if window.window_minutes == Some(i64::from(GeneralWindow::SevenDay.window_minutes())) {
        Some(("weekly", ApplicableWindow::SevenDay))
    } else {
        None
    };
    StatusWindow {
        kind: window
            .window_kind
            .as_deref()
            .or(reported_kind)
            .or(general.map(|(kind, _)| kind))
            .unwrap_or("reported")
            .to_string(),
        used_percent: window.used_percent,
        resets_at: window.resets_at.clone(),
        window_minutes: window.window_minutes,
        reserve_window: reserve_window
            .unwrap_or_else(|| general.map_or(ApplicableWindow::SevenDay, |(_, window)| window)),
    }
}

pub(crate) fn account_status(params: &Value, ctx: &Ctx<'_>) -> OpReply {
    let parsed: AccountStatusParams = match serde_json::from_value(params.clone()) {
        Ok(parsed) => parsed,
        Err(error) => return invalid(format!("invalid route.account_status params: {error}")),
    };
    if parsed.provider.trim().is_empty() || parsed.account_id.trim().is_empty() {
        return invalid("`provider` and `account_id` must not be empty");
    }
    let provider = normalize(&parsed.provider);
    let mut observed_at = None;
    let mut band = None;
    let mut windows = Vec::<StatusWindow>::new();
    let reason = (|| {
        if !ctx.flags.enabled {
            return WarmReason::AccountSelectionDisabled;
        }
        let Some(Ok(state)) = ctx.accounts.map(|accounts| accounts.lock()) else {
            return WarmReason::ReadingMissing;
        };
        // Like account placement, identity comes from the vault, not an
        // Insula-only reading or an email alias.
        let built = allocator_roster_for_model(&state, &provider, None, ctx.now_ms, &[]);
        let known = built.roster.as_ref().is_some_and(|roster| {
            roster
                .accounts
                .iter()
                .any(|account| account.account_id == parsed.account_id)
        });
        if !known {
            return WarmReason::AccountUnknown;
        }
        // Readings are keyed by producer (for example `codex`), while the
        // requested routing provider is the entry's apiProvider (`openai`).
        let Some(stored) = state
            .readings
            .readings
            .values()
            .filter(|reading| {
                reading.account_id == parsed.account_id
                    && reading.routing_provider() == Some(provider.as_str())
            })
            .filter(|reading| reading.tombstoned_at_ms.is_none())
            .max_by_key(|reading| {
                (
                    reading
                        .entry
                        .get("fetchedAt")
                        .and_then(Value::as_str)
                        .and_then(account_quota::parse_rfc3339_ms),
                    &reading.provider,
                )
            })
        else {
            return WarmReason::ReadingMissing;
        };
        let Ok(entry) = serde_json::from_value::<AccountEntry>(stored.entry.clone()) else {
            return WarmReason::ReadingMissing;
        };
        observed_at = entry.fetched_at.clone();
        let Some(reading) = quota_reading_for_model(&stored.entry, &stored.account_id, None, &[])
        else {
            return WarmReason::ReadingMissing;
        };
        let freshness = assess_reading(Some(&reading), ctx.now_ms, ctx.policy);
        band = freshness.age_band;
        if let Some(usage) = &entry.usage {
            // Use the allocator's topology: pooled readings ignore misleading
            // aggregate slots and infer pool durations from their producer ids.
            let reported =
                prefrontal_routing_core::quota_extract::account_rate_windows(usage, None, &[]);
            for (_, window) in reported.iter().filter(|(slot, _)| *slot != "extra") {
                windows.push(status_window(window, None, None));
            }
            let mut pools = reported
                .iter()
                .filter(|(slot, _)| *slot == "extra")
                .map(|(_, window)| window);
            for extra in usage.extra_rate_windows.iter().flatten() {
                let Some(window) = &extra.window else {
                    return WarmReason::ReadingMissing;
                };
                let pool = prefrontal_routing_core::quota_extract::is_account_pool_window(extra);
                let window = if pool {
                    pools
                        .next()
                        .expect("reported pool windows retain producer order")
                } else {
                    window
                };
                let reserve_window = if pool {
                    None
                } else if ctx.policy.scope_ids.iter().any(|scope| {
                    extra.id.as_deref() == Some(scope.scope_id.as_str())
                        && window.window_minutes == Some(i64::from(scope.window_minutes))
                }) {
                    Some(ApplicableWindow::FamilyScope)
                } else {
                    // A producer-reported pool outside routing's scope mapping
                    // gets the same conservative fallback as a general unknown.
                    Some(ApplicableWindow::SevenDay)
                };
                windows.push(status_window(
                    window,
                    extra.window_kind.as_deref().or(extra.id.as_deref()),
                    reserve_window,
                ));
            }
        }
        // The allocator marks malformed general topology as degraded. For this
        // read, it is a missing/invalid window rather than a stale observation.
        if entry.error_class.is_none()
            && entry.error.is_none()
            && reading.error_class.as_deref() == Some("invalid_general_window")
        {
            return WarmReason::ReadingMissing;
        }
        if !freshness.unknown.is_empty() {
            return WarmReason::ReadingStale;
        }
        if entry.stale.is_some()
            && windows.iter().any(|window| {
                window
                    .resets_at
                    .as_deref()
                    .and_then(account_quota::parse_rfc3339_ms)
                    .is_some_and(|reset| reset <= ctx.now_ms)
            })
        {
            return WarmReason::ReadingStale;
        }
        // Missing or malformed windows cannot be treated as unused quota.
        if windows.is_empty()
            || reading.required_windows().is_empty()
            || windows.iter().any(|window| {
                !window.used_percent.is_finite() || !(0.0..=100.0).contains(&window.used_percent)
            })
        {
            return WarmReason::ReadingMissing;
        }
        for window in &windows {
            if 100.0 - window.used_percent <= reserve_for(window.reserve_window, ctx.policy) {
                return WarmReason::WithinReserve {
                    window: window.kind.clone(),
                };
            }
        }
        WarmReason::Headroom
    })();
    OpReply::Result(json!({
        "provider": provider,
        "account_id": parsed.account_id,
        "observed_at": observed_at,
        "age_band": band,
        "windows": windows,
        "warm_ok": matches!(reason, WarmReason::Headroom),
        "reason": reason,
    }))
}

mod continuation;
pub(crate) use continuation::continue_account;

fn account_for(params: &Value, principal: &str, ctx: &Ctx<'_>) -> OpReply {
    checking("account_for_parse");
    let mut parsed = match parse_account_for(params) {
        Ok(parsed) => parsed,
        Err(reply) => return reply,
    };
    let provider = normalize(&parsed.provider);
    let mut classes = ctx.classes.clone();
    if let Some(classes) = &mut classes {
        classes.probe_owner = parsed.model_decision_id.clone();
    }
    let ctx = &Ctx {
        store: ctx.store,
        accounts: ctx.accounts,
        flags: ctx.flags.clone(),
        policy: ctx.policy,
        now_ms: ctx.now_ms,
        classes,
    };
    let request_fingerprint = fingerprint(&parsed);
    checking("admission_replay");
    match ctx.store.account_transaction(|tx| {
        tx.admission(principal, &parsed.admission_id)?
            .map(|row| replay(tx, principal, row, &request_fingerprint, ctx.now_ms))
            .transpose()
    }) {
        Ok(Some(reply)) => return reply,
        Ok(None) => {}
        Err(error) => return store_error(error),
    }
    checking("roster_prepare");
    let bound_tier = match parsed.model_decision_id.as_deref() {
        Some(id) => match ctx.store.decision_service_tier(id) {
            Ok(tier) => tier.map(|(tier, _)| tier),
            Err(error) => return store_error(error),
        },
        None => None,
    };
    let tier = bound_tier
        .as_deref()
        .or_else(|| parsed.service_tier.map(|tier| tier.as_str()));
    let unobserved = tier.filter(|tier| *tier != "standard").and_then(|_| {
        ctx.classes.as_ref().and_then(|classes| {
            crate::service_tiers::unobserved_account_input(&classes.credentials).or_else(|| {
                crate::routing_v1::runners::tier_view_unknown(
                    &classes.credentials,
                    &parsed.caller.runner,
                    ctx.now_ms,
                )
                .then_some("routing just started and hasn't read the runner's service tiers yet")
            })
        })
    });
    if let Some(reason) = unobserved {
        return because(
            "service_tier_input_unobserved",
            OpReply::typed(
                "account_selection_unavailable",
                "Routing has just started and hasn't yet loaded what it needs to judge this service tier. Retry in 3 seconds; a bound decision keeps its tier.",
                service_tier_refusal_detail(&parsed, reason, Some(3_000)),
            ),
        );
    }
    let (mut roster, route_fenced, class) = match prepare_roster(&parsed, ctx) {
        Ok(prepared) => prepared,
        Err(reply) => return reply,
    };
    checking("service_tier");
    parsed.service_tier_chosen = match prepare_service_tier(&mut parsed, ctx, &mut roster, class) {
        Ok(tier) => tier,
        Err(reply) => return reply,
    };
    if let Some(reply) = route_fence_refusal(&roster, &route_fenced, &parsed, ctx.now_ms) {
        return because("route_fence", reply);
    }
    if !roster.roster.as_ref().is_some_and(|roster| {
        roster.accounts.iter().any(|account| {
            parsed.candidates.as_ref().is_none_or(|candidates| {
                candidates.accounts.iter().any(|candidate| {
                    candidate.account_id == account.account_id
                        && account
                            .credentials
                            .iter()
                            .any(|credential| credential.credential_id == candidate.credential_id)
                })
            })
        })
    }) {
        if let Some(classes) = ctx.classes.as_ref() {
            if let Some(reply) = empty_roster_refusal(
                classes,
                &provider,
                &normalize(&parsed.model),
                class == prefrontal_routing_core::accounts::AllowedAccountClass::PricedPerTokenOnly,
                &route_fenced,
                ctx.now_ms,
                Some(&parsed),
            ) {
                return reply;
            }
            if roster
                .roster
                .as_ref()
                .is_none_or(|roster| roster.accounts.is_empty())
            {
                let accounts: BTreeSet<_> = classes.facts.iter()
                    .filter(|fact| fact.provider_id == provider && fact.model_id == normalize(&parsed.model))
                    .flat_map(|fact| classes.class(fact, class == prefrontal_routing_core::accounts::AllowedAccountClass::PricedPerTokenOnly).rows)
                    .map(|row| row.account_id.clone().unwrap_or_else(|| row.id.clone()))
                    .collect();
                return because(
                    "candidates_filter",
                    // An empty candidate list needs a permanent refusal, not
                    // a timed retry or reauthentication. The pinned routing
                    // contract does not yet declare account_selection_unsupported;
                    // retain the existing code until that contract addition lands.
                    OpReply::typed(
                        "account_selection_unavailable",
                        "The runner's candidate list leaves out every account that can serve this model, so routing admitted none. Each exclusion names an account the runner didn't offer. Offer those accounts as candidates, or send no candidate list to let routing choose.",
                        json!({"code":"account_selection_unavailable", "retry_after_s":null, "decision_id":null,
                        "exclusions":accounts.into_iter().map(|account| json!({"account_id":account,"reason":"not_in_runner_candidates"})).collect::<Vec<_>>()}),
                    ),
                );
            }
        } else if roster
            .roster
            .as_ref()
            .is_none_or(|roster| roster.accounts.is_empty())
        {
            // Without class context the vault/quota roster names nothing usable
            // for this provider/model: no filtering by eligibility, size or
            // candidates happens here, so an empty roster means no credential
            // of the right kind serves it. That is `auth_required`, untimed:
            // the runner needs a login it does not have.
            checking("missing_model_credentials");
            return OpReply::typed(
                "auth_required",
                format!("routing selects no accounts for provider `{provider}`"),
                json!({
                    "code": "auth_required",
                    "retry_after_s": null,
                    "decision_id": null,
                    "exclusions": [],
                }),
            );
        }
    }
    // The roster is read before the transaction: no lock or fetch happens
    // while the selection transaction is open.
    ctx.store
        .account_transaction(|tx| {
            tx.release_expired_continuations(ctx.now_ms)?;
            serve(
                tx,
                ctx,
                principal,
                &parsed,
                &provider,
                &roster,
                &request_fingerprint,
            )
        })
        .unwrap_or_else(store_error)
}

fn prepare_roster(
    parsed: &AccountForParams,
    ctx: &Ctx<'_>,
) -> Result<
    (
        RosterBuild,
        BTreeMap<String, i64>,
        prefrontal_routing_core::accounts::AllowedAccountClass,
    ),
    OpReply,
> {
    let provider = normalize(&parsed.provider);
    let class = match parsed.model_decision_id.as_deref() {
        None => prefrontal_routing_core::accounts::AllowedAccountClass::NotPayPerToken,
        Some(id) => match ctx.store.v1_decision(id) {
            Ok(Some(decision)) => decision.allowed_account_class,
            Ok(None) => match ctx.store.decision_account_class(id) {
                Ok(Some(class)) => class,
                Ok(None) => {
                    checking("unknown_decision");
                    return Err(OpReply::error(
                        "unknown_decision",
                        "selection decision does not exist",
                    ));
                }
                Err(error) => return Err(store_error(error)),
            },
            Err(error) => return Err(store_error(error)),
        },
    };
    let mut route_fenced = BTreeMap::new();
    let mut roster = match ctx.classes.as_ref() {
        Some(classes) => {
            let (roster, fenced) = classes.roster_and_fences_for(
                &provider,
                &normalize(&parsed.model),
                class == prefrontal_routing_core::accounts::AllowedAccountClass::PricedPerTokenOnly,
            );
            route_fenced = fenced;
            roster
        }
        None => roster_for_model(ctx, &provider, Some(&normalize(&parsed.model))),
    };
    if let Some(id) = parsed.model_decision_id.as_deref() {
        match ctx.store.decision_service_tier(id) {
            Ok(Some((tier, allowed))) => {
                // A decision made by the older select (every core mason launch)
                // has no v1 row naming its runner. The runner asking is then the
                // one that will serve the run, so its tier support is checked.
                let runner = match ctx.store.v1_decision(id) {
                    Ok(decision) => Some(decision.map_or_else(
                        || normalize(&parsed.caller.runner),
                        |decision| decision.runner_module,
                    )),
                    Err(error) => return Err(store_error(error)),
                };
                let registry = ctx.classes.as_ref().map(|classes| {
                    crate::routing_v1::runners::Registry::build(
                        classes.config.v1_runners(),
                        &classes.credentials,
                        &classes.facts,
                        classes.now_ms.max(0) as u64,
                    )
                });
                if let Some(roster) = &mut roster.roster {
                    for account in &mut roster.accounts {
                        // The standard tier needs no plan, model or runner
                        // support, so only the decision's allowed accounts apply.
                        account.credentials.retain(|credential| {
                            allowed.contains(&credential.credential_id)
                                && (tier == "standard"
                                    || ctx.classes.as_ref().is_some_and(|classes| {
                                        let fact = classes.facts.iter().find(|fact| {
                                            fact.provider_id == provider
                                                && fact.model_id == normalize(&parsed.model)
                                        });
                                        let row = classes
                                            .credentials
                                            .listed_credentials()
                                            .iter()
                                            .find(|row| row.id == credential.credential_id);
                                        match (fact, row, runner.as_deref()) {
                                        (Some(fact), Some(row), Some(runner)) => {
                                            crate::service_tiers::model_offers(
                                                &classes.credentials,
                                                fact,
                                                &tier,
                                            ) && crate::service_tiers::plan_rate(
                                                &classes.credentials.plan_prices,
                                                row,
                                                &provider,
                                                &tier,
                                                false,
                                            )
                                            .is_some()
                                                && (tier == "standard"
                                                    || registry
                                                        .as_ref()
                                                        .and_then(|registry| registry.get(runner))
                                                        .is_some_and(|runner| {
                                                            crate::service_tiers::runner_supports(
                                                                &classes.credentials,
                                                                runner,
                                                                fact,
                                                                row,
                                                                &tier,
                                                            )
                                                        }))
                                        }
                                        _ => false,
                                    }
                                    }))
                        });
                    }
                    roster
                        .accounts
                        .retain(|account| !account.credentials.is_empty());
                }
            }
            Ok(None) => {}
            Err(error) => return Err(store_error(error)),
        }
    }
    if let Some(classes) = &ctx.classes {
        classes.restrict_size(
            &mut roster,
            &provider,
            &normalize(&parsed.model),
            parsed
                .request_input_tokens
                .unwrap_or_else(|| parsed.initial_input_bytes.div_ceil(4)),
        );
    }
    Ok((roster, route_fenced, class))
}

fn tier_candidate_matches(
    parsed: &AccountForParams,
    account: &AccountView,
    credential: &CredentialRecord,
) -> bool {
    parsed.candidates.as_ref().is_none_or(|candidates| {
        candidates.accounts.iter().any(|candidate| {
            candidate.account_id == account.account_id
                && candidate.credential_id == credential.credential_id
        })
    }) && parsed.requested_account.as_ref().is_none_or(|pin| {
        pin.account_id.as_ref().map_or_else(
            || {
                pin.credential_id
                    .as_ref()
                    .is_none_or(|id| *id == credential.credential_id)
            },
            |id| *id == account.account_id,
        )
    })
}

fn service_tier_refusal_detail(
    params: &AccountForParams,
    reason: &str,
    retry_after_ms: Option<u64>,
) -> Value {
    let exclusions: Vec<_> = params
        .candidates
        .iter()
        .flat_map(|candidates| &candidates.accounts)
        .map(|candidate| json!({"account_id": candidate.account_id, "reason": reason}))
        .collect();
    let mut detail = json!({
        "code": "account_selection_unavailable",
        "retry_after_s": retry_after_ms.map(|ms| ms.div_ceil(1_000)),
        "decision_id": params.model_decision_id,
        "exclusions": exclusions,
    });
    if let Some(wait) = retry_after_ms {
        detail["retry_after_ms"] = json!(wait);
    }
    detail
}

fn bound_service_tier_refusal(params: &AccountForParams) -> OpReply {
    checking("bound_service_tier");
    OpReply::typed(
        "account_selection_unavailable",
        "No account can serve this routing decision's service tier any more, and a bound decision never steps down to a lower tier. Ask routing for a new decision.",
        service_tier_refusal_detail(params, "bound_service_tier_unavailable", None),
    )
}

fn prepare_service_tier(
    parsed: &mut AccountForParams,
    ctx: &Ctx<'_>,
    roster: &mut RosterBuild,
    class: prefrontal_routing_core::accounts::AllowedAccountClass,
) -> Result<Option<cortexkit_role_routing::service_tier::ServiceTier>, OpReply> {
    use cortexkit_role_routing::service_tier::{fallback, ServiceTier};
    let bound = parsed
        .model_decision_id
        .as_deref()
        .map(|id| ctx.store.decision_service_tier(id))
        .transpose()
        .map_err(store_error)?
        .flatten();
    // prepare_roster already revalidated the decision's tier and allowed
    // credentials. Do not step down or re-rank that stored credential set.
    if let Some((tier, _)) = bound {
        let tier = ServiceTier::parse(&tier).ok_or_else(|| {
            OpReply::error(
                "account_selection_unavailable",
                "The service tier recorded on this routing decision isn't one routing knows, so it can't keep an account bound to it. Ask routing for a new decision.",
            )
        })?;
        parsed.service_tier_bound = !tier.is_standard();
        if parsed.service_tier_bound
            && !roster.roster.as_ref().is_some_and(|roster| {
                roster.accounts.iter().any(|account| {
                    account
                        .credentials
                        .iter()
                        .any(|credential| tier_candidate_matches(parsed, account, credential))
                })
            })
        {
            return Err(bound_service_tier_refusal(parsed));
        }
        return Ok((!tier.is_standard() || parsed.service_tier.is_some()).then_some(tier));
    }
    let Some(requested) = parsed.service_tier else {
        return Ok(None);
    };
    for tier in fallback(requested).filter(|tier| !tier.is_standard()) {
        let Some(mut classes) = ctx.classes.clone() else {
            continue;
        };
        let mut facts: Vec<_> = classes
            .facts
            .iter()
            .filter(|fact| {
                fact.provider_id == normalize(&parsed.provider)
                    && fact.model_id == normalize(&parsed.model)
            })
            .cloned()
            .collect();
        let runners: Vec<_> = classes
            .config
            .v1_runners()
            .into_iter()
            .filter(|runner| runner.module == parsed.caller.runner)
            .collect();
        let registry = crate::routing_v1::runners::Registry::build(
            classes.config.v1_runners(),
            &classes.credentials,
            &classes.facts,
            classes.now_ms.max(0) as u64,
        );
        classes.apply_service_tier(
            &mut facts,
            tier.as_str(),
            &runners,
            &registry,
            class == prefrontal_routing_core::accounts::AllowedAccountClass::PricedPerTokenOnly,
            None,
        );
        let allowed =
            classes.service_tier_accounts(&normalize(&parsed.provider), &normalize(&parsed.model));
        let eligible = |account: &AccountView, credential: &CredentialRecord| {
            allowed.contains(&credential.credential_id)
                && tier_candidate_matches(parsed, account, credential)
        };
        if roster.roster.as_ref().is_some_and(|roster| {
            roster.accounts.iter().any(|account| {
                account
                    .credentials
                    .iter()
                    .any(|credential| eligible(account, credential))
            })
        }) {
            if let Some(roster) = &mut roster.roster {
                for account in &mut roster.accounts {
                    account
                        .credentials
                        .retain(|credential| allowed.contains(&credential.credential_id));
                }
                roster
                    .accounts
                    .retain(|account| !account.credentials.is_empty());
            }
            return Ok(Some(tier));
        }
    }
    Ok(Some(ServiceTier::Standard))
}

/// The refusal for a request every account it could use is route-fenced on:
/// `account_pool_exhausted`, timed by the earliest next check among them.
/// Only one request may claim that check; the retry time is not an expiry.
fn route_fence_refusal(
    roster: &RosterBuild,
    fenced: &BTreeMap<String, i64>,
    params: &AccountForParams,
    now: i64,
) -> Option<OpReply> {
    let wanted = |account: &str| {
        params.candidates.as_ref().is_none_or(|candidates| {
            candidates
                .accounts
                .iter()
                .any(|candidate| candidate.account_id == account)
        })
    };
    if roster
        .roster
        .iter()
        .flat_map(|roster| &roster.accounts)
        .any(|view| wanted(&view.account_id))
    {
        return None;
    }
    let until = fenced
        .iter()
        .filter(|(account, _)| wanted(account))
        .map(|(_, until)| *until)
        .min()?;
    let retry_after_s = u64::try_from(until.saturating_sub(now))
        .unwrap_or(0)
        .div_ceil(1000);
    Some(OpReply::typed(
        "account_pool_exhausted",
        format!(
            "a failed report closed {}/{} on every account this request may use until {}",
            params.provider,
            params.model,
            rfc3339(until)
        ),
        json!({
            "code": "account_pool_exhausted",
            "retry_after_s": retry_after_s,
            "decision_id": null,
            "exclusions": [],
            "reset_at_ms": until,
        }),
    ))
}

/// The contract code for an empty eligible roster with class-aware routing.
/// A route fence that closed every usable account is already answered as
/// `account_pool_exhausted` by `route_fence_refusal`. What remains is split
/// by cause, because Broca classes `account_not_authorized` as an auth
/// failure and pauses the worker: an account limit must never use it.
/// - No serving credential for the payment class at all needs a login
///   (`auth_required`).
/// - Every remaining credential excluded by a fence (a billing or
///   entitlement credential fence, a route fence, or quota exhaustion) is an
///   account limit (`account_pool_exhausted`), with `reset_at_ms` when any
///   fence or window names a known reset and without it otherwise.
/// - Every remaining credential excluded by operator policy for this request
///   (model or account eligibility, or the size band) is one the caller may
///   not use (`account_not_authorized`).
/// When causes mix, the most recoverable wins: `account_pool_exhausted`
/// beats `account_not_authorized`.
/// Return `None` when serving credentials exist but the calling model-execution
/// module offers none of them in its candidate list. The allocator preserves
/// that disagreement as unavailable with `not_in_runner_candidates` exclusions,
/// rather than requiring sign-in.
fn empty_roster_refusal(
    classes: &ClassContext,
    provider: &str,
    model: &str,
    priced: bool,
    fenced: &BTreeMap<String, i64>,
    now: i64,
    params: Option<&AccountForParams>,
) -> Option<OpReply> {
    let untimed = |code: &str, message: String, exclusions: &[Value]| {
        Some(OpReply::typed(
            code,
            message,
            json!({
                "code": code,
                "retry_after_s": null,
                "decision_id": null,
                "exclusions": exclusions,
            }),
        ))
    };
    let exhausted = |until: Option<i64>, exclusions: &[Value]| {
        let (retry_after_s, reset_at_ms) = match until.filter(|until| *until > now) {
            Some(until) => (
                Some(
                    u64::try_from(until.saturating_sub(now))
                        .unwrap_or(0)
                        .div_ceil(1000),
                ),
                Some(until),
            ),
            None => (None, None),
        };
        Some(OpReply::typed(
            "account_pool_exhausted",
            "Routing found no usable account among the offered credentials. Each one is listed in exclusions with its reason and, where it has one, when it can be tried again.",
            json!({
                "code": "account_pool_exhausted",
                "retry_after_s": retry_after_s,
                "decision_id": null,
                "exclusions": exclusions,
                "reset_at_ms": reset_at_ms,
            }),
        ))
    };
    let fact = classes
        .facts
        .iter()
        .find(|fact| fact.provider_id == provider && fact.model_id == model);
    let class_rows = fact.map_or_else(Vec::new, |fact| classes.class(fact, priced).rows);
    let mut exclusions = Vec::new();
    if let Some(candidates) = params.and_then(|params| params.candidates.as_ref()) {
        for candidate in &candidates.accounts {
            let row = classes
                .credentials
                .listed_credentials()
                .iter()
                .find(|row| row.id == candidate.credential_id);
            let reason = match row {
                None => Some("unknown_credential"),
                Some(row)
                    if !crate::broca_credentials::credential_providers(row, &classes.catalog)
                        .iter()
                        .any(|mapped| mapped == provider) =>
                {
                    Some("wrong_provider")
                }
                Some(row)
                    if row.account_id.as_deref().unwrap_or(&row.id) != candidate.account_id =>
                {
                    Some("account_id_mismatch")
                }
                Some(row) if !class_rows.iter().any(|allowed| allowed.id == row.id) => {
                    Some("not_in_account_class")
                }
                Some(_) => None,
            };
            if let Some(reason) = reason {
                exclusions.push(json!({"account_id":candidate.account_id,
                    "credential_id":candidate.credential_id, "reason":reason}));
            }
        }
    }
    if fact.is_none() {
        checking("missing_model_credentials");
        return untimed(
            "auth_required",
            format!("routing selects no accounts for provider `{provider}`"),
            &exclusions,
        );
    }
    let rows = class_rows;
    if rows.is_empty() {
        checking("missing_class_credentials");
        return untimed(
            "auth_required",
            format!("routing selects no accounts for provider `{provider}`"),
            &exclusions,
        );
    }
    let rows: Vec<_> = rows
        .into_iter()
        .filter(|row| {
            params
                .and_then(|params| params.candidates.as_ref())
                .is_none_or(|candidates| {
                    candidates.accounts.iter().any(|candidate| {
                        candidate.credential_id == row.id
                            && candidate.account_id == row.account_id.as_deref().unwrap_or(&row.id)
                    })
                })
        })
        .collect();
    if rows.is_empty() {
        return None;
    }
    let tokens = params.map_or(0, |params| {
        params
            .request_input_tokens
            .unwrap_or_else(|| params.initial_input_bytes.div_ceil(4))
    });
    let size_class = prefrontal_routing_core::accounts::input_size_class(tokens);
    let mut fenced_cause = false;
    let mut row_resets: Vec<Option<i64>> = Vec::new();
    for row in &rows {
        if let Some(until) = classes
            .probe_eligible(provider, model, &row.id)
            .then(|| {
                probe_reservations::blocked_until(
                    &classes.probe_scope,
                    provider,
                    &row.id,
                    classes.probe_owner.as_deref(),
                    now,
                )
            })
            .flatten()
        {
            let until = classes
                .rechecks
                .iter()
                .filter(|origin| {
                    classes.origin_applies(
                        origin,
                        provider,
                        row.account_id.as_deref().unwrap_or(&row.id),
                        &row.id,
                        model,
                    ) && !classes.origin_due(
                        origin,
                        row.account_id.as_deref().unwrap_or(&row.id),
                        model,
                    )
                })
                .map(|origin| {
                    classes.recheck_hint(
                        origin,
                        row.account_id.as_deref().unwrap_or(&row.id),
                        model,
                    )
                })
                .max()
                .map_or(until, |at| at.max(until));
            fenced_cause = true;
            row_resets.push(Some(until));
            let code = classes
                .refusals
                .iter()
                .find(|fence| fence.provider_id == provider && fence.credential_id == row.id)
                .map(|fence| fence.code.as_str())
                .unwrap_or("probe_reserved");
            exclusions.push(json!({"account_id":row.account_id.as_deref().unwrap_or(&row.id),
                "credential_id":row.id, "reason":code, "reset_at_ms":until,
                "message":"Another selection holds this credential's one probe attempt for now. Retry after that hold expires, at most two minutes."}));
            continue;
        }
        if classes
            .pool_walls
            .contains(&(provider.to_owned(), row.id.clone()))
        {
            fenced_cause = true;
            row_resets.push(None);
            exclusions.push(
                json!({"account_id":row.account_id.as_deref().unwrap_or(&row.id),
                "credential_id":row.id, "reason":"pool_backoff", "reset_at_ms":null}),
            );
            continue;
        }
        let account = row.account_id.clone().unwrap_or_else(|| row.id.clone());
        let route_reset = fenced.get(&account).copied();
        if route_reset.is_some() {
            fenced_cause = true;
            // A reset adds a check, never postpones the next quota recheck.
            // Independent non-quota origins still combine by their deadlines.
            let row_reset = route_reset;
            row_resets.push(row_reset);
            exclusions.push(json!({"account_id":account, "credential_id":row.id,
                "reason":"route_fenced", "reset_at_ms":row_reset}));
            continue;
        }
        if let Some(refusal) = classes.refusals.iter().find(|fence| {
            fence.provider_id == provider
                && fence.credential_id == row.id
                && classes.rechecks.iter().any(|origin| {
                    origin.kind == "credential"
                        && origin.provider == provider
                        && origin.credential_id.as_deref() == Some(&row.id)
                        && !classes.origin_due(origin, &account, model)
                })
        }) {
            fenced_cause = true;
            // Only a billing refusal can inherit a quota reset. Entitlement
            // recovery depends on a successful request, not utilization.
            let quota_reset = crate::credential_fences::is_billing_code(&refusal.code)
                .then(|| classes.quota_hold_reset(provider, &row.id))
                .flatten();
            let probe_reset = classes
                .rechecks
                .iter()
                .filter(|origin| {
                    origin.kind == "credential"
                        && origin.provider == provider
                        && origin.credential_id.as_deref() == Some(&row.id)
                })
                .map(|origin| classes.recheck_hint(origin, &account, model))
                .filter(|reset| *reset > now)
                .min();
            let reset = probe_reset.or(quota_reset).filter(|reset| *reset > now);
            row_resets.push(reset);
            exclusions.push(json!({"account_id":account, "credential_id":row.id,
                "reason":refusal.code, "reset_at_ms":reset,
                "message":if reset.is_some() {
                    "This credential is blocked until its quota resets or its next single probe window opens; reset_at_ms says when."
                } else {
                    "This credential is blocked with no timed reset: an operator must clear it, or its vault listing must change."
                }}));
            continue;
        }
        if classes.eligibility.iter().any(|fact_| {
            fact_.provider == provider
                && fact_.credential_id == row.id
                && (fact_.model.is_empty() || fact_.model == model)
        }) {
            exclusions.push(json!({"account_id":account, "credential_id":row.id,
                "reason":"account_not_authorized"}));
            continue;
        }
        if classes.size_refusals.iter().any(|fact_| {
            fact_.provider == provider
                && fact_.model == model
                && fact_.credential_id == row.id
                && fact_.size_class == size_class
        }) {
            exclusions.push(json!({"account_id":account, "credential_id":row.id,
                "reason":"input_size"}));
            continue;
        }
        // A row none of the filters excludes would have left the roster
        // non-empty; treat an unclassified row as policy-filtered so a new
        // filter cannot silently become a timed pool refusal.
        exclusions.push(json!({"account_id":account, "credential_id":row.id,
            "reason":"account_policy_filter"}));
    }
    if fenced_cause {
        checking("credential_or_quota_block");
        // Retry when the first known hold ends; accounts with unknown
        // resets remain excluded while another account can recover.
        let earliest = row_resets.into_iter().flatten().min();
        return exhausted(earliest, &exclusions);
    }
    checking("account_policy_filter");
    untimed(
        "account_not_authorized",
        format!("routing selects no accounts for provider `{provider}`"),
        &exclusions,
    )
}

/// A pool recovers when its first eligible account clears every dated blocker.
/// Read only the windows that actually excluded each account: an unrelated
/// short-window reset must not shorten a weekly reserve hold.
fn pool_recovery_reset(
    decision: &Decision,
    roster: Option<&Roster>,
    fences: &[Fence],
    policy: &AllocatorPolicy,
    model: &str,
    now: i64,
) -> Option<i64> {
    use prefrontal_routing_core::accounts::{ApplicableWindow, ExclusionReason};
    let roster = roster?;
    let family = policy.family_for_model(model);
    let mut releases = Vec::new();
    for note in &decision.explanation.accounts {
        let account = roster
            .accounts
            .iter()
            .find(|a| a.account_id == note.account_id)?;
        let mut resets = Vec::new();
        let window_reset = |window: ApplicableWindow| -> Option<i64> {
            let quota = account.quota.as_ref()?;
            let general = match window {
                ApplicableWindow::FiveHour => GeneralWindow::FiveHour,
                ApplicableWindow::SevenDay => GeneralWindow::SevenDay,
                ApplicableWindow::Reported(minutes) => GeneralWindow::Reported(minutes),
                ApplicableWindow::FamilyScope => {
                    let ScopedList::Listed(windows) = &quota.scoped else {
                        return None;
                    };
                    return windows
                        .iter()
                        .filter(|window| policy.family_for_scope(window) == Some(family))
                        .map(|window| window.resets_at.at_ms())
                        .collect::<Option<Vec<_>>>()?
                        .into_iter()
                        .max();
                }
            };
            quota.window(general)?.resets_at.at_ms()
        };
        for reason in &note.exclusions {
            match reason {
                ExclusionReason::BelowReserve { windows } => {
                    for window in windows {
                        resets.push(window_reset(*window)?);
                    }
                }
                ExclusionReason::BelowBrocaFloor { window, .. } => {
                    resets.push(window_reset((*window).into())?);
                }
                ExclusionReason::ScopeExhausted { .. } => {
                    resets.push(window_reset(ApplicableWindow::FamilyScope)?);
                }
                ExclusionReason::ConfirmedFence { pool } => {
                    let matching: Vec<_> = fences
                        .iter()
                        .filter(|fence| {
                            fence.account_id == account.account_id
                                && fence.pool == *pool
                                && fence.applies_to(
                                    model,
                                    family,
                                    request_pool(None, &account.routing_provider, model).as_deref(),
                                )
                        })
                        .collect();
                    if matching.is_empty() {
                        return None;
                    }
                    for fence in matching {
                        resets.push(fence.reset.at_ms()?);
                    }
                }
                // Missing evidence and permanent eligibility failures do not
                // promise a recovery time.
                _ => return None,
            }
        }
        let release = resets.into_iter().max()?;
        if release <= now {
            return None;
        }
        releases.push(release);
    }
    releases.into_iter().min()
}

/// Check a cold worker launch without reserving an account or creating affinity.
/// The runner still performs the usual atomic admission when it starts sending.
#[cfg(test)]
pub(crate) fn preflight_model(
    ctx: &Ctx<'_>,
    provider: &str,
    model: &str,
) -> Result<(), (String, String)> {
    preflight_model_for_class(ctx, provider, model, false)
}

pub(crate) fn preflight_model_for_class(
    ctx: &Ctx<'_>,
    provider: &str,
    model: &str,
    priced_per_token: bool,
) -> Result<(), (String, String)> {
    preflight_model_for_class_with_reset(ctx, provider, model, priced_per_token)
        .map_err(|refusal| (refusal.code, refusal.message))
}

pub(crate) struct PreflightRefusal {
    pub code: String,
    pub message: String,
    pub reset_at_ms: Option<i64>,
}

/// Keep dated account evidence separate from the legacy diagnostic message.
pub(crate) fn preflight_model_for_class_with_reset(
    ctx: &Ctx<'_>,
    provider: &str,
    model: &str,
    priced_per_token: bool,
) -> Result<(), PreflightRefusal> {
    if !ctx.flags.enabled {
        return Ok(());
    }
    let roster = ctx.classes.as_ref().map_or_else(
        || roster_for_model(ctx, provider, Some(model)),
        |classes| classes.roster_for(provider, model, priced_per_token),
    );
    // Providers outside the account allocator retain their normal credential admission.
    if roster
        .roster
        .as_ref()
        .is_some_and(|roster| roster.accounts.is_empty())
    {
        if ctx.classes.is_some() {
            return Err(PreflightRefusal {
                code: "no_eligible_account".into(),
                message: format!("{provider}/{model}: no eligible account"),
                reset_at_ms: ctx.classes.as_ref().and_then(|classes| {
                    classes
                        .blocked_reset(provider, model, priced_per_token)
                        .flatten()
                }),
            });
        }
        return Ok(());
    }
    let request = synthetic_request(provider, model, "worker-model-preflight".into());
    let policy = ctx.provider_policy(provider);
    let inputs = ctx
        .classes
        .as_ref()
        .map_or_else(Default::default, |classes| {
            classes.allocation_context(provider, model)
        });
    let (decision, fences) = ctx
        .store
        .account_read(|tx| {
            let fences = load_fences(tx, provider)?;
            let decision = prefrontal_routing_core::accounts::decide_with_context(
                &request,
                &AllocatorState {
                    roster: roster.roster.clone(),
                    affinity: None,
                    fences: fences.clone(),
                    principal_exclusions: Vec::new(),
                    admissions: load_demand(
                        tx,
                        provider,
                        roster.roster.as_ref(),
                        &policy,
                        &inputs,
                    )?,
                    operator_override: None,
                    policy: policy.clone(),
                },
                ctx.now_ms,
                &inputs,
            );
            Ok((decision, fences))
        })
        .map_err(|error| PreflightRefusal {
            code: "account_selection_unavailable".into(),
            message: error.to_string(),
            reset_at_ms: None,
        })?;
    match decision.outcome {
        Outcome::Admit(_) => Ok(()),
        Outcome::Hold(hold) => Err(PreflightRefusal {
            code: "quota_exhausted".into(),
            message: format!(
                "{provider}/{model}: quota_exhausted until {}",
                rfc3339(hold.reset_at_ms)
            ),
            reset_at_ms: Some(hold.reset_at_ms),
        }),
        Outcome::Refuse(refusal) => {
            let code = refusal_code(refusal.code).to_string();
            // A retry includes jitter/margin and is not itself a provider reset.
            // Name the published reset only for quota refusals with dated evidence.
            let family = ctx.policy.family_for_model(model);
            let resets = roster
                .roster
                .iter()
                .flat_map(|roster| &roster.accounts)
                .filter_map(|account| account.quota.as_ref())
                .flat_map(|quota| {
                    let mut dates: Vec<i64> = quota
                        .required_windows()
                        .into_iter()
                        .filter_map(|window| quota.window(window))
                        .filter_map(|window| window.resets_at.at_ms())
                        .collect();
                    if let ScopedList::Listed(windows) = &quota.scoped {
                        dates.extend(
                            windows
                                .iter()
                                .filter(|window| {
                                    ctx.policy.family_for_scope(window) == Some(family)
                                })
                                .filter_map(|window| window.resets_at.at_ms()),
                        );
                    }
                    dates
                })
                .chain(
                    fences
                        .iter()
                        .filter(|fence| fence.applies_to(model, family, request.pool.as_deref()))
                        .filter_map(|fence| fence.reset.at_ms()),
                );
            let reset_at_ms = if matches!(
                refusal.code,
                RefusalCode::AccountPoolExhausted | RefusalCode::AccountPoolReserveLimited
            ) {
                resets.filter(|date| *date > ctx.now_ms).min()
            } else {
                None
            };
            let clearing = if matches!(
                refusal.code,
                RefusalCode::AccountPoolExhausted | RefusalCode::AccountPoolReserveLimited
            ) {
                reset_at_ms
                    .map(|date| format!(" until {}", rfc3339(date)))
                    .unwrap_or_else(|| "; clearing time unknown".into())
            } else {
                String::new()
            };
            Err(PreflightRefusal {
                message: format!("{provider}/{model}: {code}{clearing}"),
                code,
                reset_at_ms: ctx.classes.as_ref().map_or(reset_at_ms, |classes| {
                    classes
                        .blocked_reset(provider, model, priced_per_token)
                        .flatten()
                }),
            })
        }
    }
}

/// The whole admission inside one write transaction: idempotency, the
/// compare-and-set, the decision and its commit. The transaction commits
/// before the reply leaves, so a reply always describes a durable decision.
fn serve(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    params: &AccountForParams,
    provider: &str,
    roster: &RosterBuild,
    request_fingerprint: &str,
) -> RouteStateResult<OpReply> {
    checking("admission_transaction");
    let now = ctx.now_ms;
    let policy = ctx.provider_policy(provider);
    if let Some(existing) = tx.admission(principal, &params.admission_id)? {
        return replay(tx, principal, existing, request_fingerprint, now);
    }
    let salt = tx.salt(now, fresh_salt)?;
    let hash = conversation_hash(&salt, principal, &params.conversation_key);
    let affinity_row = tx.affinity(principal, &hash, provider)?;
    // The tier-filtered roster must still contain the conversation's exact
    // credential. A healthy sibling cannot replace a bound session's pin.
    if params.service_tier_bound
        && affinity_row.as_ref().is_some_and(|affinity| {
            !roster.roster.as_ref().is_some_and(|roster| {
                roster.accounts.iter().any(|account| {
                    account.account_id == affinity.account_id
                        && account.credentials.iter().any(|credential| {
                            credential.credential_id == affinity.credential_id
                                && tier_candidate_matches(params, account, credential)
                        })
                })
            })
        })
    {
        return Ok(bound_service_tier_refusal(params));
    }
    let established = match affinity_row.as_ref() {
        Some(affinity) => tx.pin_has_started(affinity)?,
        None => false,
    };
    let mut roster = roster.clone();
    if !established {
        if let Some(classes) = &ctx.classes {
            classes.avoid_transient_accounts(&mut roster, provider, &params.model);
        }
    }
    let roster = &roster;
    if let Some(expected) = &params.expected_pin {
        let current = affinity_row
            .as_ref()
            .map(|row| (row.pin_id.as_str(), row.pin_generation));
        if params.mode != RequestMode::Admit
            && current != Some((expected.id.as_str(), expected.generation))
        {
            return Ok(pin_conflict(affinity_row.as_ref(), expected));
        }
    }
    refresh_blocks(
        tx,
        provider,
        roster
            .roster
            .as_ref()
            .map_or(&[][..], |roster| &roster.accounts),
        now,
        &policy,
    )?;
    // The allocation context comes first: whether an account may go on paid
    // credits decides how much of its demand history the allocator can use.
    let mut inputs = ctx
        .classes
        .as_ref()
        .map_or_else(Default::default, |classes| {
            classes.allocation_context(provider, &params.model)
        });
    inputs.hold_ok = params.hold_ok;
    if let Some(parent) = &params.parent_conversation_key {
        let hash = conversation_hash(&salt, principal, parent);
        inputs.parent_account_id = tx.parent_used_account(principal, &hash, provider, now)?;
    }
    let admissions = load_demand(tx, provider, roster.roster.as_ref(), &policy, &inputs)?;
    let blocks = load_blocks(tx, provider)?;
    let bound_pool = request_pool(params.pool.as_deref(), provider, &params.model);
    let origins: Vec<_> = tx
        .fence_rechecks()?
        .into_iter()
        .filter(|origin| {
            origin.kind != "account"
                || blocks.iter().any(|(id, fence)| {
                    origin.origin_key == json!([id]).to_string()
                        && fence.applies_to(
                            &params.model,
                            policy.family_for_model(&params.model),
                            bound_pool.as_deref(),
                        )
                })
        })
        .collect();
    let targets = |origin: &prefrontal_routing_store::FenceRecheck,
                   account: &str,
                   credential: &str| {
        ctx.classes.as_ref().map_or_else(
            || {
                let named = origin
                    .credential_id
                    .as_deref()
                    .filter(|id| {
                        origin.quota
                            && roster.roster.as_ref().is_some_and(|roster| {
                                roster.accounts.iter().any(|view| {
                                    view.account_id == account
                                        && view
                                            .credentials
                                            .iter()
                                            .any(|record| record.credential_id == *id)
                                })
                            })
                    })
                    .unwrap_or(credential);
                origin.applies(provider, account, named, &params.model)
            },
            |classes| classes.origin_targets(origin, provider, account, credential, &params.model),
        )
    };
    let size = prefrontal_routing_core::accounts::input_size_class(
        params
            .request_input_tokens
            .unwrap_or_else(|| params.initial_input_bytes.div_ceil(4)),
    ) as i64;
    let quota_due = |account: &str, credential: &str| {
        prefrontal_routing_store::FenceRecheck::quota_group_due(
            origins.iter().filter(|origin| {
                targets(origin, account, credential)
                    && origin.principal.is_none()
                    && origin.size_class.is_none_or(|class| class == size)
            }),
            now,
            params.model_decision_id.as_deref(),
        )
    };
    let block_fences: Vec<_> = blocks.iter().map(|(_, fence)| fence.clone()).collect();
    let binding_reading = |origin: &prefrontal_routing_store::FenceRecheck, account: &str| {
        ctx.classes.as_ref().map_or_else(
            || {
                let reading = roster
                    .roster
                    .as_ref()
                    .and_then(|roster| {
                        roster
                            .accounts
                            .iter()
                            .find(|view| view.account_id == account)
                    })
                    .and_then(|view| view.quota.as_ref());
                quota_reading_binds_origin(origin, account, &block_fences, reading, now, &policy)
            },
            |classes| classes.origin_has_binding_reading(origin, account),
        )
    };
    let mut probe_roster = roster.roster.clone();
    if let Some(roster) = &mut probe_roster {
        for account in &mut roster.accounts {
            let fresh = origins
                .iter()
                .filter(|origin| {
                    origin.quota
                        && account.credentials.iter().any(|credential| {
                            targets(origin, &account.account_id, &credential.credential_id)
                        })
                })
                .any(|origin| binding_reading(origin, &account.account_id));
            if !fresh
                && origins.iter().any(|origin| {
                    origin.quota
                        && account.credentials.iter().any(|credential| {
                            targets(origin, &account.account_id, &credential.credential_id)
                                && quota_due(&account.account_id, &credential.credential_id)
                        })
                })
            {
                // A stale reading is not an independent wall against the very
                // probe that will recheck its retained quota evidence.
                account.quota = None;
            }
        }
    }
    let mut fences = load_fences(tx, provider)?;
    fences.retain(|fence| {
        !origins.iter().any(|origin| {
            origin.kind == "account"
                && origin.account_id.as_deref() == Some(&fence.account_id)
                && origin.evidence_at == fence.observed_at_ms
                && quota_due(&fence.account_id, "")
                && !binding_reading(origin, &fence.account_id)
        })
    });
    let mut state = AllocatorState {
        roster: probe_roster,
        affinity: affinity_row
            .as_ref()
            .map(|row| conversation_affinity(row, now)),
        fences,
        principal_exclusions: load_exclusions(tx, principal, &params.admission_id)?,
        admissions,
        operator_override: None,
        policy: policy.clone(),
    };
    let request = params.request(principal, affinity_row.is_none());
    let mut reserved_probe = false;
    let mut lost_probe_fences = BTreeMap::new();
    let mut decision = loop {
        let decision =
            prefrontal_routing_core::accounts::decide_with_context(&request, &state, now, &inputs);
        let Outcome::Admit(admission) = &decision.outcome else {
            break decision;
        };
        let credential = admission.credential_id.clone();
        let applicable: Vec<_> = origins
            .iter()
            .filter(|origin| {
                origin.kind != "principal"
                    || state.principal_exclusions.iter().any(|exclusion| {
                        exclusion.credential_id == credential
                            && exclusion.recorded_at_ms == origin.evidence_at
                            && state.roster.as_ref().is_none_or(|roster| {
                                roster.view_digest == exclusion.roster_view_digest
                            })
                            && request.candidates.as_ref().is_none_or(|candidates| {
                                candidates.view_digest == exclusion.runner_view_digest
                            })
                    })
            })
            .filter(|origin| {
                ctx.classes
                    .as_ref()
                    .is_none_or(|classes| !classes.origin_recovered(origin, &admission.account_id))
            })
            .filter(|origin| {
                targets(origin, &admission.account_id, &credential)
                    && origin.principal.as_deref().is_none_or(|id| id == principal)
                    && origin
                        .admission_id
                        .as_deref()
                        .is_none_or(|id| id == params.admission_id)
                    && origin.size_class.is_none_or(|class| {
                        class
                            == prefrontal_routing_core::accounts::input_size_class(
                                params
                                    .request_input_tokens
                                    .unwrap_or_else(|| params.initial_input_bytes.div_ceil(4)),
                            ) as i64
                    })
            })
            .cloned()
            .collect();
        if applicable.is_empty() {
            break decision;
        }
        if established
            && state.affinity.as_ref().is_some_and(|affinity| {
                affinity.account_id == admission.account_id && affinity.credential_id == credential
            })
            && applicable
                .iter()
                .any(|origin| origin.kind == "transient_route" && origin.due_at() > now)
            && applicable
                .iter()
                .filter(|origin| origin.kind != "transient_route")
                .all(|origin| {
                    ctx.classes
                        .as_ref()
                        .map_or(origin.due_at() <= now, |classes| {
                            classes.origin_due(origin, &admission.account_id, &params.model)
                        })
                })
        {
            // A live affinity waits out its independent short transient retry;
            // the hold below does not consume any probe authorization.
            break decision;
        }
        let claim = || {
            let ready = applicable.iter().all(|origin| {
                if origin.quota {
                    let fresh = binding_reading(origin, &admission.account_id);
                    return !fresh
                        && prefrontal_routing_store::FenceRecheck::quota_group_due(
                            &applicable,
                            now,
                            params.model_decision_id.as_deref(),
                        );
                }
                ctx.classes
                    .as_ref()
                    .map_or(origin.due_at() <= now, |classes| {
                        classes.origin_due(origin, &admission.account_id, &params.model)
                    })
            });
            Ok(
                if ready
                    && tx.claim_fence_rechecks_for_authorized_admission(
                        &applicable,
                        now,
                        params.model_decision_id.as_deref(),
                        &admission_probe_owner(principal, &params.admission_id),
                    )?
                {
                    prefrontal_routing_store::CredentialProbeClaim::Claimed
                } else {
                    prefrontal_routing_store::CredentialProbeClaim::Blocked
                },
            )
        };
        let claimed = if let Some(classes) = &ctx.classes {
            probe_reservations::claim(
                &classes.probe_scope,
                provider,
                &credential,
                classes.probe_owner.as_deref(),
                now,
                claim,
            )?
        } else {
            Some(claim()?)
        };
        let Some(claimed) = claimed else {
            reserved_probe = true;
            if let Some(until) = ctx.classes.as_ref().and_then(|classes| {
                probe_reservations::blocked_until(
                    &classes.probe_scope,
                    provider,
                    &credential,
                    classes.probe_owner.as_deref(),
                    now,
                )
            }) {
                lost_probe_fences.insert(admission.account_id.clone(), until);
            }
            inputs.credential_probe_notes.remove(&credential);
            if let Some(roster) = &mut state.roster {
                for account in &mut roster.accounts {
                    account
                        .credentials
                        .retain(|record| record.credential_id != credential);
                }
            }
            continue;
        };
        match claimed {
            prefrontal_routing_store::CredentialProbeClaim::Claimed => break decision,
            prefrontal_routing_store::CredentialProbeClaim::Unfenced => {
                inputs.credential_probe_notes.remove(&credential);
            }
            prefrontal_routing_store::CredentialProbeClaim::Blocked => {
                // Another request can claim the probe after scoring prepared
                // this roster. Preserve the actual fence and its retry time;
                // removing only the credential misreports missing candidates.
                if let Some(until) = applicable
                    .iter()
                    .map(|origin| origin.due_at())
                    .filter(|until| *until > now)
                    .max()
                {
                    lost_probe_fences.insert(admission.account_id.clone(), until);
                }
                inputs.credential_probe_notes.remove(&credential);
                if let Some(roster) = &mut state.roster {
                    for account in &mut roster.accounts {
                        account
                            .credentials
                            .retain(|record| record.credential_id != credential);
                    }
                }
            }
        }
    };
    if !matches!(decision.outcome, Outcome::Admit(_)) && !lost_probe_fences.is_empty() {
        let mut remaining = state.roster.clone();
        if let Some(roster) = &mut remaining {
            roster
                .accounts
                .retain(|account| !account.credentials.is_empty());
        }
        let remaining = RosterBuild {
            roster: remaining,
            context: roster.context.clone(),
        };
        if let Some(reply) = route_fence_refusal(&remaining, &lost_probe_fences, params, now) {
            return Ok(because("route_fence", reply));
        }
    }
    if reserved_probe && !matches!(decision.outcome, Outcome::Admit(_)) {
        if let Some(classes) = &ctx.classes {
            if let Some(reply) = empty_roster_refusal(
                classes,
                provider,
                &params.model,
                roster.context["priced_per_token"]
                    .as_bool()
                    .unwrap_or(false),
                &BTreeMap::new(),
                now,
                Some(params),
            ) {
                return Ok(reply);
            }
        }
    }
    if established {
        if let (Some(affinity), Outcome::Admit(admitted), Some(classes)) = (
            state.affinity.as_ref(),
            &decision.outcome,
            ctx.classes.as_ref(),
        ) {
            if admitted.account_id == affinity.account_id
                && admitted.credential_id == affinity.credential_id
            {
                if let Some(until) = classes
                    .accounts_fenced_by(&classes.transient_route_fences, provider, &params.model)
                    .get(&affinity.account_id)
                {
                    let seconds = until.saturating_sub(now).max(0) as u64;
                    let seconds = seconds.div_ceil(1_000);
                    decision.outcome = Outcome::Hold(prefrontal_routing_core::accounts::Hold {
                        account_id: affinity.account_id.clone(),
                        credential_id: affinity.credential_id.clone(),
                        pin_id: affinity.pin_id.clone(),
                        pin_generation: affinity.pin_generation,
                        reset_at_ms: *until,
                        base_retry_after_s: seconds,
                        retry_after_s: seconds,
                        basis: prefrontal_routing_core::accounts::HoldBasis::ConfirmedFence,
                    });
                    decision.explanation.affinity =
                        prefrontal_routing_core::accounts::AffinityNote::Held {
                            account_id: affinity.account_id.clone(),
                        };
                }
            }
        }
    }
    let decision_id = new_id("acd");
    let mut context = json!({ "roster": roster.context });
    if ctx
        .classes
        .as_ref()
        .is_some_and(|classes| classes.media_route)
    {
        // Persist the admission's media origin; the same provider can also
        // serve LLM runs, whose success is reported through route.report.
        context["media_admission"] = json!(true);
    }
    tx.insert_decision(&decision_record(RecordInput {
        decision_id: &decision_id,
        mode: DECISION_MODE_SERVE,
        trigger: "request",
        principal,
        admission_id: Some(&params.admission_id),
        request_mode: Some(params.mode.as_str()),
        decision: &decision,
        context,
        now_ms: now,
    }))?;
    if let Some(tier) = params.service_tier_chosen {
        tx.record_account_service_tier(
            &decision_id,
            params.service_tier.map(|tier| tier.as_str()),
            tier.as_str(),
        )?;
    }
    match &decision.outcome {
        Outcome::Admit(admission) => commit_admission(
            tx,
            ctx,
            principal,
            params,
            provider,
            &hash,
            affinity_row.as_ref(),
            &decision_id,
            &decision,
            admission,
            request_fingerprint,
            roster,
        ),
        Outcome::Hold(hold) => {
            tx.insert_hold(&AccountHoldRow {
                decision_id: decision_id.clone(),
                principal: principal.to_string(),
                conversation_hash: hash.clone(),
                provider: provider.to_string(),
                pin_id: hold.pin_id.clone(),
                pin_generation: hold.pin_generation,
                account_id: hold.account_id.clone(),
                credential_id: hold.credential_id.clone(),
                basis: wire_str(&hold.basis),
                reset_at: hold.reset_at_ms,
                retry_after_s: hold.retry_after_s,
                created_at: now,
            })?;
            let refresh_adapter = refresh_adapter_for(roster, &hold.account_id);
            Ok(OpReply::typed(
                "account_hold",
                format!(
                    "account {} is in a short hold; retry after {} s",
                    hold.account_id, hold.retry_after_s
                ),
                json!({
                    "code": "account_hold",
                    "retry_after_s": hold.retry_after_s,
                    "decision_id": decision_id,
                    "account": {
                        "provider": provider,
                        "account_id": hold.account_id,
                        "credential_id": hold.credential_id,
                        "refresh_adapter": refresh_adapter,
                        "method": ctx.classes.as_ref().map_or("oauth", |classes| classes.method(&hold.credential_id)),
                    },
                    "pin": { "id": hold.pin_id, "generation": hold.pin_generation },
                    "reset_at": rfc3339(hold.reset_at_ms),
                    "basis": wire(&hold.basis),
                    "exclusions": wire_exclusions(&decision),
                }),
            ))
        }
        Outcome::Refuse(refusal) => {
            checking("allocator_filters");
            // The caller sees the allocator's own contract code. The store
            // keeps that code too (`outcome_columns`), so this mapping must
            // not change the recorded decision, only the reply. The contract
            // has no `account_selection_unsupported`, so map it by cause.
            // The allocator emits it in three places: an empty provider
            // slice of a present roster, a cold roster whose accounts all
            // fail model reach, and a hard-filtered operator override that
            // fails reach (the override never runs here). The first two
            // both mean no credential of the right kind serves this
            // provider/model, which is `auth_required`: the runner needs a
            // login it does not have. It is untimed and must never read as
            // a transient retry. The one temporary case is probe
            // contention: the probe loop below removed every credential
            // after another admission claimed the probe, and a re-read may
            // admit, so that stays `account_selection_unavailable`.
            let mut code = refusal_code(refusal.code);
            if code == "account_selection_unsupported" {
                let probe_emptied = roster
                    .roster
                    .as_ref()
                    .is_some_and(|roster| !roster.accounts.is_empty())
                    && state
                        .roster
                        .as_ref()
                        .is_none_or(|roster| roster.accounts.is_empty());
                code = if probe_emptied {
                    "account_selection_unavailable"
                } else {
                    "auth_required"
                };
            }
            let reset = pool_recovery_reset(
                &decision,
                state.roster.as_ref(),
                &state.fences,
                ctx.policy,
                &params.model,
                now,
            );
            let timed = matches!(
                code,
                "account_pool_exhausted" | "account_pool_reserve_limited"
            );
            Ok(OpReply::typed(
                code,
                format!("no account admitted: {code}"),
                json!({
                    "code": code,
                    "retry_after_s": timed.then_some(refusal.retry_after_s).flatten(),
                    "decision_id": decision_id,
                    "exclusions": wire_exclusions(&decision),
                    "reset_at_ms": timed.then_some(reset).flatten(),
                }),
            ))
        }
    }
}

/// A duplicate admission id: the same identity gets the committed decision
/// back (marked `expired` once its start window has passed) and charges
/// nothing; different identity fields are a conflict.
fn replay(
    tx: &AccountTx<'_>,
    principal: &str,
    existing: AccountAdmissionRow,
    request_fingerprint: &str,
    now: i64,
) -> RouteStateResult<OpReply> {
    if existing.request_fingerprint != request_fingerprint {
        checking("admission_fingerprint");
        return Ok(OpReply::typed(
            "admission_conflict",
            format!(
                "admission {} was already decided with different parameters",
                existing.admission_id
            ),
            json!({
                "code": "admission_conflict",
                "retry_after_s": null,
                "decision_id": existing.decision_id,
                "exclusions": [],
            }),
        ));
    }
    let mut state = existing.state.clone();
    if state == "reserved" && now >= existing.admission_valid_until {
        tx.mark_admission_expired(principal, &existing.admission_id)?;
        state = "expired".into();
    }
    let mut reply: Value = serde_json::from_str(&existing.reply_json).map_err(|error| {
        RouteStateStoreError(format!("stored admission reply undecodable: {error}"))
    })?;
    reply["admission"]["state"] = json!(state);
    Ok(OpReply::Result(reply))
}

fn pin_conflict(current: Option<&AccountAffinityRow>, expected: &PinRef) -> OpReply {
    checking("pin_mismatch");
    OpReply::typed(
        "pin_generation_conflict",
        format!(
            "expected pin {} generation {} is not the conversation's current pin",
            expected.id, expected.generation
        ),
        json!({
            "code": "pin_generation_conflict",
            "retry_after_s": null,
            "decision_id": null,
            "exclusions": [],
            "current_pin": current.map(|row| json!({
                "id": row.pin_id,
                "generation": row.pin_generation,
                "account_id": row.account_id,
                "credential_id": row.credential_id,
            })),
        }),
    )
}

#[allow(clippy::too_many_arguments)]
fn commit_admission(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    params: &AccountForParams,
    provider: &str,
    hash: &str,
    affinity_row: Option<&AccountAffinityRow>,
    decision_id: &str,
    decision: &Decision,
    admission: &prefrontal_routing_core::accounts::Admission,
    request_fingerprint: &str,
    roster: &RosterBuild,
) -> RouteStateResult<OpReply> {
    let now = ctx.now_ms;
    let pin_mode = match params.caller_kind() {
        CallerKind::Broca => "run_frozen",
        CallerKind::Plugin => "conversation_lease",
    };
    let lease_valid_until = now + LEASE_MS;
    let admission_valid_until = now + ADMISSION_VALID_MS;
    let expected = affinity_row.map(|row| (row.pin_id.as_str(), row.pin_generation));
    let (pin_id, pin_generation, affinity_expires_at, moved) = match &admission.affinity {
        AffinityOutcome::Retained {
            pin_id,
            pin_generation,
        } => {
            let row = affinity_row.ok_or_else(|| {
                RouteStateStoreError("allocator retained an affinity routing does not hold".into())
            })?;
            // Accepted use renews affinity at most once a minute; the lease
            // moves on every admission.
            let (last_used, expires_at) = if now - row.last_used_at >= AFFINITY_TOUCH_MS {
                (now, now + AFFINITY_IDLE_MS)
            } else {
                (row.last_used_at, row.expires_at)
            };
            if !tx.touch_affinity(
                principal,
                hash,
                provider,
                (pin_id, *pin_generation),
                last_used,
                expires_at,
                Some(lease_valid_until),
            )? {
                return Err(RouteStateStoreError(
                    "conversation pin changed inside the admission transaction".into(),
                ));
            }
            (pin_id.clone(), *pin_generation, expires_at, false)
        }
        AffinityOutcome::New {
            previous,
            pin_generation,
        } => {
            let pin_id = new_id("pin");
            tx.insert_pin(&AccountPinRow {
                pin_id: pin_id.clone(),
                principal: principal.to_string(),
                conversation_hash: hash.to_string(),
                provider: provider.to_string(),
                pin_generation: *pin_generation,
                affinity_model: normalize(params.affinity_model()),
                account_id: admission.account_id.clone(),
                credential_id: admission.credential_id.clone(),
                pin_mode: pin_mode.to_string(),
                previous_pin_id: previous.as_ref().map(|pin| pin.pin_id.clone()),
                decision_id: decision_id.to_string(),
                created_at: now,
            })?;
            let binding_revision = match affinity_row {
                Some(row) if row.credential_id == admission.credential_id => {
                    row.credential_binding_revision
                }
                Some(row) => row.credential_binding_revision + 1,
                None => 1,
            };
            let expires_at = now + AFFINITY_IDLE_MS;
            let applied = tx.put_affinity(
                &AccountAffinityRow {
                    principal: principal.to_string(),
                    conversation_hash: hash.to_string(),
                    provider: provider.to_string(),
                    affinity_model: normalize(params.affinity_model()),
                    pin_id: pin_id.clone(),
                    pin_generation: *pin_generation,
                    account_id: admission.account_id.clone(),
                    credential_id: admission.credential_id.clone(),
                    credential_binding_revision: binding_revision,
                    policy_version: decision.explanation.policy_version,
                    initial_demand_bytes: admission.request_bytes,
                    created_at: now,
                    last_used_at: now,
                    expires_at,
                    lease_valid_until: Some(lease_valid_until),
                },
                expected,
            )?;
            if !applied {
                return Err(RouteStateStoreError(
                    "conversation pin changed inside the admission transaction".into(),
                ));
            }
            (pin_id, *pin_generation, expires_at, true)
        }
    };
    // Admission charges its prompt once; a renewal charges nothing; a
    // reassignment charges only when it actually moved the conversation.
    let charged = match params.mode {
        RequestMode::Admit => true,
        RequestMode::Renew => false,
        RequestMode::Reassign => moved,
    };
    let refresh_adapter = refresh_adapter_for(roster, &admission.account_id);
    let mut reply = json!({
        "admission": { "id": params.admission_id, "state": "reserved" },
        "account": {
            "provider": provider,
            "account_id": admission.account_id,
            "credential_id": admission.credential_id,
            "refresh_adapter": refresh_adapter,
            "method": ctx.classes.as_ref().map_or("oauth", |classes| classes.method(&admission.credential_id)),
        },
        "pin": {
            "id": pin_id,
            "generation": pin_generation,
            "affinity_expires_at": rfc3339(affinity_expires_at),
            "admission_valid_until": rfc3339(admission_valid_until),
            "lease_valid_until": rfc3339(lease_valid_until),
            "mode": pin_mode,
        },
        "reason": {
            "code": wire(&admission.reason),
            "decision_id": decision_id,
            "policy_version": decision.explanation.policy_version,
            "quota_fetched_at": admission.quota_observed_at_ms.map(rfc3339),
            "quota_age_band": admission.quota_age_band.map(|band| wire(&band)),
        },
    });
    if let Some(tier) = params.service_tier_chosen {
        reply["service_tier_chosen"] = json!(tier);
    }
    if let Some(note) = &decision.explanation.credential_fence_probe {
        reply["reason"]["fence_probe"] = json!(note);
    }
    if let Some(model_decision) = &params.model_decision_id {
        tx.bind_model_account(
            model_decision,
            provider,
            &normalize(&params.model),
            &admission.account_id,
            &admission.credential_id,
        )?;
    }
    tx.insert_admission(&AccountAdmissionRow {
        principal: principal.to_string(),
        admission_id: params.admission_id.clone(),
        request_fingerprint: request_fingerprint.to_string(),
        request_mode: params.mode.as_str().to_string(),
        decision_id: decision_id.to_string(),
        conversation_hash: hash.to_string(),
        provider: provider.to_string(),
        model: normalize(&params.model),
        account_id: admission.account_id.clone(),
        credential_id: admission.credential_id.clone(),
        pin_id,
        pin_generation,
        state: "reserved".into(),
        charged,
        request_bytes: if charged { admission.request_bytes } else { 0 },
        request_bytes_estimated: admission.request_bytes_estimated,
        committed_at: now,
        admission_valid_until,
        lease_valid_until,
        reply_json: reply.to_string(),
        run_id: None,
        started_at: None,
        started_late: false,
        released_at: None,
        finished_at: None,
    })?;
    tx.record_admission_probe_scope(
        principal,
        &params.admission_id,
        prefrontal_routing_core::accounts::input_size_class(
            params
                .request_input_tokens
                .unwrap_or_else(|| params.initial_input_bytes.div_ceil(4)),
        ),
        params.model_decision_id.as_deref(),
    )?;
    Ok(OpReply::Result(reply))
}

/// Return the selected identity's actual vault adapter. Catalog vendors and
/// refresh adapters are different namespaces and must not be interchanged.
fn refresh_adapter_for(roster: &RosterBuild, account_id: &str) -> Option<String> {
    roster
        .roster
        .as_ref()?
        .accounts
        .iter()
        .find(|view| view.account_id == account_id)
        .map(|view| view.refresh_adapter.clone())
}

fn tracked_providers(ctx: &Ctx<'_>) -> BTreeSet<String> {
    ctx.accounts
        .and_then(|state| state.lock().ok())
        .map(|state| {
            state
                .readings
                .readings
                .values()
                .filter_map(|reading| reading.routing_provider().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// route.account_observe
// ---------------------------------------------------------------------------

/// `route.account_observe` params. The whole params object is also kept
/// verbatim with the observation.
#[derive(Debug, Clone, Deserialize)]
struct ObserveParams {
    /// Broca uses `<lineage_id>:<wal_seq>`; a repeat from the same runner
    /// and principal is recognized and changes nothing.
    event_id: String,
    caller: CallerParams,
    /// `admission_started`, `run_finished`, `attempt`, `scoped_not_found`,
    /// `account_limit_exhausted`, `transient_backoff` or `credential_refused`.
    /// An unlisted kind is stored and answered but changes no routing state.
    kind: String,
    #[serde(default)]
    admission_id: Option<String>,
    #[serde(default)]
    run_id: Option<String>,
    /// The account that actually served, when the runner knows it.
    #[serde(default)]
    served_account_id: Option<String>,
    #[serde(default)]
    credential_id: Option<String>,
    /// The vault view digest at the attempt's credential read.
    #[serde(default)]
    view_digest: Option<String>,
    /// The original observation time, never a forwarding time.
    #[serde(default)]
    observed_at_ms: Option<i64>,
    /// Which upstream frame the headers came from.
    #[serde(default)]
    upstream_origin: Option<FrameOrigin>,
    /// Lowercase `anthropic-ratelimit-unified-*` names and raw string values,
    /// including unknown names. Absence means no headers were received.
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    limit_window: Option<String>,
    #[serde(default)]
    resets_at_ms: Option<i64>,
    #[serde(default)]
    resets_at_source: Option<ResetSource>,
    /// The quota pool an exhaustion report names (the routing/v1 `pool`
    /// member); absent means the account is one pool.
    #[serde(default)]
    pool: Option<String>,
    #[serde(default)]
    retry_at_ms: Option<u64>,
    #[serde(default)]
    provider_code: Option<String>,
}

/// What `route.account_observe` reads from each tracked provider's roster
/// before its transaction opens: the roster digest a scoped not-found is
/// recorded against, the credential ids the vault lists, and each account's
/// latest reading, from which a whole-account block takes its recovery set.
#[derive(Default, Clone)]
struct ProviderRosterFacts {
    digest: String,
    listed: BTreeSet<String>,
    readings: BTreeMap<String, QuotaReading>,
}

const OBSERVATION_KINDS: &[&str] = &[
    "used_account",
    "admission_started",
    "run_finished",
    "attempt",
    "scoped_not_found",
    "account_limit_exhausted",
];

fn observe(params: &Value, principal: &str, ctx: &Ctx<'_>) -> OpReply {
    observe_authorized(params, principal, principal, false, ctx)
}

/// `routing_v1::accounts::account_observe` may authorize a decision owner to
/// report on its runner's admission. The event still belongs to the observer.
pub(crate) fn observe_authorized(
    params: &Value,
    principal: &str,
    admission_principal: &str,
    allow_served_account: bool,
    ctx: &Ctx<'_>,
) -> OpReply {
    checking("account_selection_enabled");
    if !ctx.flags.enabled {
        return OpReply::typed(
            DISABLED_CODE,
            "route.account_observe is not served: account selection is disabled in routing policy",
            json!({"code":DISABLED_CODE}),
        );
    }
    checking("account_observe_parse");
    let parsed: ObserveParams = match serde_json::from_value(params.clone()) {
        Ok(parsed) => parsed,
        Err(error) => return invalid(format!("invalid route.account_observe params: {error}")),
    };
    if parsed.event_id.trim().is_empty() {
        return because(
            "observation_event_id",
            invalid("`event_id` must not be empty"),
        );
    }
    if let Some(reply) = invalid_pool(parsed.pool.as_deref()) {
        return because("observation_pool", reply);
    }
    let roster_digests: BTreeMap<String, ProviderRosterFacts> = tracked_providers(ctx)
        .into_iter()
        .map(|provider| {
            let roster = roster_for(ctx, &provider).roster;
            let digest = roster
                .as_ref()
                .map(|r| r.view_digest.clone())
                .unwrap_or_default();
            let listed = roster
                .iter()
                .flat_map(|r| r.accounts.iter())
                .flat_map(|view| view.credentials.iter().map(|c| c.credential_id.clone()))
                .collect();
            let readings = roster
                .iter()
                .flat_map(|r| r.accounts.iter())
                .filter_map(|view| Some((view.account_id.clone(), view.quota.clone()?)))
                .collect();
            (
                provider,
                ProviderRosterFacts {
                    digest,
                    listed,
                    readings,
                },
            )
        })
        .collect();
    checking("observation_transaction");
    ctx.store
        .account_transaction(|tx| {
            record_observation(
                tx,
                ctx,
                principal,
                admission_principal,
                allow_served_account,
                &parsed,
                params,
                &roster_digests,
            )
        })
        .unwrap_or_else(store_error)
}

fn record_observation(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    admission_principal: &str,
    allow_served_account: bool,
    parsed: &ObserveParams,
    raw: &Value,
    roster_digests: &BTreeMap<String, ProviderRosterFacts>,
) -> RouteStateResult<OpReply> {
    let now = ctx.now_ms;
    let runner = normalize(&parsed.caller.runner);
    let existing = if allow_served_account || admission_principal != principal {
        tx.observation_by_principal_event(principal, &parsed.event_id)?
    } else {
        tx.observation_by_event(principal, &runner, &parsed.event_id)?
    };
    if let Some(existing) = existing {
        return Ok(OpReply::Result(json!({
            "observation": {
                "id": existing.observation_id,
                "duplicate": true,
                "bound": existing.bound,
                "effects": serde_json::from_str::<Value>(&existing.effects_json).unwrap_or(Value::Null),
            }
        })));
    }
    let observation_id = new_id("aco");
    // `routing_v1::accounts` resolves the admission's principal before the
    // transaction; a decision owner's event stays under the owner's principal.
    let admission = match &parsed.admission_id {
        Some(id) => tx.admission(admission_principal, id)?,
        None => None,
    };
    tx.insert_observation(&AccountObservationRow {
        observation_id: observation_id.clone(),
        principal: principal.to_string(),
        runner: runner.clone(),
        event_id: parsed.event_id.clone(),
        kind: parsed.kind.clone(),
        admission_id: parsed.admission_id.clone(),
        bound: false,
        observation_json: raw.to_string(),
        effects_json: "[]".into(),
        received_at: now,
    })?;
    let mut effects: Vec<Value> = Vec::new();
    let mut bound = false;
    let served_providers: BTreeSet<String> =
        if allow_served_account && parsed.kind == "transient_backoff" {
            ctx.classes
                .as_ref()
                .map(|classes| {
                    classes
                        .credentials
                        .listed_credentials()
                        .iter()
                        .filter(|row| {
                            parsed.served_account_id.as_deref()
                                == Some(row.account_id.as_deref().unwrap_or(&row.id))
                        })
                        .flat_map(|row| row.serves.iter().chain(&row.provider_ids).cloned())
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Default::default()
        };
    match &admission {
        None => {
            bound = !served_providers.is_empty();
            if !bound && parsed.kind != "credential_refused" {
                effects.push(json!({"effect":"advisory", "why":"names no admission routing issued to this principal"}));
            }
        }
        Some(row) => {
            let mismatch = parsed
                .served_account_id
                .as_deref()
                .is_some_and(|served| served != row.account_id);
            if mismatch {
                tracing::warn!(
                    admission = %row.admission_id,
                    selected = %row.account_id,
                    served = ?parsed.served_account_id,
                    "account selection: served account differs from the admitted one"
                );
                effects.push(json!({
                    "effect": "served_account_mismatch",
                    "selected_account_id": row.account_id,
                    "served_account_id": parsed.served_account_id,
                    "pin_generation": row.pin_generation,
                }));
            } else {
                bound = true;
            }
        }
    }
    if parsed.kind == "credential_refused" {
        apply_credential_refused(tx, ctx, parsed, admission.as_ref(), &mut effects)?;
    } else if parsed.kind == "transient_backoff" && parsed.retry_at_ms.is_some() && bound {
        let until = parsed
            .retry_at_ms
            .unwrap()
            .min((now.max(0) as u64).saturating_add(
                prefrontal_routing_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS as u64,
            ));
        if let Some(admission) = &admission {
            effects.push(
                json!({"effect":"transient_backoff", "provider":admission.provider,
                "account_id":admission.account_id, "retry_at_ms":until}),
            );
        } else if let Some(account) = &parsed.served_account_id {
            for provider in served_providers {
                effects.push(json!({"effect":"transient_backoff", "provider":provider,
                    "account_id":account, "retry_at_ms":until}));
            }
        }
    } else if !OBSERVATION_KINDS.contains(&parsed.kind.as_str()) {
        effects.push(json!({ "effect": "advisory", "why": "unknown observation kind" }));
    } else if let (true, Some(row)) = (bound, admission.as_ref()) {
        let policy = ctx.provider_policy(&row.provider);
        let scoped_ctx = Ctx {
            store: ctx.store,
            accounts: ctx.accounts,
            flags: ctx.flags,
            policy: &policy,
            now_ms: ctx.now_ms,
            classes: ctx.classes.clone(),
        };
        apply_bound_observation(
            tx,
            &scoped_ctx,
            principal,
            parsed,
            row,
            &observation_id,
            roster_digests,
            &mut effects,
        )?;
    }
    let effects_value = Value::Array(effects);
    tx.set_observation_effects(&observation_id, bound, &effects_value.to_string())?;
    Ok(OpReply::Result(json!({
        "observation": {
            "id": observation_id,
            "duplicate": false,
            "bound": bound,
            "effects": effects_value,
        }
    })))
}

fn apply_credential_refused(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    parsed: &ObserveParams,
    admission: Option<&AccountAdmissionRow>,
    effects: &mut Vec<Value>,
) -> RouteStateResult<()> {
    checking("credential_refused_target");
    let credential = parsed
        .credential_id
        .as_deref()
        .or_else(|| admission.map(|row| row.credential_id.as_str()));
    let (Some(credential), Some(classes)) = (credential, &ctx.classes) else {
        return Ok(());
    };
    // Resolve membership even if the vault already marked the credential
    // invalid. Its current usability must not erase the provider's refusal.
    let mut providers: BTreeSet<String> = classes
        .credentials
        .listed_credentials()
        .iter()
        .filter(|row| row.id == credential)
        .flat_map(|row| crate::broca_credentials::credential_providers(row, &classes.catalog))
        .collect();
    if let Some(row) = admission.filter(|row| row.credential_id == credential) {
        providers.insert(row.provider.clone());
    }
    let fingerprint = match classes.credentials.listing_fingerprint(credential) {
        crate::broca_credentials::ListingLookup::Listed(fingerprint) => Some(fingerprint),
        _ => None,
    };
    let until = ctx
        .now_ms
        .saturating_add(crate::credential_fences::CREDENTIAL_REFUSED_REPROBE_MS);
    for provider in providers {
        // A provider with no catalog rows borrows a vendor's rows through its
        // active API keys. Even if the refused key is now invalid and no longer
        // supplies that mapping, record its refusal in the pooled-key wall.
        let pool = classes.targets.contains_key(&provider)
            || (!classes.catalog.contains(&provider)
                && classes.credentials.listed_credentials().iter().any(|row| {
                    row.id == credential
                        && row.credential_type == "apikey"
                        && row
                            .serves
                            .iter()
                            .filter(|vendor| classes.catalog.contains(*vendor))
                            .count()
                            == 1
                }));
        if pool {
            tx.pool_wall(
                &provider,
                credential,
                prefrontal_routing_store::PoolWallKind::Billing,
                until,
                parsed
                    .provider_code
                    .as_deref()
                    .unwrap_or("credential_refused"),
                ctx.now_ms,
            )?;
        } else {
            // Keep the wall independent of quota readings: an empty balance or
            // a revoked key is not a resettable usage window. The provider's
            // verbatim code remains in the stored observation.
            tx.record_credential_refusal(
                &prefrontal_routing_core::route_state_store::CredentialRefusal {
                    provider_id: provider.clone(),
                    credential_id: credential.into(),
                    code: "credential_refused".into(),
                },
                fingerprint.as_deref(),
                ctx.now_ms,
            )?;
        }
        effects.push(json!({"effect":"credential_refused", "provider":provider,
            "credential_id":credential}));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_bound_observation(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    parsed: &ObserveParams,
    admission: &AccountAdmissionRow,
    observation_id: &str,
    roster_digests: &BTreeMap<String, ProviderRosterFacts>,
    effects: &mut Vec<Value>,
) -> RouteStateResult<()> {
    let now = ctx.now_ms;
    let id = &admission.admission_id;
    match parsed.kind.as_str() {
        "admission_started" => {
            let Some(run_id) = parsed.run_id.as_deref() else {
                effects.push(json!({ "effect": "none", "why": "admission_started needs run_id" }));
                return Ok(());
            };
            let late = now >= admission.admission_valid_until;
            if tx.mark_admission_started(principal, id, run_id, now, late)? {
                effects.push(json!({ "effect": "admission_started", "late": late }));
            } else {
                effects.push(json!({ "effect": "none", "state": admission.state }));
            }
        }
        "run_finished" => {
            if tx.mark_admission_finished(principal, id, now)? {
                effects.push(json!({ "effect": "admission_finished" }));
                let lease_ended = tx.end_lease(&admission.pin_id, now)?;
                effects.push(json!({ "effect": "lease_ended", "lease_ended": lease_ended }));
                let media = tx
                    .decision_context(&admission.decision_id)?
                    .and_then(|context| serde_json::from_str::<Value>(&context).ok())
                    .is_some_and(|context| context["media_admission"] == true);
                if media
                    && !tx.admission_reported_failure(principal, id)?
                    && crate::credential_fences::lift_after_success_in(
                        tx,
                        &admission.provider,
                        &admission.credential_id,
                        now,
                        admission.committed_at,
                    )?
                {
                    effects.push(json!({
                        "effect": "credential_fence_lifted",
                        "provider": admission.provider,
                        "credential_id": admission.credential_id,
                    }));
                }
                if media && !tx.admission_reported_failure(principal, id)? {
                    tx.heal_authorized_probe(
                        &admission_probe_owner(&admission.principal, &admission.admission_id),
                        &admission.provider,
                        &admission.model,
                        now,
                    )?;
                    tx.heal_serving_fences_in_scope(
                        &admission.provider,
                        &admission.account_id,
                        &admission.credential_id,
                        &admission.model,
                        tx.admission_size_class(principal, &admission.admission_id)?,
                        admission.committed_at,
                        now,
                        Some(&prefrontal_routing_store::QuotaSuccessScope {
                            family: ctx
                                .provider_policy(&admission.provider)
                                .family_for_model(&admission.model),
                            pool: request_pool(None, &admission.provider, &admission.model),
                        }),
                    )?;
                }
            } else {
                effects.push(json!({ "effect": "none", "state": admission.state }));
            }
            header_fences(
                tx,
                ctx,
                principal,
                parsed,
                admission,
                observation_id,
                effects,
            )?;
        }
        "attempt" => header_fences(
            tx,
            ctx,
            principal,
            parsed,
            admission,
            observation_id,
            effects,
        )?,
        "scoped_not_found" => {
            let (Some(credential_id), Some(view_digest)) = (
                parsed.credential_id.as_deref(),
                parsed.view_digest.as_deref(),
            ) else {
                effects.push(json!({
                    "effect": "none",
                    "why": "scoped_not_found needs credential_id and view_digest",
                }));
                return Ok(());
            };
            let ProviderRosterFacts {
                digest: roster_digest,
                listed,
                ..
            } = roster_digests
                .get(&admission.provider)
                .cloned()
                .unwrap_or_default();
            if credential_id != admission.credential_id {
                effects.push(json!({"effect":"advisory", "why":"credential differs from the bound admission"}));
                return Ok(());
            }
            let listing_fingerprint = ctx.classes.as_ref().and_then(|classes| {
                match classes.credentials.listing_fingerprint(credential_id) {
                    crate::broca_credentials::ListingLookup::Listed(fingerprint) => {
                        Some(fingerprint)
                    }
                    _ => None,
                }
            });
            tx.record_eligibility(&prefrontal_routing_store::AccountEligibilityFact {
                provider: admission.provider.clone(),
                credential_id: credential_id.into(),
                model: admission.model.clone(),
                listing_fingerprint,
                code: "model_not_on_account".into(),
                observed_at: parsed.observed_at_ms.unwrap_or(now),
            })?;
            tx.insert_exclusion_with_recheck(
                &AccountExclusionRow {
                    principal: principal.to_string(),
                    admission_id: id.clone(),
                    credential_id: credential_id.to_string(),
                    runner_view_digest: view_digest.to_string(),
                    roster_view_digest: roster_digest,
                    observation_id: Some(observation_id.to_string()),
                    recorded_at: now,
                },
                ctx.provider_policy(&admission.provider)
                    .principal_exclusion_max_ms,
            )?;
            effects.push(json!({
                "effect": "excluded_for_admission",
                "credential_id": credential_id,
            }));
            // Only the listing can drop a candidate; a credential refused again
            // while the vault still lists it is an alarm, not a revocation.
            if listed.contains(credential_id) && tx.exclusion_count(principal, credential_id)? > 1 {
                tracing::warn!(
                    principal,
                    credential_id,
                    "account selection: repeated scoped not-found for a credential the vault still lists"
                );
                effects.push(
                    json!({ "effect": "repeated_not_found_alarm", "credential_id": credential_id }),
                );
            }
        }
        "account_limit_exhausted" => {
            if parsed.limit_window.as_deref() == Some("credit") {
                // Credit is a spending wall on a valid credential, not a quota
                // pool. Utilization cannot prove that the account can pay again.
                let cap = now.saturating_add(
                    prefrontal_routing_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS,
                );
                let retry = parsed
                    .retry_at_ms
                    .map(|at| i64::try_from(at).unwrap_or(i64::MAX))
                    .or_else(|| {
                        (parsed.resets_at_source == Some(ResetSource::RetryAfter))
                            .then_some(parsed.resets_at_ms)
                            .flatten()
                    });
                let until = retry.unwrap_or(cap).min(cap);
                tx.record_non_quota_route_fence(&prefrontal_routing_store::AccountRouteFence {
                    provider: admission.provider.clone(),
                    credential_id: admission.credential_id.clone(),
                    model: admission.model.clone(),
                    until_ms: until,
                    decision_id: admission.decision_id.clone(),
                    recorded_at: now,
                })?;
                effects.push(json!({"effect":"fence","fence_id":new_id("acf"),
                    "fence":wire(&Fence {
                        account_id: admission.account_id.clone(), pool: None, recovery_set: Vec::new(),
                        observed_at_ms: now, reset: ResetTime::Missing, retry_hint_ms: Some(until),
                        source: prefrontal_routing_core::accounts::ConfirmationSource::BrocaAccountLimit,
                    })}));
                return Ok(());
            }
            let observation = ExhaustionObservation {
                account_id: admission.account_id.clone(),
                bound: true,
                kind: ObservationKind::BrocaLimitExhausted {
                    limit_window: parsed.limit_window.clone(),
                    pool: parsed.pool.clone(),
                    requested_family: ctx.policy.family_for_model(&admission.model),
                    resets_at_ms: parsed.resets_at_ms,
                    resets_at_source: parsed.resets_at_source,
                    observed_at_ms: parsed.observed_at_ms.unwrap_or(now),
                },
            };
            apply_confirmation(
                tx,
                ctx,
                principal,
                admission,
                observation_id,
                &observation,
                latest_reading(roster_digests, admission),
                effects,
            )?;
        }
        _ => {}
    }
    Ok(())
}

/// A response from the real upstream (a direct response, an upstream frame an
/// HTTP relay carried, or the WebSocket `response_start` frame; never headers
/// a relay made up) whose own raw 5h or 7d utilization header reads 1.0 or
/// more confirms that window is exhausted, without a usage poll. Everything
/// else a header says is kept with the observation and changes nothing shared.
fn header_fences(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    parsed: &ObserveParams,
    admission: &AccountAdmissionRow,
    observation_id: &str,
    effects: &mut Vec<Value>,
) -> RouteStateResult<()> {
    let headers: BTreeMap<String, &String> = parsed
        .headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value))
        .collect();
    for (window, label) in [
        (GeneralWindow::FiveHour, "5h"),
        (GeneralWindow::SevenDay, "7d"),
    ] {
        let Some(raw) = headers.get(&format!("anthropic-ratelimit-unified-{label}-utilization"))
        else {
            continue;
        };
        let Some(frame) = parsed.upstream_origin else {
            effects.push(json!({
                "effect": "none",
                "window": wire(&window),
                "why": "header evidence without upstream_origin cannot confirm",
            }));
            continue;
        };
        let raw_utilization = raw.trim().parse::<f64>().unwrap_or(f64::NAN);
        let reset = headers
            .get(&format!("anthropic-ratelimit-unified-{label}-reset"))
            .map(|value| value.trim().parse::<f64>().unwrap_or(f64::NAN));
        let quota_window = header_window(window, raw_utilization, reset, ObservationSource::Header);
        let observed_at_ms = parsed.observed_at_ms.unwrap_or(ctx.now_ms);
        let observation = ExhaustionObservation {
            account_id: admission.account_id.clone(),
            bound: true,
            kind: ObservationKind::ServedHeader(WindowEvidence {
                scope: EvidenceScope::General(window),
                derivation: Derivation::Header {
                    frame,
                    raw_utilization: Some(raw_utilization).filter(|raw| raw.is_finite()),
                },
                carrier: Carrier::Direct,
                observed_at_ms,
                received_at_ms: ctx.now_ms,
                reset: quota_window.resets_at,
            }),
        };
        apply_confirmation(
            tx,
            ctx,
            principal,
            admission,
            observation_id,
            &observation,
            // A header names its window, so the block is never whole-account.
            None,
            effects,
        )?;
    }
    Ok(())
}

/// The admitted account's latest reading, as the observation found it.
fn latest_reading<'r>(
    roster_digests: &'r BTreeMap<String, ProviderRosterFacts>,
    admission: &AccountAdmissionRow,
) -> Option<&'r QuotaReading> {
    roster_digests
        .get(&admission.provider)
        .and_then(|facts| facts.readings.get(&admission.account_id))
}

#[allow(clippy::too_many_arguments)]
fn apply_confirmation(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    admission: &AccountAdmissionRow,
    observation_id: &str,
    observation: &ExhaustionObservation,
    latest_reading: Option<&QuotaReading>,
    effects: &mut Vec<Value>,
) -> RouteStateResult<()> {
    match confirm_exhaustion(observation) {
        ConfirmationOutcome::Confirmed { mut fence } => {
            // A runner clock ahead of routing cannot postpone both recovery
            // and the next probe. The original timestamp remains in the
            // append-only observation, separate from this operational fence.
            fence.observed_at_ms = fence.observed_at_ms.min(ctx.now_ms);
            let mut prior = fence.clone();
            prior.observed_at_ms = i64::MIN;
            if matches!(
                observation.kind,
                ObservationKind::BrocaLimitExhausted { .. }
            ) && block_end(
                &prior,
                latest_reading,
                ctx.now_ms,
                &ctx.provider_policy(&admission.provider),
            )
            .is_some()
            {
                tx.record_non_quota_route_fence(&prefrontal_routing_store::AccountRouteFence {
                    provider: admission.provider.clone(),
                    credential_id: admission.credential_id.clone(),
                    model: admission.model.clone(),
                    until_ms: [
                        match fence.reset {
                            ResetTime::At(at) if at > ctx.now_ms => Some(at),
                            _ => None,
                        },
                        fence.retry_hint_ms.filter(|at| *at > ctx.now_ms),
                    ]
                    .into_iter()
                    .flatten()
                    .chain(std::iter::once(ctx.now_ms.saturating_add(
                        prefrontal_routing_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS,
                    )))
                    .min()
                    .unwrap(),
                    decision_id: admission.decision_id.clone(),
                    recorded_at: ctx.now_ms,
                })?;
                effects
                    .push(json!({"effect":"fence","fence_id":new_id("acf"),"fence":wire(&fence)}));
                return Ok(());
            }
            if fence.pool.is_none() {
                // A whole-account block lifts only on a reading that reports
                // every pool the account's latest fresh reading reported.
                fence.recovery_set = recovery_set_from(latest_reading, ctx.now_ms, ctx.policy);
            }
            let fence_id = new_id("acf");
            tx.insert_pool_fence(&AccountPoolFenceRow {
                evidence: AccountFenceRow {
                    fence_id: fence_id.clone(),
                    provider: admission.provider.clone(),
                    account_id: fence.account_id.clone(),
                    scope_json: wire(&fence.pool).to_string(),
                    fence_json: wire(&fence).to_string(),
                    source: wire_str(&fence.source),
                    observed_at: fence.observed_at_ms,
                    principal: principal.to_string(),
                    admission_id: Some(admission.admission_id.clone()),
                    observation_id: Some(observation_id.to_string()),
                    created_at: ctx.now_ms,
                    cleared_at: None,
                    cleared_reason: None,
                },
                pool_key: fence.pool.clone(),
                recovery_set: fence.recovery_set.clone(),
            })?;
            effects.push(json!({ "effect": "fence", "fence_id": fence_id, "fence": wire(&fence) }));
        }
        other => effects.push(json!({ "effect": "not_confirmed", "evaluation": wire(&other) })),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// route.account_release
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct ReleaseParams {
    admission_id: String,
}

fn release(params: &Value, principal: &str, ctx: &Ctx<'_>) -> OpReply {
    let parsed: ReleaseParams = match serde_json::from_value(params.clone()) {
        Ok(parsed) => parsed,
        Err(error) => return invalid(format!("invalid route.account_release params: {error}")),
    };
    let now = ctx.now_ms;
    ctx.store
        .account_transaction(|tx| {
            // Only this principal's own admission can be released; any other
            // id changes nothing.
            let Some(row) = tx.admission(principal, &parsed.admission_id)? else {
                return Ok(OpReply::Result(json!({
                    "release": {
                        "admission_id": parsed.admission_id,
                        "state": null,
                        "released": false,
                        "lease_ended": false,
                    }
                })));
            };
            let released = tx.mark_admission_released(principal, &row.admission_id, now)?;
            // Release ends the conversation's lease; it never erases affinity
            // and never cancels a request in flight.
            // A completed or already released admission must not end a lease
            // that another admission may since have renewed on the same pin.
            let lease_ended = if matches!(row.state.as_str(), "finished" | "released") {
                false
            } else {
                tx.end_lease(&row.pin_id, now)?
            };
            let state = tx
                .admission(principal, &row.admission_id)?
                .map_or(row.state.clone(), |current| current.state);
            Ok(OpReply::Result(json!({
                "release": {
                    "admission_id": row.admission_id,
                    "state": state,
                    "released": released,
                    "lease_ended": lease_ended,
                }
            })))
        })
        .unwrap_or_else(store_error)
}

// ---------------------------------------------------------------------------
// Preflight request
// ---------------------------------------------------------------------------

/// A synthetic new cold session for `model`, decided by the worker-launch
/// preflight (`preflight_model`) and never recorded.
fn synthetic_request(provider: &str, model: &str, key: String) -> AccountRequest {
    AccountRequest {
        principal: PREFLIGHT_PRINCIPAL.to_string(),
        provider: provider.to_string(),
        model: model.to_string(),
        affinity_model: model.to_string(),
        conversation_key: key,
        conversation_key_source: ConversationKeySource::Caller,
        // Broca is the first caller account selection will serve, and its
        // frozen runs need the stricter spendable floor.
        caller: CallerKind::Broca,
        via_safety_recovery: false,
        initial_input_bytes: 0,
        initial_input_estimated: true,
        candidates: None,
        requested_account: None,
        preferred_existing_account: None,
        hold_wait_max_s: DEFAULT_HOLD_WAIT_S,
        pool: request_pool(None, provider, model),
    }
}

pub fn now_ms() -> i64 {
    crate::now_ms() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The model-specific quota pools the config converter writes.
    fn shipped_model_pools() -> Vec<prefrontal_routing_core::quota_extract::ModelPool> {
        serde_json::from_value(crate::config_convert::defaults()["quota_model_pools"].clone())
            .expect("converter quota model pools")
    }

    /// Default account tuning with the converter's model-specific quota pools.
    fn default_account_policy() -> prefrontal_routing_core::accounts::AllocatorPolicy {
        prefrontal_routing_core::accounts::AllocatorPolicy {
            model_pools: shipped_model_pools(),
            ..Default::default()
        }
    }

    use crate::account_quota::{apply_provider_reply, apply_roster_listing};
    use cortexkit_claustrum_client::{ListedCredential, ListedGrant, ScopedCredentialListing};
    use prefrontal_routing_store::dev_descriptor;
    use std::sync::{Arc, Mutex};

    /// The fixed test clock, 2026-09-29T12:00:00Z.
    const NOW: i64 = 1_790_683_200_000;
    const ACCOUNT_A: &str = "00000000-0000-4000-8000-00000000000a";
    const ACCOUNT_B: &str = "00000000-0000-4000-8000-00000000000b";
    /// Insula reports it; the vault does not list it.
    const INSULA_ONLY: &str = "00000000-0000-4000-8000-00000000000c";
    /// The vault lists it; Insula has never reported it.
    const VAULT_ONLY: &str = "00000000-0000-4000-8000-00000000000d";
    const BROCA: &str = "reserved:broca";

    static NEXT_RIG: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        dir: std::path::PathBuf,
        store: Option<SqliteRouteStateStore>,
        accounts: SharedAccountQuota,
        policy: AllocatorPolicy,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.store = None;
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn open_store(dir: &std::path::Path) -> SqliteRouteStateStore {
        let path = dir.join("store.db").to_string_lossy().into_owned();
        SqliteRouteStateStore::open(&dev_descriptor("prefrontal-routing", path))
            .expect("open store")
    }

    fn claude_entry(account: &str) -> Value {
        json!({
            "provider": "claude",
            "apiProvider": "anthropic",
            "account": account,
            "accountInfo": { "email": format!("{account}@example.test") },
            "source": "vault",
            "fetchedAt": "2026-09-29T11:59:00Z",
            "usage": {
                "primary": { "usedPercent": 10.0, "windowMinutes": 300, "resetsAt": "2026-09-29T14:00:00Z" },
                "secondary": { "usedPercent": 40.0, "windowMinutes": 10080 },
                "extraRateWindows": [
                    { "id": "Fable", "title": "7 Day (Fable)", "window": { "usedPercent": 5.0, "windowMinutes": 10080 } }
                ]
            }
        })
    }

    #[test]
    fn live_display_name_scope_adds_to_weekly_only_for_fable() {
        use prefrontal_routing_core::accounts::eligibility::evaluate_quota;
        let mut entry = claude_entry("live-scope");
        entry["usage"]["tertiary"] = Value::Null;
        let policy = default_account_policy();
        let evaluate = |entry: &Value, model| {
            let reading = quota_reading_for_model(entry, "live-scope", None, &[]).unwrap();
            evaluate_quota(Some(&reading), model, CallerKind::Plugin, NOW, &policy)
        };
        for model in [
            "claude-fable-5",
            "claude-fable-5[1m]",
            "claude-fable-5-1",
            "claude-fable-5-1[1m]",
            "claude-opus-5-5",
            "claude-opus-5-5[1m]",
            "claude-mythos-5-1",
            "claude-mythos-5-1[1m]",
        ] {
            let eval = evaluate(&entry, model);
            assert!(eval.eligible(), "{model}: {eval:?}");
            assert!(eval
                .window_weights
                .iter()
                .any(|window| window.window == ApplicableWindow::SevenDay));
            let scoped: Vec<_> = eval
                .window_weights
                .iter()
                .filter(|window| window.window == ApplicableWindow::FamilyScope)
                .collect();
            let fable = model.starts_with("claude-fable-");
            assert_eq!(scoped.len(), usize::from(fable), "{model}");
            assert_eq!(
                eval.window_weights.len(),
                if fable { 3 } else { 2 },
                "{model}"
            );

            let mut exhausted = entry.clone();
            exhausted["usage"]["extraRateWindows"][0]["window"]["usedPercent"] = json!(100.0);
            assert_eq!(evaluate(&exhausted, model).eligible(), !fable, "{model}");
            exhausted["usage"]["secondary"]["usedPercent"] = json!(100.0);
            assert!(
                !evaluate(&exhausted, model).eligible(),
                "{model}: weekly must always apply"
            );
        }
    }

    #[test]
    fn future_model_id_scope_applies_without_a_family_table() {
        use prefrontal_routing_core::accounts::eligibility::evaluate_quota;
        let mut entry = claude_entry("future-scope");
        entry["usage"]["extraRateWindows"][0]["id"] = json!("claude-fable-5");
        entry["usage"]["extraRateWindows"][0]["window"]["usedPercent"] = json!(100.0);
        let reading = quota_reading_for_model(&entry, "future-scope", None, &[]).unwrap();
        let eval = evaluate_quota(
            Some(&reading),
            "claude-fable-5-1[1m]",
            CallerKind::Plugin,
            NOW,
            &default_account_policy(),
        );
        assert!(
            eval.reserve.iter().any(|reason| matches!(
                reason,
                prefrontal_routing_core::accounts::ExclusionReason::ScopeExhausted { .. }
            )),
            "future id must gate the pick: {eval:?}"
        );
    }

    #[test]
    fn literal_null_scope_id_never_uses_the_title_and_warns_once_per_account() {
        use prefrontal_routing_core::accounts::eligibility::evaluate_quota;
        let account = "null-scope-warning-account";
        let mut entry = claude_entry(account);
        entry["usage"]["extraRateWindows"][0]["id"] = Value::Null;
        entry["usage"]["extraRateWindows"][0]["window"]["usedPercent"] = json!(100.0);
        for title in ["Fable", "7 Day (Fable)"] {
            entry["usage"]["extraRateWindows"][0]["title"] = json!(title);
            let reading = quota_reading_for_model(&entry, account, None, &[]).unwrap();
            let eval = evaluate_quota(
                Some(&reading),
                "claude-fable-5",
                CallerKind::Plugin,
                NOW,
                &default_account_policy(),
            );
            assert!(eval.eligible(), "{eval:?}");
            assert_eq!(eval.window_weights.len(), 2);
        }
        assert!(!claim_unattributable_scope_warning(account));
        let other = "another-null-scope-warning-account";
        assert!(claim_unattributable_scope_warning(other));
        assert!(!claim_unattributable_scope_warning(other));
        entry["usage"]["extraRateWindows"][0]["id"] = json!("unrelated");
        let reading = quota_reading_for_model(&entry, account, None, &[]).unwrap();
        assert!(evaluate_quota(
            Some(&reading),
            "claude-fable-5",
            CallerKind::Plugin,
            NOW,
            &default_account_policy()
        )
        .eligible());
    }

    #[test]
    fn last_good_is_admissible_until_thirty_minutes_but_not_after_a_reset() {
        use prefrontal_routing_core::accounts::eligibility::evaluate_quota;
        use prefrontal_routing_core::accounts::ExclusionReason;
        let mut state = AccountQuotaState::default();
        let mut good = claude_entry(ACCOUNT_A);
        good["fetchedAt"] = json!("2026-09-29T12:00:00Z");
        let body = json!({"result": [good], "completeProviders": ["claude"]});
        apply_provider_reply(
            &mut state,
            "claude",
            &serde_json::to_vec(&body).unwrap(),
            NOW,
        )
        .unwrap();
        let body = json!({"result": [{"provider": "claude", "apiProvider": "anthropic", "account": ACCOUNT_A,
            "errorClass": "upstream_failed", "error": "restart"}], "completeProviders": ["claude"]});
        apply_provider_reply(
            &mut state,
            "claude",
            &serde_json::to_vec(&body).unwrap(),
            NOW + 27 * 60_000,
        )
        .unwrap();
        let entry = &state.readings.readings.values().next().unwrap().entry;
        let reading = quota_reading_for_model(entry, "acct-a", None, &[]).unwrap();
        let evaluate = |reading: &QuotaReading, now| {
            evaluate_quota(
                Some(reading),
                "claude-opus-5-5",
                CallerKind::Plugin,
                now,
                &AllocatorPolicy::default(),
            )
        };
        let kept = evaluate(&reading, NOW + 27 * 60_000);
        assert!(kept.eligible(), "{:?}", kept.unknown);
        assert_eq!(reading.five_hour.as_ref().unwrap().used_percent, 10.0);
        let expired = evaluate(&reading, NOW + 30 * 60_000);
        assert!(expired
            .unknown
            .iter()
            .any(|reason| matches!(reason, ExclusionReason::AgeExcluded { .. })));

        let mut past_reset = entry.clone();
        past_reset["usage"]["primary"]["resetsAt"] = json!("2026-09-29T12:01:00Z");
        let reading = quota_reading_for_model(&past_reset, "acct-a", None, &[]).unwrap();
        let expired_window = evaluate(&reading, NOW + 2 * 60_000);
        assert!(!expired_window.eligible());
        assert!(expired_window
            .unknown
            .iter()
            .any(|reason| matches!(reason, ExclusionReason::WindowResetElapsed { .. })));
    }

    fn listing() -> ScopedCredentialListing {
        let row = |id: &str, account: &str| ListedCredential {
            id: id.into(),
            categories: vec!["anthropic-native".into()],
            credential_type: "oauth".into(),
            refresh_adapter: Some("anthropic".into()),
            state: "active".into(),
            record_version: 1,
            operations: vec!["list".into()],
            account_id: Some(account.into()),
            email: Some(format!("{account}@example.test")),
            ..Default::default()
        };
        ScopedCredentialListing {
            credentials: vec![
                row("oauth:anthropic", ACCOUNT_A),
                row("oauth:anthropic:ysample", ACCOUNT_B),
                row("oauth:anthropic:dsample", VAULT_ONLY),
            ],
            grant_tuples: vec![ListedGrant {
                selector_kind: "category".into(),
                selector: "anthropic-native".into(),
                operation: "list".into(),
            }],
            view: "vault-view-1".into(),
            ..Default::default()
        }
    }

    fn fixture(label: &str) -> Fixture {
        let id = NEXT_RIG.fetch_add(1, Ordering::Relaxed);
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "prefrontal-account-selection-module-{label}-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut state = AccountQuotaState::default();
        // Listed 20 minutes ago, so the vault-only identity is overdue for its
        // first reading.
        apply_roster_listing(&mut state, &listing(), NOW - 20 * 60_000);
        let reply = serde_json::to_vec(&json!({
            "result": [claude_entry(ACCOUNT_A), claude_entry(ACCOUNT_B), claude_entry(INSULA_ONLY)],
            "completeProviders": ["claude"],
        }))
        .unwrap();
        apply_provider_reply(&mut state, "claude", &reply, NOW - 30_000).unwrap();
        Fixture {
            store: Some(open_store(&dir)),
            dir,
            accounts: Arc::new(Mutex::new(state)),
            policy: default_account_policy(),
        }
    }

    impl Fixture {
        fn store(&self) -> &SqliteRouteStateStore {
            self.store.as_ref().unwrap()
        }

        fn reopen(&mut self) {
            self.store = None;
            self.store = Some(open_store(&self.dir));
        }

        fn ctx(&self, enabled: bool, now_ms: i64) -> Ctx<'_> {
            Ctx {
                store: self.store(),
                accounts: Some(&self.accounts),
                flags: AccountSelectionFlags { enabled },
                policy: &self.policy,
                now_ms,
                classes: None,
            }
        }

        fn call(&self, ctx: &Ctx<'_>, method: &str, principal: &str, params: Value) -> OpReply {
            let body = serde_json::to_vec(&json!({ "method": method, "params": params })).unwrap();
            handle(method, &body, principal, ctx)
        }

        fn serve_rows(&self) -> Vec<AccountDecisionRecord> {
            self.store()
                .account_decisions(DECISION_MODE_SERVE, 1000)
                .unwrap()
        }
    }

    fn admit_params(admission_id: &str, key: &str, runner: &str) -> Value {
        json!({
            "provider": "anthropic",
            "model": "claude-opus-5-5",
            "affinity_model": "claude-opus-5-5",
            "conversation_key": key,
            "caller": { "runner": runner, "harness": "prefrontal-core" },
            "admission_id": admission_id,
            "mode": "admit",
            "expected_pin": null,
            "initial_input_bytes": 12000,
            "initial_input_estimated": false,
            "model_decision_id": null,
            "candidates": null,
            "requested_account": null,
            "hold_wait_max_s": 900
        })
    }

    fn result(reply: OpReply) -> Value {
        match reply {
            OpReply::Result(value) => value,
            OpReply::Error { code, message, .. } => {
                panic!("expected a result, got {code}: {message}")
            }
        }
    }

    #[test]
    fn a_probe_claimed_after_roster_preparation_returns_its_fence_not_missing_candidates() {
        let fixture = fixture("claim-after-roster");
        let ctx = fixture.ctx(true, NOW);
        let mut roster = roster_for_model(&ctx, "anthropic", Some("claude-opus-5-5"));
        let accounts = &mut roster.roster.as_mut().unwrap().accounts;
        accounts.retain(|account| account.account_id == ACCOUNT_A);
        assert_eq!(accounts.len(), 1);
        accounts[0].quota = None;
        fixture
            .store()
            .account_transaction(|tx| {
                tx.record_non_quota_route_fence(&prefrontal_routing_store::AccountRouteFence {
                    provider: "anthropic".into(),
                    credential_id: "oauth:anthropic".into(),
                    model: "claude-opus-5-5".into(),
                    until_ms: NOW,
                    recorded_at: NOW - 100,
                    decision_id: "refusal".into(),
                })
            })
            .unwrap();
        let due = fixture.store().fence_rechecks().unwrap();
        assert_eq!(due.len(), 1);
        assert!(fixture
            .store()
            .account_transaction(|tx| tx.grant_fence_rechecks(&due, NOW, Some("winning-decision")))
            .unwrap());
        let mut request = admit_params("loser", "claim-after-roster", "broca");
        request["candidates"] = json!({"view_digest":"same-runner-list","accounts":[{"account_id":ACCOUNT_A,"credential_id":"oauth:anthropic"}]});
        let parsed = parse_account_for(&request).unwrap();
        let reply = fixture
            .store()
            .account_transaction(|tx| {
                serve(
                    tx,
                    &ctx,
                    "runner",
                    &parsed,
                    "anthropic",
                    &roster,
                    "loser-fingerprint",
                )
            })
            .unwrap();
        match reply {
            OpReply::Error { code, detail, .. } => {
                assert_eq!(
                    code, "account_pool_exhausted",
                    "a lost claim must retain its fence cause"
                );
                assert_eq!(detail.unwrap()["reset_at_ms"], NOW + 3_600_000);
            }
            other => panic!("a second probe was admitted: {other:?}"),
        }
    }

    #[test]
    fn account_exhaustion_fences_only_its_account_and_opus_seats_with_a_93_percent_sibling() {
        use prefrontal_routing_core::{RouteStateStore, RouteStateUpdate, RouteWallClass};
        let fixture = fixture("account-wall-versus-seat");
        let ctx = fixture.ctx(true, NOW);
        let first = result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("wall-1", "broca:wall-1", "broca"),
        ));
        let account = first["account"]["account_id"].as_str().unwrap();
        let sibling = if account == ACCOUNT_A {
            ACCOUNT_B
        } else {
            ACCOUNT_A
        };
        for reading in fixture
            .accounts
            .lock()
            .unwrap()
            .readings
            .readings
            .values_mut()
            .filter(|reading| reading.account_id == account)
        {
            reading.entry["usage"]["secondary"]["usedPercent"] = json!(100.0);
        }
        let observed = result(fixture.call(&ctx,ACCOUNT_OBSERVE_OP,BROCA,json!({
            "event_id":"exhausted-account","caller":{"runner":"broca"},"kind":"account_limit_exhausted",
            "admission_id":"wall-1","limit_window":"seven_day","resets_at_ms":NOW+77*3_600_000,
            "resets_at_source":"header","observed_at_ms":NOW+1000,
        })));
        assert_eq!(
            observed["observation"]["effects"][0]["fence"]["account_id"],
            account
        );
        assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 1);

        // A caller retaining the old model-cooldown protocol cannot fan this
        // account's dated exhaustion out to the whole Opus pool.
        let route = "anthropic/claude-opus-5-5";
        fixture
            .store()
            .update_route_state(
                route,
                route,
                &RouteStateUpdate {
                    cooldown_until: Some((NOW + 77 * 3_600_000) as f64),
                    quota_exhausted_until: Some((NOW + 77 * 3_600_000) as f64),
                    wall_class: Some(RouteWallClass::Quota),
                    last_error: Some(
                        json!({"provider_code":"account_limit_exhausted","account_ref":account})
                            .to_string(),
                    ),
                    ..Default::default()
                },
            )
            .unwrap();
        let model_rows: i64 = rusqlite::Connection::open(fixture.dir.join("store.db"))
            .unwrap()
            .query_row(
                "SELECT count(*) FROM model_route_state WHERE route_key=?1",
                [route],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            model_rows, 0,
            "account evidence lives in account_fence, not model_route_state"
        );

        let mut exhausted = claude_entry(account);
        exhausted["usage"]["secondary"]["usedPercent"] = json!(100);
        exhausted["usage"]["secondary"]["resetsAt"] = json!("2026-10-02T17:00:00Z");
        let mut healthy = claude_entry(sibling);
        healthy["usage"]["secondary"]["usedPercent"] = json!(93);
        healthy["usage"]["secondary"]["resetsAt"] = json!("2026-10-03T12:00:00Z");
        apply_provider_reply(&mut fixture.accounts.lock().unwrap(),"claude",
            &serde_json::to_vec(&json!({"result":[exhausted.clone(),healthy.clone()],"completeProviders":["claude"]})).unwrap(),NOW+1000).unwrap();
        let next = result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("wall-2", "broca:wall-2", "broca"),
        ));
        assert_eq!(
            next["account"]["account_id"], sibling,
            "93% still leaves spendable quota for Broca"
        );

        // Production seating builds the same account-wise overlay before shared
        // panel admission. One 100% account cannot wall the 93% sibling.
        let mut fact = prefrontal_routing_core::model_facts_seed()[0].clone();
        fact.provider_id = "anthropic".into();
        fact.model_id = "claude-opus-5-5".into();
        fact.variant = None;
        let facts = vec![fact];
        let mut snapshot = fixture.store().build_snapshot(NOW as f64).unwrap();
        let windows: Vec<prefrontal_routing_core::ProviderUsage> =
            serde_json::from_value(json!([exhausted, healthy])).unwrap();
        prefrontal_routing_core::merge_quota_overlay(
            &mut snapshot,
            &windows,
            &facts,
            &shipped_model_pools(),
        );
        assert_eq!(snapshot.by_capacity[route].quota_exhausted_until, None);
        let seat = |snapshot| {
            prefrontal_routing_core::select_panel(prefrontal_routing_core::SelectPanelInput {
                package_tokens: None,
                panel_spec: serde_json::from_value(
                    json!({"size":1,"iqFloor":90,"class":"SPEC","diversity":"none"}),
                )
                .unwrap(),
                selection: prefrontal_routing_core::PanelSelection::Roster,
                roster: serde_json::from_value(json!([{"model":route}])).unwrap(),
                facts: facts.clone(),
                runtime_snapshot: Some(snapshot),
                exclude_route_keys: vec![],
                operator_exclude_keys: vec![],
                required_capabilities: None,
                available_models: [route.to_string()].into_iter().collect(),
                availability_substrate: prefrontal_routing_core::AvailabilitySubstrate::Broca,
                availability_label: crate::legacy_wire::availability_label(
                    prefrontal_routing_core::AvailabilitySubstrate::Broca,
                )
                .to_string(),
                is_subscription_route: None,
            })
            .unwrap()
        };
        assert_eq!(seat(snapshot.clone()).members[0].route_key, route);
        // The all-exhausted control proves seating still honors a real pool wall.
        healthy["usage"]["secondary"]["usedPercent"] = json!(100);
        let windows: Vec<prefrontal_routing_core::ProviderUsage> =
            serde_json::from_value(json!([exhausted, healthy])).unwrap();
        prefrontal_routing_core::merge_quota_overlay(
            &mut snapshot,
            &windows,
            &facts,
            &shipped_model_pools(),
        );
        assert!(seat(snapshot).members.is_empty());
    }

    fn provider_fixture(label: &str, adapter: &str, entries: Vec<Value>) -> Fixture {
        let rig = fixture(label);
        let mut state = AccountQuotaState::default();
        let mut vault = listing();
        vault.credentials.truncate(2);
        for row in &mut vault.credentials {
            row.refresh_adapter = Some(adapter.into());
            row.id = format!("oauth:{adapter}:{}", row.account_id.as_deref().unwrap());
        }
        apply_roster_listing(&mut state, &vault, NOW);
        account_quota::apply_usage_reply(
            &mut state,
            &serde_json::to_vec(&json!({"result": entries})).unwrap(),
            NOW,
        )
        .unwrap();
        *rig.accounts.lock().unwrap() = state;
        rig
    }

    fn reported_entry(
        provider: &str,
        vendor: Value,
        account: &str,
        kind: &str,
        minutes: i64,
        used: f64,
    ) -> Value {
        json!({
            "provider": provider, "apiProvider": vendor, "account": account,
            "source": "oauth", "fetchedAt": "2026-09-29T11:59:00Z",
            "usage": {"primary": {"windowKind": kind, "windowMinutes": minutes, "usedPercent": used}}
        })
    }

    #[test]
    fn codex_vendor_joins_two_openai_accounts_and_selects_headroom() {
        let rig = provider_fixture(
            "codex",
            "openai",
            vec![
                reported_entry("codex", json!("openai"), ACCOUNT_A, "weekly", 10080, 100.0),
                reported_entry("codex", json!("openai"), ACCOUNT_B, "weekly", 10080, 10.0),
            ],
        );
        let built = allocator_roster(&rig.accounts.lock().unwrap(), "openai", NOW);
        assert_eq!(built.roster.unwrap().accounts.len(), 2);
        let mut params = admit_params("codex-admit", "codex-conversation", "broca");
        params["provider"] = json!("openai");
        params["model"] = json!("gpt-6");
        params["affinity_model"] = json!("gpt-6");
        let admitted = result(rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params));
        assert_eq!(admitted["account"]["account_id"], ACCOUNT_B);
        assert_eq!(admitted["account"]["provider"], "openai");
        assert_eq!(admitted["account"]["refresh_adapter"], "openai");
    }

    #[test]
    fn antigravity_vendor_and_model_pool_select_the_joined_account() {
        // This positive vector requires a published vendor. The live producer
        // currently omits it; the live-shape vector below pins that refusal.
        let entry = |account: &str, used: f64| {
            json!({
                "provider": "antigravity", "apiProvider": "google", "account": account,
                "source": "oauth", "fetchedAt": "2026-09-29T11:59:00Z",
                "usage": {"extraRateWindows": [
                    {"id": "gemini-5h", "windowKind": "five_hour", "window": {"usedPercent": used, "windowMinutes": 300}},
                    {"id": "3p-weekly", "windowKind": "weekly", "window": {"usedPercent": 100.0, "windowMinutes": 10080}}
                ]}
            })
        };
        let rig = provider_fixture(
            "antigravity",
            "antigravity",
            vec![entry(ACCOUNT_A, 100.0), entry(ACCOUNT_B, 10.0)],
        );
        let state = rig.accounts.lock().unwrap();
        let report = account_quota::build_report(
            &state.readings,
            state.roster.as_ref(),
            &state.roster_poll,
            NOW,
        );
        assert_eq!(report.accounts.len(), 2);
        assert_eq!(
            report.accounts[0].routing_provider.as_deref(),
            Some("google")
        );
        assert!(report
            .accounts
            .iter()
            .all(|account| account.vault.joined && account.cold_input_usable));
        assert!(report.alarms.is_empty());
        drop(state);
        let mut params = admit_params("ag-admit", "ag-conversation", "broca");
        params["provider"] = json!("google");
        params["model"] = json!("antigravity-gemini-3.8-flash");
        params["affinity_model"] = params["model"].clone();
        let admitted = result(rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params));
        assert_eq!(admitted["account"]["account_id"], ACCOUNT_B);
        assert_eq!(admitted["account"]["refresh_adapter"], "antigravity");
    }

    #[test]
    fn antigravity_pro_argon_weekly_limit_gates_argon_only() {
        // A paid Pro account publishes its separate Gemini 4 Argon weekly limit
        // as `gemini-argon-wtu-weekly`; Ultra and free accounts publish none.
        let entry = |account: &str, gemini_weekly: f64, argon: Option<f64>| {
            let mut windows = vec![
                json!({"id": "gemini-5h", "windowKind": "five_hour", "window": {"usedPercent": 10.0, "windowMinutes": 300}}),
                json!({"id": "gemini-weekly", "windowKind": "weekly", "window": {"usedPercent": gemini_weekly, "windowMinutes": 10080}}),
            ];
            if let Some(used) = argon {
                windows.push(json!({"id": "gemini-argon-wtu-weekly", "windowKind": "weekly", "window": {"usedPercent": used, "windowMinutes": 10080}}));
            }
            json!({
                "provider": "antigravity", "apiProvider": "google", "account": account,
                "source": "oauth", "fetchedAt": "2026-09-29T11:59:00Z",
                "usage": {"extraRateWindows": windows}
            })
        };
        let admit = |rig: &Fixture, id: &str, model: &str| {
            let mut params = admit_params(id, id, "broca");
            params["provider"] = json!("google");
            params["model"] = json!(model);
            params["affinity_model"] = json!(model);
            rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params)
        };
        const ARGON: &str = "antigravity-gemini-4-argon";
        const GEMINI: &str = "antigravity-gemini-3.8-flash";

        // Both accounts are Pro with the Argon limit spent: Argon is refused,
        // while another Gemini route is still admitted on the same accounts.
        let pro = provider_fixture(
            "argon-pro",
            "antigravity",
            vec![
                entry(ACCOUNT_A, 20.0, Some(100.0)),
                entry(ACCOUNT_B, 30.0, Some(100.0)),
            ],
        );
        let refused = admit(&pro, "argon-pro", ARGON);
        assert!(
            refused.code().is_some(),
            "a spent Argon weekly limit refuses Argon: {refused:?}"
        );
        assert!(preflight_model(&pro.ctx(true, NOW), "google", ARGON).is_err());
        let admitted = result(admit(&pro, "gemini-pro", GEMINI));
        assert_eq!(admitted["account"]["account_id"], ACCOUNT_A);
        let state = pro.accounts.lock().unwrap();
        let report = account_quota::build_report(
            &state.readings,
            state.roster.as_ref(),
            &state.roster_poll,
            NOW,
        );
        assert!(
            report.alarms.is_empty(),
            "the Argon window is a known pool, not an unmapped scope: {:?}",
            report.alarms
        );
        drop(state);

        // Account A has no Argon window (Ultra): its Gemini windows alone admit
        // Argon, beside a Pro account whose Argon limit is spent.
        let mixed = provider_fixture(
            "argon-mixed",
            "antigravity",
            vec![
                entry(ACCOUNT_A, 20.0, None),
                entry(ACCOUNT_B, 10.0, Some(100.0)),
            ],
        );
        let admitted = result(admit(&mixed, "argon-mixed", ARGON));
        assert_eq!(admitted["account"]["account_id"], ACCOUNT_A);

        // The same account's Gemini windows still gate Argon: with its Gemini
        // weekly pool spent, Argon has no account left.
        let ultra_spent = provider_fixture(
            "argon-ultra-spent",
            "antigravity",
            vec![
                entry(ACCOUNT_A, 100.0, None),
                entry(ACCOUNT_B, 10.0, Some(100.0)),
            ],
        );
        assert!(admit(&ultra_spent, "argon-ultra-spent", ARGON)
            .code()
            .is_some());
    }

    #[test]
    fn vendorless_reading_is_unmapped_and_never_borrows_a_siblings_vendor() {
        let mut missing = reported_entry("codex", Value::Null, ACCOUNT_A, "weekly", 10080, 0.0);
        missing.as_object_mut().unwrap().remove("apiProvider");
        let rig = provider_fixture(
            "vendorless",
            "openai",
            vec![
                missing,
                reported_entry("codex", json!("openai"), ACCOUNT_B, "weekly", 10080, 10.0),
            ],
        );
        let state = rig.accounts.lock().unwrap();
        let report = account_quota::build_report(
            &state.readings,
            state.roster.as_ref(),
            &state.roster_poll,
            NOW,
        );
        let unmapped = report
            .accounts
            .iter()
            .find(|account| account.account_id == ACCOUNT_A)
            .unwrap();
        assert_eq!(unmapped.routing_provider, None);
        assert!(!unmapped.cold_input_usable);
        assert!(report
            .information
            .iter()
            .any(|alarm| alarm.contains("unmapped") && alarm.contains(ACCOUNT_A)));
        assert!(report.alarms.is_empty());
        let roster = allocator_roster(&state, "openai", NOW).roster.unwrap();
        assert_eq!(roster.accounts.len(), 1);
        assert_eq!(roster.accounts[0].account_id, ACCOUNT_B);
        drop(state);
        let mut params = admit_params("vendorless-admit", "vendorless-conversation", "broca");
        params["provider"] = json!("openai");
        params["model"] = json!("gpt-6");
        params["affinity_model"] = json!("gpt-6");
        assert_eq!(
            result(rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params))["account"]
                ["account_id"],
            ACCOUNT_B
        );
    }

    #[test]
    fn arbitrary_vendor_uses_monthly_and_session_windows_and_unknown_vendor_is_typed() {
        let mut spent = reported_entry(
            "new-feed",
            json!("new-vendor"),
            ACCOUNT_A,
            "monthly",
            43200,
            100.0,
        );
        spent["usage"]["secondary"] =
            json!({"windowKind": "session", "windowMinutes": 60, "usedPercent": 0.0});
        let rig = provider_fixture(
            "new-vendor",
            "unrelated-adapter",
            vec![
                spent,
                reported_entry(
                    "new-feed",
                    json!("new-vendor"),
                    ACCOUNT_B,
                    "session",
                    60,
                    10.0,
                ),
            ],
        );
        let mut params = admit_params("new-admit", "new-conversation", "broca");
        params["provider"] = json!("new-vendor");
        params["model"] = json!("new-model");
        params["affinity_model"] = json!("new-model");
        let admitted = result(rig.call(
            &rig.ctx(true, NOW),
            "route.account_for",
            BROCA,
            params.clone(),
        ));
        assert_eq!(admitted["account"]["account_id"], ACCOUNT_B);
        assert_eq!(admitted["account"]["refresh_adapter"], "unrelated-adapter");
        params["provider"] = json!("untracked");
        assert_eq!(
            rig.call(
                &rig.ctx(true, NOW),
                "route.account_for",
                BROCA,
                params.clone()
            )
            .code(),
            Some("admission_conflict")
        );
        params["admission_id"] = json!("unknown-provider-admit");
        // No credential in the vault serves an untracked provider: the
        // runner needs a login it does not have (`auth_required`, untimed),
        // not a transient retry.
        assert_eq!(
            rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params)
                .code(),
            Some("auth_required")
        );
    }

    #[test]
    fn account_id_join_does_not_require_one_vendor_per_refresh_adapter() {
        let rig = provider_fixture(
            "shared-adapter",
            "shared-adapter",
            vec![
                reported_entry("feed-a", json!("vendor-a"), ACCOUNT_A, "session", 60, 10.0),
                reported_entry(
                    "feed-b",
                    json!("vendor-b"),
                    ACCOUNT_B,
                    "monthly",
                    43200,
                    10.0,
                ),
            ],
        );
        let state = rig.accounts.lock().unwrap();
        let a = allocator_roster(&state, "vendor-a", NOW).roster.unwrap();
        let b = allocator_roster(&state, "vendor-b", NOW).roster.unwrap();
        assert_eq!(a.accounts.len(), 1);
        assert_eq!(a.accounts[0].account_id, ACCOUNT_A);
        assert_eq!(b.accounts.len(), 1);
        assert_eq!(b.accounts[0].account_id, ACCOUNT_B);
    }

    /// Sanitized from the 2026-10-03 usage.get capture: both Antigravity lanes
    /// omit apiProvider; the vault lane reports 5h + weekly pools while the
    /// OAuth lane reports weekly pools only. Identity and dates are test-local.
    fn live_antigravity_shapes() -> Vec<Value> {
        vec![
            json!({
                "provider": "antigravity", "apiProvider": null, "account": ACCOUNT_A,
                "source": "vault", "fetchedAt": "2026-09-29T11:59:00Z",
                "usage": {"extraRateWindows": [
                    {"id": "gemini-5h", "title": "Gemini Models 5h", "window": {"usedPercent": 3.85, "windowKind": "five_hour", "windowMinutes": 300}},
                    {"id": "gemini-weekly", "title": "Gemini Models weekly", "window": {"usedPercent": 52.81, "windowKind": "weekly", "windowMinutes": 10080}},
                    {"id": "3p-5h", "title": "Claude and GPT models 5h", "window": {"usedPercent": 0.0, "windowKind": "five_hour", "windowMinutes": 300}},
                    {"id": "3p-weekly", "title": "Claude and GPT models weekly", "window": {"usedPercent": 0.0, "windowKind": "weekly", "windowMinutes": 10080}}
                ]}
            }),
            json!({
                "provider": "antigravity", "apiProvider": null, "account": ACCOUNT_B,
                "source": "oauth", "fetchedAt": "2026-09-29T11:59:00Z",
                "usage": {"extraRateWindows": [
                    {"id": "gemini-weekly", "title": "Gemini Models weekly", "window": {"usedPercent": 100.0, "windowKind": "weekly", "windowMinutes": 10080}},
                    {"id": "3p-weekly", "title": "Claude and GPT models weekly", "window": {"usedPercent": 0.0, "windowKind": "weekly", "windowMinutes": 10080}}
                ]}
            }),
        ]
    }

    #[test]
    fn live_google_shapes_keep_vendorless_antigravity_informational_and_unselectable() {
        let mut entries = live_antigravity_shapes();
        // The live Gemini CLI row has a vendor but no account or usage because
        // its credential is absent. It is not a tracked Google account.
        entries.push(json!({"provider": "gemini", "apiProvider": "google", "errorClass": "credential_absent", "usage": null}));
        let rig = provider_fixture("live-google", "antigravity", entries);
        let state = rig.accounts.lock().unwrap();
        let report = account_quota::build_report(
            &state.readings,
            state.roster.as_ref(),
            &state.roster_poll,
            NOW,
        );
        assert_eq!(report.accounts.len(), 2);
        assert!(report
            .accounts
            .iter()
            .all(|account| account.routing_provider.is_none()
                && account.vault.joined
                && !account.cold_input_usable));
        assert_eq!(report.information.len(), 2);
        assert!(report
            .information
            .iter()
            .all(|information| information.contains("unmapped entry: no apiProvider")));
        assert!(
            report.alarms.is_empty(),
            "a missing producer vendor must not page through a roster gap: {:?}",
            report.alarms
        );
        assert!(report.gaps.insula_only.is_empty());
        assert!(report.gaps.vault_only.is_empty());
        drop(state);
        let mut params = admit_params("live-google", "live-google", "broca");
        params["provider"] = json!("google");
        params["model"] = json!("antigravity-gemini-3.8-flash");
        params["affinity_model"] = params["model"].clone();
        // The only google credential is absent and the vendorless lanes are
        // unmapped: no credential of the right kind serves this model, so
        // `auth_required` (untimed), not a transient retry.
        assert_eq!(
            rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params)
                .code(),
            Some("auth_required")
        );
    }

    #[test]
    fn gemini_cli_account_cannot_serve_antigravity_even_with_sticky_affinity() {
        let mut entries = live_antigravity_shapes();
        // The captured Gemini row is credential_absent and unkeyed. Model a
        // future authorized CLI account with headroom to exercise the vendor
        // collision, rather than passing only because today's account is absent.
        let mut gemini =
            reported_entry("gemini", json!("google"), INSULA_ONLY, "session", 1440, 0.0);
        gemini["usage"]["extraRateWindows"] = json!([{"id": "gemini-2.5-flash", "title": "gemini-2.5-flash", "window": {"usedPercent": 0.0, "windowMinutes": 1440}}]);
        entries.push(gemini);
        let rig = provider_fixture("gemini-cli-collision", "antigravity", entries);
        let mut vault = listing();
        vault.credentials.truncate(1);
        let row = &mut vault.credentials[0];
        row.account_id = Some(INSULA_ONLY.into());
        row.refresh_adapter = Some("gemini-cli".into());
        row.id = "oauth:gemini-cli".into();
        let gemini_roster = account_quota::build_roster(&vault, None, NOW);
        rig.accounts
            .lock()
            .unwrap()
            .roster
            .as_mut()
            .unwrap()
            .identities
            .extend(gemini_roster.identities);
        let ctx = rig.ctx(true, NOW);
        let mut params = admit_params("gemini-cli-initial", "google-product-collision", "broca");
        params["provider"] = json!("google");
        params["model"] = json!("gemini-2.5-flash");
        params["affinity_model"] = json!("antigravity-gemini-3.8-flash");
        let initial = result(rig.call(&ctx, "route.account_for", BROCA, params.clone()));
        assert_eq!(
            initial["account"]["account_id"], INSULA_ONLY,
            "the control CLI route can use its own account"
        );
        params["admission_id"] = json!("antigravity-sticky");
        params["model"] = params["affinity_model"].clone();
        // The gemini-cli account cannot serve antigravity models: its pool
        // reach covers only gemini-2.5-flash, so no credential of the right
        // kind exists (`auth_required`, untimed), even with sticky affinity.
        assert_eq!(
            rig.call(&ctx, "route.account_for", BROCA, params.clone())
                .code(),
            Some("auth_required")
        );
        params["admission_id"] = json!("antigravity-cold");
        params["conversation_key"] = json!("antigravity-cold");
        assert_eq!(
            rig.call(&ctx, "route.account_for", BROCA, params).code(),
            Some("auth_required")
        );
        let mut unknown = admit_params("unknown-pool", "unknown-pool", "broca");
        unknown["provider"] = json!("google");
        unknown["model"] = json!("antigravity-new-family-model");
        unknown["affinity_model"] = unknown["model"].clone();
        // Same kind mismatch for a model no account's pools name.
        assert_eq!(
            rig.call(&ctx, "route.account_for", BROCA, unknown).code(),
            Some("auth_required")
        );
    }

    fn status(fixture: &Fixture, ctx: &Ctx<'_>, account: &str) -> Value {
        result(fixture.call(
            ctx,
            ACCOUNT_STATUS_OP,
            BROCA,
            json!({ "provider": "anthropic", "account_id": account }),
        ))
    }

    fn assert_status_vector(fixture: &Fixture, ctx: &Ctx<'_>, source: &str) {
        let vector: Value = serde_json::from_str(source).unwrap();
        let request = &vector["request"];
        let reply = result(fixture.call(
            ctx,
            request["method"].as_str().unwrap(),
            BROCA,
            request["params"].clone(),
        ));
        assert_eq!(json!({ "result": reply }), vector["reply"]);
    }

    #[test]
    fn account_status_codex_uses_openai_vendor_and_reported_weekly_window() {
        let mut fixture = provider_fixture(
            "status-codex",
            "openai",
            vec![reported_entry(
                "codex",
                json!("openai"),
                ACCOUNT_A,
                "weekly",
                10080,
                10.0,
            )],
        );
        fixture.policy.reserve_five_hour = 95.0;
        fixture.policy.reserve_seven_day = 8.0;
        for (used, warm_ok, reason) in [
            (10.0, true, json!({ "code": "headroom" })),
            (
                92.0,
                false,
                json!({ "code": "within_reserve", "window": "weekly" }),
            ),
        ] {
            fixture
                .accounts
                .lock()
                .unwrap()
                .readings
                .readings
                .get_mut(&("codex".into(), ACCOUNT_A.into()))
                .unwrap()
                .entry["usage"]["primary"]["usedPercent"] = json!(used);
            let reply = result(fixture.call(
                &fixture.ctx(true, NOW),
                ACCOUNT_STATUS_OP,
                BROCA,
                json!({ "provider": "openai", "account_id": ACCOUNT_A }),
            ));
            assert_eq!(reply["provider"], "openai");
            assert_eq!(reply["account_id"], ACCOUNT_A);
            assert_eq!(reply["observed_at"], "2026-09-29T11:59:00Z");
            assert_eq!(reply["age_band"], "full");
            assert_eq!(reply["warm_ok"], warm_ok);
            assert_eq!(reply["reason"], reason);
            assert_eq!(
                reply["windows"],
                json!([{ "kind": "weekly", "used_percent": used,
                         "resets_at": null, "window_minutes": 10080 }])
            );
        }
    }

    #[test]
    fn account_status_antigravity_uses_pools_not_aggregate_windows() {
        let mut fixture = provider_fixture(
            "status-antigravity",
            "antigravity",
            vec![json!({
                "provider": "antigravity", "apiProvider": "google", "account": ACCOUNT_A,
                "source": "oauth", "fetchedAt": "2026-09-29T11:59:00Z",
                "usage": {
                    "primary": { "windowKind": "weekly", "windowMinutes": 10080, "usedPercent": 100.0 },
                    "extraRateWindows": [
                        { "id": "gemini-5h", "window": { "usedPercent": 90.0 } },
                        { "id": "3p-weekly", "window": { "usedPercent": 10.0 } }
                    ]
                }
            })],
        );
        fixture.policy.reserve_five_hour = 10.0;
        fixture.policy.reserve_seven_day = 1.0;
        let reply = result(fixture.call(
            &fixture.ctx(true, NOW),
            ACCOUNT_STATUS_OP,
            BROCA,
            json!({ "provider": "google", "account_id": ACCOUNT_A }),
        ));
        assert_eq!(reply["warm_ok"], false);
        assert_eq!(
            reply["reason"],
            json!({ "code": "within_reserve", "window": "gemini-5h" })
        );
        assert_eq!(
            reply["windows"],
            json!([
                { "kind": "gemini-5h", "used_percent": 90.0,
                  "resets_at": null, "window_minutes": 300 },
                { "kind": "3p-weekly", "used_percent": 10.0,
                  "resets_at": null, "window_minutes": 10080 },
            ])
        );
    }

    #[test]
    fn account_status_fresh_headroom_allows_warming() {
        let fixture = fixture("status-headroom");
        assert_status_vector(
            &fixture,
            &fixture.ctx(true, NOW),
            include_str!("../../../test-vectors/route-account-status-v1/warm_ok.json"),
        );
        let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
        assert_eq!(reply["warm_ok"], true);
        assert_eq!(reply["reason"], json!({ "code": "headroom" }));
        assert_eq!(reply["provider"], "anthropic");
        assert_eq!(reply["account_id"], ACCOUNT_A);
        assert_eq!(reply["observed_at"], "2026-09-29T11:59:00Z");
        assert_eq!(reply["age_band"], "full");
        assert_eq!(
            reply["windows"],
            json!([
                { "kind": "five_hour", "used_percent": 10.0,
                  "resets_at": "2026-09-29T14:00:00Z", "window_minutes": 300 },
                { "kind": "weekly", "used_percent": 40.0,
                  "resets_at": null, "window_minutes": 10080 },
                { "kind": "Fable", "used_percent": 5.0,
                  "resets_at": null, "window_minutes": 10080 },
            ])
        );
    }

    #[test]
    fn account_status_within_reserve_names_the_window() {
        let mut fixture = fixture("status-reserve");
        fixture
            .accounts
            .lock()
            .unwrap()
            .readings
            .readings
            .get_mut(&("claude".into(), ACCOUNT_A.into()))
            .unwrap()
            .entry["usage"]["primary"]["usedPercent"] = json!(99.5);
        assert_status_vector(
            &fixture,
            &fixture.ctx(true, NOW),
            include_str!("../../../test-vectors/route-account-status-v1/within_reserve.json"),
        );
        fixture.policy.reserve_five_hour = 12.0;
        fixture.policy.reserve_seven_day = 8.0;
        fixture.policy.reserve_scoped = 3.0;
        for (slot, kind, used) in [
            ("primary", "five_hour", 88.0),
            ("secondary", "weekly", 93.0),
            ("scoped", "Fable", 97.0),
            ("reported", "producer_monthly", 93.0),
            ("reported_extra", "producer_pool", 93.0),
        ] {
            let mut entry = claude_entry(ACCOUNT_A);
            if slot == "scoped" {
                entry["usage"]["extraRateWindows"][0]["window"]["usedPercent"] = json!(used);
            } else if slot == "reported" {
                entry["usage"]["tertiary"] = json!({
                    "windowKind": kind, "usedPercent": used, "windowMinutes": 43200,
                });
            } else if slot == "reported_extra" {
                entry["usage"]["extraRateWindows"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({
                        "id": "unknown_scope", "windowKind": kind,
                        "window": { "usedPercent": used, "windowMinutes": 300 },
                    }));
            } else {
                entry["usage"][slot]["usedPercent"] = json!(used);
            }
            fixture
                .accounts
                .lock()
                .unwrap()
                .readings
                .readings
                .get_mut(&("claude".into(), ACCOUNT_A.into()))
                .unwrap()
                .entry = entry;
            let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
            assert_eq!(reply["warm_ok"], false, "{kind}");
            assert_eq!(
                reply["reason"],
                json!({ "code": "within_reserve", "window": kind })
            );
        }
    }

    #[test]
    fn account_status_preserves_producer_window_identity_not_slot_names() {
        let fixture = fixture("status-identity");
        let mut entry = claude_entry(ACCOUNT_A);
        let five_hour = entry["usage"]["primary"].clone();
        entry["usage"]["primary"] = entry["usage"]["secondary"].clone();
        entry["usage"]["secondary"] = five_hour;
        entry["usage"]["primary"]["windowKind"] = json!("producer_weekly");
        entry["usage"]["extraRateWindows"][0]["windowKind"] = json!("weekly");
        fixture
            .accounts
            .lock()
            .unwrap()
            .readings
            .readings
            .get_mut(&("claude".into(), ACCOUNT_A.into()))
            .unwrap()
            .entry = entry;
        let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
        assert_eq!(reply["warm_ok"], true);
        assert_eq!(reply["windows"][0]["kind"], "producer_weekly");
        assert_eq!(reply["windows"][1]["kind"], "five_hour");
        assert_eq!(reply["windows"][2]["kind"], "weekly");
    }

    #[test]
    fn account_status_stale_reading_stops_warming() {
        let fixture = fixture("status-stale");
        // The reading was observed a minute before NOW: exactly the allocator's
        // excluded boundary, not a separate warming timeout.
        let ctx = fixture.ctx(true, NOW - 60_000 + fixture.policy.excluded_band_ms);
        assert_status_vector(
            &fixture,
            &ctx,
            include_str!("../../../test-vectors/route-account-status-v1/reading_stale.json"),
        );
        let reply = status(&fixture, &ctx, ACCOUNT_A);
        assert_eq!(reply["warm_ok"], false);
        assert_eq!(reply["reason"], json!({ "code": "reading_stale" }));
        assert_eq!(reply["age_band"], "excluded");
        assert_eq!(reply["windows"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn account_status_uses_cold_placement_freshness_rules() {
        let mut fixture = fixture("status-freshness");
        fixture.policy.full_band_ms = 2 * 60_000;
        fixture.policy.excluded_band_ms = 10 * 60_000;
        let discounted = fixture.ctx(true, NOW + 60_000);
        let reply = status(&fixture, &discounted, ACCOUNT_A);
        assert_eq!(reply["age_band"], "discounted");
        assert_eq!(reply["warm_ok"], true);
        for (field, value, expected) in [
            ("errorClass", json!("upstream_failed"), false),
            ("error", json!("poll failed"), false),
            ("stale", json!({ "class": "unknown" }), false),
            ("stale", json!({ "class": "upstream_failed" }), true),
            (
                "fetchedAt",
                json!(rfc3339(NOW + fixture.policy.future_skew_ms + 1000)),
                false,
            ),
            ("fetchedAt", Value::Null, false),
        ] {
            let mut entry = claude_entry(ACCOUNT_A);
            entry[field] = value;
            fixture
                .accounts
                .lock()
                .unwrap()
                .readings
                .readings
                .get_mut(&("claude".into(), ACCOUNT_A.into()))
                .unwrap()
                .entry = entry;
            let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
            assert_eq!(reply["warm_ok"], expected, "{field}");
            assert_eq!(
                reply["reason"]["code"],
                if expected {
                    "headroom"
                } else {
                    "reading_stale"
                }
            );
        }
    }

    #[test]
    fn account_status_unknown_account_is_not_a_warming_target() {
        let fixture = fixture("status-unknown");
        for account in ["unknown", INSULA_ONLY] {
            let reply = status(&fixture, &fixture.ctx(true, NOW), account);
            assert_eq!(reply["warm_ok"], false);
            assert_eq!(reply["reason"], json!({ "code": "account_unknown" }));
            assert_eq!(reply["observed_at"], Value::Null);
            assert_eq!(reply["windows"], json!([]));
        }
    }

    #[test]
    fn account_status_missing_or_invalid_windows_stop_warming() {
        let fixture = fixture("status-missing");
        let reply = status(&fixture, &fixture.ctx(true, NOW), VAULT_ONLY);
        assert_eq!(reply["warm_ok"], false);
        assert_eq!(reply["reason"]["code"], "reading_missing");
        for usage in [
            Value::Null,
            json!({}),
            json!({ "primary": { "usedPercent": 10.0 } }),
            json!({ "primary": { "usedPercent": -1.0, "windowMinutes": 300 },
                    "secondary": { "usedPercent": 10.0, "windowMinutes": 10080 } }),
        ] {
            let mut entry = claude_entry(ACCOUNT_A);
            entry["usage"] = usage;
            fixture
                .accounts
                .lock()
                .unwrap()
                .readings
                .readings
                .get_mut(&("claude".into(), ACCOUNT_A.into()))
                .unwrap()
                .entry = entry;
            let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
            assert_eq!(reply["warm_ok"], false);
            assert_eq!(reply["reason"]["code"], "reading_missing");
        }
        fixture
            .accounts
            .lock()
            .unwrap()
            .readings
            .readings
            .get_mut(&("claude".into(), ACCOUNT_A.into()))
            .unwrap()
            .tombstoned_at_ms = Some(NOW);
        let reply = status(&fixture, &fixture.ctx(true, NOW), ACCOUNT_A);
        assert_eq!(reply["warm_ok"], false);
        assert_eq!(reply["reason"]["code"], "reading_missing");
    }

    #[test]
    fn account_status_disabled_never_allows_warming() {
        let fixture = fixture("status-disabled");
        let reply = status(&fixture, &fixture.ctx(false, NOW), ACCOUNT_A);
        assert_eq!(reply["warm_ok"], false);
        assert_eq!(
            reply["reason"],
            json!({ "code": "account_selection_disabled" })
        );
    }

    #[test]
    fn account_status_writes_nothing_whether_serving_is_on_or_off() {
        let fixture = fixture("status-no-writes");
        // data_version on a separate connection changes for any committed write
        // on the store connection, including tables outside account serving.
        let observer = rusqlite::Connection::open(fixture.dir.join("store.db")).unwrap();
        let version = || {
            observer
                .query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0))
                .unwrap()
        };
        let before = version();
        for enabled in [true, false] {
            let ctx = fixture.ctx(enabled, NOW);
            for account in [ACCOUNT_A, VAULT_ONLY, "unknown"] {
                status(&fixture, &ctx, account);
            }
        }
        assert_eq!(
            version(),
            before,
            "a quota read must not commit any store writes"
        );
        assert_eq!(fixture.store().account_serving_counts().unwrap().total(), 0);
        assert!(fixture.serve_rows().is_empty());
    }

    #[test]
    fn pinned_model_account_preflight_refuses_all_walls_but_admits_a_healthy_sibling() {
        let fixture = fixture("pin-preflight");
        let mut a = claude_entry(ACCOUNT_A);
        let mut b = claude_entry(ACCOUNT_B);
        a["usage"]["primary"]["usedPercent"] = json!(100.0);
        b["usage"]["primary"]["usedPercent"] = json!(100.0);
        let apply = |b: &Value| {
            let mut vault_only = a.clone();
            vault_only["account"] = json!(VAULT_ONLY);
            let bytes = serde_json::to_vec(
                &json!({"result":[a,b,vault_only],"completeProviders":["claude"]}),
            )
            .unwrap();
            apply_provider_reply(&mut fixture.accounts.lock().unwrap(), "claude", &bytes, NOW)
                .unwrap();
        };
        apply(&b);
        let ctx = fixture.ctx(true, NOW);
        let (code, message) = preflight_model(&ctx, "anthropic", "claude-opus-5-5").unwrap_err();
        assert_eq!(code, "account_pool_reserve_limited");
        assert!(message.contains("until 2026-09-29T14:00:00Z"), "{message}");
        b["usage"]["primary"]["usedPercent"] = json!(10.0);
        apply(&b);
        assert!(preflight_model(&ctx, "anthropic", "claude-opus-5-5").is_ok());
        assert!(
            fixture.serve_rows().is_empty(),
            "preflight must not commit a runner admission"
        );
    }

    #[test]
    fn every_account_op_refuses_with_account_selection_disabled_and_writes_nothing() {
        let fixture = fixture("disabled");
        let ctx = fixture.ctx(false, NOW);
        let calls = [
            (
                ACCOUNT_FOR_OP,
                admit_params("adm-1", "consult:c:s:1", "broca"),
            ),
            (
                ACCOUNT_OBSERVE_OP,
                json!({ "event_id": "e1", "caller": { "runner": "broca" }, "kind": "admission_started",
                        "admission_id": "adm-1", "run_id": "run-1" }),
            ),
            (ACCOUNT_RELEASE_OP, json!({ "admission_id": "adm-1" })),
        ];
        for (method, params) in calls {
            let reply = fixture.call(&ctx, method, BROCA, params);
            assert_eq!(
                reply.code(),
                Some(DISABLED_CODE),
                "{method} must refuse while disabled"
            );
            let OpReply::Error { detail, .. } = &reply else {
                unreachable!()
            };
            assert_eq!(detail.as_ref().unwrap()["code"], DISABLED_CODE);
        }
        // Malformed params are refused the same way: nothing is even parsed.
        let reply = handle(
            ACCOUNT_FOR_OP,
            b"{\"method\":\"route.account_for\"}",
            BROCA,
            &ctx,
        );
        assert_eq!(reply.code(), Some(DISABLED_CODE));
        assert_eq!(fixture.store().account_serving_counts().unwrap().total(), 0);
        assert!(fixture.serve_rows().is_empty());
    }

    #[test]
    fn a_roster_gap_is_reported_in_both_directions_without_emails() {
        let fixture = fixture("gap");
        let build = {
            let guard = fixture.accounts.lock().unwrap();
            allocator_roster(&guard, "anthropic", NOW)
        };
        let roster = build.roster.as_ref().expect("vault roster");
        let ids: Vec<&str> = roster
            .accounts
            .iter()
            .map(|view| view.account_id.as_str())
            .collect();
        assert_eq!(ids, vec![ACCOUNT_A, ACCOUNT_B, VAULT_ONLY]);
        assert!(
            !ids.contains(&INSULA_ONLY),
            "an account the vault does not list is never a candidate"
        );
        let unread = roster
            .accounts
            .iter()
            .find(|view| view.account_id == VAULT_ONLY)
            .unwrap();
        assert!(unread.quota.is_none());
        let gaps = &build.context["gaps"];
        assert_eq!(gaps["insula_only"][0]["account_id"], INSULA_ONLY);
        assert_eq!(gaps["vault_only"][0]["account_id"], VAULT_ONLY);
        assert_eq!(gaps["vault_only"][0]["first_reading_overdue"], true);
        let alarms = build.context["alarms"].as_array().unwrap();
        assert!(alarms
            .iter()
            .any(|alarm| alarm.as_str().unwrap().contains(VAULT_ONLY)));
        assert!(
            !build.context.to_string().contains("@example.test"),
            "emails stay hidden"
        );

        // The served decision's record carries the same gaps.
        let ctx = fixture.ctx(true, NOW);
        result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-gap", "k-gap", "broca"),
        ));
        let row = &fixture.serve_rows()[0];
        let context: Value = serde_json::from_str(row.context_json.as_deref().unwrap()).unwrap();
        assert_eq!(
            context["roster"]["gaps"]["insula_only"][0]["account_id"],
            INSULA_ONLY
        );

        // A roster past 24 hours is still passed through, marked, so existing
        // affinity continues while new cold work is refused.
        let old = {
            let guard = fixture.accounts.lock().unwrap();
            allocator_roster(&guard, "anthropic", NOW + 24 * 60 * 60_000)
        };
        assert_eq!(old.context["past_max_age"], true);
        let ctx = fixture.ctx(true, NOW + 24 * 60 * 60_000);
        let reply = fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-old", "k-old", "broca"),
        );
        assert_eq!(reply.code(), Some("account_selection_unavailable"));
    }

    #[test]
    fn different_candidates_replay_the_stored_decision_without_charging() {
        let fixture = fixture("replay-candidates");
        let ctx = fixture.ctx(true, NOW);
        let mut params = admit_params("adm-retry", "broca:retry", "broca");
        params["candidates"] = json!({
            "view_digest": "vault-before",
            "accounts": [{ "account_id": ACCOUNT_A, "credential_id": "oauth:anthropic" }]
        });
        let reply = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params.clone()));
        assert_eq!(reply["account"]["account_id"], ACCOUNT_A);
        let before = fixture
            .store()
            .account_transaction(|tx| tx.admission(BROCA, "adm-retry"))
            .unwrap()
            .unwrap();
        assert!(before.charged);
        let counts = fixture.store().account_serving_counts().unwrap().total();
        params["candidates"] = json!({
            "view_digest": "vault-after",
            "accounts": [{ "account_id": ACCOUNT_B, "credential_id": "oauth:anthropic:ysample" }]
        });
        let replayed = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params));
        assert_eq!(
            replayed, reply,
            "replay keeps even an account absent from candidates"
        );
        let after = fixture
            .store()
            .account_transaction(|tx| tx.admission(BROCA, "adm-retry"))
            .unwrap()
            .unwrap();
        assert_eq!(after, before, "replay must not change the charged demand");
        assert_eq!(
            fixture.store().account_serving_counts().unwrap().total(),
            counts
        );
        assert_eq!(
            fixture.serve_rows().len(),
            1,
            "replay creates no new decision"
        );
    }

    #[test]
    fn admission_fingerprint_is_independent_of_json_key_order() {
        let forward: Value = serde_json::from_str(r#"{
            "provider":"anthropic", "model":"claude-opus-5-5",
            "conversation_key":"broca:retry", "caller":{"runner":"broca","harness":"test"},
            "admission_id":"adm-retry", "requested_account":{"account_id":"account-a","credential_id":"cred-a"}
        }"#).unwrap();
        let reversed: Value = serde_json::from_str(
            r#"{
            "requested_account":{"credential_id":"cred-a","account_id":"account-a"},
            "admission_id":"adm-retry", "caller":{"harness":"test","runner":"broca"},
            "conversation_key":"broca:retry", "model":"claude-opus-5-5", "provider":"anthropic"
        }"#,
        )
        .unwrap();
        assert_eq!(
            fingerprint(&parse_account_for(&forward).unwrap()),
            fingerprint(&parse_account_for(&reversed).unwrap())
        );
    }

    #[test]
    fn admission_fingerprint_ignores_advisory_inputs_but_covers_identity() {
        let params = admit_params("adm-identity", "broca:identity", "broca");
        let baseline = fingerprint(&parse_account_for(&params).unwrap());
        for (field, value) in [
            ("initial_input_bytes", json!(99)),
            ("initial_input_estimated", json!(true)),
            ("model_decision_id", json!("new-decision")),
            ("hold_wait_max_s", json!(10)),
            ("caller", json!({"runner":"BROCA", "harness":"new-harness"})),
            ("vault_digest", json!("new-view")),
            ("quota_hint", json!(123)),
            ("timestamp", json!(NOW + 1)),
        ] {
            let mut changed = params.clone();
            changed[field] = value;
            assert_eq!(
                fingerprint(&parse_account_for(&changed).unwrap()),
                baseline,
                "{field}"
            );
        }
        for (field, value) in [
            ("provider", json!("openai")),
            ("model", json!("claude-fable-5-5")),
            ("affinity_model", json!("claude-fable-5-5")),
            ("conversation_key", json!("broca:other")),
            ("caller", json!({"runner":"plugin"})),
            ("expected_pin", json!({"id":"pin", "generation":1})),
            ("requested_account", json!({"account_id":"other"})),
            (
                "preferred_existing_account",
                json!({"credential_id":"other"}),
            ),
        ] {
            let mut changed = params.clone();
            changed[field] = value;
            assert_ne!(
                fingerprint(&parse_account_for(&changed).unwrap()),
                baseline,
                "{field}"
            );
        }
        let mut renew = params;
        renew["mode"] = json!("renew");
        renew["expected_pin"] = json!({"id":"pin", "generation":1});
        let renewing = fingerprint(&parse_account_for(&renew).unwrap());
        renew["mode"] = json!("reassign");
        assert_ne!(fingerprint(&parse_account_for(&renew).unwrap()), renewing);
    }

    #[test]
    fn a_served_pin_survives_a_reopen_and_renewal_compare_and_sets_on_it() {
        let mut fixture = fixture("serve");
        let ctx = fixture.ctx(true, NOW);
        let params = admit_params("adm-1", "opencode:host:session-1", "anthropic-auth");
        let reply = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params.clone()));
        assert_eq!(
            reply["admission"],
            json!({ "id": "adm-1", "state": "reserved" })
        );
        assert_eq!(reply["account"]["provider"], "anthropic");
        assert_eq!(reply["account"]["refresh_adapter"], "anthropic");
        assert_eq!(reply["account"]["method"], "oauth");
        assert_eq!(reply["pin"]["generation"], 1);
        assert_eq!(reply["pin"]["mode"], "conversation_lease");
        assert_eq!(
            reply["pin"]["admission_valid_until"],
            "2026-09-29T12:01:00Z"
        );
        assert_eq!(reply["pin"]["lease_valid_until"], "2026-09-29T12:02:00Z");
        assert_eq!(reply["pin"]["affinity_expires_at"], "2026-10-06T12:00:00Z");
        assert_eq!(reply["reason"]["code"], "cold_scored");
        assert_eq!(reply["reason"]["policy_version"], 2);
        assert_eq!(reply["reason"]["quota_fetched_at"], "2026-09-29T11:59:00Z");
        assert_eq!(reply["reason"]["quota_age_band"], "full");
        let pin_id = reply["pin"]["id"].as_str().unwrap().to_string();
        let account = reply["account"]["account_id"].as_str().unwrap().to_string();

        // A transport retry gets the committed decision back; reusing the id
        // with another model changes admission identity and is a conflict.
        let again = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params.clone()));
        assert_eq!(again, reply);
        let mut changed = params.clone();
        changed["model"] = json!("claude-fable-5-5");
        let conflict = fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, changed);
        assert_eq!(conflict.code(), Some("admission_conflict"));

        fixture.reopen();
        let later = NOW + 30_000;
        let ctx = fixture.ctx(true, later);
        let mut renew = admit_params("adm-2", "opencode:host:session-1", "anthropic-auth");
        renew["mode"] = json!("renew");
        renew["expected_pin"] = json!({ "id": pin_id, "generation": 1 });
        let renewed = result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, renew.clone()));
        assert_eq!(
            renewed["pin"]["id"],
            json!(pin_id),
            "the pin survived the reopen"
        );
        assert_eq!(renewed["pin"]["generation"], 1);
        assert_eq!(renewed["account"]["account_id"], json!(account));
        assert_eq!(renewed["reason"]["code"], "sticky_hit");
        let renewal = fixture
            .store()
            .account_transaction(|tx| tx.admission(BROCA, "adm-2"))
            .unwrap()
            .unwrap();
        assert!(!renewal.charged, "a renewal charges no demand");

        // A renewal naming an older generation gets the current pin back and
        // changes nothing.
        let mut stale = renew.clone();
        stale["admission_id"] = json!("adm-3");
        stale["expected_pin"] = json!({ "id": pin_id, "generation": 0 });
        let refused = fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, stale);
        assert_eq!(refused.code(), Some("pin_generation_conflict"));
        let OpReply::Error { detail, .. } = refused else {
            unreachable!()
        };
        assert_eq!(detail.unwrap()["current_pin"]["id"], json!(pin_id));
        assert!(fixture
            .store()
            .account_transaction(|tx| tx.admission(BROCA, "adm-3"))
            .unwrap()
            .is_none());

        // Another principal's conversation with the same key is its own.
        let other = result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            "reserved:other",
            admit_params("adm-1", "opencode:host:session-1", "anthropic-auth"),
        ));
        assert_ne!(other["pin"]["id"], json!(pin_id));
    }

    #[test]
    fn observations_and_releases_act_only_on_the_principals_own_admission() {
        let fixture = fixture("observe");
        let ctx = fixture.ctx(true, NOW);
        let admitted = result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-1", "broca:run-1", "broca"),
        ));
        let account = admitted["account"]["account_id"].clone();
        let exhausted = |event: &str| {
            json!({
                "event_id": event,
                "caller": { "runner": "broca" },
                "kind": "account_limit_exhausted",
                "admission_id": "adm-1",
                "limit_window": "seven_day",
                "resets_at_ms": NOW + 3_600_000,
                "resets_at_source": "header",
                "observed_at_ms": NOW + 1_000,
            })
        };
        // Another principal naming Broca's admission changes nothing shared.
        let foreign = result(fixture.call(
            &ctx,
            ACCOUNT_OBSERVE_OP,
            "reserved:plugin",
            exhausted("e-1"),
        ));
        assert_eq!(foreign["observation"]["bound"], false);
        assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 0);
        let foreign_release = result(fixture.call(
            &ctx,
            ACCOUNT_RELEASE_OP,
            "reserved:plugin",
            json!({ "admission_id": "adm-1" }),
        ));
        assert_eq!(foreign_release["release"]["released"], false);

        let bound = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("e-2")));
        assert_eq!(bound["observation"]["bound"], true);
        assert_eq!(bound["observation"]["effects"][0]["effect"], "fence");
        assert_eq!(
            bound["observation"]["effects"][0]["fence"]["account_id"],
            account
        );
        // The same event again is recognized and changes nothing.
        let repeat = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("e-2")));
        assert_eq!(repeat["observation"]["duplicate"], true);
        assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 0);
        assert_eq!(
            fixture
                .store()
                .non_quota_account_route_fences(NOW)
                .unwrap()
                .len(),
            1,
            "headroom contradicts this refusal, so only its non-quota route fence stands"
        );

        // A new cold admission now avoids the fenced account.
        let next = result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-2", "broca:run-2", "broca"),
        ));
        assert_ne!(next["account"]["account_id"], account);

        // Release happens once, and never after the run started.
        let first = result(fixture.call(
            &ctx,
            ACCOUNT_RELEASE_OP,
            BROCA,
            json!({ "admission_id": "adm-2" }),
        ));
        assert_eq!(first["release"]["released"], true);
        assert_eq!(first["release"]["state"], "released");
        let second = result(fixture.call(
            &ctx,
            ACCOUNT_RELEASE_OP,
            BROCA,
            json!({ "admission_id": "adm-2" }),
        ));
        assert_eq!(second["release"]["released"], false);
        let late = fixture.ctx(true, NOW + 120_000);
        let started = result(fixture.call(
            &late,
            ACCOUNT_OBSERVE_OP,
            BROCA,
            json!({ "event_id": "s-1", "caller": { "runner": "broca" }, "kind": "admission_started",
                    "admission_id": "adm-1", "run_id": "run-1" }),
        ));
        assert_eq!(
            started["observation"]["effects"][0],
            json!({ "effect": "admission_started", "late": true })
        );
        let after_start = result(fixture.call(
            &late,
            ACCOUNT_RELEASE_OP,
            BROCA,
            json!({ "admission_id": "adm-1" }),
        ));
        assert_eq!(after_start["release"]["released"], false);
        assert_eq!(after_start["release"]["state"], "started");
    }

    #[test]
    fn header_observations_bind_only_to_the_admitted_account() {
        for kind in ["attempt", "run_finished"] {
            let fixture = fixture("header-observation-account");
            let ctx = fixture.ctx(true, NOW);
            let admitted = result(fixture.call(
                &ctx,
                ACCOUNT_FOR_OP,
                BROCA,
                admit_params("adm-usage", "broca:usage", "broca"),
            ));
            let account = admitted["account"]["account_id"].as_str().unwrap();
            let other = if account == ACCOUNT_A {
                ACCOUNT_B
            } else {
                ACCOUNT_A
            };
            for (event, served, bound) in [
                ("mismatch", Some(other), false),
                ("matching", Some(account), true),
                ("unreported", None, true),
            ] {
                let headers = BTreeMap::from([
                    (
                        "anthropic-ratelimit-unified-5h-status".to_string(),
                        "limited".to_string(),
                    ),
                    (
                        "anthropic-ratelimit-unified-fallback-percentage".to_string(),
                        "0.12345".to_string(),
                    ),
                ]);
                let mut params = json!({
                    "event_id": event,
                    "caller": { "runner": "broca" },
                    "kind": kind,
                    "admission_id": "adm-usage",
                    "observed_at_ms": NOW - 1_000,
                    "headers": headers,
                });
                if let Some(served) = served {
                    params["served_account_id"] = json!(served);
                }
                let body = serde_json::to_vec(&json!({
                    "method": ACCOUNT_OBSERVE_OP,
                    "params": params,
                }))
                .unwrap();
                let recorded = result(handle(ACCOUNT_OBSERVE_OP, &body, BROCA, &ctx));
                assert_eq!(recorded["observation"]["bound"], bound, "{kind}: {event}");
                if !bound {
                    assert_eq!(
                        recorded["observation"]["effects"][0]["effect"],
                        "served_account_mismatch"
                    );
                }
            }
        }
    }

    #[test]
    fn broca_raw_headers_preserve_unknowns_and_confirm_only_raw_exhaustion() {
        let fixture = fixture("raw-headers");
        let ctx = fixture.ctx(true, NOW);
        result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-raw", "broca:raw", "broca"),
        ));
        let reading = |event: &str, utilization: &str| {
            json!({
                "event_id": event,
                "caller": { "runner": "broca" },
                "kind": "attempt",
                "admission_id": "adm-raw",
                "upstream_origin": "upstream",
                "observed_at_ms": NOW,
                "view_digest": "credential-read-view",
                "headers": {
                    "anthropic-ratelimit-unified-5h-utilization": utilization,
                    "anthropic-ratelimit-unified-5h-reset": ((NOW + 3_600_000) / 1000).to_string(),
                    "anthropic-ratelimit-unified-fallback-percentage": "0.12345"
                }
            })
        };
        let below = reading("lineage:1", "0.995");
        let reply = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, below.clone()));
        assert_eq!(
            reply["observation"]["effects"][0]["effect"],
            "not_confirmed"
        );
        assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 0);
        let stored = fixture
            .store()
            .account_transaction(|tx| tx.observation_by_event(BROCA, "broca", "lineage:1"))
            .unwrap()
            .unwrap();
        let stored: Value = serde_json::from_str(&stored.observation_json).unwrap();
        assert_eq!(stored, below);
        assert_eq!(
            stored["headers"]["anthropic-ratelimit-unified-fallback-percentage"],
            "0.12345"
        );
        let full = reading("lineage:2", "1.0");
        let reply = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, full.clone()));
        assert_eq!(reply["observation"]["effects"][0]["effect"], "fence");
        assert_eq!(
            reply["observation"]["effects"][0]["fence"]["reset"]["at_ms"],
            json!(NOW + 3_600_000)
        );
        let repeat = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, full));
        assert_eq!(repeat["observation"]["duplicate"], true);
        assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 1);
    }

    /// A backup may restore the observation and decision logs empty. A runner
    /// re-sending an event routing applied before the backup gets the first
    /// report's answer back and the event is not applied again; a new event
    /// is applied as usual.
    #[test]
    fn a_re_sent_event_is_not_applied_again_after_the_history_logs_are_restored_empty() {
        let mut fixture = fixture("restored-observations");
        let ctx = fixture.ctx(true, NOW);
        result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-restored", "broca:restored", "broca"),
        ));
        let exhausted = |event: &str| {
            json!({
                "event_id": event,
                "caller": { "runner": "broca" },
                "kind": "attempt",
                "admission_id": "adm-restored",
                "upstream_origin": "upstream",
                "observed_at_ms": NOW,
                "view_digest": "credential-read-view",
                "headers": {
                    "anthropic-ratelimit-unified-5h-utilization": "1.0",
                    "anthropic-ratelimit-unified-5h-reset": ((NOW + 3_600_000) / 1000).to_string()
                }
            })
        };
        let first = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("restored:1")));
        assert_eq!(first["observation"]["duplicate"], false);
        assert_eq!(first["observation"]["effects"][0]["effect"], "fence");
        assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 1);
        drop(ctx);

        fixture.store = None;
        {
            let connection = rusqlite::Connection::open(fixture.dir.join("store.db")).unwrap();
            connection
                .execute_batch("DELETE FROM account_observation; DELETE FROM account_decision;")
                .unwrap();
        }
        fixture.reopen();
        let ctx = fixture.ctx(true, NOW);
        assert_eq!(
            fixture
                .store()
                .account_serving_counts()
                .unwrap()
                .observations,
            0
        );

        let repeat = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("restored:1")));
        assert_eq!(repeat["observation"]["duplicate"], true);
        assert_eq!(repeat["observation"]["id"], first["observation"]["id"]);
        assert_eq!(
            repeat["observation"]["bound"],
            first["observation"]["bound"]
        );
        assert_eq!(
            repeat["observation"]["effects"],
            first["observation"]["effects"]
        );
        let counts = fixture.store().account_serving_counts().unwrap();
        assert_eq!((counts.observations, counts.fences), (0, 1));

        let next = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, exhausted("restored:2")));
        assert_eq!(next["observation"]["duplicate"], false);
        assert_eq!(
            fixture
                .store()
                .account_serving_counts()
                .unwrap()
                .observations,
            1
        );
    }

    #[test]
    fn broca_transport_failure_without_headers_records_no_quota_evidence() {
        let fixture = fixture("no-headers");
        let ctx = fixture.ctx(true, NOW);
        result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-transport", "broca:transport", "broca"),
        ));
        let params = json!({ "event_id": "lineage:3", "caller": { "runner": "broca" },
            "kind": "attempt", "admission_id": "adm-transport",
            "upstream_origin": "upstream", "terminal_reason": "transport_failure" });
        let reply = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, params.clone()));
        assert_eq!(reply["observation"]["effects"], json!([]));
        assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 0);
        let stored = fixture
            .store()
            .account_transaction(|tx| tx.observation_by_event(BROCA, "broca", "lineage:3"))
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&stored.observation_json).unwrap(),
            params
        );
    }

    #[test]
    fn broca_run_finished_ends_lease_and_later_release_is_noop() {
        let fixture = fixture("finish-lease");
        let ctx = fixture.ctx(true, NOW);
        result(fixture.call(
            &ctx,
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("adm-finish", "broca:finish", "broca"),
        ));
        result(fixture.call(
            &ctx,
            ACCOUNT_OBSERVE_OP,
            BROCA,
            json!({
                "event_id": "lineage:4", "caller": { "runner": "broca" },
                "kind": "admission_started", "admission_id": "adm-finish", "run_id": "run-finish"
            }),
        ));
        let finish_time = NOW + 10_000;
        let ctx = fixture.ctx(true, finish_time);
        let finish = json!({ "event_id": "lineage:5", "caller": { "runner": "broca" },
            "kind": "run_finished", "admission_id": "adm-finish", "run_id": "run-finish",
            "terminal_reason": "success", "upstream_origin": "upstream", "headers": {
                "anthropic-ratelimit-unified-7d-utilization": "1.0"
            } });
        let reply = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, finish.clone()));
        assert_eq!(reply["observation"]["effects"][1]["lease_ended"], true);
        assert_eq!(reply["observation"]["effects"][2]["effect"], "fence");
        let lease_until = || {
            fixture
                .store()
                .account_transaction(|tx| {
                    let row = tx.admission(BROCA, "adm-finish")?.unwrap();
                    Ok(tx
                        .affinity(BROCA, &row.conversation_hash, &row.provider)?
                        .unwrap()
                        .lease_valid_until)
                })
                .unwrap()
        };
        assert_eq!(lease_until(), Some(finish_time));
        let reply = result(fixture.call(&ctx, ACCOUNT_OBSERVE_OP, BROCA, finish.clone()));
        assert_eq!(reply["observation"]["duplicate"], true);
        // A different admission can renew the same affinity after run end.
        let renewed_until = NOW + 180_000;
        fixture
            .store()
            .account_transaction(|tx| {
                let row = tx.admission(BROCA, "adm-finish")?.unwrap();
                let mut affinity = tx
                    .affinity(BROCA, &row.conversation_hash, &row.provider)?
                    .unwrap();
                affinity.lease_valid_until = Some(renewed_until);
                let expected = (affinity.pin_id.clone(), affinity.pin_generation);
                assert!(tx.put_affinity(&affinity, Some((&expected.0, expected.1)))?);
                Ok(())
            })
            .unwrap();
        let later = fixture.ctx(true, finish_time + 1000);
        let mut new_finish = finish;
        new_finish["event_id"] = json!("lineage:6");
        new_finish.as_object_mut().unwrap().remove("headers");
        let reply = result(fixture.call(&later, ACCOUNT_OBSERVE_OP, BROCA, new_finish));
        assert_eq!(
            reply["observation"]["effects"],
            json!([{ "effect": "none", "state": "finished" }])
        );
        let reply = result(fixture.call(
            &later,
            ACCOUNT_RELEASE_OP,
            BROCA,
            json!({ "admission_id": "adm-finish" }),
        ));
        assert_eq!(reply["release"]["released"], false);
        assert_eq!(reply["release"]["lease_ended"], false);
        assert_eq!(lease_until(), Some(renewed_until));
        assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 1);
    }

    #[test]
    fn rfc3339_formats_whole_seconds_in_utc() {
        assert_eq!(rfc3339(NOW), "2026-09-29T12:00:00Z");
        assert_eq!(
            rfc3339(NOW + 7 * 24 * 3_600_000 + 999),
            "2026-10-06T12:00:00Z"
        );
    }

    /// The runner lists only the account `first` admitted, so no sibling can
    /// stand in while that account is blocked.
    fn only_on(first: &Value, admission_id: &str, key: &str) -> Value {
        let mut params = admit_params(admission_id, key, "broca");
        params["candidates"] = json!({
            "view_digest": "runner-view",
            "accounts": [{
                "account_id": first["account"]["account_id"],
                "credential_id": first["account"]["credential_id"],
            }],
        });
        params
    }

    fn exhaust(
        fixture: &Fixture,
        event_id: &str,
        admission_id: &str,
        at: i64,
        resets_at_ms: Value,
        resets_at_source: Value,
    ) -> Value {
        // Quota recovery tests need a refusal consistent with its reading.
        // Contradictory headroom is exercised separately as a non-quota wall.
        if let Some(admission) = fixture
            .store()
            .account_read(|tx| tx.admission(BROCA, admission_id))
            .unwrap()
        {
            fn exhausted_windows(value: &mut Value) {
                match value {
                    Value::Object(fields) => {
                        for (name, value) in fields {
                            if name == "usedPercent" {
                                *value = json!(100.0);
                            } else {
                                exhausted_windows(value);
                            }
                        }
                    }
                    Value::Array(values) => {
                        for value in values {
                            exhausted_windows(value);
                        }
                    }
                    _ => {}
                }
            }
            for reading in fixture
                .accounts
                .lock()
                .unwrap()
                .readings
                .readings
                .values_mut()
                .filter(|reading| reading.account_id == admission.account_id)
            {
                exhausted_windows(&mut reading.entry);
            }
        }
        result(fixture.call(
            &fixture.ctx(true, at),
            ACCOUNT_OBSERVE_OP,
            BROCA,
            json!({
                "event_id": event_id, "caller": {"runner": "broca"},
                "kind": "account_limit_exhausted", "admission_id": admission_id,
                "resets_at_ms": resets_at_ms, "resets_at_source": resets_at_source,
                "observed_at_ms": at,
            }),
        ))
    }

    #[test]
    fn a_whole_account_reset_schedules_one_probe_without_expiring_evidence() {
        let fixture = fixture("trial-run");
        let first = result(fixture.call(
            &fixture.ctx(true, NOW),
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("trial-1", "broca:trial-1", "broca"),
        ));
        // A ChatGPT-shaped exhaustion: no window, a header reset in 10 min.
        let reset = NOW + 10 * 60_000;
        let observed = exhaust(
            &fixture,
            "trial-e1",
            "trial-1",
            NOW + 1000,
            json!(reset),
            json!("header"),
        );
        let fence = &observed["observation"]["effects"][0]["fence"];
        for reading in fixture
            .accounts
            .lock()
            .unwrap()
            .readings
            .readings
            .values_mut()
        {
            reading.entry["fetchedAt"] = json!(rfc3339(NOW - 3_600_000));
        }
        assert_eq!(
            fence["pool"],
            Value::Null,
            "names no pool: the whole account"
        );
        let at = |now: i64, id: &str| {
            fixture.call(
                &fixture.ctx(true, now),
                ACCOUNT_FOR_OP,
                BROCA,
                only_on(&first, id, &format!("broca:{id}")),
            )
        };
        assert_eq!(
            at(NOW + 1001, "trial-before-reset").code(),
            Some("account_hold")
        );
        assert!(
            at(reset - 1, "trial-2").code().is_some(),
            "blocked until the extra reset check"
        );
        let trial = result(at(reset, "trial-3"));
        assert!(
            at(reset, "concurrent-trial").code().is_some(),
            "the reset admits only one probe"
        );
        assert!(
            !fixture
                .store
                .as_ref()
                .unwrap()
                .account_read(|tx| tx.active_fences("anthropic"))
                .unwrap()
                .is_empty(),
            "a probe does not delete quota evidence"
        );
        assert_eq!(
            trial["account"]["account_id"],
            first["account"]["account_id"]
        );
        // The trial run's exhaustion blocks the account again.
        exhaust(
            &fixture,
            "trial-e2",
            "trial-3",
            reset + 2000,
            Value::Null,
            Value::Null,
        );
        assert!(at(reset + 3000, "trial-4").code().is_some());
    }

    #[test]
    fn quota_without_readings_rechecks_at_fifteen_minutes_even_with_a_later_hint() {
        let confirmed = NOW + 1000;
        for (label, resets_at, source, retry_at) in [
            (
                "missing-reset",
                Value::Null,
                Value::Null,
                confirmed + 15 * 60_000,
            ),
            (
                "hint",
                json!(confirmed + 30 * 60_000),
                json!("retry_after"),
                confirmed + 15 * 60_000,
            ),
        ] {
            let fixture = fixture(label);
            let key = format!("conversation-{label}");
            let first = result(fixture.call(
                &fixture.ctx(true, NOW),
                ACCOUNT_FOR_OP,
                BROCA,
                admit_params(&format!("{label}-1"), &key, "broca"),
            ));
            exhaust(
                &fixture,
                &format!("{label}-e"),
                &format!("{label}-1"),
                confirmed,
                resets_at,
                source,
            );
            for reading in fixture
                .accounts
                .lock()
                .unwrap()
                .readings
                .readings
                .values_mut()
            {
                reading.entry["fetchedAt"] = json!(rfc3339(NOW - 3_600_000));
            }
            let at = |now: i64, id: String| {
                fixture.call(
                    &fixture.ctx(true, now),
                    ACCOUNT_FOR_OP,
                    BROCA,
                    only_on(&first, &id, &key),
                )
            };
            assert!(
                at(retry_at - 1000, format!("{label}-2")).code().is_some(),
                "{label}"
            );
            let retried = result(at(retry_at, format!("{label}-3")));
            assert!(at(retry_at, format!("{label}-concurrent")).code().is_some());
            assert_eq!(
                retried["account"]["account_id"], first["account"]["account_id"],
                "{label}"
            );
        }
    }

    /// An Antigravity-shaped reading of one account, observed at `fetched_at`.
    fn pooled_entry(account: &str, fetched_at: &str, windows: &[(&str, f64, i64)]) -> Value {
        let windows: Vec<Value> = windows
            .iter()
            .map(|(id, used, minutes)| {
                json!({"id": id, "window": {"usedPercent": used, "windowMinutes": minutes}})
            })
            .collect();
        json!({
            "provider": "antigravity", "apiProvider": "google", "account": account,
            "source": "oauth", "fetchedAt": fetched_at,
            "usage": {"extraRateWindows": windows}
        })
    }

    #[test]
    fn a_no_pool_block_lifts_only_on_a_reading_reporting_every_recorded_pool() {
        let both = [("gemini-5h", 10.0, 300), ("3p-weekly", 20.0, 10080)];
        let mut rig = provider_fixture(
            "recovery-set",
            "antigravity",
            vec![pooled_entry(ACCOUNT_A, "2026-09-29T11:59:00Z", &both)],
        );
        let mut params = admit_params("pool-1", "pool-conversation", "broca");
        params["provider"] = json!("google");
        params["model"] = json!("antigravity-gemini-3.8-flash");
        params["affinity_model"] = params["model"].clone();
        let first = result(rig.call(&rig.ctx(true, NOW), ACCOUNT_FOR_OP, BROCA, params.clone()));
        assert_eq!(first["account"]["account_id"], ACCOUNT_A);
        let observed = exhaust(
            &rig,
            "pool-e",
            "pool-1",
            NOW + 1000,
            Value::Null,
            Value::Null,
        );
        let fence = &observed["observation"]["effects"][0]["fence"];
        assert_eq!(fence["pool"], Value::Null);
        assert_eq!(
            fence["recovery_set"].as_array().map(Vec::len),
            Some(2),
            "both pools of the latest fresh reading: {fence}"
        );
        let read = |rig: &Fixture, fetched_at: &str, windows: &[(&str, f64, i64)]| {
            let body = json!({"result": [pooled_entry(ACCOUNT_A, fetched_at, windows)]});
            account_quota::apply_usage_reply(
                &mut rig.accounts.lock().unwrap(),
                &serde_json::to_vec(&body).unwrap(),
                NOW,
            )
            .unwrap();
        };
        let admit = |rig: &Fixture, now: i64, id: &str| {
            let mut retry = params.clone();
            retry["admission_id"] = json!(id);
            retry["candidates"] = json!({
                "view_digest": "runner-view",
                "accounts": [{
                    "account_id": first["account"]["account_id"],
                    "credential_id": first["account"]["credential_id"],
                }],
            });
            rig.call(&rig.ctx(true, now), ACCOUNT_FOR_OP, BROCA, retry)
        };
        let gemini_only = [("gemini-5h", 10.0, 300)];
        // A newer fresh reading with headroom in one pool, omitting the other.
        read(&rig, "2026-09-29T12:02:00Z", &gemini_only);
        assert!(admit(&rig, NOW + 3 * 60_000, "pool-2").code().is_some());
        // The same partial reading again, after a restart.
        rig.reopen();
        read(&rig, "2026-09-29T12:04:00Z", &gemini_only);
        assert!(admit(&rig, NOW + 5 * 60_000, "pool-3").code().is_some());
        // Headroom in one pool but not the other.
        read(
            &rig,
            "2026-09-29T12:06:00Z",
            &[("gemini-5h", 10.0, 300), ("3p-weekly", 100.0, 10080)],
        );
        assert!(admit(&rig, NOW + 7 * 60_000, "pool-4").code().is_some());
        // Both pools above reserve: the block lifts.
        read(&rig, "2026-09-29T12:08:00Z", &both);
        let lifted = result(admit(&rig, NOW + 9 * 60_000, "pool-5"));
        assert_eq!(lifted["account"]["account_id"], ACCOUNT_A);
    }

    #[test]
    fn missing_recovery_pool_headroom_does_not_suppress_the_quota_probe() {
        let rig = provider_fixture(
            "partial-reading-probe",
            "antigravity",
            vec![pooled_entry(
                ACCOUNT_A,
                "2026-09-29T11:59:00Z",
                &[("gemini-5h", 10.0, 300), ("3p-weekly", 20.0, 10080)],
            )],
        );
        let mut params = admit_params("partial-first", "partial-conversation", "broca");
        params["provider"] = json!("google");
        params["model"] = json!("antigravity-gemini-3.8-flash");
        params["affinity_model"] = params["model"].clone();
        let first = result(rig.call(&rig.ctx(true, NOW), ACCOUNT_FOR_OP, BROCA, params.clone()));
        exhaust(
            &rig,
            "partial-e",
            "partial-first",
            NOW + 1_000,
            Value::Null,
            Value::Null,
        );
        let partial = pooled_entry(
            ACCOUNT_A,
            "2026-09-29T12:15:00Z",
            &[("gemini-5h", 10.0, 300)],
        );
        account_quota::apply_usage_reply(
            &mut rig.accounts.lock().unwrap(),
            &serde_json::to_vec(&json!({"result":[partial]})).unwrap(),
            NOW + 15 * 60_000,
        )
        .unwrap();
        params["admission_id"] = json!("partial-probe");
        params["candidates"] = json!({"view_digest":"runner-view","accounts":[{
            "account_id":first["account"]["account_id"],"credential_id":first["account"]["credential_id"]}]});
        let probe = rig.call(
            &rig.ctx(true, NOW + 16 * 60_000),
            ACCOUNT_FOR_OP,
            BROCA,
            params.clone(),
        );
        assert!(
            probe.code().is_none(),
            "a partial healthy reading must not disable the due probe: {probe:?}"
        );
        params["admission_id"] = json!("partial-second");
        assert!(rig
            .call(
                &rig.ctx(true, NOW + 16 * 60_000),
                ACCOUNT_FOR_OP,
                BROCA,
                params
            )
            .code()
            .is_some());
    }

    #[test]
    fn a_malformed_pool_name_is_refused_naming_the_member_and_an_unknown_one_is_accepted() {
        let fixture = fixture("pool-names");
        let ctx = fixture.ctx(true, NOW);
        let mut unknown = admit_params("pool-name-1", "broca:pool-name-1", "broca");
        unknown["pool"] = json!("never-seen.pool_1");
        result(fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, unknown));
        for bad in [
            String::new(),
            "a".repeat(65),
            "Upper".into(),
            "has space".into(),
        ] {
            let mut params = admit_params("pool-name-2", "broca:pool-name-2", "broca");
            params["pool"] = json!(bad);
            let OpReply::Error { code, detail, .. } =
                fixture.call(&ctx, ACCOUNT_FOR_OP, BROCA, params)
            else {
                panic!("{bad:?} accepted");
            };
            assert_eq!(code, "invalid_request");
            assert_eq!(detail.unwrap()["field"], "pool");
            let OpReply::Error { code, detail, .. } = fixture.call(
                &ctx,
                ACCOUNT_OBSERVE_OP,
                BROCA,
                json!({"event_id": "bad-pool", "caller": {"runner": "broca"},
                       "kind": "account_limit_exhausted", "pool": bad}),
            ) else {
                panic!("{bad:?} accepted on observe");
            };
            assert_eq!(code, "invalid_request");
            assert_eq!(detail.unwrap()["field"], "pool");
        }
    }

    /// A fence stored before blocks were keyed by pool keeps its effect: the
    /// stored row's scope decides its pool key, and its evidence still decodes.
    #[test]
    fn a_fence_stored_before_pool_keys_blocks_by_its_migrated_key() {
        // Every fixture lists the same accounts; this one only names one.
        let seed = fixture("legacy-fence");
        let first = result(seed.call(
            &seed.ctx(true, NOW),
            ACCOUNT_FOR_OP,
            BROCA,
            admit_params("legacy-1", "broca:legacy-1", "broca"),
        ));
        let account = first["account"]["account_id"].as_str().unwrap().to_string();
        for (id, scope) in [
            ("legacy-family", json!({"kind": "family", "value": "opus"})),
            (
                "legacy-unknown",
                json!({"kind": "unknown_scope", "value": "opus"}),
            ),
        ] {
            let rig = fixture(id);
            rig.store()
                .account_transaction(|tx| {
                    tx.insert_fence(&AccountFenceRow {
                        fence_id: id.into(),
                        provider: "anthropic".into(),
                        account_id: account.clone(),
                        scope_json: scope.to_string(),
                        fence_json: json!({
                            "account_id": account, "scope": scope,
                            "observed_at_ms": NOW, "reset": {"kind": "missing"},
                            "retry_hint_ms": null, "source": "broca_account_limit",
                        })
                        .to_string(),
                        source: "broca_account_limit".into(),
                        observed_at: NOW,
                        principal: BROCA.into(),
                        admission_id: None,
                        observation_id: None,
                        created_at: NOW,
                        cleared_at: None,
                        cleared_reason: None,
                    })
                })
                .unwrap();
            let ctx = rig.ctx(true, NOW + 1000);
            let opus = rig.call(
                &ctx,
                ACCOUNT_FOR_OP,
                BROCA,
                only_on(&first, "opus", "broca:opus"),
            );
            assert!(opus.code().is_some(), "{id}: blocks its family");
            let mut sonnet = only_on(&first, "sonnet", "broca:sonnet");
            sonnet["model"] = json!("claude-sonnet-5");
            sonnet["affinity_model"] = json!("claude-sonnet-5");
            let sonnet = result(rig.call(&ctx, ACCOUNT_FOR_OP, BROCA, sonnet));
            assert_eq!(sonnet["account"]["account_id"], account.as_str(), "{id}");
        }
    }

    /// Demand is read per roster account from the commit time the allocator
    /// still charges: after the account's reading less the reflection lag, or
    /// from the start when the account has no reading, may go on paid
    /// credits, or the strategy counts admissions of any age. Accounts outside
    /// the roster are not read.
    #[test]
    fn demand_reads_each_roster_account_only_from_the_admissions_that_can_count() {
        use prefrontal_routing_core::accounts::AllocationContext;
        let fixture = fixture("demand-floor");
        let store = fixture.store();
        let sticky = AllocatorPolicy::default();
        let observed = NOW - 30_000;
        let floor = observed - sticky.reflection_lag_ms;
        let admissions = [
            ("reflected", ACCOUNT_A, floor - 1),
            ("at-floor", ACCOUNT_A, floor),
            ("pending", ACCOUNT_A, floor + 1),
            ("unread", ACCOUNT_B, floor - 3_600_000),
            ("outside-roster", INSULA_ONLY, NOW),
        ];
        store
            .account_transaction(|tx| {
                tx.insert_pin(&AccountPinRow {
                    pin_id: "pin-1".into(),
                    principal: BROCA.into(),
                    conversation_hash: "conversation".into(),
                    provider: "anthropic".into(),
                    pin_generation: 1,
                    affinity_model: "claude-opus-5-5".into(),
                    account_id: ACCOUNT_A.into(),
                    credential_id: "oauth:anthropic".into(),
                    pin_mode: "run_frozen".into(),
                    previous_pin_id: None,
                    decision_id: "acd-1".into(),
                    created_at: floor - 3_600_000,
                })?;
                for (admission_id, account_id, committed_at) in admissions {
                    tx.insert_admission(&AccountAdmissionRow {
                        principal: BROCA.into(),
                        admission_id: admission_id.into(),
                        request_fingerprint: "fp".into(),
                        request_mode: "admit".into(),
                        decision_id: "acd-1".into(),
                        conversation_hash: "conversation".into(),
                        provider: "anthropic".into(),
                        model: "claude-opus-5-5".into(),
                        account_id: account_id.into(),
                        credential_id: "oauth:anthropic".into(),
                        pin_id: "pin-1".into(),
                        pin_generation: 1,
                        state: "finished".into(),
                        charged: true,
                        request_bytes: 1_000,
                        request_bytes_estimated: false,
                        committed_at,
                        admission_valid_until: committed_at + 60_000,
                        lease_valid_until: committed_at + 120_000,
                        reply_json: "{}".into(),
                        run_id: None,
                        started_at: None,
                        started_late: false,
                        released_at: None,
                        finished_at: Some(committed_at + 1),
                    })?;
                }
                Ok(())
            })
            .unwrap();
        let view = |account_id: &str, observed_at_ms: Option<i64>| AccountView {
            account_id: account_id.into(),
            routing_provider: "anthropic".into(),
            refresh_adapter: "anthropic".into(),
            credentials: Vec::new(),
            model_reach: ModelReach::All,
            quota: observed_at_ms.map(|observed_at_ms| QuotaReading {
                observed_at_ms: Some(observed_at_ms),
                error_class: None,
                stale: None,
                five_hour: None,
                seven_day: None,
                general_windows: None,
                scoped: ScopedList::Missing,
                pool_windows: None,
            }),
        };
        let roster = Roster {
            view_digest: "vault-view-1".into(),
            fetched_at_ms: NOW,
            accounts: vec![view(ACCOUNT_A, Some(observed)), view(ACCOUNT_B, None)],
        };
        let loaded = |policy: &AllocatorPolicy, context: &AllocationContext| {
            store
                .account_transaction(|tx| {
                    load_demand(tx, "anthropic", Some(&roster), policy, context)
                })
                .unwrap()
                .into_iter()
                .map(|demand| (demand.account_id, demand.committed_at_ms))
                .collect::<Vec<_>>()
        };
        let row = |account_id: &str, committed_at: i64| (account_id.to_string(), committed_at);
        let credits = |spendable: bool| AllocationContext {
            credits: BTreeMap::from([(
                ACCOUNT_A.to_string(),
                prefrontal_routing_core::accounts::CreditReading {
                    observed_at_ms: NOW,
                    spendable,
                    prepaid: false,
                },
            )]),
            ..AllocationContext::default()
        };

        // A reading bounds its account; an account without one is read whole.
        // A credit reading the allocator cannot spend changes nothing.
        let bounded = vec![row(ACCOUNT_A, floor + 1), row(ACCOUNT_B, floor - 3_600_000)];
        assert_eq!(loaded(&sticky, &AllocationContext::default()), bounded);
        assert_eq!(loaded(&sticky, &credits(false)), bounded);
        let no_credits = AllocatorPolicy {
            paid_credits: false,
            ..AllocatorPolicy::default()
        };
        assert_eq!(loaded(&no_credits, &credits(true)), bounded);
        // Counting strategies, and an account that may go on paid credits,
        // charge admissions of any age.
        let every_roster_admission = vec![
            row(ACCOUNT_A, floor - 1),
            row(ACCOUNT_A, floor),
            row(ACCOUNT_A, floor + 1),
            row(ACCOUNT_B, floor - 3_600_000),
        ];
        assert_eq!(loaded(&sticky, &credits(true)), every_roster_admission);
        for strategy in [AccountStrategy::RoundRobin, AccountStrategy::Hybrid] {
            let policy = AllocatorPolicy {
                strategy,
                ..AllocatorPolicy::default()
            };
            assert_eq!(
                loaded(&policy, &AllocationContext::default()),
                every_roster_admission,
                "{strategy:?}"
            );
        }
    }
}
