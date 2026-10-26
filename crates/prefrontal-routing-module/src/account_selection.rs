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
    AccountRequest, AccountView, AdmissionDemand, AdmissionState, AffinityOutcome, AllocatorPolicy,
    AllocatorState, ApplicableWindow, CallerKind, CandidateRef, ConfirmationSource,
    ConversationAffinity, ConversationKeySource, CredentialRecord, Decision, EpochMs, Fence,
    GeneralWindow, ModelFamily, ModelReach, ObservationSource, Outcome, PoolWindowReading,
    PrincipalExclusion, QuotaReading, QuotaWindow, RefusalCode, ResetTime, Roster,
    RunnerCandidates, ScopedList, ScopedWindow, StaleDisclosure,
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
use crate::config_overrides::AccountSelectionFlags;

/// Choose the account for one admission, renewal or reassignment.
pub const ACCOUNT_FOR_OP: &str = "route.account_for";
/// Read whether an account's quota has room for prompt-cache warming.
pub const ACCOUNT_STATUS_OP: &str = "route.account_status";
/// Report account-bound admission, dispatch and failure evidence.
pub const ACCOUNT_OBSERVE_OP: &str = "route.account_observe";
/// Release an admission that never started, or end a conversation's lease.
pub const ACCOUNT_RELEASE_OP: &str = "route.account_release";

/// The refusal every account operation answers while serving is off.
pub const DISABLED_CODE: &str = "account_selection_disabled";

/// A reservation that has not started within this long expires.
const ADMISSION_VALID_MS: i64 = 60_000;
/// How long a caller may dispatch on a pin without renewing it.
const LEASE_MS: i64 = 2 * 60_000;
/// Conversation affinity lasts this long idle.
const AFFINITY_IDLE_MS: i64 = 7 * 24 * 60 * 60_000;
/// Accepted use renews affinity at most this often.
const AFFINITY_TOUCH_MS: i64 = 60_000;
/// The longest short hold a caller waits out when it does not say.
const DEFAULT_HOLD_WAIT_S: u64 = 900;
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
    pub credentials: crate::broca_credentials::BrocaCredentials,
    pub config: std::sync::Arc<crate::config_overrides::LiveConfig>,
    pub facts: Vec<prefrontal_routing_core::ModelFact>,
    pub refusals: Vec<prefrontal_routing_core::route_state_store::CredentialEntitlementFence>,
    pub quota: AccountQuotaState,
    pub fences: BTreeMap<String, Vec<Fence>>,
    pub now_ms: i64,
    eligibility: Vec<prefrontal_routing_store::AccountEligibilityFact>,
    size_refusals: Vec<prefrontal_routing_store::AccountSizeRefusal>,
    catalog: BTreeSet<String>,
    targets: BTreeMap<String, String>,
}

pub(crate) struct RouteClass<'a> {
    pub rows: Vec<&'a cortexkit_claustrum_client::ListedCredential>,
    pub subscription: bool,
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
        let refusals = crate::credential_fences::live(store, credentials)?;
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
        let size_refusals = store
            .account_size_refusals(now_ms)
            .map_err(|error| error.to_string())?;
        let catalog = facts.iter().map(|fact| fact.provider_id.clone()).collect();
        let targets = credentials.catalog_targets(&facts);
        prefrontal_routing_core::score_alias::append_alias_rows(&mut facts, &targets);
        let quota = accounts
            .and_then(|accounts| accounts.lock().ok().map(|state| state.clone()))
            .unwrap_or_default();
        let fences = store
            .account_transaction(|tx| {
                facts
                    .iter()
                    .map(|fact| fact.provider_id.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .map(|provider| Ok((provider.clone(), load_fences(tx, &provider)?)))
                    .collect()
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            credentials,
            config,
            facts,
            refusals,
            quota,
            fences,
            now_ms,
            eligibility,
            size_refusals,
            catalog,
            targets,
        })
    }

    pub(crate) fn class(
        &self,
        fact: &prefrontal_routing_core::ModelFact,
        priced_per_token: bool,
    ) -> RouteClass<'_> {
        let specs = prefrontal_routing_core::provider_model_spec::parse_spec_list(
            &self.config.operator_policy().subscription,
        );
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

    fn view(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &prefrontal_routing_core::ModelFact,
    ) -> AccountView {
        let account_id = row.account_id.clone().unwrap_or_else(|| row.id.clone());
        let capacity = format!("{}/{}", fact.provider_id, fact.model_id);
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
            account_id,
            routing_provider: fact.provider_id.clone(),
            refresh_adapter: row.refresh_adapter.clone().unwrap_or_default(),
            credentials: vec![CredentialRecord {
                credential_id: row.id.clone(),
                default_record: is_default_record(&row.id),
                needs_reauth: false,
            }],
            model_reach,
            quota: reading
                .and_then(|reading| quota_reading_for_model(&reading.entry, Some(&capacity))),
        }
    }

    fn eligible(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &prefrontal_routing_core::ModelFact,
    ) -> bool {
        !self.eligibility.iter().any(|fact_| {
            fact_.provider == fact.provider_id
                && fact_.credential_id == row.id
                && (fact_.model.is_empty() || fact_.model == fact.model_id)
        }) && !self.refusals.iter().any(|fence| {
            fence.provider_id == fact.provider_id
                && fence.credential_id == row.id
                && (self.targets.contains_key(&fact.provider_id)
                    || self.now_ms.saturating_sub(fence.observed_at_ms)
                        <= crate::credential_fences::FENCE_PROBE_RECHECK_MS)
        })
    }

    fn probe_eligible(&self, provider: &str, credential: &str) -> bool {
        !self.targets.contains_key(provider)
            && self.refusals.iter().any(|fence| {
                fence.provider_id == provider
                    && fence.credential_id == credential
                    && self.now_ms.saturating_sub(fence.observed_at_ms)
                        > crate::credential_fences::FENCE_PROBE_RECHECK_MS
            })
    }

    /// A stale refusal is a fallback, never a reason to probe while an
    /// unfenced eligible identity in the same payment class can serve.
    fn eligible_rows<'a>(
        &self,
        rows: Vec<&'a cortexkit_claustrum_client::ListedCredential>,
        fact: &prefrontal_routing_core::ModelFact,
    ) -> Vec<&'a cortexkit_claustrum_client::ListedCredential> {
        let mut rows: Vec<_> = rows
            .into_iter()
            .filter(|row| self.eligible(row, fact))
            .collect();
        if rows
            .iter()
            .any(|row| !self.probe_eligible(&fact.provider_id, &row.id))
        {
            rows.retain(|row| !self.probe_eligible(&fact.provider_id, &row.id));
        }
        rows
    }

    fn usable(
        &self,
        row: &cortexkit_claustrum_client::ListedCredential,
        fact: &prefrontal_routing_core::ModelFact,
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
                covered(other) == covered(row)
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
                    fences: self
                        .fences
                        .get(&fact.provider_id)
                        .cloned()
                        .unwrap_or_default(),
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
        let fact = self
            .facts
            .iter()
            .find(|fact| fact.provider_id == provider && fact.model_id == model);
        let mut accounts: BTreeMap<String, AccountView> = BTreeMap::new();
        if let Some(fact) = fact {
            for row in self.eligible_rows(self.class(fact, priced_per_token).rows, fact) {
                let view = self.view(row, fact);
                if let Some(existing) = accounts.get_mut(&view.account_id) {
                    existing.credentials.extend(view.credentials);
                } else {
                    accounts.insert(view.account_id.clone(), view);
                }
            }
        }
        RosterBuild {
            roster: Some(self.roster(accounts.into_values().collect())),
            context: json!({"provider": provider, "account_classes": true}),
        }
    }

    fn method(&self, credential: &str) -> &str {
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

    /// Inspect exhaustion with the allocator's own filters. Relaxing wait limits
    /// reveals a known reset; removing reserve floors distinguishes exhaustion
    /// from policy headroom. Eligibility and missing evidence remain refusals.
    pub(crate) fn blocked_reset(
        &self,
        provider: &str,
        model: &str,
        priced: bool,
    ) -> Option<Option<i64>> {
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
        let mut policy = self.config.allocator_policy(provider);
        policy.max_hold_s = i64::MAX as u64 / 1000;
        policy.short_hold_max_ms = i64::MAX;
        policy.reserve_five_hour = 0.0;
        policy.reserve_seven_day = 0.0;
        policy.reserve_scoped = 0.0;
        policy.broca_floor_points = 0.0;
        policy.soft_quota_remaining_percent = None;
        policy.pool_floors.clear();
        let reset = self.probe_with_policy(
            provider,
            model,
            priced,
            true,
            Some(policy.max_hold_s),
            "blocked-preflight",
            policy,
        );
        if let Some(hold) = reset.hold() {
            return Some(Some(hold.reset_at_ms));
        }
        let exhausted = !reset.explanation.accounts.is_empty()
            && reset.explanation.accounts.iter().all(|account| {
                account.exclusions.iter().any(|reason| matches!(reason,
                prefrontal_routing_core::accounts::ExclusionReason::ConfirmedFence { .. }
                | prefrontal_routing_core::accounts::ExclusionReason::ScopeExhausted { .. }
                | prefrontal_routing_core::accounts::ExclusionReason::BelowReserve { .. }))
            });
        exhausted.then_some(None)
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
        policy: AllocatorPolicy,
    ) -> Decision {
        let mut request = synthetic_request(provider, model, conversation.into());
        if let Some(wait) = wait {
            request.hold_wait_max_s = wait;
        }
        let mut inputs = self.allocation_context(provider, model);
        inputs.hold_ok = Some(hold_ok);
        prefrontal_routing_core::accounts::decide_with_context(
            &request,
            &AllocatorState {
                roster: self.roster_for(provider, model, priced).roster,
                affinity: None,
                fences: self.fences.get(provider).cloned().unwrap_or_default(),
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

    pub(crate) fn needs_credential_pin(&self, provider: &str, model: &str) -> bool {
        self.refusals
            .iter()
            .any(|fact| fact.provider_id == provider)
            || self.eligibility.iter().any(|fact| {
                fact.provider == provider && (fact.model.is_empty() || fact.model == model)
            })
            || self
                .size_refusals
                .iter()
                .any(|fact| fact.provider == provider && fact.model == model)
    }

    fn allocation_context(
        &self,
        provider: &str,
        model: &str,
    ) -> prefrontal_routing_core::accounts::AllocationContext {
        let mut context = prefrontal_routing_core::accounts::AllocationContext::default();
        for fence in self.refusals.iter().filter(|fence| {
            fence.provider_id == provider && self.probe_eligible(provider, &fence.credential_id)
        }) {
            context.credential_probe_notes.insert(
                fence.credential_id.clone(),
                crate::credential_fences::probe_note(provider, &fence.credential_id),
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
        if matches!(
            pool.get("id").and_then(Value::as_str),
            Some("extra_usage" | "credits")
        ) {
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
        assert_eq!(context.credits["a"].observed_at_ms, at);
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
fn quota_reading_for_model(entry: &Value, capacity_key: Option<&str>) -> Option<QuotaReading> {
    let entry: AccountEntry = serde_json::from_value(entry.clone()).ok()?;
    let usage = entry.usage.as_ref();
    let reported = usage
        .map(|usage| {
            prefrontal_routing_core::quota_extract::account_rate_windows(usage, capacity_key)
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
    allocator_roster_for_model(state, provider, None, now_ms)
}

fn allocator_roster_for_model(
    state: &AccountQuotaState,
    provider: &str,
    model: Option<&str>,
    now_ms: i64,
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
                        quota_reading_for_model(&reading.entry, capacity_key.as_deref())
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
        Some(Ok(guard)) => allocator_roster_for_model(&guard, provider, model, ctx.now_ms),
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
) -> Vec<AccountView> {
    allocator_roster_for_model(state, provider, Some(model), now_ms)
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
                        observed_at_ms: evidence.observed_at_ms,
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

/// Bring the standing blocks on `provider` up to date with the accounts'
/// latest readings: clear every block that ended (its reset passed, its retry
/// time came, or newer headroom lifted it), and add to each whole-account
/// block's recovery set the pools a later fresh reading reports. Clearing a
/// lifted block keeps a later partial reading from bringing it back.
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
        for provider in tx.active_fence_providers()? {
            let accounts = allocator_roster_for_model(state, &provider, None, now)
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

/// The pool a request's model draws on: the one the caller names, or else
/// the pool the reading assigns that model's route, if any.
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

fn load_demand(tx: &AccountTx<'_>, provider: &str) -> RouteStateResult<Vec<AdmissionDemand>> {
    Ok(tx
        .demand(provider)?
        .into_iter()
        .filter_map(|row| {
            let state: AdmissionState = serde_json::from_value(json!(row.state)).ok()?;
            Some(AdmissionDemand {
                account_id: row.account_id,
                state,
                committed_at_ms: row.committed_at,
                request_bytes: row.request_bytes,
            })
        })
        .collect())
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
    #[serde(default)]
    #[allow(dead_code)]
    harness: Option<String>,
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
            return Err(invalid("`admission_id` is required and must not be empty"));
        }
        if self.conversation_key.trim().is_empty() {
            return Err(invalid("`conversation_key` must not be empty"));
        }
        if self.provider.trim().is_empty() || self.model.trim().is_empty() {
            return Err(invalid("`provider` and `model` must not be empty"));
        }
        if let Some(reply) = invalid_pool(self.pool.as_deref()) {
            return Err(reply);
        }
        if self.mode != RequestMode::Admit && self.expected_pin.is_none() {
            return Err(invalid(format!(
                "mode `{}` requires `expected_pin` as {{id, generation}}",
                self.mode.as_str()
            )));
        }
        Ok(())
    }

    fn caller_kind(&self) -> CallerKind {
        if self.caller.runner.eq_ignore_ascii_case("broca") {
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
            conversation_key_source: if self.conversation_key.starts_with("broca:") {
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
    let parsed: AccountForParams = serde_json::from_value(params.clone())
        .map_err(|error| invalid(format!("invalid route.account_for params: {error}")))?;
    parsed.validate()?;
    Ok(parsed)
}

/// Handle one account-selection operation. `body` is the whole request
/// (`{"method", "params"}`); `principal` is the route's attested principal.
pub fn handle(method: &str, body: &[u8], principal: &str, ctx: &Ctx<'_>) -> OpReply {
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

fn account_status(params: &Value, ctx: &Ctx<'_>) -> OpReply {
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
        let built = allocator_roster_for_model(&state, &provider, None, ctx.now_ms);
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
        let Some(reading) = quota_reading_for_model(&stored.entry, None) else {
            return WarmReason::ReadingMissing;
        };
        let freshness = assess_reading(Some(&reading), ctx.now_ms, ctx.policy);
        band = freshness.age_band;
        if let Some(usage) = &entry.usage {
            // Use the allocator's topology: pooled readings ignore misleading
            // aggregate slots and infer pool durations from their producer ids.
            let reported =
                prefrontal_routing_core::quota_extract::account_rate_windows(usage, None);
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

fn account_for(params: &Value, principal: &str, ctx: &Ctx<'_>) -> OpReply {
    let parsed = match parse_account_for(params) {
        Ok(parsed) => parsed,
        Err(reply) => return reply,
    };
    let provider = normalize(&parsed.provider);
    let request_fingerprint = fingerprint(&parsed);
    match ctx.store.account_transaction(|tx| {
        tx.admission(principal, &parsed.admission_id)?
            .map(|row| replay(tx, principal, row, &request_fingerprint, ctx.now_ms))
            .transpose()
    }) {
        Ok(Some(reply)) => return reply,
        Ok(None) => {}
        Err(error) => return store_error(error),
    }
    let class = match parsed.model_decision_id.as_deref() {
        None => prefrontal_routing_core::accounts::AllowedAccountClass::NotPayPerToken,
        Some(id) => match ctx.store.v1_decision(id) {
            Ok(Some(decision)) => decision.allowed_account_class,
            Ok(None) => match ctx.store.decision_account_class(id) {
                Ok(Some(class)) => class,
                Ok(None) => {
                    return OpReply::error("unknown_decision", "selection decision does not exist")
                }
                Err(error) => return store_error(error),
            },
            Err(error) => return store_error(error),
        },
    };
    let mut roster = match ctx.classes.as_ref() {
        Some(classes) => classes.roster_for(
            &provider,
            &normalize(&parsed.model),
            class == prefrontal_routing_core::accounts::AllowedAccountClass::PricedPerTokenOnly,
        ),
        None => roster_for_model(ctx, &provider, Some(&normalize(&parsed.model))),
    };
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
    if !roster
        .roster
        .as_ref()
        .is_some_and(|roster| !roster.accounts.is_empty())
    {
        let code = if ctx.classes.is_some() {
            "no_eligible_account"
        } else {
            "account_selection_unsupported"
        };
        return OpReply::typed(
            code,
            format!("routing selects no accounts for provider `{provider}`"),
            json!({
                "code": code,
                "retry_after_s": null,
                "decision_id": null,
                "exclusions": [],
            }),
        );
    }
    // The roster is read before the transaction: no lock or fetch happens
    // while the selection transaction is open.
    ctx.store
        .account_transaction(|tx| {
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
            return Err((
                "no_eligible_account".into(),
                format!("{provider}/{model}: no eligible account"),
            ));
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
        .account_transaction(|tx| {
            let fences = load_fences(tx, provider)?;
            let decision = prefrontal_routing_core::accounts::decide_with_context(
                &request,
                &AllocatorState {
                    roster: roster.roster.clone(),
                    affinity: None,
                    fences: fences.clone(),
                    principal_exclusions: Vec::new(),
                    admissions: load_demand(tx, provider)?,
                    operator_override: None,
                    policy: policy.clone(),
                },
                ctx.now_ms,
                &inputs,
            );
            Ok((decision, fences))
        })
        .map_err(|error| ("account_selection_unavailable".into(), error.to_string()))?;
    match decision.outcome {
        Outcome::Admit(_) => Ok(()),
        Outcome::Hold(hold) => Err((
            "quota_exhausted".into(),
            format!(
                "{provider}/{model}: quota_exhausted until {}",
                rfc3339(hold.reset_at_ms)
            ),
        )),
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
            let clearing = if matches!(
                refusal.code,
                RefusalCode::AccountPoolExhausted | RefusalCode::AccountPoolReserveLimited
            ) {
                resets
                    .filter(|date| *date > ctx.now_ms)
                    .min()
                    .map(|date| format!(" until {}", rfc3339(date)))
                    .unwrap_or_else(|| "; clearing time unknown".into())
            } else {
                String::new()
            };
            Err((
                code.clone(),
                format!("{provider}/{model}: {code}{clearing}"),
            ))
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
    let now = ctx.now_ms;
    let policy = ctx.provider_policy(provider);
    if let Some(existing) = tx.admission(principal, &params.admission_id)? {
        return replay(tx, principal, existing, request_fingerprint, now);
    }
    let salt = tx.salt(now, fresh_salt)?;
    let hash = conversation_hash(&salt, principal, &params.conversation_key);
    let affinity_row = tx.affinity(principal, &hash, provider)?;
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
    let mut state = AllocatorState {
        roster: roster.roster.clone(),
        affinity: affinity_row
            .as_ref()
            .map(|row| conversation_affinity(row, now)),
        fences: load_fences(tx, provider)?,
        principal_exclusions: load_exclusions(tx, principal, &params.admission_id)?,
        admissions: load_demand(tx, provider)?,
        operator_override: None,
        policy,
    };
    let request = params.request(principal, affinity_row.is_none());
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
    let decision = loop {
        let decision =
            prefrontal_routing_core::accounts::decide_with_context(&request, &state, now, &inputs);
        let Outcome::Admit(admission) = &decision.outcome else {
            break decision;
        };
        let credential = admission.credential_id.clone();
        if !inputs.credential_probe_notes.contains_key(&credential) {
            break decision;
        }
        match tx.claim_credential_probe(
            provider,
            &credential,
            now.saturating_sub(crate::credential_fences::FENCE_PROBE_RECHECK_MS),
            now,
        )? {
            prefrontal_routing_store::CredentialProbeClaim::Claimed => break decision,
            prefrontal_routing_store::CredentialProbeClaim::Unfenced => {
                inputs.credential_probe_notes.remove(&credential);
            }
            prefrontal_routing_store::CredentialProbeClaim::Blocked => {
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
    let decision_id = new_id("acd");
    tx.insert_decision(&decision_record(RecordInput {
        decision_id: &decision_id,
        mode: DECISION_MODE_SERVE,
        trigger: "request",
        principal,
        admission_id: Some(&params.admission_id),
        request_mode: Some(params.mode.as_str()),
        decision: &decision,
        context: json!({ "roster": roster.context }),
        now_ms: now,
    }))?;
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
            let code = if ctx.classes.is_some() {
                "no_eligible_account"
            } else {
                refusal_code(refusal.code)
            };
            let family = ctx.policy.family_for_model(&params.model);
            let reset = state
                .fences
                .iter()
                .filter(|fence| {
                    fence.applies_to(&normalize(&params.model), family, request.pool.as_deref())
                })
                .filter(|fence| {
                    roster
                        .roster
                        .iter()
                        .flat_map(|roster| &roster.accounts)
                        .any(|account| account.account_id == fence.account_id)
                })
                .filter_map(|fence| fence.reset.at_ms())
                .chain(
                    roster
                        .roster
                        .iter()
                        .flat_map(|roster| &roster.accounts)
                        .filter_map(|account| account.quota.as_ref())
                        .flat_map(|quota| {
                            let mut dates: Vec<_> = quota
                                .required_windows()
                                .into_iter()
                                .filter_map(|window| quota.window(window))
                                .filter(|window| window.used_percent >= 100.0)
                                .filter_map(|window| window.resets_at.at_ms())
                                .collect();
                            if let ScopedList::Listed(windows) = &quota.scoped {
                                dates.extend(
                                    windows
                                        .iter()
                                        .filter(|window| {
                                            ctx.policy.family_for_scope(window) == Some(family)
                                        })
                                        .filter(|window| window.used_percent >= 100.0)
                                        .filter_map(|window| window.resets_at.at_ms()),
                                );
                            }
                            dates
                        }),
                )
                .filter(|reset| *reset > now)
                .min();
            Ok(OpReply::typed(
                code,
                format!("no account admitted: {code}"),
                json!({
                    "code": code,
                    "retry_after_s": refusal.retry_after_s,
                    "decision_id": decision_id,
                    "exclusions": wire_exclusions(&decision),
                    "reset_at_ms": reset,
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
    /// `admission_started`, `run_finished`, `attempt`, `scoped_not_found` or
    /// `account_limit_exhausted`. Any other kind is kept as advisory.
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
    let parsed: ObserveParams = match serde_json::from_value(params.clone()) {
        Ok(parsed) => parsed,
        Err(error) => return invalid(format!("invalid route.account_observe params: {error}")),
    };
    if parsed.event_id.trim().is_empty() {
        return invalid("`event_id` must not be empty");
    }
    if let Some(reply) = invalid_pool(parsed.pool.as_deref()) {
        return reply;
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
    ctx.store
        .account_transaction(|tx| {
            record_observation(tx, ctx, principal, &parsed, params, &roster_digests)
        })
        .unwrap_or_else(store_error)
}

/// Resolve response headers only through the admission that this route principal owns.
pub(crate) fn usage_observation(
    body: &[u8],
    principal: &str,
    ctx: &Ctx<'_>,
) -> Option<crate::usage_observe::UsageObservation> {
    if !ctx.flags.enabled {
        return None;
    }
    let params = decode_params(body).ok()?;
    let parsed: ObserveParams = serde_json::from_value(params).ok()?;
    if !matches!(parsed.kind.as_str(), "attempt" | "run_finished")
        || parsed.event_id.trim().is_empty()
    {
        return None;
    }
    let admission_id = parsed.admission_id.as_deref()?;
    let admission = ctx
        .store
        .account_transaction(|tx| tx.admission(principal, admission_id))
        .ok()??;
    let is_anthropic_subscription = ctx.accounts.is_some_and(|accounts| {
        accounts.lock().is_ok_and(|state| {
            state.roster.as_ref().is_some_and(|roster| {
                roster.identities.iter().any(|identity| {
                    identity.account_id == admission.account_id
                        && identity.refresh_adapter == "anthropic"
                        && identity.credential_ids.contains(&admission.credential_id)
                })
            })
        })
    });
    let headers = parsed
        .headers
        .into_iter()
        .filter(|(name, _)| name.starts_with("anthropic-ratelimit-unified-"))
        .collect();
    crate::usage_observe::observation_for_admission(
        &admission.provider,
        Some(&admission.account_id),
        is_anthropic_subscription,
        parsed.observed_at_ms.unwrap_or(ctx.now_ms),
        headers,
    )
}

fn record_observation(
    tx: &AccountTx<'_>,
    ctx: &Ctx<'_>,
    principal: &str,
    parsed: &ObserveParams,
    raw: &Value,
    roster_digests: &BTreeMap<String, ProviderRosterFacts>,
) -> RouteStateResult<OpReply> {
    let now = ctx.now_ms;
    let runner = normalize(&parsed.caller.runner);
    if let Some(existing) = tx.observation_by_event(principal, &runner, &parsed.event_id)? {
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
    // Mutation authorization: only an admission this principal was issued
    // binds the evidence, and only to the account that admission assigned.
    let admission = match &parsed.admission_id {
        Some(id) => tx.admission(principal, id)?,
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
    match &admission {
        None => effects.push(json!({
            "effect": "advisory",
            "why": "names no admission routing issued to this principal",
        })),
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
    if !OBSERVATION_KINDS.contains(&parsed.kind.as_str()) {
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
            tx.insert_exclusion(&AccountExclusionRow {
                principal: principal.to_string(),
                admission_id: id.clone(),
                credential_id: credential_id.to_string(),
                runner_view_digest: view_digest.to_string(),
                roster_view_digest: roster_digest,
                observation_id: Some(observation_id.to_string()),
                recorded_at: now,
            })?;
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
        let reading = quota_reading_for_model(entry, None).unwrap();
        let evaluate = |reading: &QuotaReading, now| {
            evaluate_quota(
                Some(reading),
                ModelFamily::Opus,
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
        let reading = quota_reading_for_model(&past_reset, None).unwrap();
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
        let dir = prefrontal_test_support::scratch_root().join(format!(
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
            policy: AllocatorPolicy::default(),
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
        prefrontal_routing_core::merge_quota_overlay(&mut snapshot, &windows, &facts);
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
                is_subscription_route: None,
            })
            .unwrap()
        };
        assert_eq!(seat(snapshot.clone()).members[0].route_key, route);
        // The all-exhausted control proves seating still honors a real pool wall.
        healthy["usage"]["secondary"]["usedPercent"] = json!(100);
        let windows: Vec<prefrontal_routing_core::ProviderUsage> =
            serde_json::from_value(json!([exhausted, healthy])).unwrap();
        prefrontal_routing_core::merge_quota_overlay(&mut snapshot, &windows, &facts);
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
        assert_eq!(
            rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params)
                .code(),
            Some("account_selection_unsupported")
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
        assert_eq!(
            rig.call(&rig.ctx(true, NOW), "route.account_for", BROCA, params)
                .code(),
            Some("account_selection_unsupported")
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
        assert_eq!(
            rig.call(&ctx, "route.account_for", BROCA, params.clone())
                .code(),
            Some("account_selection_unsupported")
        );
        params["admission_id"] = json!("antigravity-cold");
        params["conversation_key"] = json!("antigravity-cold");
        assert_eq!(
            rig.call(&ctx, "route.account_for", BROCA, params).code(),
            Some("account_selection_unsupported")
        );
        let mut unknown = admit_params("unknown-pool", "unknown-pool", "broca");
        unknown["provider"] = json!("google");
        unknown["model"] = json!("antigravity-new-family-model");
        unknown["affinity_model"] = unknown["model"].clone();
        assert_eq!(
            rig.call(&ctx, "route.account_for", BROCA, unknown).code(),
            Some("account_selection_unsupported")
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
        assert_eq!(fixture.store().account_serving_counts().unwrap().fences, 1);

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
    fn a_whole_account_block_ends_at_its_reset_and_a_new_exhaustion_blocks_again() {
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
            at(reset, "trial-2").code().is_some(),
            "blocked until the reset passes"
        );
        let trial = result(at(reset + 1000, "trial-3"));
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
    fn an_unknown_reset_retries_after_six_hours_or_at_the_retry_hint() {
        let confirmed = NOW + 1000;
        for (label, resets_at, source, retry_at) in [
            (
                "six-hours",
                Value::Null,
                Value::Null,
                confirmed + 6 * 3_600_000,
            ),
            (
                "hint",
                json!(confirmed + 30 * 60_000),
                json!("retry_after"),
                confirmed + 30 * 60_000,
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
}
