//! The route-state store DOMAIN trait: backend-agnostic.
//!
//! `-core` names only the trait (and its row/enum types); it pulls in NO sqlite
//! dependency, so the pure scorer + the module + any consumer stay free of a
//! database backend. The sqlite implementation lives in `prefrontal-routing-store`
//! (consuming the shared `cortexkit-store` lib), and a postgres sibling is a clean
//! drop-in later: exactly the core-names-trait + backend-crate layering the port
//! playbook prescribes (and that llm-runner proves).
//!
//! Ported from the Alfonso TS plugin's `ModelRouteStore`
//! (`src/features/model-routing/route-state-store.ts`), scoped to the route-owned
//! domain. Quota windows are NOT here; they live in `insula` (formerly `ai-provider-quota`) and are
//! consumed module-to-module (see `crate::quota`).

use crate::model_facts::{ClassVector, FamilySource, GrandFamily, ModelFact, RoutePricing};
use crate::model_router::ModelRouteStateSnapshot;

/// Provenance of a stored fact. The closed vocabulary the router and listing ops
/// speak is `curated` / `override` / `aa_inferred` (plus `unscored` as a catalog
/// state, not a score). Existing rows keep `source='seed'`; that stamp is curated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactSource {
    Seed,
    Override,
    AaInferred,
    Unscored,
}

/// Catalog fact fields that an operator can override independently of score policy.
/// Fusiform owns these facts unless the same field has this explicit override stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogFactField {
    ContextWindow,
    MaxOutputTokens,
    CostTier,
    CostPerMtokOut,
    SupportsVision,
    SupportsToolUse,
}

impl FactSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            FactSource::Seed => "seed",
            FactSource::Override => "override",
            FactSource::AaInferred => "aa_inferred",
            FactSource::Unscored => "unscored",
        }
    }

    pub fn parse(value: &str) -> Self {
        match value {
            "override" => FactSource::Override,
            "aa_inferred" => FactSource::AaInferred,
            "unscored" => FactSource::Unscored,
            // `curated` is the closed vocabulary; `seed` is the historical stamp.
            _ => FactSource::Seed,
        }
    }

    pub fn provenance(&self) -> crate::model_facts::ScoreProvenance {
        match self {
            FactSource::Seed => crate::model_facts::ScoreProvenance::Curated,
            FactSource::Override => crate::model_facts::ScoreProvenance::Override,
            FactSource::AaInferred => crate::model_facts::ScoreProvenance::AaInferred,
            FactSource::Unscored => crate::model_facts::ScoreProvenance::Unscored,
        }
    }

    /// `aa_inferred` never overwrites these stamps; `override` always wins over it.
    pub fn blocks_aa_inferred(&self) -> bool {
        matches!(self, FactSource::Seed | FactSource::Override)
    }
}

/// Errors a route-state store backend can surface. Kept backend-agnostic (a
/// `String` detail) so the trait does not leak `rusqlite`/`tokio-postgres` types
/// into `-core`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteStateStoreError(pub String);

impl std::fmt::Display for RouteStateStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "route-state store: {}", self.0)
    }
}

impl std::error::Error for RouteStateStoreError {}

pub type RouteStateResult<T> = Result<T, RouteStateStoreError>;

/// Cause of a live quota/cooldown wall. NULL in storage remains unknown so rows
/// written before the typed cause was introduced retain their prior behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteWallClass {
    /// An account-local retry interval, never a hard model-admission wall.
    Transient,
    Quota,
    Billing,
}

impl RouteWallClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Quota => "quota",
            Self::Billing => "billing",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "transient" => Ok(Self::Transient),
            "quota" => Ok(Self::Quota),
            "billing" => Ok(Self::Billing),
            other => Err(format!("unknown route wall class {other:?}")),
        }
    }
}

/// A partial update to a route's runtime telemetry. `None` fields are left as-is
/// (the existing stored value survives); `Some(value)` overwrites. The two
/// increment flags add to the persisted counters. Mirrors the TS `RouteStateUpdate`.
#[derive(Debug, Clone, Default)]
pub struct RouteStateUpdate {
    pub cooldown_until: Option<f64>,
    pub quota_exhausted_until: Option<f64>,
    pub wall_class: Option<RouteWallClass>,
    pub quota_pressure: Option<f64>,
    pub latency_ema_ms: Option<f64>,
    pub success_ema: Option<f64>,
    pub last_error: Option<String>,
    pub increment_failure: bool,
    pub increment_quota_error: bool,
}

/// The provider-wide durable horizons used to inherit cooldown state into every
/// route minted for the provider. Horizons are merged independently and never
/// shortened by an older signal.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProviderCooldownState {
    pub provider_key: String,
    pub cooldown_until: Option<f64>,
    pub quota_exhausted_until: Option<f64>,
    pub quota_pressure: Option<f64>,
    pub wall_class: Option<RouteWallClass>,
}

/// Terminal outcome recorded against a route decision (audit + routing learning).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteOutcome {
    Completed,
    /// The provider answered, but the reply failed the caller's output contract.
    /// Kept distinct in the decision ledger without charging route availability.
    ContractError,
    Error,
    SessionError,
    Cancelled,
    Interrupted,
    LaunchFailed,
    QuotaExhausted {
        retry_after_secs: u64,
    },
}

impl RouteOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            RouteOutcome::Completed => "completed",
            RouteOutcome::ContractError => "contract_error",
            RouteOutcome::Error => "error",
            RouteOutcome::SessionError => "session_error",
            RouteOutcome::Cancelled => "cancelled",
            RouteOutcome::Interrupted => "interrupted",
            RouteOutcome::LaunchFailed => "launch_failed",
            RouteOutcome::QuotaExhausted { .. } => "quota_exhausted",
        }
    }

    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            RouteOutcome::QuotaExhausted { retry_after_secs } => Some(*retry_after_secs),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionOutcomeUpdate {
    pub provider_outcome: Option<ProviderOutcome>,
    pub outcome: RouteOutcome,
    pub last_error: Option<String>,
    pub observed_at_ms: i64,
    pub failure_backoff: crate::failure_backoff::FailureBackoffConfig,
}

/// Provider identity and optional absolute quota reset for one executed request.
/// Kept separate from model health: a large request can exhaust an allowance while
/// smaller requests on the same model and credential still succeed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderOutcome {
    pub account_ref: String,
    pub credential_id: Option<String>,
    pub resets_at_ms: Option<i64>,
    /// A permanent refusal of the credential itself: the provider's
    /// `ENTITLEMENT_ERROR` (the account has no plan for the provider's models) or
    /// `billing_exhausted` (its paid balance is empty). Optional on the wire: a
    /// routing build that predates the field ignores it, and a core build that
    /// predates it never sends it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_refusal: Option<CredentialRefusal>,
}

/// A provider's permanent refusal of one vault credential, as the run that hit it
/// reported it. Routing stops choosing the provider through that credential until
/// the credential's vault listing changes; see [`CredentialEntitlementFence`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialRefusal {
    /// Routing's provider id the refused run was launched on.
    pub provider_id: String,
    /// The vault credential Broca reported for the run: its frozen
    /// `auth_selection`, or the `credential_id` a billing error names.
    pub credential_id: String,
    /// The provider's refusal code as Broca reported it (`ENTITLEMENT_ERROR`,
    /// `billing_exhausted`).
    pub code: String,
}

/// A stored (provider, credential) pair that routing does not select
/// automatically. `listing_fingerprint` is the credential's vault listing
/// (provider ids, plan tier and state) when the refusal arrived; the fence ends
/// when the listing no longer matches it, when a recheck probe on the pair
/// succeeds, or when an operator lifts it. A stale fence (one whose refusal is
/// older than the recheck interval) admits a single probe request, which renews
/// its observation clock whether it succeeds or fails again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialEntitlementFence {
    pub provider_id: String,
    pub credential_id: String,
    pub code: String,
    /// `None` when routing had no vault listing for the credential at the time.
    pub listing_fingerprint: Option<String>,
    pub observed_at_ms: i64,
}

/// The route-local failure-backoff fields before an operator clears them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FailureBackoffState {
    pub consecutive_terminal_failures: u32,
    pub failure_backoff_until: Option<f64>,
    pub failure_backoff_reason: Option<String>,
}

/// Route-local wall horizons before an operator clears them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CooldownState {
    pub cooldown_until: Option<f64>,
    pub quota_exhausted_until: Option<f64>,
}

/// What `record_decision` persists: the selection (or none) + its audit context.
/// `score_json`/`rejected_json`/`explanation_json` are pre-serialized by the
/// caller (the module serializes the scorer's `SelectedModelRoute`), keeping this
/// trait free of the scorer's wire shape.
#[derive(Debug, Clone, Default)]
pub struct DecisionInput {
    pub allowed_account_class: crate::accounts::AllowedAccountClass,
    /// Request class supplied by the launcher, independent of the target agent.
    pub consumer: Option<String>,
    /// Allowed provider IDs for this selection; absent means no provider restriction.
    pub allowed_providers_json: Option<String>,
    pub decision_id: Option<String>,
    /// `single` keeps the historical shape; panel rows leave singular columns null.
    pub decision_kind: String,
    pub task_id: Option<String>,
    pub parent_session_id: Option<String>,
    /// The caller's harness, copied from the selection input when one was provided.
    pub harness: Option<String>,
    /// The caller's project directory, used to match the decision with recorded spend facts.
    pub project_root: Option<String>,
    /// The Entorhinal project id the daemon stamped on the caller's scope, when
    /// the route was opened under one. Attribution only: absent for an unscoped
    /// route, and never derived from the request or from `project_root`.
    pub project_id: Option<String>,
    /// The session folder the daemon stamped on the caller's scope, when the
    /// route was opened under one. Absent for an unscoped route.
    pub workdir: Option<String>,
    /// Caller-provided launch attempt id. A decision without an execution keeps this null.
    pub send_id: Option<String>,
    pub target_agent: String,
    pub requirements_json: String,
    pub selected_route_key: Option<String>,
    pub selected_provider_id: Option<String>,
    pub selected_model_id: Option<String>,
    pub selected_variant: Option<String>,
    pub score_json: String,
    pub rejected_json: String,
    pub explanation_json: Option<String>,
    /// Compact summary of the quota overlay consumed during selection.
    pub quota_signal_json: Option<String>,
    /// Digest or named fallback revision of the selection configuration actually applied.
    pub policy_revision: Option<String>,
    /// The live catalog fingerprint is stamped by the durable store at write time.
    /// Callers do not supply it, so a decision cannot claim a stale fact generation.
    pub anchor_version: u32,
}

/// A stored decision row.
#[derive(Debug, Clone, PartialEq)]
pub struct DecisionRow {
    pub id: String,
    pub decision_kind: String,
    pub task_id: Option<String>,
    pub send_id: Option<String>,
    pub target_agent: String,
    pub selected_route_key: Option<String>,
    pub rejected_json: String,
    pub explanation_json: Option<String>,
    pub quota_signal_json: Option<String>,
    pub policy_revision: Option<String>,
    /// NULL for decisions written before the catalog-generation migration.
    pub catalog_fingerprint: Option<String>,
    pub outcome: Option<String>,
    pub created_at: f64,
}

/// One completed assistant-message usage delivery. `id` is the message id (the
/// idempotency key: a redrive of the same message must not double-count). Optional
/// values distinguish an unavailable measurement from an authoritative zero.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageObservation {
    pub id: String,
    pub route_key: String,
    pub capacity_key: String,
    pub provider_id: String,
    pub input_tokens: Option<f64>,
    pub output_tokens: f64,
    pub reasoning_tokens: Option<f64>,
    pub cached_read_tokens: Option<f64>,
    pub cache_write_tokens: Option<f64>,
    /// Model time for the output, in milliseconds. Not finite or negative
    /// when unknown: the store then records no duration, and the row is never
    /// a speed sample.
    pub duration_ms: f64,
    pub workload_class: Option<String>,
    pub account_key: Option<String>,
    pub observed_at: f64,
}

/// Per-delivery effects of the usage-row upsert.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageRecordResult {
    /// This delivery newly made the stored row speed-eligible.
    pub recorded: bool,
    /// This delivery inserted the message-id row rather than updating it.
    pub usage_recorded: bool,
}

/// The optional producer disclosure attached to a preserved quota reading.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaStaleDisclosure {
    pub since: Option<String>,
    pub class: Option<String>,
}

/// One immutable quota-poll sample. The composite key deliberately uses the local
/// poll clock; `fetched_at` is the independent producer-observation clock used for
/// elapsed-time admission.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaReadingLogRow {
    pub account_key: String,
    pub window_key: String,
    pub observed_at: i64,
    pub used_percent: f64,
    pub raw_used_percent: Option<f64>,
    pub resets_at: Option<i64>,
    pub window_minutes: Option<i64>,
    pub banked_credit_count: Option<i64>,
    pub fetched_at: Option<i64>,
    pub stale: Option<QuotaStaleDisclosure>,
}

// Observed-speed gates: ported VERBATIM from the TS `ModelRouteStore` so the Rust
// observed TPS matches the plugin's bit-for-bit. A sample is only eligible if it
// generated >= MIN_OUTPUT_TOKENS over >= MIN_DURATION_MS; an aggregate is only
// trusted with >= MIN_SAMPLES eligible rows totalling >= MIN_OUTPUT_TOKENS, all
// within the trailing WINDOW_MS, capped at SAMPLE_LIMIT most-recent rows.
pub const USAGE_MIN_OUTPUT_TOKENS: f64 = 100.0;
pub const USAGE_MIN_DURATION_MS: f64 = 1_000.0;
pub const OBSERVED_SPEED_WINDOW_MS: f64 = 7.0 * 24.0 * 60.0 * 60_000.0;
pub const OBSERVED_SPEED_SAMPLE_LIMIT: i64 = 20;
pub const OBSERVED_SPEED_MIN_SAMPLES: usize = 5;
pub const OBSERVED_SPEED_MIN_OUTPUT_TOKENS: f64 = 1_000.0;

/// The route-owned durable store: curated facts, route-state telemetry, decision
/// audit rows, observed-TPS samples, and the scorer snapshot built from them.
pub trait RouteStateStore {
    /// Upsert a fact (by route key), tagging its provenance. Used for runtime
    /// config overrides; the curated seed is applied once via migration, not here.
    fn upsert_fact(&self, fact: &ModelFact, source: FactSource) -> RouteStateResult<()>;

    /// Stamp only the named fact fields as operator-owned. Backends predating
    /// per-field provenance may keep the default no-op implementation.
    fn mark_operator_fact_fields(
        &self,
        _route_key: &str,
        _fields: &[CatalogFactField],
    ) -> RouteStateResult<()> {
        Ok(())
    }

    /// Clear operator provenance only for the named factual fields. The return value
    /// identifies fields that actually carried an operator stamp before the clear, so
    /// callers can restore only withdrawn values and leave unrelated catalog facts intact.
    /// Backends predating per-field provenance may keep the default no-op implementation.
    fn clear_operator_fact_fields(
        &self,
        _route_key: &str,
        _fields: &[CatalogFactField],
    ) -> RouteStateResult<Vec<CatalogFactField>> {
        Ok(Vec::new())
    }

    /// Update only the family fact and its provenance without changing score ownership.
    fn set_model_family(
        &self,
        _route_key: &str,
        _family: Option<GrandFamily>,
        _source: FamilySource,
    ) -> RouteStateResult<()> {
        Ok(())
    }

    /// Read one family fact with its independent provenance.
    fn get_model_family(
        &self,
        route_key: &str,
    ) -> RouteStateResult<Option<(Option<GrandFamily>, FamilySource)>> {
        Ok(self.get_fact_by_route_key(route_key)?.map(|fact| {
            let family = fact.family.as_deref().and_then(GrandFamily::from_wire);
            let source = if family.is_some() {
                FamilySource::Curated
            } else {
                FamilySource::Absent
            };
            (family, source)
        }))
    }

    /// All facts currently in the store (seed + overrides).
    fn get_facts(&self) -> RouteStateResult<Vec<ModelFact>>;

    /// A single fact by exact route key, or `None`.
    fn get_fact_by_route_key(&self, route_key: &str) -> RouteStateResult<Option<ModelFact>>;

    /// Route keys of facts written as user `override` (for reconcile on config change).
    fn list_override_route_keys(&self) -> RouteStateResult<Vec<String>>;

    /// Remove a fact by route key.
    fn delete_fact(&self, route_key: &str) -> RouteStateResult<()>;
    /// Withdraw an override without deleting the row: the fact stays in the
    /// catalog under source 'seed'. Rows leave the catalog by migration only.
    fn demote_fact_source_to_seed(&self, route_key: &str) -> RouteStateResult<()>;

    /// Distinct capacity keys (`provider/model`) known for a provider.
    fn get_capacity_keys_for_provider(&self, provider_id: &str) -> RouteStateResult<Vec<String>>;

    /// Read provider-wide cooldown state for diagnostics. Admission does not call
    /// this method: provider horizons are materialized into route rows before the
    /// pure scorer runs, so the admission path performs no provider lookup.
    fn get_provider_cooldown(
        &self,
        _provider_key: &str,
    ) -> RouteStateResult<Option<ProviderCooldownState>> {
        Ok(None)
    }

    /// Record a provider-wide cooldown and fan it out to existing route rows in
    /// one transaction. The provider row is the durable source for routes minted
    /// after the write; the fanout keeps the existing snapshot shape unchanged.
    /// Backends with durable provider state must override this method; the default
    /// preserves compatibility for lightweight in-memory test stores.
    fn record_provider_cooldown(
        &self,
        _provider_key: &str,
        route_key: &str,
        capacity_key: &str,
        update: &RouteStateUpdate,
    ) -> RouteStateResult<()> {
        self.update_route_state(route_key, capacity_key, update)
    }

    // ── Route-state telemetry ───────────────────────────────
    /// Upsert a route's runtime telemetry. `None` update fields preserve the
    /// existing stored value; the increment flags add to the persisted counters.
    fn update_route_state(
        &self,
        route_key: &str,
        capacity_key: &str,
        update: &RouteStateUpdate,
    ) -> RouteStateResult<()>;

    // ── Decisions (audit + routing learning) ────────────────
    /// Record a route decision. Returns the decision id.
    fn record_decision(&self, input: &DecisionInput) -> RouteStateResult<String>;

    /// Set a decision's terminal outcome and atomically update the selected
    /// route's consecutive-terminal-failure backoff state.
    fn set_decision_outcome(
        &self,
        decision_id: &str,
        update: &DecisionOutcomeUpdate,
    ) -> RouteStateResult<()>;

    /// Clear only the route-local terminal-failure streak and backoff fields. The
    /// returned state is the durable value before the clear; provider cooldown and
    /// quota state remain outside this operator recovery action. Lightweight stores
    /// may not keep this optional state and return `None`.
    fn clear_failure_backoff(
        &self,
        _route_key: &str,
    ) -> RouteStateResult<Option<FailureBackoffState>> {
        Ok(None)
    }

    /// Clear cooldown and quota walls for a route, including variants of a base key.
    /// `None` means the route is unknown; known routes without state return empty horizons.
    fn clear_cooldown(&self, _route_key: &str) -> RouteStateResult<Option<CooldownState>> {
        Ok(None)
    }

    /// Decisions for a task, oldest first.
    fn get_decisions_for_task(&self, task_id: &str) -> RouteStateResult<Vec<DecisionRow>>;

    /// Delete at most `limit` decision audit rows created before the supplied
    /// Unix-millisecond cutoff, oldest first, and return how many went. The
    /// limit keeps each call one short transaction; a caller with more to
    /// delete calls again. Durable backends override this; lightweight test
    /// stores may skip it.
    fn delete_decisions_older_than(
        &self,
        _created_before_ms: i64,
        _limit: usize,
    ) -> RouteStateResult<usize> {
        Ok(0)
    }

    // ── Usage observations (observed TPS) ───────────────────
    /// Return both effects of one completed-message usage upsert. A later delivery
    /// only fills measurements that are still NULL.
    fn record_usage_delivery(&self, obs: &UsageObservation) -> RouteStateResult<UsageRecordResult>;

    /// Compatibility projection for callers that only need to know whether this
    /// delivery newly made the stored row eligible for observed speed.
    fn record_usage_observation(&self, obs: &UsageObservation) -> RouteStateResult<bool> {
        Ok(self.record_usage_delivery(obs)?.recorded)
    }

    // ── Quota reading log ────────────────────────────────────
    /// Append poll samples without changing an existing composite-key row.
    fn append_quota_readings(&self, _rows: &[QuotaReadingLogRow]) -> RouteStateResult<usize> {
        Ok(0)
    }

    /// Read one account's immutable samples in window/time order.
    fn get_quota_readings(&self, _account_key: &str) -> RouteStateResult<Vec<QuotaReadingLogRow>> {
        Ok(Vec::new())
    }

    /// Apply the named reading-log retention period relative to the supplied clock.
    fn prune_quota_readings(&self, _now_ms: i64) -> RouteStateResult<usize> {
        Ok(0)
    }

    /// Observed output tokens/sec for a route: the median per-sample speed of its
    /// most recent eligible usage rows, or `None` if there is not enough trusted
    /// signal. `now` is the window anchor.
    fn get_observed_speed(&self, route_key: &str, now: f64) -> RouteStateResult<Option<f64>>;

    /// All facts with their `speed` score overlaid by the observed TPS where a
    /// trusted observation exists (the selection input). Mirrors the TS
    /// `overlayObservedSpeeds`: a fact's observed route key is `provider/model` +
    /// the fact's prompt-default variant (falling back to its own variant), so a
    /// max-effort sibling's observations attach to the right fact. Facts without a
    /// trusted observation keep their seed speed. Speed and subscription
    /// multipliers come from `pricing`, the caller's one copy of the operator's
    /// settings, so every fact in the result is priced against the same values.
    fn get_facts_with_observed_speed(
        &self,
        now: f64,
        pricing: &RoutePricing,
    ) -> RouteStateResult<Vec<ModelFact>>;

    /// Compute the complete-case token profile vector for a workload class over the
    /// trailing 7-day window (now - 7d, now].
    fn get_class_vector(
        &self,
        _workload_class: &str,
        _now: f64,
    ) -> RouteStateResult<Option<ClassVector>> {
        Ok(None)
    }

    // ── Snapshot ────────────────────────────────────────────
    /// Build the ROUTE-STATE-ONLY runtime snapshot the scorer consumes: per-route
    /// telemetry plus capacity-shared cooldown/quota signals. The quota
    /// overlay is NOT applied here (quota is `insula` (formerly `ai-provider-quota`)'s domain, consumed
    /// module-to-module and merged on top at the module via `crate::quota`).
    fn build_snapshot(&self, now: f64) -> RouteStateResult<ModelRouteStateSnapshot>;
}
