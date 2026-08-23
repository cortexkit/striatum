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

use crate::model_facts::ModelFact;
use crate::model_router::ModelRouteStateSnapshot;

/// Provenance of a stored fact: the curated seed, or a user/config override.
/// Overrides survive a re-seed; the seed only fills absent rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactSource {
    Seed,
    Override,
}

impl FactSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            FactSource::Seed => "seed",
            FactSource::Override => "override",
        }
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

/// A partial update to a route's runtime telemetry. `None` fields are left as-is
/// (the existing stored value survives); `Some(value)` overwrites. The two
/// increment flags add to the persisted counters. Mirrors the TS `RouteStateUpdate`.
#[derive(Debug, Clone, Default)]
pub struct RouteStateUpdate {
    pub cooldown_until: Option<f64>,
    pub quota_exhausted_until: Option<f64>,
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
}

/// Terminal outcome recorded against a route decision (audit + routing learning).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteOutcome {
    Completed,
    Error,
    Cancelled,
    Interrupted,
    LaunchFailed,
}

impl RouteOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            RouteOutcome::Completed => "completed",
            RouteOutcome::Error => "error",
            RouteOutcome::Cancelled => "cancelled",
            RouteOutcome::Interrupted => "interrupted",
            RouteOutcome::LaunchFailed => "launch_failed",
        }
    }
}

/// What `record_decision` persists: the selection (or none) + its audit context.
/// `score_json`/`rejected_json`/`explanation_json` are pre-serialized by the
/// caller (the module serializes the scorer's `SelectedModelRoute`), keeping this
/// trait free of the scorer's wire shape.
#[derive(Debug, Clone, Default)]
pub struct DecisionInput {
    pub decision_id: Option<String>,
    /// `single` keeps the historical shape; panel rows leave singular columns null.
    pub decision_kind: String,
    pub task_id: Option<String>,
    pub parent_session_id: Option<String>,
    /// The caller's harness, copied from the selection input when one was provided.
    pub harness: Option<String>,
    /// The caller's project directory, used to match the decision with recorded spend facts.
    pub project_root: Option<String>,
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

/// One completed assistant-message TPS observation. `id` is the message id (the
/// idempotency key: a redrive of the same message must not double-count). Mirrors
/// the TS `ModelRouteUsageObservation` write fields.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageObservation {
    pub id: String,
    pub route_key: String,
    pub capacity_key: String,
    pub provider_id: String,
    pub output_tokens: f64,
    pub duration_ms: f64,
    pub observed_at: f64,
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

    /// Set a decision's terminal outcome.
    fn set_decision_outcome(
        &self,
        decision_id: &str,
        outcome: RouteOutcome,
    ) -> RouteStateResult<()>;

    /// Decisions for a task, oldest first.
    fn get_decisions_for_task(&self, task_id: &str) -> RouteStateResult<Vec<DecisionRow>>;

    /// Delete decision audit rows created before the supplied Unix-millisecond
    /// cutoff. Durable backends override this; lightweight test stores may skip it.
    fn delete_decisions_older_than(&self, _created_before_ms: i64) -> RouteStateResult<usize> {
        Ok(0)
    }

    // ── Usage observations (observed TPS) ───────────────────
    /// Record one completed-message TPS observation. Idempotent by `id` (the message
    /// id): a redrive of the same message is a no-op. Returns true if a new row was
    /// inserted, false if the observation was rejected by the eligibility gates or
    /// was a duplicate. Mirrors the TS `recordUsageObservation`.
    fn record_usage_observation(&self, obs: &UsageObservation) -> RouteStateResult<bool>;

    /// Aggregate recent eligible usage rows for a route into observed output
    /// tokens/sec, or `None` if there is not enough trusted signal. `now` is the
    /// window anchor. Mirrors the TS `getObservedSpeed`.
    fn get_observed_speed(&self, route_key: &str, now: f64) -> RouteStateResult<Option<f64>>;

    /// All facts with their `speed` score overlaid by the observed TPS where a
    /// trusted observation exists (the selection input). Mirrors the TS
    /// `overlayObservedSpeeds`: a fact's observed route key is `provider/model` +
    /// the fact's prompt-default variant (falling back to its own variant), so a
    /// max-effort sibling's observations attach to the right fact. Facts without a
    /// trusted observation keep their seed speed.
    fn get_facts_with_observed_speed(&self, now: f64) -> RouteStateResult<Vec<ModelFact>>;

    // ── Snapshot ────────────────────────────────────────────
    /// Build the ROUTE-STATE-ONLY runtime snapshot the scorer consumes: per-route
    /// telemetry plus capacity-shared cooldown/quota signals. The quota
    /// overlay is NOT applied here (quota is `insula` (formerly `ai-provider-quota`)'s domain, consumed
    /// module-to-module and merged on top at the module via `crate::quota`).
    fn build_snapshot(&self, now: f64) -> RouteStateResult<ModelRouteStateSnapshot>;
}
