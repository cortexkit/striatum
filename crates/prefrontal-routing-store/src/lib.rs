//! SQLite backend for the route-state store.
//!
//! Implements `prefrontal-routing-core::RouteStateStore` over the shared
//! `cortexkit-store` lib: the lib owns the single-writer lease, the durable
//! pragmas, and the namespace-keyed migration runner; this crate owns the
//! `route_state` migration set (the schema, the curated catalog seed and its
//! later revisions) and the domain queries.
//! `-core` stays rusqlite-free; the backend lives here (postgres sibling later).
//!
//! Ported from the Alfonso TS plugin's `ModelRouteStore`
//! (`src/features/model-routing/route-state-store.ts`), facts domain first.

use cortexkit_store::{open_sqlite, Migration, SqliteStore};
use cortexkit_store_types::{Isolation, StorageBackend, StorageDescriptor};
use prefrontal_routing_core::canonical_scores::resolve_canonical_scores;
use prefrontal_routing_core::model_facts::{
    normalize_grand_family_id, ClassVector, Confidence, FamilySource, GrandFamily, ModelFact,
    ModelPromptDefaults, ModelScores, CLASS_VECTOR_TRAILING_WINDOW_DAYS,
    CLASS_VECTOR_TRAILING_WINDOW_MS, ENUMERATED_WORKLOAD_CLASSES,
};
use prefrontal_routing_core::model_router::{ModelRouteStateSnapshot, RouteRuntimeState};
use prefrontal_routing_core::quota::quota_poll::reading_log_retention_cutoff;
use prefrontal_routing_core::route_state_store::{
    QuotaReadingLogRow, QuotaStaleDisclosure, UsageRecordResult,
};
use prefrontal_routing_core::{
    derive_provider_key, fact_to_route, failure_backoff_transition, CatalogFactField,
    CooldownState, DecisionInput, DecisionOutcomeUpdate, DecisionRow, FactSource,
    FailureBackoffState, FailureBackoffTransition, ProviderCooldownState, RouteStateResult,
    RouteStateStore, RouteStateStoreError, RouteStateUpdate, RouteWallClass, UsageObservation,
    OBSERVED_SPEED_MIN_OUTPUT_TOKENS, OBSERVED_SPEED_MIN_SAMPLES, OBSERVED_SPEED_SAMPLE_LIMIT,
    OBSERVED_SPEED_WINDOW_MS, USAGE_MIN_DURATION_MS, USAGE_MIN_OUTPUT_TOKENS,
};
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

mod account_selection;
// The backup table classification is a test oracle: the store's integration
// tests, the engram catalog test and the restore rehearsal example check the
// shipped catalog against it, and no production path reads it.
#[cfg(feature = "test-support")]
pub mod backup;
mod diagnostics;
mod operator_quality;
pub use operator_quality::{OperatorQuality, UnscoredAdmissibleModel};
mod key_pool;
mod reasoning_catalog;
mod retention;
mod routing_v1;
mod task_usage;

pub use diagnostics::{
    DiagnosticAccountBlock, DiagnosticAccountHold, DiagnosticDecision, DiagnosticKeyWall,
    DiagnosticRouteWall,
};
pub use key_pool::{PoolAcquire, PoolAcquireOutcome, PoolLease, PoolWallKind};
pub use reasoning_catalog::{read_reasoning_catalog_offline, PublishedEfforts, ReasoningCatalog};
pub use retention::AffinityRetentionBatch;
pub use routing_v1::{V1Decision, V1Recorded};
pub use task_usage::{ClosedTaskUsage, ProviderTokenReporting, TaskTokenTotals};

#[cfg(any(test, feature = "test-support"))]
pub use account_selection::AccountServingCounts;
pub use account_selection::{
    AccountAdmissionRow, AccountAffinityRow, AccountDecisionRecord, AccountDemandRow,
    AccountEligibilityFact, AccountExclusionRow, AccountFenceRow, AccountHoldRow,
    AccountObservationRow, AccountPinRow, AccountPoolFenceRow, AccountRouteFence,
    AccountSizeRefusal, AccountTx, CredentialProbeClaim, ACCOUNT_DEMAND_SQL, DECISION_MODE_SERVE,
};

const ROUTE_STATE_NAMESPACE: &str = "route_state";

/// Migration 1: the route-owned schema + the curated 33-fact seed. FROZEN: a
/// later schema/seed change is a NEW migration, never an edit to this string.
const MIGRATION_001: &str = include_str!("../migrations/001_route_state.sql");

/// Migration 2: the observed-TPS usage table (record-per-message + speed overlay).
const MIGRATION_002: &str = include_str!("../migrations/002_usage.sql");

/// Migration 3: drop the never-wired reservation table once manager concurrency moved to slots.
const MIGRATION_003: &str = include_str!("../migrations/003_drop_reservations.sql");

/// Migration 4: the gpt-5.6 family seed rows (sol/terra/luna + fast/pro).
const MIGRATION_004: &str = include_str!("../migrations/004_gpt56_family.sql");

/// Migration 5: curated family labels and panel decision discrimination.
const MIGRATION_005: &str = include_str!("../migrations/005_panel_selection.sql");

/// Migration 6: the grok-4.5 seed row + the stale grok-4.3 iq rescale.
const MIGRATION_006: &str = include_str!("../migrations/006_grok_45.sql");

/// Migration 7: AA Intelligence Index v4.1 iq rebase, drop opencode-go, seed ollama-cloud.
const MIGRATION_007: &str = include_str!("../migrations/007_aa_v41_rebase.sql");

/// Migration 8: fable-5 seed row (pin-only: roster seating needs the fact).
const MIGRATION_008: &str = include_str!("../migrations/008_fable_5.sql");

/// Migration 9: kimi-for-coding/k3 seed row (new Kimi coding subscription).
const MIGRATION_009: &str = include_str!("../migrations/009_kimi_k3.sql");

/// Migration 10: alibaba-token-plan/qwen3.8-max-preview seed row (new Alibaba subscription).
const MIGRATION_010: &str = include_str!("../migrations/010_qwen38_max.sql");

/// Migration 11: qwen3.8-max-preview rescore to the k3/terra band (operator curation).
const MIGRATION_011: &str = include_str!("../migrations/011_qwen38_rescore.sql");

/// Migration 12: provider output-token ceilings for seated fanout models.
const MIGRATION_012: &str = include_str!("../migrations/012_max_output_tokens.sql");
const MIGRATION_013: &str = include_str!("../migrations/013_alibaba_token_plan_expansion.sql");
const MIGRATION_014: &str = include_str!("../migrations/014_alibaba_entitlement_prune.sql");
/// Migration 15: durable provider cooldowns and route-state inheritance for
/// catalog rows inserted after a provider-wide cooldown is recorded.
const MIGRATION_015: &str = include_str!("../migrations/015_provider_cooldown_inheritance.sql");
/// Migration 16: opus default variant max -> xhigh (max pays heavy latency for
/// indistinguishable output on opus-4-8; pins carry their own variant).
const MIGRATION_016: &str = include_str!("../migrations/016_opus_default_variant_xhigh.sql");

/// Migration 17: Gemini 3.6 Flash seed (scores carried from 3.5 Flash, which is
/// simultaneously retired from auto-routing via the config exclude list).
const MIGRATION_017: &str = include_str!("../migrations/017_gemini_36_flash.sql");

/// Migration 18: Claude Opus 5 seed (and its -fast sibling). Pin-only like the
/// rest of the Anthropic line, which the provider-wide config exclude enforces.
const MIGRATION_018: &str = include_str!("../migrations/018_opus_5.sql");

/// Migration 19: corrected Anthropic list prices. Load-bearing once cost is
/// route-derived rather than zeroed for subscriptions.
const MIGRATION_019: &str = include_str!("../migrations/019_anthropic_real_prices.sql");
/// Migration 20: make age-based decision retention an indexed delete.
const MIGRATION_020: &str = include_str!("../migrations/020_decision_retention_index.sql");
const MIGRATION_021: &str = include_str!("../migrations/021_new_sibling_models.sql");
const MIGRATION_022: &str = include_str!("../migrations/022_elo_rescore.sql");
/// Migration 23: ollama-cloud began serving Kimi K3; an unseeded route made the
/// operator's roster seat silently unseatable.
const MIGRATION_023: &str = include_str!("../migrations/023_ollama_kimi_k3.sql");
/// Migration 24: restore fable-5/opus-5/opus-5-fast after the config
/// override-revert deleted migration-seeded rows absent from the JSON seed.
const MIGRATION_024: &str = include_str!("../migrations/024_restore_override_reverted_facts.sql");
/// Migration 25: explicit image capability facts for known vision model families.
const MIGRATION_025: &str = include_str!("../migrations/025_known_vision_capabilities.sql");
/// Migration 26: Grok 4.6 catalog fact (Ufuk curation, elo 1798, xhigh default).
const MIGRATION_026: &str = include_str!("../migrations/026_grok_46.sql");
const MIGRATION_027: &str = include_str!("../migrations/027_gemini_37_flash.sql");
/// Migration 28: persist decision-to-send correlation and selection provenance.
const MIGRATION_028: &str = include_str!("../migrations/028_decision_ledger_provenance.sql");
/// Migration 29: catalog fingerprints stamped on new decision rows.
const MIGRATION_029: &str = include_str!("../migrations/029_catalog_fingerprint.sql");
/// Migration 30: decision join identity for safe spend attribution.
const MIGRATION_030: &str = include_str!("../migrations/030_decision_ledger_join_identity.sql");
/// Migration 31: retire the never-read catalog generation history.
const MIGRATION_031: &str = include_str!("../migrations/031_drop_catalog_generation.sql");
/// Migration 32: consecutive terminal-send-failure counter and short backoff.
const MIGRATION_032: &str = include_str!("../migrations/032_consecutive_failure_backoff.sql");
/// Migration 33: AA-inferred provenance columns and the AA index cache.
const MIGRATION_033: &str = include_str!("../migrations/033_aa_inferred_provenance.sql");
/// Migration 34: Fusiform facts, per-field overrides, and the replay cursor.
const MIGRATION_034: &str = include_str!("../migrations/034_fusiform_catalog.sql");
/// Migration 35: canonical model-family spellings and independent provenance.
const MIGRATION_035: &str = include_str!("../migrations/035_model_family_vocabulary.sql");
/// Migration 36: per-message token profile, attribution, and class/time lookup.
const MIGRATION_036: &str = include_str!("../migrations/036_usage_profile.sql");
/// Migration 37: the remaining catalog rate card fields. Renumbered from 36 at
/// settle under CR-Q4: two slices legitimately claimed the next free number, which
/// is a merge concern rather than a design one.
const MIGRATION_037: &str = include_str!("../migrations/037_catalog_rates.sql");
/// Migration 38: append-only per-account quota reading history.
const MIGRATION_038: &str = include_str!("../migrations/038_quota_reading_log.sql");
/// Migration 39: typed hard-wall cause for route state and provider inheritance.
const MIGRATION_039: &str = include_str!("../migrations/039_route_wall_class.sql");
/// Migration 40: the last announced route-winner ladder, kept across restarts.
const MIGRATION_040: &str = include_str!("../migrations/040_route_winner_baseline.sql");
const MIGRATION_041: &str = include_str!("../migrations/041_retire_fast_routes.sql");
/// Migration 42: the last accepted quota snapshot, kept across restarts.
const MIGRATION_042: &str = include_str!("../migrations/042_quota_snapshot.sql");
/// Migration 43: per-account quota readings, completeness and the vault roster.
const MIGRATION_043: &str = include_str!("../migrations/043_account_quota.sql");

/// Migration 44: the account-selection store (decisions, affinity, pins,
/// admissions, holds, observations, fences and per-admission exclusions).
const MIGRATION_044: &str = include_str!("../migrations/044_account_selection.sql");

const MIGRATION_045: &str = include_str!("../migrations/045_account_decision_latest.sql");

/// Migration 46: partial covering index for routing demand queries.
const MIGRATION_046: &str = include_str!("../migrations/046_account_admission_demand.sql");
/// Migration 47: the indexes the bounded retention deletes walk.
const MIGRATION_047: &str = include_str!("../migrations/047_retention_indexes.sql");
/// Migration 48: the key pool for explicit-only API-key providers (leases,
/// per-key walls and cooldowns, and session workspace affinity).
const MIGRATION_048: &str = include_str!("../migrations/048_pool_keys.sql");
const MIGRATION_049: &str = include_str!("../migrations/049_model_auth_methods.sql");
/// Migration 50: the reasoning-effort values Fusiform publishes per model.
const MIGRATION_050: &str = include_str!("../migrations/050_model_reasoning_efforts.sql");
/// Migration 51: Fusiform's canonical model identity, shared by every route of
/// one model so they can share one quality score, and the upstream `model.status`
/// that withdraws a deprecated route from routing.
const MIGRATION_051: &str = include_str!("../migrations/051_model_identity_and_status.sql");
/// Migration 52: retire shadow mode's heartbeat index and add the indexes the
/// account-selection history retention walks.
const MIGRATION_052: &str = include_str!("../migrations/052_retire_account_shadow.sql");
const MIGRATION_053: &str = include_str!("../migrations/053_selected_harness_workload.sql");
const MIGRATION_054: &str = include_str!("../migrations/054_parent_failure_index.sql");
const MIGRATION_055: &str = include_str!("../migrations/055_drop_aa_index_cache.sql");
/// Migration 56: the runner-event dedupe table that lets a backup restore the
/// observation log empty.
const MIGRATION_056: &str = include_str!("../migrations/056_restorable_history.sql");
/// Migration 57: prune unserved, unpicked facts while retaining decision history.
const MIGRATION_057: &str = include_str!("../migrations/057_prune_absent_fusiform_facts.sql");
/// Migration 58: rebuild the five account tables that held foreign keys into
/// the history logs, without those keys. Runs with foreign-key enforcement
/// suspended (see `SqliteRouteStateStore::open`).
const MIGRATION_058: &str = include_str!("../migrations/058_drop_history_foreign_keys.sql");
const MIGRATION_059: &str = include_str!("../migrations/059_provider_reset_fences.sql");
const MIGRATION_060: &str = include_str!("../migrations/060_credential_entitlement_fences.sql");
const MIGRATION_061: &str = include_str!("../migrations/061_allowed_providers.sql");
// routing/v1 decision binding, affinity, report and admission idempotency.
const MIGRATION_062: &str = include_str!("../migrations/062_routing_v1.sql");
/// The project and session folder the daemon stamped on a decision's scope.
const MIGRATION_064: &str = include_str!("../migrations/064_decision_scope_attribution.sql");
const MIGRATION_065: &str = include_str!("../migrations/065_routing_policy_end_state.sql");
const MIGRATION_066: &str = include_str!("../migrations/066_account_eligibility.sql");
const MIGRATION_067: &str = include_str!("../migrations/067_account_route_fence.sql");
const MIGRATION_068: &str = include_str!("../migrations/068_account_demand_by_account.sql");
const MIGRATION_069: &str = include_str!("../migrations/069_task_workload.sql");

/// Stored rejection detail stays useful without growing with every catalog route.
const MAX_STORED_REJECTIONS: usize = 12;
/// How many omitted `not_available` route keys the rejection summary names.
const MAX_OMITTED_NOT_AVAILABLE_KEYS: usize = 200;

/// Fetch one route's bounded observed-speed sample set through the route/time index.
/// The batched lookup prepares this statement once and reuses it for every
/// effective catalog key. The window is `(now - 7d, now]`, the same bounds as the
/// workload-class samples below, so a row exactly seven days old counts in
/// neither.
pub const OBSERVED_SPEED_SAMPLES_SQL: &str = "SELECT output_tokens, duration_ms
     FROM model_route_usage INDEXED BY model_route_usage_route_observed_idx
     WHERE route_key = ?1
       AND observed_at > ?2
       AND observed_at <= ?3
       AND output_tokens >= ?4
       AND duration_ms >= ?5
     ORDER BY observed_at DESC
     LIMIT ?6";

/// Fetch one workload class's complete token-profile population through its class/time index.
pub const WORKLOAD_CLASS_USAGE_SAMPLES_SQL: &str = "SELECT input_tokens, output_tokens,
            reasoning_tokens, cached_read_tokens, cache_write_tokens
     FROM model_route_usage INDEXED BY model_route_usage_workload_class_observed_idx
     WHERE workload_class = ?1
       AND observed_at > ?2
       AND observed_at <= ?3
       AND input_tokens IS NOT NULL
       AND output_tokens IS NOT NULL
       AND reasoning_tokens IS NOT NULL
       AND cached_read_tokens IS NOT NULL
        AND cache_write_tokens IS NOT NULL";

/// One head's failed decisions since a time, newest first, read from the
/// narrow per-head copy through its (parent, outcome time) index. Every row in
/// that table has an error, so it needs no `outcome_error IS NOT NULL` filter.
pub const RECENT_PARENT_FAILURES_SQL: &str =
    "SELECT selected_route_key, selected_provider_id, outcome_error, outcome_at
     FROM model_route_parent_failures INDEXED BY model_route_parent_failures_by_parent
     WHERE parent_session_id = ?1
       AND outcome_at >= ?2
     ORDER BY outcome_at DESC, decision_id DESC";

#[derive(Debug, Clone, PartialEq)]
struct ObservedSpeedAggregate {
    route_key: String,
    sample_count: i64,
    output_tokens: i64,
    duration_ms: i64,
    /// Trusted speed for the window, or `None` when the trust gates reject it.
    /// See `observed_speed_from_samples`.
    speed: Option<f64>,
}

fn route_state_migrations() -> [Migration; 69] {
    [
        Migration {
            version: 1,
            statements: MIGRATION_001,
        },
        Migration {
            version: 2,
            statements: MIGRATION_002,
        },
        Migration {
            version: 3,
            statements: MIGRATION_003,
        },
        Migration {
            version: 4,
            statements: MIGRATION_004,
        },
        Migration {
            version: 5,
            statements: MIGRATION_005,
        },
        Migration {
            version: 6,
            statements: MIGRATION_006,
        },
        Migration {
            version: 7,
            statements: MIGRATION_007,
        },
        Migration {
            version: 8,
            statements: MIGRATION_008,
        },
        Migration {
            version: 9,
            statements: MIGRATION_009,
        },
        Migration {
            version: 10,
            statements: MIGRATION_010,
        },
        Migration {
            version: 11,
            statements: MIGRATION_011,
        },
        Migration {
            version: 12,
            statements: MIGRATION_012,
        },
        Migration {
            version: 13,
            statements: MIGRATION_013,
        },
        Migration {
            version: 14,
            statements: MIGRATION_014,
        },
        Migration {
            version: 15,
            statements: MIGRATION_015,
        },
        Migration {
            version: 16,
            statements: MIGRATION_016,
        },
        Migration {
            version: 17,
            statements: MIGRATION_017,
        },
        Migration {
            version: 18,
            statements: MIGRATION_018,
        },
        Migration {
            version: 19,
            statements: MIGRATION_019,
        },
        Migration {
            version: 20,
            statements: MIGRATION_020,
        },
        Migration {
            version: 21,
            statements: MIGRATION_021,
        },
        Migration {
            version: 22,
            statements: MIGRATION_022,
        },
        Migration {
            version: 23,
            statements: MIGRATION_023,
        },
        Migration {
            version: 24,
            statements: MIGRATION_024,
        },
        Migration {
            version: 25,
            statements: MIGRATION_025,
        },
        Migration {
            version: 26,
            statements: MIGRATION_026,
        },
        Migration {
            version: 27,
            statements: MIGRATION_027,
        },
        Migration {
            version: 28,
            statements: MIGRATION_028,
        },
        Migration {
            version: 29,
            statements: MIGRATION_029,
        },
        Migration {
            version: 30,
            statements: MIGRATION_030,
        },
        Migration {
            version: 31,
            statements: MIGRATION_031,
        },
        Migration {
            version: 32,
            statements: MIGRATION_032,
        },
        Migration {
            version: 33,
            statements: MIGRATION_033,
        },
        Migration {
            version: 34,
            statements: MIGRATION_034,
        },
        Migration {
            version: 35,
            statements: MIGRATION_035,
        },
        Migration {
            version: 36,
            statements: MIGRATION_036,
        },
        Migration {
            version: 37,
            statements: MIGRATION_037,
        },
        Migration {
            version: 38,
            statements: MIGRATION_038,
        },
        Migration {
            version: 39,
            statements: MIGRATION_039,
        },
        Migration {
            version: 40,
            statements: MIGRATION_040,
        },
        Migration {
            version: 41,
            statements: MIGRATION_041,
        },
        Migration {
            version: 42,
            statements: MIGRATION_042,
        },
        Migration {
            version: 43,
            statements: MIGRATION_043,
        },
        Migration {
            version: 44,
            statements: MIGRATION_044,
        },
        Migration {
            version: 45,
            statements: MIGRATION_045,
        },
        Migration {
            version: 46,
            statements: MIGRATION_046,
        },
        Migration {
            version: 47,
            statements: MIGRATION_047,
        },
        Migration {
            version: 48,
            statements: MIGRATION_048,
        },
        Migration {
            version: 49,
            statements: MIGRATION_049,
        },
        Migration {
            version: 50,
            statements: MIGRATION_050,
        },
        Migration {
            version: 51,
            statements: MIGRATION_051,
        },
        Migration {
            version: 52,
            statements: MIGRATION_052,
        },
        Migration {
            version: 53,
            statements: MIGRATION_053,
        },
        Migration {
            version: 54,
            statements: MIGRATION_054,
        },
        Migration {
            version: 55,
            statements: MIGRATION_055,
        },
        Migration {
            version: 56,
            statements: MIGRATION_056,
        },
        Migration {
            version: 57,
            statements: MIGRATION_057,
        },
        Migration {
            version: 58,
            statements: MIGRATION_058,
        },
        Migration {
            version: 59,
            statements: MIGRATION_059,
        },
        Migration {
            version: 60,
            statements: MIGRATION_060,
        },
        Migration {
            version: 61,
            statements: MIGRATION_061,
        },
        Migration {
            version: 62,
            statements: MIGRATION_062,
        },
        Migration {
            version: 64,
            statements: MIGRATION_064,
        },
        Migration {
            version: 65,
            statements: MIGRATION_065,
        },
        Migration {
            version: 66,
            statements: MIGRATION_066,
        },
        Migration {
            version: 67,
            statements: MIGRATION_067,
        },
        Migration {
            version: 68,
            statements: MIGRATION_068,
        },
        Migration {
            version: 69,
            statements: MIGRATION_069,
        },
        Migration {
            version: 70,
            statements: include_str!("../migrations/070_operator_quality.sql"),
        },
    ]
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn err(e: impl std::fmt::Display) -> RouteStateStoreError {
    RouteStateStoreError(e.to_string())
}

fn validate_family_spellings(store: &SqliteStore) -> RouteStateResult<()> {
    let unknown = store
        .with_conn(|connection: &Connection| {
            let mut statement = connection.prepare(
                "SELECT route_key, family FROM model_facts WHERE family IS NOT NULL ORDER BY route_key",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let unknown = rows
                .collect::<rusqlite::Result<Vec<_>>>()?
                .into_iter()
                .filter(|(_, family)| GrandFamily::from_wire(family).is_none())
                .map(|(route_key, family)| format!("{route_key}={family}"))
                .collect::<Vec<_>>();
            Ok(unknown)
        })
        .map_err(err)?;
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(err(format!(
            "unknown model family spelling(s): {}",
            unknown.join(", ")
        )))
    }
}

/// One catalog row served to read-only consumers. `excluded` stays separate from
/// the model fact because it describes routing policy state rather than a model
/// capability, but catalog readers need both to render an honest inventory.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogFact {
    #[serde(flatten)]
    pub fact: ModelFact,
    /// Whether routing's exclusion policy fences this route. Computed by the
    /// serving module from the live operator policy, never read from the durable
    /// row: nothing has ever written that column.
    pub excluded: bool,
    /// No price and no cost tier: nobody has published what this route costs.
    /// Selection ranks such a route at MAXIMUM cost (never as free), so an
    /// inventory reader has to be able to see the difference between "expensive"
    /// and "nobody knows". Derived from the row, so it can never go stale.
    pub price_unknown: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facts_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fusiform_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fusiform_present: Option<bool>,
    /// Fusiform's `model.canonical_id` for this route; absent when the catalog
    /// states no identity.
    #[serde(rename = "canonicalID", skip_serializing_if = "Option::is_none")]
    pub canonical_id: Option<String>,
    /// The reasoning efforts the catalog publishes for this model
    /// (Fusiform's `capability.reasoning_options` effort values), which Broca
    /// admits as a send's variant on any route of the model. Absent when the
    /// catalog has not been read for them; empty when it publishes none. A
    /// consumer stepping a seat's effort down reads its lower variants here: the
    /// route rows themselves carry only the variants an operator configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_efforts: Option<Vec<String>>,
    /// The route whose operator score this row's Elo/EQ were read from, when the
    /// row has no score of its own and shares a canonical id with one that does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_borrowed_from: Option<String>,
}

/// Durable cursor committed atomically with a Fusiform catalog snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FusiformCatalogCursor {
    pub catalog_version: i64,
    pub resolved_at_ms: i64,
}

/// The factual fields Fusiform may author. No score, variant, exclusion, speed
/// multiplier, or prompt-default field exists in this type by construction.
#[derive(Debug, Clone, PartialEq)]
pub struct FusiformFactUpdate {
    pub provider_id: String,
    pub model_id: String,
    pub auth_methods: Option<Vec<String>>,
    pub present: bool,
    pub context_window: Option<f64>,
    pub max_output_tokens: Option<u64>,
    pub cost_tier: Option<f64>,
    pub cost_per_mtok_out: Option<f64>,
    pub input_rate: Option<f64>,
    pub cache_read_rate: Option<f64>,
    pub cache_write_rate: Option<f64>,
    pub reasoning_rate: Option<f64>,
    pub supports_vision: Option<bool>,
    pub supports_tool_use: Option<bool>,
    /// Effort values from the model's `capability.reasoning_options`: an empty
    /// list when the catalog serves the key with no effort entry (or `null`).
    /// `None` only when the catalog did not serve the key at all, which leaves
    /// the row unread for reasoning purposes.
    pub reasoning_efforts: Option<Vec<String>>,
    /// Fusiform's `model.canonical_id`, verbatim. `None` when the catalog served
    /// `null` or no value: the upstream states no identity for this route.
    pub canonical_id: Option<String>,
    /// Fusiform's `model.status` verbatim (`deprecated`, `beta`, `alpha`), or
    /// `None` when the catalog states no status.
    pub upstream_status: Option<String>,
}

#[derive(Serialize)]
struct CatalogFingerprintRow {
    route_key: String,
    capacity_key: String,
    provider_id: String,
    model_id: String,
    variant: Option<String>,
    family: Option<String>,
    elo: f64,
    eq: f64,
    speed: f64,
    context_window: Option<f64>,
    max_output_tokens: Option<u64>,
    cost_tier: Option<f64>,
    cost_per_mtok_out: Option<f64>,
    input_rate: Option<f64>,
    cache_read_rate: Option<f64>,
    cache_write_rate: Option<f64>,
    reasoning_rate: Option<f64>,
    supports_vision: Option<bool>,
    supports_tool_use: Option<bool>,
    prompt_defaults_json: Option<String>,
    confidence: String,
    speed_multiplier: Option<f64>,
    excluded: bool,
    fusiform_present: Option<bool>,
    auth_methods_json: Option<String>,
    reasoning_efforts_json: Option<String>,
    canonical_id: Option<String>,
    upstream_status: Option<String>,
}

/// BLAKE3 over every routing-read `model_facts` column: route/capacity identity,
/// provider/model/variant, family, Elo/EQ/speed, context and output limits, price
/// data, vision/tool capability flags, allowed login methods, published
/// reasoning efforts, canonical model identity (it decides which score a route
/// borrows), upstream status (a deprecated route is refused), prompt defaults,
/// confidence, speed multiplier, and exclusion, plus Fusiform operator grades.
/// Adding a routing-read column
/// without extending this row is the defect this hash exists to catch. Query ordering—not insertion ordering—is the
/// generation contract, so a no-op reseed or shuffled input cannot mint a different
/// fingerprint for the same catalog.
///
/// Widening this row is intentionally migration-free even though it changes every
/// fingerprint: comparisons only have meaning within a catalog generation, and a
/// historical NULL fingerprint was never comparable to a live catalog value.
///
/// Scores are hashed as `stored_scores` decodes them, the same values routing
/// reads, so a row whose `scores_json` does not decode hashes as zero scores
/// instead of failing the fingerprint and with it every decision write.
fn calculate_catalog_fingerprint(connection: &Connection) -> rusqlite::Result<String> {
    let mut statement = connection.prepare(
        "SELECT route_key, capacity_key, provider_id, model_id, variant, family,
                scores_json, context_window, max_output_tokens, cost_tier, cost_per_mtok_out,
                input_rate, cache_read_rate, cache_write_rate, reasoning_rate,
                supports_vision, supports_tool_use, prompt_defaults_json, confidence,
                speed_multiplier, excluded, fusiform_present, auth_methods_json,
                reasoning_efforts_json, canonical_id, upstream_status
         FROM model_facts
         ORDER BY route_key",
    )?;
    let rows = statement.query_map([], |row| {
        let scores = stored_scores(&row.get::<_, String>(6)?);
        Ok(CatalogFingerprintRow {
            route_key: row.get(0)?,
            capacity_key: row.get(1)?,
            provider_id: row.get(2)?,
            model_id: row.get(3)?,
            variant: row.get(4)?,
            family: row.get(5)?,
            elo: scores.elo,
            eq: scores.eq,
            speed: scores.speed,
            context_window: row.get(7)?,
            max_output_tokens: row.get(8)?,
            cost_tier: row.get(9)?,
            cost_per_mtok_out: row.get(10)?,
            input_rate: row.get(11)?,
            cache_read_rate: row.get(12)?,
            cache_write_rate: row.get(13)?,
            reasoning_rate: row.get(14)?,
            supports_vision: row.get::<_, Option<i64>>(15)?.map(|value| value != 0),
            supports_tool_use: row.get::<_, Option<i64>>(16)?.map(|value| value != 0),
            prompt_defaults_json: row.get(17)?,
            confidence: row.get(18)?,
            speed_multiplier: row.get(19)?,
            excluded: row.get::<_, i64>(20)? != 0,
            fusiform_present: row.get::<_, Option<i64>>(21)?.map(|value| value != 0),
            auth_methods_json: row.get(22)?,
            reasoning_efforts_json: row.get(23)?,
            canonical_id: row.get(24)?,
            upstream_status: row.get(25)?,
        })
    })?;
    let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let grades = operator_quality::fingerprint_rows(connection)?;
    let bytes = serde_json::to_vec(&(rows, grades))
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let hex = blake3::hash(&bytes).to_hex().to_string();
    Ok(hex[..16].to_string())
}

/// Count every `model_facts` or operator-grade write through this connection, so a cached
/// catalog fingerprint can tell whether the catalog changed since it was
/// computed. Returns `false` (and installs nothing) when the store has no
/// `model_facts` table.
///
/// The counter and triggers are TEMP objects: they live only on the store's
/// one connection, are never written to the database file, and need no
/// migration. Triggers catch every writer, including ones added later and
/// plain SQL, which a list of Rust call sites that clear the cache would not.
/// Writes from other connections are covered by `PRAGMA data_version` in
/// `CatalogFingerprintKey` instead.
fn install_catalog_change_tracking(connection: &Connection) -> rusqlite::Result<bool> {
    let has_facts: bool = connection.query_row(
        "SELECT EXISTS (SELECT 1 FROM main.sqlite_schema
                        WHERE type = 'table' AND name = 'model_facts')",
        [],
        |row| row.get(0),
    )?;
    if !has_facts {
        return Ok(false);
    }
    // An unqualified table name in a TEMP trigger resolves to the temp schema
    // first, so the triggers update this counter and watch `main.model_facts`.
    connection.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS catalog_facts_writes (
           singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
           writes INTEGER NOT NULL
         );
         INSERT OR IGNORE INTO temp.catalog_facts_writes (singleton, writes) VALUES (1, 0);
         CREATE TEMP TRIGGER IF NOT EXISTS catalog_facts_writes_on_insert
         AFTER INSERT ON model_facts
         BEGIN
           UPDATE catalog_facts_writes SET writes = writes + 1 WHERE singleton = 1;
         END;
         CREATE TEMP TRIGGER IF NOT EXISTS catalog_facts_writes_on_update
         AFTER UPDATE ON model_facts
         BEGIN
           UPDATE catalog_facts_writes SET writes = writes + 1 WHERE singleton = 1;
         END;
         CREATE TEMP TRIGGER IF NOT EXISTS catalog_facts_writes_on_delete
         AFTER DELETE ON model_facts
         BEGIN
           UPDATE catalog_facts_writes SET writes = writes + 1 WHERE singleton = 1;
         END;",
    )?;
    if operator_quality::has_quality_table(connection)? {
        connection.execute_batch(
            "CREATE TEMP TRIGGER IF NOT EXISTS catalog_quality_writes_on_insert
             AFTER INSERT ON routing_operator_quality
             BEGIN UPDATE catalog_facts_writes SET writes = writes + 1 WHERE singleton = 1; END;
             CREATE TEMP TRIGGER IF NOT EXISTS catalog_quality_writes_on_update
             AFTER UPDATE ON routing_operator_quality
             BEGIN UPDATE catalog_facts_writes SET writes = writes + 1 WHERE singleton = 1; END;
             CREATE TEMP TRIGGER IF NOT EXISTS catalog_quality_writes_on_delete
             AFTER DELETE ON routing_operator_quality
             BEGIN UPDATE catalog_facts_writes SET writes = writes + 1 WHERE singleton = 1; END;",
        )?;
    }
    Ok(true)
}

/// Decode a stored `model_facts.scores_json`. Every reader of stored scores goes
/// through this one decoder so they all see the same values: a row that is not
/// a complete `{elo, eq, speed}` object reads as zero scores (unscored) rather
/// than failing the read, wherever it is read.
fn stored_scores(scores_json: &str) -> ModelScores {
    serde_json::from_str(scores_json).unwrap_or_default()
}

/// Retain actionable rejection detail within the ledger's fixed-size bound,
/// then append one reason-count summary for the omitted tail.
fn bound_rejected_json(raw: &str) -> String {
    let Ok(serde_json::Value::Array(rejected)) = serde_json::from_str(raw) else {
        return raw.to_string();
    };
    if rejected.len() <= MAX_STORED_REJECTIONS {
        return raw.to_string();
    }

    // Runtime walls and subscription budgets explain why otherwise usable
    // routes were refused. Preserve their deadlines and reserve arithmetic
    // ahead of catalog-tail exclusions so an outage remains diagnosable. A
    // credential refusal names the credential that has to be fixed, so it is
    // kept as well.
    let is_actionable = |entry: &&serde_json::Value| {
        matches!(
            entry.get("reason").and_then(serde_json::Value::as_str),
            Some(
                "backoff_active"
                    | "cooldown_active"
                    | "quota_exhausted"
                    | "subscription_reserve"
                    | "credential_unentitled"
                    | "credential_billing_exhausted"
            )
        )
    };
    let mut prioritized = Vec::with_capacity(rejected.len());
    prioritized.extend(rejected.iter().filter(is_actionable));
    prioritized.extend(rejected.iter().filter(|entry| !is_actionable(entry)));

    let mut reasons = BTreeMap::<String, usize>::new();
    for entry in &prioritized[MAX_STORED_REJECTIONS..] {
        let reason = entry
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        *reasons.entry(reason.to_string()).or_default() += 1;
    }
    // A route the caller's host does not serve is the answer to "why was this
    // model never considered", and a count alone cannot answer it for a named
    // model. The omitted unavailable routes keep their keys in the summary, up
    // to a bound, so a model missing from a host's list reads as unavailable
    // rather than as absent from the decision.
    let omitted_not_available = prioritized[MAX_STORED_REJECTIONS..]
        .iter()
        .filter(|entry| {
            entry.get("reason").and_then(serde_json::Value::as_str) == Some("not_available")
        })
        .filter_map(|entry| entry.get("routeKey").cloned())
        .take(MAX_OMITTED_NOT_AVAILABLE_KEYS)
        .collect::<Vec<_>>();
    let truncated = prioritized.len() - MAX_STORED_REJECTIONS;
    let mut bounded = prioritized[..MAX_STORED_REJECTIONS]
        .iter()
        .map(|entry| (*entry).clone())
        .collect::<Vec<_>>();
    let mut summary = serde_json::json!({
        "truncated": truncated,
        "reasons": reasons,
    });
    if !omitted_not_available.is_empty() {
        summary["notAvailable"] = serde_json::Value::Array(omitted_not_available);
    }
    bounded.push(summary);
    serde_json::to_string(&bounded).unwrap_or_else(|_| raw.to_string())
}

/// Construct a local sqlite `StorageDescriptor` for DEV/standalone use, before
/// subc's `HELLO_ACK.storage` delivery lands. This is the ONLY descriptor-
/// acquisition site, so flipping to the subc-delivered descriptor is a one-line
/// change here (the rest of the module takes a descriptor it does not construct).
pub fn dev_descriptor(module_id: &str, sqlite_path: impl Into<String>) -> StorageDescriptor {
    StorageDescriptor {
        module_id: module_id.to_string(),
        storage_namespace: ROUTE_STATE_NAMESPACE.to_string(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: sqlite_path.into(),
        },
    }
}

/// The sqlite-backed route-state store. Opening acquires the single-writer lease
/// and applies the `route_state` migration set (run-once, including the seed).
pub struct SqliteRouteStateStore {
    store: SqliteStore,
    /// Class vectors are means over every complete token profile in a seven-day
    /// window, recomputed from the whole population on every route selection
    /// before this cache existed (Loop R184: 1.9 million VM steps per selection
    /// at 100k profiles). A computed vector is reused until usage is written
    /// (`usage_generation` moves) or the window's minute boundary passes, so
    /// selection pays the scan at most once a minute per class while usage is
    /// quiet and never serves a vector that predates a write.
    class_vectors: Mutex<HashMap<String, CachedClassVector>>,
    usage_generation: AtomicU64,
    /// Same-model score disagreements already logged by this process, so a
    /// read on every selection does not repeat the same warning.
    reported_score_conflicts: Mutex<HashSet<String>>,
    /// The catalog fingerprint stamped on each new decision row, kept with the
    /// catalog state it was computed from (see `CatalogFingerprintKey`). Before
    /// this cache, every decision write read and hashed every `model_facts` row.
    /// `None` until a fingerprint is computed while the change tracking is in
    /// place, and reset whenever that tracking has to be installed again.
    catalog_fingerprint: Mutex<Option<CachedCatalogFingerprint>>,
}

/// What a cached catalog fingerprint is valid for: this connection's count of
/// `model_facts` writes (bumped by the temporary triggers that
/// `install_catalog_change_tracking` creates) and SQLite's `data_version`,
/// which moves when another connection commits a change to the database file.
/// While both are unchanged, `model_facts` holds what it held when the
/// fingerprint was computed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CatalogFingerprintKey {
    facts_writes: i64,
    data_version: i64,
}

#[derive(Debug, Clone)]
struct CachedCatalogFingerprint {
    key: CatalogFingerprintKey,
    fingerprint: String,
}

/// Read the current `CatalogFingerprintKey`. Fails when the write counter
/// that `install_catalog_change_tracking` creates is missing on this
/// connection.
fn read_catalog_fingerprint_key(
    connection: &Connection,
) -> rusqlite::Result<CatalogFingerprintKey> {
    Ok(CatalogFingerprintKey {
        facts_writes: connection.query_row(
            "SELECT writes FROM temp.catalog_facts_writes WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?,
        data_version: connection.query_row("PRAGMA data_version", [], |row| row.get(0))?,
    })
}
#[derive(Debug, Clone)]
struct CachedClassVector {
    usage_generation: u64,
    window_minute: i64,
    vector: Option<ClassVector>,
}

/// The last quota snapshot the routing module accepted, as it serialized it.
/// `observed_at` is the local poll clock (Unix milliseconds) at acceptance, so a
/// restarted module can report how old the reading it restored is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedQuotaSnapshot {
    pub windows_json: String,
    pub observed_at: i64,
}

/// One stored per-account quota reading. `entry_json` is the whole Insula entry
/// as published; `fetched_at` is that entry's producer date, kept verbatim so a
/// restart restores the original source time rather than a local stamp.
/// `received_at` and `tombstoned_at` are local Unix milliseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedAccountReading {
    pub provider: String,
    pub account_id: String,
    pub entry_json: String,
    pub fetched_at: Option<String>,
    pub received_at: i64,
    pub tombstoned_at: Option<i64>,
}

/// Per-provider Insula completeness, in local Unix milliseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedProviderCompleteness {
    pub provider: String,
    pub last_polled_at: i64,
    pub last_poll_complete: bool,
    pub last_complete_at: Option<i64>,
}

/// The last vault roster routing listed, serialized by the routing module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedVaultRoster {
    pub view: String,
    pub roster_json: String,
    pub fetched_at: i64,
}

/// One failed decision from a head's recent launches, enough to tell which
/// provider refused and why without loading the audit payloads.
#[derive(Debug, Clone, PartialEq)]
pub struct ParentFailedDecision {
    pub route_key: String,
    pub provider_id: String,
    pub outcome_error: String,
    pub outcome_at: i64,
}

/// One durable routing decision rendered for a diagnostic reader. JSON fields stay
/// serialized here so the wire layer can return exactly the evidence that was
/// recorded rather than reconstructing an explanation from today's catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct DecisionExplainRow {
    pub allowed_providers_json: Option<String>,
    pub id: String,
    pub decision_kind: String,
    pub task_id: Option<String>,
    pub parent_session_id: Option<String>,
    pub harness: Option<String>,
    pub project_root: Option<String>,
    /// The project the decision's scope was bound to, when it had one.
    pub project_id: Option<String>,
    /// The session folder the decision's scope was bound to, when it had one.
    pub workdir: Option<String>,
    pub send_id: Option<String>,
    pub target_agent: String,
    pub requirements_json: String,
    pub selected_route_key: Option<String>,
    pub selected_provider_id: Option<String>,
    pub selected_model_id: Option<String>,
    pub selected_variant: Option<String>,
    pub selected_harness: Option<String>,
    pub selected_workload: Option<String>,
    pub score_json: String,
    pub rejected_json: String,
    pub explanation_json: Option<String>,
    pub quota_signal_json: Option<String>,
    pub policy_revision: Option<String>,
    pub anchor_version: i64,
    pub catalog_fingerprint: Option<String>,
    pub created_at: i64,
    pub outcome: Option<String>,
    pub outcome_at: Option<i64>,
    pub outcome_error: Option<String>,
}

/// Optional predicates for the compact decision-ledger list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DecisionListFilter {
    pub send_id: Option<String>,
    pub target_agent: Option<String>,
    pub route_key: Option<String>,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    pub limit: usize,
}

/// One compact decision-ledger list row. The top rejection is derived in SQLite,
/// so callers do not need the persisted rejection payload to render a recent list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionListRow {
    pub id: String,
    pub created_at: i64,
    pub selected_route_key: Option<String>,
    pub decision_kind: String,
    pub top_rejection_reason: Option<String>,
    pub top_rejection_count: usize,
}

/// One `aa_inferred` catalog row for the operator's hand-placement worklist.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InferredScoreRow {
    pub route_key: String,
    pub aa_index: Option<f64>,
    pub elo: f64,
    pub review_by_ms: Option<i64>,
    pub review_by_passed: bool,
}

/// A catalog row with no score, waiting for Fusiform or a hand placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnscoredRow {
    pub route_key: String,
}

/// An operator-scored route whose catalog row states no canonical model id,
/// so its score cannot be shared with other routes of the same model.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalGapRow {
    pub route_key: String,
    /// `curated` (migration seed) or `override` (operator write).
    pub source: String,
    pub elo: f64,
    pub eq: f64,
    /// Whether Fusiform serves this route at all. `false` means the catalog has
    /// no row for it, so there is nothing for Fusiform to annotate yet.
    pub fusiform_served: bool,
}

impl SqliteRouteStateStore {
    /// Open from a resolved descriptor, applying migrations. The single-writer
    /// lease location is derived by the lib from the descriptor's db path (one
    /// lease per database, by construction), so there is no lease-dir to pass.
    pub fn open(descriptor: &StorageDescriptor) -> RouteStateResult<Self> {
        Self::open_with_migrations(descriptor, &route_state_migrations())
    }

    /// Build an older on-disk schema for upgrade tests that exercise live callers.
    #[cfg(any(test, feature = "test-support"))]
    pub fn open_at_version_for_test(
        descriptor: &StorageDescriptor,
        version: u32,
    ) -> RouteStateResult<Self> {
        let migrations: Vec<_> = route_state_migrations()
            .into_iter()
            .filter(|migration| migration.version <= version)
            .collect();
        Self::open_with_migrations(descriptor, &migrations)
    }

    /// Seed the decision columns an older build wrote, without supplying a
    /// payment class. Upgrade tests must exercise the migration's own backfill.
    #[cfg(any(test, feature = "test-support"))]
    pub fn seed_preupgrade_decision_for_test(
        &self,
        decision: &V1Decision,
        v1_binding: bool,
    ) -> RouteStateResult<()> {
        self.store.with_conn(|connection| {
            connection.execute(
                "INSERT INTO model_route_decisions (id, target_agent, consumer, requirements_json,
                 selected_route_key, selected_provider_id, selected_model_id, score_json,
                 rejected_json, anchor_version, created_at)
                 VALUES (?1, ?2, ?2, '{}', ?3, ?4, ?5, '{}', '[]', 1, ?6)",
                params![decision.decision_id, decision.consumer, format!("{}/{}", decision.provider, decision.model),
                    decision.provider, decision.model, decision.created_at_ms],
            )?;
            if v1_binding {
                connection.execute(
                    "INSERT INTO routing_v1_decision (decision_id, principal, consumer, provider, model,
                     runner_module, created_at_ms, expires_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![decision.decision_id, decision.principal, decision.consumer, decision.provider,
                        decision.model, decision.runner_module, decision.created_at_ms, decision.expires_at_ms],
                )?;
            }
            Ok(())
        }).map_err(err)
    }

    fn open_with_migrations(
        descriptor: &StorageDescriptor,
        migrations: &[Migration],
    ) -> RouteStateResult<Self> {
        let store = open_sqlite(descriptor).map_err(err)?;
        // Migration 58 rebuilds `account_pin` while `account_admission` holds a
        // foreign key to it; with enforcement on, dropping the old table would
        // first delete every pin and fail on the admissions that name them.
        // SQLite ignores `PRAGMA foreign_keys` inside a transaction and the
        // shared runner opens one per migration, so enforcement is suspended
        // around the whole run here and switched back on before serving. The
        // migration checks its own result with `foreign_key_check`.
        store
            .with_conn(|conn| conn.pragma_update(None, "foreign_keys", "OFF"))
            .map_err(err)?;
        // A store written by a longer chain than this binary carries used to
        // migrate as a silent no-op and then serve a schema the binary cannot
        // query. The route-state chain rebuilds tables and re-keys facts, so an
        // ahead store is refused here, naming both versions.
        let migration = store.migrate(ROUTE_STATE_NAMESPACE, migrations);
        store
            .with_conn(|conn| {
                conn.pragma_update(None, "foreign_keys", "ON")?;
                let enabled: i64 = conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
                if enabled == 1 {
                    Ok(())
                } else {
                    Err(rusqlite::Error::InvalidQuery)
                }
            })
            .map_err(|error| err(format!("re-enabling foreign keys after migration: {error}")))?;
        let outcome = migration.map_err(err)?;
        if outcome.store_ahead() {
            return Err(err(format!(
                "store_ahead: namespace {ROUTE_STATE_NAMESPACE} is at version {} but this binary's chain ends at {}; \
                 this binary would serve a schema it cannot query (binary rollback or reset numbering); \
                 deploy a binary carrying version {} or newer",
                outcome.recorded, outcome.chain_max, outcome.recorded
            )));
        }
        validate_family_spellings(&store)?;
        // A failure here costs only the cache: every fingerprint read finds the
        // tracking missing, computes the value fresh and tries to install it again.
        if let Err(error) = store.with_conn(install_catalog_change_tracking) {
            tracing::warn!(%error, "catalog change tracking not installed at open; fingerprints are computed fresh until it is");
        }
        Ok(Self {
            store,
            class_vectors: Mutex::new(HashMap::new()),
            usage_generation: AtomicU64::new(0),
            reported_score_conflicts: Mutex::new(HashSet::new()),
            catalog_fingerprint: Mutex::new(None),
        })
    }

    /// The single-writer fence epoch for this open (for diagnostics / future
    /// cloud-backend CAS).
    pub fn epoch(&self) -> u64 {
        self.store.epoch()
    }

    /// The fingerprint for the catalog currently available to routing decisions.
    pub fn catalog_fingerprint(&self) -> RouteStateResult<String> {
        self.store
            .with_conn(|connection: &Connection| self.current_catalog_fingerprint(connection))
            .map_err(err)
    }

    /// The catalog fingerprint as `calculate_catalog_fingerprint` would compute
    /// it now, reusing the last computed value while `model_facts` is unchanged.
    ///
    /// A value computed inside an open transaction is returned but not kept:
    /// that transaction may still roll back, and the rollback would also undo
    /// the write count the value was keyed on, so a later, different catalog
    /// could reach the same count and be served a fingerprint for rows that were
    /// never committed.
    ///
    /// The cache can never fail a caller that a fresh hash would not fail. If
    /// the change tracking cannot be read (for example, the TEMP objects are
    /// gone because the connection is no longer the one they were created on),
    /// the value is computed fresh, is not cached, and the tracking is
    /// installed again for the next read.
    fn current_catalog_fingerprint(&self, connection: &Connection) -> rusqlite::Result<String> {
        let key = match read_catalog_fingerprint_key(connection) {
            Ok(key) => key,
            Err(cause) => {
                let fingerprint = calculate_catalog_fingerprint(connection)?;
                self.reinstall_catalog_change_tracking(connection, &cause);
                return Ok(fingerprint);
            }
        };
        let mut cache = self
            .catalog_fingerprint
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(cached) = cache.as_ref().filter(|cached| cached.key == key) {
            return Ok(cached.fingerprint.clone());
        }
        let fingerprint = calculate_catalog_fingerprint(connection)?;
        if connection.is_autocommit() {
            *cache = Some(CachedCatalogFingerprint {
                key,
                fingerprint: fingerprint.clone(),
            });
        }
        Ok(fingerprint)
    }

    /// Install the catalog change tracking once more after reading it failed,
    /// and forget the cached fingerprint. A recreated write counter starts from
    /// zero, so without the reset it could climb back to the count of the
    /// cached entry over a different catalog and serve that entry's value.
    /// A failed install is only logged; the next read tries again.
    fn reinstall_catalog_change_tracking(&self, connection: &Connection, cause: &rusqlite::Error) {
        *self
            .catalog_fingerprint
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        match install_catalog_change_tracking(connection) {
            Ok(true) => tracing::warn!(
                %cause,
                "catalog change tracking was missing on the store connection; reinstalled it"
            ),
            Ok(false) => {}
            Err(error) => tracing::warn!(
                %cause,
                %error,
                "catalog change tracking is missing and could not be reinstalled; \
                 fingerprints are computed fresh until it is"
            ),
        }
    }

    /// The reasoning-effort values the catalog publishes for every stored model.
    pub fn reasoning_catalog(&self) -> RouteStateResult<ReasoningCatalog> {
        self.store
            .with_conn(|connection: &Connection| {
                reasoning_catalog::read_reasoning_catalog(connection)
            })
            .map_err(err)
    }

    /// All durable catalog rows, including each row's routing-exclusion state.
    ///
    /// This is intentionally separate from `RouteStateStore::get_facts`: selection
    /// only needs capabilities, while read-only inventory consumers must not infer
    /// policy state from a missing capability or a filtered result.
    pub fn get_catalog_facts(&self) -> RouteStateResult<Vec<CatalogFact>> {
        let mut rows = self
            .store
            .with_conn(|connection: &Connection| {
                let mut statement =
                    connection.prepare("SELECT * FROM model_facts ORDER BY route_key")?;
                let rows = statement.query_map([], |row| {
                    let fact = row_to_fact(row)?;
                    Ok(CatalogFact {
                        price_unknown: fact.cost_per_m_tok_out.is_none()
                            && fact.cost_tier.is_none(),
                        fact,
                        excluded: row.get::<_, i64>("excluded")? != 0,
                        facts_source: row.get("facts_source")?,
                        fusiform_version: row.get("fusiform_version")?,
                        fusiform_present: row
                            .get::<_, Option<i64>>("fusiform_present")?
                            .map(|value| value != 0),
                        canonical_id: row.get("canonical_id")?,
                        reasoning_efforts: row
                            .get::<_, Option<String>>("reasoning_efforts_json")?
                            .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok()),
                        score_borrowed_from: None,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)?;
        // The inventory shows the Elo/EQ routing actually uses, and names the
        // route a shared score came from.
        let canonical_ids = rows
            .iter()
            .map(|row| row.canonical_id.clone())
            .collect::<Vec<_>>();
        let mut facts = rows.iter().map(|row| row.fact.clone()).collect::<Vec<_>>();
        let borrowed_from = self.share_canonical_scores(&mut facts, &canonical_ids);
        for ((row, fact), borrowed) in rows.iter_mut().zip(facts).zip(borrowed_from) {
            row.fact = fact;
            row.score_borrowed_from = borrowed;
        }
        Ok(rows)
    }

    /// Override score rows that Fusiform does not currently list. Operator
    /// overrides remain routable, so catalog refresh reports these instead of
    /// deleting or silently hiding them.
    pub fn unserved_override_route_keys(&self) -> RouteStateResult<Vec<String>> {
        self.store
            .with_conn(|connection: &Connection| {
                let mut statement = connection.prepare(
                    "SELECT route_key FROM model_facts
                     WHERE source = 'override' AND COALESCE(fusiform_present, 0) != 1
                     ORDER BY route_key",
                )?;
                let routes = statement.query_map([], |row| row.get(0))?;
                routes.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    /// List every absent route before pruning unpicked non-overrides whose last
    /// selection is older than 30 days. Decision history is deliberately kept.
    pub fn prune_unserved_unpicked_facts(&self) -> RouteStateResult<Vec<String>> {
        self.store
            .with_conn(|connection: &Connection| {
                let tx = connection.unchecked_transaction()?;
                let mut statement = tx.prepare(
                    "SELECT route_key FROM model_facts
                     WHERE fusiform_present = 0 ORDER BY route_key",
                )?;
                let unserved = statement
                    .query_map([], |row| row.get(0))?
                    .collect::<rusqlite::Result<Vec<String>>>()?;
                drop(statement);
                tx.execute(
                    "DELETE FROM model_route_state
                     WHERE route_key IN (
                       SELECT route_key FROM model_facts
                       WHERE fusiform_present = 0
                         AND source <> 'override'
                         AND NOT EXISTS (
                           SELECT 1 FROM model_route_decisions
                           WHERE model_route_decisions.selected_route_key = model_facts.route_key
                             AND model_route_decisions.created_at >=
                               CAST(strftime('%s', 'now') AS INTEGER) * 1000
                               - 30 * 24 * 60 * 60 * 1000
                         )
                     )",
                    [],
                )?;
                tx.execute(
                    "DELETE FROM model_facts
                     WHERE fusiform_present = 0
                       AND source <> 'override'
                       AND NOT EXISTS (
                         SELECT 1 FROM model_route_decisions
                         WHERE model_route_decisions.selected_route_key = model_facts.route_key
                           AND model_route_decisions.created_at >=
                             CAST(strftime('%s', 'now') AS INTEGER) * 1000
                             - 30 * 24 * 60 * 60 * 1000
                       )",
                    [],
                )?;
                tx.commit()?;
                Ok(unserved)
            })
            .map_err(err)
    }

    /// Resolve shared Elo/EQ scores before deciding which rows routing may
    /// select. `canonical_id` and `variant` group routes for one upstream model,
    /// and `scores_json` stores their scores. Resolve that group before filtering
    /// so a row withdrawn by Fusiform can still supply its score to a served
    /// sibling. A row is routable when `fusiform_present` is 1, when the flag is
    /// still NULL (an older row before its first full sync), or when `source` is
    /// an operator `override`. The bool carries that final routing decision.
    fn read_resolved_facts(&self) -> RouteStateResult<Vec<(ModelFact, bool)>> {
        let rows = self
            .store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT *,
                            (COALESCE(fusiform_present, 1) = 1 OR source = 'override') AS present_now
                     FROM model_facts",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row_to_fact(row)?,
                        row.get::<_, Option<String>>("canonical_id")?,
                        row.get::<_, bool>("present_now")?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)?;
        let mut facts = Vec::with_capacity(rows.len());
        let mut canonical_ids = Vec::with_capacity(rows.len());
        let mut present = Vec::with_capacity(rows.len());
        for (fact, canonical_id, present_now) in rows {
            facts.push(fact);
            canonical_ids.push(canonical_id);
            present.push(present_now);
        }
        self.share_canonical_scores(&mut facts, &canonical_ids);
        self.overlay_operator_quality(&mut facts, &canonical_ids)?;
        Ok(facts.into_iter().zip(present).collect())
    }

    /// Give routes without an operator score the score of a route serving the
    /// same model (see `prefrontal_routing_core::canonical_scores`), and log each
    /// distinct disagreement between same-model operator scores once per process.
    /// Returns, per fact, the route key its score was borrowed from.
    fn share_canonical_scores(
        &self,
        facts: &mut [ModelFact],
        canonical_ids: &[Option<String>],
    ) -> Vec<Option<String>> {
        let resolution = resolve_canonical_scores(facts, canonical_ids);
        if !resolution.conflicts.is_empty() {
            if let Ok(mut reported) = self.reported_score_conflicts.lock() {
                for conflict in &resolution.conflicts {
                    if !reported.insert(conflict.signature()) {
                        continue;
                    }
                    let scores = conflict
                        .scores
                        .iter()
                        .map(|score| {
                            format!("{} elo={} eq={}", score.route_key, score.elo, score.eq)
                        })
                        .collect::<Vec<_>>()
                        .join("; ");
                    tracing::warn!(
                        canonical_id = %conflict.canonical_id,
                        variant = conflict.variant.as_deref().unwrap_or(""),
                        donor = %conflict.donor_route_key,
                        scores = %scores,
                        "routes serving the same model carry different operator scores; \
                         each keeps its own, unscored routes borrow from the donor"
                    );
                }
            }
        }
        resolution.borrowed_from
    }

    /// Every stored route key that carries a non-blank canonical model id.
    pub fn canonical_ids_by_route_key(&self) -> RouteStateResult<HashMap<String, String>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT route_key, canonical_id FROM model_facts
                     WHERE canonical_id IS NOT NULL AND trim(canonical_id) != ''",
                )?;
                let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
                rows.collect::<rusqlite::Result<HashMap<String, String>>>()
            })
            .map_err(err)
    }

    /// Every routable route key carrying an operator score whose catalog row
    /// states no canonical id. These routes neither lend nor borrow a shared
    /// score; the list is what to ask Fusiform to identify. Deprecated routes
    /// are left out: they are never offered.
    pub fn list_canonical_gaps(&self) -> RouteStateResult<Vec<CanonicalGapRow>> {
        let rows = self
            .store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT route_key, source, scores_json, facts_source
                     FROM model_facts
                     WHERE (canonical_id IS NULL OR trim(canonical_id) = '')
                       AND source IN ('seed', 'override')
                       AND COALESCE(fusiform_present, 1) = 1
                       AND COALESCE(upstream_status, '') != 'deprecated'
                     ORDER BY route_key",
                )?;
                let rows = stmt.query_map([], |row| {
                    let scores = stored_scores(&row.get::<_, String>(2)?);
                    Ok(CanonicalGapRow {
                        route_key: row.get(0)?,
                        source: FactSource::parse(&row.get::<_, String>(1)?)
                            .provenance()
                            .as_str()
                            .to_string(),
                        elo: scores.elo,
                        eq: scores.eq,
                        fusiform_served: row.get::<_, Option<String>>(3)?.as_deref()
                            == Some("fusiform"),
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)?;
        // Same test as score sharing: a curated stamp with no Elo is not a score.
        Ok(rows
            .into_iter()
            .filter(|row| row.elo.is_finite() && row.elo > 0.0)
            .collect())
    }

    /// Route keys of one model (all rows with this canonical id) at `variant`,
    /// for a score placement that covers every route of the model.
    pub fn route_keys_for_canonical_id(
        &self,
        canonical_id: &str,
        variant: Option<&str>,
    ) -> RouteStateResult<Vec<String>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT route_key FROM model_facts
                     WHERE canonical_id = ?1 AND variant IS ?2
                     ORDER BY route_key",
                )?;
                let rows = stmt.query_map(params![canonical_id, variant], |row| row.get(0))?;
                rows.collect::<rusqlite::Result<Vec<String>>>()
            })
            .map_err(err)
    }

    /// Read the last Fusiform snapshot cursor committed with catalog facts.
    pub fn fusiform_catalog_cursor(&self) -> RouteStateResult<Option<FusiformCatalogCursor>> {
        self.store
            .with_conn(|connection: &Connection| {
                connection
                    .query_row(
                        "SELECT catalog_version, resolved_at_ms
                         FROM fusiform_catalog_cursor WHERE singleton = 1",
                        [],
                        |row| {
                            Ok(FusiformCatalogCursor {
                                catalog_version: row.get(0)?,
                                resolved_at_ms: row.get(1)?,
                            })
                        },
                    )
                    .optional()
            })
            .map_err(err)
    }

    /// Apply a batch of Fusiform facts and advance its cursor atomically. The
    /// catalog refresh loop uses `apply_fusiform_catalog_snapshot_for_providers`
    /// when it has a complete snapshot, so rows omitted from that snapshot are
    /// marked absent before they are considered for cleanup.
    /// Matching is always the exact `(provider_id, model_id)` pair; variants of
    /// that pair receive the same external facts without changing variant policy.
    pub fn apply_fusiform_catalog(
        &self,
        facts: &[FusiformFactUpdate],
        cursor: FusiformCatalogCursor,
    ) -> RouteStateResult<()> {
        self.apply_fusiform_catalog_for_providers(facts, cursor, &HashSet::new())
    }

    /// Credential metadata's `provider_ids` and explicit `route.model_upsert`
    /// requests name providers the operator intends to use. Admit their rows
    /// without importing thousands of models under unrelated providers.
    pub fn apply_fusiform_catalog_for_providers(
        &self,
        facts: &[FusiformFactUpdate],
        cursor: FusiformCatalogCursor,
        mapped_providers: &HashSet<String>,
    ) -> RouteStateResult<()> {
        self.apply_fusiform_catalog_for_providers_inner(
            facts,
            cursor,
            mapped_providers,
            false,
            &HashSet::new(),
        )
    }

    /// Apply a complete catalog snapshot, treating existing rows omitted from
    /// it as absent before applying the served facts.
    ///
    /// `retained` names `(provider_id, model_id)` pairs the catalog did serve
    /// but whose entry this consumer could not read. Their rows are left exactly
    /// as stored: an entry that failed to decode says nothing about whether the
    /// model still exists, so it must not be marked absent and later pruned.
    pub fn apply_fusiform_catalog_snapshot_for_providers(
        &self,
        facts: &[FusiformFactUpdate],
        cursor: FusiformCatalogCursor,
        mapped_providers: &HashSet<String>,
        retained: &HashSet<(String, String)>,
    ) -> RouteStateResult<()> {
        self.apply_fusiform_catalog_for_providers_inner(
            facts,
            cursor,
            mapped_providers,
            true,
            retained,
        )
    }

    fn apply_fusiform_catalog_for_providers_inner(
        &self,
        facts: &[FusiformFactUpdate],
        cursor: FusiformCatalogCursor,
        mapped_providers: &HashSet<String>,
        complete_snapshot: bool,
        retained: &HashSet<(String, String)>,
    ) -> RouteStateResult<()> {
        let scores_json = serde_json::to_string(&ModelScores::default()).map_err(err)?;
        // Serialized as `[[provider_id, model_id], ...]` for json_each below.
        let retained_json =
            serde_json::to_string(&retained.iter().collect::<Vec<_>>()).map_err(err)?;
        self.store
            .with_conn(|connection: &Connection| {
                let tx = connection.unchecked_transaction()?;
                if complete_snapshot {
                    tx.execute(
                        "UPDATE model_facts
                         SET facts_source = 'fusiform', fusiform_version = ?1,
                             fusiform_present = 0, updated_at = ?2
                         WHERE NOT EXISTS (
                           SELECT 1 FROM json_each(?3) AS kept
                           WHERE json_extract(kept.value, '$[0]') = model_facts.provider_id
                             AND json_extract(kept.value, '$[1]') = model_facts.model_id
                         )",
                        params![cursor.catalog_version, now_ms(), retained_json],
                    )?;
                }
                // Every stored (provider, model) pair, read once for the whole
                // batch. A live catalog carries thousands of entries, and asking
                // SQLite per entry whether its pair or its provider is already
                // stored cost two counting queries each. A row minted below is
                // added here too, so a later entry in the same batch sees it
                // exactly as a fresh query would.
                let mut stored_models: HashMap<String, HashSet<String>> = HashMap::new();
                {
                    let mut statement =
                        tx.prepare("SELECT provider_id, model_id FROM model_facts")?;
                    let pairs = statement.query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })?;
                    for pair in pairs {
                        let (provider_id, model_id) = pair?;
                        stored_models.entry(provider_id).or_default().insert(model_id);
                    }
                }
                for fact in facts {
                    if fact.model_id.ends_with("-fast") {
                        // The request service rejects -fast IDs at admission, so routing must not retain them.
                        continue;
                    }
                    if fact.provider_id.trim().is_empty() || fact.model_id.trim().is_empty() {
                        return Err(rusqlite::Error::InvalidParameterName(
                            "Fusiform provider/model identity must be non-empty".to_string(),
                        ));
                    }
                    let auth_methods_json = fact
                        .auth_methods
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                    let reasoning_efforts_json = fact
                        .reasoning_efforts
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                    let route_key = prefrontal_routing_core::make_route_key(
                        &fact.provider_id,
                        &fact.model_id,
                        None,
                    );
                    let pair_stored = stored_models
                        .get(&fact.provider_id)
                        .is_some_and(|models| models.contains(&fact.model_id));
                    if !pair_stored {
                        // A model first seen already withdrawn has nothing to route and
                        // nothing for the operator to place; minting it would only add a
                        // dead row per retired model under every routed provider.
                        if !fact.present {
                            continue;
                        }
                        // Discover models only under providers already in the store
                        // or authorized by credential metadata or a score placement.
                        // Importing every provider would turn route.list_unscored
                        // into thousands of unrelated rows instead of an operator
                        // worklist, and deepen every selection's candidate scan.
                        let known_provider = stored_models.contains_key(&fact.provider_id);
                        if !known_provider && !mapped_providers.contains(&fact.provider_id) {
                            continue;
                        }
                        tx.execute(
                            "INSERT INTO model_facts (
                               route_key, capacity_key, provider_id, model_id, variant, family,
                               scores_json, context_window, max_output_tokens, cost_tier,
                               cost_per_mtok_out, input_rate, cache_read_rate,
                               cache_write_rate, reasoning_rate, supports_vision,
                               supports_tool_use, prompt_defaults_json, confidence, notes,
                               speed_multiplier, excluded, source, updated_at, aa_index,
                               review_by_ms, eq_source, facts_source, fusiform_version,
                               fusiform_present, context_window_source,
                               max_output_tokens_source, cost_tier_source,
                               cost_per_mtok_out_source, supports_vision_source,
                               supports_tool_use_source, auth_methods_json,
                               reasoning_efforts_json, canonical_id, upstream_status
                             ) VALUES (
                               ?1, ?1, ?2, ?3, NULL, NULL, ?4, ?5, ?6, ?7,
                               ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL, 'unknown',
                               'discovered by Fusiform; awaiting operator score placement',
                               NULL, 0, 'unscored', ?15, NULL, NULL, NULL,
                               'fusiform', ?16, ?17,
                               'fusiform', 'fusiform', 'fusiform', 'fusiform',
                               'fusiform', 'fusiform', ?18, ?19, ?20, ?21
                             )",
                            params![
                                route_key,
                                fact.provider_id,
                                fact.model_id,
                                scores_json,
                                fact.context_window,
                                fact.max_output_tokens,
                                fact.cost_tier,
                                fact.cost_per_mtok_out,
                                fact.input_rate,
                                fact.cache_read_rate,
                                fact.cache_write_rate,
                                fact.reasoning_rate,
                                bool_to_int(fact.supports_vision),
                                bool_to_int(fact.supports_tool_use),
                                now_ms(),
                                cursor.catalog_version,
                                if fact.present { 1_i64 } else { 0_i64 },
                                auth_methods_json,
                                reasoning_efforts_json,
                                fact.canonical_id,
                                fact.upstream_status,
                            ],
                        )?;
                        stored_models
                            .entry(fact.provider_id.clone())
                            .or_default()
                            .insert(fact.model_id.clone());
                        // The model_facts insert trigger has already created this route's
                        // state row, inheriting any live provider wall and its cause.
                        derive_provider_key(&fact.provider_id).ok_or_else(|| {
                            rusqlite::Error::InvalidParameterName(format!(
                                "provider {} has no canonical key",
                                fact.provider_id
                            ))
                        })?;
                    } else {
                        tx.execute(
                            "UPDATE model_facts SET
                               context_window = CASE WHEN context_window_source = 'override'
                                 THEN context_window ELSE ?1 END,
                               max_output_tokens = CASE WHEN max_output_tokens_source = 'override'
                                 THEN max_output_tokens ELSE ?2 END,
                               cost_tier = CASE WHEN cost_tier_source = 'override'
                                 THEN cost_tier ELSE ?3 END,
                               cost_per_mtok_out = CASE WHEN cost_per_mtok_out_source = 'override'
                                 THEN cost_per_mtok_out ELSE ?4 END,
                               input_rate = ?5,
                               cache_read_rate = ?6,
                               cache_write_rate = ?7,
                               reasoning_rate = ?8,
                               supports_vision = CASE WHEN supports_vision_source = 'override'
                                 THEN supports_vision ELSE ?9 END,
                               supports_tool_use = CASE WHEN supports_tool_use_source = 'override'
                                 THEN supports_tool_use ELSE ?10 END,
                               context_window_source = CASE WHEN context_window_source = 'override'
                                 THEN context_window_source ELSE 'fusiform' END,
                               max_output_tokens_source = CASE WHEN max_output_tokens_source = 'override'
                                 THEN max_output_tokens_source ELSE 'fusiform' END,
                               cost_tier_source = CASE WHEN cost_tier_source = 'override'
                                 THEN cost_tier_source ELSE 'fusiform' END,
                               cost_per_mtok_out_source = CASE WHEN cost_per_mtok_out_source = 'override'
                                 THEN cost_per_mtok_out_source ELSE 'fusiform' END,
                               supports_vision_source = CASE WHEN supports_vision_source = 'override'
                                 THEN supports_vision_source ELSE 'fusiform' END,
                               supports_tool_use_source = CASE WHEN supports_tool_use_source = 'override'
                                 THEN supports_tool_use_source ELSE 'fusiform' END,
                               facts_source = 'fusiform', fusiform_version = ?11,
                               fusiform_present = ?12, updated_at = ?13,
                               auth_methods_json = ?16, reasoning_efforts_json = ?17,
                               canonical_id = ?18, upstream_status = ?19
                             WHERE provider_id = ?14 AND model_id = ?15",
                            params![
                                fact.context_window,
                                fact.max_output_tokens,
                                fact.cost_tier,
                                fact.cost_per_mtok_out,
                                fact.input_rate,
                                fact.cache_read_rate,
                                fact.cache_write_rate,
                                fact.reasoning_rate,
                                bool_to_int(fact.supports_vision),
                                bool_to_int(fact.supports_tool_use),
                                cursor.catalog_version,
                                if fact.present { 1_i64 } else { 0_i64 },
                                now_ms(),
                                fact.provider_id,
                                fact.model_id,
                                auth_methods_json,
                                reasoning_efforts_json,
                                fact.canonical_id,
                                fact.upstream_status,
                            ],
                        )?;
                    }
                }
                tx.execute(
                    "INSERT INTO fusiform_catalog_cursor (
                       singleton, catalog_version, resolved_at_ms, updated_at
                     ) VALUES (1, ?1, ?2, ?3)
                     ON CONFLICT(singleton) DO UPDATE SET
                       catalog_version = excluded.catalog_version,
                       resolved_at_ms = excluded.resolved_at_ms,
                       updated_at = excluded.updated_at",
                    params![cursor.catalog_version, cursor.resolved_at_ms, now_ms()],
                )?;
                tx.commit()
            })
            .map_err(err)
    }

    /// The last announced route-winner ladder as the routing module serialized
    /// it, or `None` before the first ladder was ever recorded.
    pub fn route_winner_baseline(&self) -> RouteStateResult<Option<String>> {
        self.store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT ladder_json FROM route_winner_baseline WHERE singleton = 1",
                    [],
                    |row| row.get(0),
                )
                .optional()
            })
            .map_err(err)
    }

    /// Replace the stored route-winner ladder with `ladder_json`, recording the
    /// change that produced it.
    pub fn put_route_winner_baseline(
        &self,
        ladder_json: &str,
        trigger: &str,
        recorded_at: i64,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "INSERT INTO route_winner_baseline (singleton, ladder_json, trigger, recorded_at)
                     VALUES (1, ?1, ?2, ?3)
                     ON CONFLICT(singleton) DO UPDATE SET
                       ladder_json = excluded.ladder_json,
                       trigger = excluded.trigger,
                       recorded_at = excluded.recorded_at",
                    params![ladder_json, trigger, recorded_at],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    /// The last quota snapshot the routing module persisted, or `None` before the
    /// first accepted poll was ever recorded.
    pub fn quota_snapshot(&self) -> RouteStateResult<Option<PersistedQuotaSnapshot>> {
        self.store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT windows_json, observed_at FROM quota_snapshot WHERE singleton = 1",
                    [],
                    |row| {
                        Ok(PersistedQuotaSnapshot {
                            windows_json: row.get(0)?,
                            observed_at: row.get(1)?,
                        })
                    },
                )
                .optional()
            })
            .map_err(err)
    }

    /// Replace the stored quota snapshot with `windows_json`, accepted from a poll
    /// at `observed_at` (Unix milliseconds on the local poll clock).
    pub fn put_quota_snapshot(&self, windows_json: &str, observed_at: i64) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "INSERT INTO quota_snapshot (singleton, windows_json, observed_at)
                     VALUES (1, ?1, ?2)
                     ON CONFLICT(singleton) DO UPDATE SET
                       windows_json = excluded.windows_json,
                       observed_at = excluded.observed_at",
                    params![windows_json, observed_at],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    /// Every stored per-account quota reading, tombstoned ones included.
    pub fn account_quota_readings(&self) -> RouteStateResult<Vec<PersistedAccountReading>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut statement = c.prepare(
                    "SELECT provider, account_id, entry_json, fetched_at, received_at, tombstoned_at
                     FROM account_quota_reading ORDER BY provider, account_id",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok(PersistedAccountReading {
                        provider: row.get(0)?,
                        account_id: row.get(1)?,
                        entry_json: row.get(2)?,
                        fetched_at: row.get(3)?,
                        received_at: row.get(4)?,
                        tombstoned_at: row.get(5)?,
                    })
                })?;
                rows.collect()
            })
            .map_err(err)
    }

    /// Every stored provider completeness row.
    pub fn account_quota_completeness(
        &self,
    ) -> RouteStateResult<Vec<PersistedProviderCompleteness>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut statement = c.prepare(
                    "SELECT provider, last_polled_at, last_poll_complete, last_complete_at
                     FROM account_quota_completeness ORDER BY provider",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok(PersistedProviderCompleteness {
                        provider: row.get(0)?,
                        last_polled_at: row.get(1)?,
                        last_poll_complete: row.get::<_, i64>(2)? != 0,
                        last_complete_at: row.get(3)?,
                    })
                })?;
                rows.collect()
            })
            .map_err(err)
    }

    /// Upsert the readings and completeness rows one poll changed, in one
    /// transaction. Rows the poll did not name are left as they are: this never
    /// deletes a reading, so a partial poll cannot erase a stored sibling.
    pub fn put_account_quota_poll(
        &self,
        readings: &[PersistedAccountReading],
        completeness: &[PersistedProviderCompleteness],
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = c.unchecked_transaction()?;
                for reading in readings {
                    tx.execute(
                        "INSERT INTO account_quota_reading
                           (provider, account_id, entry_json, fetched_at, received_at, tombstoned_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                         ON CONFLICT(provider, account_id) DO UPDATE SET
                           entry_json = excluded.entry_json,
                           fetched_at = excluded.fetched_at,
                           received_at = excluded.received_at,
                           tombstoned_at = excluded.tombstoned_at",
                        params![
                            reading.provider,
                            reading.account_id,
                            reading.entry_json,
                            reading.fetched_at,
                            reading.received_at,
                            reading.tombstoned_at,
                        ],
                    )?;
                }
                for row in completeness {
                    tx.execute(
                        "INSERT INTO account_quota_completeness
                           (provider, last_polled_at, last_poll_complete, last_complete_at)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(provider) DO UPDATE SET
                           last_polled_at = excluded.last_polled_at,
                           last_poll_complete = excluded.last_poll_complete,
                           last_complete_at = excluded.last_complete_at",
                        params![
                            row.provider,
                            row.last_polled_at,
                            i64::from(row.last_poll_complete),
                            row.last_complete_at,
                        ],
                    )?;
                }
                tx.commit()
            })
            .map_err(err)
    }

    /// The last vault roster routing persisted, or `None` before the first listing.
    pub fn account_vault_roster(&self) -> RouteStateResult<Option<PersistedVaultRoster>> {
        self.store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT view, roster_json, fetched_at FROM account_vault_roster WHERE singleton = 1",
                    [],
                    |row| {
                        Ok(PersistedVaultRoster {
                            view: row.get(0)?,
                            roster_json: row.get(1)?,
                            fetched_at: row.get(2)?,
                        })
                    },
                )
                .optional()
            })
            .map_err(err)
    }

    /// Replace the stored vault roster.
    pub fn put_account_vault_roster(&self, roster: &PersistedVaultRoster) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "INSERT INTO account_vault_roster (singleton, view, roster_json, fetched_at)
                     VALUES (1, ?1, ?2, ?3)
                     ON CONFLICT(singleton) DO UPDATE SET
                       view = excluded.view,
                       roster_json = excluded.roster_json,
                       fetched_at = excluded.fetched_at",
                    params![roster.view, roster.roster_json, roster.fetched_at],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    /// Every `aa_inferred` row with review-by status relative to `now_ms`.
    pub fn list_inferred_rows(&self, now_ms: i64) -> RouteStateResult<Vec<InferredScoreRow>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT route_key, aa_index, scores_json, review_by_ms
                     FROM model_facts
                     WHERE source = 'aa_inferred'
                     ORDER BY route_key",
                )?;
                let rows = stmt.query_map([], |row| {
                    let review_by_ms: Option<i64> = row.get(3)?;
                    Ok(InferredScoreRow {
                        route_key: row.get(0)?,
                        aa_index: row.get(1)?,
                        elo: stored_scores(&row.get::<_, String>(2)?).elo,
                        review_by_ms,
                        review_by_passed: review_by_ms.is_some_and(|deadline| deadline <= now_ms),
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    /// Routable catalog rows that still have no score after same-model sharing:
    /// a route that borrows its model's placement is not awaiting one, and a
    /// route the upstream marks deprecated will never be offered.
    pub fn list_unscored_rows(&self) -> RouteStateResult<Vec<UnscoredRow>> {
        let mut rows = self
            .read_resolved_facts()?
            .into_iter()
            .filter(|(fact, present)| {
                *present
                    && !fact.is_deprecated_upstream()
                    && fact.score_provenance
                        == prefrontal_routing_core::model_facts::ScoreProvenance::Unscored
            })
            .map(|(fact, _)| UnscoredRow {
                route_key: prefrontal_routing_core::make_route_key(
                    &fact.provider_id,
                    &fact.model_id,
                    fact.variant.as_deref(),
                ),
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.route_key.cmp(&right.route_key));
        Ok(rows)
    }

    /// Providers whose workers launched by `parent_session_id` were refused on
    /// content-policy grounds since `since_ms`, with the route and the refusal
    /// time of the newest such decision per provider.
    ///
    /// A content-policy refusal is a fact about the words the caller sent, not
    /// about the route, so it charges no strike and leaves route health
    /// untouched. That is correct for the fleet and exactly wrong for the next
    /// launch from the same head, which carries the same words and routes to the
    /// same provider at full adequacy: two seats lost two workers each this way
    /// in one day, every one on OpenAI, every relaunch back onto OpenAI
    /// (2026-09-20). The refusal text is the classifier's own, matched by the
    /// caller against `classify_failure_attribution`; this read only narrows the
    /// rows to one head's recent failures.
    ///
    /// Every route.select that names a parent session runs this while holding
    /// the store's single connection, so it reads the narrow, indexed
    /// `model_route_parent_failures` copy (migration 054) rather than the
    /// decision ledger, which has no index for it and cost a whole-ledger scan.
    pub fn recent_failed_decisions_for_parent(
        &self,
        parent_session_id: &str,
        since_ms: i64,
    ) -> RouteStateResult<Vec<ParentFailedDecision>> {
        self.store
            .with_conn(|connection: &Connection| {
                let mut statement = connection.prepare(RECENT_PARENT_FAILURES_SQL)?;
                let rows = statement.query_map(params![parent_session_id, since_ms], |row| {
                    Ok(ParentFailedDecision {
                        route_key: row.get(0)?,
                        provider_id: row.get(1)?,
                        outcome_error: row.get(2)?,
                        outcome_at: row.get(3)?,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    /// Record that the provider permanently refused `credential_id`.
    ///
    /// A repeated report for the same pair renews the observation time and the
    /// reported code, so a fence that keeps failing stays fresh and its next
    /// recheck probe waits another interval; the fingerprint it was recorded
    /// against is kept, so a replay cannot move the fence onto a newer listing.
    /// A fingerprint is filled in only where none was known.
    pub fn record_credential_entitlement_fence(
        &self,
        refusal: &prefrontal_routing_core::route_state_store::CredentialRefusal,
        listing_fingerprint: Option<&str>,
        observed_at_ms: i64,
    ) -> RouteStateResult<()> {
        if refusal.provider_id.trim().is_empty() || refusal.credential_id.trim().is_empty() {
            return Ok(());
        }
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "INSERT INTO credential_entitlement_fences
                       (provider_id, credential_id, code, listing_fingerprint, observed_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(provider_id, credential_id) DO UPDATE SET
                       code = excluded.code,
                        observed_at_ms = MAX(credential_entitlement_fences.observed_at_ms, excluded.observed_at_ms),
                       listing_fingerprint = COALESCE(
                         credential_entitlement_fences.listing_fingerprint,
                         excluded.listing_fingerprint)",
                    params![
                        refusal.provider_id,
                        refusal.credential_id,
                        refusal.code,
                        listing_fingerprint,
                        observed_at_ms
                    ],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    /// Every stored credential entitlement fence.
    pub fn credential_entitlement_fences(
        &self,
    ) -> RouteStateResult<Vec<prefrontal_routing_core::route_state_store::CredentialEntitlementFence>>
    {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT provider_id, credential_id, code, listing_fingerprint, observed_at_ms
                     FROM credential_entitlement_fences ORDER BY provider_id, credential_id",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok(
                        prefrontal_routing_core::route_state_store::CredentialEntitlementFence {
                            provider_id: row.get(0)?,
                            credential_id: row.get(1)?,
                            code: row.get(2)?,
                            listing_fingerprint: row.get(3)?,
                            observed_at_ms: row.get(4)?,
                        },
                    )
                })?;
                rows.collect::<Result<Vec<_>, _>>()
            })
            .map_err(err)
    }

    /// Renew a fence's observation clock without changing its code or listing
    /// fingerprint. A recheck probe consumes its interval this way: admitting
    /// the probe stamps the fence fresh, so no second probe is admitted until
    /// another interval passes or an outcome lifts or renews it.
    pub fn touch_credential_entitlement_fence(
        &self,
        provider_id: &str,
        credential_id: &str,
        observed_at_ms: i64,
    ) -> RouteStateResult<usize> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "UPDATE credential_entitlement_fences SET observed_at_ms = ?3
                      WHERE provider_id = ?1 AND credential_id = ?2",
                    params![provider_id, credential_id, observed_at_ms],
                )
            })
            .map_err(err)
    }

    /// End the fence on one (provider, credential) pair, as a successful run on
    /// that pair proves the credential serves again; returns how many ended.
    pub fn clear_credential_entitlement_fence(
        &self,
        provider_id: &str,
        credential_id: &str,
    ) -> RouteStateResult<usize> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "DELETE FROM credential_entitlement_fences
                      WHERE provider_id = ?1 AND credential_id = ?2",
                    params![provider_id, credential_id],
                )
            })
            .map_err(err)
    }

    /// End every fence naming `credential_id`, on every provider it is fenced
    /// for, as an operator does right after topping the credential up; returns
    /// the fences that were lifted, ordered by provider, so the reply can name
    /// what the lift removed.
    pub fn clear_credential_entitlement_fences_for_credential(
        &self,
        credential_id: &str,
    ) -> RouteStateResult<Vec<prefrontal_routing_core::route_state_store::CredentialEntitlementFence>>
    {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT provider_id, credential_id, code, listing_fingerprint, observed_at_ms
                      FROM credential_entitlement_fences WHERE credential_id = ?1
                      ORDER BY provider_id",
                )?;
                let lifted: Vec<
                    prefrontal_routing_core::route_state_store::CredentialEntitlementFence,
                > = stmt
                    .query_map([credential_id], |row| {
                        Ok(
                            prefrontal_routing_core::route_state_store::CredentialEntitlementFence {
                                provider_id: row.get(0)?,
                                credential_id: row.get(1)?,
                                code: row.get(2)?,
                                listing_fingerprint: row.get(3)?,
                                observed_at_ms: row.get(4)?,
                            },
                        )
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                c.execute(
                    "DELETE FROM credential_entitlement_fences WHERE credential_id = ?1",
                    [credential_id],
                )?;
                Ok(lifted)
            })
            .map_err(err)
    }

    /// End every fence on `credential_id` that was recorded against
    /// `fingerprint` (or against no listing at all); returns how many ended.
    pub fn clear_credential_entitlement_fences(
        &self,
        credential_id: &str,
        fingerprint: Option<&str>,
    ) -> RouteStateResult<usize> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "DELETE FROM credential_entitlement_fences
                     WHERE credential_id = ?1 AND listing_fingerprint IS ?2",
                    params![credential_id, fingerprint],
                )
            })
            .map_err(err)
    }

    /// Record the listing a fence recorded without one was first seen against.
    pub fn adopt_credential_entitlement_fingerprint(
        &self,
        credential_id: &str,
        fingerprint: &str,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "UPDATE credential_entitlement_fences SET listing_fingerprint = ?2
                     WHERE credential_id = ?1 AND listing_fingerprint IS NULL",
                    params![credential_id, fingerprint],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    pub fn set_decision_provider_identity(
        &self,
        decision_id: &str,
        identity: &prefrontal_routing_core::route_state_store::ProviderOutcome,
    ) -> RouteStateResult<()> {
        let json =
            serde_json::to_string(identity).map_err(|e| RouteStateStoreError(e.to_string()))?;
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "UPDATE model_route_decisions SET provider_identity_json=?1 WHERE id=?2",
                    params![json, decision_id],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    pub fn decision_provider_identity(
        &self,
        decision_id: &str,
    ) -> RouteStateResult<Option<prefrontal_routing_core::route_state_store::ProviderOutcome>> {
        let json: Option<String> = self
            .store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT provider_identity_json FROM model_route_decisions WHERE id=?1",
                    [decision_id],
                    |row| row.get(0),
                )
                .optional()
            })
            .map_err(err)?
            .flatten();
        json.map(|raw| serde_json::from_str(&raw).map_err(|e| RouteStateStoreError(e.to_string())))
            .transpose()
    }

    /// Persist the resolved execution identity in the same insert as the audit row.
    /// The generic store trait leaves these values unknown; routing supplies them
    /// only after selection, without repurposing the caller's `input.harness`.
    pub fn record_decision_with_selection(
        &self,
        input: &DecisionInput,
        selected_harness: Option<&str>,
        selected_workload: Option<&str>,
    ) -> RouteStateResult<String> {
        self.record_decision_with_task_ref(
            input,
            selected_harness,
            selected_workload,
            input.task_id.as_deref(),
        )
    }

    /// Record an optional opaque task key alongside the selection, in `task_id`.
    /// The wire's `task_ref` is attribution only; it does not affect selection.
    pub fn record_decision_with_task_ref(
        &self,
        input: &DecisionInput,
        selected_harness: Option<&str>,
        selected_workload: Option<&str>,
        task_ref: Option<&str>,
    ) -> RouteStateResult<String> {
        let id = input
            .decision_id
            .clone()
            .unwrap_or_else(|| format!("dec_{}", short_id()));
        let id_ret = id.clone();
        let created_at = now_ms();
        let rejected_json = bound_rejected_json(&input.rejected_json);
        self.store
            .with_conn(|c: &Connection| {
                let catalog_fingerprint = self.current_catalog_fingerprint(c)?;
                c.execute(
                     "INSERT INTO model_route_decisions (
                         id, decision_kind, task_id, parent_session_id, harness, project_root, send_id,
                         target_agent, requirements_json, selected_route_key, selected_provider_id,
                         selected_model_id, selected_variant, score_json, rejected_json, explanation_json,
                         quota_signal_json, policy_revision, anchor_version, catalog_fingerprint, created_at,
                         selected_harness, selected_workload, consumer, allowed_providers_json,
                         project_id, workdir, allowed_account_class
                       ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28)",
                    params![
                        id,
                        if input.decision_kind.is_empty() { "single" } else { &input.decision_kind },
                        task_ref,
                        input.parent_session_id,
                        input.harness,
                        input.project_root,
                        input.send_id,
                        input.target_agent,
                        input.requirements_json,
                        input.selected_route_key,
                        input.selected_provider_id,
                        input.selected_model_id,
                        input.selected_variant,
                        input.score_json,
                        rejected_json,
                        input.explanation_json,
                        input.quota_signal_json,
                        input.policy_revision,
                        input.anchor_version,
                        catalog_fingerprint,
                        created_at,
                        selected_harness,
                        selected_workload,
                        input.consumer,
                        input.allowed_providers_json,
                        input.project_id,
                        input.workdir,
                        input.allowed_account_class.as_str(),
                    ],
                )?;
                operator_quality::observe_decision(c, &input.score_json, created_at)?;
                Ok(())
            })
            .map_err(err)?;
        Ok(id_ret)
    }

    /// Read the account-class authority frozen when this decision was selected.
    pub fn decision_account_class(
        &self,
        decision_id: &str,
    ) -> RouteStateResult<Option<prefrontal_routing_core::accounts::AllowedAccountClass>> {
        let value: Option<String> = self
            .store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT allowed_account_class FROM model_route_decisions WHERE id=?1",
                    [decision_id],
                    |row| row.get(0),
                )
                .optional()
            })
            .map_err(err)?;
        value
            .map(|value| {
                prefrontal_routing_core::accounts::AllowedAccountClass::parse(&value).map_err(err)
            })
            .transpose()
    }

    /// Look up one decision by its durable id for a diagnostic explanation.
    pub fn get_decision_by_id(
        &self,
        decision_id: &str,
    ) -> RouteStateResult<Option<DecisionExplainRow>> {
        self.store
            .with_conn(|connection: &Connection| {
                connection
                    .query_row(
                        "SELECT id, decision_kind, task_id, parent_session_id, harness, project_root,
                                send_id, target_agent, requirements_json, selected_route_key,
                                selected_provider_id, selected_model_id, selected_variant, score_json,
                                rejected_json, explanation_json, quota_signal_json, policy_revision,
                                anchor_version, catalog_fingerprint, created_at, outcome, outcome_at,
                                outcome_error, selected_harness, selected_workload, allowed_providers_json,
                                project_id, workdir
                         FROM model_route_decisions
                         WHERE id=?1",
                        [decision_id],
                        decision_explain_row,
                    )
                    .optional()
            })
            .map_err(err)
    }

    /// Look up the most recent decision correlated to one launch attempt.
    pub fn get_decision_by_send_id(
        &self,
        send_id: &str,
    ) -> RouteStateResult<Option<DecisionExplainRow>> {
        self.store
            .with_conn(|connection: &Connection| {
                connection
                    .query_row(
                        "SELECT id, decision_kind, task_id, parent_session_id, harness, project_root,
                                send_id, target_agent, requirements_json, selected_route_key,
                                selected_provider_id, selected_model_id, selected_variant, score_json,
                                rejected_json, explanation_json, quota_signal_json, policy_revision,
                                anchor_version, catalog_fingerprint, created_at, outcome, outcome_at,
                                outcome_error, selected_harness, selected_workload, allowed_providers_json,
                                project_id, workdir
                         FROM model_route_decisions
                         WHERE send_id=?1
                         ORDER BY created_at DESC, id DESC
                         LIMIT 1",
                        [send_id],
                        decision_explain_row,
                    )
                    .optional()
            })
            .map_err(err)
    }

    /// List recent decisions without returning their full audit payloads.
    pub fn list_decisions(
        &self,
        filter: &DecisionListFilter,
    ) -> RouteStateResult<Vec<DecisionListRow>> {
        let limit = filter.limit.min(100) as i64;
        self.store
            .with_conn(|connection: &Connection| {
                // Observability queries that pulled send-payload-scale JSON caused measured disk
                // churn. Keep this list to compact scalars and derive rejection counts in SQLite;
                // requirements, scores, explanations, and quota snapshots stay on explain only.
                let mut statement = connection.prepare(
                    "WITH filtered AS (
                         SELECT id, created_at, selected_route_key, decision_kind, rejected_json
                         FROM model_route_decisions
                         WHERE (?1 IS NULL OR send_id=?1)
                           AND (?2 IS NULL OR target_agent=?2)
                           AND (?3 IS NULL OR selected_route_key=?3)
                           AND (?4 IS NULL OR created_at>=?4)
                           AND (?5 IS NULL OR created_at<=?5)
                         ORDER BY created_at DESC, id DESC
                         LIMIT ?6
                     ), rejection_counts AS (
                         SELECT filtered.id,
                                json_extract(
                                    CASE WHEN rejection.type='object' THEN rejection.value ELSE '{}' END,
                                    '$.reason'
                                ) AS reason,
                                COUNT(*) AS count
                         FROM filtered
                         JOIN json_each(
                             CASE WHEN json_valid(filtered.rejected_json)
                                  THEN filtered.rejected_json ELSE '[]' END
                         ) AS rejection
                         WHERE json_type(
                             CASE WHEN rejection.type='object' THEN rejection.value ELSE '{}' END,
                             '$.reason'
                         )='text'
                         GROUP BY filtered.id, reason
                     ), top_rejections AS (
                         SELECT id, reason, count,
                                ROW_NUMBER() OVER (
                                    PARTITION BY id ORDER BY count DESC, reason ASC
                                ) AS rank
                         FROM rejection_counts
                     )
                     SELECT filtered.id, filtered.created_at, filtered.selected_route_key,
                            filtered.decision_kind, top_rejections.reason,
                            COALESCE(top_rejections.count, 0)
                     FROM filtered
                     LEFT JOIN top_rejections
                       ON top_rejections.id=filtered.id AND top_rejections.rank=1
                     ORDER BY filtered.created_at DESC, filtered.id DESC",
                )?;
                let rows = statement.query_map(
                    params![
                        filter.send_id.as_deref(),
                        filter.target_agent.as_deref(),
                        filter.route_key.as_deref(),
                        filter.since_ms,
                        filter.until_ms,
                        limit,
                    ],
                    |row| {
                        let top_rejection_count = row.get::<_, i64>(5)?;
                        Ok(DecisionListRow {
                            id: row.get(0)?,
                            created_at: row.get(1)?,
                            selected_route_key: row.get(2)?,
                            decision_kind: row.get(3)?,
                            top_rejection_reason: row.get(4)?,
                            top_rejection_count: top_rejection_count.max(0) as usize,
                        })
                    },
                )?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    /// Fetch each effective catalog route through its own bounded index seek while
    /// retaining one store call, connection checkout, and prepared statement.
    fn get_observed_speed_aggregates(
        &self,
        observed_route_keys: &BTreeSet<String>,
        now: f64,
    ) -> RouteStateResult<Vec<ObservedSpeedAggregate>> {
        if observed_route_keys.is_empty() {
            return Ok(Vec::new());
        }
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(OBSERVED_SPEED_SAMPLES_SQL)?;
                let mut aggregates = Vec::with_capacity(observed_route_keys.len());
                for route_key in observed_route_keys {
                    let samples = stmt
                        .query_map(
                            params![
                                route_key,
                                (now - OBSERVED_SPEED_WINDOW_MS).floor() as i64,
                                now.floor() as i64,
                                USAGE_MIN_OUTPUT_TOKENS.floor() as i64,
                                USAGE_MIN_DURATION_MS.floor() as i64,
                                OBSERVED_SPEED_SAMPLE_LIMIT,
                            ],
                            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                        )?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    if !samples.is_empty() {
                        aggregates.push(ObservedSpeedAggregate {
                            route_key: route_key.clone(),
                            sample_count: samples.len() as i64,
                            output_tokens: samples.iter().map(|(output, _)| output).sum(),
                            duration_ms: samples.iter().map(|(_, duration)| duration).sum(),
                            speed: observed_speed_from_samples(&samples),
                        });
                    }
                }
                Ok(aggregates)
            })
            .map_err(err)
    }

    fn get_observed_speeds(
        &self,
        observed_route_keys: &BTreeSet<String>,
        now: f64,
    ) -> RouteStateResult<HashMap<String, f64>> {
        let mut speeds = HashMap::new();
        for aggregate in self.get_observed_speed_aggregates(observed_route_keys, now)? {
            // Absent or untrusted aggregates leave the catalog speed untouched.
            if let Some(speed) = aggregate.speed {
                speeds.insert(aggregate.route_key, speed);
            }
        }
        Ok(speeds)
    }
}

/// Trusted observed speed, in output tokens per second, for one route's bounded
/// sample window of `(output_tokens, duration_ms)` rows.
///
/// The window must hold at least `OBSERVED_SPEED_MIN_SAMPLES` rows and
/// `OBSERVED_SPEED_MIN_OUTPUT_TOKENS` output tokens in total; otherwise the
/// signal is untrusted and the result is `None`. The speed is the median of the
/// per-sample speeds rather than total tokens over total duration: a single very
/// long generation (for example one that ran to the output-token limit) would
/// otherwise dominate both sums and set the route's speed on its own.
fn observed_speed_from_samples(samples: &[(i64, i64)]) -> Option<f64> {
    if samples.len() < OBSERVED_SPEED_MIN_SAMPLES {
        return None;
    }
    let output_tokens: i64 = samples.iter().map(|(output, _)| output).sum();
    if (output_tokens as f64) < OBSERVED_SPEED_MIN_OUTPUT_TOKENS {
        return None;
    }
    median_of(
        samples
            .iter()
            .filter(|(_, duration)| *duration > 0)
            .map(|(output, duration)| (*output as f64 / *duration as f64) * 1000.0),
    )
}

/// Median of the measured speeds, used as the stand-in for routes with none.
/// Median rather than mean because one very fast or very slow route should not
/// move where an unmeasured route enters the ranking.
fn median_of(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut sorted: Vec<f64> = values
        .filter(|value| value.is_finite() && *value > 0.0)
        .collect();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    Some(if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    })
}

fn observed_route_key_for_fact(fact: &ModelFact) -> String {
    let observed_variant = fact
        .prompt_defaults
        .as_ref()
        .and_then(|defaults| defaults.variant.as_deref())
        .or(fact.variant.as_deref());
    prefrontal_routing_core::make_route_key(&fact.provider_id, &fact.model_id, observed_variant)
}

fn decision_explain_row(row: &Row<'_>) -> rusqlite::Result<DecisionExplainRow> {
    Ok(DecisionExplainRow {
        allowed_providers_json: row.get(26)?,
        id: row.get(0)?,
        decision_kind: row.get(1)?,
        task_id: row.get(2)?,
        parent_session_id: row.get(3)?,
        harness: row.get(4)?,
        project_root: row.get(5)?,
        send_id: row.get(6)?,
        target_agent: row.get(7)?,
        requirements_json: row.get(8)?,
        selected_route_key: row.get(9)?,
        selected_provider_id: row.get(10)?,
        selected_model_id: row.get(11)?,
        selected_variant: row.get(12)?,
        score_json: row.get(13)?,
        rejected_json: row.get(14)?,
        explanation_json: row.get(15)?,
        quota_signal_json: row.get(16)?,
        policy_revision: row.get(17)?,
        anchor_version: row.get(18)?,
        catalog_fingerprint: row.get(19)?,
        created_at: row.get(20)?,
        outcome: row.get(21)?,
        outcome_at: row.get(22)?,
        outcome_error: row.get(23)?,
        selected_harness: row.get(24)?,
        selected_workload: row.get(25)?,
        project_id: row.get(27)?,
        workdir: row.get(28)?,
    })
}

fn row_to_fact(row: &Row<'_>) -> rusqlite::Result<ModelFact> {
    let auth_methods_index = row.as_ref().column_index("auth_methods_json")?;
    let auth_methods = row
        .get::<_, Option<String>>("auth_methods_json")?
        .map(|json| {
            serde_json::from_str(&json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    auth_methods_index,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        })
        .transpose()?;
    let scores_json: String = row.get("scores_json")?;
    let scores = stored_scores(&scores_json);
    let prompt_defaults_json: Option<String> = row.get("prompt_defaults_json")?;
    let prompt_defaults: Option<ModelPromptDefaults> =
        prompt_defaults_json.and_then(|s| serde_json::from_str(&s).ok());
    let confidence = match row.get::<_, String>("confidence")?.as_str() {
        "unknown" => Confidence::Unknown,
        _ => Confidence::Curated,
    };
    let source = FactSource::parse(&row.get::<_, String>("source")?);
    Ok(ModelFact {
        billing_plane: None,
        auth_methods,
        // Read-time provenance, never stored: a fact loaded from the row has whatever
        // speed the row holds, and the median stand-in is applied above this.
        speed_is_fleet_median: false,
        provider_id: row.get("provider_id")?,
        model_id: row.get("model_id")?,
        variant: row.get::<_, Option<String>>("variant")?,
        family: row.get::<_, Option<String>>("family")?,
        scores,
        context_window: row.get::<_, Option<f64>>("context_window")?,
        max_output_tokens: row.get::<_, Option<u64>>("max_output_tokens")?,
        cost_tier: row.get::<_, Option<f64>>("cost_tier")?,
        cost_per_m_tok_out: row.get::<_, Option<f64>>("cost_per_mtok_out")?,
        input_rate: row.get::<_, Option<f64>>("input_rate")?,
        cache_read_rate: row.get::<_, Option<f64>>("cache_read_rate")?,
        cache_write_rate: row.get::<_, Option<f64>>("cache_write_rate")?,
        reasoning_rate: row.get::<_, Option<f64>>("reasoning_rate")?,
        supports_vision: row
            .get::<_, Option<i64>>("supports_vision")?
            .map(|v| v != 0),
        supports_tool_use: row
            .get::<_, Option<i64>>("supports_tool_use")?
            .map(|v| v != 0),
        prompt_defaults,
        confidence,
        notes: row.get::<_, Option<String>>("notes")?,
        speed_multiplier: row.get::<_, Option<f64>>("speed_multiplier")?,
        subscription_multiplier: None,
        score_provenance: source.provenance(),
        aa_index: row.get::<_, Option<f64>>("aa_index")?,
        review_by_ms: row.get::<_, Option<i64>>("review_by_ms")?,
        eq_source: row.get::<_, Option<String>>("eq_source")?,
        class_vector: None,
        class_vector_is_overlay: false,
        upstream_status: row.get::<_, Option<String>>("upstream_status")?,
    })
}

fn row_wall_class(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<RouteWallClass>> {
    row.get::<_, Option<String>>(index)?
        .map(|value| {
            RouteWallClass::parse(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
                )
            })
        })
        .transpose()
}

fn bool_to_int(v: Option<bool>) -> Option<i64> {
    v.map(|b| if b { 1 } else { 0 })
}

fn floored_i64(value: f64) -> Option<i64> {
    let floored = value.floor();
    (value.is_finite() && floored >= i64::MIN as f64 && floored < i64::MAX as f64)
        .then_some(floored as i64)
}

fn stored_token_count(value: f64) -> Option<i64> {
    (value >= 0.0).then(|| floored_i64(value)).flatten()
}

fn stored_output_tokens(value: f64) -> Option<i64> {
    stored_token_count(value).filter(|tokens| *tokens > 0)
}

fn stored_workload_class(value: Option<&str>) -> Option<&str> {
    value.filter(|class| matches!(*class, "ephemeral" | "head" | "gather" | "hire"))
}

impl SqliteRouteStateStore {
    /// The whole-population mean behind `get_class_vector`; the cache above is
    /// the only caller.
    fn compute_class_vector(
        &self,
        workload_class: &str,
        now: f64,
    ) -> RouteStateResult<Option<ClassVector>> {
        let window_start = (now - CLASS_VECTOR_TRAILING_WINDOW_MS).floor() as i64;
        let window_end = now.floor() as i64;
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(WORKLOAD_CLASS_USAGE_SAMPLES_SQL)?;
                let rows =
                    stmt.query_map(params![workload_class, window_start, window_end], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                        ))
                    })?;

                let mut sample_count = 0_u64;
                let mut sum_fresh_input = 0_f64;
                let mut sum_output_excluding_reasoning = 0_f64;
                let mut sum_reasoning = 0_f64;
                let mut sum_cached_read = 0_f64;
                let mut sum_cache_write = 0_f64;

                for row in rows {
                    let (
                        input_tokens,
                        output_tokens,
                        reasoning_tokens,
                        cached_read_tokens,
                        cache_write_tokens,
                    ) = row?;
                    sample_count += 1;
                    sum_fresh_input += input_tokens as f64;
                    let output_excl = (output_tokens - reasoning_tokens).max(0) as f64;
                    sum_output_excluding_reasoning += output_excl;
                    sum_reasoning += reasoning_tokens as f64;
                    sum_cached_read += cached_read_tokens as f64;
                    sum_cache_write += cache_write_tokens as f64;
                }

                if sample_count == 0 {
                    return Ok(None);
                }

                let count = sample_count as f64;
                Ok(Some(ClassVector {
                    workload_class: workload_class.to_string(),
                    fresh_input: sum_fresh_input / count,
                    output_excluding_reasoning: sum_output_excluding_reasoning / count,
                    reasoning: sum_reasoning / count,
                    cached_read: sum_cached_read / count,
                    cache_write: sum_cache_write / count,
                    sample_count,
                    trailing_window_days: CLASS_VECTOR_TRAILING_WINDOW_DAYS,
                    statistic: "arithmetic_mean".to_string(),
                    is_overlay: true,
                }))
            })
            .map_err(err)
    }
}

impl SqliteRouteStateStore {
    /// Copy task and workload attribution from the decision, never from the report.
    pub fn record_usage_delivery_for_decision(
        &self,
        obs: &UsageObservation,
        decision_id: &str,
    ) -> RouteStateResult<UsageRecordResult> {
        self.record_usage_delivery_with_decision(obs, Some(decision_id))
    }

    fn record_usage_delivery_with_decision(
        &self,
        obs: &UsageObservation,
        decision_id: Option<&str>,
    ) -> RouteStateResult<UsageRecordResult> {
        if obs.id.trim().is_empty()
            || obs.route_key.trim().is_empty()
            || obs.capacity_key.trim().is_empty()
            || obs.provider_id.trim().is_empty()
        {
            return Ok(UsageRecordResult::default());
        }
        let Some(observed_at) = floored_i64(obs.observed_at) else {
            return Ok(UsageRecordResult::default());
        };

        let input_tokens = obs.input_tokens.and_then(stored_token_count);
        let output_tokens = stored_output_tokens(obs.output_tokens);
        let reasoning_tokens = obs.reasoning_tokens.and_then(stored_token_count);
        let cached_read_tokens = obs.cached_read_tokens.and_then(stored_token_count);
        let cache_write_tokens = obs.cache_write_tokens.and_then(stored_token_count);
        let duration_ms = (obs.duration_ms >= 0.0)
            .then(|| floored_i64(obs.duration_ms))
            .flatten();
        let workload_class = stored_workload_class(obs.workload_class.as_deref());

        self.store
            .with_conn(|c: &Connection| {
                let (task_id, decision_workload): (Option<String>, Option<String>) =
                    match decision_id {
                        Some(id) => c.query_row(
                            "SELECT task_id, COALESCE(consumer, selected_workload, target_agent)
                             FROM model_route_decisions WHERE id = ?1",
                            params![id],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )?,
                        None => (None, None),
                    };
                let task_workload = task_id.as_ref().and(decision_workload);
                let held_speed_observation = c
                    .query_row(
                        "SELECT COALESCE(output_tokens >= ?2 AND duration_ms >= ?3, FALSE)
                         FROM model_route_usage
                         WHERE id = ?1",
                        params![
                            obs.id,
                            USAGE_MIN_OUTPUT_TOKENS.floor() as i64,
                            USAGE_MIN_DURATION_MS.floor() as i64,
                        ],
                        |row| row.get::<_, bool>(0),
                    )
                    .optional()?;

                let stored_speed_values = c
                    .query_row(
                        "INSERT INTO model_route_usage
                           (id, task_id, route_key, capacity_key, provider_id,
                            input_tokens, output_tokens, duration_ms, observed_at,
                            reasoning_tokens, cached_read_tokens, cache_write_tokens,
                            workload_class, account_key, task_workload)
                         VALUES (?1, ?14, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                                 ?9, ?10, ?11, ?12, ?13, ?15)
                         ON CONFLICT(id) DO UPDATE SET
                           task_id = COALESCE(model_route_usage.task_id, excluded.task_id),
                           input_tokens = COALESCE(model_route_usage.input_tokens,
                                                   excluded.input_tokens),
                           output_tokens = COALESCE(model_route_usage.output_tokens,
                                                    excluded.output_tokens),
                           duration_ms = COALESCE(model_route_usage.duration_ms,
                                                 excluded.duration_ms),
                           reasoning_tokens = COALESCE(model_route_usage.reasoning_tokens,
                                                       excluded.reasoning_tokens),
                           cached_read_tokens = COALESCE(model_route_usage.cached_read_tokens,
                                                         excluded.cached_read_tokens),
                           cache_write_tokens = COALESCE(model_route_usage.cache_write_tokens,
                                                         excluded.cache_write_tokens),
                           workload_class = COALESCE(model_route_usage.workload_class,
                                                     excluded.workload_class),
                           task_workload = CASE
                             WHEN model_route_usage.task_id IS NULL OR model_route_usage.task_id = excluded.task_id
                               THEN COALESCE(model_route_usage.task_workload, excluded.task_workload)
                             ELSE model_route_usage.task_workload
                           END,
                            account_key = COALESCE(model_route_usage.account_key,
                                                   excluded.account_key)
                         WHERE (model_route_usage.input_tokens IS NULL AND excluded.input_tokens IS NOT NULL)
                            OR (model_route_usage.output_tokens IS NULL AND excluded.output_tokens IS NOT NULL)
                            OR (model_route_usage.duration_ms IS NULL AND excluded.duration_ms IS NOT NULL)
                            OR (model_route_usage.reasoning_tokens IS NULL AND excluded.reasoning_tokens IS NOT NULL)
                            OR (model_route_usage.cached_read_tokens IS NULL AND excluded.cached_read_tokens IS NOT NULL)
                            OR (model_route_usage.cache_write_tokens IS NULL AND excluded.cache_write_tokens IS NOT NULL)
                            OR (model_route_usage.workload_class IS NULL AND excluded.workload_class IS NOT NULL)
                             OR (model_route_usage.account_key IS NULL AND excluded.account_key IS NOT NULL)
                              OR (model_route_usage.task_id IS NULL AND excluded.task_id IS NOT NULL)
                              OR (model_route_usage.task_workload IS NULL AND excluded.task_workload IS NOT NULL
                                  AND (model_route_usage.task_id IS NULL OR model_route_usage.task_id = excluded.task_id))
                         RETURNING output_tokens, duration_ms",
                        params![
                            obs.id,
                            obs.route_key,
                            obs.capacity_key,
                            obs.provider_id,
                            input_tokens,
                            output_tokens,
                            duration_ms,
                            observed_at,
                            reasoning_tokens,
                            cached_read_tokens,
                            cache_write_tokens,
                            workload_class,
                            obs.account_key.as_deref(),
                            task_id,
                            task_workload,
                        ],
                        |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?)),
                    )
                    .optional()?;

                let holds_speed_observation =
                    stored_speed_values.is_some_and(|(output, duration)| {
                        output
                            .is_some_and(|tokens| tokens >= USAGE_MIN_OUTPUT_TOKENS.floor() as i64)
                            && duration.is_some_and(|ms| ms >= USAGE_MIN_DURATION_MS.floor() as i64)
                    });
                Ok((UsageRecordResult {
                    recorded: !held_speed_observation.unwrap_or(false) && holds_speed_observation,
                    usage_recorded: held_speed_observation.is_none(),
                }, stored_speed_values.is_some()))
            })
            .map_err(err)
            .map(|(result, changed)| {
                // Retries that supply no new measurements must not invalidate the
                // seven-day population mean. Only inserts/enrichment can move it.
                if changed {
                    self.usage_generation.fetch_add(1, Ordering::AcqRel);
                }
                result
            })
    }
}

impl RouteStateStore for SqliteRouteStateStore {
    fn upsert_fact(&self, fact: &ModelFact, source: FactSource) -> RouteStateResult<()> {
        let mut fact = fact.clone();
        if let Some(family) = fact.family.as_deref() {
            let normalized = normalize_grand_family_id(family).ok_or_else(|| {
                err(format!(
                    "unknown model family spelling for {}/{}: {family}",
                    fact.provider_id, fact.model_id
                ))
            })?;
            fact.family = Some(normalized.as_str().to_string());
        }
        let fact = &fact;
        let route = fact_to_route(fact);
        derive_provider_key(&fact.provider_id)
            .ok_or_else(|| err("model fact has no canonical provider key"))?;
        let scores_json = serde_json::to_string(&fact.scores).map_err(err)?;
        let prompt_defaults_json = fact
            .prompt_defaults
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(err)?;
        let (aa_index, review_by_ms, eq_source) = if source == FactSource::AaInferred {
            (fact.aa_index, fact.review_by_ms, fact.eq_source.clone())
        } else {
            (None, None, None)
        };
        self.store
            .with_conn(|c: &Connection| {
                // Fact replacement and route-state insertion share one transaction.
                // The store connection mutex serializes this with provider fanout,
                // so a new route resolves either before fanout or after inheritance.
                let tx = c.unchecked_transaction()?;
                if source == FactSource::AaInferred {
                    let existing: Option<String> = tx
                        .query_row(
                            "SELECT source FROM model_facts WHERE route_key = ?1",
                            [&route.route_key],
                            |row| row.get(0),
                        )
                        .optional()?;
                    if let Some(existing) = existing {
                        if FactSource::parse(&existing).blocks_aa_inferred() {
                            return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
                                std::io::Error::other(
                                    "aa_inferred does not overwrite curated or override rows",
                                ),
                            )));
                        }
                    }
                }
                 // Login restrictions, the canonical model identity and the
                 // upstream status belong to the served model, not its route
                 // variant. When upsert_fact places a new variant's scores, it
                 // inherits all three; updating scores cannot erase any of them.
                 tx.execute(
                     "INSERT INTO model_facts (
                       route_key, capacity_key, provider_id, model_id, variant, family, family_source,
                       scores_json, context_window, max_output_tokens, cost_tier, cost_per_mtok_out,
                         input_rate, cache_read_rate, cache_write_rate, reasoning_rate,
                         supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, speed_multiplier,
                          source, updated_at, aa_index, review_by_ms, eq_source, auth_methods_json,
                          canonical_id, upstream_status
                         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,
                           (SELECT auth_methods_json FROM model_facts
                            WHERE provider_id = ?3 AND model_id = ?4 ORDER BY route_key LIMIT 1),
                           (SELECT canonical_id FROM model_facts
                            WHERE provider_id = ?3 AND model_id = ?4 ORDER BY route_key LIMIT 1),
                           (SELECT upstream_status FROM model_facts
                            WHERE provider_id = ?3 AND model_id = ?4 ORDER BY route_key LIMIT 1))
                      ON CONFLICT(route_key) DO UPDATE SET
                       capacity_key=excluded.capacity_key, provider_id=excluded.provider_id,
                       model_id=excluded.model_id, variant=excluded.variant, family=excluded.family,
                       family_source=CASE
                         WHEN model_facts.family_source IN ('override', 'rule') THEN model_facts.family_source
                         ELSE excluded.family_source
                       END,
                       scores_json=excluded.scores_json, context_window=excluded.context_window,
                       max_output_tokens=excluded.max_output_tokens, cost_tier=excluded.cost_tier,
                       cost_per_mtok_out=excluded.cost_per_mtok_out,
                       input_rate=excluded.input_rate, cache_read_rate=excluded.cache_read_rate,
                       cache_write_rate=excluded.cache_write_rate,
                       reasoning_rate=excluded.reasoning_rate,
                       supports_vision=excluded.supports_vision, supports_tool_use=excluded.supports_tool_use,
                       prompt_defaults_json=excluded.prompt_defaults_json, confidence=excluded.confidence,
                        notes=excluded.notes, speed_multiplier=excluded.speed_multiplier,
                        source=excluded.source, updated_at=excluded.updated_at,
                        aa_index=excluded.aa_index, review_by_ms=excluded.review_by_ms,
                        eq_source=excluded.eq_source",
                    params![
                        route.route_key,
                        route.capacity_key,
                        fact.provider_id,
                        fact.model_id,
                        fact.variant,
                         fact.family,
                         if fact.family.is_some() {
                             FamilySource::Curated.as_str()
                         } else {
                             FamilySource::Absent.as_str()
                         },
                         scores_json,
                         fact.context_window,
                        fact.max_output_tokens,
                        fact.cost_tier,
                        fact.cost_per_m_tok_out,
                        fact.input_rate,
                        fact.cache_read_rate,
                        fact.cache_write_rate,
                        fact.reasoning_rate,
                        bool_to_int(fact.supports_vision),
                        bool_to_int(fact.supports_tool_use),
                        prompt_defaults_json,
                        confidence_str(fact.confidence),
                        fact.notes,
                        fact.speed_multiplier,
                        source.as_str(),
                        now_ms(),
                        aa_index,
                        review_by_ms,
                        eq_source,
                    ],
                )?;
                // A fresh INSERT fires the model_facts insert trigger, which creates
                // the route's state row with any live provider wall and its cause.
                // An update leaves the existing state row alone.
                tx.commit()
            })
            .map_err(err)
    }

    fn mark_operator_fact_fields(
        &self,
        route_key: &str,
        fields: &[CatalogFactField],
    ) -> RouteStateResult<()> {
        let marked = |field| {
            if fields.contains(&field) {
                Some("override")
            } else {
                None
            }
        };
        self.store
            .with_conn(|connection: &Connection| {
                connection.execute(
                    "UPDATE model_facts SET
                       context_window_source = COALESCE(?2, context_window_source),
                       max_output_tokens_source = COALESCE(?3, max_output_tokens_source),
                       cost_tier_source = COALESCE(?4, cost_tier_source),
                       cost_per_mtok_out_source = COALESCE(?5, cost_per_mtok_out_source),
                       supports_vision_source = COALESCE(?6, supports_vision_source),
                       supports_tool_use_source = COALESCE(?7, supports_tool_use_source)
                     WHERE route_key = ?1",
                    params![
                        route_key,
                        marked(CatalogFactField::ContextWindow),
                        marked(CatalogFactField::MaxOutputTokens),
                        marked(CatalogFactField::CostTier),
                        marked(CatalogFactField::CostPerMtokOut),
                        marked(CatalogFactField::SupportsVision),
                        marked(CatalogFactField::SupportsToolUse),
                    ],
                )?;
                Ok(())
            })
            .map_err(err)
    }

    fn clear_operator_fact_fields(
        &self,
        route_key: &str,
        fields: &[CatalogFactField],
    ) -> RouteStateResult<Vec<CatalogFactField>> {
        if fields.is_empty() {
            return Ok(Vec::new());
        }
        self.store
            .with_conn(|connection: &Connection| {
                let tx = connection.unchecked_transaction()?;
                let sources = tx
                    .query_row(
                        "SELECT context_window_source, max_output_tokens_source,
                                cost_tier_source, cost_per_mtok_out_source,
                                supports_vision_source, supports_tool_use_source
                         FROM model_facts WHERE route_key = ?1",
                        [route_key],
                        |row| {
                            Ok((
                                row.get::<_, Option<String>>(0)?,
                                row.get::<_, Option<String>>(1)?,
                                row.get::<_, Option<String>>(2)?,
                                row.get::<_, Option<String>>(3)?,
                                row.get::<_, Option<String>>(4)?,
                                row.get::<_, Option<String>>(5)?,
                            ))
                        },
                    )
                    .optional()?;
                let Some(sources) = sources else {
                    tx.commit()?;
                    return Ok(Vec::new());
                };
                let candidates = [
                    (CatalogFactField::ContextWindow, sources.0),
                    (CatalogFactField::MaxOutputTokens, sources.1),
                    (CatalogFactField::CostTier, sources.2),
                    (CatalogFactField::CostPerMtokOut, sources.3),
                    (CatalogFactField::SupportsVision, sources.4),
                    (CatalogFactField::SupportsToolUse, sources.5),
                ];
                let cleared = candidates
                    .into_iter()
                    .filter_map(|(field, source)| {
                        (fields.contains(&field) && source.as_deref() == Some("override"))
                            .then_some(field)
                    })
                    .collect::<Vec<_>>();
                let should_clear = |field| i64::from(cleared.contains(&field));
                tx.execute(
                    "UPDATE model_facts SET
                       context_window_source = CASE
                         WHEN ?2 = 1 AND context_window_source = 'override' THEN NULL
                         ELSE context_window_source END,
                       max_output_tokens_source = CASE
                         WHEN ?3 = 1 AND max_output_tokens_source = 'override' THEN NULL
                         ELSE max_output_tokens_source END,
                       cost_tier_source = CASE
                         WHEN ?4 = 1 AND cost_tier_source = 'override' THEN NULL
                         ELSE cost_tier_source END,
                       cost_per_mtok_out_source = CASE
                         WHEN ?5 = 1 AND cost_per_mtok_out_source = 'override' THEN NULL
                         ELSE cost_per_mtok_out_source END,
                       supports_vision_source = CASE
                         WHEN ?6 = 1 AND supports_vision_source = 'override' THEN NULL
                         ELSE supports_vision_source END,
                       supports_tool_use_source = CASE
                         WHEN ?7 = 1 AND supports_tool_use_source = 'override' THEN NULL
                         ELSE supports_tool_use_source END
                     WHERE route_key = ?1",
                    params![
                        route_key,
                        should_clear(CatalogFactField::ContextWindow),
                        should_clear(CatalogFactField::MaxOutputTokens),
                        should_clear(CatalogFactField::CostTier),
                        should_clear(CatalogFactField::CostPerMtokOut),
                        should_clear(CatalogFactField::SupportsVision),
                        should_clear(CatalogFactField::SupportsToolUse),
                    ],
                )?;
                tx.commit()?;
                Ok(cleared)
            })
            .map_err(err)
    }

    fn set_model_family(
        &self,
        route_key: &str,
        family: Option<GrandFamily>,
        source: FamilySource,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|connection: &Connection| {
                connection.execute(
                    "UPDATE model_facts SET family = ?2, family_source = ?3, updated_at = ?4
                     WHERE route_key = ?1",
                    params![
                        route_key,
                        family.map(GrandFamily::as_str),
                        source.as_str(),
                        now_ms()
                    ],
                )?;
                Ok(())
            })
            .map_err(err)
    }

    fn get_model_family(
        &self,
        route_key: &str,
    ) -> RouteStateResult<Option<(Option<GrandFamily>, FamilySource)>> {
        self.store
            .with_conn(|connection: &Connection| {
                connection
                    .query_row(
                        "SELECT family, family_source FROM model_facts WHERE route_key = ?1",
                        [route_key],
                        |row| {
                            let family = row
                                .get::<_, Option<String>>(0)?
                                .as_deref()
                                .and_then(GrandFamily::from_wire);
                            let source_raw = row.get::<_, String>(1)?;
                            let source = FamilySource::from_wire(&source_raw)
                                .unwrap_or(FamilySource::Absent);
                            Ok((family, source))
                        },
                    )
                    .optional()
            })
            .map_err(err)
    }

    fn get_facts(&self) -> RouteStateResult<Vec<ModelFact>> {
        // Routes of one model share its operator score (keyed on Fusiform's
        // canonical id). `get_fact_by_route_key` stays the raw row, so a write
        // that merges over it never persists a borrowed score as the route's own.
        Ok(self
            .read_resolved_facts()?
            .into_iter()
            .filter_map(|(fact, present)| present.then_some(fact))
            .collect())
    }

    fn get_fact_by_route_key(&self, route_key: &str) -> RouteStateResult<Option<ModelFact>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare("SELECT * FROM model_facts WHERE route_key = ?1")?;
                let mut rows = stmt.query_map([route_key], row_to_fact)?;
                match rows.next() {
                    Some(r) => Ok(Some(r?)),
                    None => Ok(None),
                }
            })
            .map_err(err)
    }

    fn list_override_route_keys(&self) -> RouteStateResult<Vec<String>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt =
                    c.prepare("SELECT route_key FROM model_facts WHERE source = 'override'")?;
                let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    fn delete_fact(&self, route_key: &str) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = c.unchecked_transaction()?;
                tx.execute("DELETE FROM model_facts WHERE route_key = ?1", [route_key])?;
                tx.commit()
            })
            .map_err(err)
    }

    /// Withdraw an override without deleting the row: the fact stays in the
    /// catalog under source 'seed' so migration-seeded models survive their
    /// config override's removal. Rows leave the catalog by migration only.
    fn demote_fact_source_to_seed(&self, route_key: &str) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = c.unchecked_transaction()?;
                tx.execute(
                    "UPDATE model_facts SET source = 'seed' WHERE route_key = ?1",
                    [route_key],
                )?;
                tx.commit()
            })
            .map_err(err)
    }

    fn get_capacity_keys_for_provider(&self, provider_id: &str) -> RouteStateResult<Vec<String>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT DISTINCT capacity_key FROM model_facts WHERE provider_id = ?1",
                )?;
                let rows = stmt.query_map([provider_id], |r| r.get::<_, String>(0))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    fn get_provider_cooldown(
        &self,
        provider_key: &str,
    ) -> RouteStateResult<Option<ProviderCooldownState>> {
        let provider_key = derive_provider_key(provider_key)
            .ok_or_else(|| err("provider cooldown lookup has no canonical provider key"))?
            .to_owned();
        self.store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT provider_key, cooldown_until, quota_exhausted_until, quota_pressure,
                            wall_class, last_error
                     FROM provider_cooldown_state WHERE provider_key = ?1",
                    [&provider_key],
                    |row| {
                        let state = ProviderCooldownState {
                            provider_key: row.get(0)?,
                            cooldown_until: row.get(1)?,
                            quota_exhausted_until: row.get(2)?,
                            quota_pressure: row.get(3)?,
                            wall_class: row_wall_class(row, 4)?,
                        };
                        let last_error: Option<String> = row.get(5)?;
                        Ok((state, last_error))
                    },
                )
                .optional()
            })
            .map_err(err)
            .map(|row| {
                // A provider row written from one credential's refusal (before
                // such refusals were kept off the provider) is read as absent: it
                // is evidence about that credential, which its fence covers.
                row.filter(|(_, last_error)| !credential_refusal_wall(last_error.as_deref()))
                    .map(|(state, _)| state)
            })
    }

    fn record_provider_cooldown(
        &self,
        provider_key: &str,
        route_key: &str,
        capacity_key: &str,
        update: &RouteStateUpdate,
    ) -> RouteStateResult<()> {
        // Old callers can still send an account failure as a route cooldown.
        // Keep account evidence in the account allocator, never fan it out to
        // every model or replace an independent model wall's cause.
        if prefrontal_routing_core::account_wall::account_scoped_wall(
            update.last_error.as_deref(),
            update.wall_class,
        ) {
            return Ok(());
        }
        let provider_key = derive_provider_key(provider_key)
            .ok_or_else(|| err("provider cooldown write has no canonical provider key"))?
            .to_owned();
        let route_provider = derive_provider_key(route_key)
            .ok_or_else(|| err("provider cooldown route has no canonical provider key"))?
            .to_owned();
        if route_provider != provider_key
            || derive_provider_key(capacity_key) != Some(provider_key.as_str())
        {
            return Err(err("provider cooldown route/provider keys do not match"));
        }
        self.store
            .with_conn(|c: &Connection| {
                // The store connection mutex serializes this transaction with
                // upsert_fact and every other route-state writer. The provider
                // row, inheritance row, and existing-route fanout therefore
                // commit together or roll back together.
                let tx = c.unchecked_transaction()?;
                let existing = tx
                    .query_row(
                        "SELECT cooldown_until, quota_exhausted_until, quota_pressure,
                                last_error, last_error_at, wall_class
                         FROM provider_cooldown_state WHERE provider_key = ?1",
                        [&provider_key],
                        |row| {
                            Ok(ExistingProviderState {
                                cooldown_until: row.get(0)?,
                                quota_exhausted_until: row.get(1)?,
                                quota_pressure: row.get(2)?,
                                last_error: row.get(3)?,
                                last_error_at: row.get(4)?,
                                wall_class: row_wall_class(row, 5)?,
                            })
                        },
                    )
                    .optional()?;
                // A stored row from one credential's refusal holds no provider
                // wall, so a new provider-wide signal merges with nothing.
                let existing = existing
                    .filter(|state| !credential_refusal_wall(state.last_error.as_deref()))
                    .unwrap_or_default();
                let cooldown_until = max_defined(existing.cooldown_until, update.cooldown_until);
                let quota_exhausted_until =
                    max_defined(existing.quota_exhausted_until, update.quota_exhausted_until);
                let quota_pressure = update.quota_pressure.or(existing.quota_pressure);
                let wall_class = update.wall_class.or(existing.wall_class);
                let (last_error, last_error_at) = match &update.last_error {
                    Some(error) => (Some(error.clone()), Some(now_ms())),
                    None if (update.cooldown_until.is_some()
                        || update.quota_exhausted_until.is_some())
                        && prefrontal_routing_core::account_wall::account_scoped_wall(
                            existing.last_error.as_deref(),
                            existing.wall_class,
                        ) =>
                    {
                        (Some("route wall (scope unknown)".into()), Some(now_ms()))
                    }
                    None => (existing.last_error, existing.last_error_at),
                };
                let updated_at = now_ms();

                tx.execute(
                    "INSERT INTO provider_cooldown_state (
                       provider_key, cooldown_until, quota_exhausted_until, quota_pressure,
                       last_error, last_error_at, updated_at, wall_class
                     ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
                     ON CONFLICT(provider_key) DO UPDATE SET
                       cooldown_until=excluded.cooldown_until,
                       quota_exhausted_until=excluded.quota_exhausted_until,
                       quota_pressure=excluded.quota_pressure,
                       last_error=excluded.last_error,
                       last_error_at=excluded.last_error_at,
                       updated_at=excluded.updated_at,
                       wall_class=excluded.wall_class",
                    params![
                        provider_key,
                        cooldown_until,
                        quota_exhausted_until,
                        quota_pressure,
                        last_error,
                        last_error_at,
                        updated_at,
                        wall_class.map(RouteWallClass::as_str),
                    ],
                )?;

                // A direct cooldown signal can name a route that is not in the
                // catalog yet. Materialize it from the just-merged provider row
                // before the fanout, so it cannot miss this write.
                let (_, model_id, variant) = route_identity(&tx, route_key, None)?;
                tx.execute(
                    "INSERT OR IGNORE INTO model_route_state (
                       route_key, capacity_key, provider_id, model_id, variant,
                       cooldown_until, quota_exhausted_until, quota_pressure,
                       latency_ema_ms, success_ema, failure_count, quota_error_count,
                       last_error, last_error_at, updated_at, wall_class
                     ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,NULL,NULL,0,0,?9,?10,?11,?12)",
                    params![
                        route_key,
                        capacity_key,
                        route_provider,
                        model_id,
                        variant,
                        cooldown_until,
                        quota_exhausted_until,
                        quota_pressure,
                        last_error,
                        last_error_at,
                        updated_at,
                        wall_class.map(RouteWallClass::as_str),
                    ],
                )?;

                // Merge each horizon independently. NULL incoming values preserve
                // route-specific state, and an older provider signal never shortens
                // a later route or provider deadline.
                tx.execute(
                    "UPDATE model_route_state SET
                       cooldown_until = CASE
                         WHEN ?1 IS NULL THEN cooldown_until
                         WHEN cooldown_until IS NULL OR cooldown_until < ?1 THEN ?1
                         ELSE cooldown_until END,
                       quota_exhausted_until = CASE
                         WHEN ?2 IS NULL THEN quota_exhausted_until
                         WHEN quota_exhausted_until IS NULL OR quota_exhausted_until < ?2 THEN ?2
                         ELSE quota_exhausted_until END,
                       quota_pressure = COALESCE(?3, quota_pressure),
                       wall_class = COALESCE(?4, wall_class),
                       failure_count = failure_count + ?5,
                       quota_error_count = quota_error_count + ?6,
                       last_error = COALESCE(?7, last_error),
                       last_error_at = CASE WHEN ?7 IS NULL THEN last_error_at ELSE ?8 END,
                       updated_at = ?8
                     WHERE provider_id = ?9",
                    params![
                        cooldown_until,
                        quota_exhausted_until,
                        update.quota_pressure,
                        update.wall_class.map(RouteWallClass::as_str),
                        if update.increment_failure { 1 } else { 0 },
                        if update.increment_quota_error { 1 } else { 0 },
                        last_error,
                        updated_at,
                        provider_key,
                    ],
                )?;
                tx.commit()
            })
            .map_err(err)
    }

    fn update_route_state(
        &self,
        route_key: &str,
        capacity_key: &str,
        update: &RouteStateUpdate,
    ) -> RouteStateResult<()> {
        if prefrontal_routing_core::account_wall::account_scoped_wall(
            update.last_error.as_deref(),
            update.wall_class,
        ) {
            return Ok(());
        }
        let provider_key = derive_provider_key(route_key)
            .ok_or_else(|| err("route state update has no canonical provider key"))?
            .to_owned();
        self.store
            .with_conn(|c: &Connection| {
                // Read-modify-write under one transaction so counter increments,
                // provider inheritance, and field merges see one consistent row.
                let tx = c.unchecked_transaction()?;
                let (provider_id, model_id, variant) = route_identity(&tx, route_key, None)?;
                let existing = tx
                    .query_row(
                        "SELECT cooldown_until, quota_exhausted_until, quota_pressure,
                                latency_ema_ms, success_ema, failure_count, quota_error_count,
                                last_error, last_error_at, wall_class
                         FROM model_route_state WHERE route_key = ?1",
                        [route_key],
                        |r| {
                            Ok(ExistingState {
                                cooldown_until: r.get(0)?,
                                quota_exhausted_until: r.get(1)?,
                                wall_class: row_wall_class(r, 9)?,
                                quota_pressure: r.get(2)?,
                                latency_ema_ms: r.get(3)?,
                                success_ema: r.get(4)?,
                                failure_count: r.get(5)?,
                                quota_error_count: r.get(6)?,
                                last_error: r.get(7)?,
                                last_error_at: r.get(8)?,
                            })
                        },
                    )
                    .optional()?;
                let mut ex = existing.unwrap_or_default();
                // The route's own wall from one credential's refusal keeps its
                // evidence (last_error) but lends no horizon to this write.
                if credential_refusal_wall(ex.last_error.as_deref()) {
                    ex.cooldown_until = None;
                    ex.quota_exhausted_until = None;
                    ex.wall_class = None;
                }
                let provider = tx
                    .query_row(
                        "SELECT cooldown_until, quota_exhausted_until, quota_pressure,
                                last_error, last_error_at, wall_class
                         FROM provider_cooldown_state WHERE provider_key = ?1",
                        [&provider_key],
                        |r| {
                            Ok(ExistingProviderState {
                                cooldown_until: r.get(0)?,
                                quota_exhausted_until: r.get(1)?,
                                quota_pressure: r.get(2)?,
                                last_error: r.get(3)?,
                                last_error_at: r.get(4)?,
                                wall_class: row_wall_class(r, 5)?,
                            })
                        },
                    )
                    .optional()?
                    .unwrap_or_default();
                // A stored provider row from one credential's refusal is not
                // inherited: that refusal never walled the provider.
                let provider = if credential_refusal_wall(provider.last_error.as_deref()) {
                    ExistingProviderState::default()
                } else {
                    provider
                };

                let cooldown_until = max_defined(
                    max_defined(ex.cooldown_until, provider.cooldown_until),
                    update.cooldown_until,
                );
                let quota_exhausted_until = max_defined(
                    max_defined(ex.quota_exhausted_until, provider.quota_exhausted_until),
                    update.quota_exhausted_until,
                );
                let quota_pressure = update
                    .quota_pressure
                    .or(ex.quota_pressure)
                    .or(provider.quota_pressure);
                let wall_class = update
                    .wall_class
                    .or(ex.wall_class)
                    .or(provider.wall_class);
                let latency_ema_ms = update.latency_ema_ms.or(ex.latency_ema_ms);
                let success_ema = update.success_ema.or(ex.success_ema);
                let failure_count = ex.failure_count + if update.increment_failure { 1 } else { 0 };
                let quota_error_count =
                    ex.quota_error_count + if update.increment_quota_error { 1 } else { 0 };
                let (last_error, last_error_at) = match &update.last_error {
                    Some(error) => (Some(error.clone()), Some(now_ms())),
                    None if (update.cooldown_until.is_some() || update.quota_exhausted_until.is_some())
                        && prefrontal_routing_core::account_wall::account_scoped_wall(ex.last_error.as_deref(), ex.wall_class) => {
                        // A new unscoped wall must not inherit a legacy account
                        // narrative, which would falsely quarantine the new wall.
                        (Some("route wall (scope unknown)".into()), Some(now_ms()))
                    }
                    None => (ex
                        .last_error
                        .clone()
                        .or(provider.last_error.clone()), ex.last_error_at.or(provider.last_error_at)),
                };

                tx.execute(
                    "INSERT INTO model_route_state (
                       route_key, capacity_key, provider_id, model_id, variant,
                       cooldown_until, quota_exhausted_until, quota_pressure,
                       latency_ema_ms, success_ema, failure_count, quota_error_count,
                       last_error, last_error_at, updated_at, wall_class
                     ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)
                     ON CONFLICT(route_key) DO UPDATE SET
                       capacity_key=excluded.capacity_key, provider_id=excluded.provider_id,
                       model_id=excluded.model_id, variant=excluded.variant,
                       cooldown_until=excluded.cooldown_until,
                       quota_exhausted_until=excluded.quota_exhausted_until,
                       quota_pressure=excluded.quota_pressure,
                       latency_ema_ms=excluded.latency_ema_ms, success_ema=excluded.success_ema,
                       failure_count=excluded.failure_count, quota_error_count=excluded.quota_error_count,
                       last_error=excluded.last_error, last_error_at=excluded.last_error_at,
                       updated_at=excluded.updated_at, wall_class=excluded.wall_class",
                    params![
                        route_key,
                        capacity_key,
                        provider_id,
                        model_id,
                        variant,
                        cooldown_until,
                        quota_exhausted_until,
                        quota_pressure,
                        latency_ema_ms,
                        success_ema,
                        failure_count,
                        quota_error_count,
                        last_error,
                        last_error_at,
                        now_ms(),
                        wall_class.map(RouteWallClass::as_str),
                    ],
                )?;
                tx.commit()
            })
            .map_err(err)
    }

    fn record_decision(&self, input: &DecisionInput) -> RouteStateResult<String> {
        self.record_decision_with_selection(input, None, None)
    }

    fn set_decision_outcome(
        &self,
        decision_id: &str,
        update: &DecisionOutcomeUpdate,
    ) -> RouteStateResult<()> {
        // Retain the full error in the audit, but treat an explicitly account-
        // scoped failure as neutral model-health evidence. This closes both the
        // route-global quota writer and the terminal-failure backoff writer. The
        // request-class provider reset below is independent of both.
        // A permanent refusal of one credential is the same kind of evidence: the
        // credential is fenced, and the refusal does not by itself block the
        // route's other credentials.
        let mut normalized = update.clone();
        let last_error = update.last_error.as_deref().unwrap_or("");
        if prefrontal_routing_core::account_wall::account_scoped_error(last_error)
            || prefrontal_routing_core::account_wall::credential_refusal_in(last_error).is_some()
            || update
                .provider_outcome
                .as_ref()
                .is_some_and(|provider| provider.credential_refusal.is_some())
        {
            normalized.outcome = prefrontal_routing_core::RouteOutcome::Interrupted;
        }
        let update = &normalized;
        self.store
            .with_conn(|c: &Connection| {
                let tx = c.unchecked_transaction()?;
                type DecisionRoute = (
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                );
                let decision: Option<DecisionRoute> = tx
                    .query_row(
                        "SELECT selected_route_key, selected_provider_id, selected_model_id, outcome
                         FROM model_route_decisions WHERE id=?1",
                        [decision_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .optional()?;
                // The decision ledger is history a backup may restore empty, and
                // retention deletes its old rows. An outcome for a decision the
                // ledger no longer holds cannot name its route, so it changes
                // nothing; answering an error instead would only make the caller
                // retry an outcome that can never land.
                let Some((route_key, selected_provider_id, selected_model_id, prior_outcome)) =
                    decision
                else {
                    tracing::warn!(
                        decision_id,
                        outcome = update.outcome.as_str(),
                        "route outcome names a decision the ledger does not hold; \
                         route health is left unchanged"
                    );
                    return Ok(());
                };

                // Outcome delivery is soft and may be retried after a lost ack. The first
                // terminal write owns the counter transition; a replay must not count twice.
                if prior_outcome.is_some() {
                    tx.commit()?;
                    return Ok(());
                }


                tx.execute(
                    "UPDATE model_route_decisions
                     SET outcome=?1, outcome_at=?2, outcome_error=?3
                     WHERE id=?4 AND outcome IS NULL",
                    params![
                        update.outcome.as_str(),
                        update.observed_at_ms,
                        update.last_error,
                        decision_id
                    ],
                )?;

                let Some(route_key) = route_key else {
                    tx.commit()?;
                    return Ok(());
                };
                let (provider_id, model_id, variant) = route_identity(
                    &tx,
                    &route_key,
                    selected_provider_id.as_deref().zip(selected_model_id.as_deref()),
                )?;
                let capacity_key = format!("{provider_id}/{model_id}");
                if let Some(retry_after_secs) = update.outcome.retry_after_secs() {
                    let quota_exhausted_until = update
                        .observed_at_ms
                        .saturating_add((retry_after_secs.min(i64::MAX as u64) as i64).saturating_mul(1_000))
                        as f64;
                    let surfaced_error = update
                        .last_error
                        .clone()
                        .unwrap_or_else(|| update.outcome.as_str().to_string());
                    tx.execute(
                        "INSERT INTO model_route_state (
                           route_key, capacity_key, provider_id, model_id, variant,
                           quota_exhausted_until, failure_count, quota_error_count,
                           last_error, last_error_at, updated_at
                         ) VALUES (?1,?2,?3,?4,?5,?6,1,1,?7,?8,?8)
                         ON CONFLICT(route_key) DO UPDATE SET
                           capacity_key=excluded.capacity_key,
                           provider_id=excluded.provider_id,
                           model_id=excluded.model_id,
                           variant=excluded.variant,
                           quota_exhausted_until=CASE
                             WHEN model_route_state.quota_exhausted_until IS NULL
                               OR model_route_state.quota_exhausted_until < excluded.quota_exhausted_until
                             THEN excluded.quota_exhausted_until
                             ELSE model_route_state.quota_exhausted_until END,
                           failure_count=model_route_state.failure_count + 1,
                           quota_error_count=model_route_state.quota_error_count + 1,
                           last_error=excluded.last_error,
                           last_error_at=excluded.last_error_at,
                           updated_at=excluded.updated_at",
                        params![
                            route_key,
                            capacity_key,
                            provider_id,
                            model_id,
                            variant,
                            quota_exhausted_until,
                            surfaced_error,
                            update.observed_at_ms,
                        ],
                    )?;
                }
                let existing: Option<(i64, Option<f64>, Option<String>)> = tx
                    .query_row(
                        "SELECT consecutive_terminal_failures, failure_backoff_until,
                                failure_backoff_reason
                         FROM model_route_state WHERE route_key=?1",
                        [&route_key],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()?;
                let previous_failures = existing
                    .as_ref()
                    .map(|state| state.0.max(0) as u32)
                    .unwrap_or(0);

                match failure_backoff_transition(
                    previous_failures,
                    update.outcome,
                    update.last_error.as_deref(),
                    update.observed_at_ms,
                    update.failure_backoff,
                ) {
                    FailureBackoffTransition::Unchanged => {}
                    FailureBackoffTransition::Reset => {
                        tx.execute(
                            "UPDATE model_route_state
                             SET consecutive_terminal_failures=0,
                                 failure_backoff_until=NULL,
                                 failure_backoff_reason=NULL,
                                 updated_at=?1
                             WHERE route_key=?2",
                            params![update.observed_at_ms, route_key],
                        )?;
                    }
                    FailureBackoffTransition::Failure {
                        consecutive_failures,
                        cooldown_until,
                        reason,
                    } => {
                        let previous_until = existing.as_ref().and_then(|state| state.1);
                        let previous_reason = existing.as_ref().and_then(|state| state.2.clone());
                        let failure_backoff_until =
                            max_defined(previous_until, cooldown_until.map(|value| value as f64));
                        let failure_backoff_reason = reason.or(previous_reason);
                        let surfaced_error = update
                            .last_error
                            .clone()
                            .unwrap_or_else(|| update.outcome.as_str().to_string());
                        tx.execute(
                            "INSERT INTO model_route_state (
                               route_key, capacity_key, provider_id, model_id, variant,
                               failure_count, last_error, last_error_at, updated_at,
                               consecutive_terminal_failures, failure_backoff_until,
                               failure_backoff_reason
                             ) VALUES (?1,?2,?3,?4,?5,1,?6,?7,?7,?8,?9,?10)
                             ON CONFLICT(route_key) DO UPDATE SET
                               capacity_key=excluded.capacity_key,
                               provider_id=excluded.provider_id,
                               model_id=excluded.model_id,
                               variant=excluded.variant,
                               failure_count=model_route_state.failure_count + 1,
                               last_error=excluded.last_error,
                               last_error_at=excluded.last_error_at,
                               updated_at=excluded.updated_at,
                               consecutive_terminal_failures=excluded.consecutive_terminal_failures,
                               failure_backoff_until=excluded.failure_backoff_until,
                               failure_backoff_reason=excluded.failure_backoff_reason",
                            params![
                                route_key,
                                capacity_key,
                                provider_id,
                                model_id,
                                variant,
                                surfaced_error,
                                update.observed_at_ms,
                                consecutive_failures,
                                failure_backoff_until,
                                failure_backoff_reason,
                            ],
                        )?;
                    }
                }

                tx.commit()
            })
            .map_err(err)
    }

    fn clear_failure_backoff(
        &self,
        route_key: &str,
    ) -> RouteStateResult<Option<FailureBackoffState>> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = c.unchecked_transaction()?;
                let prior = tx
                    .query_row(
                        "SELECT consecutive_terminal_failures, failure_backoff_until,
                                failure_backoff_reason
                         FROM model_route_state WHERE route_key = ?1",
                        [route_key],
                        |row| {
                            Ok(FailureBackoffState {
                                consecutive_terminal_failures: row.get::<_, i64>(0)?.max(0) as u32,
                                failure_backoff_until: row.get(1)?,
                                failure_backoff_reason: row.get(2)?,
                            })
                        },
                    )
                    .optional()?;
                if prior.is_some() {
                    tx.execute(
                        "UPDATE model_route_state
                         SET consecutive_terminal_failures = 0,
                             failure_backoff_until = NULL,
                             failure_backoff_reason = NULL,
                             updated_at = ?1
                         WHERE route_key = ?2",
                        params![now_ms(), route_key],
                    )?;
                }
                tx.commit()?;
                Ok(prior)
            })
            .map_err(err)
    }

    fn clear_cooldown(&self, route_key: &str) -> RouteStateResult<Option<CooldownState>> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = c.unchecked_transaction()?;
                let known = tx
                    .query_row(
                        "SELECT 1 FROM model_facts WHERE route_key = ?1",
                        [route_key],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?
                    .is_some();
                if !known {
                    return Ok(None);
                }
                let prior = tx
                    .query_row(
                        "SELECT cooldown_until, quota_exhausted_until
                     FROM model_route_state WHERE route_key = ?1",
                        [route_key],
                        |row| {
                            Ok(CooldownState {
                                cooldown_until: row.get(0)?,
                                quota_exhausted_until: row.get(1)?,
                            })
                        },
                    )
                    .optional()?
                    .unwrap_or_default();
                tx.execute(
                    "UPDATE model_route_state
                     SET cooldown_until = NULL, quota_exhausted_until = NULL,
                         wall_class = NULL, updated_at = ?1
                     WHERE route_key = ?2",
                    params![now_ms(), route_key],
                )?;
                tx.commit()?;
                Ok(Some(prior))
            })
            .map_err(err)
    }

    fn get_decisions_for_task(&self, task_id: &str) -> RouteStateResult<Vec<DecisionRow>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT id, decision_kind, task_id, send_id, target_agent, selected_route_key,
                              rejected_json, explanation_json, quota_signal_json, policy_revision,
                              catalog_fingerprint, outcome, created_at
                      FROM model_route_decisions WHERE task_id=?1 ORDER BY created_at",
                )?;
                let rows = stmt.query_map([task_id], |r| {
                    Ok(DecisionRow {
                        id: r.get(0)?,
                        decision_kind: r.get(1)?,
                        task_id: r.get(2)?,
                        send_id: r.get(3)?,
                        target_agent: r.get(4)?,
                        selected_route_key: r.get(5)?,
                        rejected_json: r.get(6)?,
                        explanation_json: r.get(7)?,
                        quota_signal_json: r.get(8)?,
                        policy_revision: r.get(9)?,
                        catalog_fingerprint: r.get(10)?,
                        outcome: r.get(11)?,
                        created_at: r.get(12)?,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    fn delete_decisions_older_than(
        &self,
        created_before_ms: i64,
        limit: usize,
    ) -> RouteStateResult<usize> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "DELETE FROM model_route_decisions WHERE rowid IN ( \
                       SELECT rowid FROM model_route_decisions \
                       WHERE created_at < ?1 ORDER BY created_at LIMIT ?2)",
                    params![created_before_ms, i64::try_from(limit).unwrap_or(i64::MAX)],
                )
            })
            .map_err(err)
    }

    fn record_usage_delivery(&self, obs: &UsageObservation) -> RouteStateResult<UsageRecordResult> {
        self.record_usage_delivery_with_decision(obs, None)
    }
    fn append_quota_readings(&self, rows: &[QuotaReadingLogRow]) -> RouteStateResult<usize> {
        self.store
            .with_conn(|connection: &Connection| {
                let transaction = connection.unchecked_transaction()?;
                let mut inserted = 0;
                {
                    let mut statement = transaction.prepare(
                        "INSERT OR IGNORE INTO quota_reading_log
                           (account_key, window_key, observed_at, used_percent,
                            raw_used_percent, resets_at, window_minutes,
                            banked_credit_count, fetched_at, is_stale,
                            stale_since, stale_class)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    )?;
                    for row in rows {
                        if row.account_key.is_empty()
                            || row.window_key.is_empty()
                            || row.observed_at < 0
                            || !row.used_percent.is_finite()
                            || row.raw_used_percent.is_some_and(|value| !value.is_finite())
                        {
                            continue;
                        }
                        let (is_stale, stale_since, stale_class) = match row.stale.as_ref() {
                            Some(stale) => (1_i64, stale.since.as_deref(), stale.class.as_deref()),
                            None => (0_i64, None, None),
                        };
                        inserted += statement.execute(params![
                            row.account_key,
                            row.window_key,
                            row.observed_at,
                            row.used_percent,
                            row.raw_used_percent,
                            row.resets_at,
                            row.window_minutes,
                            row.banked_credit_count,
                            row.fetched_at,
                            is_stale,
                            stale_since,
                            stale_class,
                        ])?;
                    }
                }
                transaction.commit()?;
                Ok(inserted)
            })
            .map_err(err)
    }

    fn get_quota_readings(&self, account_key: &str) -> RouteStateResult<Vec<QuotaReadingLogRow>> {
        self.store
            .with_conn(|connection: &Connection| {
                let mut statement = connection.prepare(
                    "SELECT account_key, window_key, observed_at, used_percent,
                            raw_used_percent, resets_at, window_minutes,
                            banked_credit_count, fetched_at, is_stale,
                            stale_since, stale_class
                     FROM quota_reading_log
                     WHERE account_key = ?1
                     ORDER BY window_key, observed_at",
                )?;
                let rows = statement
                    .query_map([account_key], |row| {
                        let is_stale = row.get::<_, i64>(9)? != 0;
                        let stale = if is_stale {
                            Some(QuotaStaleDisclosure {
                                since: row.get(10)?,
                                class: row.get(11)?,
                            })
                        } else {
                            None
                        };
                        Ok(QuotaReadingLogRow {
                            account_key: row.get(0)?,
                            window_key: row.get(1)?,
                            observed_at: row.get(2)?,
                            used_percent: row.get(3)?,
                            raw_used_percent: row.get(4)?,
                            resets_at: row.get(5)?,
                            window_minutes: row.get(6)?,
                            banked_credit_count: row.get(7)?,
                            fetched_at: row.get(8)?,
                            stale,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .map_err(err)
    }

    fn prune_quota_readings(&self, now_ms: i64) -> RouteStateResult<usize> {
        let cutoff = reading_log_retention_cutoff(now_ms);
        self.store
            .with_conn(|connection: &Connection| {
                connection.execute(
                    "DELETE FROM quota_reading_log WHERE observed_at < ?1",
                    [cutoff],
                )
            })
            .map_err(err)
    }

    fn get_observed_speed(&self, route_key: &str, now: f64) -> RouteStateResult<Option<f64>> {
        if route_key.trim().is_empty() {
            return Ok(None);
        }
        self.store
            .with_conn(|c: &Connection| {
                // The most-recent eligible rows in the trailing window, capped at the
                // sample limit; the batched catalog overlay reads the same rows.
                let mut stmt = c.prepare(OBSERVED_SPEED_SAMPLES_SQL)?;
                let rows = stmt.query_map(
                    params![
                        route_key,
                        (now - OBSERVED_SPEED_WINDOW_MS).floor() as i64,
                        now.floor() as i64,
                        USAGE_MIN_OUTPUT_TOKENS.floor() as i64,
                        USAGE_MIN_DURATION_MS.floor() as i64,
                        OBSERVED_SPEED_SAMPLE_LIMIT,
                    ],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
                )?;
                let samples = rows.collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(observed_speed_from_samples(&samples))
            })
            .map_err(err)
    }

    fn get_class_vector(
        &self,
        workload_class: &str,
        now: f64,
    ) -> RouteStateResult<Option<ClassVector>> {
        if !ENUMERATED_WORKLOAD_CLASSES.contains(&workload_class) {
            return Ok(None);
        }
        let generation = self.usage_generation.load(Ordering::Acquire);
        let window_minute = (now / 60_000.0).floor() as i64;
        if let Some(cached) = self
            .class_vectors
            .lock()
            .expect("class vector cache poisoned")
            .get(workload_class)
        {
            if cached.usage_generation == generation && cached.window_minute == window_minute {
                return Ok(cached.vector.clone());
            }
        }
        let vector = self.compute_class_vector(workload_class, now)?;
        self.class_vectors
            .lock()
            .expect("class vector cache poisoned")
            .insert(
                workload_class.to_string(),
                CachedClassVector {
                    usage_generation: generation,
                    window_minute,
                    vector: vector.clone(),
                },
            );
        Ok(vector)
    }

    fn get_facts_with_observed_speed(
        &self,
        now: f64,
        pricing: &prefrontal_routing_core::RoutePricing,
    ) -> RouteStateResult<Vec<ModelFact>> {
        let facts = self.get_facts()?;
        let ephemeral_vector = self.get_class_vector("ephemeral", now)?;
        // Multiple catalog facts can resolve to the same prompt-default variant.
        // Deduplicating here prevents repeated seeks without consulting usage for keys.
        let observed_route_keys = facts
            .iter()
            .map(observed_route_key_for_fact)
            .collect::<BTreeSet<_>>();
        let observed_speeds = self.get_observed_speeds(&observed_route_keys, now)?;
        // A route with too few samples has NO speed, which is not the same as being
        // slow - but `scores.speed` carries 0.0 for both, and 0.0 is the worst value
        // the tie-break can hold. That tie-break decides the whole effectively-free
        // bucket, so an unmeasured route loses every comparison it enters, and
        // winning one is the only way it is ever run and measured. A model could
        // therefore never earn its own speed (live: 231 of 291 catalog rows sit at
        // 0.0, among them routes with the best elo on their price tier).
        //
        // Standing in for the unknown with the MEDIAN of what is measured breaks the
        // deadlock without inventing an advantage: a new route enters as ordinary
        // rather than as worst, gets picked when its price and quality justify it,
        // and replaces the stand-in with its own observation after the sample floor.
        // Falling back to the catalog number first keeps a hand-seeded speed
        // authoritative over a fleet median.
        let median_observed_speed = median_of(observed_speeds.values().copied());
        let mut overlaid = Vec::with_capacity(facts.len());
        for mut fact in facts {
            fact.class_vector = ephemeral_vector.clone();
            fact.class_vector_is_overlay = ephemeral_vector.is_some();
            let observed_route_key = observed_route_key_for_fact(&fact);
            if let Some(speed) = observed_speeds.get(&observed_route_key) {
                fact.scores.speed = *speed;
            } else if fact.scores.speed <= 0.0 {
                if let Some(median) = median_observed_speed {
                    fact.scores.speed = median;
                    fact.speed_is_fleet_median = true;
                }
            }
            let route_key = prefrontal_routing_core::make_route_key(
                &fact.provider_id,
                &fact.model_id,
                fact.variant.as_deref(),
            );
            if let Some(multiplier) = pricing.speed_multiplier(&route_key) {
                fact.scores.speed *= multiplier;
                fact.speed_multiplier = Some(multiplier);
            }
            // Subscription subsidy is overlaid rather than persisted, so the
            // objective can price the route at list ÷ multiplier.
            if let Some(multiplier) = pricing.subscription_multiplier(&route_key) {
                fact.subscription_multiplier = Some(multiplier);
            }
            overlaid.push(fact);
        }
        Ok(overlaid)
    }

    fn build_snapshot(&self, now: f64) -> RouteStateResult<ModelRouteStateSnapshot> {
        self.store
            .with_conn(|c: &Connection| {
                let mut by_route: HashMap<String, RouteRuntimeState> = HashMap::new();
                let mut by_capacity: HashMap<String, RouteRuntimeState> = HashMap::new();

                let mut stmt = c.prepare(
                    "SELECT route_key, capacity_key, cooldown_until, quota_exhausted_until,
                            quota_pressure, latency_ema_ms, success_ema,
                            failure_backoff_until, failure_backoff_reason, last_error, wall_class
                     FROM model_route_state",
                )?;
                let rows = stmt.query_map([], |r| {
                    Ok(StateRow {
                        route_key: r.get(0)?,
                        capacity_key: r.get(1)?,
                        cooldown_until: r.get(2)?,
                        quota_exhausted_until: r.get(3)?,
                        quota_pressure: r.get(4)?,
                        latency_ema_ms: r.get(5)?,
                        success_ema: r.get(6)?,
                        failure_backoff_until: r.get(7)?,
                        failure_backoff_reason: r.get(8)?,
                        last_error: r.get(9)?,
                        wall_class: row_wall_class(r, 10)?,
                    })
                })?;
                for row in rows {
                    let row = row?;
                    let mut state = RouteRuntimeState {
                        cooldown_until: row.cooldown_until,
                        quota_exhausted_until: row.quota_exhausted_until,
                        wall_class: row.wall_class,
                        last_error: row.last_error.clone(),
                        latency_ema_ms: row.latency_ema_ms,
                        success_ema: row.success_ema,
                        failure_backoff_until: row.failure_backoff_until,
                        failure_backoff_reason: row.failure_backoff_reason,
                        ..Default::default()
                    };
                    // Read-side quarantine leaves the original row available for
                    // diagnosis. Its single-account horizon cannot reject another
                    // credential, including through a sibling capacity key.
                    if prefrontal_routing_core::account_wall::account_scoped_wall(
                        state.last_error.as_deref(),
                        state.wall_class,
                    ) {
                        state.cooldown_until = None;
                        state.quota_exhausted_until = None;
                        state.wall_class = None;
                    }
                    // Capacity-shared signals propagate across variants of the same
                    // provider/model. Soft pressure lives ONLY on the capacity key so a
                    // fresh quota reading can replace a stale stored value for every
                    // sibling route at once.
                    let cap = by_capacity.entry(row.capacity_key.clone()).or_default();
                    cap.cooldown_until = max_defined(cap.cooldown_until, state.cooldown_until);
                    cap.quota_exhausted_until =
                        max_defined(cap.quota_exhausted_until, state.quota_exhausted_until);
                    let row_wall_is_live = state.cooldown_until.is_some_and(|until| until > now)
                        || state.quota_exhausted_until.is_some_and(|until| until > now);
                    if row_wall_is_live
                        && (cap.wall_class.is_none()
                            || state.wall_class == Some(RouteWallClass::Billing))
                    {
                        cap.wall_class = state.wall_class;
                        cap.last_error = state.last_error.clone();
                    }
                    if let Some(pressure) = row.quota_pressure {
                        cap.quota_pressure = Some(match cap.quota_pressure {
                            Some(existing) => existing.max(pressure),
                            None => pressure,
                        });
                    }
                    by_route.insert(row.route_key, state);
                }

                Ok(ModelRouteStateSnapshot {
                    now,
                    by_route,
                    by_capacity,
                })
            })
            .map_err(err)
    }
}

#[derive(Default)]
struct ExistingProviderState {
    cooldown_until: Option<f64>,
    quota_exhausted_until: Option<f64>,
    quota_pressure: Option<f64>,
    wall_class: Option<RouteWallClass>,
    last_error: Option<String>,
    last_error_at: Option<i64>,
}

#[derive(Default)]
struct ExistingState {
    cooldown_until: Option<f64>,
    quota_exhausted_until: Option<f64>,
    wall_class: Option<RouteWallClass>,
    quota_pressure: Option<f64>,
    latency_ema_ms: Option<f64>,
    success_ema: Option<f64>,
    failure_count: i64,
    quota_error_count: i64,
    last_error: Option<String>,
    last_error_at: Option<i64>,
}

struct StateRow {
    route_key: String,
    capacity_key: String,
    cooldown_until: Option<f64>,
    quota_exhausted_until: Option<f64>,
    wall_class: Option<RouteWallClass>,
    last_error: Option<String>,
    quota_pressure: Option<f64>,
    latency_ema_ms: Option<f64>,
    success_ema: Option<f64>,
    failure_backoff_until: Option<f64>,
    failure_backoff_reason: Option<String>,
}

/// Whether a stored wall was written from a permanent refusal of one named
/// credential. Such a wall is not provider or route evidence: the credential's
/// fence covers it, so it neither blocks nor lends its horizon to a later write.
fn credential_refusal_wall(last_error: Option<&str>) -> bool {
    last_error.is_some_and(|error| {
        prefrontal_routing_core::account_wall::credential_refusal_in(error).is_some()
    })
}

/// `max` over two optional times, treating `None` as "no value" (the defined one
/// wins; the larger of two defined values wins). Mirrors the TS `maxDefined`.
fn max_defined(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) => Some(x),
        (None, b) => b,
    }
}

/// Split `provider/model[/variant]` into its parts. A key with three or more
/// segments is ambiguous: `p/a/b` is model `a/b` with no variant or model `a`
/// with variant `b`. This reads it as the latter, so it is only the fallback
/// for a route the catalog does not hold; see `route_identity`.
fn split_route_key(route_key: &str) -> (String, String, Option<String>) {
    let parts: Vec<&str> = route_key.split('/').collect();
    if parts.len() >= 3 {
        (
            parts[0].to_string(),
            parts[1].to_string(),
            Some(parts[2..].join("/")),
        )
    } else {
        (
            parts.first().copied().unwrap_or("").to_string(),
            parts.get(1).copied().unwrap_or("").to_string(),
            None,
        )
    }
}

/// The provider, model and variant behind `route_key`. A catalog route's stored
/// fact is authoritative, since its model id may itself contain `/`. Without a
/// fact, a caller that knows the provider and model (a decision records both)
/// passes them as `known_model`, and the variant is whatever the key adds after
/// them. With no catalog fact, and no `known_model` that the key starts with,
/// the key falls back to `split_route_key`.
fn route_identity(
    connection: &Connection,
    route_key: &str,
    known_model: Option<(&str, &str)>,
) -> rusqlite::Result<(String, String, Option<String>)> {
    if let Some(identity) = connection
        .query_row(
            "SELECT provider_id, model_id, variant FROM model_facts WHERE route_key = ?1",
            [route_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?
    {
        return Ok(identity);
    }
    if let Some((provider_id, model_id)) = known_model {
        let capacity_key = prefrontal_routing_core::make_capacity_key(provider_id, model_id);
        if route_key == capacity_key {
            return Ok((provider_id.to_owned(), model_id.to_owned(), None));
        }
        if let Some(variant) = route_key
            .strip_prefix(capacity_key.as_str())
            .and_then(|rest| rest.strip_prefix('/'))
        {
            return Ok((
                provider_id.to_owned(),
                model_id.to_owned(),
                Some(variant.to_owned()),
            ));
        }
    }
    Ok(split_route_key(route_key))
}

/// A short unique-ish id (12 hex chars), process-monotonic + time-seeded. Avoids a
/// uuid dependency for decision ids (their uniqueness needs are local).
fn short_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{:08x}{:04x}", t & 0xffff_ffff, n & 0xffff)
}

fn confidence_str(c: Confidence) -> &'static str {
    match c {
        Confidence::Curated => "curated",
        Confidence::Unknown => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prefrontal_routing_core::ScoreProvenance;

    #[cfg(unix)]
    #[test]
    fn route_store_state_is_owner_only_under_test_scratch_root() {
        use std::os::unix::fs::PermissionsExt;

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-private-state-{}-{timestamp}",
            std::process::id()
        ));
        let db = root.join("store.db");
        let descriptor = dev_descriptor("prefrontal-routing-private-state", db.to_string_lossy());
        let store = SqliteRouteStateStore::open(&descriptor).expect("open scratch routing store");

        let mut pending = vec![root.clone()];
        while let Some(path) = pending.pop() {
            let metadata = std::fs::symlink_metadata(&path).expect("stat route-state path");
            let mode = metadata.permissions().mode() & 0o777;
            if metadata.is_dir() {
                assert_eq!(
                    mode,
                    0o700,
                    "routing directory is not owner-only: {}",
                    path.display()
                );
                pending.extend(
                    std::fs::read_dir(&path)
                        .expect("list route-state directory")
                        .map(|entry| entry.unwrap().path()),
                );
            } else {
                assert_eq!(
                    mode,
                    0o600,
                    "routing file is not owner-only: {}",
                    path.display()
                );
            }
        }

        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn migration_065_preserves_062_evidence_and_freezes_historical_classes() {
        use prefrontal_routing_core::accounts::{AllowedAccountClass, ModelFamily, PoolScope};
        let scratch = prefrontal_test_support::ScratchDir::new("routing-policy-065");
        let descriptor = dev_descriptor(
            "routing-policy-065",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let legacy = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 62).unwrap();
        let observed_at = 1_000_000;
        legacy.store.with_conn(|c| {
            populate_account_tables(c, observed_at)?;
            for (id, consumer) in [("cm", Some("codemode")), ("fl", Some("flow")), ("ma", Some("mason")), ("null", None)] {
                insert_ledger_row(c, id, None, observed_at, 2)?;
                c.execute("UPDATE model_route_decisions SET consumer=?2 WHERE id=?1", params![id, consumer])?;
                c.execute("INSERT INTO routing_v1_decision (decision_id, principal, consumer, provider, model,
                    runner_module, affinity, created_at_ms, expires_at_ms)
                    VALUES (?1, 'owner', ?2, 'p', 'm', 'runner', 'conversation', ?3, ?4)",
                    params![id, consumer.unwrap_or("other"), observed_at, observed_at+60_000])?;
            }
            for (id, scope) in [
                ("general", r#"{"kind":"general","value":"seven_day"}"#),
                ("short", r#"{"kind":"general","value":"five_hour"}"#),
                ("reported", r#"{"kind":"general","value":{"reported":90}}"#),
                ("family", r#"{"kind":"family","value":"opus"}"#),
                ("unknown", r#"{"kind":"unknown_scope","value":"fable"}"#),
            ] {
                let evidence = format!(r#"{{"account_id":"a","scope":{scope},"observed_at_ms":{observed_at},"reset":{{"kind":"at","at_ms":2000000}},"retry_hint_ms":null,"source":"raw_header"}}"#);
                c.execute("INSERT INTO account_fence (fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at)
                    VALUES (?1,'anthropic','a',?2,?3,'raw_header',?4,'p',?4)", params![id,scope,evidence,observed_at])?;
            }
            c.execute_batch("INSERT INTO routing_v1_affinity VALUES ('owner','mason','conversation','ma',1000000,NULL);
                INSERT INTO routing_v1_report VALUES ('ma','report','report body','report answer',1000000);
                INSERT INTO routing_v1_admission VALUES ('runner','admission','original body','original answer',1000000);
                INSERT INTO credential_entitlement_fences VALUES ('p','cred','ENTITLEMENT_ERROR','listing',1000000);
                INSERT INTO provider_reset_fences VALUES ('p/m','mason','a','cred',2000000,1000000);")?;
            Ok(())
        }).unwrap();
        let old_admission = legacy
            .account_transaction(|tx| tx.admission("p", "adm-1"))
            .unwrap()
            .unwrap();
        let old_fences = legacy
            .account_transaction(|tx| tx.active_fences("anthropic"))
            .unwrap();
        drop(legacy);

        let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
        drop(migrated);
        let restarted = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert_eq!(
            restarted
                .account_transaction(|tx| tx.admission("p", "adm-1"))
                .unwrap(),
            Some(old_admission)
        );
        let blocks = restarted
            .account_transaction(|tx| tx.active_pool_fences("anthropic"))
            .unwrap();
        for block in &blocks {
            assert_eq!(
                &block.evidence,
                old_fences
                    .iter()
                    .find(|row| row.fence_id == block.evidence.fence_id)
                    .unwrap()
            );
            assert!(block.recovery_set.is_empty());
            if block.evidence.fence_id == "fence-1" {
                continue;
            }
            let key = block.pool_key.as_ref().unwrap();
            assert_eq!(key.name, None);
            match block.evidence.fence_id.as_str() {
                "general" => {
                    assert_eq!(key.window_minutes, Some(10080));
                    assert_eq!(key.scope, PoolScope::EveryModel);
                }
                "short" => {
                    assert_eq!(key.window_minutes, Some(300));
                    assert_eq!(key.scope, PoolScope::EveryModel);
                }
                "reported" => {
                    assert_eq!(key.window_minutes, Some(90));
                    assert_eq!(key.scope, PoolScope::EveryModel);
                }
                "family" => {
                    assert_eq!(key.window_minutes, None);
                    assert_eq!(key.scope, PoolScope::Family(ModelFamily::Opus));
                }
                "unknown" => {
                    assert_eq!(key.window_minutes, None);
                    assert_eq!(key.scope, PoolScope::Family(ModelFamily::Fable));
                }
                other => panic!("unexpected fence: {other}"),
            }
        }
        assert_eq!(blocks.len(), 6);
        for (id, expected) in [
            ("cm", AllowedAccountClass::PricedPerTokenOnly),
            ("fl", AllowedAccountClass::PricedPerTokenOnly),
            ("ma", AllowedAccountClass::NotPayPerToken),
            ("null", AllowedAccountClass::NotPayPerToken),
        ] {
            assert_eq!(
                restarted.decision_account_class(id).unwrap(),
                Some(expected)
            );
            assert_eq!(
                restarted
                    .v1_decision(id)
                    .unwrap()
                    .unwrap()
                    .allowed_account_class,
                expected
            );
        }
        assert_eq!(
            restarted
                .v1_affinity_decision("owner", "mason", "conversation")
                .unwrap()
                .unwrap()
                .decision_id,
            "ma"
        );
        assert_eq!(
            restarted.v1_report("ma").unwrap().unwrap().answer_json,
            "report answer"
        );
        let standing = restarted
            .v1_admission("runner", "admission")
            .unwrap()
            .unwrap();
        assert_eq!(standing.body_json, "original body");
        assert_eq!(standing.answer_json, "original answer");
        assert_eq!(
            restarted
                .v1_record_admission(
                    "runner",
                    "admission",
                    &V1Recorded {
                        body_json: "different body".into(),
                        answer_json: "different answer".into()
                    },
                    2_000_000
                )
                .unwrap(),
            standing
        );
        assert_eq!(restarted.account_serving_counts().unwrap().holds, 0);
        assert_eq!(
            restarted.credential_entitlement_fences().unwrap()[0].code,
            "ENTITLEMENT_ERROR"
        );
        restarted
            .store
            .with_conn(|c| {
                let dropped: i64 = c.query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE name='provider_reset_fences'",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(dropped, 0);
                assert_eq!(
                    c.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                    0
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn migration_065_upgrades_a_deployed_064_database() {
        let scratch = prefrontal_test_support::ScratchDir::new("routing-migration-064");
        let descriptor = dev_descriptor(
            "routing-migration-064",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let legacy = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 64).unwrap();
        legacy
            .store
            .with_conn(|c| {
                insert_ledger_row(c, "deployed", None, 1_000, 2)?;
                c.execute(
                    "UPDATE model_route_decisions SET consumer='codemode' WHERE id='deployed'",
                    [],
                )
            })
            .unwrap();
        drop(legacy);
        let upgraded = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert_eq!(
            upgraded.decision_account_class("deployed").unwrap(),
            Some(prefrontal_routing_core::accounts::AllowedAccountClass::PricedPerTokenOnly)
        );
    }

    #[test]
    fn whole_account_recovery_set_survives_restart() {
        use prefrontal_routing_core::accounts::{ModelFamily, PoolKey, PoolScope};
        let scratch = prefrontal_test_support::ScratchDir::new("routing-pool-recovery");
        let descriptor = dev_descriptor(
            "routing-pool-recovery",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let block = AccountPoolFenceRow {
            evidence: AccountFenceRow {
                fence_id: "whole".into(),
                provider: "p".into(),
                account_id: "a".into(),
                scope_json: "null".into(),
                fence_json: "{}".into(),
                source: "observation".into(),
                observed_at: 1_000,
                principal: "runner".into(),
                admission_id: None,
                observation_id: None,
                created_at: 2_000,
                cleared_at: None,
                cleared_reason: None,
            },
            pool_key: None,
            recovery_set: vec![
                PoolKey {
                    window_minutes: None,
                    scope: PoolScope::Family(ModelFamily::Opus),
                    name: None,
                },
                PoolKey {
                    window_minutes: Some(90),
                    scope: PoolScope::EveryModel,
                    name: Some("pool-p".into()),
                },
                PoolKey {
                    window_minutes: Some(90),
                    scope: PoolScope::EveryModel,
                    name: Some("pool-q".into()),
                },
            ],
        };
        store
            .account_transaction(|tx| tx.insert_pool_fence(&block))
            .unwrap();
        drop(store);
        let restarted = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert_eq!(
            restarted
                .account_transaction(|tx| tx.active_pool_fences("p"))
                .unwrap(),
            vec![block]
        );
    }

    fn legacy_account_wall_error() -> String {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../prefrontal-routing-core/tests/fixtures/account-wall-scope.json"
        ))
        .unwrap();
        vectors[0]["error"].as_str().unwrap().into()
    }

    fn insert_legacy_account_wall(store: &SqliteRouteStateStore, route: &str, class: &str) {
        store.store.with_conn(|conn| conn.execute(
            "INSERT INTO model_route_state (route_key,capacity_key,provider_id,model_id,cooldown_until,quota_exhausted_until,last_error,wall_class,updated_at)
             VALUES (?1,'anthropic/opus','anthropic','opus',1791493229401,1791493229401,?2,?3,1791048824539)",
            params![route,legacy_account_wall_error(),class],
        )).unwrap();
    }

    #[test]
    fn legacy_account_wall_is_quarantined_without_erasing_evidence_or_real_walls() {
        let scratch = prefrontal_test_support::ScratchDir::new("account-wall-quarantine");
        let store = SqliteRouteStateStore::open(&dev_descriptor(
            "account-wall-quarantine",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        insert_legacy_account_wall(&store, "anthropic/opus/medium", "quota");
        insert_legacy_account_wall(&store, "anthropic/billing", "billing");
        let snapshot = store.build_snapshot(1791214158478.0).unwrap();
        let scoped = &snapshot.by_route["anthropic/opus/medium"];
        assert_eq!(scoped.cooldown_until, None);
        assert_eq!(scoped.quota_exhausted_until, None);
        assert!(scoped.last_error.as_ref().unwrap().contains("57ce2cb7"));
        assert_eq!(
            snapshot.by_route["anthropic/billing"].quota_exhausted_until,
            Some(1791493229401.0)
        );
        store.store.with_conn(|conn| {
            let stored:(i64,String) = conn.query_row("SELECT quota_exhausted_until,last_error FROM model_route_state WHERE route_key='anthropic/opus/medium'",[],|row| Ok((row.get(0)?,row.get(1)?)))?;
            assert_eq!(stored,(1791493229401,legacy_account_wall_error()),"quarantine is read-only");
            Ok::<_,rusqlite::Error>(())
        }).unwrap();
        // An independent sibling keeps the capacity closed; quarantining one
        // account's row never clears a real pool-wide or billing wall.
        store
            .update_route_state(
                "anthropic/opus/high",
                "anthropic/opus",
                &RouteStateUpdate {
                    quota_exhausted_until: Some(1791493229500.0),
                    wall_class: Some(RouteWallClass::Quota),
                    last_error: Some("provider pool exhausted".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            store.build_snapshot(1791214158478.0).unwrap().by_capacity["anthropic/opus"]
                .quota_exhausted_until,
            Some(1791493229500.0)
        );
        // A new unscoped wall must replace the old account narrative even if
        // its caller supplies no diagnostic message.
        store
            .update_route_state(
                "anthropic/opus/medium",
                "anthropic/opus",
                &RouteStateUpdate {
                    quota_exhausted_until: Some(1791493229600.0),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            store.build_snapshot(1791214158478.0).unwrap().by_route["anthropic/opus/medium"]
                .quota_exhausted_until,
            Some(1791493229600.0)
        );
    }

    #[test]
    fn route_state_writers_keep_account_exhaustion_out_of_model_health() {
        let scratch = prefrontal_test_support::ScratchDir::new("account-wall-writers");
        let store = SqliteRouteStateStore::open(&dev_descriptor(
            "account-wall-writers",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        for (index, error) in [
            legacy_account_wall_error(),
            r#"{"provider_code":"account_limit_exhausted","account_ref":"ysample"}"#.into(),
        ]
        .into_iter()
        .enumerate()
        {
            let route = format!("anthropic/writer-{index}");
            let update = RouteStateUpdate {
                cooldown_until: Some(1791493229401.0),
                quota_exhausted_until: Some(1791493229401.0),
                wall_class: Some(RouteWallClass::Quota),
                last_error: Some(error.clone()),
                increment_failure: true,
                increment_quota_error: true,
                ..Default::default()
            };
            store.update_route_state(&route, &route, &update).unwrap();
            store
                .record_provider_cooldown("anthropic", &route, &route, &update)
                .unwrap();
            let decision = store
                .record_decision(&DecisionInput {
                    selected_route_key: Some(route.clone()),
                    selected_provider_id: Some("anthropic".into()),
                    selected_model_id: Some(format!("writer-{index}")),
                    target_agent: "mason".into(),
                    requirements_json: "{}".into(),
                    score_json: "{}".into(),
                    rejected_json: "[]".into(),
                    ..Default::default()
                })
                .unwrap();
            store
                .set_decision_outcome(
                    &decision,
                    &DecisionOutcomeUpdate {
                        provider_outcome: None,
                        outcome: prefrontal_routing_core::RouteOutcome::QuotaExhausted {
                            retry_after_secs: 279100,
                        },
                        last_error: Some(error),
                        observed_at_ms: 1791214158478,
                        failure_backoff: Default::default(),
                    },
                )
                .unwrap();
            store
                .store
                .with_conn(|conn| {
                    let rows: i64 = conn.query_row(
                        "SELECT count(*) FROM model_route_state WHERE route_key=?1",
                        [&route],
                        |row| row.get(0),
                    )?;
                    assert_eq!(
                        rows, 0,
                        "an account error must not write any model-health row"
                    );
                    let outcome: String = conn.query_row(
                        "SELECT outcome FROM model_route_decisions WHERE id=?1",
                        [&decision],
                        |row| row.get(0),
                    )?;
                    assert_eq!(outcome, "interrupted");
                    Ok::<_, rusqlite::Error>(())
                })
                .unwrap();
        }
        assert!(store.get_provider_cooldown("anthropic").unwrap().is_none());
    }

    #[test]
    fn clear_failure_backoff_preserves_cooldown_and_quota_walls() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-clear-failure-backoff-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor(
            "prefrontal-routing-clear-failure-backoff",
            path.to_string_lossy(),
        );
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        store
            .store
            .with_conn(|connection| {
                connection.execute(
                    "INSERT INTO model_route_state (
                       route_key, capacity_key, provider_id, model_id, updated_at,
                       consecutive_terminal_failures, failure_backoff_until,
                       failure_backoff_reason, cooldown_until, quota_exhausted_until
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        "test-provider/test-model",
                        "test-provider/test-model",
                        "test-provider",
                        "test-model",
                        1_i64,
                        3_i64,
                        1_000_i64,
                        "consecutive terminal send failures: 3",
                        // A 17-day wall, the shape a misread stack trace once wrote.
                        1_500_000_000_000_i64,
                        1_500_000_000_000_i64
                    ],
                )
            })
            .unwrap();

        assert_eq!(
            store
                .clear_failure_backoff("test-provider/test-model")
                .unwrap(),
            Some(FailureBackoffState {
                consecutive_terminal_failures: 3,
                failure_backoff_until: Some(1_000.0),
                failure_backoff_reason: Some("consecutive terminal send failures: 3".into()),
            })
        );
        let after: (i64, Option<f64>, Option<String>, Option<f64>, Option<f64>) = store
            .store
            .with_conn(|connection| {
                connection.query_row(
                    "SELECT consecutive_terminal_failures, failure_backoff_until,
                            failure_backoff_reason, cooldown_until, quota_exhausted_until
                     FROM model_route_state WHERE route_key = ?1",
                    ["test-provider/test-model"],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
            })
            .unwrap();
        assert_eq!(
            after,
            (
                0,
                None,
                None,
                Some(1_500_000_000_000.0),
                Some(1_500_000_000_000.0)
            )
        );
        drop(store);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(path.with_extension("sqlite.lock"));
    }

    #[test]
    fn migration_031_drops_a_populated_catalog_generation_table_through_the_store_runner() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-catalog-generation-migration-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor(
            "prefrontal-routing-catalog-generation-migration",
            path.to_string_lossy(),
        );

        let legacy = open_sqlite(&descriptor).unwrap();
        let migrations = route_state_migrations();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &migrations[..30])
            .unwrap();
        legacy
            .with_conn(|connection| {
                connection.execute(
                    "INSERT INTO catalog_generation(fingerprint,created_at) VALUES ('legacy',1)",
                    [],
                )
            })
            .unwrap();
        drop(legacy);

        let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
        let table_exists: bool = migrated
            .store
            .with_conn(|connection| {
                connection.query_row(
                    "SELECT EXISTS(
                       SELECT 1 FROM sqlite_schema
                       WHERE type='table' AND name='catalog_generation'
                     )",
                    [],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert!(!table_exists);
        drop(migrated);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(path.with_extension("sqlite.lock"));
    }

    #[test]
    fn migration_053_keeps_legacy_execution_identity_unknown() {
        let scratch = prefrontal_test_support::scratch_root().join(format!(
            "routing-selected-identity-{}-{}",
            std::process::id(),
            short_id()
        ));
        std::fs::create_dir_all(&scratch).unwrap();
        let descriptor = dev_descriptor(
            "routing-selected-identity",
            scratch.join("store.db").to_string_lossy(),
        );
        let legacy = open_sqlite(&descriptor).unwrap();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &route_state_migrations()[..52])
            .unwrap();
        legacy.with_conn(|connection| {
            connection.execute(
                "INSERT INTO model_route_decisions
                 (id, harness, send_id, target_agent, requirements_json, selected_route_key,
                  selected_provider_id, selected_model_id, score_json, rejected_json, anchor_version, created_at)
                 VALUES ('legacy', 'pi', 'legacy-send', 'explore', '{}', 'provider/model',
                         'provider', 'model', '{}', '[]', 1, 1)",
                [],
            )
        }).unwrap();
        drop(legacy);

        for _ in 0..2 {
            let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
            let row = migrated.get_decision_by_id("legacy").unwrap().unwrap();
            assert_eq!(row.harness.as_deref(), Some("pi"));
            assert_eq!(row.selected_harness, None);
            assert_eq!(row.selected_workload, None);
            assert_eq!(row.selected_route_key.as_deref(), Some("provider/model"));
            assert_eq!(
                migrated.get_decision_by_send_id("legacy-send").unwrap(),
                Some(row)
            );
        }
        std::fs::remove_dir_all(scratch).unwrap();
    }

    #[test]
    fn migration_052_retires_the_shadow_index_and_the_purge_removes_only_shadow_rows() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-shadow-retirement-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor(
            "prefrontal-routing-shadow-retirement",
            path.to_string_lossy(),
        );

        // A store as the previous binary left it: shadow decisions from the
        // timer, observations and refused requests beside served history
        // (a pinned admit with its admission and affinity, and a refusal).
        let legacy = open_sqlite(&descriptor).unwrap();
        let migrations = route_state_migrations();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &migrations[..51])
            .unwrap();
        legacy
            .with_conn(|connection| {
                let mut insert = connection.prepare(
                    "INSERT INTO account_decision (decision_id, mode, trigger, principal, provider, \
                       model, family, outcome, code, policy_version, decision_json, created_at) \
                     VALUES (?1, ?2, ?3, 'reserved:broca', ?4, 'claude-opus-5-5', 'opus', ?5, ?6, 2, '{}', ?7)",
                )?;
                for index in 0..5_i64 {
                    insert.execute(rusqlite::params![
                        format!("shadow-timer-{index}"), "shadow", "timer", "anthropic",
                        "admit", "cold_scored", 1_000 + index
                    ])?;
                }
                insert.execute(rusqlite::params![
                    "shadow-observation", "shadow", "observation", "xai", "refuse",
                    "account_selection_unsupported", 2_000
                ])?;
                insert.execute(rusqlite::params![
                    "shadow-request", "shadow", "request", "anthropic", "admit", "cold_scored", 3_000
                ])?;
                insert.execute(rusqlite::params![
                    "serve-admit", "serve", "request", "anthropic", "admit", "cold_scored", 1_500
                ])?;
                insert.execute(rusqlite::params![
                    "serve-refuse", "serve", "request", "anthropic", "refuse",
                    "account_quota_unknown", 2_500
                ])?;
                connection.execute_batch(
                    "INSERT INTO account_pin (pin_id, principal, conversation_hash, provider, \
                       pin_generation, affinity_model, account_id, credential_id, pin_mode, \
                       decision_id, created_at) \
                     VALUES ('pin-1', 'reserved:broca', 'h', 'anthropic', 1, 'claude-opus-5-5', \
                       'acct-a', 'oauth:a', 'run_frozen', 'serve-admit', 1500); \
                     INSERT INTO account_affinity (principal, conversation_hash, provider, \
                       affinity_model, pin_id, pin_generation, account_id, credential_id, \
                       credential_binding_revision, policy_version, initial_demand_bytes, \
                       created_at, last_used_at, expires_at, lease_valid_until) \
                     VALUES ('reserved:broca', 'h', 'anthropic', 'claude-opus-5-5', 'pin-1', 1, \
                       'acct-a', 'oauth:a', 1, 2, 10, 1500, 1500, 9000, 1620); \
                     INSERT INTO account_admission (principal, admission_id, request_fingerprint, \
                       request_mode, decision_id, conversation_hash, provider, model, account_id, \
                       credential_id, pin_id, pin_generation, state, charged, request_bytes, \
                       request_bytes_estimated, committed_at, admission_valid_until, \
                       lease_valid_until, reply_json) \
                     VALUES ('reserved:broca', 'adm-1', 'fp', 'admit', 'serve-admit', 'h', \
                       'anthropic', 'claude-opus-5-5', 'acct-a', 'oauth:a', 'pin-1', 1, \
                       'finished', 1, 10, 0, 1500, 1560, 1620, '{}');",
                )
            })
            .unwrap();
        drop(legacy);

        let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
        let indexes: Vec<String> = migrated
            .store
            .with_conn(|connection| {
                let mut statement = connection.prepare(
                    "SELECT name FROM sqlite_schema WHERE type = 'index' AND name IN \
                     ('account_decision_latest', 'account_admission_by_committed', \
                      'account_admission_by_pin', 'account_affinity_by_expiry', \
                      'account_hold_by_created') ORDER BY name",
                )?;
                let names = statement.query_map([], |row| row.get(0))?;
                names.collect()
            })
            .unwrap();
        assert_eq!(
            indexes,
            vec![
                "account_admission_by_committed",
                "account_admission_by_pin",
                "account_affinity_by_expiry",
                "account_hold_by_created",
            ],
            "the shadow heartbeat index is dropped and the retention indexes exist"
        );

        let mut purged = 0;
        loop {
            let removed = migrated.purge_shadow_decisions(2).unwrap();
            purged += removed;
            if removed < 2 {
                break;
            }
        }
        assert_eq!(purged, 7);
        let remaining = migrated
            .store
            .with_conn(|connection| {
                connection.query_row(
                    "SELECT group_concat(decision_id, ','), \
                       (SELECT count(*) FROM account_pin), \
                       (SELECT count(*) FROM account_affinity), \
                       (SELECT count(*) FROM account_admission) \
                     FROM (SELECT decision_id FROM account_decision ORDER BY decision_id)",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
            })
            .unwrap();
        assert_eq!(remaining, ("serve-admit,serve-refuse".to_string(), 1, 1, 1));
        drop(migrated);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(path.with_extension("sqlite.lock"));
    }

    #[test]
    fn migration_033_adds_aa_columns_without_rewriting_existing_scores() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-aa-inferred-migration-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor(
            "prefrontal-routing-aa-inferred-migration",
            path.to_string_lossy(),
        );

        let legacy = open_sqlite(&descriptor).unwrap();
        let migrations = route_state_migrations();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &migrations[..32])
            .unwrap();
        let (elo, source): (f64, String) = legacy
            .with_conn(|connection| {
                connection.query_row(
                    "SELECT json_extract(scores_json, '$.elo'), source
                     FROM model_facts WHERE route_key = 'openai/gpt-5.6-sol'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
            })
            .unwrap();
        assert_eq!(elo, 1_838.0);
        assert_eq!(source, "seed");
        drop(legacy);

        let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
        let sol = migrated
            .get_fact_by_route_key("openai/gpt-5.6-sol")
            .unwrap()
            .unwrap();
        assert_eq!(sol.scores.elo, 1_838.0);
        assert_eq!(sol.score_provenance, ScoreProvenance::Curated);
        assert_eq!(sol.aa_index, None);
        let columns: bool = migrated
            .store
            .with_conn(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) > 0 FROM pragma_table_info('model_facts') WHERE name = 'aa_index'",
                    [],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert!(columns);
        drop(migrated);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(path.with_extension("sqlite.lock"));
    }

    #[test]
    fn migration_035_normalizes_every_historical_family_spelling() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-family-migration-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor(
            "prefrontal-routing-family-migration",
            path.to_string_lossy(),
        );
        let fixtures = [
            (
                "anthropic/claude-opus-4-5",
                "anthropic",
                GrandFamily::Claude,
            ),
            ("openai/gpt-5.4", "openai", GrandFamily::Gpt),
            ("openai/gpt-5.5", "openai-gpt", GrandFamily::Gpt),
            (
                "google/antigravity-gemini-3.1-pro",
                "google-gemini",
                GrandFamily::Gemini,
            ),
            ("xai/grok-4.3", "xai-grok", GrandFamily::Grok),
        ];

        let legacy = open_sqlite(&descriptor).unwrap();
        let migrations = route_state_migrations();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &migrations[..34])
            .unwrap();
        legacy
            .with_conn(|connection| {
                for (route_key, spelling, _) in fixtures {
                    connection.execute(
                        "UPDATE model_facts SET family = ?2 WHERE route_key = ?1",
                        params![route_key, spelling],
                    )?;
                }
                Ok(())
            })
            .unwrap();
        drop(legacy);

        let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
        for (route_key, _, expected) in fixtures {
            assert_eq!(
                migrated.get_model_family(route_key).unwrap(),
                Some((Some(expected), FamilySource::Curated))
            );
        }
        drop(migrated);
        remove_observed_speed_test_store(&path);
    }

    #[test]
    fn migration_035_refuses_and_lists_unknown_family_rows() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-family-refusal-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor =
            dev_descriptor("prefrontal-routing-family-refusal", path.to_string_lossy());
        let legacy = open_sqlite(&descriptor).unwrap();
        let migrations = route_state_migrations();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &migrations[..34])
            .unwrap();
        legacy
            .with_conn(|connection| {
                connection.execute(
                    "UPDATE model_facts SET family = 'mystery-family' WHERE route_key = 'openai/gpt-5.4'",
                    [],
                )
            })
            .unwrap();
        drop(legacy);

        let error = match SqliteRouteStateStore::open(&descriptor) {
            Ok(_) => panic!("unknown family spelling was accepted"),
            Err(error) => error,
        };
        assert!(error.0.contains("openai/gpt-5.4=mystery-family"), "{error}");
        remove_observed_speed_test_store(&path);
    }

    const OLD_OBSERVED_SPEED_AGGREGATE_SQL: &str = "WITH ranked_usage AS (
       SELECT route_key, output_tokens, duration_ms,
              ROW_NUMBER() OVER (
                PARTITION BY route_key ORDER BY observed_at DESC
              ) AS sample_rank
       FROM model_route_usage
       WHERE observed_at > ?1
         AND observed_at <= ?2
         AND output_tokens >= ?3
         AND duration_ms >= ?4
     )
     SELECT route_key, COUNT(*), SUM(output_tokens), SUM(duration_ms)
     FROM ranked_usage
     WHERE sample_rank <= ?5
     GROUP BY route_key";

    fn observed_speed_test_store(label: &str) -> (std::path::PathBuf, SqliteRouteStateStore) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = prefrontal_test_support::scratch_root().join(format!(
            "prefrontal-routing-observed-speed-{label}-{}-{timestamp}.sqlite",
            std::process::id()
        ));
        let descriptor = dev_descriptor(
            &format!("prefrontal-routing-observed-speed-{label}"),
            path.to_string_lossy(),
        );
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        (path, store)
    }

    fn io_observation(id: &str, at: f64) -> UsageObservation {
        UsageObservation {
            id: id.into(),
            route_key: "io/model".into(),
            capacity_key: "io/model".into(),
            provider_id: "io".into(),
            input_tokens: Some(1000.0),
            output_tokens: 500.0,
            duration_ms: 1000.0,
            observed_at: at,
            reasoning_tokens: Some(100.0),
            cached_read_tokens: Some(200.0),
            cache_write_tokens: Some(300.0),
            workload_class: Some("ephemeral".into()),
            account_key: Some("io/account".into()),
        }
    }

    #[test]
    fn duplicate_usage_delivery_changes_no_database_pages() {
        let (dir, store) = observed_speed_test_store("duplicate-io");
        let obs = io_observation("message", 1_800_000_000_000.0);
        assert!(store.record_usage_delivery(&obs).unwrap().usage_recorded);
        let observer = Connection::open(&dir).unwrap();
        let version = || {
            observer
                .pragma_query_value(None, "data_version", |r| r.get::<_, i64>(0))
                .unwrap()
        };
        let before = version();
        for _ in 0..10 {
            assert_eq!(
                store.record_usage_delivery(&obs).unwrap(),
                UsageRecordResult::default()
            );
        }
        assert_eq!(
            version(),
            before,
            "a duplicate must not write even unchanged values"
        );
        drop(observer);
        drop(store);
        remove_observed_speed_test_store(&dir);
    }

    #[test]
    fn duplicate_usage_delivery_does_not_invalidate_the_population_cache() {
        let (dir, store) = observed_speed_test_store("duplicate-cache");
        let mut obs = io_observation("message", 1_800_000_000_000.0);
        obs.cached_read_tokens = None;
        store.record_usage_delivery(&obs).unwrap();
        let before = store.usage_generation.load(Ordering::Acquire);
        store.record_usage_delivery(&obs).unwrap();
        assert_eq!(store.usage_generation.load(Ordering::Acquire), before);
        // A NULL-only profile upgrade still invalidates, even if speed was
        // already eligible and the reply reports neither new recording effect.
        obs.cached_read_tokens = Some(200.0);
        assert_eq!(
            store.record_usage_delivery(&obs).unwrap(),
            UsageRecordResult::default()
        );
        assert_eq!(store.usage_generation.load(Ordering::Acquire), before + 1);
        drop(store);
        remove_observed_speed_test_store(&dir);
    }

    #[test]
    fn usage_delivery_enriches_each_null_column_and_preserves_first_values() {
        use rusqlite::types::Value;

        struct Case {
            column: &'static str,
            clear: fn(&mut UsageObservation),
            conflict: fn(&mut UsageObservation),
            expected: Value,
        }

        let cases = [
            Case {
                column: "input_tokens",
                clear: |obs| obs.input_tokens = None,
                conflict: |obs| obs.input_tokens = Some(4000.0),
                expected: Value::Integer(1000),
            },
            Case {
                column: "output_tokens",
                clear: |obs| obs.output_tokens = f64::NAN,
                conflict: |obs| obs.output_tokens = 2000.0,
                expected: Value::Integer(500),
            },
            Case {
                column: "duration_ms",
                clear: |obs| obs.duration_ms = f64::NAN,
                conflict: |obs| obs.duration_ms = 2000.0,
                expected: Value::Integer(1000),
            },
            Case {
                column: "reasoning_tokens",
                clear: |obs| obs.reasoning_tokens = None,
                conflict: |obs| obs.reasoning_tokens = Some(300.0),
                expected: Value::Integer(100),
            },
            Case {
                column: "cached_read_tokens",
                clear: |obs| obs.cached_read_tokens = None,
                conflict: |obs| obs.cached_read_tokens = Some(400.0),
                expected: Value::Integer(200),
            },
            Case {
                column: "cache_write_tokens",
                clear: |obs| obs.cache_write_tokens = None,
                conflict: |obs| obs.cache_write_tokens = Some(500.0),
                expected: Value::Integer(300),
            },
            Case {
                column: "workload_class",
                clear: |obs| obs.workload_class = None,
                conflict: |obs| obs.workload_class = Some("head".into()),
                expected: Value::Text("ephemeral".into()),
            },
            Case {
                column: "account_key",
                clear: |obs| obs.account_key = None,
                conflict: |obs| obs.account_key = Some("io/rebound".into()),
                expected: Value::Text("io/account".into()),
            },
        ];
        let (path, store) = observed_speed_test_store("all-enrichment-columns");
        let observer = Connection::open(&path).unwrap();
        let stored = |id: &str, column: &str| {
            observer
                .query_row(
                    &format!("SELECT {column} FROM model_route_usage WHERE id = ?1"),
                    [id],
                    |row| row.get::<_, Value>(0),
                )
                .unwrap()
        };

        for case in cases {
            let complete =
                io_observation(&format!("enrichment-{}", case.column), 1_800_000_000_000.0);
            let mut missing = complete.clone();
            // Output/duration are non-optional in the domain observation; an
            // unavailable measurement is stored as NULL. Verify that setup
            // rather than assuming the sentinel reached the database as NULL.
            (case.clear)(&mut missing);
            store.record_usage_delivery(&missing).unwrap();
            assert_eq!(
                stored(&complete.id, case.column),
                Value::Null,
                "{}: first delivery must leave this column NULL",
                case.column
            );

            let before = store.usage_generation.load(Ordering::Acquire);
            store.record_usage_delivery(&complete).unwrap();
            assert_eq!(
                stored(&complete.id, case.column),
                case.expected,
                "{}: late enrichment must persist",
                case.column
            );
            assert_eq!(
                store.usage_generation.load(Ordering::Acquire),
                before + 1,
                "{}: enrichment must invalidate the population cache",
                case.column
            );

            let mut conflicting = complete.clone();
            (case.conflict)(&mut conflicting);
            let before = store.usage_generation.load(Ordering::Acquire);
            store.record_usage_delivery(&conflicting).unwrap();
            assert_eq!(
                stored(&complete.id, case.column),
                case.expected,
                "{}: an already-set value must not be overwritten",
                case.column
            );
            assert_eq!(
                store.usage_generation.load(Ordering::Acquire),
                before,
                "{}: a conflicting repeat is still a no-op",
                case.column
            );

            // Also exercise COALESCE during a real UPDATE, not just a delivery
            // whose WHERE rejects every assignment. Filling another column
            // must not replace the first value in the column under test.
            let mut first = complete.clone();
            first.id = format!("first-value-{}", case.column);
            let (witness, witness_value) = if case.column == "input_tokens" {
                first.reasoning_tokens = None;
                ("reasoning_tokens", Value::Integer(100))
            } else {
                first.input_tokens = None;
                ("input_tokens", Value::Integer(1000))
            };
            store.record_usage_delivery(&first).unwrap();
            assert_eq!(stored(&first.id, case.column), case.expected);
            assert_eq!(stored(&first.id, witness), Value::Null);
            conflicting.id = first.id.clone();
            let before = store.usage_generation.load(Ordering::Acquire);
            store.record_usage_delivery(&conflicting).unwrap();
            assert_eq!(
                stored(&first.id, witness),
                witness_value,
                "{}: witness enrichment must execute the UPDATE",
                case.column
            );
            assert_eq!(
                stored(&first.id, case.column),
                case.expected,
                "{}: COALESCE must preserve the first value during another column's enrichment",
                case.column
            );
            assert_eq!(store.usage_generation.load(Ordering::Acquire), before + 1);
        }
        drop(observer);
        drop(store);
        remove_observed_speed_test_store(&path);
    }

    fn insert_usage_row(
        store: &SqliteRouteStateStore,
        id: &str,
        route_key: &str,
        output_tokens: i64,
        duration_ms: i64,
        observed_at: i64,
    ) {
        store
            .store
            .with_conn(|connection| {
                connection.execute(
                    "INSERT INTO model_route_usage
                       (id, task_id, route_key, capacity_key, provider_id,
                        input_tokens, output_tokens, duration_ms, observed_at)
                     VALUES (?1, NULL, ?2, ?2, 'test', NULL, ?3, ?4, ?5)",
                    params![id, route_key, output_tokens, duration_ms, observed_at],
                )
            })
            .unwrap();
    }

    fn old_observed_speed_aggregates(
        store: &SqliteRouteStateStore,
        now: f64,
    ) -> Vec<ObservedSpeedAggregate> {
        let mut aggregates = store
            .store
            .with_conn(|connection| {
                let mut statement = connection.prepare(OLD_OBSERVED_SPEED_AGGREGATE_SQL)?;
                let rows = statement.query_map(
                    params![
                        (now - OBSERVED_SPEED_WINDOW_MS).floor() as i64,
                        now.floor() as i64,
                        USAGE_MIN_OUTPUT_TOKENS.floor() as i64,
                        USAGE_MIN_DURATION_MS.floor() as i64,
                        OBSERVED_SPEED_SAMPLE_LIMIT,
                    ],
                    |row| {
                        Ok(ObservedSpeedAggregate {
                            route_key: row.get(0)?,
                            sample_count: row.get(1)?,
                            output_tokens: row.get(2)?,
                            duration_ms: row.get(3)?,
                            // The SQL oracle checks row selection only; the speed is
                            // compared separately after `without_speed`.
                            speed: None,
                        })
                    },
                )?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap();
        aggregates.sort_by(|left, right| left.route_key.cmp(&right.route_key));
        aggregates
    }

    /// Drop the computed speed so bounded-seek aggregates can be compared with the
    /// SQL oracle, which reproduces row selection and sums but not the median.
    fn without_speed(aggregates: &[ObservedSpeedAggregate]) -> Vec<ObservedSpeedAggregate> {
        aggregates
            .iter()
            .map(|aggregate| ObservedSpeedAggregate {
                speed: None,
                ..aggregate.clone()
            })
            .collect()
    }

    fn remove_observed_speed_test_store(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(path.with_extension("sqlite.lock"));
    }

    #[test]
    fn bounded_observed_speed_aggregates_match_the_shipped_window_oracle() {
        let (path, store) = observed_speed_test_store("equivalence");
        let now = OBSERVED_SPEED_WINDOW_MS + 10_000.0;
        let window_start = (now - OBSERVED_SPEED_WINDOW_MS) as i64;
        let route_keys = BTreeSet::from([
            "test/more-than-limit".to_string(),
            "test/exactly-limit".to_string(),
            "test/insufficient".to_string(),
            "test/zero-eligible".to_string(),
        ]);

        for index in 0..25_i64 {
            insert_usage_row(
                &store,
                &format!("more-{index}"),
                "test/more-than-limit",
                1_000 + index,
                1_000,
                window_start + 100 + index,
            );
        }
        for index in 0..OBSERVED_SPEED_SAMPLE_LIMIT {
            insert_usage_row(
                &store,
                &format!("exact-{index}"),
                "test/exactly-limit",
                2_000,
                1_000,
                window_start + 200 + index,
            );
        }
        for index in 0..(OBSERVED_SPEED_MIN_SAMPLES as i64 - 1) {
            insert_usage_row(
                &store,
                &format!("insufficient-{index}"),
                "test/insufficient",
                3_000,
                1_000,
                window_start + 300 + index,
            );
        }

        insert_usage_row(
            &store,
            "more-outside-window",
            "test/more-than-limit",
            50_000,
            1_000,
            window_start - 1,
        );
        insert_usage_row(
            &store,
            "more-below-token-floor",
            "test/more-than-limit",
            USAGE_MIN_OUTPUT_TOKENS as i64 - 1,
            1_000,
            window_start + 1_000,
        );
        insert_usage_row(
            &store,
            "more-below-duration-floor",
            "test/more-than-limit",
            50_000,
            USAGE_MIN_DURATION_MS as i64 - 1,
            window_start + 1_001,
        );
        insert_usage_row(
            &store,
            "zero-outside-window",
            "test/zero-eligible",
            50_000,
            1_000,
            window_start - 1,
        );
        insert_usage_row(
            &store,
            "zero-below-token-floor",
            "test/zero-eligible",
            USAGE_MIN_OUTPUT_TOKENS as i64 - 1,
            1_000,
            window_start + 1_100,
        );
        insert_usage_row(
            &store,
            "zero-below-duration-floor",
            "test/zero-eligible",
            50_000,
            USAGE_MIN_DURATION_MS as i64 - 1,
            window_start + 1_101,
        );

        let expected = old_observed_speed_aggregates(&store, now);
        let actual = store
            .get_observed_speed_aggregates(&route_keys, now)
            .unwrap();
        assert_eq!(without_speed(&actual), expected);
        let sample_count = |route_key: &str| {
            actual
                .iter()
                .find(|aggregate| aggregate.route_key == route_key)
                .unwrap()
                .sample_count
        };
        assert_eq!(
            sample_count("test/more-than-limit"),
            OBSERVED_SPEED_SAMPLE_LIMIT
        );
        assert_eq!(
            sample_count("test/exactly-limit"),
            OBSERVED_SPEED_SAMPLE_LIMIT
        );
        assert_eq!(
            sample_count("test/insufficient"),
            OBSERVED_SPEED_MIN_SAMPLES as i64 - 1
        );

        let trusted = store.get_observed_speeds(&route_keys, now).unwrap();
        assert_eq!(trusted.len(), 2);
        assert!(trusted.contains_key("test/more-than-limit"));
        assert!(trusted.contains_key("test/exactly-limit"));
        assert!(!trusted.contains_key("test/insufficient"));
        assert!(!trusted.contains_key("test/zero-eligible"));

        drop(store);
        remove_observed_speed_test_store(&path);
    }

    #[test]
    fn observed_speed_and_class_vectors_share_the_trailing_window_bounds() {
        // Both read `(now - 7d, now]` from the same usage table: a row exactly
        // seven days old is in neither population. The class-vector side of the
        // bound is pinned by the integration test
        // `complete_ephemeral_row_trailing_window_boundary_pinned_to_now_argument`.
        let (path, store) = observed_speed_test_store("window-bounds");
        let now = OBSERVED_SPEED_WINDOW_MS + 10_000.0;
        let window_start = (now - OBSERVED_SPEED_WINDOW_MS) as i64;
        assert_eq!(OBSERVED_SPEED_WINDOW_MS, CLASS_VECTOR_TRAILING_WINDOW_MS);
        insert_usage_row(
            &store,
            "at-start",
            "test/bounds",
            2_000,
            1_000,
            window_start,
        );
        insert_usage_row(
            &store,
            "after-start",
            "test/bounds",
            2_000,
            1_000,
            window_start + 1,
        );

        let aggregates = store
            .get_observed_speed_aggregates(&BTreeSet::from(["test/bounds".to_string()]), now)
            .unwrap();
        assert_eq!(aggregates.len(), 1);
        assert_eq!(
            aggregates[0].sample_count, 1,
            "the row exactly seven days old is outside the speed window"
        );

        drop(store);
        remove_observed_speed_test_store(&path);
    }

    #[test]
    #[ignore = "prints a local wall-time comparison for the shipped and bounded query shapes"]
    fn observed_speed_bounded_seek_benchmark() {
        let (path, store) = observed_speed_test_store("benchmark");
        let now = OBSERVED_SPEED_WINDOW_MS + 200_000.0;
        let window_start = (now - OBSERVED_SPEED_WINDOW_MS) as i64;
        store
            .store
            .with_conn(|connection| {
                connection.execute(
                    "WITH RECURSIVE sequence(n) AS (
                       VALUES(1)
                       UNION ALL
                       SELECT n + 1 FROM sequence WHERE n < 100000
                     )
                     INSERT INTO model_route_usage
                       (id, task_id, route_key, capacity_key, provider_id,
                        input_tokens, output_tokens, duration_ms, observed_at)
                     SELECT printf('benchmark-%d', n), NULL, 'benchmark/route',
                            'benchmark/route', 'benchmark', NULL, 1000, 1000, ?1 + n
                     FROM sequence",
                    [window_start],
                )
            })
            .unwrap();

        let shipped_started = std::time::Instant::now();
        let expected = old_observed_speed_aggregates(&store, now);
        let shipped_elapsed = shipped_started.elapsed();
        let route_keys = BTreeSet::from(["benchmark/route".to_string()]);
        let bounded_started = std::time::Instant::now();
        let actual = store
            .get_observed_speed_aggregates(&route_keys, now)
            .unwrap();
        let bounded_elapsed = bounded_started.elapsed();
        assert_eq!(without_speed(&actual), expected);
        println!(
            "observed_speed_benchmark rows=100000 shipped_ms={:.3} bounded_ms={:.3}",
            shipped_elapsed.as_secs_f64() * 1_000.0,
            bounded_elapsed.as_secs_f64() * 1_000.0,
        );

        drop(store);
        remove_observed_speed_test_store(&path);
    }

    /// The head-history query as it read the decision ledger before migration
    /// 054: no index covers it, so it visits every ledger row. Kept here as the
    /// oracle the narrow copy must agree with, and as the slow shape the
    /// benchmark measures.
    const LEDGER_PARENT_FAILURES_SQL: &str =
        "SELECT selected_route_key, selected_provider_id, outcome_error, outcome_at
         FROM model_route_decisions
         WHERE parent_session_id = ?1
           AND outcome_at >= ?2
           AND outcome_error IS NOT NULL
         ORDER BY outcome_at DESC, id DESC";

    /// The test clock, 2026-10-19T00:00:00Z.
    const HEAD_NOW: i64 = 1_792_368_000_000;

    fn ledger_parent_failures(
        connection: &Connection,
        parent_session_id: &str,
        since_ms: i64,
    ) -> rusqlite::Result<Vec<ParentFailedDecision>> {
        let mut statement = connection.prepare(LEDGER_PARENT_FAILURES_SQL)?;
        let rows = statement.query_map(params![parent_session_id, since_ms], |row| {
            Ok(ParentFailedDecision {
                route_key: row.get(0)?,
                provider_id: row.get(1)?,
                outcome_error: row.get(2)?,
                outcome_at: row.get(3)?,
            })
        })?;
        rows.collect()
    }

    /// Insert one ledger row the way an older routing build left it: a
    /// `payload_bytes`-long score payload sits before the outcome columns, as
    /// real score JSON does.
    fn insert_ledger_row(
        connection: &Connection,
        id: &str,
        parent_session_id: Option<&str>,
        created_at: i64,
        payload_bytes: i64,
    ) -> rusqlite::Result<usize> {
        connection.execute(
            "INSERT INTO model_route_decisions
               (id, parent_session_id, target_agent, requirements_json, selected_route_key,
                selected_provider_id, selected_model_id, score_json, rejected_json,
                anchor_version, created_at)
             VALUES (?1, ?2, 'mason', '{}', 'p/' || ?1, 'p', ?1,
                     printf('%.*c', ?4, 'x'), '[]', 1, ?3)",
            params![id, parent_session_id, created_at, payload_bytes],
        )
    }

    fn fail_ledger_row(
        connection: &Connection,
        id: &str,
        outcome_at: i64,
        error: Option<&str>,
    ) -> rusqlite::Result<usize> {
        connection.execute(
            "UPDATE model_route_decisions
             SET outcome = 'session_error', outcome_at = ?2, outcome_error = ?3
             WHERE id = ?1",
            params![id, outcome_at, error],
        )
    }

    /// Count SQLite virtual-machine steps on the store's connection while `f`
    /// runs: a machine-independent measure of how many rows a read visited.
    fn vm_steps_during<T>(store: &SqliteRouteStateStore, f: impl FnOnce() -> T) -> (T, u64) {
        unsafe extern "C" fn count_step(counter: *mut std::ffi::c_void) -> std::ffi::c_int {
            // SAFETY: `counter` is the AtomicU64 below, which outlives the
            // handler because the handler is removed before it is dropped.
            unsafe { &*(counter as *const AtomicU64) }.fetch_add(1, Ordering::Relaxed);
            0
        }
        let counter = AtomicU64::new(0);
        let counter_ptr = &counter as *const AtomicU64 as *mut std::ffi::c_void;
        store
            .store
            .with_conn(|connection| {
                // SAFETY: the handle belongs to the live connection guarded by
                // the store mutex; the callback only touches `counter`.
                unsafe {
                    rusqlite::ffi::sqlite3_progress_handler(
                        connection.handle(),
                        1,
                        Some(count_step),
                        counter_ptr,
                    );
                }
                Ok(())
            })
            .unwrap();
        let out = f();
        store
            .store
            .with_conn(|connection| {
                // SAFETY: as above; passing no callback removes the handler.
                unsafe {
                    rusqlite::ffi::sqlite3_progress_handler(
                        connection.handle(),
                        0,
                        None,
                        std::ptr::null_mut(),
                    );
                }
                Ok(())
            })
            .unwrap();
        (out, counter.load(Ordering::Relaxed))
    }

    #[test]
    fn head_failure_history_answers_like_the_ledger_without_scanning_it() {
        let (path, store) = observed_speed_test_store("head-failures");
        let recent = HEAD_NOW - 10 * 60_000;
        let created = HEAD_NOW - 60 * 60_000;
        store
            .store
            .with_conn(|c| {
                for (id, parent) in [
                    ("a-recent", Some("head-a")),
                    ("a-tie-1", Some("head-a")),
                    ("a-tie-2", Some("head-a")),
                    ("a-old", Some("head-a")),
                    ("a-ok", Some("head-a")),
                    ("a-pending", Some("head-a")),
                    ("a-retracted", Some("head-a")),
                    ("a-real", Some("head-a")),
                    ("b-recent", Some("head-b")),
                    ("n-recent", None),
                ] {
                    insert_ledger_row(c, id, parent, created, 64)?;
                }
                // Ancient, so the retention delete below removes it.
                insert_ledger_row(c, "a-expired", Some("head-a"), 1, 64)?;
                fail_ledger_row(c, "a-recent", recent, Some("content policy"))?;
                fail_ledger_row(c, "a-tie-1", recent - 1, Some("refused"))?;
                fail_ledger_row(c, "a-tie-2", recent - 1, Some("refused"))?;
                fail_ledger_row(c, "a-old", HEAD_NOW - 2 * 60 * 60_000, Some("old"))?;
                fail_ledger_row(c, "a-ok", recent, None)?;
                fail_ledger_row(c, "a-retracted", recent, Some("refused"))?;
                fail_ledger_row(c, "b-recent", recent, Some("refused"))?;
                fail_ledger_row(c, "n-recent", recent, Some("refused"))?;
                fail_ledger_row(c, "a-expired", recent, Some("refused"))?;
                // A later change to the copied columns must reach the copy.
                c.execute(
                    "UPDATE model_route_decisions SET outcome_error = NULL WHERE id = 'a-retracted'",
                    [],
                )
            })
            .unwrap();
        // The routing module's own outcome writer.
        store
            .set_decision_outcome(
                "a-real",
                &DecisionOutcomeUpdate {
                    provider_outcome: None,
                    outcome: prefrontal_routing_core::RouteOutcome::SessionError,
                    last_error: Some("blocked by the content policy".into()),
                    observed_at_ms: recent + 1,
                    failure_backoff: Default::default(),
                },
            )
            .unwrap();
        // The retention pass's delete.
        assert_eq!(store.delete_decisions_older_than(1_000, 10).unwrap(), 1);

        let since = HEAD_NOW - 60 * 60_000;
        for (parent, since_ms) in [
            ("head-a", since),
            ("head-a", 0),
            ("head-b", since),
            ("head-c", 0),
        ] {
            let expected = store
                .store
                .with_conn(|c| ledger_parent_failures(c, parent, since_ms))
                .unwrap();
            let actual = store
                .recent_failed_decisions_for_parent(parent, since_ms)
                .unwrap();
            assert_eq!(actual, expected, "{parent} since {since_ms}");
        }
        let head_a: Vec<String> = store
            .recent_failed_decisions_for_parent("head-a", since)
            .unwrap()
            .into_iter()
            .map(|row| row.route_key)
            .collect();
        assert_eq!(head_a, ["p/a-real", "p/a-recent", "p/a-tie-2", "p/a-tie-1"]);

        // Fill the ledger with other heads' decisions, a third of them failed.
        // The ledger query visits all of them; the copy visits head-a's rows.
        store
            .store
            .with_conn(|c| {
                let tx = c.unchecked_transaction()?;
                for index in 0..3_000 {
                    let id = format!("filler-{index}");
                    insert_ledger_row(
                        &tx,
                        &id,
                        Some(&format!("head-{}", index % 50)),
                        created,
                        2_000,
                    )?;
                    if index % 3 == 0 {
                        fail_ledger_row(&tx, &id, recent, Some("refused"))?;
                    }
                }
                tx.commit()
            })
            .unwrap();
        let (copy_rows, copy_steps) = vm_steps_during(&store, || {
            store
                .recent_failed_decisions_for_parent("head-a", since)
                .unwrap()
        });
        let (ledger_rows, ledger_steps) = vm_steps_during(&store, || {
            store
                .store
                .with_conn(|c| ledger_parent_failures(c, "head-a", since))
                .unwrap()
        });
        assert_eq!(copy_rows, ledger_rows);
        println!("head_failure_vm_steps ledger_rows=3011 copy={copy_steps} ledger={ledger_steps}");
        assert!(
            copy_steps < 500,
            "the head-history read must visit only that head's failures, \
             not the decision ledger: {copy_steps} VM steps"
        );
        assert!(
            ledger_steps > 20 * copy_steps,
            "the oracle must show the scan this test guards against: \
             ledger {ledger_steps} vs copy {copy_steps} VM steps"
        );

        drop(store);
        remove_observed_speed_test_store(&path);
    }

    #[test]
    fn migration_054_copies_recent_failures_found_through_the_created_at_index() {
        let scratch = prefrontal_test_support::scratch_root().join(format!(
            "routing-parent-failures-{}-{}",
            std::process::id(),
            short_id()
        ));
        std::fs::create_dir_all(&scratch).unwrap();
        let descriptor = dev_descriptor(
            "routing-parent-failures",
            scratch.join("store.db").to_string_lossy(),
        );
        let now = now_ms();
        let legacy = open_sqlite(&descriptor).unwrap();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &route_state_migrations()[..53])
            .unwrap();
        legacy
            .with_conn(|c| {
                insert_ledger_row(c, "recent", Some("head"), now - 60 * 60_000, 64)?;
                fail_ledger_row(c, "recent", now - 30 * 60_000, Some("refused"))?;
                // Made thirty days ago, refused just now: outside the backfill's
                // seven-day decision window, as the migration documents.
                insert_ledger_row(c, "long-lived", Some("head"), now - 30 * 86_400_000, 64)?;
                fail_ledger_row(c, "long-lived", now - 60_000, Some("refused"))?;
                insert_ledger_row(c, "no-head", None, now - 60 * 60_000, 64)?;
                fail_ledger_row(c, "no-head", now - 30 * 60_000, Some("refused"))
            })
            .unwrap();
        drop(legacy);

        let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
        let rows = migrated
            .recent_failed_decisions_for_parent("head", now - 60 * 60_000)
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.route_key.as_str())
                .collect::<Vec<_>>(),
            ["p/recent"]
        );
        drop(migrated);
        std::fs::remove_dir_all(scratch).unwrap();
    }

    fn history_foreign_keys(connection: &Connection) -> Vec<String> {
        let mut statement = connection
            .prepare(
                "SELECT s.name || ' -> ' || f.\"table\" \
                 FROM sqlite_schema AS s, pragma_foreign_key_list(s.name) AS f \
                 WHERE s.type = 'table' ORDER BY 1",
            )
            .unwrap();
        let rows = statement.query_map([], |row| row.get(0)).unwrap();
        rows.collect::<rusqlite::Result<_>>().unwrap()
    }

    /// The tables migration 58 rebuilds.
    const REBUILT_TABLES: [&str; 5] = [
        "account_pin",
        "account_admission",
        "account_hold",
        "account_fence",
        "account_principal_exclusion",
    ];

    /// Everything about the rebuilt tables a rebuild must keep: every column
    /// (name, type, NOT NULL, default, key position) and every index and
    /// trigger on them (name and SQL with whitespace collapsed; autoindexes,
    /// which have no SQL, by name).
    fn rebuilt_table_shape(connection: &Connection) -> Vec<String> {
        let mut shape = Vec::new();
        for table in REBUILT_TABLES {
            let mut columns = connection
                .prepare(
                    "SELECT name, type, \"notnull\", COALESCE(dflt_value, ''), pk \
                     FROM pragma_table_xinfo(?1) ORDER BY cid",
                )
                .unwrap();
            let rows = columns
                .query_map([table], |row| {
                    Ok(format!(
                        "{table} column {} {} notnull={} default={} pk={}",
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?
                    ))
                })
                .unwrap();
            shape.extend(rows.map(Result::unwrap));
            let mut objects = connection
                .prepare(
                    "SELECT type, name, COALESCE(sql, '') FROM sqlite_schema \
                     WHERE tbl_name = ?1 AND type IN ('index', 'trigger') ORDER BY type, name",
                )
                .unwrap();
            let rows = objects
                .query_map([table], |row| {
                    let sql: String = row.get(2)?;
                    Ok(format!(
                        "{table} {} {} {}",
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        sql.split_whitespace().collect::<Vec<_>>().join(" ")
                    ))
                })
                .unwrap();
            shape.extend(rows.map(Result::unwrap));
        }
        shape
    }

    fn populate_account_tables(c: &Connection, now: i64) -> rusqlite::Result<()> {
        c.execute_batch(&format!(
            "INSERT INTO account_decision (decision_id, mode, trigger, principal, provider,
               model, family, outcome, code, policy_version, decision_json, created_at)
             VALUES ('dec-1', 'serve', 'request', 'p', 'anthropic', 'm', 'opus', 'admit',
               'cold_scored', 2, '{{}}', {now});
             INSERT INTO account_pin (pin_id, principal, conversation_hash, provider,
               pin_generation, affinity_model, account_id, credential_id, pin_mode,
               decision_id, created_at)
             VALUES ('pin-1', 'p', 'c', 'anthropic', 1, 'm', 'a', 'cred', 'run_frozen',
               'dec-1', {now});
             INSERT INTO account_admission (principal, admission_id, request_fingerprint,
               request_mode, decision_id, conversation_hash, provider, model, account_id,
               credential_id, pin_id, pin_generation, state, charged, request_bytes,
               request_bytes_estimated, committed_at, admission_valid_until,
               lease_valid_until, reply_json)
             VALUES ('p', 'adm-1', 'fp', 'admit', 'dec-1', 'c', 'anthropic', 'm', 'a',
               'cred', 'pin-1', 1, 'reserved', 1, 10, 0, {now}, {now}, {now}, '{{}}');
             INSERT INTO account_hold (decision_id, principal, conversation_hash, provider,
               pin_id, pin_generation, account_id, credential_id, basis, reset_at,
               retry_after_s, created_at)
             VALUES ('dec-1', 'p', 'c', 'anthropic', 'pin-1', 1, 'a', 'cred', 'five_hour',
               {now}, 60, {now});
             INSERT INTO account_observation (observation_id, principal, runner, event_id,
               kind, bound, observation_json, effects_json, received_at)
             VALUES ('aco-recent', 'p', 'broca', 'recent', 'attempt', 1, '{{}}',
               '[\"fence\"]', {recent}),
               ('aco-old', 'p', 'broca', 'old', 'attempt', 0, '{{}}', '[]', {old});
             INSERT INTO account_fence (fence_id, provider, account_id, scope_json,
               fence_json, source, observed_at, principal, observation_id, created_at)
             VALUES ('fence-1', 'anthropic', 'a', '{{}}', '{{}}', 'observation', {now},
               'p', 'aco-recent', {now});
             INSERT INTO account_principal_exclusion (principal, admission_id,
               credential_id, runner_view_digest, roster_view_digest, observation_id,
               recorded_at)
             VALUES ('p', 'adm-1', 'cred', 'r', 'v', 'aco-recent', {now});",
            recent = now - 60_000,
            old = now - 7 * 3_600_000,
        ))
    }

    fn admission_insert(id: &str, decision: &str, pin: &str) -> String {
        format!(
            "INSERT INTO account_admission (principal, admission_id, request_fingerprint,
               request_mode, decision_id, conversation_hash, provider, model, account_id,
               credential_id, pin_id, pin_generation, state, charged, request_bytes,
               request_bytes_estimated, committed_at, admission_valid_until,
               lease_valid_until, reply_json)
             VALUES ('p', '{id}', 'fp', 'admit', '{decision}', 'c', 'anthropic', 'm', 'a',
               'cred', '{pin}', 1, 'reserved', 1, 10, 0, 1, 1, 1, '{{}}')"
        )
    }

    fn route_state_version(c: &Connection) -> rusqlite::Result<i64> {
        c.query_row(
            "SELECT MAX(version) FROM cortexkit_schema_version WHERE namespace = ?1",
            [ROUTE_STATE_NAMESPACE],
            |row| row.get(0),
        )
    }

    /// A store at migration 55, with every account table populated while
    /// foreign keys are enforced, opens through migrations 56 and 58: the
    /// foreign keys into the two history logs are gone, the admission-to-pin
    /// key holds, every row, column, index and trigger of the rebuilt tables
    /// is kept, the integrity check passes, and migration 56 copied only the
    /// recent runner event's key.
    #[test]
    fn migration_058_rebuilds_the_account_tables_without_history_foreign_keys() {
        let scratch = prefrontal_test_support::scratch_root().join(format!(
            "routing-history-foreign-keys-{}-{}",
            std::process::id(),
            short_id()
        ));
        std::fs::create_dir_all(&scratch).unwrap();
        let descriptor = dev_descriptor(
            "routing-history-foreign-keys",
            scratch.join("store.db").to_string_lossy(),
        );
        let now = now_ms();
        let legacy = open_sqlite(&descriptor).unwrap();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &route_state_migrations()[..55])
            .unwrap();
        let (legacy_keys, legacy_shape) = legacy
            .with_conn(|c| {
                assert_eq!(route_state_version(c)?, 55);
                populate_account_tables(c, now)?;
                Ok((history_foreign_keys(c), rebuilt_table_shape(c)))
            })
            .unwrap();
        drop(legacy);
        assert_eq!(
            legacy_keys,
            [
                "account_admission -> account_decision",
                "account_admission -> account_pin",
                "account_fence -> account_observation",
                "account_hold -> account_decision",
                "account_pin -> account_decision",
                "account_principal_exclusion -> account_observation",
            ]
        );
        assert!(legacy_shape.iter().any(|line| line.contains(" index ")));

        let migrated = SqliteRouteStateStore::open(&descriptor).unwrap();
        migrated
            .store
            .with_conn(|c| {
                assert_eq!(
                    route_state_version(c)?,
                    route_state_migrations().last().unwrap().version as i64
                );
                assert_eq!(
                    history_foreign_keys(c),
                    ["account_admission -> account_pin"]
                );
                // Later migrations add the fence's pool columns and (068)
                // rekey the demand index by account; the demand query-plan
                // test checks that index's current shape.
                let demand_index = "account_admission index account_admission_demand ";
                let shape: Vec<_> = rebuilt_table_shape(c)
                    .into_iter()
                    .filter(|line| {
                        !line.starts_with("account_fence column pool_key_json ")
                            && !line.starts_with("account_fence column recovery_set_json ")
                            && !line.starts_with(demand_index)
                    })
                    .collect();
                let legacy_shape: Vec<_> = legacy_shape
                    .iter()
                    .filter(|line| !line.starts_with(demand_index))
                    .cloned()
                    .collect();
                assert_eq!(shape, legacy_shape);
                let integrity: String =
                    c.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
                assert_eq!(integrity, "ok");
                let leftovers: i64 = c.query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE name LIKE '%_rebuilt'",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(leftovers, 0);
                for (table, rows) in [
                    ("account_decision", 1),
                    ("account_pin", 1),
                    ("account_admission", 1),
                    ("account_hold", 0),
                    ("account_observation", 2),
                    ("account_fence", 1),
                    ("account_principal_exclusion", 1),
                ] {
                    let count: i64 =
                        c.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                            row.get(0)
                        })?;
                    assert_eq!(count, rows, "{table}");
                }
                let keys: String = c.query_row(
                    "SELECT group_concat(event_id || '=' || observation_id || ':' || bound
                       || effects_json)
                     FROM account_observation_event",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(keys, "recent=aco-recent:1[\"fence\"]");
                // Enforcement is back on for serving, and now ignores the
                // history logs: an admission naming a decision that was never
                // recorded is accepted; one naming an unknown pin is refused.
                c.execute_batch(&admission_insert("adm-2", "dec-gone", "pin-1"))?;
                assert!(
                    c.execute_batch(&admission_insert("adm-3", "dec-1", "pin-missing"))
                        .is_err(),
                    "the pin foreign key must still hold"
                );
                Ok(())
            })
            .unwrap();
        drop(migrated);
        std::fs::remove_dir_all(scratch).unwrap();
    }

    /// Migration 58 checks its own result: a store whose admission names a
    /// pin that does not exist fails the foreign-key check, migration 58
    /// rolls itself back, and the store stays at migration 57 with its tables
    /// and their foreign keys as they were before that rebuild.
    #[test]
    fn migration_058_refuses_a_store_with_a_dangling_pin_reference() {
        let scratch = prefrontal_test_support::scratch_root().join(format!(
            "routing-history-foreign-keys-dangling-{}-{}",
            std::process::id(),
            short_id()
        ));
        std::fs::create_dir_all(&scratch).unwrap();
        let descriptor = dev_descriptor(
            "routing-history-foreign-keys-dangling",
            scratch.join("store.db").to_string_lossy(),
        );
        let now = now_ms();
        let legacy = open_sqlite(&descriptor).unwrap();
        legacy
            .migrate(ROUTE_STATE_NAMESPACE, &route_state_migrations()[..55])
            .unwrap();
        legacy
            .with_conn(|c| {
                populate_account_tables(c, now)?;
                c.pragma_update(None, "foreign_keys", "OFF")?;
                c.execute_batch(&admission_insert("adm-dangling", "dec-1", "pin-missing"))?;
                c.pragma_update(None, "foreign_keys", "ON")
            })
            .unwrap();
        drop(legacy);

        let refused = SqliteRouteStateStore::open(&descriptor)
            .err()
            .expect("open refused");
        assert!(refused.0.contains("migration 58"), "{}", refused.0);
        let connection = Connection::open(scratch.join("store.db")).unwrap();
        assert_eq!(route_state_version(&connection).unwrap(), 57);
        assert_eq!(history_foreign_keys(&connection).len(), 6);
        let admissions: i64 = connection
            .query_row("SELECT COUNT(*) FROM account_admission", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(admissions, 2);
        drop(connection);
        std::fs::remove_dir_all(scratch).unwrap();
    }

    /// Reproduces the route.select stall: a selection's head-history read runs
    /// while another thread keeps recording decisions, both through the store's
    /// single connection, the way concurrent route.select requests share it.
    /// Prints the reader's latency and the writer's worst wait for the ledger
    /// query shape (before migration 054) and for the narrow copy (after).
    /// `PREFRONTAL_ROUTING_STALL_ROWS` sets the ledger size (default 60,000).
    #[test]
    #[ignore = "prints a local latency comparison of the head-history read under a concurrent writer"]
    fn head_failure_history_under_a_concurrent_writer_benchmark() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        let rows: i64 = std::env::var("PREFRONTAL_ROUTING_STALL_ROWS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(60_000);
        let (path, store) = observed_speed_test_store("head-failure-stall");
        let store = Arc::new(store);
        let created = HEAD_NOW - 60 * 60_000;
        store
            .store
            .with_conn(|c| {
                let tx = c.unchecked_transaction()?;
                for index in 0..rows {
                    let id = format!("ledger-{index}");
                    // Score payloads of a few kilobytes, like real selections.
                    insert_ledger_row(
                        &tx,
                        &id,
                        Some(&format!("head-{}", index % 200)),
                        created,
                        6_000,
                    )?;
                    if index % 10 == 0 {
                        fail_ledger_row(&tx, &id, HEAD_NOW - 60_000, Some("refused"))?;
                    }
                }
                tx.commit()
            })
            .unwrap();

        let measure = |label: &str, read: &dyn Fn() -> Vec<ParentFailedDecision>| {
            let stop = Arc::new(AtomicBool::new(false));
            let writer = {
                let store = Arc::clone(&store);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut worst = Duration::ZERO;
                    let mut writes = 0_u64;
                    while !stop.load(Ordering::Relaxed) {
                        let input = DecisionInput {
                            parent_session_id: Some("writer-head".into()),
                            target_agent: "mason".into(),
                            requirements_json: "{}".into(),
                            score_json: "{}".into(),
                            rejected_json: "[]".into(),
                            ..DecisionInput::default()
                        };
                        let started = Instant::now();
                        store
                            .record_decision_with_selection(&input, None, None)
                            .unwrap();
                        worst = worst.max(started.elapsed());
                        writes += 1;
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    (worst, writes)
                })
            };
            let mut reads = Vec::new();
            for _ in 0..10 {
                let started = Instant::now();
                assert!(!read().is_empty());
                reads.push(started.elapsed());
                std::thread::sleep(Duration::from_millis(5));
            }
            stop.store(true, Ordering::Relaxed);
            let (writer_worst, writes) = writer.join().unwrap();
            let total: Duration = reads.iter().sum();
            println!(
                "head_failure_stall rows={rows} shape={label} read_avg_ms={:.3} read_max_ms={:.3} writer_max_wait_ms={:.3} writes={writes}",
                total.as_secs_f64() * 1_000.0 / reads.len() as f64,
                reads.iter().max().unwrap().as_secs_f64() * 1_000.0,
                writer_worst.as_secs_f64() * 1_000.0,
            );
        };
        let since = HEAD_NOW - 60 * 60_000;
        measure("ledger", &|| {
            store
                .store
                .with_conn(|c| ledger_parent_failures(c, "head-10", since))
                .unwrap()
        });
        measure("copy", &|| {
            store
                .recent_failed_decisions_for_parent("head-10", since)
                .unwrap()
        });

        drop(store);
        remove_observed_speed_test_store(&path);
    }

    fn fingerprint_test_store(
        scratch: &prefrontal_test_support::ScratchDir,
        label: &str,
    ) -> SqliteRouteStateStore {
        SqliteRouteStateStore::open(&dev_descriptor(
            label,
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap()
    }

    /// The fingerprint hashed from `model_facts` as it is now, bypassing the cache.
    fn fresh_catalog_fingerprint(store: &SqliteRouteStateStore) -> String {
        store
            .store
            .with_conn(calculate_catalog_fingerprint)
            .unwrap()
    }

    /// Record a decision and read back the fingerprint its ledger row carries.
    fn stamped_fingerprint(store: &SqliteRouteStateStore, task_id: &str) -> String {
        let id = store
            .record_decision(&DecisionInput {
                task_id: Some(task_id.to_string()),
                target_agent: "mason".into(),
                requirements_json: "{}".into(),
                score_json: "{}".into(),
                rejected_json: "[]".into(),
                anchor_version: 2,
                ..Default::default()
            })
            .unwrap();
        store
            .store
            .with_conn(|c| {
                c.query_row(
                    "SELECT catalog_fingerprint FROM model_route_decisions WHERE id = ?1",
                    [&id],
                    |row| row.get(0),
                )
            })
            .unwrap()
    }

    /// After a catalog write: the served and stamped fingerprints equal a fresh
    /// hash of the catalog, both before and after the value is cached, and the
    /// write produced a fingerprint not seen before.
    fn assert_fingerprint_follows(
        store: &SqliteRouteStateStore,
        label: &str,
        seen: &mut Vec<String>,
    ) {
        let fresh = fresh_catalog_fingerprint(store);
        assert_eq!(
            stamped_fingerprint(store, label),
            fresh,
            "{label}: first decision"
        );
        assert_eq!(store.catalog_fingerprint().unwrap(), fresh, "{label}: read");
        assert_eq!(
            stamped_fingerprint(store, &format!("{label}-again")),
            fresh,
            "{label}: decision served from the cache"
        );
        assert!(
            !seen.contains(&fresh),
            "{label}: the catalog write must change the fingerprint"
        );
        seen.push(fresh);
    }

    #[test]
    fn cached_catalog_fingerprint_equals_a_fresh_hash_after_every_kind_of_catalog_write() {
        let scratch = prefrontal_test_support::ScratchDir::new("catalog-fingerprint-cache");
        let store = fingerprint_test_store(&scratch, "catalog-fingerprint-cache");
        let mut seen = Vec::new();
        assert_fingerprint_follows(&store, "seeded catalog", &mut seen);

        let mut fact = store.get_facts().unwrap().remove(0);
        let route_key = prefrontal_routing_core::make_route_key(
            &fact.provider_id,
            &fact.model_id,
            fact.variant.as_deref(),
        );
        fact.scores.elo += 1.0;
        store.upsert_fact(&fact, FactSource::Override).unwrap();
        assert_fingerprint_follows(&store, "operator upsert", &mut seen);

        store
            .apply_fusiform_catalog(
                &[FusiformFactUpdate {
                    auth_methods: None,
                    provider_id: fact.provider_id.clone(),
                    model_id: "fingerprint-cache-probe".to_string(),
                    present: true,
                    context_window: Some(64_000.0),
                    max_output_tokens: Some(4_096),
                    cost_tier: Some(1.0),
                    cost_per_mtok_out: Some(1.0),
                    input_rate: None,
                    cache_read_rate: None,
                    cache_write_rate: None,
                    reasoning_rate: None,
                    supports_vision: Some(false),
                    supports_tool_use: Some(true),
                    reasoning_efforts: None,
                    canonical_id: None,
                    upstream_status: None,
                }],
                FusiformCatalogCursor {
                    catalog_version: 3,
                    resolved_at_ms: 300,
                },
            )
            .unwrap();
        assert_fingerprint_follows(&store, "fusiform apply", &mut seen);

        // Plain SQL on the store's own connection, which no Rust writer method
        // sees: only the change tracking on the connection can notice it.
        store
            .store
            .with_conn(|c| {
                c.execute(
                    "UPDATE model_facts SET excluded = 1 WHERE route_key = ?1",
                    [&route_key],
                )
            })
            .unwrap();
        assert_fingerprint_follows(&store, "plain SQL update", &mut seen);

        store
            .delete_fact(&prefrontal_routing_core::make_route_key(
                &fact.provider_id,
                "fingerprint-cache-probe",
                None,
            ))
            .unwrap();
        assert_fingerprint_follows(&store, "fact delete", &mut seen);

        // A commit from a different connection to the same database file.
        Connection::open(scratch.path().join("store.db"))
            .unwrap()
            .execute(
                "UPDATE model_facts SET cost_per_mtok_out = 987.0 WHERE route_key = ?1",
                [&route_key],
            )
            .unwrap();
        assert_fingerprint_follows(&store, "other connection", &mut seen);
    }

    #[test]
    fn a_fingerprint_read_inside_a_rolled_back_catalog_write_is_never_reused() {
        let scratch = prefrontal_test_support::ScratchDir::new("catalog-fingerprint-rollback");
        let store = fingerprint_test_store(&scratch, "catalog-fingerprint-rollback");
        let committed = store.catalog_fingerprint().unwrap();
        let fact = store.get_facts().unwrap().remove(0);
        let route_key = prefrontal_routing_core::make_route_key(
            &fact.provider_id,
            &fact.model_id,
            fact.variant.as_deref(),
        );

        let uncommitted = store
            .store
            .with_conn(|c| {
                let tx = c.unchecked_transaction()?;
                tx.execute(
                    "UPDATE model_facts SET excluded = 1 - excluded WHERE route_key = ?1",
                    [&route_key],
                )?;
                let inside = store.current_catalog_fingerprint(&tx)?;
                assert_eq!(inside, calculate_catalog_fingerprint(&tx)?);
                tx.rollback()?;
                Ok(inside)
            })
            .unwrap();
        assert_ne!(uncommitted, committed);
        assert_eq!(store.catalog_fingerprint().unwrap(), committed);

        // The rollback also undid the write count, so this single committed
        // write brings the count back to what the rolled-back read saw. The
        // value must come from the committed rows, not from that read.
        store
            .store
            .with_conn(|c| {
                c.execute(
                    "UPDATE model_facts SET cost_per_mtok_out = 123.0 WHERE route_key = ?1",
                    [&route_key],
                )
            })
            .unwrap();
        let fresh = fresh_catalog_fingerprint(&store);
        assert_ne!(fresh, uncommitted);
        assert_eq!(store.catalog_fingerprint().unwrap(), fresh);
        assert_eq!(stamped_fingerprint(&store, "after-rollback"), fresh);
    }

    fn cached_key(store: &SqliteRouteStateStore) -> Option<CatalogFingerprintKey> {
        store
            .catalog_fingerprint
            .lock()
            .unwrap()
            .as_ref()
            .map(|cached| cached.key)
    }

    fn facts_write_count(store: &SqliteRouteStateStore) -> i64 {
        store
            .store
            .with_conn(|c| {
                c.query_row(
                    "SELECT writes FROM temp.catalog_facts_writes WHERE singleton = 1",
                    [],
                    |row| row.get(0),
                )
            })
            .unwrap()
    }

    fn set_price(store: &SqliteRouteStateStore, route_key: &str, price: f64) {
        store
            .store
            .with_conn(|c| {
                c.execute(
                    "UPDATE model_facts SET cost_per_mtok_out = ?2 WHERE route_key = ?1",
                    params![route_key, price],
                )
            })
            .unwrap();
    }

    fn first_route_key(store: &SqliteRouteStateStore) -> String {
        let fact = store.get_facts().unwrap().remove(0);
        prefrontal_routing_core::make_route_key(
            &fact.provider_id,
            &fact.model_id,
            fact.variant.as_deref(),
        )
    }

    #[test]
    fn a_missing_change_counter_falls_back_to_a_fresh_fingerprint_and_tracking_returns() {
        let scratch = prefrontal_test_support::ScratchDir::new("catalog-fingerprint-lost-counter");
        let store = fingerprint_test_store(&scratch, "catalog-fingerprint-lost-counter");
        let route_key = first_route_key(&store);
        store.catalog_fingerprint().unwrap();
        assert!(cached_key(&store).is_some());

        store
            .store
            .with_conn(|c| c.execute_batch("DROP TABLE temp.catalog_facts_writes"))
            .unwrap();

        // The decision still records, stamped with a fresh hash, and that
        // value is not cached because the tracking was missing when it ran.
        let fresh = fresh_catalog_fingerprint(&store);
        assert_eq!(stamped_fingerprint(&store, "lost-counter"), fresh);
        assert_eq!(cached_key(&store), None);

        // The tracking is back: a later read is cached again, and a catalog
        // write through plain SQL is both accepted and noticed.
        assert_eq!(store.catalog_fingerprint().unwrap(), fresh);
        assert!(cached_key(&store).is_some());
        set_price(&store, &route_key, 4_321.0);
        let after_write = fresh_catalog_fingerprint(&store);
        assert_ne!(after_write, fresh);
        assert_eq!(store.catalog_fingerprint().unwrap(), after_write);
        assert_eq!(stamped_fingerprint(&store, "after-reinstall"), after_write);
    }

    #[test]
    fn a_reinstalled_change_counter_never_serves_a_fingerprint_cached_before_the_loss() {
        let scratch = prefrontal_test_support::ScratchDir::new("catalog-fingerprint-reinstall");
        let store = fingerprint_test_store(&scratch, "catalog-fingerprint-reinstall");
        let route_key = first_route_key(&store);
        let start = facts_write_count(&store);
        for price in [1.0, 2.0, 3.0] {
            set_price(&store, &route_key, price);
        }
        let before_loss = store.catalog_fingerprint().unwrap();
        let writes_before_loss = facts_write_count(&store);
        assert_eq!(writes_before_loss, start + 3);

        // Lose every TEMP object, as a replaced connection would, and change
        // the catalog while nothing is counting.
        store
            .store
            .with_conn(|c| {
                c.execute_batch(
                    "DROP TRIGGER temp.catalog_facts_writes_on_insert;
                     DROP TRIGGER temp.catalog_facts_writes_on_update;
                     DROP TRIGGER temp.catalog_facts_writes_on_delete;
                     DROP TABLE temp.catalog_facts_writes;",
                )
            })
            .unwrap();
        set_price(&store, &route_key, 999.0);
        assert_eq!(
            stamped_fingerprint(&store, "after-loss"),
            fresh_catalog_fingerprint(&store)
        );

        // The recreated counter starts from zero. Bring it back to the count
        // the pre-loss entry was keyed on, over a different catalog: the read
        // must hash these rows, not return the pre-loss value.
        let restarted = facts_write_count(&store);
        for price in 0..(writes_before_loss - restarted) {
            set_price(&store, &route_key, 500.0 + price as f64);
        }
        assert_eq!(facts_write_count(&store), writes_before_loss);
        let fresh = fresh_catalog_fingerprint(&store);
        assert_ne!(fresh, before_loss);
        assert_eq!(store.catalog_fingerprint().unwrap(), fresh);
    }
}
