//! SQLite backend for the route-state store.
//!
//! Implements `striatum-core::RouteStateStore` over the shared
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
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use striatum_core::canonical_scores::resolve_canonical_scores;
use striatum_core::model_facts::{
    ClassVector, Confidence, FamilySource, GrandFamily, ModelFact, ModelPromptDefaults,
    ModelScores, CLASS_VECTOR_TRAILING_WINDOW_DAYS, CLASS_VECTOR_TRAILING_WINDOW_MS,
    ENUMERATED_WORKLOAD_CLASSES,
};
use striatum_core::model_router::ModelRouteStateSnapshot;
use striatum_core::quota::quota_poll::reading_log_retention_cutoff;
use striatum_core::route_state_store::{
    QuotaReadingLogRow, QuotaStaleDisclosure, UsageRecordResult,
};
use striatum_core::{
    derive_provider_key, fact_to_route, failure_backoff_transition, CatalogFactField,
    CooldownState, DecisionInput, DecisionOutcomeUpdate, DecisionRow, FactSource,
    FailureBackoffState, FailureBackoffTransition, ProviderCooldownState, RouteStateResult,
    RouteStateStore, RouteStateStoreError, RouteStateUpdate, RouteWallClass, UsageObservation,
    OBSERVED_SPEED_MIN_OUTPUT_TOKENS, OBSERVED_SPEED_MIN_SAMPLES, OBSERVED_SPEED_SAMPLE_LIMIT,
    OBSERVED_SPEED_WINDOW_MS, USAGE_MIN_DURATION_MS, USAGE_MIN_OUTPUT_TOKENS,
};

mod account_selection;
mod call_counts;
pub use call_counts::{RoutingCallCount, RoutingCallCounts};
mod decision_detail;
pub use decision_detail::{
    compact_quota_signal_json, compact_score_json, decision_id_hash, in_full_detail_sample,
    DetailStripBatch,
};
mod report_delivery;
#[cfg(test)]
mod test_exe;
pub use report_delivery::{V1ReportEffects, V1ReportOutcome, V1ReportUsage};
#[cfg(test)]
mod speed_sample_tests;
// The backup table classification is a test oracle: the store's integration
// tests, the engram catalog test and the restore rehearsal example check the
// shipped catalog against it, and no production path reads it.
#[cfg(feature = "test-support")]
pub mod backup;
mod diagnostics;
pub mod job_timing;
mod operator_quality;
mod prune_facts;
pub use operator_quality::{OperatorQuality, UnscoredAdmissibleModel};
mod block_state;
#[cfg(feature = "test-support")]
pub use block_state::{count_recheck_rows, recheck_row_visits};
mod block_storage;
mod catalog_efforts;
mod engine_measurements;
mod fence_rechecks;
#[cfg(any(test, feature = "test-support"))]
#[path = "frozen_block_data/tests.rs"]
mod frozen_block_data;
pub use catalog_efforts::{CatalogEfforts, CatalogWorkload, FusiformFactUpdate};
mod history;
#[cfg(any(test, feature = "test-support"))]
pub use fence_rechecks::parity::verify_split_evaluation;
#[cfg(any(test, feature = "test-support"))]
pub use history::{open_offline as open_offline_split_copy, run_split_for_test};
mod route_snapshot;
mod sql_real;
mod store_open;
#[cfg(any(test, feature = "test-support"))]
#[path = "writer_entry_trace/tests.rs"]
pub mod writer_entry_trace;
#[cfg(any(test, feature = "test-support"))]
pub use block_state::reviewed_exempt_reads;
pub use block_state::{
    block_read_backs, block_reconcile_reloads, BlockRows, BlockState, DryRunRepair,
    ProbeAuthorization, SuccessPool, BLOCK_TABLES,
};
#[cfg(any(test, feature = "test-support"))]
pub use frozen_block_data::FrozenBlockData;
#[cfg(any(test, feature = "test-support"))]
mod block_parity_fixture;
#[cfg(any(test, feature = "test-support"))]
pub mod group_reads;
mod key_pool;
pub use fence_rechecks::{
    BlockParity, FenceRecheck, LoadedBlocks, QuotaSuccessScope, ReleasedAdmissionProbes, ViewOrigin,
};
#[cfg(test)]
mod continuation_migration_tests;
#[cfg(test)]
mod last_observation_tests;
mod reasoning_catalog;
mod retention;
mod routing_v1;
mod task_usage;
mod transactions;
use transactions::write_transaction;
#[cfg(test)]
mod transaction_contention_tests;

#[cfg(any(test, feature = "test-support"))]
pub mod replay;

pub use diagnostics::{
    DiagnosticAccountBlock, DiagnosticAccountHold, DiagnosticDecision, DiagnosticKeyWall,
    DiagnosticRouteWall,
};
pub use engine_measurements::{EngineEvaluation, EngineSpeedSample, ENGINE_SPEED_SAMPLE_LIMIT};
pub use key_pool::{PoolAcquire, PoolAcquireOutcome, PoolLease, PoolWallKind, RecoveredOrigin};
pub use reasoning_catalog::{
    read_counts_offline, read_reasoning_catalog_offline, PublishedEfforts, ReasoningCatalog,
};
pub use retention::{
    AffinityRetentionBatch, AffinityRetentionKey, AffinityRetentionPlan, AffinityRetentionPosition,
    V1DecisionRetentionBatch,
};
pub use routing_v1::{V1Decision, V1Recorded, V1SelectionAdmission};
pub use task_usage::{ClosedTaskUsage, ProviderTokenReporting, TaskTokenTotals};

#[cfg(any(test, feature = "test-support"))]
pub use account_selection::AccountServingCounts;
pub use account_selection::ContinuationSourceRow;
pub use account_selection::{
    keyed_exception_reads, note_keyed_exception, AccountAdmissionRow, AccountAffinityRow,
    AccountDecisionRecord, AccountDemandRow, AccountEligibilityFact, AccountExclusionRow,
    AccountFenceRow, AccountHoldRow, AccountObservationRow, AccountPinRow, AccountPoolFenceRow,
    AccountRouteFence, AccountSizeRefusal, AccountTx, AdmissionGroupRows, AdmissionScanRow,
    AdmissionStateRow, ContinuationStateRow, CredentialProbeClaim, LatestAdmissionRow,
    PrunedAdmission, PrunedContinuation, ReservationRef, SignedOutObservationRow,
    ACCOUNT_DEMAND_SQL, DECISION_MODE_SERVE, KEYED_EXCEPTION, OBSERVATION_BACKOFFS_SQL,
    SIGNED_OUT_OBSERVATIONS_SQL,
};

mod catalog;
mod decision_ledger;
mod route_state;
mod routing_checkpoints;
mod usage_measurements;

use catalog::CachedCatalogFingerprint;
pub use catalog::{
    CanonicalGapRow, CatalogFact, CatalogFactsSnapshot, CatalogFingerprintKey,
    FusiformCatalogCursor, UnscoredRow,
};
pub use connection_store::store_now_ms;
use connection_store::{err, now_ms};
#[cfg(test)]
use decision_ledger::short_id;
pub use decision_ledger::{
    new_decision_id, DecisionExplainRow, DecisionListFilter, DecisionListRow, ParentFailedDecision,
    RECENT_PARENT_FAILURES_SQL,
};
pub use route_state::ClearedRouteCooldowns;
pub use routing_checkpoints::{
    PersistedAccountReading, PersistedProviderCompleteness, PersistedQuotaSnapshot,
    PersistedVaultRoster,
};
use usage_measurements::CachedClassVector;
pub use usage_measurements::{OBSERVED_SPEED_SAMPLES_SQL, WORKLOAD_CLASS_USAGE_SAMPLES_SQL};

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
const MIGRATION_071: &str = include_str!("../migrations/071_service_tiers.sql");

fn route_state_migrations() -> [Migration; 103] {
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
        Migration {
            version: 71,
            statements: MIGRATION_071,
        },
        Migration {
            version: 72,
            statements: include_str!("../migrations/072_account_continue.sql"),
        },
        Migration {
            version: 73,
            statements: include_str!("../migrations/073_speed_samples.sql"),
        },
        Migration {
            version: 74,
            statements: include_str!("../migrations/074_route_fence_report_times.sql"),
        },
        Migration {
            version: 75,
            statements: include_str!("../migrations/075_account_service_tiers.sql"),
        },
        Migration {
            version: 76,
            statements: include_str!("../migrations/076_transient_account_route_fence.sql"),
        },
        Migration {
            version: 77,
            statements: include_str!("../migrations/077_selection_snapshot.sql"),
        },
        Migration {
            version: 78,
            statements: include_str!("../migrations/078_report_runs_and_call_counts.sql"),
        },
        Migration {
            version: 79,
            statements: include_str!("../migrations/079_observation_backoff_index.sql"),
        },
        Migration {
            version: 80,
            statements: include_str!("../migrations/080_v1_panels.sql"),
        },
        Migration {
            version: 81,
            statements: include_str!("../migrations/081_engine_measurements.sql"),
        },
        Migration {
            version: 82,
            statements: include_str!("../migrations/082_fence_rechecks.sql"),
        },
        Migration {
            version: 83,
            statements: include_str!("../migrations/083_model_classification.sql"),
        },
        Migration {
            version: 84,
            statements: include_str!("../migrations/084_history_retention.sql"),
        },
        Migration {
            version: 85,
            statements: include_str!("../migrations/085_observation_principal_event.sql"),
        },
        Migration {
            version: 86,
            statements: include_str!("../migrations/086_operator_decommissioned.sql"),
        },
        Migration {
            version: 87,
            statements: include_str!("../migrations/087_login_methods_coverage.sql"),
        },
        Migration {
            version: 88,
            statements: include_str!("../migrations/088_continuation_whole_offers.sql"),
        },
        Migration {
            version: 89,
            statements: include_str!("../migrations/089_admission_last_observed.sql"),
        },
        Migration {
            version: 90,
            statements: include_str!("../migrations/090_account_affinity_by_pin.sql"),
        },
        Migration {
            version: 91,
            statements: include_str!("../migrations/091_admission_conversation_lookup.sql"),
        },
        Migration {
            version: 92,
            statements: include_str!("../migrations/092_triggers_without_replace.sql"),
        },
        Migration {
            version: 93,
            statements: include_str!("../migrations/093_pin_retention.sql"),
        },
        Migration {
            version: 94,
            statements: include_str!("../migrations/094_consumer_names.sql"),
        },
        Migration {
            version: 95,
            statements: include_str!("../migrations/095_observation_signed_out_index.sql"),
        },
        Migration {
            version: 96,
            statements: block_storage::MIGRATION,
        },
        Migration {
            version: 97,
            statements: include_str!("../migrations/097_admission_verdict_index.sql"),
        },
        history::STATE_MIGRATION,
        Migration {
            version: 99,
            statements: include_str!("../migrations/099_catalog_efforts.sql"),
        },
        Migration {
            version: 100,
            statements: include_str!("../migrations/100_clear_curated_efforts.sql"),
        },
        Migration {
            version: 101,
            statements: include_str!("../migrations/101_bounded_writer_lookups.sql"),
        },
        Migration {
            version: 102,
            statements: include_str!("../migrations/102_model_mode_of.sql"),
        },
        Migration {
            version: 103,
            statements: include_str!("../migrations/103_retention_lookups.sql"),
        },
        Migration {
            version: 104,
            statements: include_str!("../migrations/104_remove_aa_inferred_provenance.sql"),
        },
    ]
}

fn migrate_route_state(store: &SqliteStore, migrations: &[Migration]) -> RouteStateResult<()> {
    let block_storage = migrations.iter().any(|migration| migration.version == 96);
    if block_storage {
        store
            .with_conn(block_storage::install_migration_function)
            .map_err(err)?;
    }
    // Migration 58 rebuilds account_pin while admissions reference it. SQLite
    // ignores foreign_keys changes inside a transaction, so suspend enforcement
    // around the shared runner and restore it before this store can serve.
    let migration = (|| {
        store
            .with_conn(|conn| conn.pragma_update(None, "foreign_keys", "OFF"))
            .map_err(err)?;
        store
            .migrate(ROUTE_STATE_NAMESPACE, migrations)
            .map_err(err)
    })();
    // Attempt both cleanups even if migration or the other cleanup failed.
    store
        .with_conn(|conn| {
            let foreign_keys = conn.pragma_update(None, "foreign_keys", "ON");
            let function = if block_storage {
                conn.remove_function("block_migration_quarantined", 3)
            } else {
                Ok(())
            };
            foreign_keys?;
            function?;
            let enabled: i64 = conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
            if enabled == 1 {
                Ok(())
            } else {
                Err(rusqlite::Error::InvalidQuery)
            }
        })
        .map_err(|error| err(format!("cleaning up route-state migration: {error}")))?;
    let outcome = migration?;
    // A previous binary must refuse a newer schema, not silently serve tables
    // that its shorter migration chain cannot query.
    if outcome.store_ahead() {
        return Err(err(format!(
            "store_ahead: namespace {ROUTE_STATE_NAMESPACE} is at version {} but this binary's chain ends at {}; \
             this binary would serve a schema it cannot query (binary rollback or reset numbering); \
             deploy a binary carrying version {} or newer",
             outcome.recorded,outcome.chain_max,outcome.recorded
        )));
    }
    Ok(())
}

/// Retain at most 16 MiB when the writer reuses a fully checkpointed WAL.
pub const WAL_SIZE_CAP_BYTES: u64 = 16 * 1024 * 1024;
/// SQLite negative cache sizes are KiB, allowing up to 8 MiB per pooled reader.
pub const READER_CACHE_SIZE_KIB: i64 = -8192;
/// Maintenance connection busy-timeout bound; passive checkpoints never wait.
pub const WAL_TRUNCATE_WAIT_BOUND: std::time::Duration = std::time::Duration::from_millis(10);

mod route_write_queries;
pub(crate) use route_write_queries::*;

fn validate_family_spellings(store: &SqliteStore) -> RouteStateResult<()> {
    let unknown = store
        .with_conn(|connection: &Connection| {
            let mut statement = connection.prepare(
                "SELECT route_key, family FROM model_facts AS candidate
                 WHERE family IS NOT NULL AND NOT EXISTS (
                   SELECT 1 FROM model_facts AS authority
                   WHERE authority.provider_id = candidate.provider_id
                     AND authority.model_id = candidate.model_id
                     AND authority.facts_source = 'fusiform'
                 ) ORDER BY route_key",
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
    catalog_cache_identity: AtomicU64,
    store: std::sync::Arc<connection_store::ConnectionStore>,
    account_writer: std::sync::OnceLock<account_writer::AccountWriter>,
    account_state: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn AccountStateBridge>>>,
    /// Single-flight retries of a failed account state load: held while one
    /// runs, with the last one's error.
    account_state_retry: Mutex<Option<String>>,
    /// How many single-flight retries have finished. Read without the lock,
    /// so a caller can tell that a retry finished while it waited.
    account_state_retries: AtomicU64,
    maintenance: Option<Mutex<Connection>>,
    history: Option<history::History>,
    call_counts: call_counts::RoutingCallBuffer,
    /// Class vectors are means over every complete token profile in a seven-day
    /// window, recomputed from the whole population on every route selection
    /// before this cache existed (Loop R184: 1.9 million VM steps per selection
    /// at 100k profiles). A computed vector is reused until a usage write
    /// changes that class's population (its entry in `class_generations`
    /// moves) or the window's minute boundary passes, so selection pays the
    /// scan at most once a minute per class while that class is quiet and never
    /// serves a vector that predates a write to it. Usage of another class, or
    /// a write that only fills columns outside the token profile, leaves the
    /// cached vector in place.
    class_vectors: Mutex<HashMap<String, CachedClassVector>>,
    /// One generation per entry of `ENUMERATED_WORKLOAD_CLASSES`, in order.
    class_generations: [AtomicU64; ENUMERATED_WORKLOAD_CLASSES.len()],
    /// Same-model score disagreements already logged by this process, so a
    /// read on every selection does not repeat the same warning.
    reported_score_conflicts: Mutex<HashSet<String>>,
    /// The catalog fingerprint stamped on each new decision row, kept with the
    /// catalog state it was computed from (see `CatalogFingerprintKey`). Before
    /// this cache, every decision write read and hashed every `model_facts` row.
    /// `None` until a fingerprint is computed while the change tracking is in
    /// place, and reset whenever that tracking has to be installed again.
    catalog_fingerprint: Mutex<Option<CachedCatalogFingerprint>>,
    /// Resolved capabilities and operator grades share the catalog fingerprint's
    /// write tracking. Unrelated writes on this connection do not invalidate it.
    resolved_facts: Mutex<
        Option<(
            CatalogFingerprintKey,
            Vec<(ModelFact, bool)>,
            Arc<BTreeMap<String, CatalogEfforts>>,
        )>,
    >,
    selection_catalog: Mutex<Option<Arc<CatalogFactsSnapshot>>>,
    /// Inventory metadata shares the catalog-change key and keeps withdrawn
    /// rows, but does not apply operator grades used only for selection.
    catalog_facts: Mutex<Option<(CatalogFingerprintKey, Vec<CatalogFact>)>>,
    #[cfg(any(test, feature = "test-support"))]
    account_write_transactions: std::sync::Arc<AtomicU64>,
}

mod account_writer;
mod connection_store;
pub use account_writer::{AccountCommandError, AccountCommandKind, AccountReply, STATE_DIVERGED};
mod account_state;
pub use account_state::{AccountStateBridge, AccountWorkingState, ACCOUNT_STATE_LOAD_PENDING};
#[cfg(any(test, feature = "test-support"))]
pub use connection_store::record_stage_timing_for_test;
pub use connection_store::{scope_stage_timing, StoreStageTiming};

#[cfg(test)]
mod consumer_names_tests;

#[cfg(test)]
#[path = "legacy_fixture_inputs/tests.rs"]
mod legacy_fixture_inputs;

impl SqliteRouteStateStore {
    /// Open from a resolved descriptor, applying migrations. The single-writer
    /// lease location is derived by the lib from the descriptor's db path (one
    /// lease per database, by construction), so there is no lease-dir to pass.
    pub fn open(descriptor: &StorageDescriptor) -> RouteStateResult<Self> {
        Self::open_with_migrations(descriptor, &route_state_migrations())
    }

    /// Run WAL maintenance on a dedicated connection, never on the request writer.
    /// A busy reader leaves frames for the next tick instead of delaying requests.
    pub fn checkpoint_wal(&self) -> RouteStateResult<()> {
        let Some(maintenance) = &self.maintenance else {
            return Ok(());
        };
        let conn = maintenance.lock().unwrap_or_else(|p| p.into_inner());
        // Resetting the WAL on another connection changes the writer's
        // data_version even when no account rows changed. Let the writer reset
        // its own WAL on the next commit instead, using journal_size_limit.
        conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()))
            .map_err(err)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn with_writer_for_test<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> RouteStateResult<T> {
        let _seeding = crate::block_state::Seeding::begin();
        self.store.with_conn(f).map_err(err)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn with_reader_for_test<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> RouteStateResult<T> {
        self.store.with_read(f).map_err(err)
    }

    /// The SQL route-state snapshot with its probe clocks evaluated at
    /// `recheck_now`, to compare with the account state's at the same times.
    #[cfg(any(test, feature = "test-support"))]
    pub fn build_snapshot_at_for_test(
        &self,
        now: f64,
        recheck_now: i64,
    ) -> RouteStateResult<ModelRouteStateSnapshot> {
        self.with_reader_for_test(|conn| Ok(route_snapshot::read_snapshot(conn, now, recheck_now)))?
    }

    /// Replace writer and maintenance waits with a test-controlled lock handshake.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_busy_handler_for_test(
        &self,
        handler: Option<fn(i32) -> bool>,
    ) -> RouteStateResult<()> {
        if let Some(maintenance) = &self.maintenance {
            maintenance
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .busy_handler(handler)
                .map_err(err)?;
        }
        self.store
            .with_conn(|connection| connection.busy_handler(handler))
            .map_err(err)
    }

    /// Newest registered migration, for upgrade-test version expectations.
    #[cfg(any(test, feature = "test-support"))]
    pub fn latest_migration_version_for_test() -> u32 {
        route_state_migrations().last().unwrap().version
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

    /// The single-writer fence epoch for this open (for diagnostics / future
    /// cloud-backend CAS).
    pub fn epoch(&self) -> u64 {
        self.store.epoch()
    }
}
