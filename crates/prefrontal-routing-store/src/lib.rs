//! SQLite backend for the route-state store.
//!
//! Implements `prefrontal-routing-core::RouteStateStore` over the shared
//! `cortexkit-store` lib: the lib owns the single-writer lease, the durable
//! pragmas, and the namespace-keyed migration runner; this crate owns the
//! `route_state` migration set (DDL + the 33-fact seed) and the domain queries.
//! `-core` stays rusqlite-free; the backend lives here (postgres sibling later).
//!
//! Ported from the Alfonso TS plugin's `ModelRouteStore`
//! (`src/features/model-routing/route-state-store.ts`), facts domain first.

use cortexkit_store::{open_sqlite, Migration, SqliteStore};
use cortexkit_store_types::{Isolation, StorageBackend, StorageDescriptor};
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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

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

/// Stored rejection detail stays useful without growing with every catalog route.
const MAX_STORED_REJECTIONS: usize = 12;

/// Fetch one route's bounded observed-speed sample set through the route/time index.
/// The batched lookup prepares this statement once and reuses it for every
/// effective catalog key.
pub const OBSERVED_SPEED_SAMPLES_SQL: &str = "SELECT output_tokens, duration_ms
     FROM model_route_usage INDEXED BY model_route_usage_route_observed_idx
     WHERE route_key = ?1
       AND observed_at >= ?2
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

fn route_state_migrations() -> [Migration; 41] {
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
}

/// BLAKE3 over every routing-read `model_facts` column: route/capacity identity,
/// provider/model/variant, family, Elo/EQ/speed, context and output limits, price
/// data, vision/tool capability flags, prompt defaults, confidence, speed multiplier,
/// and exclusion. Adding a routing-read column without extending this row is the
/// defect this hash exists to catch. Query ordering—not insertion ordering—is the
/// generation contract, so a no-op reseed or shuffled input cannot mint a different
/// fingerprint for the same catalog.
///
/// Widening this row is intentionally migration-free even though it changes every
/// fingerprint: comparisons only have meaning within a catalog generation, and a
/// historical NULL fingerprint was never comparable to a live catalog value.
fn calculate_catalog_fingerprint(connection: &Connection) -> rusqlite::Result<String> {
    let mut statement = connection.prepare(
        "SELECT route_key, capacity_key, provider_id, model_id, variant, family,
                json_extract(scores_json, '$.elo') AS elo,
                json_extract(scores_json, '$.eq') AS eq,
                json_extract(scores_json, '$.speed') AS speed,
                context_window, max_output_tokens, cost_tier, cost_per_mtok_out,
                input_rate, cache_read_rate, cache_write_rate, reasoning_rate,
                supports_vision, supports_tool_use, prompt_defaults_json, confidence,
                speed_multiplier, excluded, fusiform_present
         FROM model_facts
         ORDER BY route_key",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(CatalogFingerprintRow {
            route_key: row.get(0)?,
            capacity_key: row.get(1)?,
            provider_id: row.get(2)?,
            model_id: row.get(3)?,
            variant: row.get(4)?,
            family: row.get(5)?,
            elo: row.get(6)?,
            eq: row.get(7)?,
            speed: row.get(8)?,
            context_window: row.get(9)?,
            max_output_tokens: row.get(10)?,
            cost_tier: row.get(11)?,
            cost_per_mtok_out: row.get(12)?,
            input_rate: row.get(13)?,
            cache_read_rate: row.get(14)?,
            cache_write_rate: row.get(15)?,
            reasoning_rate: row.get(16)?,
            supports_vision: row.get::<_, Option<i64>>(17)?.map(|value| value != 0),
            supports_tool_use: row.get::<_, Option<i64>>(18)?.map(|value| value != 0),
            prompt_defaults_json: row.get(19)?,
            confidence: row.get(20)?,
            speed_multiplier: row.get(21)?,
            excluded: row.get::<_, i64>(22)? != 0,
            fusiform_present: row.get::<_, Option<i64>>(23)?.map(|value| value != 0),
        })
    })?;
    let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let bytes = serde_json::to_vec(&rows)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let hex = blake3::hash(&bytes).to_hex().to_string();
    Ok(hex[..16].to_string())
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

    // Runtime walls (backoff, cooldown, quota) are the operator-actionable answer
    // to "why not this route" and they carry the deadline they were judged
    // against, so they stay inline even when an alphabetically earlier catalog
    // tail (unscored, excluded, unavailable rows) would fill the bound first.
    let is_runtime_wall = |entry: &&serde_json::Value| {
        matches!(
            entry.get("reason").and_then(serde_json::Value::as_str),
            Some("backoff_active" | "cooldown_active" | "quota_exhausted")
        )
    };
    let mut prioritized = Vec::with_capacity(rejected.len());
    prioritized.extend(rejected.iter().filter(is_runtime_wall));
    prioritized.extend(rejected.iter().filter(|entry| !is_runtime_wall(entry)));

    let mut reasons = BTreeMap::<String, usize>::new();
    for entry in &prioritized[MAX_STORED_REJECTIONS..] {
        let reason = entry
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        *reasons.entry(reason.to_string()).or_default() += 1;
    }
    let truncated = prioritized.len() - MAX_STORED_REJECTIONS;
    let mut bounded = prioritized[..MAX_STORED_REJECTIONS]
        .iter()
        .map(|entry| (*entry).clone())
        .collect::<Vec<_>>();
    bounded.push(serde_json::json!({
        "truncated": truncated,
        "reasons": reasons,
    }));
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
}

#[derive(Debug, Clone)]
struct CachedClassVector {
    usage_generation: u64,
    window_minute: i64,
    vector: Option<ClassVector>,
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
    pub id: String,
    pub decision_kind: String,
    pub task_id: Option<String>,
    pub parent_session_id: Option<String>,
    pub harness: Option<String>,
    pub project_root: Option<String>,
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

impl SqliteRouteStateStore {
    /// Open from a resolved descriptor, applying migrations. The single-writer
    /// lease location is derived by the lib from the descriptor's db path (one
    /// lease per database, by construction), so there is no lease-dir to pass.
    pub fn open(descriptor: &StorageDescriptor) -> RouteStateResult<Self> {
        let store = open_sqlite(descriptor).map_err(err)?;
        // A store written by a longer chain than this binary carries used to
        // migrate as a silent no-op and then serve a schema the binary cannot
        // query. The route-state chain rebuilds tables and re-keys facts, so an
        // ahead store is refused here, naming both versions.
        let outcome = store
            .migrate(ROUTE_STATE_NAMESPACE, &route_state_migrations())
            .map_err(err)?;
        if outcome.store_ahead() {
            return Err(err(format!(
                "store_ahead: namespace {ROUTE_STATE_NAMESPACE} is at version {} but this binary's chain ends at {}; \
                 this binary would serve a schema it cannot query (binary rollback or reset numbering); \
                 deploy a binary carrying version {} or newer",
                outcome.recorded, outcome.chain_max, outcome.recorded
            )));
        }
        validate_family_spellings(&store)?;
        Ok(Self {
            store,
            class_vectors: Mutex::new(HashMap::new()),
            usage_generation: AtomicU64::new(0),
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
            .with_conn(|connection: &Connection| calculate_catalog_fingerprint(connection))
            .map_err(err)
    }

    /// All durable catalog rows, including each row's routing-exclusion state.
    ///
    /// This is intentionally separate from `RouteStateStore::get_facts`: selection
    /// only needs capabilities, while read-only inventory consumers must not infer
    /// policy state from a missing capability or a filtered result.
    pub fn get_catalog_facts(&self) -> RouteStateResult<Vec<CatalogFact>> {
        self.store
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
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
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

    /// Apply one complete Fusiform snapshot and advance its cursor atomically.
    /// Matching is always the exact `(provider_id, model_id)` pair; variants of
    /// that pair receive the same external facts without changing variant policy.
    pub fn apply_fusiform_catalog(
        &self,
        facts: &[FusiformFactUpdate],
        cursor: FusiformCatalogCursor,
    ) -> RouteStateResult<()> {
        let scores_json = serde_json::to_string(&ModelScores::default()).map_err(err)?;
        self.store
            .with_conn(|connection: &Connection| {
                let tx = connection.unchecked_transaction()?;
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
                    let route_key = prefrontal_routing_core::make_route_key(
                        &fact.provider_id,
                        &fact.model_id,
                        None,
                    );
                    let matching_rows: i64 = tx.query_row(
                        "SELECT COUNT(*) FROM model_facts
                         WHERE provider_id = ?1 AND model_id = ?2",
                        params![fact.provider_id, fact.model_id],
                        |row| row.get(0),
                    )?;
                    if matching_rows == 0 {
                        // A model first seen already withdrawn has nothing to route and
                        // nothing for the operator to place; minting it would only add a
                        // dead row per retired model under every routed provider.
                        if !fact.present {
                            continue;
                        }
                        // First sight of a model. Fusiform serves ~7,500 models across
                        // ~200 providers; a first-sight row is minted only under a
                        // provider the operator already routes (one that has any fact
                        // row), so `route.list_unscored` stays an operator worklist and
                        // the candidate scan is not 7,000 rejections deep. Models under
                        // unknown providers are not stored at all.
                        let known_provider: i64 = tx.query_row(
                            "SELECT COUNT(*) FROM model_facts WHERE provider_id = ?1",
                            params![fact.provider_id],
                            |row| row.get(0),
                        )?;
                        if known_provider == 0 {
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
                               supports_tool_use_source
                             ) VALUES (
                               ?1, ?1, ?2, ?3, NULL, NULL, ?4, ?5, ?6, ?7,
                               ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL, 'unknown',
                               'discovered by Fusiform; awaiting operator score placement',
                               NULL, 0, 'unscored', ?15, NULL, NULL, NULL,
                               'fusiform', ?16, ?17,
                               'fusiform', 'fusiform', 'fusiform', 'fusiform',
                               'fusiform', 'fusiform'
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
                            ],
                        )?;
                        let provider_key = derive_provider_key(&fact.provider_id).ok_or_else(|| {
                            rusqlite::Error::InvalidParameterName(format!(
                                "provider {} has no canonical key",
                                fact.provider_id
                            ))
                        })?;
                        tx.execute(
                            "INSERT OR IGNORE INTO model_route_state (
                               route_key, capacity_key, provider_id, model_id, variant,
                               cooldown_until, quota_exhausted_until, quota_pressure,
                               latency_ema_ms, success_ema, failure_count, quota_error_count,
                               last_error, last_error_at, updated_at
                             )
                             SELECT ?1, ?1, ?2, ?3, NULL,
                                    provider.cooldown_until, provider.quota_exhausted_until,
                                    provider.quota_pressure, NULL, NULL, 0, 0,
                                    provider.last_error, provider.last_error_at, ?5
                             FROM (SELECT ?4 AS provider_key) AS route
                             LEFT JOIN provider_cooldown_state AS provider
                               ON provider.provider_key = route.provider_key",
                            params![
                                route_key,
                                fact.provider_id,
                                fact.model_id,
                                provider_key,
                                now_ms(),
                            ],
                        )?;
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
                               fusiform_present = ?12, updated_at = ?13
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

    /// Replace the AA index cache in one transaction. A failed fetch must not
    /// call this, so the previous table survives (fail-open).
    pub fn replace_aa_index_table(
        &self,
        rows: &[(String, f64)],
        fetched_at: i64,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = c.unchecked_transaction()?;
                tx.execute("DELETE FROM aa_index_cache", [])?;
                for (slug, index) in rows {
                    tx.execute(
                        "INSERT INTO aa_index_cache (aa_slug, aa_index, fetched_at)
                         VALUES (?1, ?2, ?3)",
                        params![slug, index, fetched_at],
                    )?;
                }
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

    /// Latest cached AA intelligence index for an AA slug, plus when it was fetched.
    pub fn get_aa_index(&self, slug: &str) -> RouteStateResult<Option<(f64, i64)>> {
        self.store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT aa_index, fetched_at FROM aa_index_cache WHERE aa_slug = ?1",
                    [slug],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
            })
            .map_err(err)
    }

    /// Every `aa_inferred` row with review-by status relative to `now_ms`.
    pub fn list_inferred_rows(&self, now_ms: i64) -> RouteStateResult<Vec<InferredScoreRow>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT route_key, aa_index,
                            json_extract(scores_json, '$.elo') AS elo,
                            review_by_ms
                     FROM model_facts
                     WHERE source = 'aa_inferred'
                     ORDER BY route_key",
                )?;
                let rows = stmt.query_map([], |row| {
                    let review_by_ms: Option<i64> = row.get(3)?;
                    Ok(InferredScoreRow {
                        route_key: row.get(0)?,
                        aa_index: row.get(1)?,
                        elo: row.get::<_, Option<f64>>(2)?.unwrap_or(0.0),
                        review_by_ms,
                        review_by_passed: review_by_ms.is_some_and(|deadline| deadline <= now_ms),
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
    }

    /// Catalog rows that exist without a score (Fusiform's later target).
    pub fn list_unscored_rows(&self) -> RouteStateResult<Vec<UnscoredRow>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT route_key FROM model_facts
                     WHERE source = 'unscored'
                       AND COALESCE(fusiform_present, 1) = 1
                     ORDER BY route_key",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok(UnscoredRow {
                        route_key: row.get(0)?,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
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
    pub fn recent_failed_decisions_for_parent(
        &self,
        parent_session_id: &str,
        since_ms: i64,
    ) -> RouteStateResult<Vec<ParentFailedDecision>> {
        self.store
            .with_conn(|connection: &Connection| {
                let mut statement = connection.prepare(
                    "SELECT selected_route_key, selected_provider_id, outcome_error, outcome_at
                     FROM model_route_decisions
                     WHERE parent_session_id = ?1
                       AND outcome_at >= ?2
                       AND outcome_error IS NOT NULL
                     ORDER BY outcome_at DESC, id DESC",
                )?;
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
                                outcome_error
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
                                outcome_error
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
    })
}

fn row_to_fact(row: &Row<'_>) -> rusqlite::Result<ModelFact> {
    let scores_json: String = row.get("scores_json")?;
    let scores: ModelScores = serde_json::from_str(&scores_json).unwrap_or(ModelScores {
        elo: 0.0,
        eq: 0.0,
        speed: 0.0,
    });
    let prompt_defaults_json: Option<String> = row.get("prompt_defaults_json")?;
    let prompt_defaults: Option<ModelPromptDefaults> =
        prompt_defaults_json.and_then(|s| serde_json::from_str(&s).ok());
    let confidence = match row.get::<_, String>("confidence")?.as_str() {
        "unknown" => Confidence::Unknown,
        _ => Confidence::Curated,
    };
    let source = FactSource::parse(&row.get::<_, String>("source")?);
    Ok(ModelFact {
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
        let provider_key = derive_provider_key(&fact.provider_id)
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
                 tx.execute(
                     "INSERT INTO model_facts (
                       route_key, capacity_key, provider_id, model_id, variant, family, family_source,
                       scores_json, context_window, max_output_tokens, cost_tier, cost_per_mtok_out,
                         input_rate, cache_read_rate, cache_write_rate, reasoning_rate,
                         supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, speed_multiplier,
                         source, updated_at, aa_index, review_by_ms, eq_source
                        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27)
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
                // The migration trigger covers fresh INSERTs. This idempotent
                // backstop also repairs databases where an older route fact was
                // updated or where a route-state row was absent before migration 15.
                tx.execute(
                    "INSERT OR IGNORE INTO model_route_state (
                       route_key, capacity_key, provider_id, model_id, variant,
                       cooldown_until, quota_exhausted_until, quota_pressure,
                       latency_ema_ms, success_ema, failure_count, quota_error_count,
                       last_error, last_error_at, updated_at
                     )
                     SELECT ?1, ?2, ?3, ?4, ?5,
                            provider.cooldown_until, provider.quota_exhausted_until,
                            provider.quota_pressure, NULL, NULL, 0, 0,
                            provider.last_error, provider.last_error_at, ?7
                     FROM (SELECT ?6 AS provider_key) AS route
                     LEFT JOIN provider_cooldown_state AS provider
                       ON provider.provider_key = route.provider_key",
                    params![
                        route.route_key,
                        route.capacity_key,
                        fact.provider_id,
                        fact.model_id,
                        fact.variant,
                        provider_key,
                        now_ms(),
                    ],
                )?;
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
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "SELECT * FROM model_facts
                     WHERE COALESCE(fusiform_present, 1) = 1",
                )?;
                let rows = stmt.query_map([], row_to_fact)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(err)
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
                            wall_class
                     FROM provider_cooldown_state WHERE provider_key = ?1",
                    [&provider_key],
                    |row| {
                        Ok(ProviderCooldownState {
                            provider_key: row.get(0)?,
                            cooldown_until: row.get(1)?,
                            quota_exhausted_until: row.get(2)?,
                            quota_pressure: row.get(3)?,
                            wall_class: row_wall_class(row, 4)?,
                        })
                    },
                )
                .optional()
            })
            .map_err(err)
    }

    fn record_provider_cooldown(
        &self,
        provider_key: &str,
        route_key: &str,
        capacity_key: &str,
        update: &RouteStateUpdate,
    ) -> RouteStateResult<()> {
        let provider_key = derive_provider_key(provider_key)
            .ok_or_else(|| err("provider cooldown write has no canonical provider key"))?
            .to_owned();
        let (route_provider, model_id, variant) = split_route_key(route_key);
        let route_provider = derive_provider_key(&route_provider)
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
                let existing = existing.unwrap_or_default();
                let cooldown_until = max_defined(existing.cooldown_until, update.cooldown_until);
                let quota_exhausted_until =
                    max_defined(existing.quota_exhausted_until, update.quota_exhausted_until);
                let quota_pressure = update.quota_pressure.or(existing.quota_pressure);
                let wall_class = update.wall_class.or(existing.wall_class);
                let (last_error, last_error_at) = match &update.last_error {
                    Some(error) => (Some(error.clone()), Some(now_ms())),
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
        let (provider_id, model_id, variant) = split_route_key(route_key);
        let provider_key = derive_provider_key(&provider_id)
            .ok_or_else(|| err("route state update has no canonical provider key"))?
            .to_owned();
        self.store
            .with_conn(|c: &Connection| {
                // Read-modify-write under one transaction so counter increments,
                // provider inheritance, and field merges see one consistent row.
                let tx = c.unchecked_transaction()?;
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
                let ex = existing.unwrap_or_default();
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
        let id = input
            .decision_id
            .clone()
            .unwrap_or_else(|| format!("dec_{}", short_id()));
        let id_ret = id.clone();
        let rejected_json = bound_rejected_json(&input.rejected_json);
        self.store
            .with_conn(|c: &Connection| {
                let catalog_fingerprint = calculate_catalog_fingerprint(c)?;
                c.execute(
                     "INSERT INTO model_route_decisions (
                         id, decision_kind, task_id, parent_session_id, harness, project_root, send_id,
                         target_agent, requirements_json, selected_route_key, selected_provider_id,
                         selected_model_id, selected_variant, score_json, rejected_json, explanation_json,
                         quota_signal_json, policy_revision, anchor_version, catalog_fingerprint, created_at
                       ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
                    params![
                        id,
                        if input.decision_kind.is_empty() { "single" } else { &input.decision_kind },
                        input.task_id,
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
                        now_ms(),
                    ],
                )?;
                Ok(())
            })
            .map_err(err)?;
        Ok(id_ret)
    }

    fn set_decision_outcome(
        &self,
        decision_id: &str,
        update: &DecisionOutcomeUpdate,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                let tx = c.unchecked_transaction()?;
                let (route_key, selected_provider_id, selected_model_id, prior_outcome): (
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                ) = tx.query_row(
                    "SELECT selected_route_key, selected_provider_id, selected_model_id, outcome
                     FROM model_route_decisions WHERE id=?1",
                    [decision_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )?;

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
                let (parsed_provider_id, parsed_model_id, variant) = split_route_key(&route_key);
                let provider_id = selected_provider_id.unwrap_or(parsed_provider_id);
                let model_id = selected_model_id.unwrap_or(parsed_model_id);
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

    fn delete_decisions_older_than(&self, created_before_ms: i64) -> RouteStateResult<usize> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "DELETE FROM model_route_decisions WHERE created_at < ?1",
                    [created_before_ms],
                )
            })
            .map_err(err)
    }

    fn record_usage_delivery(&self, obs: &UsageObservation) -> RouteStateResult<UsageRecordResult> {
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
                            workload_class, account_key)
                         VALUES (?1, NULL, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                                 ?9, ?10, ?11, ?12, ?13)
                         ON CONFLICT(id) DO UPDATE SET
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
                           account_key = COALESCE(model_route_usage.account_key,
                                                  excluded.account_key)
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
                Ok(UsageRecordResult {
                    recorded: !held_speed_observation.unwrap_or(false) && holds_speed_observation,
                    usage_recorded: held_speed_observation.is_none(),
                })
            })
            .map_err(err)
            .inspect(|_| {
                // Any write here, an insert or the enrichment of an existing row,
                // can move a class mean; the next selection recomputes.
                self.usage_generation.fetch_add(1, Ordering::AcqRel);
            })
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

    fn get_facts_with_observed_speed(&self, now: f64) -> RouteStateResult<Vec<ModelFact>> {
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
            if let Some(multiplier) =
                prefrontal_routing_core::model_facts::get_speed_multiplier(&route_key)
            {
                fact.scores.speed *= multiplier;
                fact.speed_multiplier = Some(multiplier);
            }
            // Subscription subsidy is overlaid rather than persisted. Selection
            // refreshes the process-global registry from configuration before this
            // read so the objective can price the route at list ÷ multiplier.
            if let Some(multiplier) =
                prefrontal_routing_core::model_facts::get_subscription_multiplier(&route_key)
            {
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
                    let state = RouteRuntimeState {
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

/// `max` over two optional times, treating `None` as "no value" (the defined one
/// wins; the larger of two defined values wins). Mirrors the TS `maxDefined`.
fn max_defined(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) => Some(x),
        (None, b) => b,
    }
}

/// Split `provider/model[/variant]` into its parts (mirrors the TS `splitRouteKey`).
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
       WHERE observed_at >= ?1
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
}
