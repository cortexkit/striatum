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
    Confidence, ModelFact, ModelPromptDefaults, ModelScores,
};
use prefrontal_routing_core::model_router::{ModelRouteStateSnapshot, RouteRuntimeState};
use prefrontal_routing_core::{
    derive_provider_key, fact_to_route, DecisionInput, DecisionRow, FactSource,
    ProviderCooldownState, RouteOutcome, RouteStateResult, RouteStateStore, RouteStateStoreError,
    RouteStateUpdate, UsageObservation, OBSERVED_SPEED_MIN_OUTPUT_TOKENS,
    OBSERVED_SPEED_MIN_SAMPLES, OBSERVED_SPEED_SAMPLE_LIMIT, OBSERVED_SPEED_WINDOW_MS,
    USAGE_MIN_DURATION_MS, USAGE_MIN_OUTPUT_TOKENS,
};
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

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
/// Migration 29: catalog generation fingerprints stamped on new decision rows.
const MIGRATION_029: &str = include_str!("../migrations/029_catalog_fingerprint.sql");

/// Stored rejection detail stays useful without growing with every catalog route.
const MAX_STORED_REJECTIONS: usize = 12;

fn route_state_migrations() -> [Migration; 29] {
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
    supports_vision: Option<bool>,
    supports_tool_use: Option<bool>,
    prompt_defaults_json: Option<String>,
    confidence: String,
    speed_multiplier: Option<f64>,
    excluded: bool,
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
                supports_vision, supports_tool_use, prompt_defaults_json, confidence,
                speed_multiplier, excluded
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
            supports_vision: row.get::<_, Option<i64>>(13)?.map(|value| value != 0),
            supports_tool_use: row.get::<_, Option<i64>>(14)?.map(|value| value != 0),
            prompt_defaults_json: row.get(15)?,
            confidence: row.get(16)?,
            speed_multiplier: row.get(17)?,
            excluded: row.get::<_, i64>(18)? != 0,
        })
    })?;
    let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let bytes = serde_json::to_vec(&rows)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let hex = blake3::hash(&bytes).to_hex().to_string();
    Ok(hex[..16].to_string())
}

fn record_catalog_generation(connection: &Connection) -> rusqlite::Result<String> {
    let fingerprint = calculate_catalog_fingerprint(connection)?;
    connection.execute(
        "INSERT OR IGNORE INTO catalog_generation (fingerprint, created_at) VALUES (?1, ?2)",
        params![fingerprint, now_ms()],
    )?;
    Ok(fingerprint)
}

/// Retain detailed rejected routes for the common case, then append one summary
/// for the omitted tail. The stored value remains a JSON array for audit readers.
fn bound_rejected_json(raw: &str) -> String {
    let Ok(serde_json::Value::Array(rejected)) = serde_json::from_str(raw) else {
        return raw.to_string();
    };
    if rejected.len() <= MAX_STORED_REJECTIONS {
        return raw.to_string();
    }

    let mut reasons = BTreeMap::<String, usize>::new();
    for entry in &rejected[MAX_STORED_REJECTIONS..] {
        let reason = entry
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        *reasons.entry(reason.to_string()).or_default() += 1;
    }
    let truncated = rejected.len() - MAX_STORED_REJECTIONS;
    let mut bounded = rejected[..MAX_STORED_REJECTIONS].to_vec();
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
}

impl SqliteRouteStateStore {
    /// Open from a resolved descriptor, applying migrations. The single-writer
    /// lease location is derived by the lib from the descriptor's db path (one
    /// lease per database, by construction), so there is no lease-dir to pass.
    pub fn open(descriptor: &StorageDescriptor) -> RouteStateResult<Self> {
        let store = open_sqlite(descriptor).map_err(err)?;
        store
            .migrate(ROUTE_STATE_NAMESPACE, &route_state_migrations())
            .map_err(err)?;
        let route_store = Self { store };
        route_store.refresh_catalog_generation()?;
        Ok(route_store)
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

    fn refresh_catalog_generation(&self) -> RouteStateResult<String> {
        self.store
            .with_conn(|connection: &Connection| record_catalog_generation(connection))
            .map_err(err)
    }

    /// Aggregate each route's most-recent eligible usage samples in one query.
    /// The window ranking preserves the per-route cap that `get_observed_speed`
    /// applies for a singular lookup, without issuing one query per catalog fact.
    fn get_observed_speeds(&self, now: f64) -> RouteStateResult<HashMap<String, f64>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare(
                    "WITH ranked_usage AS (
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
                     GROUP BY route_key",
                )?;
                let rows = stmt.query_map(
                    params![
                        (now - OBSERVED_SPEED_WINDOW_MS).floor() as i64,
                        now.floor() as i64,
                        USAGE_MIN_OUTPUT_TOKENS.floor() as i64,
                        USAGE_MIN_DURATION_MS.floor() as i64,
                        OBSERVED_SPEED_SAMPLE_LIMIT,
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )?;
                let mut speeds = HashMap::new();
                for row in rows {
                    let (route_key, count, output_tokens, duration_ms) = row?;
                    // Keep the singular lookup's trust gates byte-for-byte: absent
                    // or untrusted aggregates leave the catalog speed untouched.
                    if count < OBSERVED_SPEED_MIN_SAMPLES as i64
                        || (output_tokens as f64) < OBSERVED_SPEED_MIN_OUTPUT_TOKENS
                        || duration_ms <= 0
                    {
                        continue;
                    }
                    let speed = (output_tokens as f64 / duration_ms as f64) * 1000.0;
                    if speed.is_finite() && speed > 0.0 {
                        speeds.insert(route_key, speed);
                    }
                }
                Ok(speeds)
            })
            .map_err(err)
    }
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
    Ok(ModelFact {
        provider_id: row.get("provider_id")?,
        model_id: row.get("model_id")?,
        variant: row.get::<_, Option<String>>("variant")?,
        family: row.get::<_, Option<String>>("family")?,
        scores,
        context_window: row.get::<_, Option<f64>>("context_window")?,
        max_output_tokens: row.get::<_, Option<u64>>("max_output_tokens")?,
        cost_tier: row.get::<_, Option<f64>>("cost_tier")?,
        cost_per_m_tok_out: row.get::<_, Option<f64>>("cost_per_mtok_out")?,
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
    })
}

fn bool_to_int(v: Option<bool>) -> Option<i64> {
    v.map(|b| if b { 1 } else { 0 })
}

impl RouteStateStore for SqliteRouteStateStore {
    fn upsert_fact(&self, fact: &ModelFact, source: FactSource) -> RouteStateResult<()> {
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
        self.store
            .with_conn(|c: &Connection| {
                // Fact replacement and route-state insertion share one transaction.
                // The store connection mutex serializes this with provider fanout,
                // so a new route resolves either before fanout or after inheritance.
                let tx = c.unchecked_transaction()?;
                tx.execute(
                    "INSERT INTO model_facts (
                       route_key, capacity_key, provider_id, model_id, variant, family,
                       scores_json, context_window, max_output_tokens, cost_tier, cost_per_mtok_out,
                        supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, speed_multiplier,
                        source, updated_at
                      ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)
                     ON CONFLICT(route_key) DO UPDATE SET
                       capacity_key=excluded.capacity_key, provider_id=excluded.provider_id,
                       model_id=excluded.model_id, variant=excluded.variant, family=excluded.family,
                       scores_json=excluded.scores_json, context_window=excluded.context_window,
                       max_output_tokens=excluded.max_output_tokens, cost_tier=excluded.cost_tier,
                       cost_per_mtok_out=excluded.cost_per_mtok_out,
                       supports_vision=excluded.supports_vision, supports_tool_use=excluded.supports_tool_use,
                       prompt_defaults_json=excluded.prompt_defaults_json, confidence=excluded.confidence,
                        notes=excluded.notes, speed_multiplier=excluded.speed_multiplier,
                        source=excluded.source, updated_at=excluded.updated_at",
                    params![
                        route.route_key,
                        route.capacity_key,
                        fact.provider_id,
                        fact.model_id,
                        fact.variant,
                        fact.family,
                        scores_json,
                        fact.context_window,
                        fact.max_output_tokens,
                        fact.cost_tier,
                        fact.cost_per_m_tok_out,
                        bool_to_int(fact.supports_vision),
                        bool_to_int(fact.supports_tool_use),
                        prompt_defaults_json,
                        confidence_str(fact.confidence),
                        fact.notes,
                        fact.speed_multiplier,
                        source.as_str(),
                        now_ms(),
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
                record_catalog_generation(&tx)?;
                tx.commit()
            })
            .map_err(err)
    }

    fn get_facts(&self) -> RouteStateResult<Vec<ModelFact>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare("SELECT * FROM model_facts")?;
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
                record_catalog_generation(&tx)?;
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
                record_catalog_generation(&tx)?;
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
                    "SELECT provider_key, cooldown_until, quota_exhausted_until, quota_pressure
                     FROM provider_cooldown_state WHERE provider_key = ?1",
                    [&provider_key],
                    |row| {
                        Ok(ProviderCooldownState {
                            provider_key: row.get(0)?,
                            cooldown_until: row.get(1)?,
                            quota_exhausted_until: row.get(2)?,
                            quota_pressure: row.get(3)?,
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
                                last_error, last_error_at
                         FROM provider_cooldown_state WHERE provider_key = ?1",
                        [&provider_key],
                        |row| {
                            Ok(ExistingProviderState {
                                cooldown_until: row.get(0)?,
                                quota_exhausted_until: row.get(1)?,
                                quota_pressure: row.get(2)?,
                                last_error: row.get(3)?,
                                last_error_at: row.get(4)?,
                            })
                        },
                    )
                    .optional()?;
                let existing = existing.unwrap_or_default();
                let cooldown_until = max_defined(existing.cooldown_until, update.cooldown_until);
                let quota_exhausted_until =
                    max_defined(existing.quota_exhausted_until, update.quota_exhausted_until);
                let quota_pressure = update.quota_pressure.or(existing.quota_pressure);
                let (last_error, last_error_at) = match &update.last_error {
                    Some(error) => (Some(error.clone()), Some(now_ms())),
                    None => (existing.last_error, existing.last_error_at),
                };
                let updated_at = now_ms();

                tx.execute(
                    "INSERT INTO provider_cooldown_state (
                       provider_key, cooldown_until, quota_exhausted_until, quota_pressure,
                       last_error, last_error_at, updated_at
                     ) VALUES (?1,?2,?3,?4,?5,?6,?7)
                     ON CONFLICT(provider_key) DO UPDATE SET
                       cooldown_until=excluded.cooldown_until,
                       quota_exhausted_until=excluded.quota_exhausted_until,
                       quota_pressure=excluded.quota_pressure,
                       last_error=excluded.last_error,
                       last_error_at=excluded.last_error_at,
                       updated_at=excluded.updated_at",
                    params![
                        provider_key,
                        cooldown_until,
                        quota_exhausted_until,
                        quota_pressure,
                        last_error,
                        last_error_at,
                        updated_at,
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
                       last_error, last_error_at, updated_at
                     ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,NULL,NULL,0,0,?9,?10,?11)",
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
                       failure_count = failure_count + ?4,
                       quota_error_count = quota_error_count + ?5,
                       last_error = COALESCE(?6, last_error),
                       last_error_at = CASE WHEN ?6 IS NULL THEN last_error_at ELSE ?7 END,
                       updated_at = ?7
                     WHERE provider_id = ?8",
                    params![
                        cooldown_until,
                        quota_exhausted_until,
                        update.quota_pressure,
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
                                last_error, last_error_at
                         FROM model_route_state WHERE route_key = ?1",
                        [route_key],
                        |r| {
                            Ok(ExistingState {
                                cooldown_until: r.get(0)?,
                                quota_exhausted_until: r.get(1)?,
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
                                last_error, last_error_at
                         FROM provider_cooldown_state WHERE provider_key = ?1",
                        [&provider_key],
                        |r| {
                            Ok(ExistingProviderState {
                                cooldown_until: r.get(0)?,
                                quota_exhausted_until: r.get(1)?,
                                quota_pressure: r.get(2)?,
                                last_error: r.get(3)?,
                                last_error_at: r.get(4)?,
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
                       last_error, last_error_at, updated_at
                     ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)
                     ON CONFLICT(route_key) DO UPDATE SET
                       capacity_key=excluded.capacity_key, provider_id=excluded.provider_id,
                       model_id=excluded.model_id, variant=excluded.variant,
                       cooldown_until=excluded.cooldown_until,
                       quota_exhausted_until=excluded.quota_exhausted_until,
                       quota_pressure=excluded.quota_pressure,
                       latency_ema_ms=excluded.latency_ema_ms, success_ema=excluded.success_ema,
                       failure_count=excluded.failure_count, quota_error_count=excluded.quota_error_count,
                       last_error=excluded.last_error, last_error_at=excluded.last_error_at,
                       updated_at=excluded.updated_at",
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
                let catalog_fingerprint = record_catalog_generation(c)?;
                c.execute(
                    "INSERT INTO model_route_decisions (
                        id, decision_kind, task_id, parent_session_id, send_id, target_agent,
                        requirements_json, selected_route_key, selected_provider_id, selected_model_id,
                        selected_variant, score_json, rejected_json, explanation_json,
                        quota_signal_json, policy_revision, anchor_version, catalog_fingerprint, created_at
                      ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
                    params![
                        id,
                        if input.decision_kind.is_empty() { "single" } else { &input.decision_kind },
                        input.task_id,
                        input.parent_session_id,
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
        outcome: RouteOutcome,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "UPDATE model_route_decisions SET outcome=?1, outcome_at=?2 WHERE id=?3",
                    params![outcome.as_str(), now_ms(), decision_id],
                )?;
                Ok(())
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

    fn record_usage_observation(&self, obs: &UsageObservation) -> RouteStateResult<bool> {
        // Eligibility gates, ported verbatim from the TS recordUsageObservation: a
        // sample must have non-empty keys and clear the per-sample minimums, else it
        // is rejected (returns false, no row). These are the same constants used by
        // get_observed_speed, so the write and the read agree on what "eligible" means.
        if obs.id.trim().is_empty()
            || obs.route_key.trim().is_empty()
            || obs.capacity_key.trim().is_empty()
            || obs.provider_id.trim().is_empty()
        {
            return Ok(false);
        }
        if !obs.output_tokens.is_finite() || obs.output_tokens < USAGE_MIN_OUTPUT_TOKENS {
            return Ok(false);
        }
        if !obs.duration_ms.is_finite() || obs.duration_ms < USAGE_MIN_DURATION_MS {
            return Ok(false);
        }
        if !obs.observed_at.is_finite() {
            return Ok(false);
        }
        self.store
            .with_conn(|c: &Connection| {
                // Idempotent by message id: a redrive of the same message is a no-op.
                let changes = c.execute(
                    "INSERT INTO model_route_usage
                       (id, task_id, route_key, capacity_key, provider_id,
                        input_tokens, output_tokens, duration_ms, observed_at)
                     VALUES (?1, NULL, ?2, ?3, ?4, NULL, ?5, ?6, ?7)
                     ON CONFLICT(id) DO NOTHING",
                    params![
                        obs.id,
                        obs.route_key,
                        obs.capacity_key,
                        obs.provider_id,
                        obs.output_tokens.floor() as i64,
                        obs.duration_ms.floor() as i64,
                        obs.observed_at.floor() as i64,
                    ],
                )?;
                Ok(changes > 0)
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
                // sample limit: same query shape + gates as the TS getObservedSpeed.
                let mut stmt = c.prepare(
                    "SELECT output_tokens, duration_ms
                     FROM model_route_usage
                     WHERE route_key = ?1
                       AND observed_at >= ?2
                       AND observed_at <= ?3
                       AND output_tokens >= ?4
                       AND duration_ms >= ?5
                     ORDER BY observed_at DESC
                     LIMIT ?6",
                )?;
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
                let mut output_tokens: i64 = 0;
                let mut duration_ms: i64 = 0;
                let mut count: usize = 0;
                for row in rows {
                    let (out, dur) = row?;
                    output_tokens += out;
                    duration_ms += dur;
                    count += 1;
                }
                // Trust the aggregate only with enough eligible samples + total volume.
                if count < OBSERVED_SPEED_MIN_SAMPLES {
                    return Ok(None);
                }
                if (output_tokens as f64) < OBSERVED_SPEED_MIN_OUTPUT_TOKENS || duration_ms <= 0 {
                    return Ok(None);
                }
                let speed = (output_tokens as f64 / duration_ms as f64) * 1000.0;
                Ok(if speed.is_finite() && speed > 0.0 {
                    Some(speed)
                } else {
                    None
                })
            })
            .map_err(err)
    }

    fn get_facts_with_observed_speed(&self, now: f64) -> RouteStateResult<Vec<ModelFact>> {
        let facts = self.get_facts()?;
        let observed_speeds = self.get_observed_speeds(now)?;
        let mut overlaid = Vec::with_capacity(facts.len());
        for mut fact in facts {
            // The observed route key is provider/model + the fact's prompt-default
            // variant (falling back to its own variant), so a max-effort sibling's
            // observations attach to the right fact (ported from observedRouteKeyForFact).
            let observed_variant = fact
                .prompt_defaults
                .as_ref()
                .and_then(|d| d.variant.clone())
                .or_else(|| fact.variant.clone());
            let observed_route_key = prefrontal_routing_core::make_route_key(
                &fact.provider_id,
                &fact.model_id,
                observed_variant.as_deref(),
            );
            if let Some(speed) = observed_speeds.get(&observed_route_key) {
                fact.scores.speed = *speed;
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
            // Subscription subsidy is overlaid the same way: it is not persisted,
            // only read from the boot-time config registry and attached to the fact
            // so the objective can price the route at list ÷ multiplier.
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
                            quota_pressure, latency_ema_ms, success_ema
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
                    })
                })?;
                for row in rows {
                    let row = row?;
                    let state = RouteRuntimeState {
                        cooldown_until: row.cooldown_until,
                        quota_exhausted_until: row.quota_exhausted_until,
                        latency_ema_ms: row.latency_ema_ms,
                        success_ema: row.success_ema,
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
    last_error: Option<String>,
    last_error_at: Option<i64>,
}

#[derive(Default)]
struct ExistingState {
    cooldown_until: Option<f64>,
    quota_exhausted_until: Option<f64>,
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
    quota_pressure: Option<f64>,
    latency_ema_ms: Option<f64>,
    success_ema: Option<f64>,
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
