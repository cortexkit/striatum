//! Model facts, catalog updates, and the catalog views used by readers.
//! The cache and its write tracking sit together so inventory and selection
//! agree on when the stored facts have changed.

use super::*;

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
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    login_methods_documented: bool,
    reasoning_efforts_json: Option<String>,
    canonical_id: Option<String>,
    upstream_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_kind: Option<String>,
}

/// BLAKE3 over every routing-read `model_facts` column: route/capacity identity,
/// provider/model/variant, family, Elo/EQ/speed, context and output limits, price
/// data, vision/tool capability flags, allowed login methods, published
/// reasoning efforts, canonical model identity (it decides which score a route
/// borrows), upstream status, model type and specialist kind, prompt defaults,
/// confidence, speed multiplier, and exclusion, plus Fusiform operator grades.
/// Adding a routing-read column
/// without extending this row is the defect this hash exists to catch. Query ordering—not insertion ordering—is the
/// generation contract, so a no-op reseed or shuffled input cannot mint a different
/// fingerprint for the same catalog.
///
/// Widening this row is intentionally migration-free even when it changes a
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
                reasoning_efforts_json, canonical_id, upstream_status, model_type, model_kind,
                login_methods_documented
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
            login_methods_documented: row.get(28)?,
            reasoning_efforts_json: row.get(23)?,
            canonical_id: row.get(24)?,
            upstream_status: row.get(25)?,
            model_type: row.get(26)?,
            model_kind: row.get(27)?,
        })
    })?;
    let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let grades = operator_quality::fingerprint_rows(connection)?;
    let decommissioned = operator_quality::decommissioned_rows(connection)?;
    // Creating an empty withdrawal table changes no routing facts, so keep the
    // catalog fingerprint unchanged. Nonempty withdrawals affect routing and
    // must participate in the hash.
    let bytes = if decommissioned.is_empty() {
        serde_json::to_vec(&(rows, grades))
    } else {
        serde_json::to_vec(&(rows, grades, decommissioned))
    }
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
pub(super) fn install_catalog_change_tracking(connection: &Connection) -> rusqlite::Result<bool> {
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
    let has_decommissioned: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type = 'table' AND name = 'routing_operator_decommissioned')",
        [], |row| row.get(0),
    )?;
    if has_decommissioned {
        connection.execute_batch(
            "CREATE TEMP TRIGGER IF NOT EXISTS catalog_decommissioned_writes_on_insert
             AFTER INSERT ON routing_operator_decommissioned
             BEGIN UPDATE catalog_facts_writes SET writes = writes + 1 WHERE singleton = 1; END;
             CREATE TEMP TRIGGER IF NOT EXISTS catalog_decommissioned_writes_on_update
             AFTER UPDATE ON routing_operator_decommissioned
             BEGIN UPDATE catalog_facts_writes SET writes = writes + 1 WHERE singleton = 1; END;
             CREATE TEMP TRIGGER IF NOT EXISTS catalog_decommissioned_writes_on_delete
             AFTER DELETE ON routing_operator_decommissioned
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

/// What a cached catalog fingerprint is valid for: this connection's count of
/// `model_facts` writes (bumped by the temporary triggers that
/// `install_catalog_change_tracking` creates) and SQLite's `data_version`,
/// which moves when another connection commits a change to the database file.
/// While both are unchanged, `model_facts` holds what it held when the
/// fingerprint was computed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogFingerprintKey {
    store_identity: u64,
    facts_writes: i64,
    data_version: i64,
}

#[derive(Clone)]
pub struct CatalogFactsSnapshot {
    pub generation: CatalogFingerprintKey,
    pub facts: Vec<ModelFact>,
    pub providers: BTreeSet<String>,
}

pub(super) fn next_catalog_cache_identity() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone)]
pub(super) struct CachedCatalogFingerprint {
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
        store_identity: 0,
        facts_writes: connection.query_row(
            "SELECT writes FROM temp.catalog_facts_writes WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?,
        data_version: connection.query_row("PRAGMA data_version", [], |row| row.get(0))?,
    })
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

pub(super) fn row_to_fact(row: &Row<'_>) -> rusqlite::Result<ModelFact> {
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
        login_methods_documented: row.get("login_methods_documented")?,
        // Read-time provenance, never stored: a fact loaded from the row has whatever
        // speed the row holds, and the median stand-in is applied above this.
        speed_is_fleet_median: false,
        decommissioned: false,
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
        model_type: row.get("model_type")?,
        model_kind: row.get("model_kind")?,
    })
}

fn bool_to_int(v: Option<bool>) -> Option<i64> {
    v.map(|b| if b { 1 } else { 0 })
}

fn confidence_str(c: Confidence) -> &'static str {
    match c {
        Confidence::Curated => "curated",
        Confidence::Unknown => "unknown",
    }
}

impl SqliteRouteStateStore {
    /// Stored providers only; score aliases added to a request are not catalog
    /// identities and must not be used to resolve credential ownership.
    pub fn catalog_providers(&self) -> RouteStateResult<std::collections::BTreeSet<String>> {
        self.store
            .with_read(|conn: &Connection| {
                let mut statement = conn.prepare("SELECT DISTINCT provider_id FROM model_facts")?;
                let rows = statement.query_map([], |row| row.get(0))?;
                rows.collect::<rusqlite::Result<_>>()
            })
            .map_err(err)
    }

    /// Shared committed catalog view for selection and background serving joins.
    pub fn get_selection_catalog(&self) -> RouteStateResult<Arc<CatalogFactsSnapshot>> {
        self.read_selection_catalog(|| {})
    }

    fn read_selection_catalog(
        &self,
        after_key: impl FnOnce(),
    ) -> RouteStateResult<Arc<CatalogFactsSnapshot>> {
        // Read the writer's TEMP change counter BEFORE opening the reader's
        // snapshot. Facts can then be as new as the key or newer, never older.
        // A commit between these reads may cache newer facts under the old key
        // (one extra rebuild), but cannot cache an old index under the new key.
        let key = self.catalog_inventory_key()?;
        let generation = key
            .map(|mut key| {
                key.store_identity = self.catalog_cache_identity.load(Ordering::Relaxed);
                key
            })
            .unwrap_or_else(|| CatalogFingerprintKey {
                store_identity: next_catalog_cache_identity(),
                facts_writes: 0,
                data_version: 0,
            });
        after_key();
        if let Some(snapshot) = self
            .selection_catalog
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .filter(|snapshot| snapshot.generation == generation)
        {
            return Ok(Arc::clone(snapshot));
        }
        let rows = self
            .store
            .with_read(|connection| {
                // A pooled reader arrives inside its own read transaction. The
                // private single-connection store behind what-if previews hands
                // over a bare connection, so open the snapshot here.
                let tx = if connection.is_autocommit() {
                    Some(connection.unchecked_transaction()?)
                } else {
                    None
                };
                let rows = self.load_resolved_facts(connection)?;
                if let Some(tx) = tx {
                    crate::connection_store::timed_commit(tx)?;
                }
                Ok(rows)
            })
            .map_err(err)?;
        let snapshot = Arc::new(CatalogFactsSnapshot {
            generation,
            providers: rows
                .iter()
                .map(|(fact, _)| fact.provider_id.clone())
                .collect(),
            facts: rows
                .into_iter()
                .filter_map(|(fact, present)| present.then_some(fact))
                .collect(),
        });
        if key.is_some() {
            *self
                .selection_catalog
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(&snapshot));
        }
        Ok(snapshot)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn selection_catalog_with_key_hook_for_test(
        &self,
        after_key: impl FnOnce(),
    ) -> RouteStateResult<Arc<CatalogFactsSnapshot>> {
        self.read_selection_catalog(after_key)
    }

    /// Facts and the generation of exactly that view, in one writer acquisition.
    /// The generation is meaningful only on the writer connection: it combines
    /// a TEMP write counter with that connection's `data_version`. Callers that
    /// need uncommitted-transaction visibility must stay on this writer path;
    /// selection uses the reader-backed path instead.
    pub fn get_facts_with_generation(&self) -> RouteStateResult<CatalogFactsSnapshot> {
        let (generation, facts) = self.read_resolved_facts()?;
        let generation = generation.ok_or_else(|| err("catalog generation unavailable"))?;
        let providers = facts
            .iter()
            .map(|(fact, _)| fact.provider_id.clone())
            .collect();
        Ok(CatalogFactsSnapshot {
            generation,
            providers,
            facts: facts
                .into_iter()
                .filter_map(|(fact, present)| present.then_some(fact))
                .collect(),
        })
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
    pub(super) fn current_catalog_fingerprint(
        &self,
        connection: &Connection,
    ) -> rusqlite::Result<String> {
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
            .resolved_facts
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = None;
        *self.catalog_facts.lock().unwrap_or_else(|p| p.into_inner()) = None;
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
        self.cached_catalog_inventory(|| {})
    }

    fn cached_catalog_inventory(
        &self,
        after_load: impl FnOnce(),
    ) -> RouteStateResult<Vec<CatalogFact>> {
        let key = self.catalog_inventory_key()?;
        {
            let mut cache = self.catalog_facts.lock().unwrap_or_else(|p| p.into_inner());
            if let Some((_, rows)) = cache.as_ref().filter(|(cached, _)| Some(*cached) == key) {
                return Ok(rows.clone());
            }
            *cache = None;
        }
        let mut rows = self
            .store
            .with_read(|connection| {
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
        // Inventory keeps the shared model scores but not the operator grades
        // that are overlaid only for selection.
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
        after_load();
        // The writer is released during the load. A concurrently published
        // catalog makes this request's view uncacheable, not invalid to return.
        let current_key = self.catalog_inventory_key()?;
        if let Some(key) = key.filter(|candidate| Some(*candidate) == current_key) {
            *self.catalog_facts.lock().unwrap_or_else(|p| p.into_inner()) =
                Some((key, rows.clone()));
        }
        Ok(rows)
    }

    fn catalog_inventory_key(&self) -> RouteStateResult<Option<CatalogFingerprintKey>> {
        self.store
            .with_conn(
                |connection| match read_catalog_fingerprint_key(connection) {
                    Ok(key) => Ok(connection.is_autocommit().then_some(key)),
                    Err(cause) => {
                        self.reinstall_catalog_change_tracking(connection, &cause);
                        Ok(None)
                    }
                },
            )
            .map_err(err)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn get_catalog_facts_with_load_hook_for_test(
        &self,
        after_load: impl FnOnce(),
    ) -> RouteStateResult<Vec<CatalogFact>> {
        self.cached_catalog_inventory(after_load)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn catalog_inventory_is_cached_for_test(&self) -> bool {
        self.catalog_facts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
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

    /// Resolve shared Elo/EQ scores before deciding which rows routing may
    /// select. `canonical_id` and `variant` group routes for one upstream model,
    /// and `scores_json` stores their scores. Resolve that group before filtering
    /// so a row withdrawn by Fusiform can still supply its score to a served
    /// sibling. A row is routable when `fusiform_present` is 1, when the flag is
    /// still NULL (an older row before its first full sync), or when `source` is
    /// an operator `override`. The bool carries that final routing decision.
    pub(super) fn read_resolved_fact_rows(&self) -> RouteStateResult<Vec<(ModelFact, bool)>> {
        self.read_resolved_facts().map(|(_, facts)| facts)
    }

    fn read_resolved_facts(
        &self,
    ) -> RouteStateResult<(Option<CatalogFingerprintKey>, Vec<(ModelFact, bool)>)> {
        self.store
            .with_conn(|c: &Connection| {
                loop {
                    // Never reuse or publish a transaction-local view. The TEMP counter
                    // counts model-fact and operator-grade writes, including uncommitted
                    // ones. Rollback restores that count; different later writes could
                    // reach the same count but must not reuse the rolled-back facts.
                    let cacheable = c.is_autocommit();
                    let key = match read_catalog_fingerprint_key(c) {
                        Ok(key) => Some(key),
                        Err(cause) => {
                            self.reinstall_catalog_change_tracking(c, &cause);
                            None
                        }
                    };
                    if cacheable {
                        let cache = self
                            .resolved_facts
                            .lock()
                            .unwrap_or_else(|p| p.into_inner());
                        if let Some((_, facts)) =
                            cache.as_ref().filter(|(cached, _)| Some(*cached) == key)
                        {
                            let mut generation = key.unwrap();
                            generation.store_identity =
                                self.catalog_cache_identity.load(Ordering::Relaxed);
                            return Ok((Some(generation), facts.clone()));
                        }
                    }
                    // Keep catalog rows and their operator-grade overlay in one SQLite
                    // snapshot even when a second connection updates the catalog.
                    let tx = if cacheable {
                        Some(c.unchecked_transaction()?)
                    } else {
                        None
                    };
                    let facts = self.load_resolved_facts(c)?;
                    if let Some(tx) = tx {
                        crate::connection_store::timed_commit(tx)?;
                    }
                    if cacheable && read_catalog_fingerprint_key(c).ok() != key {
                        continue;
                    }
                    if let Some(key) = key.filter(|_| cacheable) {
                        *self
                            .resolved_facts
                            .lock()
                            .unwrap_or_else(|p| p.into_inner()) = Some((key, facts.clone()));
                    }
                    let generation = key.map(|mut key| {
                        key.store_identity = if cacheable {
                            self.catalog_cache_identity.load(Ordering::Relaxed)
                        } else {
                            next_catalog_cache_identity()
                        };
                        key
                    });
                    return Ok((generation, facts));
                }
            })
            .map_err(err)
    }

    fn load_resolved_facts(&self, c: &Connection) -> rusqlite::Result<Vec<(ModelFact, bool)>> {
        let rows = {
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
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut facts = Vec::with_capacity(rows.len());
        let mut canonical_ids = Vec::with_capacity(rows.len());
        let mut present = Vec::with_capacity(rows.len());
        for (fact, canonical_id, present_now) in rows {
            facts.push(fact);
            canonical_ids.push(canonical_id);
            present.push(present_now);
        }
        self.share_canonical_scores(&mut facts, &canonical_ids);
        operator_quality::overlay_operator_quality(c, &mut facts, &canonical_ids)?;
        Ok(facts.into_iter().zip(present).collect())
    }

    /// Give routes without an operator score the score of a route serving the
    /// same model (see `striatum_core::canonical_scores`), and log each
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
        let facts = facts.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.apply_fusiform_catalog(&facts, cursor)
        })
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
        let facts = facts.to_owned();
        let mapped_providers = mapped_providers.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.apply_fusiform_catalog_for_providers(&facts, cursor, &mapped_providers)
        })
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
        let facts = facts.to_owned();
        let mapped_providers = mapped_providers.to_owned();
        let retained = retained.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.apply_fusiform_catalog_snapshot_for_providers(
                &facts,
                cursor,
                &mapped_providers,
                &retained,
            )
        })
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
            .read_resolved_fact_rows()?
            .into_iter()
            .filter(|(fact, present)| {
                *present
                    && !fact.is_deprecated_upstream()
                    && fact.score_provenance
                        == striatum_core::model_facts::ScoreProvenance::Unscored
            })
            .map(|(fact, _)| UnscoredRow {
                route_key: striatum_core::make_route_key(
                    &fact.provider_id,
                    &fact.model_id,
                    fact.variant.as_deref(),
                ),
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.route_key.cmp(&right.route_key));
        Ok(rows)
    }
}

impl crate::AccountTx<'_> {
    pub fn apply_fusiform_catalog(
        &self,
        facts: &[FusiformFactUpdate],
        cursor: FusiformCatalogCursor,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::catalog::apply_fusiform_catalog");
        self.apply_fusiform_catalog_for_providers(facts, cursor, &HashSet::new())
    }

    pub fn apply_fusiform_catalog_for_providers(
        &self,
        facts: &[FusiformFactUpdate],
        cursor: FusiformCatalogCursor,
        mapped_providers: &HashSet<String>,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::catalog::apply_fusiform_catalog_for_providers",
        );
        self.apply_fusiform_catalog_for_providers_inner(
            facts,
            cursor,
            mapped_providers,
            false,
            &HashSet::new(),
        )
    }

    pub fn apply_fusiform_catalog_snapshot_for_providers(
        &self,
        facts: &[FusiformFactUpdate],
        cursor: FusiformCatalogCursor,
        mapped_providers: &HashSet<String>,
        retained: &HashSet<(String, String)>,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::catalog::apply_fusiform_catalog_snapshot_for_providers",
        );
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
        self.command_sql(|connection: &Connection| {
                let tx = connection;
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
                    let route_key = striatum_core::make_route_key(
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
                               reasoning_efforts_json, canonical_id, upstream_status, model_type, model_kind,
                               login_methods_documented
                             ) VALUES (
                               ?1, ?1, ?2, ?3, NULL, NULL, ?4, ?5, ?6, ?7,
                               ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL, 'unknown',
                               'discovered by Fusiform; awaiting operator score placement',
                               NULL, 0, 'unscored', ?15, NULL, NULL, NULL,
                               'fusiform', ?16, ?17,
                               'fusiform', 'fusiform', 'fusiform', 'fusiform',
                                'fusiform', 'fusiform', ?18, ?19, ?20, ?21, ?22, ?23, ?24
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
                                fact.model_type,
                                fact.model_kind,
                                fact.login_methods_documented,
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
                               canonical_id = ?18, upstream_status = ?19,
                               model_type = ?20, model_kind = ?21, login_methods_documented = ?22
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
                                fact.model_type,
                                fact.model_kind,
                                fact.login_methods_documented,
                            ],
                        )?;
                    }
                    // Lineage is catalog-owned even when a row previously carried
                    // an operator family or a glob-derived family. Update variants too.
                    let catalog_efforts_json = fact.catalog_efforts.as_ref()
                        .map(serde_json::to_string).transpose()
                        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                    tx.execute(
                        "UPDATE model_facts SET family = ?1, family_source = ?2, catalog_efforts_json = ?5
                         WHERE provider_id = ?3 AND model_id = ?4",
                        params![
                            fact.lineage,
                            if fact.lineage.is_some() { "curated" } else { "absent" },
                            fact.provider_id,
                            fact.model_id,
                            catalog_efforts_json,
                        ],
                    )?;
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
                Ok(())
            })
            .map_err(err)
    }

    pub fn upsert_fact(&self, fact: &ModelFact, source: FactSource) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter("account_tx::catalog::upsert_fact");
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
        self.command_sql(|c: &Connection| {
                // Fact replacement and route-state insertion share one transaction.
                // The store connection mutex serializes this with provider fanout,
                // so a new route resolves either before fanout or after inheritance.
                let tx = c;
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
                 // Score placement and new variants cannot replace catalog lineage,
                 // including an explicitly absent lineage.
                 let lineage: Option<Option<String>> = tx.query_row(
                     "SELECT family FROM model_facts
                      WHERE provider_id = ?1 AND model_id = ?2 AND facts_source = 'fusiform'
                      ORDER BY route_key LIMIT 1",
                     params![fact.provider_id, fact.model_id],
                     |row| row.get(0),
                 ).optional()?;
                 let family = lineage.as_ref().unwrap_or(&fact.family);
                 let classification = tx.query_row(
                     "SELECT model_type, model_kind FROM model_facts
                      WHERE provider_id = ?1 AND model_id = ?2 ORDER BY route_key LIMIT 1",
                     params![fact.provider_id, fact.model_id],
                     |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, Option<String>>(1)?)),
                 ).optional()?;
                 let (model_type, model_kind) = classification.unwrap_or_else(||
                     (fact.model_type.clone(), fact.model_kind.clone()));
                 // Catalog identity, classifications and login restrictions belong
                 // to the served model. Score edits and new reasoning variants
                 // must retain even an explicitly absent catalog classification.
                 tx.execute(
                     "INSERT INTO model_facts (
                       route_key, capacity_key, provider_id, model_id, variant, family, family_source,
                       scores_json, context_window, max_output_tokens, cost_tier, cost_per_mtok_out,
                         input_rate, cache_read_rate, cache_write_rate, reasoning_rate,
                         supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, speed_multiplier,
                          source, updated_at, aa_index, review_by_ms, eq_source, auth_methods_json, login_methods_documented,
                          canonical_id, upstream_status, model_type, model_kind
                         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,
                           (SELECT auth_methods_json FROM model_facts
                            WHERE provider_id = ?3 AND model_id = ?4 ORDER BY route_key LIMIT 1),
                           COALESCE((SELECT login_methods_documented FROM model_facts
                            WHERE provider_id = ?3 AND model_id = ?4 ORDER BY route_key LIMIT 1), 0),
                           (SELECT canonical_id FROM model_facts
                            WHERE provider_id = ?3 AND model_id = ?4 ORDER BY route_key LIMIT 1),
                            (SELECT upstream_status FROM model_facts
                             WHERE provider_id = ?3 AND model_id = ?4 ORDER BY route_key LIMIT 1),
                            ?28, ?29)
                      ON CONFLICT(route_key) DO UPDATE SET
                       capacity_key=excluded.capacity_key, provider_id=excluded.provider_id,
                       model_id=excluded.model_id, variant=excluded.variant, family=excluded.family,
                        family_source=excluded.family_source,
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
                         family,
                         if family.is_some() {
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
                        model_type,
                        model_kind,
                    ],
                )?;
                // A fresh INSERT fires the model_facts insert trigger, which creates
                // the route's state row with any live provider wall and its cause.
                // An update leaves the existing state row alone.
                Ok(())
            })
            .map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fingerprint_test_store(
        scratch: &cortexkit_test_support::ScratchDir,
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
        let scratch = cortexkit_test_support::ScratchDir::new("catalog-fingerprint-cache");
        let store = fingerprint_test_store(&scratch, "catalog-fingerprint-cache");
        let mut seen = Vec::new();
        assert_fingerprint_follows(&store, "seeded catalog", &mut seen);

        let mut fact = store.get_facts().unwrap().remove(0);
        let route_key = striatum_core::make_route_key(
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
                    login_methods_documented: false,
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
                    catalog_efforts: None,
                    canonical_id: None,
                    upstream_status: None,
                    model_type: None,
                    model_kind: None,
                    lineage: None,
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
            .delete_fact(&striatum_core::make_route_key(
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
    fn resolved_fact_cache_tracks_catalog_quality_external_writes_and_rollback() {
        let scratch = cortexkit_test_support::ScratchDir::new("resolved-fact-cache");
        let store = fingerprint_test_store(&scratch, "resolved-fact-cache");
        let mut fact = store.get_facts().unwrap().remove(0);
        fact.provider_id = "cache-provider".into();
        fact.model_id = "cache-model".into();
        fact.variant = None;
        fact.scores.elo = 1234.0;
        store.upsert_fact(&fact, FactSource::Override).unwrap();
        let read = || {
            store
                .get_facts()
                .unwrap()
                .into_iter()
                .find(|f| f.provider_id == "cache-provider")
                .unwrap()
        };
        assert_eq!(read().scores.elo, 1234.0);
        let cached_ptr = || {
            store
                .resolved_facts
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .1
                .as_ptr() as usize
        };
        let before = cached_ptr();
        store
            .store
            .with_conn(|c| c.execute("UPDATE model_route_state SET updated_at=updated_at+1", []))
            .unwrap();
        assert_eq!(read().scores.elo, 1234.0);
        assert_eq!(
            cached_ptr(),
            before,
            "ordinary routing writes must not decode the catalog again"
        );
        store
            .replace_operator_quality(
                true,
                &[(
                    "cache-provider/cache-model".into(),
                    OperatorQuality {
                        model: "cache-model".into(),
                        elo: 2345.0,
                        eq: 2456.0,
                    },
                )]
                .into_iter()
                .collect(),
            )
            .unwrap();
        assert_eq!(read().scores.elo, 2345.0);
        assert_eq!(
            store
                .get_fact_by_route_key("cache-provider/cache-model")
                .unwrap()
                .unwrap()
                .scores
                .elo,
            1234.0
        );
        store
            .replace_operator_quality(false, &Default::default())
            .unwrap();
        assert_eq!(read().scores.elo, 1234.0);
        let external = rusqlite::Connection::open(scratch.path().join("store.db")).unwrap();
        external
            .execute(
                "UPDATE model_facts SET context_window=765432 WHERE provider_id='cache-provider'",
                [],
            )
            .unwrap();
        assert_eq!(read().context_window, Some(765432.0));
        store.store.with_conn(|c| c.execute_batch("BEGIN; UPDATE model_facts SET context_window=888 WHERE provider_id='cache-provider'")).unwrap();
        assert_eq!(read().context_window, Some(888.0));
        store.store.with_conn(|c| c.execute_batch("ROLLBACK; UPDATE model_facts SET context_window=999 WHERE provider_id='cache-provider'")).unwrap();
        assert_eq!(read().context_window, Some(999.0));
        store
            .store
            .with_conn(|c| c.execute_batch("DROP TABLE temp.catalog_facts_writes"))
            .unwrap();
        assert_eq!(read().context_window, Some(999.0));
        fact.context_window = Some(111.0);
        store.upsert_fact(&fact, FactSource::Override).unwrap();
        assert_eq!(read().context_window, Some(111.0));
        store.delete_fact("cache-provider/cache-model").unwrap();
        assert!(!store
            .get_facts()
            .unwrap()
            .iter()
            .any(|f| f.provider_id == "cache-provider"));
    }

    #[test]
    fn a_fingerprint_read_inside_a_rolled_back_catalog_write_is_never_reused() {
        let scratch = cortexkit_test_support::ScratchDir::new("catalog-fingerprint-rollback");
        let store = fingerprint_test_store(&scratch, "catalog-fingerprint-rollback");
        let committed = store.catalog_fingerprint().unwrap();
        let fact = store.get_facts().unwrap().remove(0);
        let route_key = striatum_core::make_route_key(
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
        striatum_core::make_route_key(&fact.provider_id, &fact.model_id, fact.variant.as_deref())
    }

    #[test]
    fn a_missing_change_counter_falls_back_to_a_fresh_fingerprint_and_tracking_returns() {
        let scratch = cortexkit_test_support::ScratchDir::new("catalog-fingerprint-lost-counter");
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
        let scratch = cortexkit_test_support::ScratchDir::new("catalog-fingerprint-reinstall");
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
