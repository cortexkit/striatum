//! BlockState over consolidated `block` rows, compared with the schema-93
//! implementation before migration 094 switches production readers and writers.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension, Row};
use striatum_core::blocks::Block;

use super::{ProbeAuthorization, Shards, SuccessPool};
use crate::block_storage::{self, Meta, StoredBlock};
use crate::fence_rechecks::{self, FenceRecheck, LoadedBlocks, ViewOrigin};

#[path = "decisions/tests.rs"]
mod decisions;
#[path = "provider/tests.rs"]
mod provider;

const TABLES: [&str; 8] = [
    "block",
    "probe_authorization",
    "success_proof",
    "retirement_tombstone",
    "pool_key_state",
    "provider_cooldown_state",
    "model_route_state",
    "account_eligibility",
];

#[derive(Clone, Debug, PartialEq)]
struct PoolTelemetry {
    provider: String,
    credential_id: String,
    last_used_at: rusqlite::types::Value,
}

#[derive(Clone, Debug, PartialEq)]
enum Record {
    Block(StoredBlock),
    Authorization(ProbeAuthorization),
    Success(SuccessPool),
    Pool(PoolTelemetry),
    Provider(crate::route_snapshot::ProviderCooldownRow),
    Route {
        provider: String,
        row: crate::route_snapshot::RouteStateRow,
    },
    Eligibility(crate::AccountEligibilityFact),
}

#[derive(Clone, Default, PartialEq)]
struct Table {
    rows: Shards<i64, (String, Arc<Record>)>,
    keys: Shards<String, i64>,
    scopes: Shards<String, Shards<i64, ()>>,
}

impl Record {
    fn scopes(&self) -> Vec<String> {
        match self {
            // Pool checks inspect principal clocks under the empty provider;
            // admission checks inspect the same row by principal and admission.
            Self::Block(row) if row.block.kind == "principal" => vec![
                provider_scope(&row.block.scope.provider),
                exclusion_scope(
                    row.block.scope.principal.as_deref().unwrap_or(""),
                    row.block.scope.admission_id.as_deref().unwrap_or(""),
                ),
            ],
            Self::Block(row) => vec![provider_scope(&row.block.scope.provider)],
            Self::Success(row) => vec![serde_json::json!(["fence", row.fence_id]).to_string()],
            Self::Pool(row) => vec![provider_scope(&row.provider)],
            Self::Provider(row) => vec![provider_scope(row.provider_key())],
            Self::Route { provider, .. } => vec![provider_scope(provider)],
            Self::Authorization(_) | Self::Eligibility(_) => Vec::new(),
        }
    }
}

fn provider_scope(provider: &str) -> String {
    serde_json::json!(["provider", provider]).to_string()
}

fn exclusion_scope(principal: &str, admission_id: &str) -> String {
    serde_json::json!(["exclusion", principal, admission_id]).to_string()
}

impl Table {
    fn remove(&mut self, rowid: i64) {
        if let Some((key, record)) = self.rows.remove(&rowid) {
            if self.keys.get(&key) == Some(&rowid) {
                self.keys.remove(&key);
            }
            for scope in record.scopes() {
                if let Some(mut rowids) = self.scopes.get(&scope).cloned() {
                    rowids.remove(&rowid);
                    if rowids.len == 0 {
                        self.scopes.remove(&scope);
                    } else {
                        self.scopes.insert(scope, rowids);
                    }
                }
            }
        }
    }

    fn put(&mut self, rowid: i64, key: String, record: Option<Arc<Record>>) {
        // REPLACE deletes the previous primary key without reporting its
        // deletion to the update hook. Remove that row before installing it.
        if let Some(other) = self.keys.get(&key).copied().filter(|other| *other != rowid) {
            self.remove(other);
        }
        self.remove(rowid);
        if let Some(record) = record {
            for scope in record.scopes() {
                let mut rowids = self.scopes.get(&scope).cloned().unwrap_or_default();
                rowids.insert(rowid, ());
                self.scopes.insert(scope, rowids);
            }
            self.keys.insert(key.clone(), rowid);
            self.rows.insert(rowid, (key, record));
        }
    }

    fn scoped(&self, scope: &str) -> impl Iterator<Item = (i64, &Record)> {
        self.scopes
            .get(&scope.to_owned())
            .into_iter()
            .flat_map(|rows| rows.iter())
            .filter_map(|(rowid, ())| self.rows.get(rowid).map(|(_, row)| (*rowid, row.as_ref())))
    }
}

#[derive(Clone, Default, PartialEq)]
struct BlockState {
    enabled: bool,
    tables: Vec<Table>,
}

impl std::fmt::Debug for BlockState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut tables = f.debug_map();
        for (name, table) in TABLES.iter().zip(&self.tables) {
            let rows: BTreeMap<_, _> = table.rows.iter().collect();
            tables.entry(name, &rows);
        }
        tables.finish()
    }
}

impl BlockState {
    fn load(conn: &Connection) -> rusqlite::Result<Self> {
        if !conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='block')",
            [],
            |row| row.get::<_, bool>(0),
        )? {
            return Ok(Self::default());
        }
        let mut state = Self {
            enabled: true,
            tables: vec![Table::default(); TABLES.len()],
        };
        for table in 0..TABLES.len() {
            if table == 3 {
                continue;
            }
            let mut statement =
                conn.prepare(&format!("{} {}", super::MAINTENANCE_READ, select(table)))?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let (rowid, key, record) = decode(table, row)?;
                state.tables[table].put(rowid, key, record);
            }
        }
        Ok(state)
    }

    fn apply(&mut self, rows: &BlockRows) {
        if self.enabled {
            for (table, rowid, key, record) in &rows.0 {
                self.tables[*table].put(*rowid, key.clone(), record.clone());
            }
        }
    }

    fn fingerprints(&self) -> Vec<(usize, Option<i64>)> {
        self.tables
            .iter()
            .map(|table| {
                (
                    table.rows.len,
                    table.rows.iter().map(|(rowid, _)| *rowid).max(),
                )
            })
            .collect()
    }

    fn shares_storage_with(&self, other: &Self) -> bool {
        self.tables.len() == other.tables.len()
            && self.tables.iter().zip(&other.tables).all(|(a, b)| {
                Arc::ptr_eq(&a.rows.shards, &b.rows.shards)
                    && Arc::ptr_eq(&a.keys.shards, &b.keys.shards)
                    && Arc::ptr_eq(&a.scopes.shards, &b.scopes.shards)
            })
    }

    fn table_sizes(&self) -> Vec<(&'static str, usize)> {
        TABLES
            .iter()
            .zip(&self.tables)
            .map(|(name, table)| (*name, table.rows.len))
            .collect()
    }

    fn rows(&self, table: usize) -> impl Iterator<Item = (i64, &Record)> {
        self.tables
            .get(table)
            .into_iter()
            .flat_map(|table| table.rows.iter())
            .map(|(rowid, (_, row))| (*rowid, row.as_ref()))
    }

    fn block_rows(&self) -> impl Iterator<Item = &StoredBlock> {
        self.rows(0).filter_map(|(_, record)| match record {
            Record::Block(row) => Some(row),
            _ => None,
        })
    }

    fn block(&self, kind: &str, origin_key: &str) -> Option<&StoredBlock> {
        let table = self.tables.first()?;
        let key = serde_json::json!([kind, origin_key]).to_string();
        let rowid = table.keys.get(&key)?;
        match &*table.rows.get(rowid)?.1 {
            Record::Block(row) => Some(row),
            _ => None,
        }
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn stored_clocks(&self) -> impl Iterator<Item = (&Block, Option<i64>)> {
        self.block_rows()
            .filter(|row| row.managed)
            .map(|row| (&row.block, row.cleared_at))
    }

    fn clock(&self, kind: &str, origin_key: &str) -> Option<(&Block, Option<i64>)> {
        self.block(kind, origin_key)
            .filter(|row| row.managed)
            .map(|row| (&row.block, row.cleared_at))
    }

    fn live_clocks(&self) -> HashMap<(&str, &str), &Block> {
        self.stored_clocks()
            .filter(|(_, cleared_at)| cleared_at.is_none())
            .map(|(block, _)| ((block.kind.as_str(), block.origin_key.as_str()), block))
            .collect()
    }

    fn clocks_claimed_by(&self, owner: &str) -> Vec<(&Block, Option<i64>)> {
        self.stored_clocks()
            .filter(|(block, _)| block.claim_owner.as_deref() == Some(owner))
            .collect()
    }

    fn view_origin(&self, row: &StoredBlock) -> Option<ViewOrigin> {
        if !row.source_present
            || (row.block.kind == "account_route" && row.wall_class.as_deref() != Some("quota"))
        {
            return None;
        }
        let mut origin = row.origin();
        if origin.kind == "credential" {
            let prefix = format!("credential-quota:{}:", origin.origin_key);
            origin.quota = Some(self.block_rows().any(|hold| {
                hold.source_present
                    && hold.block.kind == "account"
                    && hold.fence_id().is_some_and(|id| id.starts_with(&prefix))
            }));
        }
        Some(origin)
    }

    fn view_rows(&self) -> Vec<((u8, i64), ViewOrigin)> {
        self.block_rows()
            .filter_map(|row| {
                let origin = self.view_origin(row)?;
                if row.block.kind == "route"
                    && self
                        .block(
                            "provider",
                            &serde_json::json!([row.block.scope.provider]).to_string(),
                        )
                        .is_some_and(|provider| {
                            provider.source_present
                                && provider.scope_error == row.scope_error
                                && provider.block.evidence_at == row.block.evidence_at
                                && provider.cooldown_until.unwrap_or(0.0)
                                    >= row.cooldown_until.unwrap_or(0.0)
                                && provider.quota_exhausted_until.unwrap_or(0.0)
                                    >= row.quota_exhausted_until.unwrap_or(0.0)
                        })
                {
                    return None;
                }
                Some((
                    (super::view_branch(&row.block.kind), row.source_order),
                    origin,
                ))
            })
            .collect()
    }

    fn view_origins(&self) -> Vec<ViewOrigin> {
        let mut rows: Vec<_> = self.view_rows().into_iter().map(|(_, row)| row).collect();
        rows.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        rows
    }

    fn view_origins_in_scan_order(&self) -> Vec<ViewOrigin> {
        let mut rows = self.view_rows();
        rows.sort_by_key(|(position, _)| *position);
        rows.into_iter().map(|(_, row)| row).collect()
    }

    fn clocks(&self, rowid_order: bool) -> fence_rechecks::Clocks {
        self.block_rows()
            .filter(|row| row.managed)
            .map(|row| {
                let mut block = row.block.clone();
                block.cleared_models = row.cleared_models.clone();
                if !rowid_order {
                    block.cleared_models.sort();
                }
                (
                    (block.kind.clone(), block.origin_key.clone()),
                    (block, row.cleared_at),
                )
            })
            .collect()
    }

    fn blocks(&self, now: i64) -> striatum_core::RouteStateResult<LoadedBlocks> {
        let mut loaded =
            fence_rechecks::derive_blocks(self.clocks(false), &[], self.view_origins(), now)
                .map_err(crate::err)?;
        loaded
            .blocks
            .sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        loaded
            .cleared_evidence
            .sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        Ok(loaded)
    }

    fn fence_rechecks(&self, now: i64) -> striatum_core::RouteStateResult<Vec<FenceRecheck>> {
        let loaded = fence_rechecks::derive_blocks(
            self.clocks(true),
            &[],
            self.view_origins_in_scan_order(),
            now,
        )
        .map_err(crate::err)?;
        Ok(loaded.blocks.iter().map(FenceRecheck::from).collect())
    }

    fn source_origin(&self, kind: &str, origin_key: &str) -> Option<ViewOrigin> {
        let mut origin = self
            .block(kind, origin_key)
            .and_then(|row| self.view_origin(row))?;
        if kind == "credential" {
            origin.quota = None;
        }
        Some(origin)
    }

    fn eligibility(&self) -> Vec<crate::AccountEligibilityFact> {
        let mut rows: Vec<_> = self
            .rows(7)
            .filter_map(|(_, record)| match record {
                Record::Eligibility(row) => Some(row.clone()),
                _ => None,
            })
            .collect();
        rows.sort_by(|a, b| {
            (&a.provider, &a.credential_id, &a.model).cmp(&(
                &b.provider,
                &b.credential_id,
                &b.model,
            ))
        });
        rows
    }

    fn size_refusals(&self) -> Vec<crate::AccountSizeRefusal> {
        let mut rows: Vec<_> = self
            .block_rows()
            .filter(|row| {
                row.source_present && matches!(row.block.kind.as_str(), "size" | "non_quota_size")
            })
            .collect();
        rows.sort_by_key(|row| {
            (
                u8::from(row.block.kind == "non_quota_size"),
                row.source_order,
            )
        });
        rows.into_iter().filter_map(StoredBlock::size).collect()
    }

    fn size_refusal(
        &self,
        quota: bool,
        provider: &str,
        credential: &str,
        model: &str,
        size_class: i64,
    ) -> Option<crate::AccountSizeRefusal> {
        self.block(
            if quota { "size" } else { "non_quota_size" },
            &serde_json::json!([provider, credential, model, size_class]).to_string(),
        )
        .filter(|row| row.source_present)
        .and_then(StoredBlock::size)
    }

    fn route_fences(&self, class: &str) -> Vec<crate::AccountRouteFence> {
        let mut rows: Vec<_> = self
            .block_rows()
            .filter(|row| {
                row.source_present
                    && row.wall_class.as_deref() == Some(class)
                    && if class == "quota" {
                        row.block.kind == "account_route"
                    } else {
                        matches!(
                            row.block.kind.as_str(),
                            "non_quota_route" | "transient_route"
                        )
                    }
            })
            .filter_map(StoredBlock::route_fence)
            .collect();
        rows.sort_by(|a, b| {
            (&a.provider, &a.credential_id, &a.model).cmp(&(
                &b.provider,
                &b.credential_id,
                &b.model,
            ))
        });
        rows
    }

    fn account_route_fence(
        &self,
        provider: &str,
        credential: &str,
        model: &str,
    ) -> Option<(crate::AccountRouteFence, Option<&str>, Option<i64>)> {
        let row = self
            .block(
                "account_route",
                &serde_json::json!([provider, credential, model]).to_string(),
            )
            .filter(|row| row.source_present)?;
        Some((
            row.route_fence()?,
            row.wall_class.as_deref(),
            row.last_reported_at,
        ))
    }

    fn non_quota_route_fence(
        &self,
        provider: &str,
        credential: &str,
        model: &str,
        wall_kind: &str,
    ) -> Option<(crate::AccountRouteFence, i64)> {
        let row = self
            .block(
                if wall_kind == "transient" {
                    "transient_route"
                } else {
                    "non_quota_route"
                },
                &serde_json::json!([provider, credential, model]).to_string(),
            )
            .filter(|row| row.source_present && row.wall_class.as_deref() == Some(wall_kind))?;
        Some((row.route_fence()?, row.block.evidence_at))
    }

    fn route_fence_latest_reports(&self) -> BTreeMap<String, i64> {
        let mut reports = BTreeMap::new();
        for row in self
            .block_rows()
            .filter(|row| row.source_present && row.block.kind == "account_route")
        {
            if let Some(credential) = &row.block.scope.credential_id {
                let at = row
                    .last_reported_at
                    .or(row.recorded_at)
                    .unwrap_or(row.block.evidence_at);
                reports
                    .entry(credential.clone())
                    .and_modify(|latest: &mut i64| *latest = (*latest).max(at))
                    .or_insert(at);
            }
        }
        reports
    }

    fn entitlement_fences(
        &self,
    ) -> Vec<striatum_core::route_state_store::CredentialEntitlementFence> {
        let mut rows: Vec<_> = self
            .block_rows()
            .filter_map(StoredBlock::entitlement)
            .collect();
        rows.sort_by(|a, b| {
            (&a.provider_id, &a.credential_id).cmp(&(&b.provider_id, &b.credential_id))
        });
        rows
    }

    fn entitlement_fence(
        &self,
        provider: &str,
        credential: &str,
    ) -> Option<striatum_core::route_state_store::CredentialEntitlementFence> {
        self.block(
            "credential",
            &serde_json::json!([provider, credential]).to_string(),
        )
        .and_then(StoredBlock::entitlement)
    }

    fn account_fence(
        &self,
        fence_id: &str,
    ) -> Option<(crate::AccountFenceRow, Option<&str>, &str)> {
        let row = self
            .block("account", &serde_json::json!([fence_id]).to_string())
            .filter(|row| row.source_present)?;
        let Meta::Account(meta) = &row.meta else {
            return None;
        };
        Some((
            row.account_fence()?,
            row.pool_key_json.as_deref(),
            &meta.recovery_set_json,
        ))
    }

    fn active_fences(&self, provider: &str) -> Vec<crate::AccountFenceRow> {
        let mut rows: Vec<_> = self
            .block_rows()
            .filter(|row| row.block.scope.provider == provider)
            .filter_map(StoredBlock::account_fence)
            .collect();
        rows.sort_by(|a, b| (a.created_at, &a.fence_id).cmp(&(b.created_at, &b.fence_id)));
        rows
    }

    fn active_fence_providers(&self) -> Vec<String> {
        self.block_rows()
            .filter(|row| row.source_present && row.block.kind == "account")
            .map(|row| row.block.scope.provider.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn active_pool_fences(
        &self,
        provider: &str,
    ) -> striatum_core::RouteStateResult<Vec<crate::AccountPoolFenceRow>> {
        use striatum_core::accounts::PoolKey;
        self.active_fences(provider)
            .into_iter()
            .map(|evidence| {
                let row = self
                    .block(
                        "account",
                        &serde_json::json!([evidence.fence_id]).to_string(),
                    )
                    .expect("active fence has its block");
                let Meta::Account(meta) = &row.meta else {
                    unreachable!("account metadata was decoded by kind")
                };
                let mut recovery_set: Vec<PoolKey> =
                    serde_json::from_str(&meta.recovery_set_json).map_err(crate::err)?;
                let healed = self
                    .success_pools()
                    .filter(|pool| {
                        pool.fence_id == evidence.fence_id
                            && pool.evidence_at == evidence.observed_at.min(evidence.created_at)
                    })
                    .map(|pool| serde_json::from_str::<PoolKey>(&pool.pool_key_json))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(crate::err)?;
                recovery_set.retain(|pool| !healed.contains(pool));
                Ok(crate::AccountPoolFenceRow {
                    evidence,
                    pool_key: row
                        .pool_key_json
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(crate::err)?,
                    recovery_set,
                })
            })
            .collect()
    }

    fn credential_quota_classification(
        &self,
        provider: &str,
        credential: &str,
        proposed: bool,
    ) -> (bool, bool) {
        if !self.enabled {
            return (proposed, false);
        }
        let classified = self
            .block(
                "credential",
                &serde_json::json!([provider, credential]).to_string(),
            )
            .filter(|row| row.source_present && row.managed && row.block.classified);
        (
            classified.map_or(proposed, |row| {
                row.block.lifetime == striatum_core::blocks::LifetimeClass::Quota
            }),
            classified.is_none(),
        )
    }

    fn admission_exclusions(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> Vec<crate::AccountExclusionRow> {
        let Some(table) = self.tables.first() else {
            return Vec::new();
        };
        let mut rows: Vec<_> = table
            .scoped(&exclusion_scope(principal, admission_id))
            .filter_map(|(_, record)| match record {
                Record::Block(row) => row.exclusion(),
                _ => None,
            })
            .collect();
        rows.sort_by(|a, b| a.credential_id.cmp(&b.credential_id));
        rows
    }

    fn exclusion_count(&self, principal: &str, credential_id: &str) -> i64 {
        self.block_rows()
            .filter(|row| {
                row.source_present
                    && row.block.kind == "principal"
                    && row.block.scope.principal.as_deref() == Some(principal)
                    && row.block.scope.credential_id.as_deref() == Some(credential_id)
            })
            .count() as i64
    }

    fn authorizations(&self) -> impl Iterator<Item = &ProbeAuthorization> {
        self.rows(1).filter_map(|(_, record)| match record {
            Record::Authorization(row) => Some(row),
            _ => None,
        })
    }

    fn authorization_owners(&self) -> Vec<String> {
        self.authorizations()
            .map(|row| row.owner.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn authorizations_of(&self, owner: &str) -> Vec<ProbeAuthorization> {
        let mut rows: Vec<_> = self
            .authorizations()
            .filter(|row| row.owner == owner)
            .cloned()
            .collect();
        rows.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        rows
    }

    fn success_pools(&self) -> impl Iterator<Item = &SuccessPool> {
        self.rows(2).filter_map(|(_, record)| match record {
            Record::Success(row) => Some(row),
            _ => None,
        })
    }

    fn probe_release(&self, owner: &str) -> Vec<(ProbeAuthorization, bool)> {
        self.authorizations_of(owner)
            .into_iter()
            .map(|row| {
                let current =
                    self.clock(&row.kind, &row.origin_key)
                        .is_some_and(|(clock, cleared)| {
                            cleared.is_none()
                                && clock.evidence == row.evidence
                                && clock.evidence_at == row.evidence_at
                                && clock.claim_generation == row.claim_generation
                        });
                (row, current)
            })
            .collect()
    }

    fn apply_probe_release(&mut self, owner: &str, now: i64) {
        for (authorization, current) in self.probe_release(owner) {
            if !current {
                continue;
            }
            let mut row = self
                .block(&authorization.kind, &authorization.origin_key)
                .expect("current authorization has a block")
                .clone();
            row.block.next_recheck_at = now;
            row.block.last_claimed_at = None;
            row.block.claim_owner = None;
            row.block.claim_generation += 1;
            let key = serde_json::json!([row.block.kind, row.block.origin_key]).to_string();
            self.tables[0].put(row.rowid, key, Some(Arc::new(Record::Block(row))));
        }
        let rowids: Vec<_> = self
            .rows(1)
            .filter_map(|(rowid, record)| match record {
                Record::Authorization(row) if row.owner == owner => Some(rowid),
                _ => None,
            })
            .collect();
        for rowid in rowids {
            self.tables[1].remove(rowid);
        }
    }

    fn clock_rechecks(&self) -> Vec<FenceRecheck> {
        let mut rows: Vec<_> = self
            .clocks(true)
            .into_values()
            .filter(|(_, cleared)| cleared.is_none())
            .map(|(block, _)| FenceRecheck::from(&block))
            .collect();
        rows.sort_by(|a, b| (&a.kind, &a.origin_key).cmp(&(&b.kind, &b.origin_key)));
        rows
    }

    fn needs_sync(&self) -> bool {
        let mut clocks: HashMap<_, _> = self
            .stored_clocks()
            .map(|(block, _)| {
                (
                    (block.kind.clone(), block.origin_key.clone()),
                    block.evidence.clone(),
                )
            })
            .collect();
        for origin in self.view_origins_in_scan_order() {
            if clocks.remove(&(origin.kind, origin.origin_key)).as_deref()
                != Some(origin.evidence.as_str())
            {
                return true;
            }
        }
        clocks.keys().any(|(kind, _)| kind != "observation")
    }

    fn pool_key_rows(
        &self,
        provider: &str,
    ) -> rusqlite::Result<Vec<(String, Option<i64>, Option<i64>, Option<i64>)>> {
        let mut rows = Vec::new();
        for (_, record) in self.rows(4) {
            let Record::Pool(row) = record else {
                continue;
            };
            if row.provider != provider {
                continue;
            }
            let last = match &row.last_used_at {
                rusqlite::types::Value::Null => None,
                rusqlite::types::Value::Integer(at) => Some(*at),
                other => {
                    return Err(rusqlite::Error::InvalidColumnType(
                        1,
                        "pool_key_state time".into(),
                        other.data_type(),
                    ))
                }
            };
            let key = serde_json::json!([provider, row.credential_id]).to_string();
            let deadline = |kind| {
                self.block(kind, &key)
                    .filter(|row| row.source_present)
                    .and_then(|row| row.deadline)
                    .map(|at| at as i64)
            };
            rows.push((
                row.credential_id.clone(),
                last,
                deadline("pool_billing"),
                deadline("pool_rate"),
            ));
        }
        Ok(rows)
    }

    fn provider_cooldown(
        &self,
        provider: &str,
    ) -> striatum_core::RouteStateResult<Option<striatum_core::ProviderCooldownState>> {
        let key = crate::route_snapshot::provider_lookup_key(provider)?;
        let telemetry = self.rows(5).find_map(|(_, record)| match record {
            Record::Provider(row) if row.provider_key() == key => Some(row),
            _ => None,
        });
        let Some(telemetry) = telemetry else {
            return Ok(None);
        };
        let mut row = telemetry.clone();
        if let Some(block) = self
            .block("provider", &serde_json::json!([key]).to_string())
            .filter(|row| row.source_present)
        {
            row.cooldown_until = block.cooldown_until;
            row.quota_exhausted_until = block.quota_exhausted_until;
            row.wall_class = block.wall_class.clone();
        }
        crate::route_snapshot::provider_cooldown(Some(&row))
    }

    fn route_state_snapshot(
        &self,
        now: f64,
        recheck_now: i64,
    ) -> striatum_core::RouteStateResult<striatum_core::model_router::ModelRouteStateSnapshot> {
        let mut rows = Vec::new();
        for (rowid, record) in self.rows(6) {
            let Record::Route { row: telemetry, .. } = record else {
                continue;
            };
            let key = serde_json::json!([telemetry.route_key]).to_string();
            let wall = self.block("route", &key).filter(|row| row.source_present);
            let failure = self.block("failure", &key).filter(|row| row.source_present);
            let mut row = telemetry.clone();
            if let Some(block) = wall {
                row.cooldown_until = block.cooldown_until;
                row.quota_exhausted_until = block.quota_exhausted_until;
                row.wall_class = block.wall_class.clone();
                row.wall_unscoped = block.wall_unscoped;
            } else if let Some(block) = failure {
                row.wall_class = block.wall_class.clone();
                row.wall_unscoped = block.wall_unscoped;
            }
            if let Some(block) = failure {
                row.failure_backoff_until = block.deadline;
                row.failure_backoff_reason = block.reason.clone();
            }
            rows.push((rowid, row));
        }
        rows.sort_by_key(|(rowid, _)| *rowid);
        let rows: Vec<_> = rows.into_iter().map(|(_, row)| row).collect();
        crate::route_snapshot::overlay(&rows, &self.fence_rechecks(recheck_now)?, self.enabled, now)
    }

    fn routes_stamped_by_provider(
        &self,
        provider: &str,
        evidence_at: i64,
        admitted_at: i64,
    ) -> Vec<(
        String,
        String,
        rusqlite::types::Value,
        rusqlite::types::Value,
    )> {
        let Some(covering) = self.block("provider", &serde_json::json!([provider]).to_string())
        else {
            return Vec::new();
        };
        self.block_rows()
            .filter(|row| {
                row.source_present
                    && row.block.kind == "route"
                    && row.block.scope.provider == provider
                    && row.scope_error == covering.scope_error
                    && row.block.evidence_at == evidence_at
                    && evidence_at < admitted_at
                    && row.cooldown_until.unwrap_or(0.0) <= covering.cooldown_until.unwrap_or(0.0)
                    && row.quota_exhausted_until.unwrap_or(0.0)
                        <= covering.quota_exhausted_until.unwrap_or(0.0)
            })
            .filter_map(|row| {
                let key: Vec<String> = serde_json::from_str(&row.block.origin_key).ok()?;
                Some((
                    key.first()?.clone(),
                    row.block.evidence.clone(),
                    row.scope_error
                        .clone()
                        .map_or(rusqlite::types::Value::Null, rusqlite::types::Value::Text),
                    rusqlite::types::Value::Integer(row.block.evidence_at),
                ))
            })
            .collect()
    }

    fn compare_rows<'a>(
        &self,
        conn: &Connection,
        table: usize,
        statement: &str,
        params: impl rusqlite::Params,
        held: impl Iterator<Item = (i64, &'a Record)>,
    ) -> rusqlite::Result<()> {
        let sql = conn
            .prepare_cached(statement)?
            .query_map(params, |row| decode(table, row))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut sql: Vec<_> = sql
            .into_iter()
            .filter_map(|(rowid, _, record)| record.map(|record| (rowid, record)))
            .collect();
        let mut state: Vec<_> = held.collect();
        sql.sort_by_key(|(rowid, _)| *rowid);
        state.sort_by_key(|(rowid, _)| *rowid);
        if sql.len() != state.len()
            || !sql
                .iter()
                .zip(state)
                .all(|((sql_rowid, sql), (rowid, state))| {
                    *sql_rowid == rowid && sql.as_ref() == state
                })
        {
            return Err(super::diverged(format_args!(
                "the {} rows differ from the state's",
                TABLES[table]
            )));
        }
        Ok(())
    }

    fn check_block(&self, conn: &Connection, kind: &str, origin_key: &str) -> rusqlite::Result<()> {
        let statement = format!(
            "{} {} WHERE kind=?1 AND origin_key=?2",
            super::STATE_CHECK,
            select(0)
        );
        let key = serde_json::json!([kind, origin_key]).to_string();
        let table = &self.tables[0];
        let held = table
            .keys
            .get(&key)
            .and_then(|rowid| table.rows.get(rowid).map(|(_, row)| (*rowid, row.as_ref())));
        self.compare_rows(conn, 0, &statement, [kind, origin_key], held.into_iter())
    }

    fn check_clock(&self, conn: &Connection, kind: &str, origin_key: &str) -> rusqlite::Result<()> {
        let sql = conn.query_row(&format!("{} SELECT {} FROM block WHERE kind=?1 AND origin_key=?2 AND managed=1 AND (cleared_by IS NULL OR cleared_by='kept') AND (source_present=1 OR kind='observation')",super::STATE_CHECK,block_storage::CLOCK_COLUMNS),[kind,origin_key],|row| fence_rechecks::clock_row(row,0)).optional()?;
        let state = self
            .clock(kind, origin_key)
            .map(|(block, cleared)| (block.clone(), cleared));
        if sql != state {
            return Err(super::diverged(format_args!(
                "the clock of {kind} {origin_key} differs from the state's"
            )));
        }
        Ok(())
    }

    fn check_source_record(
        &self,
        conn: &Connection,
        kind: &str,
        origin_key: &str,
    ) -> rusqlite::Result<()> {
        let sql = conn
            .query_row(
                &format!(
                    "{} {} WHERE kind=?1 AND origin_key=?2",
                    super::STATE_CHECK,
                    select(0)
                ),
                [kind, origin_key],
                block_storage::decode,
            )
            .optional()?
            .and_then(|row| row.source_projection());
        let state = self
            .block(kind, origin_key)
            .and_then(StoredBlock::source_projection);
        if sql != state {
            return Err(super::diverged(format_args!(
                "the source of {kind} {origin_key} differs from the state's"
            )));
        }
        Ok(())
    }

    fn check_source(
        &self,
        conn: &Connection,
        kind: &str,
        origin_key: &str,
        provider: &str,
    ) -> rusqlite::Result<()> {
        let parts = match kind {
            "account" | "provider" | "route" | "failure" => 1,
            "credential" | "pool_billing" | "pool_rate" | "observation" => 2,
            "account_route" | "non_quota_route" | "transient_route" | "principal" => 3,
            "size" | "non_quota_size" => 4,
            _ => return Ok(()),
        };
        if serde_json::from_str::<Vec<serde_json::Value>>(origin_key)
            .map_or(true, |values| values.len() != parts)
        {
            return Err(super::diverged(format_args!(
                "{kind} origin key {origin_key} is invalid"
            )));
        }
        self.check_source_record(conn, kind, origin_key)?;
        if kind == "route" {
            self.check_source_record(conn, "provider", &serde_json::json!([provider]).to_string())?;
        }
        if kind == "credential" {
            let prefix = format!("credential-quota:{origin_key}:");
            let upper = format!("credential-quota:{origin_key};");
            let prefix_bound = |value: &str| {
                let json = serde_json::to_string(value).expect("a string is valid JSON");
                format!("[{}", &json[..json.len() - 1])
            };
            let mut sql = conn.prepare_cached(&format!("{} SELECT origin_key FROM block WHERE kind='account' AND origin_key>=?1 AND origin_key<?2 AND source_present=1 ORDER BY origin_key",super::STATE_CHECK))?
                .query_map([prefix_bound(&prefix),prefix_bound(&upper)],|row| row.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut state: Vec<_> = self
                .block_rows()
                .filter(|row| {
                    row.source_present
                        && row.block.kind == "account"
                        && row.fence_id().is_some_and(|id| id.starts_with(&prefix))
                })
                .map(|row| row.block.origin_key.clone())
                .collect();
            sql.sort();
            state.sort();
            if sql != state {
                return Err(super::diverged(
                    "credential quota holds differ from the state's",
                ));
            }
        }
        Ok(())
    }

    fn check_provider_rows(&self, conn: &Connection, provider: &str) -> rusqlite::Result<()> {
        if provider.is_empty() {
            let mut sql = conn.prepare_cached(&format!("{} SELECT rowid,{},cleared_models FROM block WHERE provider=?1 AND managed=1 AND (cleared_by IS NULL OR cleared_by='kept') AND (source_present=1 OR kind='observation')",super::STATE_CHECK,block_storage::CLOCK_COLUMNS))?.query_map([provider],|row| {
                let (block,cleared) = fence_rechecks::clock_row(row,1)?;
                let json: String = row.get(20)?;
                let models: Vec<String> = serde_json::from_str(&json).map_err(|e| rusqlite::Error::FromSqlConversionFailure(20,rusqlite::types::Type::Text,Box::new(e)))?;
                Ok((row.get::<_,i64>(0)?,block,cleared,models))
            })?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut state: Vec<_> = self.tables[0]
                .scoped(&provider_scope(provider))
                .filter_map(|(rowid, record)| match record {
                    Record::Block(row) if row.managed => Some((
                        rowid,
                        row.block.clone(),
                        row.cleared_at,
                        row.cleared_models.clone(),
                    )),
                    _ => None,
                })
                .collect();
            sql.sort_by_key(|row| row.0);
            state.sort_by_key(|row| row.0);
            if sql != state {
                return Err(super::diverged("principal clocks differ from the state's"));
            }
            return Ok(());
        }
        for (table, column) in [
            (0, "provider"),
            (4, "provider"),
            (5, "provider_key"),
            (6, "provider_id"),
        ] {
            if provider.is_empty() && table != 0 {
                continue;
            }
            self.compare_rows(
                conn,
                table,
                &format!("{} {} WHERE {column}=?1", super::STATE_CHECK, select(table)),
                [provider],
                self.tables[table].scoped(&provider_scope(provider)),
            )?;
        }
        if provider.is_empty() {
            return Ok(());
        }
        let fences: Vec<_> = self.tables[0]
            .scoped(&provider_scope(provider))
            .filter_map(|(_, record)| match record {
                Record::Block(row) if row.source_present && row.block.kind == "account" => {
                    row.fence_id()
                }
                _ => None,
            })
            .collect();
        let scopes: Vec<_> = fences
            .iter()
            .map(|id| serde_json::json!(["fence", id]).to_string())
            .collect();
        self.compare_rows(conn,2,&format!("{} {} WHERE fence_id IN (SELECT json_extract(origin_key,'$[0]') FROM block WHERE kind='account' AND provider=?1 AND source_present=1)",super::STATE_CHECK,select(2)),[provider],scopes.iter().flat_map(|scope| self.tables[2].scoped(scope)))
    }

    fn check_exclusions(
        &self,
        conn: &Connection,
        principal: &str,
        admission_id: &str,
    ) -> rusqlite::Result<()> {
        let mut sql: Vec<_> = conn
            .prepare_cached(&format!(
                "{} {} WHERE kind='principal' AND principal=?1 AND admission_id=?2",
                super::STATE_CHECK,
                select(0)
            ))?
            .query_map([principal, admission_id], block_storage::decode)?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .filter_map(|row| row.source_projection())
            .collect();
        let mut state: Vec<_> = self.tables[0]
            .scoped(&exclusion_scope(principal, admission_id))
            .filter_map(|(_, record)| match record {
                Record::Block(row) => row.source_projection(),
                _ => None,
            })
            .collect();
        sql.sort_by_key(|row| row.rowid);
        state.sort_by_key(|row| row.rowid);
        if sql != state {
            return Err(super::diverged(
                "principal exclusions differ from the state's",
            ));
        }
        Ok(())
    }
}

impl StoredBlock {
    fn source_projection(&self) -> Option<Self> {
        if !self.source_present {
            return None;
        }
        let mut row = self.clone();
        // A source check and a clock check protect different inputs. A claim
        // moving a clock must not make unchanged source evidence look stale.
        row.block.lifetime = striatum_core::blocks::LifetimeClass::NonQuota;
        row.block.classified = false;
        row.block.next_recheck_at = 0;
        row.block.reset_check_at = None;
        row.block.last_claimed_at = None;
        row.block.claim_owner = None;
        row.block.claim_generation = 0;
        row.cleared_at = None;
        row.cleared_by = None;
        row.managed = false;
        row.cleared_models.clear();
        Some(row)
    }

    fn fence_id(&self) -> Option<String> {
        let key: Vec<String> = serde_json::from_str(&self.block.origin_key).ok()?;
        key.into_iter().next()
    }

    fn account_fence(&self) -> Option<crate::AccountFenceRow> {
        if !self.source_present || self.block.kind != "account" {
            return None;
        }
        let Meta::Account(meta) = &self.meta else {
            return None;
        };
        let key: Vec<String> = serde_json::from_str(&self.block.origin_key).ok()?;
        Some(crate::AccountFenceRow {
            fence_id: key.first()?.clone(),
            provider: self.block.scope.provider.clone(),
            account_id: self.block.scope.account_id.clone()?,
            scope_json: meta.scope_json.clone(),
            fence_json: meta.fence_json.clone(),
            source: meta.source.clone(),
            observed_at: self.observed_at?,
            principal: meta.principal.clone(),
            admission_id: meta.admission_id.clone(),
            observation_id: self.observation_id.clone(),
            created_at: self.created_at?,
            cleared_at: None,
            cleared_reason: self.cleared_reason.clone(),
        })
    }

    fn route_fence(&self) -> Option<crate::AccountRouteFence> {
        let Meta::Route(meta) = &self.meta else {
            return None;
        };
        Some(crate::AccountRouteFence {
            provider: self.block.scope.provider.clone(),
            credential_id: self.block.scope.credential_id.clone()?,
            model: self.block.scope.model.clone()?,
            until_ms: self.deadline? as i64,
            decision_id: meta.decision_id.clone(),
            recorded_at: self.recorded_at?,
        })
    }

    fn size(&self) -> Option<crate::AccountSizeRefusal> {
        Some(crate::AccountSizeRefusal {
            provider: self.block.scope.provider.clone(),
            credential_id: self.block.scope.credential_id.clone()?,
            model: self.block.scope.model.clone()?,
            size_class: self.block.scope.size_class?.try_into().ok()?,
            expires_at: self.deadline? as i64,
            observed_at: self.observed_at?,
        })
    }

    fn entitlement(&self) -> Option<striatum_core::route_state_store::CredentialEntitlementFence> {
        if !self.source_present || self.block.kind != "credential" {
            return None;
        }
        let Meta::Credential(meta) = &self.meta else {
            return None;
        };
        Some(
            striatum_core::route_state_store::CredentialEntitlementFence {
                provider_id: self.block.scope.provider.clone(),
                credential_id: self.block.scope.credential_id.clone()?,
                code: self.code.clone()?,
                listing_fingerprint: meta.listing_fingerprint.clone(),
                observed_at_ms: self.observed_at?,
            },
        )
    }

    fn exclusion(&self) -> Option<crate::AccountExclusionRow> {
        if !self.source_present || self.block.kind != "principal" {
            return None;
        }
        let Meta::Principal(meta) = &self.meta else {
            return None;
        };
        Some(crate::AccountExclusionRow {
            principal: self.block.scope.principal.clone()?,
            admission_id: self.block.scope.admission_id.clone()?,
            credential_id: self.block.scope.credential_id.clone()?,
            runner_view_digest: meta.runner_view_digest.clone(),
            roster_view_digest: meta.roster_view_digest.clone(),
            observation_id: self.observation_id.clone(),
            recorded_at: self.recorded_at?,
        })
    }
}

fn select(table: usize) -> String {
    match table {
        0 => block_storage::select(),
        1 => "SELECT rowid,json_array(owner,kind,origin_key),owner,kind,origin_key,evidence,evidence_at,claimed_at,claim_generation FROM probe_authorization".into(),
        2 => "SELECT rowid,json_array(fence_id,pool_key_json),fence_id,pool_key_json,evidence_at,healed_at FROM success_proof".into(),
        3 => "SELECT rowid,json_array(kind,origin_key,evidence) FROM retirement_tombstone".into(),
        4 => "SELECT rowid,json_array(provider,credential_id),provider,credential_id,last_used_at FROM pool_key_state".into(),
        5 => "SELECT rowid,json_array(provider_key),provider_key,NULL,NULL,quota_pressure,NULL,last_error FROM provider_cooldown_state".into(),
        6 => "SELECT rowid,json_array(route_key),provider_id,route_key,capacity_key,NULL,NULL,quota_pressure,latency_ema_ms,success_ema,NULL,NULL,last_error,NULL,0 FROM model_route_state".into(),
        7 => "SELECT rowid,json_array(provider,credential_id,model),provider,credential_id,model,listing_fingerprint,code,observed_at FROM account_eligibility".into(),
        _ => unreachable!("unknown captured block table"),
    }
}

fn decode(table: usize, row: &Row<'_>) -> rusqlite::Result<(i64, String, Option<Arc<Record>>)> {
    let rowid = row.get(0)?;
    if table == 0 {
        let block = block_storage::decode(row)?;
        let key = serde_json::json!([block.block.kind, block.block.origin_key]).to_string();
        return Ok((
            rowid,
            key,
            block.held().then(|| Arc::new(Record::Block(block))),
        ));
    }
    let key = row.get(1)?;
    let record = match table {
        1 => Record::Authorization(ProbeAuthorization {
            owner: row.get(2)?,
            kind: row.get(3)?,
            origin_key: row.get(4)?,
            evidence: row.get(5)?,
            evidence_at: row.get(6)?,
            claimed_at: row.get(7)?,
            claim_generation: row.get(8)?,
        }),
        2 => Record::Success(SuccessPool {
            fence_id: row.get(2)?,
            pool_key_json: row.get(3)?,
            evidence_at: row.get(4)?,
            healed_at: row.get(5)?,
        }),
        3 => return Ok((rowid, key, None)),
        4 => Record::Pool(PoolTelemetry {
            provider: row.get(2)?,
            credential_id: row.get(3)?,
            last_used_at: row.get(4)?,
        }),
        5 => Record::Provider(crate::route_snapshot::provider_row_at(row, 2)?),
        6 => Record::Route {
            provider: row.get(2)?,
            row: crate::route_snapshot::route_row_at(row, 3)?,
        },
        7 => Record::Eligibility(crate::AccountEligibilityFact {
            provider: row.get(2)?,
            credential_id: row.get(3)?,
            model: row.get(4)?,
            listing_fingerprint: row.get(5)?,
            code: row.get(6)?,
            observed_at: row.get(7)?,
        }),
        _ => unreachable!("unknown captured block table"),
    };
    Ok((rowid, key, Some(Arc::new(record))))
}

#[derive(Clone, Debug, Default)]
struct BlockRows(Vec<(usize, i64, String, Option<Arc<Record>>)>);

fn read_back(conn: &Connection, captured: &[(usize, i64)]) -> rusqlite::Result<BlockRows> {
    let mut rows = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for &(table, rowid) in captured {
        if !seen.insert((table, rowid)) {
            continue;
        }
        let found = conn
            .query_row(
                &format!(
                    "{} {} WHERE rowid=?1",
                    super::MAINTENANCE_READ,
                    select(table)
                ),
                [rowid],
                |row| decode(table, row),
            )
            .optional()?;
        match found {
            Some((rowid, key, record)) => rows.push((table, rowid, key, record)),
            None => rows.push((table, rowid, String::new(), None)),
        }
    }
    Ok(BlockRows(rows))
}

#[test]
fn migrated_state_readers_match_legacy_state_without_virtual_source_rows() {
    let scratch = cortexkit_test_support::ScratchDir::new("consolidated-state-parity");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "consolidated-state-parity",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store.assert_block_parity_fixture_coverage_for_test(now);
    store.with_writer_for_test(|conn| {
        conn.execute("INSERT INTO provider_cooldown_state(provider_key,cooldown_until,wall_observed_at,wall_unscoped,last_error,updated_at) VALUES ('inherit',?1,?2,1,'capacity',?2)",rusqlite::params![now+2000,now-1])?;
        conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,cooldown_until,wall_observed_at,wall_unscoped,last_error,updated_at) VALUES ('inherit/old','inherit/old','inherit','old',?1,?2,1,'capacity',?2)",rusqlite::params![now+1000,now-1])?;
        conn.execute("INSERT INTO fence_recheck_route_clear SELECT kind,origin_key,provider,'zzz',evidence FROM fence_recheck WHERE kind='provider'",[])?;
        conn.execute("INSERT INTO fence_recheck_route_clear SELECT kind,origin_key,provider,'aaa',evidence FROM fence_recheck WHERE kind='provider'",[])?;
        Ok(())
    }).unwrap();
    let times = store.block_parity_times(now).unwrap();
    store
        .with_writer_for_test(|conn| {
            let orphaned_cleared: i64 = conn.query_row("SELECT count(*) FROM fence_recheck r WHERE r.kind!='observation' AND r.cleared_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM fence_origin o WHERE o.kind=r.kind AND o.origin_key=r.origin_key)",[],|row| row.get(0))?;
            assert_eq!(orphaned_cleared,1,"the fixture exercises a cleared orphan before normalization");
            fence_rechecks::sync_at(conn,now)?;
            conn.execute("INSERT INTO fence_probe_authorization SELECT claim_owner,kind,origin_key,evidence,evidence_at,COALESCE(last_claimed_at,evidence_at),claim_generation FROM fence_recheck WHERE claim_owner IS NOT NULL",[])?;
            let legacy = super::BlockState::load(conn)?;
            block_storage::install_migration_function(conn)?;
            conn.execute_batch(block_storage::MIGRATION)?;
            let state = BlockState::load(conn)?;
            assert!(state.enabled());
            assert!(state.shares_storage_with(&state.clone()));
            assert_eq!(state.eligibility(), legacy.eligibility());
            assert_eq!(state.size_refusals(), legacy.size_refusals());
            assert_eq!(
                state.route_fence_latest_reports(),
                legacy.route_fence_latest_reports()
            );
            assert_eq!(state.entitlement_fences(), legacy.entitlement_fences());
            assert_eq!(
                state.active_fence_providers(),
                legacy.active_fence_providers()
            );
            assert_eq!(state.authorization_owners(), legacy.authorization_owners());
            assert_eq!(state.clock_rechecks(), legacy.clock_rechecks());
            assert_eq!(state.live_clocks(),legacy.live_clocks());
            assert!(!state.needs_sync(), "migration leaves no orphaned clocks to reconcile");
            for class in ["quota", "transient", "billing", "unknown"] {
                assert_eq!(
                    state.route_fences(class),
                    legacy.route_fences(class),
                    "{class}"
                );
            }
            let quarantined: std::collections::HashSet<_> = legacy.quarantined().into_iter().collect();
            let kept = |origin: &ViewOrigin| !quarantined.contains(&(origin.kind.clone(),origin.origin_key.clone()));
            let origins: Vec<_> = legacy.view_origins().into_iter().filter(kept).collect();
            assert_eq!(state.view_origins(), origins);
            assert_eq!(
                state.view_origins_in_scan_order(),
                legacy.view_origins_in_scan_order().into_iter().filter(kept).collect::<Vec<_>>()
            );
            for at in times {
                let before = legacy.blocks(at).unwrap();
                let after = state.blocks(at).unwrap();
                assert_eq!(after.blocks, before.blocks, "blocks at {at}");
                assert_eq!(
                    after.cleared_evidence, before.cleared_evidence,
                    "cleared at {at}"
                );
                assert_eq!(
                    state.fence_rechecks(at).unwrap(),
                    legacy.fence_rechecks(at).unwrap(),
                    "scan order at {at}"
                );
                let before = legacy.route_state_snapshot(at as f64, at).unwrap();
                let after = state.route_state_snapshot(at as f64, at).unwrap();
                assert_eq!(after.now,before.now);
                for (before,after) in [(&before.by_route,&after.by_route),(&before.by_capacity,&after.by_capacity)] {
                    assert_eq!(after.keys().collect::<std::collections::BTreeSet<_>>(),before.keys().collect::<std::collections::BTreeSet<_>>());
                    for (key,row) in before { assert_eq!(after.get(key),Some(row),"route/capacity {key} at {at}"); }
                }
            }
            for origin in origins {
                let provider = origin.provider.as_str();
                let credential = origin.credential_id.as_deref().unwrap_or("missing");
                let model = origin.model.as_deref().unwrap_or("missing");
                assert_eq!(
                    state.entitlement_fence(provider, credential),
                    legacy.entitlement_fence(provider, credential)
                );
                assert_eq!(
                    state.active_fences(provider),
                    legacy.active_fences(provider)
                );
                assert_eq!(
                    state.active_pool_fences(provider).unwrap(),
                    legacy.active_pool_fences(provider).unwrap()
                );
                for proposed in [false, true] {
                    assert_eq!(
                        state.credential_quota_classification(provider, credential, proposed),
                        legacy.credential_quota_classification(provider, credential, proposed)
                    );
                }
                let principal = origin.principal.as_deref().unwrap_or("missing");
                let admission = origin.admission_id.as_deref().unwrap_or("missing");
                assert_eq!(
                    state.admission_exclusions(principal, admission),
                    legacy.admission_exclusions(principal, admission)
                );
                assert_eq!(
                    state.exclusion_count(principal, credential),
                    legacy.exclusion_count(principal, credential)
                );
                assert_eq!(
                    state.source_origin(&origin.kind, &origin.origin_key),
                    legacy
                        .source_origin(&origin.kind, &origin.origin_key)
                        .cloned()
                );
                assert_eq!(
                    state.account_route_fence(provider, credential, model),
                    legacy
                        .account_route_fence(provider, credential, model)
                        .map(|(row, class, at)| (row.clone(), class, at))
                );
                for class in ["billing", "transient"] {
                    assert_eq!(
                        state.non_quota_route_fence(provider, credential, model, class),
                        legacy
                            .non_quota_route_fence(provider, credential, model, class)
                            .map(|(row, at)| (row.clone(), at))
                    );
                }
                if let Some(size) = origin.size_class {
                    for quota in [false, true] {
                        assert_eq!(
                            state.size_refusal(quota, provider, credential, model, size),
                            legacy
                                .size_refusal(quota, provider, credential, model, size)
                                .cloned()
                        );
                    }
                }
                if !provider.is_empty() {
                    assert_eq!(
                        state.provider_cooldown(provider).unwrap(),
                        legacy.provider_cooldown(provider).unwrap()
                    );
                    let mut before = legacy.pool_key_rows(provider)?;
                    let mut after = state.pool_key_rows(provider)?;
                    before.sort();
                    after.sort();
                    assert_eq!(after, before);
                }
                state.check_source(conn, &origin.kind, &origin.origin_key, provider)?;
                state.check_clock(conn, &origin.kind, &origin.origin_key)?;
                state.check_provider_rows(conn, provider)?;
                state.check_exclusions(conn, principal, admission)?;
                let mut before = legacy.routes_stamped_by_provider(provider,origin.evidence_at,now+1);
                let mut after = state.routes_stamped_by_provider(provider,origin.evidence_at,now+1);
                before.sort_by(|a,b| a.0.cmp(&b.0)); after.sort_by(|a,b| a.0.cmp(&b.0)); assert_eq!(after,before);
            }
            for fence in legacy.active_fences("cpu-provider-0") {
                assert_eq!(
                    state.account_fence(&fence.fence_id),
                    legacy
                        .account_fence(&fence.fence_id)
                        .map(|(row, pool, recovery)| (row.clone(), pool, recovery))
                );
            }
            for owner in legacy.authorization_owners() {
                let mut before = legacy.clocks_claimed_by(&owner);
                let mut after = state.clocks_claimed_by(&owner);
                before.sort_by(|a,b| (&a.0.kind,&a.0.origin_key).cmp(&(&b.0.kind,&b.0.origin_key)));
                after.sort_by(|a,b| (&a.0.kind,&a.0.origin_key).cmp(&(&b.0.kind,&b.0.origin_key)));
                assert_eq!(after,before);
                assert_eq!(state.probe_release(&owner), legacy.probe_release(&owner));
                let mut before = legacy.clone();
                let mut after = state.clone();
                before.apply_probe_release(&owner, now);
                after.apply_probe_release(&owner, now);
                assert_eq!(
                    after.blocks(now).unwrap().blocks,
                    before.blocks(now).unwrap().blocks
                );
                assert_eq!(after.authorization_owners(), before.authorization_owners());
            }
            assert_eq!(
                state
                    .table_sizes()
                    .iter()
                    .find(|(name, _)| *name == "retirement_tombstone")
                    .unwrap()
                    .1,
                0
            );
            assert_eq!(state.block_rows().count(), state.tables[0].rows.len);
            Ok(())
        })
        .unwrap();
}

#[test]
fn captured_physical_rows_equal_reload_after_replace_move_rollback_and_retirement() {
    let scratch = cortexkit_test_support::ScratchDir::new("consolidated-state-capture");
    let store = crate::SqliteRouteStateStore::open_at_version_for_test(
        &crate::dev_descriptor(
            "consolidated-state-capture",
            scratch.path().join("state.db").to_string_lossy(),
        ),
        93,
    )
    .unwrap();
    let now = crate::now_ms();
    store.seed_block_parity_fixture_for_test(now);
    store.with_writer_for_test(|conn| {
        block_storage::install_migration_function(conn)?;
        conn.execute_batch(block_storage::MIGRATION)?;
        conn.execute("INSERT INTO probe_authorization SELECT 'capture-owner',kind,origin_key,evidence,evidence_at,evidence_at,claim_generation FROM block WHERE kind='credential' AND source_present=1",[])?;
        let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
        let changed = Arc::clone(&captured);
        conn.update_hook(Some(move |_, database: &str, table: &str, rowid: i64| {
            if database == "main" {
                if let Some(index) = TABLES.iter().position(|name| *name == table) {
                    changed.lock().unwrap().push((index,rowid));
                }
            }
        }));
        conn.authorizer(Some(|context: rusqlite::hooks::AuthContext<'_>| {
            if matches!(context.action,rusqlite::hooks::AuthAction::Delete { table_name } if TABLES.contains(&table_name)) {
                rusqlite::hooks::Authorization::Ignore
            } else { rusqlite::hooks::Authorization::Allow }
        }));
        let mut state = BlockState::load(conn)?;
        let original = state.clone();
        assert!(state.shares_storage_with(&original));
        let commands = [
            "UPDATE block SET claim_generation=claim_generation+1,claim_owner='claimed',cleared_models='[\"partial/model\"]' WHERE kind='credential'",
            "INSERT OR REPLACE INTO block SELECT * FROM block WHERE kind='credential'",
            "UPDATE block SET origin_key='[\"moved/route\"]' WHERE kind='failure'",
            "SAVEPOINT rejected; UPDATE block SET claim_owner='rolled-back' WHERE kind='account'; ROLLBACK TO rejected; RELEASE rejected",
            "DELETE FROM block WHERE kind='non_quota_size'",
            "DELETE FROM probe_authorization",
            "UPDATE pool_key_state SET last_used_at=last_used_at+1",
            "UPDATE provider_cooldown_state SET quota_pressure=0.75",
            "UPDATE model_route_state SET latency_ema_ms=123",
            "INSERT OR REPLACE INTO success_proof VALUES ('parity-fence','{}',17,19)",
            "INSERT OR REPLACE INTO account_eligibility(provider,credential_id,model,listing_fingerprint,code,observed_at) VALUES ('new-provider','new-credential','new-model','fingerprint','known',21)",
            "INSERT INTO retirement_tombstone SELECT kind,origin_key,evidence,evidence_at,evidence_at+1 FROM block WHERE kind='size'",
        ];
        for (step,command) in commands.into_iter().enumerate() {
            captured.lock().unwrap().clear();
            conn.execute_batch(command)?;
            let changes = captured.lock().unwrap().clone();
            assert!(!changes.is_empty(),"command {step} exercises the update hook");
            let changes = read_back(conn,&changes)?;
            state.apply(&changes);
            let loaded = BlockState::load(conn)?;
            assert_eq!(state,loaded,"memory equals load after command {step}: {command}");
            assert_eq!(state.fingerprints(),loaded.fingerprints());
            for table in &state.tables { assert_eq!(table.rows.len,table.keys.len); }
            assert!(!state.shares_storage_with(&original));
        }
        assert!(state.block_rows().any(|row| row.block.kind == "credential" && !row.cleared_models.is_empty()));
        assert!(!state.block_rows().any(|row| row.block.kind == "size"));
        assert!(state.tables[1].rows.len == 0,"whole-table deletion reports every authorization");
        assert!(state.block("failure","[\"moved/route\"]").is_some());
        Ok(())
    }).unwrap();
}
