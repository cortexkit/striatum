//! Sweep-private repairs to durable block records. No operation writes SQL.
use super::*;
use crate::block_storage::{AccountMeta, Deadline};
use striatum_core::blocks::LifetimeClass;

/// A repair retained only by the private state of one evaluation sweep.
pub enum DryRunRepair {
    ClearFence(String, String),
    ClearHeld(crate::AccountFenceRow, String),
    ClearCredential(String, String),
    Classify(String, String, bool),
    InsertFence(crate::AccountPoolFenceRow),
}

impl BlockState {
    fn edit_blocks(&mut self, mut edit: impl FnMut(&mut StoredBlock)) {
        let rows: Vec<_> = self.block_rows().cloned().collect();
        for mut row in rows {
            let before = row.clone();
            edit(&mut row);
            if before != row {
                self.put_private(row);
            }
        }
    }

    fn put_private(&mut self, mut row: StoredBlock) {
        if row.source_order == 0 {
            row.source_order = self
                .block_rows()
                .filter(|prior| prior.source_present && prior.block.kind == row.block.kind)
                .map(|prior| prior.source_order)
                .max()
                .unwrap_or(0)
                + 1;
        }
        let key = serde_json::json!([row.block.kind, row.block.origin_key]).to_string();
        if let Some(rowid) = self.tables[0].keys.get(&key) {
            row.rowid = *rowid;
        } else if row.rowid == 0 {
            row.rowid = self.tables[0]
                .rows
                .iter()
                .map(|(id, _)| *id)
                .max()
                .unwrap_or(0)
                + 1;
        }
        self.tables[0].put(row.rowid, key, Some(Arc::new(Record::Block(row))));
    }

    /// A credential keeps the first classification made within the sweep.
    pub fn dry_run_repair(
        &mut self,
        repair: DryRunRepair,
        now: i64,
    ) -> striatum_core::RouteStateResult<bool> {
        match repair {
            DryRunRepair::ClearFence(id, _) => self.edit_blocks(|row| {
                if row.block.kind == "account" && row.fence_id().as_deref() == Some(&id) {
                    row.source_present = false;
                }
            }),
            DryRunRepair::ClearHeld(row, reason) => {
                return self.dry_run_repair(DryRunRepair::ClearFence(row.fence_id, reason), now)
            }
            DryRunRepair::ClearCredential(provider, credential) => self.edit_blocks(|row| {
                if row.block.kind == "credential"
                    && row.block.scope.provider == provider
                    && row.block.scope.credential_id.as_deref() == Some(&credential)
                {
                    row.source_present = false;
                }
            }),
            DryRunRepair::Classify(provider, credential, quota) => {
                let Some(mut block) = self.blocks(now)?.blocks.into_iter().find(|block| {
                    block.kind == "credential"
                        && block.scope.provider == provider
                        && block.scope.credential_id.as_deref() == Some(&credential)
                }) else {
                    return Ok(false);
                };
                if !block.classified {
                    block.classified = true;
                    block.lifetime = LifetimeClass::from_quota(quota);
                    block.next_recheck_at =
                        block
                            .next_recheck_at
                            .min(block.evidence_at.saturating_add(if quota {
                                900_000
                            } else {
                                3_600_000
                            }));
                }
                let class = block.lifetime == LifetimeClass::Quota;
                let mut row = self
                    .block(&block.kind, &block.origin_key)
                    .expect("an effective credential has a source record")
                    .clone();
                row.cleared_models = std::mem::take(&mut block.cleared_models);
                row.block = block;
                row.managed = true;
                row.cleared_at = None;
                row.cleared_by = None;
                self.put_private(row);
                return Ok(class);
            }
            DryRunRepair::InsertFence(fence) => {
                let evidence = &fence.evidence;
                let json: serde_json::Value =
                    serde_json::from_str(&evidence.fence_json).map_err(crate::err)?;
                let mut row = StoredBlock::from_origin(ViewOrigin {
                    kind: "account".into(),
                    origin_key: serde_json::json!([evidence.fence_id]).to_string(),
                    provider: evidence.provider.clone(),
                    account_id: Some(evidence.account_id.clone()),
                    credential_id: None,
                    model: None,
                    size_class: None,
                    principal: None,
                    admission_id: None,
                    evidence: evidence.fence_json.clone(),
                    evidence_at: evidence.observed_at.min(evidence.created_at),
                    quota: Some(true),
                    deadline: None,
                    reset_at: json
                        .pointer("/reset/at_ms")
                        .and_then(|value| value.as_f64()),
                })
                .map_err(crate::err)?;
                row.observation_id = evidence.observation_id.clone();
                row.pool_key_json = fence
                    .pool_key
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(crate::err)?;
                row.observed_at = Some(evidence.observed_at);
                row.created_at = Some(evidence.created_at);
                row.cleared_reason = evidence.cleared_reason.clone();
                row.recovery_set_json =
                    Some(serde_json::to_string(&fence.recovery_set).map_err(crate::err)?);
                row.meta = Meta::Account(AccountMeta {
                    scope_json: evidence.scope_json.clone(),
                    fence_json: evidence.fence_json.clone(),
                    source: evidence.source.clone(),
                    principal: evidence.principal.clone(),
                    admission_id: evidence.admission_id.clone(),
                });
                self.put_private(row);
            }
        }
        Ok(false)
    }

    pub fn dry_run_clear_size(&mut self, fact: &crate::AccountSizeRefusal) {
        self.edit_blocks(|row| {
            if row.block.kind == "size"
                && row.block.scope.provider == fact.provider
                && row.block.scope.credential_id.as_deref() == Some(&fact.credential_id)
                && row.block.scope.model.as_deref() == Some(&fact.model)
                && row.block.scope.size_class == Some(i64::from(fact.size_class))
                && row.observed_at.is_some_and(|at| at <= fact.observed_at)
            {
                row.source_present = false;
            }
        });
    }

    pub fn dry_run_clear_routes(&mut self, provider: &str, credential: &str, before: i64) {
        self.edit_blocks(|row| {
            if row.block.kind == "account_route"
                && row.block.scope.provider == provider
                && row.block.scope.credential_id.as_deref() == Some(credential)
                && row
                    .last_reported_at
                    .or(row.recorded_at)
                    .is_some_and(|at| at < before)
            {
                row.source_present = false;
            }
        });
    }

    /// Clear only the named model's route evidence whose latest report time
    /// (or recorded time when unreported) is strictly before the cutoff.
    pub fn dry_run_clear_route(
        &mut self,
        provider: &str,
        credential: &str,
        model: &str,
        before: i64,
    ) {
        self.edit_blocks(|row| {
            if row.block.kind == "account_route"
                && row.block.scope.provider == provider
                && row.block.scope.credential_id.as_deref() == Some(credential)
                && row.block.scope.model.as_deref() == Some(model)
                && row
                    .last_reported_at
                    .or(row.recorded_at)
                    .is_some_and(|at| at < before)
            {
                row.source_present = false;
            }
        });
    }

    pub fn dry_run_reconcile_route(&mut self, fence: &crate::AccountRouteFence, until: i64) {
        self.edit_blocks(|row| {
            if row.block.kind == "account_route"
                && row.block.scope.provider == fence.provider
                && row.block.scope.credential_id.as_deref() == Some(&fence.credential_id)
                && row.block.scope.model.as_deref() == Some(&fence.model)
            {
                let prior = row.block.clone();
                row.deadline = Some(Deadline::Integer(until));
                row.reset_at = Some((until as f64).into());
                row.block.evidence = serde_json::json!([
                    row.wall_class,
                    until,
                    row.last_reported_at.or(row.recorded_at)
                ])
                .to_string();
                if row.block.evidence != prior.evidence {
                    let origin = row.origin();
                    let clocks = BTreeMap::from([(
                        (prior.kind.clone(), prior.origin_key.clone()),
                        (prior, row.cleared_at),
                    )]);
                    if let Ok(mut loaded) =
                        fence_rechecks::derive_blocks(clocks, &[], [origin], i64::MIN)
                    {
                        if let Some(block) = loaded.blocks.pop() {
                            row.block = block;
                            row.cleared_at = None;
                            row.cleared_by = None;
                            row.cleared_models.clear();
                        }
                    }
                }
            }
        });
    }

    pub fn dry_run_listing(
        &mut self,
        listings: Option<&[(String, String)]>,
        changes: &[(String, String, Option<String>)],
    ) {
        self.edit_blocks(|row| {
            if row.block.kind != "credential" {
                return;
            }
            if let Some((_, _, fingerprint)) = changes.iter().find(|(provider, id, _)| {
                *provider == row.block.scope.provider
                    && row.block.scope.credential_id.as_deref() == Some(id)
            }) {
                if fingerprint.is_none() {
                    row.source_present = false;
                } else {
                    row.listing_fingerprint = fingerprint.clone();
                }
            }
        });
        if let Some(listings) = listings {
            let gone: Vec<_> = self
                .rows(7)
                .filter_map(|(id, record)| match record {
                    Record::Eligibility(row)
                        if !row.listing_fingerprint.as_ref().is_some_and(|fp| {
                            listings.contains(&(row.credential_id.clone(), fp.clone()))
                        }) =>
                    {
                        Some(id)
                    }
                    _ => None,
                })
                .collect();
            for id in gone {
                self.tables[7].remove(id);
            }
        }
    }
}
