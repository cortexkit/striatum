//! Disposable changes to an account snapshot. These operations never touch a
//! connection; they retain repairs made by earlier points in a sweep.

use super::*;
use striatum_core::blocks::LifetimeClass;

/// Request-path repairs retained only in a sweep's private state.
pub enum DryRunRepair {
    ClearFence(String, String),
    ClearHeld(crate::AccountFenceRow, String),
    ClearCredential(String, String),
    Classify(String, String, bool),
    InsertFence(crate::AccountPoolFenceRow),
}

impl BlockState {
    fn edit_rows(&mut self, table: usize, mut edit: impl FnMut(&mut Held) -> bool) {
        let rows: Vec<_> = self.tables[table]
            .rows
            .iter()
            .map(|(id, (key, held))| (*id, key.clone(), (**held).clone()))
            .collect();
        for (id, key, mut held) in rows {
            let keep = edit(&mut held);
            self.tables[table].put(id, &key, keep.then(|| Arc::new(held)).as_ref());
        }
    }

    fn insert_private(&mut self, table: usize, key: String, held: Held) {
        let id = self.tables[table]
            .rows
            .iter()
            .map(|(id, _)| *id)
            .max()
            .unwrap_or(0)
            + 1;
        self.tables[table].put(id, &key, Some(&Arc::new(held)));
    }

    /// Returns the stored class for classification, matching the live writer's
    /// first-classification rule. Other repairs return false.
    pub fn dry_run_repair(
        &mut self,
        repair: DryRunRepair,
        now: i64,
    ) -> striatum_core::RouteStateResult<bool> {
        match repair {
            DryRunRepair::ClearFence(id, _) => {
                self.edit_rows(ACCOUNT_FENCE_TABLE, |held| {
                    !matches!(held,
                    Held::Source { raw: Raw::AccountFence { row, .. }, .. } if row.fence_id == id)
                });
            }
            DryRunRepair::ClearHeld(row, reason) => {
                return self.dry_run_repair(DryRunRepair::ClearFence(row.fence_id, reason), now);
            }
            DryRunRepair::ClearCredential(provider, credential) => {
                self.edit_rows(CREDENTIAL_TABLE, |held| {
                    !matches!(held,
                    Held::Source { raw: Raw::Entitlement(row), .. }
                    if row.provider_id == provider && row.credential_id == credential)
                });
            }
            DryRunRepair::Classify(provider, credential, quota) => {
                let block = self.blocks(now)?.blocks.into_iter().find(|block| {
                    block.kind == "credential"
                        && block.scope.provider == provider
                        && block.scope.credential_id.as_deref() == Some(&credential)
                });
                if let Some(mut block) = block {
                    if !block.classified {
                        block.classified = true;
                        block.lifetime = if quota {
                            LifetimeClass::Quota
                        } else {
                            LifetimeClass::NonQuota
                        };
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
                    let key = serde_json::json!([block.kind, block.origin_key]).to_string();
                    self.insert_private(CLOCK_TABLE, key, Held::Clock(block, None));
                    return Ok(class);
                }
                return Ok(false);
            }
            DryRunRepair::InsertFence(fence) => {
                let row = &fence.evidence;
                let evidence: serde_json::Value =
                    serde_json::from_str(&row.fence_json).map_err(crate::err)?;
                let origin = ViewOrigin {
                    kind: "account".into(),
                    origin_key: serde_json::json!([row.fence_id]).to_string(),
                    provider: row.provider.clone(),
                    account_id: Some(row.account_id.clone()),
                    credential_id: None,
                    model: None,
                    size_class: None,
                    principal: None,
                    admission_id: None,
                    evidence: row.fence_json.clone(),
                    evidence_at: row.observed_at.min(row.created_at),
                    quota: Some(true),
                    deadline: None,
                    reset_at: evidence.pointer("/reset/at_ms").and_then(|v| v.as_f64()),
                };
                self.insert_private(
                    ACCOUNT_FENCE_TABLE,
                    row.fence_id.clone(),
                    Held::Source {
                        origins: vec![origin],
                        link: Link::AccountFence(row.fence_id.clone()),
                        raw: Raw::AccountFence {
                            row: row.clone(),
                            pool_key_json: fence
                                .pool_key
                                .as_ref()
                                .map(|pool| serde_json::to_string(pool).unwrap()),
                            recovery_set_json: serde_json::to_string(&fence.recovery_set).unwrap(),
                        },
                    },
                );
            }
        }
        Ok(false)
    }

    pub fn dry_run_clear_size(&mut self, fact: &crate::AccountSizeRefusal) {
        self.edit_rows(SIZE_TABLE, |held| {
            !matches!(held,
            Held::Source { raw: Raw::Size(row), .. } if row.provider == fact.provider
            && row.credential_id == fact.credential_id && row.model == fact.model
            && row.size_class == fact.size_class && row.observed_at <= fact.observed_at)
        });
    }

    pub fn dry_run_clear_routes(&mut self, provider: &str, credential: &str, before: i64) {
        self.edit_rows(ROUTE_FENCE_TABLE, |held| {
            !matches!(held,
            Held::Source { raw: Raw::RouteFence { row, last_reported_at, .. }, .. }
            if row.provider == provider && row.credential_id == credential
            && last_reported_at.unwrap_or(row.recorded_at) < before)
        });
    }

    pub fn dry_run_reconcile_route(&mut self, fence: &crate::AccountRouteFence, until: i64) {
        self.edit_rows(ROUTE_FENCE_TABLE, |held| {
            if let Held::Source {
                origins,
                raw:
                    Raw::RouteFence {
                        row,
                        wall_class,
                        last_reported_at,
                    },
                ..
            } = held
            {
                if row.provider == fence.provider
                    && row.credential_id == fence.credential_id
                    && row.model == fence.model
                {
                    row.until_ms = until;
                    for origin in origins {
                        origin.evidence = serde_json::json!([
                            wall_class,
                            until,
                            last_reported_at.unwrap_or(row.recorded_at)
                        ])
                        .to_string();
                        origin.deadline = Some(until as f64);
                        origin.reset_at = Some(until as f64);
                    }
                }
            }
            true
        });
    }

    pub fn dry_run_listing(
        &mut self,
        listings: Option<&[(String, String)]>,
        changes: &[(String, String, Option<String>)],
    ) {
        self.edit_rows(CREDENTIAL_TABLE, |held| {
            if let Held::Source {
                raw: Raw::Entitlement(row),
                ..
            } = held
            {
                if let Some((_, _, fingerprint)) = changes.iter().find(|(provider, id, _)| {
                    *provider == row.provider_id && *id == row.credential_id
                }) {
                    if fingerprint.is_none() {
                        return false;
                    }
                    row.listing_fingerprint = fingerprint.clone();
                }
            }
            true
        });
        if let Some(listings) = listings {
            self.edit_rows(15, |held| {
                if let Held::Eligibility(row) = held {
                    return row.listing_fingerprint.as_ref().is_some_and(|fp| {
                        listings.contains(&(row.credential_id.clone(), fp.clone()))
                    });
                }
                true
            });
        }
    }

    pub fn dry_run_release_owners(&mut self, owners: &[String], now: i64) {
        let authorizations: Vec<_> = self
            .authorizations()
            .filter(|row| owners.contains(&row.owner))
            .cloned()
            .collect();
        self.edit_rows(
            AUTHORIZATION_TABLE,
            |held| !matches!(held, Held::Authorization(row) if owners.contains(&row.owner)),
        );
        self.edit_rows(CLOCK_TABLE, |held| {
            if let Held::Clock(block, cleared) = held {
                if cleared.is_none()
                    && authorizations.iter().any(|row| {
                        row.kind == block.kind
                            && row.origin_key == block.origin_key
                            && row.evidence == block.evidence
                            && row.evidence_at == block.evidence_at
                            && row.claim_generation == block.claim_generation
                    })
                {
                    block.next_recheck_at = now;
                    block.last_claimed_at = None;
                    block.claim_owner = None;
                    block.claim_generation += 1;
                }
            }
            true
        });
    }
}

impl crate::SqliteRouteStateStore {
    /// Dry runs retain the captured block overlay, but route measurements are
    /// read anew at each evaluation, independently of account-state publication.
    pub fn dry_run_route_snapshot(
        &self,
        blocks: &BlockState,
        now: f64,
        recheck_now: i64,
    ) -> striatum_core::RouteStateResult<striatum_core::model_router::ModelRouteStateSnapshot> {
        let mut rows: Vec<_> = blocks
            .raws(ROUTE_TABLE)
            .filter_map(|(_, raw)| match raw {
                Raw::Route(row) => Some(row.clone()),
                _ => None,
            })
            .collect();
        crate::group_reads::declared_exception("route_data", || self.store.with_read(|conn| {
            let mut query = conn.prepare_cached("SELECT route_key,quota_pressure,latency_ema_ms,success_ema,last_error FROM model_route_state")?;
            for row in query.query_map([], |r| Ok((r.get::<_, String>(0)?,r.get::<_, Option<f64>>(1)?,r.get::<_, Option<f64>>(2)?,r.get::<_, Option<f64>>(3)?,r.get::<_, Option<String>>(4)?)))? {
                let (key, pressure, latency, success, error) = row?;
                if let Some(row) = rows.iter_mut().find(|row| row.route_key == key) {
                    row.quota_pressure = pressure; row.latency_ema_ms = latency; row.success_ema = success; row.last_error = error;
                }
            }
            Ok(())
        })).map_err(crate::err)?;
        crate::route_snapshot::overlay(
            &rows,
            &blocks.fence_rechecks(recheck_now)?,
            blocks.enabled(),
            now,
        )
    }
}
