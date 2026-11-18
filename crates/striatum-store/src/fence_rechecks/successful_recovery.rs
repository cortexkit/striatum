//! Successful probe and serving outcomes, including the clocks they retire.
//! This keeps settlement, including checking that a probe still owns its
//! evidence and claim generation, separate from granting a probe.

use super::*;

impl AccountTx<'_> {
    /// The durable launch authorization, not a reported credential or guessed
    /// pool, is the scope of a successful probe. New evidence never matches it.
    pub fn heal_authorized_probe(
        &self,
        owner: &str,
        _provider: &str,
        model: &str,
        now: i64,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::fence_rechecks::heal_authorized_probe");
        if !enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        let origins = self.fence_rechecks()?;
        // Which test this owner holds (if any) decides both the heal and the
        // authorization delete below, and a missed heal is never retried:
        // check the owner's authorizations and their clocks against SQL
        // first. On a mismatch this returns STATE_DIVERGED before healing or
        // deleting anything, and the account writer reloads its state and
        // runs the batch again.
        if let Some(blocks) = self.working_blocks()? {
            self.check_probe_inputs(&blocks, owner, true)?;
        }
        // A lease can probe a pooled key's billing refusal under the pool's
        // alias and a cooldown under the target provider. Keep those scopes distinct.
        for origin in origins
            .iter()
            .filter(|origin| origin.model.as_deref().is_none_or(|value| value == model))
        {
            // Only the test request this owner holds now: its authorization
            // must name the clock's current evidence and claim generation, so
            // a test a later one superseded heals nothing.
            let claimed: Option<i64> = match self.working_blocks()? {
                Some(blocks) => blocks
                    .authorizations_of(owner)
                    .into_iter()
                    .find(|row| {
                        row.kind == origin.kind
                            && row.origin_key == origin.origin_key
                            && row.evidence == origin.evidence
                            && row.evidence_at == origin.evidence_at
                            && test_is_current(&blocks, row, false)
                    })
                    .map(|row| row.claimed_at),
                None => self
                    .conn
                    .query_row(
                        &format!("SELECT a.claimed_at FROM probe_authorization a {}
                    WHERE a.owner=?1 AND a.kind=?2 AND a.origin_key=?3 AND a.evidence=?4 AND a.evidence_at=?5", current_test_join(6)),
                        params![
                            owner,
                            origin.kind,
                            origin.origin_key,
                            origin.evidence,
                            origin.evidence_at,
                            false
                        ],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(err)?,
            };
            if let Some(at) = claimed.filter(|at| origin.evidence_at < *at) {
                self.heal_origins(std::slice::from_ref(origin), model, at, now, None, true)?;
            }
        }
        match self.working_blocks()? {
            Some(blocks) => {
                self.delete_authorizations(owner, Some(&blocks.authorizations_of(owner)))
            }
            None => self.delete_authorizations(owner, None),
        }
    }

    /// Settles one run's test request after a successful report. The owner's
    /// authorization must name the block's exact evidence (the refusal that
    /// started it) and its current claim generation (which numbered test this
    /// was), with no other request owning the claim; an admission that owns
    /// the claim itself is current as well. An admission owner may also
    /// retire the same test's duplicate authorization held under the
    /// decision id (`decision_copy`), but never a later run's re-claim.
    pub(crate) fn heal_report_probe(
        &self,
        owner: &str,
        decision_copy: Option<&str>,
        admission_owned: bool,
        model: &str,
        admitted_at: i64,
        origins: &[FenceRecheck],
        scope: Option<&QuotaSuccessScope>,
        now: i64,
    ) -> RouteStateResult<()> {
        // The report is recorded as delivered whatever this finds, so a heal
        // skipped on a stale state would be lost: check the owner's (and the
        // decision copy's) authorizations and their clocks against SQL first.
        if let Some(blocks) = self.working_blocks()? {
            self.check_probe_inputs(&blocks, owner, true)?;
            if let Some(copy) = decision_copy.filter(|copy| *copy != owner) {
                self.check_probe_inputs(&blocks, copy, true)?;
            }
        }
        for origin in origins.iter().filter(|origin| {
            origin.evidence_at < admitted_at
                && origin.model.as_deref().is_none_or(|value| value == model)
        }) {
            let proof: Option<(i64, i64)> = match self.working_blocks()? {
                Some(blocks) => blocks
                    .authorizations_of(owner)
                    .into_iter()
                    .find(|row| {
                        row.kind == origin.kind
                            && row.origin_key == origin.origin_key
                            && row.evidence == origin.evidence
                            && row.evidence_at == origin.evidence_at
                            && row.claimed_at <= admitted_at
                    })
                    .filter(|row| test_is_current(&blocks, row, admission_owned))
                    .map(|row| (row.claimed_at, row.claim_generation)),
                None => self
                    .conn
                    .query_row(
                        &format!(
                            "SELECT a.claimed_at,a.claim_generation FROM probe_authorization a {}
                     WHERE a.owner=?1 AND a.kind=?2 AND a.origin_key=?3
                     AND a.evidence=?4 AND a.evidence_at=?5 AND a.claimed_at<=?6",
                            current_test_join(7)
                        ),
                        params![
                            owner,
                            origin.kind,
                            origin.origin_key,
                            origin.evidence,
                            origin.evidence_at,
                            admitted_at,
                            admission_owned
                        ],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
                    .map_err(err)?,
            };
            let Some((claimed_at, generation)) = proof.filter(|(at, _)| origin.evidence_at < *at)
            else {
                continue;
            };
            self.heal_origins(
                std::slice::from_ref(origin),
                model,
                admitted_at,
                now,
                scope,
                true,
            )?;
            // The owner's row, and the decision's copy of the same test when
            // the state holds one.
            let expected = match self.working_blocks()? {
                Some(blocks) => Some(
                    1 + usize::from(decision_copy.is_some_and(|copy| {
                        copy != owner
                            && blocks.authorizations_of(copy).iter().any(|row| {
                                row.kind == origin.kind
                                    && row.origin_key == origin.origin_key
                                    && row.evidence == origin.evidence
                                    && row.evidence_at == origin.evidence_at
                                    && row.claimed_at == claimed_at
                                    && row.claim_generation == generation
                            })
                    })),
                ),
                None => None,
            };
            let changed = self
                .conn
                .execute(
                    "DELETE FROM probe_authorization WHERE (owner=?1 OR owner=?2)
                 AND kind=?3 AND origin_key=?4 AND evidence=?5 AND evidence_at=?6
                 AND claimed_at=?7 AND claim_generation=?8",
                    params![
                        owner,
                        decision_copy,
                        origin.kind,
                        origin.origin_key,
                        origin.evidence,
                        origin.evidence_at,
                        claimed_at,
                        generation
                    ],
                )
                .map_err(err)?;
            if let Some(expected) = expected {
                crate::block_state::expect_changes(
                    changed,
                    expected,
                    format_args!(
                        "settled test of {} {} for {owner}",
                        origin.kind, origin.origin_key
                    ),
                )
                .map_err(err)?;
            }
            if origin.kind == "observation" {
                // Retirement spends every authorization for this evidence,
                // including superseded tests that can no longer heal it.
                self.conn
                    .execute(
                        "DELETE FROM probe_authorization WHERE kind=?1 AND origin_key=?2
                     AND evidence=?3 AND evidence_at=?4",
                        params![
                            origin.kind,
                            origin.origin_key,
                            origin.evidence,
                            origin.evidence_at
                        ],
                    )
                    .map_err(err)?;
            }
        }
        Ok(())
    }
    pub(crate) fn successful_quota_pools(
        &self,
        id: &str,
        observed: i64,
    ) -> RouteStateResult<Vec<striatum_core::accounts::PoolKey>> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(Vec::new());
        }
        let mut statement = self
            .conn
            .prepare("SELECT pool_key_json FROM success_proof WHERE fence_id=?1 AND evidence_at=?2")
            .map_err(err)?;
        let rows = statement
            .query_map(params![id, observed], |row| row.get::<_, String>(0))
            .map_err(err)?;
        rows.map(|raw| serde_json::from_str(&raw.map_err(err)?).map_err(err))
            .collect()
    }

    /// Success can heal only evidence older than this request's admission. A
    /// late completion of an older run must not erase a more recent refusal.
    #[allow(clippy::too_many_arguments)]
    pub fn heal_serving_fences(
        &self,
        provider: &str,
        account: &str,
        credential: &str,
        model: &str,
        size: Option<u32>,
        admitted_at: i64,
        now: i64,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::fence_rechecks::heal_serving_fences");
        self.heal_serving_fences_in_scope(
            provider,
            account,
            credential,
            model,
            size,
            admitted_at,
            now,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn heal_serving_fences_in_scope(
        &self,
        provider: &str,
        account: &str,
        credential: &str,
        model: &str,
        size: Option<u32>,
        admitted_at: i64,
        now: i64,
        scope: Option<&QuotaSuccessScope>,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::fence_rechecks::heal_serving_fences_in_scope",
        );
        let origins = self.fence_rechecks()?;
        let origins: Vec<_> = origins
            .into_iter()
            .filter(|origin| {
                origin.applies(provider, account, credential, model)
                    && origin.evidence_at < admitted_at
                    && origin.principal.is_none()
                    && origin
                        .size_class
                        .is_none_or(|class| Some(class as u32) == size)
            })
            .collect();
        self.heal_origins(&origins, model, admitted_at, now, scope, false)
    }

    /// Heal `origins` after a success: end each one's source evidence where
    /// it is older than `admitted_at` and clear its clock. With a working
    /// state every decision reads the state and every write is keyed and
    /// names the row it was decided on; without one, SQL decides.
    pub(crate) fn heal_origins(
        &self,
        origins: &[FenceRecheck],
        model: &str,
        admitted_at: i64,
        now: i64,
        scope: Option<&QuotaSuccessScope>,
        authorized: bool,
    ) -> RouteStateResult<()> {
        for origin in origins {
            self.heal_origin(origin, model, admitted_at, now, scope, authorized)?;
        }
        Ok(())
    }

    /// Use the command's working `BlockState` if available; otherwise load it
    /// from SQL. `heal_origin` checks the rows it updates in either case.
    fn working(&self) -> RouteStateResult<std::borrow::Cow<'_, crate::BlockState>> {
        match self.working_blocks()? {
            Some(state) => Ok(state),
            None => crate::BlockState::read(self.conn)
                .map(std::borrow::Cow::Owned)
                .map_err(err),
        }
    }

    /// [`Self::heal_origins`] for one origin, deciding on the working state.
    /// Each keyed write names the current values of its row, so it fails
    /// with [`crate::block_state::diverged`] if SQL holds something else; a
    /// field the state shows already cleared is not written again.
    fn heal_origin(
        &self,
        origin: &FenceRecheck,
        model: &str,
        admitted_at: i64,
        now: i64,
        scope: Option<&QuotaSuccessScope>,
        authorized: bool,
    ) -> RouteStateResult<()> {
        use crate::block_state::expect_changes;
        let provider = origin.provider.as_str();
        let credential = origin.credential_id.as_deref().unwrap_or("");
        let key: Vec<serde_json::Value> = serde_json::from_str(&origin.origin_key).map_err(err)?;
        let what = format!("heal of {} {}", origin.kind, origin.origin_key);
        let blocks = self.working()?;
        match origin.kind.as_str() {
            "observation" => {
                if !authorized || origin.evidence_at >= admitted_at {
                    return Ok(());
                }
                blocks
                    .check_source(self.conn, &origin.kind, &origin.origin_key, provider)
                    .map_err(err)?;
                self.retire_observation_origin(origin, now)?;
            }
            "account" => {
                // Only active fences are held: a fence already cleared is
                // not cleared again.
                let fence = key[0].as_str().and_then(|id| blocks.account_fence(id));
                let older = |row: &crate::AccountFenceRow| {
                    row.observed_at.min(row.created_at) < admitted_at
                };
                let Some((row, pool, recovery)) = fence else {
                    return if authorized {
                        self.clear_held_clock(origin, now)
                    } else {
                        Ok(())
                    };
                };
                let needed: Vec<striatum_core::accounts::PoolKey> = match pool {
                    Some(pool) => vec![serde_json::from_str(pool).map_err(err)?],
                    None => serde_json::from_str(recovery).map_err(err)?,
                };
                // Authorization permits the test, not recovery of pools the
                // request did not use. An empty whole-account recovery set
                // and pool-keyed fences retain their authorized clear.
                if authorized && (pool.is_some() || needed.is_empty()) {
                    if older(&row) {
                        self.clear_held_fence(&row, now, "probe_succeeded")?;
                    }
                    return self.clear_held_clock(origin, now);
                }
                if scope.is_none() && !authorized {
                    return Ok(());
                }
                if needed.is_empty() {
                    return Ok(());
                }
                let row = row.clone();
                let (pool_json, recovery_json) = (pool.map(str::to_owned), recovery.to_owned());
                drop(blocks);
                for pool in needed
                    .iter()
                    .filter(|pool| scope.is_some_and(|scope| scope.used(pool, model)))
                {
                    self.put_success_pool(
                        &row.fence_id,
                        &serde_json::to_string(pool).map_err(err)?,
                        origin.evidence_at,
                        now,
                    )?;
                }
                let healed = self
                    .working()?
                    .success_pools()
                    .filter(|pool| {
                        pool.fence_id == row.fence_id && pool.evidence_at == origin.evidence_at
                    })
                    .map(|pool| serde_json::from_str(&pool.pool_key_json).map_err(err))
                    .collect::<RouteStateResult<Vec<striatum_core::accounts::PoolKey>>>()?;
                if !needed.iter().all(|pool| healed.contains(pool)) {
                    return if authorized {
                        self.defer_recovery_test(origin, now)
                    } else {
                        Ok(())
                    };
                }
                if older(&row) {
                    // Decided on the fence's pools and on the successes
                    // recorded for them: the clear names both.
                    self.clear_held_pool_fence(
                        &row,
                        pool_json.as_deref(),
                        &recovery_json,
                        (origin.evidence_at, healed.len()),
                        now,
                        if authorized {
                            "probe_succeeded"
                        } else {
                            "run_succeeded"
                        },
                    )?;
                }
            }
            "credential" => {
                if origin.quota {
                    let prefix = format!(
                        "credential-quota:{}:",
                        serde_json::json!([provider, credential])
                    );
                    if blocks
                        .active_fences(provider)
                        .iter()
                        .any(|fence| fence.fence_id.starts_with(&prefix))
                    {
                        return Ok(());
                    }
                }
                if let Some(fence) = blocks
                    .entitlement_fence(provider, credential)
                    .filter(|fence| fence.observed_at_ms < admitted_at)
                {
                    self.clear_held_entitlement_fence_at(&fence, now)?;
                }
            }
            "account_route" => {
                if let Some((row, class, last)) = blocks
                    .account_route_fence(provider, credential, model)
                    .filter(|(row, _, last)| last.unwrap_or(row.recorded_at) < admitted_at)
                {
                    let changed = self.conn.execute("UPDATE block SET source_present=0,cleared_at=?8,cleared_by='removed',claim_owner=NULL
                        WHERE kind='account_route' AND source_present=1 AND provider=?1 AND credential_id=?2 AND model=?3
                        AND deadline=?4 AND recorded_at=?5 AND last_reported_at IS ?6 AND wall_class IS ?7",
                        params![provider,credential,model,row.until_ms,row.recorded_at,last,class,now]).map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            "non_quota_route" | "transient_route" => {
                let kind = if origin.kind == "transient_route" {
                    "transient"
                } else {
                    "billing"
                };
                if let Some((row, last)) = blocks
                    .non_quota_route_fence(provider, credential, model, kind)
                    .filter(|(_, last)| *last < admitted_at)
                {
                    let changed = self.conn.execute("UPDATE block SET source_present=0,cleared_at=?8,cleared_by='removed',claim_owner=NULL
                        WHERE source_present=1 AND provider=?1 AND credential_id=?2 AND model=?3
                        AND kind=CASE ?4 WHEN 'transient' THEN 'transient_route' ELSE 'non_quota_route' END
                        AND deadline=?5 AND recorded_at=?6 AND last_reported_at=?7",
                        params![provider,credential,model,kind,row.until_ms,row.recorded_at,last,now]).map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            "size" | "non_quota_size" => {
                let quota = origin.kind == "size";
                let kind = if quota { "size" } else { "non_quota_size" };
                if let Some(row) = origin.size_class.and_then(|class| {
                    blocks
                        .size_refusal(quota, provider, credential, model, class)
                        .filter(|row| row.observed_at < admitted_at)
                }) {
                    let changed = self.conn.execute(&format!("UPDATE block SET source_present=0,cleared_at=?7,cleared_by='removed',claim_owner=NULL
                        WHERE kind='{kind}' AND source_present=1 AND provider=?1 AND credential_id=?2 AND model=?3
                        AND size_class=?4 AND deadline IS ?5 AND observed_at=?6"),
                        params![provider,credential,model,row.size_class,row.expires_at,row.observed_at,now]).map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            "pool_billing" | "pool_rate" => {
                if let Some(held) = blocks.source_origin(&origin.kind, &origin.origin_key) {
                    let changed = self
                        .conn
                        .execute(
                            "UPDATE block SET source_present=0,cleared_at=?4,cleared_by='removed',claim_owner=NULL
                                WHERE provider=?1 AND credential_id=?2 AND evidence=?3 AND kind=?5 AND source_present=1",
                            params![provider, credential, held.evidence,now,origin.kind],
                        )
                        .map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            "route" => {
                if let Some(held) = blocks.source_origin("route", &origin.origin_key) {
                    let changed = self.conn.execute("UPDATE block SET source_present=0,cleared_at=?3,cleared_by='removed',claim_owner=NULL
                        WHERE kind='route' AND origin_key=json_array(?1) AND source_present=1 AND evidence=?2",
                        params![key[0].as_str(),held.evidence,now]).map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            "provider" => {
                provider_healing::heal_provider(self, &blocks, origin, admitted_at, now, &what)?;
            }
            "failure" => {
                if let Some(held) = blocks.source_origin("failure", &origin.origin_key) {
                    let changed = self.conn.execute("UPDATE block SET source_present=0,cleared_at=?3,cleared_by='removed',claim_owner=NULL
                        WHERE kind='failure' AND origin_key=json_array(?1) AND source_present=1 AND evidence=?2",
                        params![key[0].as_str(),held.evidence,now]).map_err(err)?;
                    expect_changes(changed, 1, &what).map_err(err)?;
                }
            }
            _ => {}
        }
        self.clear_held_clock(origin, now)
    }

    fn retire_observation_origin(&self, origin: &FenceRecheck, now: i64) -> RouteStateResult<()> {
        // Retain the receipt time so replayed effects cannot restore this
        // backoff or an older one. The native retirement trigger clears the
        // block, while the observations and frozen audit copy stay untouched.
        self.conn
            .execute(
                "INSERT INTO retirement_tombstone(kind,origin_key,evidence,evidence_at,retired_at)
             VALUES (?1,?2,?3,?4,?5) ON CONFLICT(kind,origin_key,evidence) DO NOTHING",
                params![
                    origin.kind,
                    origin.origin_key,
                    origin.evidence,
                    origin.evidence_at,
                    now
                ],
            )
            .map_err(err)?;
        Ok(())
    }

    /// Record that `pool` succeeded for one account fence's evidence, keyed
    /// and naming the success the working state holds for it, if any.
    fn put_success_pool(
        &self,
        fence_id: &str,
        pool: &str,
        evidence_at: i64,
        now: i64,
    ) -> RouteStateResult<()> {
        let held = self
            .working()?
            .success_pools()
            .find(|row| row.fence_id == fence_id && row.pool_key_json == pool)
            .cloned();
        let changed = match held {
            Some(held) => self.conn.execute(
                "UPDATE success_proof SET evidence_at=?3,healed_at=?4
                WHERE fence_id=?1 AND pool_key_json=?2 AND evidence_at=?5 AND healed_at=?6",
                params![
                    fence_id,
                    pool,
                    evidence_at,
                    now,
                    held.evidence_at,
                    held.healed_at
                ],
            ),
            None => self.conn.execute(
                "INSERT INTO success_proof VALUES (?1,?2,?3,?4)
                ON CONFLICT(fence_id,pool_key_json) DO NOTHING",
                params![fence_id, pool, evidence_at, now],
            ),
        }
        .map_err(err)?;
        crate::block_state::expect_changes(
            changed,
            1,
            format_args!("success of {fence_id} for {pool}"),
        )
        .map_err(err)
    }

    /// Clear an origin's stored clock, naming the clock the working state
    /// holds. An origin with no stored clock has none to clear.
    fn clear_held_clock(&self, origin: &FenceRecheck, now: i64) -> RouteStateResult<()> {
        let blocks = self.working()?;
        let Some((clock, cleared)) = blocks.clock(&origin.kind, &origin.origin_key) else {
            return Ok(());
        };
        let changed = self
            .conn
            .execute(
                 "UPDATE block SET cleared_at=?3,cleared_by=CASE WHEN source_present=1 THEN 'kept' ELSE 'removed' END WHERE kind=?1 AND origin_key=?2 AND is_origin=1 AND managed=1
            AND evidence=?4 AND evidence_at=?5 AND cleared_at IS ?6 AND next_recheck_at=?7
            AND claim_owner IS ?8 AND claim_generation=?9",
                params![
                    origin.kind,
                    origin.origin_key,
                    now,
                    clock.evidence,
                    clock.evidence_at,
                    cleared,
                    clock.next_recheck_at,
                    clock.claim_owner,
                    clock.claim_generation
                ],
            )
            .map_err(err)?;
        crate::block_state::expect_changes(
            changed,
            1,
            format_args!("clock heal of {} {}", origin.kind, origin.origin_key),
        )
        .map_err(err)
    }

    /// A successful test with pools still unproved consumes its quota test
    /// slot without clearing the block. Another request may test it after
    /// the same interval used for an unsuccessful quota test.
    fn defer_recovery_test(&self, origin: &FenceRecheck, now: i64) -> RouteStateResult<()> {
        if let Some(blocks) = self.working_blocks()? {
            let Some((clock, _)) = blocks.clock(&origin.kind, &origin.origin_key) else {
                return Ok(());
            };
            let changed = self
                .conn
                .execute(
                    &format!(
                        "UPDATE block SET next_recheck_at=?3+?4,
                claim_owner=NULL,reset_check_at=CASE WHEN reset_check_at>?3 THEN reset_check_at END
                WHERE kind=?1 AND origin_key=?2 AND {}",
                        clock_held(5)
                    ),
                    params![
                        origin.kind,
                        origin.origin_key,
                        now,
                        QUOTA_FENCE_RECHECK_MS,
                        clock.evidence,
                        clock.evidence_at,
                        clock.lifetime == LifetimeClass::Quota,
                        clock.next_recheck_at,
                        clock.reset_check_at,
                        clock.claim_owner,
                        clock.claim_generation
                    ],
                )
                .map_err(err)?;
            return crate::block_state::expect_changes(
                changed,
                1,
                format_args!("partial recovery of {} {}", origin.kind, origin.origin_key),
            )
            .map_err(err);
        }
        self.conn
            .execute(
                "UPDATE block SET next_recheck_at=?3+?4,
            claim_owner=NULL,reset_check_at=CASE WHEN reset_check_at>?3 THEN reset_check_at END
            WHERE kind=?1 AND origin_key=?2",
                params![origin.kind, origin.origin_key, now, QUOTA_FENCE_RECHECK_MS],
            )
            .map_err(err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successful_probe_heals_only_its_authorized_empty_and_named_generations() {
        let scratch = cortexkit_test_support::ScratchDir::new("authorized-quota-success");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "authorized-quota-success",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let named = striatum_core::accounts::PoolKey {
            name: Some("P".into()),
            scope: striatum_core::accounts::PoolScope::EveryModel,
            window_minutes: Some(300),
        };
        native_fixture::seed_pool_fences(
            &store,
            &[
                ("empty", None, vec![]),
                ("named", Some(&named), vec![]),
                ("unclaimed", Some(&named), vec![]),
            ],
        );
        let claimed: Vec<_> = store
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .filter(|origin| !origin.origin_key.contains("unclaimed"))
            .collect();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &claimed,
                900_100,
                Some("probe-decision")
            ))
            .unwrap());
        // Unknown-pool successes unrelated to this authorization remain unable
        // to heal anything, including the unclaimed origin on the same account.
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences("p", "a", "c", "m", None, 900_100, 900_101)
            })
            .unwrap();
        assert_eq!(
            store
                .account_read(|tx| tx.active_fences("p"))
                .unwrap()
                .len(),
            3
        );
        store
            .account_transaction(|tx| tx.heal_authorized_probe("probe-decision", "p", "m", 900_101))
            .unwrap();
        let remaining = store.account_read(|tx| tx.active_fences("p")).unwrap();
        assert_eq!(
            remaining.len(),
            1,
            "the exact claimed empty and named origins must heal"
        );
        assert_eq!(remaining[0].fence_id, "unclaimed");
    }

    #[test]
    fn authorized_sibling_probe_heals_claimed_rows_but_never_a_newer_generation() {
        let scratch = cortexkit_test_support::ScratchDir::new("authorized-sibling-success");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "authorized-sibling-success",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        for credential in ["c2", "newer", "unclaimed"] {
            store
                .record_billing_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: credential.into(),
                    model: "m".into(),
                    until_ms: 1000,
                    recorded_at: 100,
                    decision_id: "old".into(),
                })
                .unwrap();
        }
        let claimed: Vec<_> = store
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .filter(|origin| origin.credential_id.as_deref() != Some("unclaimed"))
            .collect();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &claimed,
                1000,
                Some("sibling-probe")
            ))
            .unwrap());
        store
            .record_billing_account_route_fence(&crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "newer".into(),
                model: "m".into(),
                until_ms: 10_000,
                recorded_at: 1001,
                decision_id: "repeat".into(),
            })
            .unwrap();
        store
            .account_transaction(|tx| tx.heal_authorized_probe("sibling-probe", "p", "m", 1002))
            .unwrap();
        let remaining = store.non_quota_account_route_fences(1002).unwrap();
        assert_eq!(
            remaining.len(),
            2,
            "only the claimed older generation heals, regardless of serving sibling"
        );
        assert!(remaining.iter().any(|f| f.credential_id == "newer"));
        assert!(remaining.iter().any(|f| f.credential_id == "unclaimed"));
    }

    #[test]
    fn provider_probe_success_preserves_a_newer_independent_model_refusal() {
        let scratch = cortexkit_test_support::ScratchDir::new("provider-evidence-success");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "provider-evidence-success",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store.store.with_conn(|conn|conn.execute_batch("
            INSERT INTO provider_cooldown_state(provider_key,cooldown_until,last_error,last_error_at,updated_at,wall_observed_at)
                VALUES ('p',86400100,'old',100,100,100);
            INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at,cooldown_until,last_error,last_error_at,wall_observed_at)
                VALUES ('p/m','p/m','p','m',100,86400100,'old',100,100);
        ")).unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &origins,
                3_600_100,
                Some("provider-probe")
            ))
            .unwrap());
        store.store.with_conn(|conn|conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at,cooldown_until,last_error,last_error_at,wall_observed_at)
            VALUES ('p/other','p/other','p','other',3600101,3610101,'independent',3600101,3600101)",[])).unwrap();
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences("p", "a", "c", "m", None, 3_600_100, 3_600_102)
            })
            .unwrap();
        let until: Option<i64> = store
            .store
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT cooldown_until FROM model_route_state WHERE route_key='p/other'",
                    [],
                    |row| row.get(0),
                )
            })
            .unwrap();
        assert_eq!(
            until,
            Some(3_610_101),
            "old provider recovery cannot erase an independent model's newer generation"
        );
    }

    #[test]
    fn linked_pool_success_heals_alias_and_target_provider_origins() {
        use striatum_core::key_pool::PoolKey;
        use striatum_core::{RouteStateStore, RouteStateUpdate};
        let scratch = cortexkit_test_support::ScratchDir::new("alias-pool-probe");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "alias-pool-probe",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .pool_wall(
                "alias",
                "key",
                crate::PoolWallKind::Billing,
                1000,
                "credits",
                100,
            )
            .unwrap();
        store
            .record_provider_cooldown(
                "target",
                "target/m",
                "target/m",
                &RouteStateUpdate {
                    cooldown_until: Some(1000.0),
                    observed_at_ms: Some(100),
                    unscoped_wall: true,
                    last_error: Some("rate limit".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        let keys = vec![PoolKey::new("key", Some("a"), "active")];
        let crate::PoolAcquireOutcome::Leased(lease) = store
            .pool_acquire_for_route(
                &crate::PoolAcquire {
                    provider: "alias",
                    session_id: "s",
                    keys: &keys,
                    now_ms: 1000,
                    lease_ms: 120_000,
                    affinity_idle_ms: 120_000,
                },
                &["alias", "target"],
                "m",
                &[],
                &Default::default(),
            )
            .unwrap()
        else {
            panic!("both due origins must admit one lease")
        };
        assert_eq!(store.fence_rechecks().unwrap().len(), 2);
        store
            .bind_fence_probe_owner(&lease.lease_id, "decision")
            .unwrap();
        store
            .account_transaction(|tx| tx.heal_authorized_probe("decision", "target", "m", 1001))
            .unwrap();
        assert!(
            store.fence_rechecks().unwrap().is_empty(),
            "success must heal the whole authorized alias/target launch, not only one provider"
        );
    }

    #[test]
    fn success_heals_only_the_known_pool_and_family_and_retains_other_evidence() {
        use striatum_core::accounts::{ModelFamily, PoolKey, PoolScope};
        let scratch = cortexkit_test_support::ScratchDir::new("quota-success-scope");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "quota-success-scope",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let gemini = PoolKey {
            name: Some("gemini".into()),
            scope: PoolScope::EveryModel,
            window_minutes: Some(300),
        };
        let claude = PoolKey {
            name: Some("claude_gpt".into()),
            scope: PoolScope::Family(ModelFamily::Opus),
            window_minutes: Some(300),
        };
        native_fixture::seed_pool_fences(
            &store,
            &[
                ("gemini", Some(&gemini), vec![]),
                ("claude", Some(&claude), vec![]),
                ("whole", None, vec![gemini.clone(), claude.clone()]),
            ],
        );
        // With no request scope, even a newer success cannot guess a pool.
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences("p", "a", "c", "claude", None, 101, 200)
            })
            .unwrap();
        assert_eq!(
            store
                .account_read(|tx| tx.active_fences("p"))
                .unwrap()
                .len(),
            3
        );
        let scope = QuotaSuccessScope {
            family: ModelFamily::Opus,
            pool: Some("claude_gpt".into()),
        };
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences_in_scope(
                    "p",
                    "a",
                    "c",
                    "claude",
                    None,
                    99,
                    200,
                    Some(&scope),
                )
            })
            .unwrap();
        assert_eq!(
            store
                .account_read(|tx| tx.active_fences("p"))
                .unwrap()
                .len(),
            3,
            "an older admitted run cannot heal later evidence"
        );
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences_in_scope(
                    "p",
                    "a",
                    "c",
                    "claude",
                    None,
                    101,
                    200,
                    Some(&scope),
                )
            })
            .unwrap();
        let remaining = store.account_read(|tx| tx.active_pool_fences("p")).unwrap();
        assert_eq!(remaining.len(), 2);
        assert!(remaining
            .iter()
            .any(|fence| fence.evidence.fence_id == "gemini"));
        assert_eq!(
            remaining
                .iter()
                .find(|fence| fence.evidence.fence_id == "whole")
                .unwrap()
                .recovery_set,
            vec![gemini.clone()]
        );
        let scope = QuotaSuccessScope {
            family: ModelFamily::Other,
            pool: Some("gemini".into()),
        };
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences_in_scope(
                    "p",
                    "a",
                    "c",
                    "gemini",
                    None,
                    102,
                    201,
                    Some(&scope),
                )
            })
            .unwrap();
        assert!(
            store
                .account_read(|tx| tx.active_fences("p"))
                .unwrap()
                .is_empty(),
            "whole-account evidence requires recovery of both pools"
        );
    }

    #[test]
    fn success_heals_only_serving_identity_and_evidence_older_than_admission() {
        let scratch = cortexkit_test_support::ScratchDir::new("fence-success-boundary");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "fence-success-boundary",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        for credential in ["served", "sibling"] {
            store
                .record_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: credential.into(),
                    model: "m".into(),
                    until_ms: 0,
                    decision_id: "d".into(),
                    recorded_at: 100,
                })
                .unwrap();
            store
                .record_transient_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: credential.into(),
                    model: "m".into(),
                    until_ms: 10_000,
                    decision_id: "d".into(),
                    recorded_at: 100,
                })
                .unwrap();
        }
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences("p", "a", "served", "m", None, 99, 200)
            })
            .unwrap();
        assert_eq!(store.account_route_fences(200).unwrap().len(), 2);
        store
            .account_transaction(|tx| {
                tx.heal_serving_fences("p", "a", "served", "m", None, 101, 200)
            })
            .unwrap();
        assert_eq!(
            store.account_route_fences(200).unwrap()[0].credential_id,
            "sibling"
        );
        assert_eq!(
            store.transient_account_route_fences(200).unwrap()[0].credential_id,
            "sibling"
        );
    }
}
