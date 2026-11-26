//! Persistence for the key pool of explicit-only API-key providers: leases (runs
//! in flight), per-key walls and cooldowns, and session workspace affinity. The
//! schema is migration 48; the choice itself is
//! [`striatum_core::key_pool::pick_pool_key`].
//!
//! An acquire runs as one immediate transaction (read leases, walls and
//! affinity, decide, write), so two concurrent acquires never both see a key
//! as idle. Nothing is deleted: expiry is a comparison with the clock.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::{params, Connection, OptionalExtension};
use striatum_core::key_pool::{key_block, pick_pool_key, PoolBlockedKey, PoolKey, PoolKeyState};
use striatum_core::RouteStateResult;

use super::{err, SqliteRouteStateStore};

/// One acquire request: a key for the next run of `session_id`.
#[derive(Debug, Clone)]
pub struct PoolAcquire<'a> {
    pub provider: &'a str,
    /// The session the run belongs to, stable across its revivals.
    pub session_id: &'a str,
    /// The pool's members from the current vault listing.
    pub keys: &'a [PoolKey],
    pub now_ms: i64,
    /// How long a lease counts as a run in flight without a renewal.
    pub lease_ms: i64,
    /// How long an idle session keeps its workspace.
    pub affinity_idle_ms: i64,
}

/// A probe origin the caller found recovered for a pool key (its quota
/// reading shows room again): kind, origin key and the evidence it was
/// recovered from. Acquisition skips only a clock that still holds that
/// evidence, so a recovery read from an older state cannot lift a block
/// renewed since, including on a run again after `STATE_DIVERGED`.
pub type RecoveredOrigin = (String, String, String);

/// A granted lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolLease {
    pub lease_id: String,
    pub credential_id: String,
    pub workspace: String,
    pub valid_until_ms: i64,
    /// The session's live lease was still on an admissible key and was kept.
    pub reused: bool,
    pub moved_from: Option<String>,
    pub move_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolAcquireOutcome {
    Leased(PoolLease),
    /// No admissible key: every member with its reason.
    Exhausted(Vec<PoolBlockedKey>),
}

/// What a key wall records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolWallKind {
    /// Out of credit (`billing_exhausted`): held until a bounded re-probe time.
    Billing,
    /// A 429 ended a run: held until the provider's retry-after.
    RateLimited,
}

fn lease_id(now_ms: i64) -> String {
    // Replay recordings install fixed inputs so recorded lease ids don't carry
    // this process's id; the replay module only exists in test builds.
    #[cfg(any(test, feature = "test-support"))]
    if let Some(inputs) = crate::replay::current() {
        return format!("pool_{}", inputs.next_id());
    }
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "pool_{now_ms:x}_{:x}_{:x}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod lease_id_tests {
    use super::lease_id;

    #[test]
    fn replay_inputs_control_pool_lease_ids() {
        let _inputs = crate::replay::Inputs::new(1_800_000_000_000).install();
        assert_eq!(lease_id(1_800_000_000_000), "pool_000000000000");
        assert_eq!(lease_id(1_800_000_000_000), "pool_000000000001");
    }

    #[test]
    fn production_pool_lease_ids_keep_timestamp_pid_counter_shape() {
        assert!(crate::replay::current().is_none());
        let id = lease_id(1_800_000_000_000);
        let parts: Vec<_> = id.split('_').collect();
        assert_eq!(parts.len(), 4, "{id}");
        assert_eq!(parts[0], "pool");
        assert_eq!(parts[1], "1a3185c5000");
        assert_eq!(parts[2], format!("{:x}", std::process::id()));
        assert!(u64::from_str_radix(parts[3], 16).is_ok(), "{id}");
    }
}

/// One key's row: credential, last use, wall and cooldown deadlines.
type KeyRow = (String, Option<i64>, Option<i64>, Option<i64>);

pub(crate) fn read_key_states(
    conn: &Connection,
    provider: &str,
    now_ms: i64,
    excluded_lease: Option<&str>,
) -> rusqlite::Result<BTreeMap<String, PoolKeyState>> {
    let state = crate::BlockState::read(conn)?;
    let rows = state.pool_key_rows(provider)?;
    let rechecks = state.clock_rechecks();
    let clock_schema = state.enabled();
    let leases = lease_counts(conn, provider, now_ms, excluded_lease)?;
    Ok(key_states_from(
        provider,
        rows,
        &rechecks,
        clock_schema,
        leases,
    ))
}

/// Key states from the key rows, the stored clocks and the live lease
/// counts, wherever they were read. With probe clocks, a key's wall and
/// cooldown are its clocks' due times, not the row's raw deadlines.
pub(crate) fn key_states_from(
    provider: &str,
    rows: Vec<KeyRow>,
    rechecks: &[crate::FenceRecheck],
    clock_schema: bool,
    leases: Vec<(String, i64)>,
) -> BTreeMap<String, PoolKeyState> {
    let mut states: BTreeMap<String, PoolKeyState> = BTreeMap::new();
    for (id, last_used, walled, cooldown) in rows {
        let state = states.entry(id).or_default();
        state.last_used_at_ms = last_used;
        state.walled_until_ms = walled;
        state.cooldown_until_ms = cooldown;
    }
    if clock_schema {
        for state in states.values_mut() {
            state.walled_until_ms = None;
            state.cooldown_until_ms = None;
        }
    }
    for origin in rechecks.iter().filter(|origin| {
        matches!(origin.kind.as_str(), "pool_billing" | "pool_rate")
            && origin.provider == provider
            && origin.account_id.is_none()
            && origin.model.is_none()
            && origin.principal.is_none()
            && origin.size_class.is_none()
    }) {
        if let Some(credential) = &origin.credential_id {
            let state = states.entry(credential.clone()).or_default();
            let field = if origin.kind == "pool_rate" {
                &mut state.cooldown_until_ms
            } else {
                &mut state.walled_until_ms
            };
            *field = Some(field.unwrap_or(0).max(origin.due_at()));
        }
    }
    for (id, count) in leases {
        states.entry(id).or_default().in_flight = u32::try_from(count).unwrap_or(u32::MAX);
    }
    states
}

/// Live leases per key of `provider`, from the lease table, which the
/// account state does not hold.
pub(crate) fn lease_counts(
    conn: &Connection,
    provider: &str,
    now_ms: i64,
    excluded_lease: Option<&str>,
) -> rusqlite::Result<Vec<(String, i64)>> {
    let mut statement = conn.prepare_cached(LEASE_COUNTS_SQL)?;
    let rows = statement.query_map(params![provider, now_ms, excluded_lease], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    rows.collect()
}

const LEASE_COUNTS_SQL: &str = "SELECT credential_id, COUNT(*) FROM pool_key_lease
         WHERE provider = ?1 AND released_at IS NULL AND valid_until > ?2
           AND lease_id IS NOT ?3
         GROUP BY credential_id";

fn touch(
    conn: &Connection,
    provider: &str,
    session_id: &str,
    credential_id: &str,
    workspace: &str,
    now_ms: i64,
    affinity_idle_ms: i64,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO pool_key_state (provider, credential_id, last_used_at, updated_at)
         VALUES (?1, ?2, ?3, ?3)
         ON CONFLICT(provider, credential_id) DO UPDATE SET
           last_used_at = excluded.last_used_at, updated_at = excluded.updated_at",
        params![provider, credential_id, now_ms],
    )?;
    conn.execute(
        "INSERT INTO pool_session_affinity
           (provider, session_id, workspace, credential_id, bound_at, last_used_at, expires_at, ended_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6, NULL)
         ON CONFLICT(provider, session_id) DO UPDATE SET
           bound_at = CASE WHEN pool_session_affinity.workspace = excluded.workspace
                             AND pool_session_affinity.ended_at IS NULL
                           THEN pool_session_affinity.bound_at ELSE excluded.bound_at END,
           workspace = excluded.workspace,
           credential_id = excluded.credential_id,
           last_used_at = excluded.last_used_at,
           expires_at = excluded.expires_at,
           ended_at = NULL",
        params![
            provider,
            session_id,
            workspace,
            credential_id,
            now_ms,
            now_ms.saturating_add(affinity_idle_ms)
        ],
    )?;
    Ok(())
}

impl SqliteRouteStateStore {
    /// Lease a key for the next run of a session. A session whose live lease is
    /// still on an admissible key keeps it; otherwise that lease is released and
    /// a key is picked, sticking to the session's workspace while it has one.
    pub fn pool_acquire(&self, request: &PoolAcquire<'_>) -> RouteStateResult<PoolAcquireOutcome> {
        let provider = request.provider.to_owned();
        let session = request.session_id.to_owned();
        let keys = request.keys.to_owned();
        let now_ms = request.now_ms;
        let lease_ms = request.lease_ms;
        let affinity_idle_ms = request.affinity_idle_ms;
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            let request = PoolAcquire {
                provider: &provider,
                session_id: &session,
                keys: &keys,
                now_ms,
                lease_ms,
                affinity_idle_ms,
            };
            tx.pool_acquire(&request)
        })
    }

    pub fn pool_acquire_for_route(
        &self,
        request: &PoolAcquire<'_>,
        providers: &[&str],
        model: &str,
        recovered: &[RecoveredOrigin],
        accounts: &BTreeMap<String, String>,
    ) -> RouteStateResult<PoolAcquireOutcome> {
        let provider = request.provider.to_owned();
        let session = request.session_id.to_owned();
        let keys = request.keys.to_owned();
        let now_ms = request.now_ms;
        let lease_ms = request.lease_ms;
        let affinity_idle_ms = request.affinity_idle_ms;
        let providers: Vec<String> = providers.iter().map(|s| (*s).to_owned()).collect();
        let model = model.to_owned();
        let recovered = recovered.to_owned();
        let accounts = accounts.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            let request = PoolAcquire {
                provider: &provider,
                session_id: &session,
                keys: &keys,
                now_ms,
                lease_ms,
                affinity_idle_ms,
            };
            tx.pool_acquire_for_route(
                &request,
                &providers.iter().map(String::as_str).collect::<Vec<_>>(),
                &model,
                &recovered,
                &accounts,
            )
        })
    }

    /// Keep a live run's lease counting. A lease that expired while its run was
    /// still live (a core restart outlasted it) is reinstated on the same key: a
    /// running run never changes key. Returns false for a released lease, and
    /// for a lease routing never issued unless `credential_id` is given, in
    /// which case it is recorded as that run's lease.
    pub fn pool_renew(
        &self,
        provider: &str,
        session_id: &str,
        lease_id: &str,
        credential_id: Option<&str>,
        now_ms: i64,
        lease_ms: i64,
    ) -> RouteStateResult<Option<i64>> {
        let provider = provider.to_owned();
        let session_id = session_id.to_owned();
        let lease_id = lease_id.to_owned();
        let credential_id = credential_id.map(str::to_owned);
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.pool_renew(
                &provider,
                &session_id,
                &lease_id,
                credential_id.as_deref(),
                now_ms,
                lease_ms,
            )
        })
    }

    /// Release a session's live leases (or one lease), idempotently. With
    /// `end_session` the session's workspace affinity ends too.
    pub fn pool_release(
        &self,
        provider: &str,
        session_id: &str,
        lease_id: Option<&str>,
        end_session: bool,
        now_ms: i64,
    ) -> RouteStateResult<usize> {
        let provider = provider.to_owned();
        let session_id = session_id.to_owned();
        let lease_id = lease_id.map(str::to_owned);
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.pool_release(
                &provider,
                &session_id,
                lease_id.as_deref(),
                end_session,
                now_ms,
            )
        })
    }

    /// Record a key refusal, replacing the same kind's older horizon when the
    /// evidence is newer. The deadline is capped at one hour from receipt.
    pub fn pool_wall(
        &self,
        provider: &str,
        credential_id: &str,
        kind: PoolWallKind,
        until_ms: i64,
        reason: &str,
        now_ms: i64,
    ) -> RouteStateResult<()> {
        let provider = provider.to_owned();
        let credential_id = credential_id.to_owned();
        let reason = reason.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.pool_wall(&provider, &credential_id, kind, until_ms, &reason, now_ms)
        })
    }

    /// Every key state routing holds for `provider`, with live leases counted.
    pub fn pool_key_states(
        &self,
        provider: &str,
        now_ms: i64,
    ) -> RouteStateResult<BTreeMap<String, PoolKeyState>> {
        self.read_first_key_states(provider, now_ms, || {})
    }

    #[cfg(feature = "test-support")]
    pub fn pool_key_states_after_read_for_test(
        &self,
        provider: &str,
        now_ms: i64,
        after_read: impl FnOnce(),
    ) -> RouteStateResult<BTreeMap<String, PoolKeyState>> {
        self.read_first_key_states(provider, now_ms, after_read)
    }

    /// Always synchronize recheck clocks in a write transaction before reading
    /// key rows. Tests compare rows and routing answers against
    /// [`Self::pool_key_states_from`], which checks whether synchronization is needed.
    #[cfg(feature = "test-support")]
    pub fn pre_read_first_pool_key_states_for_test(
        &self,
        provider: &str,
        now_ms: i64,
    ) -> RouteStateResult<BTreeMap<String, PoolKeyState>> {
        self.account_transaction(|tx| tx.key_states_on(provider, now_ms, None).map_err(err))
    }

    /// Key states for a request outside the writer, from `published`, the
    /// account state's block tables, with the live leases from a pooled
    /// reader. A command synchronizes the clocks first only when the state
    /// shows a sync would change one. Without a state this is the SQL
    /// [`Self::pool_key_states`].
    pub fn pool_key_states_from(
        &self,
        provider: &str,
        now_ms: i64,
        published: Option<&crate::BlockState>,
    ) -> RouteStateResult<BTreeMap<String, PoolKeyState>> {
        let Some(state) = published.filter(|state| state.enabled()) else {
            return self.pool_key_states(provider, now_ms);
        };
        if !state.needs_sync() {
            let rows = state.pool_key_rows(provider).map_err(err)?;
            let leases = self
                .store
                .with_read(|conn: &Connection| Ok(lease_counts(conn, provider, now_ms, None)?))
                .map_err(err)?;
            return Ok(key_states_from(
                provider,
                rows,
                &state.clock_rechecks(),
                true,
                leases,
            ));
        }
        let provider = provider.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.key_states_on(&provider, now_ms, None).map_err(err)
        })
    }

    fn read_first_key_states(
        &self,
        provider: &str,
        now_ms: i64,
        after_read: impl FnOnce(),
    ) -> RouteStateResult<BTreeMap<String, PoolKeyState>> {
        let (states, needs_sync) = self
            .store
            .with_read(|conn: &Connection| {
                // The SQL reader, for a store without account state; request
                // paths use `pool_key_states_from`.
                Ok((
                    read_key_states(conn, provider, now_ms, None)?,
                    crate::fence_rechecks::needs_sync(conn)?,
                ))
            })
            .map_err(err)?;
        if !needs_sync {
            return Ok(states);
        }
        after_read();
        let provider = provider.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            let conn = tx.conn;
            // The read pass is only a hint. Recheck the current fence
            // evidence before syncing: another writer may have reported
            // or claimed a newer refusal since the read.
            let changed = crate::fence_rechecks::needs_sync(conn).map_err(err)?;
            if changed {
                crate::fence_rechecks::sync_on(&crate::AccountTx::sql(conn), crate::now_ms())
                    .map_err(err)?;
            }
            let states = read_key_states(conn, &provider, now_ms, None).map_err(err)?;
            Ok(states)
        })
    }
}

impl crate::AccountTx<'_> {
    /// Key states inside a command: the clocks synchronized first, then the
    /// key rows and clocks read from the working state (SQL without one),
    /// and the live leases from the lease table.
    fn key_states_on(
        &self,
        provider: &str,
        now_ms: i64,
        excluded_lease: Option<&str>,
    ) -> rusqlite::Result<BTreeMap<String, PoolKeyState>> {
        crate::fence_rechecks::sync_on(self, crate::now_ms())?;
        let state = self
            .working_blocks()
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(error.0.into()))?;
        match state {
            Some(state) => Ok(key_states_from(
                provider,
                state.pool_key_rows(provider)?,
                &state.clock_rechecks(),
                true,
                lease_counts(self.conn, provider, now_ms, excluded_lease)?,
            )),
            None => read_key_states(self.conn, provider, now_ms, excluded_lease),
        }
    }

    /// A lease is granted, reused, released or refused on key states, clocks
    /// and route clears read from the working state, and on sync's finding
    /// that no other source row needs a clock; none of its writes names
    /// them. So before deciding, check that SQL holds each provider's block
    /// rows exactly as the state does (`BlockState::check_provider_rows`); a
    /// difference raises `STATE_DIVERGED`, and the batch runs again on a
    /// fresh load. Without a state SQL decided.
    fn check_lease_inputs(&self, providers: &[&str]) -> rusqlite::Result<()> {
        self.check_decision_inputs(providers)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(error.0.into()))
    }

    /// The stored, uncleared clocks: from the working state, or SQL
    /// without one.
    fn clock_rechecks_on(&self) -> rusqlite::Result<Vec<crate::FenceRecheck>> {
        match self
            .working_blocks()
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(error.0.into()))?
        {
            Some(state) => Ok(state.clock_rechecks()),
            None => crate::fence_rechecks::read(self.conn),
        }
    }

    /// Wall a key atomically with its ordering timestamp and retry clock.
    pub fn pool_wall(
        &self,
        provider: &str,
        credential_id: &str,
        kind: PoolWallKind,
        until_ms: i64,
        reason: &str,
        now_ms: i64,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter("account_tx::key_pool::pool_wall");
        let until_ms = until_ms
            .min(now_ms.saturating_add(striatum_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS));
        let (origin, observed) = match kind {
            PoolWallKind::Billing => ("pool_billing", "wall_observed_at"),
            PoolWallKind::RateLimited => ("pool_rate", "cooldown_observed_at"),
        };
        // Leasing can move updated_at but cannot replace refusal ordering.
        // Only a report at least as new as this refusal may replace its wall.
        let changed=self.conn.execute(&format!("INSERT INTO pool_key_state(provider,credential_id,updated_at,{observed})
            VALUES (?1,?2,?3,?3) ON CONFLICT(provider,credential_id) DO UPDATE SET
                {observed}=excluded.{observed},updated_at=MAX(pool_key_state.updated_at,excluded.updated_at)
            WHERE pool_key_state.{observed} IS NULL OR excluded.{observed}>=pool_key_state.{observed}"),
            params![provider,credential_id,now_ms]).map_err(err)?;
        let key = serde_json::json!([provider, credential_id]).to_string();
        if changed != 0 {
            let mut row =
                crate::block_storage::StoredBlock::from_origin(crate::fence_rechecks::ViewOrigin {
                    kind: origin.into(),
                    origin_key: key.clone(),
                    provider: provider.into(),
                    account_id: None,
                    credential_id: Some(credential_id.into()),
                    model: None,
                    size_class: None,
                    principal: None,
                    admission_id: None,
                    evidence: serde_json::json!([until_ms, reason, now_ms]).to_string(),
                    evidence_at: now_ms,
                    quota: Some(false),
                    deadline: Some(until_ms as f64),
                    reset_at: None,
                    rate_limit_retry: false,
                })
                .map_err(err)?;
            row.deadline = Some(crate::block_storage::Deadline::Integer(until_ms));
            row.reason = Some(reason.into());
            row.observed_at = Some(now_ms);
            crate::block_storage::write_source(self.conn, &row).map_err(err)?;
        }
        crate::fence_rechecks::register_on(self, origin, &key).map_err(err)
    }
}

#[cfg(test)]
mod wall_generation_tests {
    use super::*;

    #[test]
    fn pool_live_lease_count_uses_provider_and_expiry_range() {
        let scratch = cortexkit_test_support::ScratchDir::new("pool-live-lease-count-plan");
        let descriptor = crate::dev_descriptor(
            "pool-live-lease-count-plan",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 100).unwrap();
        old.with_writer_for_test(|conn| {
            conn.execute_batch("WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<100000)
               INSERT INTO pool_key_lease (lease_id,provider,session_id,credential_id,workspace,acquired_at,valid_until,released_at)
               SELECT 'old-'||n,'p','s','key','w',1,2,CASE WHEN n%2=0 THEN 2 END FROM ids;
               INSERT INTO pool_key_lease (lease_id,provider,session_id,credential_id,workspace,acquired_at,valid_until,released_at)
               VALUES ('active','p','s','key','w',100,200,NULL),
                      ('released','p','s','key','w',100,200,100),
                      ('excluded','p','s','key','w',100,200,NULL),
                      ('other','q','s','key','w',100,200,NULL);")
        }).unwrap();
        drop(old);

        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        store
            .with_reader_for_test(|conn| {
                assert_eq!(
                    lease_counts(conn, "p", 101, Some("excluded"))?,
                    [("key".into(), 1)]
                );
                let total: usize =
                    conn.query_row("SELECT count(*) FROM pool_key_lease", [], |row| row.get(0))?;
                assert_eq!(total, 100_004, "the upgrade must preserve lease history");
                let plan = conn
                    .prepare(&format!("EXPLAIN QUERY PLAN {LEASE_COUNTS_SQL}"))?
                    .query_map(params!["p", 101, "excluded"], |row| row.get::<_, String>(3))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                assert!(
                    plan.iter().any(|line| line.contains(
                        "USING INDEX pool_key_lease_live_by_expiry (provider=? AND valid_until>?)"
                    )),
                    "{plan:?}"
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn first_pool_refusal_survives_a_later_lease_touch() {
        let scratch = cortexkit_test_support::ScratchDir::new("first-pool-refusal");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "first-pool-refusal",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let keys = vec![PoolKey::new("key", Some("a"), "active")];
        assert!(matches!(
            store
                .pool_acquire(&PoolAcquire {
                    provider: "p",
                    session_id: "s",
                    keys: &keys,
                    now_ms: 101,
                    lease_ms: 120_000,
                    affinity_idle_ms: 120_000,
                })
                .unwrap(),
            PoolAcquireOutcome::Leased(_)
        ));
        for kind in [PoolWallKind::Billing, PoolWallKind::RateLimited] {
            store
                .pool_wall("p", "key", kind, 1100, "first refusal", 100)
                .unwrap();
        }
        let clocks = store.fence_rechecks().unwrap();
        assert_eq!(
            clocks.len(),
            2,
            "lease activity is not same-kind refusal evidence"
        );
        assert!(clocks.iter().all(|origin| origin.evidence_at == 100));
    }

    #[test]
    fn newer_pool_refusal_replaces_a_long_migrated_deadline_with_its_short_retry() {
        let scratch = cortexkit_test_support::ScratchDir::new("migrated-pool-short-retry");
        let descriptor = crate::dev_descriptor(
            "migrated-pool-short-retry",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let now = crate::now_ms();
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 81).unwrap();
        old.store.with_conn(|conn|conn.execute("INSERT INTO pool_key_state(provider,credential_id,walled_until,cooldown_until,updated_at)
            VALUES ('p','key',?1,?1,?2)",params![now+86_400_000,now-100])).unwrap();
        drop(old);
        crate::history::complete_test_upgrade(&descriptor, now);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let origins = store.fence_rechecks().unwrap();
        let probe_at = origins.iter().map(|origin| origin.due_at()).max().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, probe_at, Some("probe")))
            .unwrap());
        for kind in [PoolWallKind::Billing, PoolWallKind::RateLimited] {
            store
                .pool_wall(
                    "p",
                    "key",
                    kind,
                    probe_at + 10_001,
                    "short refusal",
                    probe_at + 1,
                )
                .unwrap();
        }
        let clocks = store.fence_rechecks().unwrap();
        assert_eq!(clocks.len(), 2);
        assert!(
            clocks
                .iter()
                .all(|clock| clock.next_recheck_at == probe_at + 10_001),
            "new short retries must replace the old raw 24h wall: {clocks:?}"
        );
        for kind in [PoolWallKind::Billing, PoolWallKind::RateLimited] {
            store
                .pool_wall(
                    "p",
                    "key",
                    kind,
                    probe_at + 3_600_000,
                    "older delayed refusal",
                    probe_at,
                )
                .unwrap();
        }
        assert!(
            store
                .fence_rechecks()
                .unwrap()
                .iter()
                .all(|clock| clock.next_recheck_at == probe_at + 10_001),
            "an older report must not overwrite newer short evidence"
        );
    }
}

impl crate::AccountTx<'_> {
    pub fn pool_acquire(&self, request: &PoolAcquire<'_>) -> RouteStateResult<PoolAcquireOutcome> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter("account_tx::key_pool::pool_acquire");
        self.pool_acquire_for_route(request, &[request.provider], "", &[], &BTreeMap::new())
    }

    pub fn pool_acquire_for_route(
        &self,
        request: &PoolAcquire<'_>,
        providers: &[&str],
        model: &str,
        recovered: &[RecoveredOrigin],
        accounts: &BTreeMap<String, String>,
    ) -> RouteStateResult<PoolAcquireOutcome> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::key_pool::pool_acquire_for_route");
        self.pool_sql(|conn| {
            let now = request.now_ms;
            let existing: Option<(String, String, String)> = conn
                .query_row(
                    "SELECT lease_id, credential_id, workspace FROM pool_key_lease
                     WHERE provider = ?1 AND session_id = ?2 AND released_at IS NULL
                       AND valid_until > ?3
                     ORDER BY acquired_at DESC LIMIT 1",
                    params![request.provider, request.session_id, now],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            let mut states = self.key_states_on(
                request.provider,
                now,
                existing.as_ref().map(|(lease, _, _)| lease.as_str()),
            )?;
            let applies=|origin:&crate::FenceRecheck,workspace:&str,credential:&str| {
                let account=accounts.get(credential).map_or(workspace,String::as_str);
                let named=origin.credential_id.as_deref().filter(|id|(origin.quota || matches!(origin.kind.as_str(),"account_route"|"non_quota_route"|"transient_route"))
                    && accounts.get(*id).is_some_and(|id|id==account)).unwrap_or(credential);
                providers.contains(&origin.provider.as_str())
                && origin.applies(&origin.provider,account,named,model) && origin.principal.is_none() && origin.size_class.is_none()
                && !recovered.iter().any(|(kind,key,evidence)|*kind==origin.kind && *key==origin.origin_key && *evidence==origin.evidence)
            };
            let origins=self.clock_rechecks_on()?;
            // The pool's own provider (its key rows and pool clocks) and
            // every provider whose clocks apply to the route.
            let mut lease_providers: Vec<&str> = providers.to_vec();
            if !lease_providers.contains(&request.provider) {
                lease_providers.push(request.provider);
            }
            // Every decision below (reuse, refusal, release, a new pick)
            // rests on these providers' rows as the state shows them.
            self.check_lease_inputs(&lease_providers)?;
            for key in request.keys {
                let quota_at=crate::FenceRecheck::quota_group_at(origins.iter().filter(|origin|applies(origin,&key.workspace,&key.credential_id)),None);
                for origin in origins.iter().filter(|origin|applies(origin,&key.workspace,&key.credential_id)) {
                    let state=states.entry(key.credential_id.clone()).or_default();
                    let field=if origin.kind=="pool_rate" {&mut state.cooldown_until_ms}else{&mut state.walled_until_ms};
                    *field=Some(field.unwrap_or(0).max(if origin.quota {quota_at}else{origin.due_at()}));
                }
            }
            if let Some((lease, credential, workspace)) = &existing {
                let still_admissible = request.keys.iter().any(|key| {
                    key.credential_id == *credential
                        && key_block(key, states.get(credential).unwrap_or(&PoolKeyState::default()), now)
                            .is_none()
                });
                let probe=crate::fence_rechecks::enabled(conn)? && conn.query_row("SELECT fence_probe FROM pool_key_lease WHERE lease_id=?1",[lease],|row|row.get::<_,bool>(0))?;
                if probe && !still_admissible {
                    let blocked=request.keys.iter().filter(|key|key.credential_id==*credential).flat_map(|key|
                        pick_pool_key(std::slice::from_ref(key),&states,Some(credential),now).err().unwrap_or_default()).collect();
                    // An acquire is another send authorization, not a renewal.
                    // Keep the original probe lease alive for its holder.
                    return Ok(PoolAcquireOutcome::Exhausted(blocked));
                }
                if still_admissible {
                    let origins:Vec<_>=origins.iter().filter(|origin|applies(origin,workspace,credential)).cloned().collect();
                    if !self.claim_for_lease(&origins,now,lease).map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))? {return Err(rusqlite::Error::InvalidQuery);}
                    if !origins.is_empty() {conn.execute("UPDATE pool_key_lease SET fence_probe=1 WHERE lease_id=?1",[lease])?;}
                    let valid_until = now.saturating_add(request.lease_ms);
                    conn.execute(
                        "UPDATE pool_key_lease SET valid_until = ?2 WHERE lease_id = ?1",
                        params![lease, valid_until],
                    )?;
                    touch(conn, request.provider, request.session_id, credential, workspace, now, request.affinity_idle_ms)?;
                    return Ok(PoolAcquireOutcome::Leased(PoolLease {
                        lease_id: lease.clone(),
                        credential_id: credential.clone(),
                        workspace: workspace.clone(),
                        valid_until_ms: valid_until,
                        reused: true,
                        moved_from: None,
                        move_reason: None,
                    }));
                }
                conn.execute(
                    "UPDATE pool_key_lease SET released_at = ?2 WHERE lease_id = ?1",
                    params![lease, now],
                )?;
            }
            let bound: Option<String> = conn
                .query_row(
                    "SELECT workspace FROM pool_session_affinity
                     WHERE provider = ?1 AND session_id = ?2 AND ended_at IS NULL
                       AND expires_at > ?3",
                    params![request.provider, request.session_id, now],
                    |row| row.get(0),
                )
                .optional()?;
            let pick = match pick_pool_key(request.keys, &states, bound.as_deref(), now) {
                Ok(pick) => pick,
                Err(blocked) => return Ok(PoolAcquireOutcome::Exhausted(blocked)),
            };
            let origins:Vec<_>=origins.iter().filter(|origin|applies(origin,&pick.workspace,&pick.credential_id)).cloned().collect();
            let lease = lease_id(now);
            if !self.claim_for_lease(&origins,now,&lease).map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))? {return Err(rusqlite::Error::InvalidQuery);}
            let valid_until = now.saturating_add(request.lease_ms);
            conn.execute(
                "INSERT INTO pool_key_lease
                   (lease_id, provider, session_id, credential_id, workspace, acquired_at, valid_until)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![lease, request.provider, request.session_id, pick.credential_id, pick.workspace, now, valid_until],
            )?;
            if !origins.is_empty() {conn.execute("UPDATE pool_key_lease SET fence_probe=1 WHERE lease_id=?1",[&lease])?;}
            touch(conn, request.provider, request.session_id, &pick.credential_id, &pick.workspace, now, request.affinity_idle_ms)?;
            Ok(PoolAcquireOutcome::Leased(PoolLease {
                lease_id: lease,
                credential_id: pick.credential_id,
                workspace: pick.workspace,
                valid_until_ms: valid_until,
                reused: false,
                moved_from: pick.moved_from,
                move_reason: pick.move_reason,
            }))
        })
    }

    pub fn pool_renew(
        &self,
        provider: &str,
        session_id: &str,
        lease_id: &str,
        credential_id: Option<&str>,
        now_ms: i64,
        lease_ms: i64,
    ) -> RouteStateResult<Option<i64>> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter("account_tx::key_pool::pool_renew");
        self.pool_sql(|conn| {
            let valid_until = now_ms.saturating_add(lease_ms);
            let released: Option<Option<i64>> = conn
                .query_row(
                    "SELECT released_at FROM pool_key_lease WHERE lease_id = ?1",
                    params![lease_id],
                    |row| row.get(0),
                )
                .optional()?;
            match released {
                Some(Some(_)) => Ok(None),
                Some(None) => {
                    conn.execute(
                        "UPDATE pool_key_lease SET valid_until = ?2 WHERE lease_id = ?1",
                        params![lease_id, valid_until],
                    )?;
                    Ok(Some(valid_until))
                }
                None => {
                    let Some(credential_id) = credential_id else {
                        return Ok(None);
                    };
                    crate::fence_rechecks::sync_on(self, crate::now_ms())?;
                    // The refusal below and the grant both rest on the
                    // provider's rows as the state shows them.
                    self.check_lease_inputs(&[provider])?;
                    if self.clock_rechecks_on()?.iter().any(|origin|origin.provider==provider) {
                        // Rehydrating an unknown lease cannot prove that it
                        // owns a fenced account's one probe authorization.
                        return Ok(None);
                    }
                    let workspace: String = conn
                        .query_row(
                            "SELECT workspace FROM pool_session_affinity
                             WHERE provider = ?1 AND session_id = ?2",
                            params![provider, session_id],
                            |row| row.get(0),
                        )
                        .optional()?
                        .unwrap_or_else(|| format!("key:{credential_id}"));
                    conn.execute(
                        "INSERT INTO pool_key_lease
                           (lease_id, provider, session_id, credential_id, workspace, acquired_at, valid_until)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![lease_id, provider, session_id, credential_id, workspace, now_ms, valid_until],
                    )?;
                    Ok(Some(valid_until))
                }
            }
        })
    }

    pub fn pool_release(
        &self,
        provider: &str,
        session_id: &str,
        lease_id: Option<&str>,
        end_session: bool,
        now_ms: i64,
    ) -> RouteStateResult<usize> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter("account_tx::key_pool::pool_release");
        self.pool_sql(|conn| {
            let released = conn.execute(
                "UPDATE pool_key_lease SET released_at = ?4
                 WHERE provider = ?1 AND session_id = ?2 AND released_at IS NULL
                   AND (?3 IS NULL OR lease_id = ?3)",
                params![provider, session_id, lease_id, now_ms],
            )?;
            if end_session {
                conn.execute(
                    "UPDATE pool_session_affinity SET ended_at = ?3
                     WHERE provider = ?1 AND session_id = ?2 AND ended_at IS NULL",
                    params![provider, session_id, now_ms],
                )?;
            }
            Ok(released)
        })
    }
}
