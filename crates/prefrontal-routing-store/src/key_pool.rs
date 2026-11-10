//! Persistence for the key pool of explicit-only API-key providers: leases (runs
//! in flight), per-key walls and cooldowns, and session workspace affinity. The
//! schema is migration 48; the choice itself is
//! [`prefrontal_routing_core::key_pool::pick_pool_key`].
//!
//! An acquire runs as one immediate transaction (read leases, walls and
//! affinity, decide, write), so two concurrent acquires never both see a key
//! as idle. Nothing is deleted: expiry is a comparison with the clock.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use prefrontal_routing_core::key_pool::{
    key_block, pick_pool_key, PoolBlockedKey, PoolKey, PoolKeyState,
};
use prefrontal_routing_core::RouteStateResult;
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

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

fn key_states(
    conn: &Connection,
    provider: &str,
    now_ms: i64,
    excluded_lease: Option<&str>,
) -> rusqlite::Result<BTreeMap<String, PoolKeyState>> {
    crate::fence_rechecks::sync(conn)?;
    let mut states: BTreeMap<String, PoolKeyState> = BTreeMap::new();
    let mut statement = conn.prepare(
        "SELECT credential_id, last_used_at, walled_until, cooldown_until
         FROM pool_key_state WHERE provider = ?1",
    )?;
    for row in statement.query_map(params![provider], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<i64>>(1)?,
            row.get::<_, Option<i64>>(2)?,
            row.get::<_, Option<i64>>(3)?,
        ))
    })? {
        let (id, last_used, walled, cooldown) = row?;
        let state = states.entry(id).or_default();
        state.last_used_at_ms = last_used;
        state.walled_until_ms = walled;
        state.cooldown_until_ms = cooldown;
    }
    let rechecks = crate::fence_rechecks::read(conn)?;
    if crate::fence_rechecks::enabled(conn)? {
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
    let mut statement = conn.prepare(
        "SELECT credential_id, COUNT(*) FROM pool_key_lease
         WHERE provider = ?1 AND released_at IS NULL AND valid_until > ?2
           AND lease_id IS NOT ?3
         GROUP BY credential_id",
    )?;
    for row in statement.query_map(params![provider, now_ms, excluded_lease], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })? {
        let (id, count) = row?;
        states.entry(id).or_default().in_flight = u32::try_from(count).unwrap_or(u32::MAX);
    }
    Ok(states)
}

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
    fn pool_transaction<T>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> RouteStateResult<T> {
        self.store
            .with_conn(|conn: &Connection| {
                let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
                let result = f(&tx)?;
                tx.commit()?;
                Ok(result)
            })
            .map_err(err)
    }

    /// Lease a key for the next run of a session. A session whose live lease is
    /// still on an admissible key keeps it; otherwise that lease is released and
    /// a key is picked, sticking to the session's workspace while it has one.
    pub fn pool_acquire(&self, request: &PoolAcquire<'_>) -> RouteStateResult<PoolAcquireOutcome> {
        self.pool_acquire_for_route(request, &[request.provider], "", &[], &BTreeMap::new())
    }

    pub fn pool_acquire_for_route(
        &self,
        request: &PoolAcquire<'_>,
        providers: &[&str],
        model: &str,
        recovered: &[(String, String)],
        accounts: &BTreeMap<String, String>,
    ) -> RouteStateResult<PoolAcquireOutcome> {
        self.pool_transaction(|conn| {
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
            let mut states = key_states(
                conn,
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
                && !recovered.contains(&(origin.kind.clone(),origin.origin_key.clone()))
            };
            let origins=crate::fence_rechecks::read(conn)?;
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
                    if !crate::fence_rechecks::claim(conn,&origins,now)? {return Err(rusqlite::Error::InvalidQuery);}
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
            if !crate::fence_rechecks::claim(conn,&origins,now)? {return Err(rusqlite::Error::InvalidQuery);}
            let lease = lease_id(now);
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
        self.pool_transaction(|conn| {
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
                    crate::fence_rechecks::sync(conn)?;
                    if crate::fence_rechecks::read(conn)?.iter().any(|origin|origin.provider==provider) {
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
        self.pool_transaction(|conn| {
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

    /// Wall one key until `until_ms`. A later wall of the same kind extends it;
    /// an earlier one never shortens it.
    pub fn pool_wall(
        &self,
        provider: &str,
        credential_id: &str,
        kind: PoolWallKind,
        until_ms: i64,
        reason: &str,
        now_ms: i64,
    ) -> RouteStateResult<()> {
        self.account_transaction(|tx| {
            tx.pool_wall(provider, credential_id, kind, until_ms, reason, now_ms)
        })
    }

    /// Every key state routing holds for `provider`, with live leases counted.
    pub fn pool_key_states(
        &self,
        provider: &str,
        now_ms: i64,
    ) -> RouteStateResult<BTreeMap<String, PoolKeyState>> {
        self.account_transaction(|tx| key_states(tx.conn, provider, now_ms, None).map_err(err))
    }
}

impl crate::AccountTx<'_> {
    /// Wall a key atomically with the observation that reported the refusal.
    pub fn pool_wall(
        &self,
        provider: &str,
        credential_id: &str,
        kind: PoolWallKind,
        until_ms: i64,
        reason: &str,
        now_ms: i64,
    ) -> RouteStateResult<()> {
        let until_ms = until_ms.min(
            now_ms.saturating_add(prefrontal_routing_core::fence_lifetimes::NON_QUOTA_FENCE_MAX_MS),
        );
        let statement = match kind {
            PoolWallKind::Billing => {
                "INSERT INTO pool_key_state (provider, credential_id, walled_until, wall_reason, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(provider, credential_id) DO UPDATE SET
                   walled_until = MAX(COALESCE(pool_key_state.walled_until, 0), excluded.walled_until),
                   wall_reason = excluded.wall_reason, updated_at = excluded.updated_at"
            }
            PoolWallKind::RateLimited => {
                "INSERT INTO pool_key_state (provider, credential_id, cooldown_until, cooldown_reason, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(provider, credential_id) DO UPDATE SET
                   cooldown_until = MAX(COALESCE(pool_key_state.cooldown_until, 0), excluded.cooldown_until),
                   cooldown_reason = excluded.cooldown_reason, updated_at = excluded.updated_at"
            }
        };
        self.conn
            .execute(
                statement,
                params![provider, credential_id, until_ms, reason, now_ms],
            )
            .map_err(err)?;
        let observed = match kind {
            PoolWallKind::Billing => "wall_observed_at",
            PoolWallKind::RateLimited => "cooldown_observed_at",
        };
        if !crate::fence_rechecks::enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        self.conn.execute(&format!("UPDATE pool_key_state SET {observed}=MAX(COALESCE({observed},?3),?3) WHERE provider=?1 AND credential_id=?2"),params![provider,credential_id,now_ms]).map(|_|()).map_err(err)
    }
}
