//! Durable, per-origin retry clocks. Selection reads these clocks without
//! consuming them; admission claims every applicable clock in one transaction.

use crate::{err, AccountTx, SqliteRouteStateStore};
use prefrontal_routing_core::RouteStateResult;
use rusqlite::{params, Connection, OptionalExtension};

pub use prefrontal_routing_core::fence_lifetimes::{
    NON_QUOTA_FENCE_MAX_MS, QUOTA_FENCE_RECHECK_MS,
};

#[derive(Clone, Debug)]
pub struct FenceRecheck {
    pub evidence: String,
    pub kind: String,
    pub origin_key: String,
    pub provider: String,
    pub account_id: Option<String>,
    pub credential_id: Option<String>,
    pub model: Option<String>,
    pub size_class: Option<i64>,
    pub principal: Option<String>,
    pub admission_id: Option<String>,
    pub evidence_at: i64,
    pub quota: bool,
    pub next_recheck_at: i64,
    pub reset_check_at: Option<i64>,
    pub last_claimed_at: Option<i64>,
    pub claim_owner: Option<String>,
    pub cleared_models: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct QuotaSuccessScope {
    pub family: prefrontal_routing_core::accounts::ModelFamily,
    pub pool: Option<String>,
}

impl QuotaSuccessScope {
    fn used(&self, key: &prefrontal_routing_core::accounts::PoolKey, model: &str) -> bool {
        key.scope.applies_to(model, self.family)
            && match (&key.name, &self.pool) {
                (Some(expected), Some(actual)) => expected == actual,
                (None, _) => true,
                _ => false,
            }
    }
}

impl FenceRecheck {
    pub fn quota_group_due<'a>(
        origins: impl IntoIterator<Item = &'a Self>,
        now: i64,
        owner: Option<&str>,
    ) -> bool {
        Self::quota_group_at(origins, owner) <= now
    }
    pub fn quota_group_at<'a>(
        origins: impl IntoIterator<Item = &'a Self>,
        owner: Option<&str>,
    ) -> i64 {
        let origins: Vec<_> = origins
            .into_iter()
            .filter(|origin| {
                origin.quota && !(owner.is_some() && origin.claim_owner.as_deref() == owner)
            })
            .collect();
        let latest = origins
            .iter()
            .map(|origin| origin.evidence_at)
            .max()
            .unwrap_or(i64::MIN);
        origins
            .iter()
            .map(|origin| origin.next_recheck_at)
            .max()
            .unwrap_or(i64::MIN)
            .min(
                origins
                    .iter()
                    .filter_map(|origin| origin.reset_check_at)
                    .filter(|reset| *reset > latest)
                    .min()
                    .unwrap_or(i64::MAX),
            )
    }
    pub fn due_at(&self) -> i64 {
        self.reset_check_at.map_or(self.next_recheck_at, |reset| {
            reset.min(self.next_recheck_at)
        })
    }

    pub fn applies(&self, provider: &str, account: &str, credential: &str, model: &str) -> bool {
        (self.provider.is_empty() || self.provider == provider)
            && self.account_id.as_deref().is_none_or(|id| id == account)
            && self
                .credential_id
                .as_deref()
                .is_none_or(|id| id == credential)
            && self.model.as_deref().is_none_or(|id| id == model)
            && !self.cleared_models.iter().any(|cleared| cleared == model)
    }
}

pub(crate) fn enabled(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='fence_recheck')",
        [],
        |row| row.get(0),
    )
}

pub(crate) fn sync(conn: &Connection) -> rusqlite::Result<()> {
    if !enabled(conn)? {
        return Ok(());
    }
    // Materialize once and index the compound origin key. A correlated lookup
    // against the UNION view otherwise rebuilds its JSON keys for every clock.
    conn.execute_batch("CREATE TEMP TABLE IF NOT EXISTS current_fence_origin AS SELECT * FROM fence_origin WHERE 0;
        CREATE UNIQUE INDEX IF NOT EXISTS current_fence_origin_key ON current_fence_origin(kind,origin_key);
        DELETE FROM current_fence_origin;
        INSERT INTO current_fence_origin SELECT * FROM fence_origin;")?;
    // A changed evidence value is new evidence, not a probe. Pool evidence
    // omits the shared last-used timestamp so leasing a key cannot extend its
    // retry deadline merely by changing that timestamp.
    conn.execute(
        "INSERT INTO fence_recheck (kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
          evidence,evidence_at,quota,next_recheck_at,reset_check_at)
         SELECT kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
          evidence,evidence_at,COALESCE(quota,0),
          CAST(CASE WHEN quota=1 THEN evidence_at+900000 ELSE MIN(COALESCE(deadline,evidence_at+3600000),evidence_at+3600000) END AS INTEGER),
          CASE WHEN quota=1 AND reset_at>evidence_at THEN reset_at END
         FROM current_fence_origin WHERE true
         ON CONFLICT(kind,origin_key) DO UPDATE SET evidence=excluded.evidence,evidence_at=excluded.evidence_at,
          quota=excluded.quota,
          next_recheck_at=CASE WHEN excluded.evidence_at>=COALESCE(fence_recheck.last_claimed_at,-9223372036854775808)
            THEN excluded.next_recheck_at ELSE MAX(fence_recheck.next_recheck_at,excluded.next_recheck_at) END,
          reset_check_at=CASE WHEN excluded.reset_check_at>COALESCE(fence_recheck.last_claimed_at,-9223372036854775808) THEN excluded.reset_check_at END,
          classified=0,cleared_at=NULL,claim_owner=NULL WHERE fence_recheck.evidence!=excluded.evidence",
        [],
    )?;
    // Observation evidence is append-only and read through a bounded sparse
    // index. Its clock survives that lookback until a successful probe heals it.
    conn.execute("DELETE FROM fence_recheck WHERE kind!='observation' AND NOT EXISTS
        (SELECT 1 FROM current_fence_origin o WHERE o.kind=fence_recheck.kind AND o.origin_key=fence_recheck.origin_key)", [])?;
    // Older writers stored account-local errors on model/provider rows.
    // Snapshot reads ignore those rows as model/provider walls. Exclude their
    // retry clocks too, so one account's error cannot hide healthy siblings.
    for (kind, key) in quarantined_origins(conn)? {
        conn.execute(
            "DELETE FROM fence_recheck WHERE kind=?1 AND origin_key=?2",
            params![kind, key],
        )?;
    }
    Ok(())
}

fn quarantined_origins(conn: &Connection) -> rusqlite::Result<Vec<(String, String)>> {
    let mut quarantined = Vec::new();
    for (table, key, kind) in [
        ("model_route_state", "route_key", "route"),
        ("provider_cooldown_state", "provider_key", "provider"),
    ] {
        let mut statement = conn.prepare(&format!(
            "SELECT {key},last_error,wall_class FROM {table} WHERE wall_unscoped=0"
        ))?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
        for row in rows {
            let (key, error, class) = row?;
            let class =
                class.and_then(|class| prefrontal_routing_core::RouteWallClass::parse(&class).ok());
            if prefrontal_routing_core::account_wall::account_scoped_wall(error.as_deref(), class) {
                quarantined.push((kind.to_owned(), serde_json::json!([key]).to_string()));
            }
        }
    }
    Ok(quarantined)
}

pub(crate) fn read(conn: &Connection) -> rusqlite::Result<Vec<FenceRecheck>> {
    if !enabled(conn)? {
        return Ok(Vec::new());
    }
    let mut statement = conn.prepare("SELECT kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
        evidence_at,quota,next_recheck_at,reset_check_at,last_claimed_at,claim_owner,evidence FROM fence_recheck WHERE cleared_at IS NULL ORDER BY kind,origin_key")?;
    let rows = statement.query_map([], |row| {
        Ok(FenceRecheck {
            evidence: row.get(15)?,
            kind: row.get(0)?,
            origin_key: row.get(1)?,
            provider: row.get(2)?,
            account_id: row.get(3)?,
            credential_id: row.get(4)?,
            model: row.get(5)?,
            size_class: row.get(6)?,
            principal: row.get(7)?,
            admission_id: row.get(8)?,
            evidence_at: row.get(9)?,
            quota: row.get(10)?,
            next_recheck_at: row.get(11)?,
            reset_check_at: row.get(12)?,
            last_claimed_at: row.get(13)?,
            claim_owner: row.get(14)?,
            cleared_models: Vec::new(),
        })
    })?;
    let mut origins: Vec<FenceRecheck> = rows.collect::<rusqlite::Result<_>>()?;
    let mut statement=conn.prepare("SELECT c.kind,c.origin_key,c.model FROM fence_recheck_route_clear c JOIN fence_recheck r USING(kind,origin_key) WHERE c.evidence=r.evidence")?;
    for row in statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })? {
        let (kind, key, model) = row?;
        if let Some(origin) = origins
            .iter_mut()
            .find(|origin| origin.kind == kind && origin.origin_key == key)
        {
            origin.cleared_models.push(model);
        }
    }
    Ok(origins)
}

/// Overlay newly written evidence for scoring without opening a writer.
/// Admission materializes and claims their durable schedules atomically.
pub(crate) fn read_effective(conn: &Connection) -> rusqlite::Result<Vec<FenceRecheck>> {
    if !enabled(conn)? {
        return Ok(Vec::new());
    }
    let mut known: std::collections::BTreeMap<_, _> = read(conn)?
        .into_iter()
        .map(|origin| ((origin.kind.clone(), origin.origin_key.clone()), origin))
        .collect();
    let mut signatures = std::collections::BTreeMap::new();
    let mut statement = conn.prepare(
        "SELECT kind,origin_key,evidence,last_claimed_at,next_recheck_at FROM fence_recheck",
    )?;
    for row in statement.query_map([], |row| {
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            (
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, i64>(4)?,
            ),
        ))
    })? {
        let (key, value) = row?;
        signatures.insert(key, value);
    }
    let quarantined = quarantined_origins(conn)?;
    let mut effective = Vec::new();
    let mut statement=conn.prepare("SELECT kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,evidence,evidence_at,quota,deadline,reset_at FROM fence_origin")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let key = (row.get::<_, String>(0)?, row.get::<_, String>(1)?);
        if quarantined.contains(&key) {
            continue;
        }
        let evidence: String = row.get(9)?;
        let prior = signatures.get(&key);
        if prior.is_some_and(|(signature, _, _)| *signature == evidence) {
            if let Some(origin) = known.remove(&key) {
                effective.push(origin);
            }
            continue;
        }
        known.remove(&key);
        let evidence_at: i64 = row.get(10)?;
        let quota = row.get::<_, Option<bool>>(11)?.unwrap_or(false);
        let deadline = row.get::<_, Option<f64>>(12)?.map(|at| at as i64);
        let reset = row.get::<_, Option<f64>>(13)?.map(|at| at as i64);
        let last_claimed_at = prior.and_then(|(_, claimed, _)| *claimed);
        let mut next = if quota {
            evidence_at.saturating_add(QUOTA_FENCE_RECHECK_MS)
        } else {
            let cap = evidence_at.saturating_add(NON_QUOTA_FENCE_MAX_MS);
            deadline.unwrap_or(cap).min(cap)
        };
        if last_claimed_at.is_some_and(|claimed| evidence_at < claimed) {
            next = next.max(prior.unwrap().2);
        }
        effective.push(FenceRecheck {
            evidence,
            kind: key.0,
            origin_key: key.1,
            provider: row.get(2)?,
            account_id: row.get(3)?,
            credential_id: row.get(4)?,
            model: row.get(5)?,
            size_class: row.get(6)?,
            principal: row.get(7)?,
            admission_id: row.get(8)?,
            evidence_at,
            quota,
            next_recheck_at: next,
            reset_check_at: reset.filter(|at| {
                quota && *at > evidence_at && last_claimed_at.is_none_or(|claimed| *at > claimed)
            }),
            last_claimed_at,
            claim_owner: None,
            cleared_models: Vec::new(),
        });
    }
    effective.extend(
        known
            .into_values()
            .filter(|origin| origin.kind == "observation"),
    );
    Ok(effective)
}

pub(crate) fn claim(
    conn: &Connection,
    origins: &[FenceRecheck],
    now: i64,
) -> rusqlite::Result<bool> {
    claim_owned(conn, origins, now, None, None, None)
}

pub(crate) fn claim_with_authorization(
    conn: &Connection,
    origins: &[FenceRecheck],
    now: i64,
    owner: &str,
) -> rusqlite::Result<bool> {
    claim_owned(conn, origins, now, None, None, Some(owner))
}

fn claim_owned(
    conn: &Connection,
    origins: &[FenceRecheck],
    now: i64,
    consume_owner: Option<&str>,
    grant_owner: Option<&str>,
    authorization: Option<&str>,
) -> rusqlite::Result<bool> {
    sync(conn)?;
    // Check the entire conjunction before updating any clock. BEGIN IMMEDIATE
    // serializes distinct connections; a lost race consumes none of the clocks.
    let mut states = Vec::new();
    let mut quota_next = i64::MIN;
    let mut quota_evidence = i64::MIN;
    let mut quota_resets = Vec::new();
    for origin in origins {
        let state = conn
            .query_row(
                "SELECT quota,next_recheck_at,reset_check_at,claim_owner FROM fence_recheck
             WHERE kind=?1 AND origin_key=?2 AND evidence_at=?3 AND evidence=?4 AND cleared_at IS NULL",
                params![origin.kind, origin.origin_key, origin.evidence_at,origin.evidence],
                |row| {
                    Ok((
                        row.get::<_, bool>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((quota, next, reset, owner)) = state else {
            return Ok(false);
        };
        if quota != origin.quota {
            return Ok(false);
        }
        let owned = consume_owner.is_some() && owner.as_deref() == consume_owner;
        let due = reset.map_or(next, |reset| reset.min(next)) <= now;
        if !quota && !due && !owned {
            return Ok(false);
        }
        if quota && !owned {
            quota_next = quota_next.max(next);
            quota_evidence = quota_evidence.max(origin.evidence_at);
            quota_resets.extend(reset);
        }
        states.push((origin, owned, due));
    }
    // Quota representations of the same selected account share one periodic
    // check. A reset after its latest refusal adds a check for the whole
    // conjunction, so an individual credential's quota record on that account
    // cannot block the account-wide probe.
    if quota_next > now
        && !quota_resets
            .into_iter()
            .any(|reset| reset > quota_evidence && reset <= now)
    {
        return Ok(false);
    }
    for (origin, owned, due) in states {
        for owner in [authorization.or(grant_owner), consume_owner]
            .into_iter()
            .flatten()
        {
            conn.execute("INSERT INTO fence_probe_authorization(owner,kind,origin_key,evidence,evidence_at,claimed_at)
                VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(owner,kind,origin_key) DO UPDATE SET
                evidence=excluded.evidence,evidence_at=excluded.evidence_at,claimed_at=excluded.claimed_at",
                params![owner,origin.kind,origin.origin_key,origin.evidence,origin.evidence_at,now])?;
        }
        if owned && !due {
            conn.execute(
                "UPDATE fence_recheck SET claim_owner=NULL WHERE kind=?1 AND origin_key=?2",
                params![origin.kind, origin.origin_key],
            )?;
            continue;
        }
        let interval = if origin.quota {
            QUOTA_FENCE_RECHECK_MS
        } else {
            NON_QUOTA_FENCE_MAX_MS
        };
        conn.execute("UPDATE fence_recheck SET next_recheck_at=?3,last_claimed_at=?4,claim_owner=?5,
            reset_check_at=CASE WHEN reset_check_at>?4 THEN reset_check_at END WHERE kind=?1 AND origin_key=?2",
            params![origin.kind,origin.origin_key,now.saturating_add(interval),now,grant_owner])?;
    }
    Ok(true)
}

impl SqliteRouteStateStore {
    pub fn bind_fence_probe_owner(&self, from: &str, to: &str) -> RouteStateResult<()> {
        self.account_transaction(|tx| {
            tx.transfer_probe_authorization(from, to)?;
            tx.conn
                .execute(
                    "UPDATE fence_recheck SET claim_owner=?2 WHERE claim_owner=?1",
                    params![from, to],
                )
                .map(|_| ())
                .map_err(err)
        })
    }
    pub fn fence_rechecks(&self) -> RouteStateResult<Vec<FenceRecheck>> {
        // Scoring overlays newly written evidence without opening a writer.
        // Admission resyncs and claims the durable schedules atomically.
        self.store.with_read(read_effective).map_err(err)
    }
}

impl AccountTx<'_> {
    pub fn transfer_probe_authorization(&self, from: &str, to: &str) -> RouteStateResult<()> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        self.conn.execute("INSERT INTO fence_probe_authorization SELECT ?2,kind,origin_key,evidence,evidence_at,claimed_at
            FROM fence_probe_authorization WHERE owner=?1
            ON CONFLICT(owner,kind,origin_key) DO UPDATE SET evidence=excluded.evidence,
            evidence_at=excluded.evidence_at,claimed_at=excluded.claimed_at",params![from,to]).map_err(err)?;
        self.conn
            .execute(
                "DELETE FROM fence_probe_authorization WHERE owner=?1 AND owner!=?2",
                params![from, to],
            )
            .map_err(err)?;
        Ok(())
    }

    /// A neutral terminal outcome is not evidence of recovery or refusal. Let
    /// another request test the same non-quota generation, without undoing a
    /// newer launch's claim or shortening the quota probe interval.
    pub fn release_inconclusive_probe(
        &self,
        owner: &str,
        provider: &str,
        model: &str,
        now: i64,
    ) -> RouteStateResult<()> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        sync(self.conn).map_err(err)?;
        self.conn.execute("UPDATE fence_recheck AS r SET next_recheck_at=?4,claim_owner=NULL
            WHERE r.quota=0 AND r.cleared_at IS NULL AND r.provider=?2 AND (r.model IS NULL OR r.model=?3)
            AND EXISTS (SELECT 1 FROM fence_probe_authorization a WHERE a.owner=?1
                AND a.kind=r.kind AND a.origin_key=r.origin_key AND a.evidence=r.evidence
                AND a.evidence_at=r.evidence_at AND a.claimed_at=r.last_claimed_at)",params![owner,provider,model,now]).map_err(err)?;
        self.conn
            .execute(
                "DELETE FROM fence_probe_authorization WHERE owner=?1",
                [owner],
            )
            .map_err(err)?;
        Ok(())
    }

    /// The durable launch authorization, not a reported credential or guessed
    /// pool, is the scope of a successful probe. New evidence never matches it.
    pub fn heal_authorized_probe(
        &self,
        owner: &str,
        provider: &str,
        model: &str,
        now: i64,
    ) -> RouteStateResult<()> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        let origins = self.fence_rechecks()?;
        for origin in origins.iter().filter(|origin| {
            origin.provider == provider
                && origin.model.as_deref().is_none_or(|value| value == model)
        }) {
            let claimed: Option<i64> = self
                .conn
                .query_row(
                    "SELECT claimed_at FROM fence_probe_authorization
                WHERE owner=?1 AND kind=?2 AND origin_key=?3 AND evidence=?4 AND evidence_at=?5",
                    params![
                        owner,
                        origin.kind,
                        origin.origin_key,
                        origin.evidence,
                        origin.evidence_at
                    ],
                    |row| row.get(0),
                )
                .optional()
                .map_err(err)?;
            if let Some(at) = claimed.filter(|at| origin.evidence_at < *at) {
                self.heal_origins(std::slice::from_ref(origin), model, at, now, None, true)?;
            }
        }
        self.conn
            .execute(
                "DELETE FROM fence_probe_authorization WHERE owner=?1",
                [owner],
            )
            .map_err(err)?;
        Ok(())
    }
    pub(crate) fn successful_quota_pools(
        &self,
        id: &str,
        observed: i64,
    ) -> RouteStateResult<Vec<prefrontal_routing_core::accounts::PoolKey>> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(Vec::new());
        }
        let mut statement = self
            .conn
            .prepare(
                "SELECT pool_key_json FROM fence_success_pool WHERE fence_id=?1 AND evidence_at=?2",
            )
            .map_err(err)?;
        let rows = statement
            .query_map(params![id, observed], |row| row.get::<_, String>(0))
            .map_err(err)?;
        rows.map(|raw| serde_json::from_str(&raw.map_err(err)?).map_err(err))
            .collect()
    }
    pub fn claim_fence_rechecks_for_admission(
        &self,
        origins: &[FenceRecheck],
        now: i64,
        owner: Option<&str>,
    ) -> RouteStateResult<bool> {
        claim_owned(self.conn, origins, now, owner, None, owner).map_err(err)
    }
    pub fn claim_fence_rechecks_for_authorized_admission(
        &self,
        origins: &[FenceRecheck],
        now: i64,
        owner: Option<&str>,
        admission: &str,
    ) -> RouteStateResult<bool> {
        claim_owned(self.conn, origins, now, owner, None, Some(admission)).map_err(err)
    }
    pub fn grant_fence_rechecks(
        &self,
        origins: &[FenceRecheck],
        now: i64,
        owner: Option<&str>,
    ) -> RouteStateResult<bool> {
        claim_owned(self.conn, origins, now, None, owner, owner).map_err(err)
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

    fn heal_origins(
        &self,
        origins: &[FenceRecheck],
        model: &str,
        admitted_at: i64,
        now: i64,
        scope: Option<&QuotaSuccessScope>,
        authorized: bool,
    ) -> RouteStateResult<()> {
        for origin in origins {
            let provider = origin.provider.as_str();
            let credential = origin.credential_id.as_deref().unwrap_or("");
            let key: Vec<serde_json::Value> =
                serde_json::from_str(&origin.origin_key).map_err(err)?;
            match origin.kind.as_str() {
                "account" => {
                    if authorized {
                        self.conn.execute("UPDATE account_fence SET cleared_at=?2,cleared_reason='probe_succeeded'
                            WHERE fence_id=?1 AND MIN(observed_at,created_at)<?3",params![key[0].as_str(),now,admitted_at]).map_err(err)?;
                        self.conn.execute("UPDATE fence_recheck SET cleared_at=?3 WHERE kind=?1 AND origin_key=?2",params![origin.kind,origin.origin_key,now]).map_err(err)?;
                        continue;
                    }
                    let Some(scope) = scope else {
                        continue;
                    };
                    let id = key[0].as_str().unwrap_or("");
                    let (pool,needed):(Option<String>,Option<String>)=self.conn.query_row("SELECT pool_key_json,recovery_set_json FROM account_fence WHERE fence_id=?1",[id],|row|Ok((row.get(0)?,row.get(1)?))).map_err(err)?;
                    let needed: Vec<prefrontal_routing_core::accounts::PoolKey> = match pool {
                        Some(pool) => vec![serde_json::from_str(&pool).map_err(err)?],
                        None => needed
                            .map(|raw| serde_json::from_str(&raw).map_err(err))
                            .transpose()?
                            .unwrap_or_default(),
                    };
                    if needed.is_empty() {
                        continue;
                    }
                    for pool in needed.iter().filter(|pool| scope.used(pool, model)) {
                        self.conn.execute("INSERT INTO fence_success_pool VALUES (?1,?2,?3,?4)
                            ON CONFLICT(fence_id,pool_key_json) DO UPDATE SET evidence_at=excluded.evidence_at,healed_at=excluded.healed_at",
                            params![id,serde_json::to_string(pool).map_err(err)?,origin.evidence_at,now]).map_err(err)?;
                    }
                    let healed = self.successful_quota_pools(id, origin.evidence_at)?;
                    if !needed.iter().all(|pool| healed.contains(pool)) {
                        continue;
                    }
                    self.conn.execute("UPDATE account_fence SET cleared_at=?2,cleared_reason='run_succeeded' WHERE fence_id=?1 AND MIN(observed_at,created_at)<?3",params![key[0].as_str(),now,admitted_at]).map_err(err)?;
                }
                "credential" => {
                    if origin.quota {
                        let prefix = format!(
                            "credential-quota:{}:",
                            serde_json::json!([provider, credential])
                        );
                        if self
                            .active_pool_fences(provider)?
                            .iter()
                            .any(|fence| fence.evidence.fence_id.starts_with(&prefix))
                        {
                            continue;
                        }
                    }
                    self.conn.execute("DELETE FROM credential_entitlement_fences WHERE provider_id=?1 AND credential_id=?2 AND observed_at_ms<?3",params![provider,credential,admitted_at]).map_err(err)?;
                }
                "account_route" | "non_quota_route" | "transient_route" => {
                    let table = if origin.kind == "account_route" {
                        "account_route_fence"
                    } else {
                        "account_non_quota_route_fence"
                    };
                    if origin.kind == "account_route" {
                        self.conn.execute(&format!("DELETE FROM {table} WHERE provider=?1 AND credential_id=?2 AND model=?3 AND COALESCE(last_reported_at,recorded_at)<?4"),params![provider,credential,model,admitted_at]).map_err(err)?;
                    } else {
                        let kind = if origin.kind == "transient_route" {
                            "transient"
                        } else {
                            "billing"
                        };
                        self.conn.execute(&format!("DELETE FROM {table} WHERE provider=?1 AND credential_id=?2 AND model=?3 AND last_reported_at<?4 AND wall_kind=?5"),params![provider,credential,model,admitted_at,kind]).map_err(err)?;
                    }
                }
                "size" | "non_quota_size" => {
                    let table = if origin.kind == "size" {
                        "account_size_refusal"
                    } else {
                        "account_non_quota_size_refusal"
                    };
                    self.conn.execute(&format!("DELETE FROM {table} WHERE provider=?1 AND credential_id=?2 AND model=?3 AND size_class=?4 AND observed_at<?5"),params![provider,credential,model,origin.size_class,admitted_at]).map_err(err)?;
                }
                "pool_billing" | "pool_rate" => {
                    let field = if origin.kind == "pool_billing" {
                        "walled_until"
                    } else {
                        "cooldown_until"
                    };
                    self.conn.execute(&format!("UPDATE pool_key_state SET {field}=NULL WHERE provider=?1 AND credential_id=?2"),params![provider,credential]).map_err(err)?;
                }
                "route" => {
                    self.conn.execute("UPDATE model_route_state SET cooldown_until=NULL,quota_exhausted_until=NULL WHERE route_key=?1",[key[0].as_str()]).map_err(err)?;
                }
                "provider" => {
                    // Only the rows stamped by this provider evidence inherit
                    // its recovery. A separate model refusal has its own clock.
                    self.conn.execute("UPDATE model_route_state SET cooldown_until=NULL,quota_exhausted_until=NULL
                        WHERE provider_id=?1 AND COALESCE(wall_observed_at,last_error_at,updated_at)=?2
                        AND COALESCE(wall_observed_at,last_error_at,updated_at)<?3
                        AND last_error IS (SELECT last_error FROM provider_cooldown_state WHERE provider_key=?1)
                        AND COALESCE(cooldown_until,0)<=(SELECT COALESCE(cooldown_until,0) FROM provider_cooldown_state WHERE provider_key=?1)
                        AND COALESCE(quota_exhausted_until,0)<=(SELECT COALESCE(quota_exhausted_until,0) FROM provider_cooldown_state WHERE provider_key=?1)",params![provider,origin.evidence_at,admitted_at]).map_err(err)?;
                    self.conn.execute("UPDATE provider_cooldown_state SET cooldown_until=NULL,quota_exhausted_until=NULL WHERE provider_key=?1",[provider]).map_err(err)?;
                }
                "failure" => {
                    self.conn.execute("UPDATE model_route_state SET failure_backoff_until=NULL WHERE route_key=?1",[key[0].as_str()]).map_err(err)?;
                }
                _ => {}
            }
            self.conn
                .execute(
                    "UPDATE fence_recheck SET cleared_at=?3 WHERE kind=?1 AND origin_key=?2",
                    params![origin.kind, origin.origin_key, now],
                )
                .map_err(err)?;
        }
        Ok(())
    }
    pub fn classify_credential_quota(
        &self,
        provider: &str,
        credential: &str,
        quota: bool,
    ) -> RouteStateResult<bool> {
        if !enabled(self.conn).map_err(err)? {
            return Ok(quota);
        }
        sync(self.conn).map_err(err)?;
        self.conn.execute("UPDATE fence_recheck SET quota=?3,classified=1,next_recheck_at=MIN(next_recheck_at,evidence_at+CASE WHEN ?3 THEN 900000 ELSE 3600000 END)
            WHERE kind='credential' AND provider=?1 AND credential_id=?2 AND classified=0",params![provider,credential,quota]).map_err(err)?;
        self.conn.query_row("SELECT quota FROM fence_recheck WHERE kind='credential' AND provider=?1 AND credential_id=?2",params![provider,credential],|row|row.get(0)).map_err(err)
    }
    pub fn fence_rechecks(&self) -> RouteStateResult<Vec<FenceRecheck>> {
        read_effective(self.conn).map_err(err)
    }

    pub fn claim_fence_rechecks(
        &self,
        origins: &[FenceRecheck],
        now: i64,
    ) -> RouteStateResult<bool> {
        claim(self.conn, origins, now).map_err(err)
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
        let named = prefrontal_routing_core::accounts::PoolKey {
            name: Some("P".into()),
            scope: prefrontal_routing_core::accounts::PoolScope::EveryModel,
            window_minutes: Some(300),
        };
        store.store.with_conn(|conn| {
            for (id,pool) in [("empty",None),("named",Some(&named)),("unclaimed",Some(&named))] {
                conn.execute("INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at,pool_key_json,recovery_set_json)
                    VALUES (?1,'p','a','{}','{\"reset\":{\"kind\":\"missing\"}}','broca_account_limit',100,'test',100,?2,'[]')",
                    params![id,pool.map(|pool|serde_json::to_string(pool).unwrap())])?;
            }
            Ok(())
        }).unwrap();
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
    fn pool_probe_authorization_survives_renewal_and_moves_to_the_serving_decision() {
        use prefrontal_routing_core::key_pool::PoolKey;
        let scratch = cortexkit_test_support::ScratchDir::new("pool-success-authorization");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "pool-success-authorization",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .pool_wall(
                "p",
                "apikey:p",
                crate::PoolWallKind::Billing,
                1000,
                "credits",
                100,
            )
            .unwrap();
        let keys = vec![PoolKey::new("apikey:p", Some("a"), "active")];
        let crate::PoolAcquireOutcome::Leased(lease) = store
            .pool_acquire_for_route(
                &crate::PoolAcquire {
                    provider: "p",
                    session_id: "session",
                    keys: &keys,
                    now_ms: 1000,
                    lease_ms: 120_000,
                    affinity_idle_ms: 120_000,
                },
                &["p"],
                "m",
                &[],
                &Default::default(),
            )
            .unwrap()
        else {
            panic!("due pool probe must lease")
        };
        assert!(store
            .pool_renew(
                "p",
                "session",
                &lease.lease_id,
                Some("apikey:p"),
                1001,
                120_000
            )
            .unwrap()
            .is_some());
        assert!(
            !store.fence_rechecks().unwrap().is_empty(),
            "renewal is not success"
        );
        store
            .bind_fence_probe_owner(&lease.lease_id, "decision")
            .unwrap();
        store
            .pool_release("p", "session", Some(&lease.lease_id), true, 1002)
            .unwrap();
        assert!(
            !store.fence_rechecks().unwrap().is_empty(),
            "release is not success either"
        );
        store
            .account_transaction(|tx| tx.heal_authorized_probe("decision", "p", "m", 1003))
            .unwrap();
        assert!(
            store.fence_rechecks().unwrap().is_empty(),
            "a linked typed success heals the claimed pool wall"
        );
    }

    #[test]
    fn linked_quota_reset_opens_the_whole_conjunction_but_not_after_a_newer_refusal() {
        let scratch = cortexkit_test_support::ScratchDir::new("quota-conjunction");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "quota-conjunction",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store.store.with_conn(|conn|conn.execute("INSERT INTO credential_entitlement_fences VALUES ('p','c','billing_exhausted',NULL,0)",[])).unwrap();
        store
            .account_transaction(|tx| tx.classify_credential_quota("p", "c", true))
            .unwrap();
        let fence = crate::AccountRouteFence {
            provider: "p".into(),
            credential_id: "c".into(),
            model: "m".into(),
            until_ms: 60_000,
            decision_id: "d".into(),
            recorded_at: 0,
        };
        store.record_account_route_fence(&fence).unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 59_999))
            .unwrap());
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 60_000))
            .unwrap());
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 60_000))
            .unwrap());
        assert!(store
            .fence_rechecks()
            .unwrap()
            .iter()
            .all(|origin| origin.next_recheck_at == 960_000));
        store
            .record_account_route_fence(&crate::AccountRouteFence {
                recorded_at: 70_000,
                until_ms: 60_000,
                ..fence
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(
            !store
                .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 70_000))
                .unwrap(),
            "a reset before the newer refusal is not another extra check"
        );
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 970_000))
            .unwrap());
    }

    #[test]
    fn inconclusive_release_preserves_quota_interval_and_a_newer_claim() {
        let scratch = cortexkit_test_support::ScratchDir::new("neutral-claim-generation");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "neutral-claim-generation",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .record_billing_account_route_fence(&crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "c".into(),
                model: "m".into(),
                until_ms: 1000,
                recorded_at: 100,
                decision_id: "d".into(),
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, 1000, Some("old")))
            .unwrap());
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, 3_601_000, Some("new")))
            .unwrap());
        store
            .account_transaction(|tx| tx.release_inconclusive_probe("old", "p", "m", 3_601_001))
            .unwrap();
        assert_eq!(
            store.fence_rechecks().unwrap()[0].next_recheck_at,
            7_201_000,
            "an older cancellation cannot undo the new launch's atomic claim"
        );
        store
            .record_account_route_fence(&crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "c".into(),
                model: "m".into(),
                until_ms: 0,
                recorded_at: 4_000_000,
                decision_id: "quota".into(),
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &origins,
                8_000_000,
                Some("quota-probe")
            ))
            .unwrap());
        store
            .account_transaction(|tx| {
                tx.release_inconclusive_probe("quota-probe", "p", "m", 8_000_001)
            })
            .unwrap();
        let quota = store
            .fence_rechecks()
            .unwrap()
            .into_iter()
            .find(|f| f.quota)
            .unwrap();
        assert_eq!(
            quota.next_recheck_at, 8_900_000,
            "neutral non-quota recovery never widens the quota probe budget"
        );
    }

    #[test]
    fn admission_sync_uses_linear_origin_membership_not_a_correlated_view_scan() {
        use std::sync::atomic::{AtomicU64, Ordering};
        unsafe extern "C" fn count(pointer: *mut std::ffi::c_void) -> std::ffi::c_int {
            // SAFETY: the counter lives until the connection's callback is removed.
            unsafe { &*(pointer as *const AtomicU64) }.fetch_add(1000, Ordering::Relaxed);
            0
        }
        let scratch = cortexkit_test_support::ScratchDir::new("linear-admission-sync");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "linear-admission-sync",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store.store.with_conn(|conn| {
            conn.execute_batch("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000)
                INSERT INTO account_non_quota_route_fence SELECT 'p','c'||i,'m',8640000000000000,'d',100,100,'billing' FROM n;")?;
            sync(conn)?;
            let steps = AtomicU64::new(0);
            // SAFETY: the store mutex owns this live handle, and the callback
            // touches only the counter, never SQLite or the surrounding store.
            unsafe { rusqlite::ffi::sqlite3_progress_handler(conn.handle(),1000,Some(count),&steps as *const _ as *mut std::ffi::c_void); }
            let started = std::time::Instant::now();
            let result = sync(conn);
            let elapsed = started.elapsed();
            // SAFETY: remove the callback before its stack counter goes away.
            unsafe { rusqlite::ffi::sqlite3_progress_handler(conn.handle(),0,None,std::ptr::null_mut()); }
            result?;
            let steps = steps.load(Ordering::Relaxed);
            eprintln!("admission_sync origins=10000 steps={steps} elapsed_us={}",elapsed.as_micros());
            assert!(steps<40_000_000,"origin membership must be indexed, not quadratic: {steps} VM steps");
            Ok(())
        }).unwrap();
    }

    #[test]
    fn a_delayed_selected_probe_renews_its_expired_claim_before_admission() {
        let scratch = cortexkit_test_support::ScratchDir::new("late-probe-transfer");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "late-probe-transfer",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .record_account_route_fence(&crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "c".into(),
                model: "m".into(),
                until_ms: 0,
                decision_id: "old".into(),
                recorded_at: 0,
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(&origins, 900_000, Some("d")))
            .unwrap());
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks_for_admission(
                &origins,
                1_800_001,
                Some("d")
            ))
            .unwrap());
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 1_800_001))
            .unwrap());
        assert_eq!(
            store.fence_rechecks().unwrap()[0].next_recheck_at,
            2_700_001
        );
    }

    #[test]
    fn a_repeat_transient_observation_replaces_the_claimed_origins_short_retry() {
        let scratch = cortexkit_test_support::ScratchDir::new("transient-retry");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "transient-retry",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let observe = |id: &str, at, until| {
            store.account_transaction(|tx|tx.insert_observation(&crate::AccountObservationRow {
            observation_id:id.into(),principal:"test".into(),runner:"broca".into(),event_id:id.into(),kind:"transient_backoff".into(),admission_id:None,bound:true,
            observation_json:"{}".into(),effects_json:serde_json::json!([{"effect":"transient_backoff","provider":"p","account_id":"a","retry_at_ms":until}]).to_string(),received_at:at,
        })).unwrap()
        };
        observe("first", 0, 10_000);
        let origins = store.fence_rechecks().unwrap();
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 10_000))
            .unwrap());
        observe("repeat", 10_001, 20_000);
        let origins = store.fence_rechecks().unwrap();
        assert_eq!(origins.len(), 1);
        assert_eq!(origins[0].due_at(), 20_000);
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 20_000))
            .unwrap());
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks(&origins, 20_000))
            .unwrap());
    }

    #[test]
    fn a_second_pool_acquire_sees_the_fence_without_revoking_the_probe_holders_lease() {
        use prefrontal_routing_core::key_pool::PoolKey;
        let scratch = cortexkit_test_support::ScratchDir::new("pool-probe-renew");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "pool-probe-renew",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        let keys = vec![PoolKey::new("apikey:p", Some("a"), "active")];
        store
            .pool_wall(
                "p",
                "apikey:p",
                crate::PoolWallKind::Billing,
                10_000,
                "empty",
                0,
            )
            .unwrap();
        let request = crate::PoolAcquire {
            provider: "p",
            session_id: "session",
            keys: &keys,
            now_ms: 10_000,
            lease_ms: 120_000,
            affinity_idle_ms: 120_000,
        };
        let crate::PoolAcquireOutcome::Leased(lease) = store.pool_acquire(&request).unwrap() else {
            panic!("first probe must lease");
        };
        let crate::PoolAcquireOutcome::Exhausted(blocked) = store.pool_acquire(&request).unwrap()
        else {
            panic!("a second acquire is a second send authorization");
        };
        assert_eq!(blocked[0].reset_at_ms, Some(3_610_000));
        assert_eq!(
            store
                .pool_renew(
                    "p",
                    "session",
                    &lease.lease_id,
                    Some("apikey:p"),
                    10_001,
                    120_000
                )
                .unwrap(),
            Some(130_001)
        );
        assert_eq!(
            store
                .pool_renew(
                    "p",
                    "session",
                    "unknown-lease",
                    Some("apikey:p"),
                    10_001,
                    120_000
                )
                .unwrap(),
            None
        );
    }

    #[test]
    fn success_heals_only_the_known_pool_and_family_and_retains_other_evidence() {
        use prefrontal_routing_core::accounts::{ModelFamily, PoolKey, PoolScope};
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
        store.store.with_conn(|conn| {
            for (id,pool,recovery) in [("gemini",Some(&gemini),vec![]),("claude",Some(&claude),vec![]),("whole",None,vec![gemini.clone(),claude.clone()])] {
                conn.execute("INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at,pool_key_json,recovery_set_json)
                    VALUES (?1,'p','a','{}','{\"reset\":{\"kind\":\"missing\"}}','broca_account_limit',100,'test',100,?2,?3)",
                    params![id,pool.map(|pool|serde_json::to_string(pool).unwrap()),serde_json::to_string(&recovery).unwrap()])?;
            }
            Ok(())
        }).unwrap();
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
    fn selected_probe_authorization_transfers_once_without_a_second_interval() {
        let scratch = cortexkit_test_support::ScratchDir::new("fence-probe-transfer");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "fence-probe-transfer",
            scratch.path().join("store.db").to_string_lossy(),
        ))
        .unwrap();
        store
            .record_account_route_fence(&crate::AccountRouteFence {
                provider: "p".into(),
                credential_id: "c".into(),
                model: "m".into(),
                until_ms: 0,
                decision_id: "old".into(),
                recorded_at: 0,
            })
            .unwrap();
        let origins = store.fence_rechecks().unwrap();
        let now = QUOTA_FENCE_RECHECK_MS;
        assert!(store
            .account_transaction(|tx| tx.grant_fence_rechecks(
                &origins,
                now,
                Some("selection-owner")
            ))
            .unwrap());
        store
            .bind_fence_probe_owner("selection-owner", "model-decision")
            .unwrap();
        let claimed = store.fence_rechecks().unwrap();
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks_for_admission(
                &claimed,
                now + 1,
                Some("other-decision")
            ))
            .unwrap());
        assert!(store
            .account_transaction(|tx| tx.claim_fence_rechecks_for_admission(
                &claimed,
                now + 1,
                Some("model-decision")
            ))
            .unwrap());
        assert!(!store
            .account_transaction(|tx| tx.claim_fence_rechecks_for_admission(
                &claimed,
                now + 1,
                Some("model-decision")
            ))
            .unwrap());
        let after = store.fence_rechecks().unwrap();
        assert_eq!(after[0].next_recheck_at, now + QUOTA_FENCE_RECHECK_MS);
        assert_eq!(after[0].evidence_at, 0);
        assert_eq!(after[0].last_claimed_at, Some(now));
        assert_eq!(after[0].claim_owner, None);
    }

    #[test]
    fn every_non_quota_origin_caps_at_one_hour_preserves_shorter_retry_and_claims_once() {
        for duration in [10_000, 86_400_000] {
            let scratch = cortexkit_test_support::ScratchDir::new("all-fence-caps");
            let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
                "all-fence-caps",
                scratch.path().join("store.db").to_string_lossy(),
            ))
            .unwrap();
            let now = crate::now_ms();
            let until = now + duration;
            store.store.with_conn(|conn|conn.execute_batch(&format!(
                "INSERT INTO credential_entitlement_fences VALUES ('p','c','ENTITLEMENT_ERROR',NULL,{now});
                 INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,updated_at,failure_backoff_until,failure_backoff_reason,failure_observed_at)
                   VALUES ('failure/m','failure/m','failure','m',{now},{until},'failure',{now});"))).unwrap();
            for kind in [
                crate::PoolWallKind::Billing,
                crate::PoolWallKind::RateLimited,
            ] {
                store
                    .pool_wall("pool", "key", kind, until, "refused", now)
                    .unwrap();
            }
            store
                .account_transaction(|tx| {
                    tx.insert_exclusion_with_recheck(
                        &crate::AccountExclusionRow {
                            principal: "test".into(),
                            admission_id: "d".into(),
                            credential_id: "c".into(),
                            runner_view_digest: "runner".into(),
                            roster_view_digest: "roster".into(),
                            observation_id: None,
                            recorded_at: now,
                        },
                        duration,
                    )?;
                    tx.record_non_quota_size_refusal(
                        &crate::AccountSizeRefusal {
                            provider: "p".into(),
                            credential_id: "c".into(),
                            model: "m".into(),
                            size_class: 17,
                            expires_at: until,
                            observed_at: now,
                        },
                        now,
                    )
                })
                .unwrap();
            store
                .record_transient_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: "c".into(),
                    model: "m".into(),
                    until_ms: until,
                    decision_id: "d".into(),
                    recorded_at: now,
                })
                .unwrap();
            store
                .record_billing_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: "c".into(),
                    model: "m".into(),
                    until_ms: until,
                    decision_id: "d".into(),
                    recorded_at: now,
                })
                .unwrap();
            use prefrontal_routing_core::{RouteStateStore, RouteStateUpdate, RouteWallClass};
            let update = RouteStateUpdate {
                cooldown_until: Some(until as f64),
                wall_class: Some(RouteWallClass::Quota),
                ..Default::default()
            };
            // Even a typed quota label cannot supply the missing identity on a
            // legacy route/provider cooldown.
            store
                .update_route_state("route/m", "route/m", &update)
                .unwrap();
            store
                .record_provider_cooldown("provider", "provider/m", "provider/m", &update)
                .unwrap();
            store.account_transaction(|tx|tx.insert_observation(&crate::AccountObservationRow {
                observation_id:"o".into(),principal:"test".into(),runner:"broca".into(),event_id:"e".into(),kind:"transient_backoff".into(),
                admission_id:None,bound:true,observation_json:"{}".into(),received_at:now,
                effects_json:serde_json::json!([{"effect":"transient_backoff","provider":"p","account_id":"a","retry_at_ms":until}]).to_string(),
            })).unwrap();
            let clocks = store.fence_rechecks().unwrap();
            let actual: std::collections::BTreeSet<_> =
                clocks.iter().map(|clock| clock.kind.as_str()).collect();
            let expected: std::collections::BTreeSet<_> = [
                "credential",
                "principal",
                "pool_billing",
                "pool_rate",
                "non_quota_route",
                "transient_route",
                "non_quota_size",
                "route",
                "provider",
                "failure",
                "observation",
            ]
            .into_iter()
            .collect();
            assert_eq!(
                actual, expected,
                "every temporal non-quota kind must be scheduled"
            );
            for clock in &clocks {
                assert!(!clock.quota, "{}", clock.kind);
                assert!(
                    clock.next_recheck_at <= clock.evidence_at + 3_600_000,
                    "{}",
                    clock.kind
                );
                if duration == 10_000 && clock.kind != "credential" {
                    assert_eq!(clock.next_recheck_at, until, "{}", clock.kind);
                }
                assert!(
                    store
                        .account_transaction(|tx| tx
                            .claim_fence_rechecks(std::slice::from_ref(clock), clock.due_at()))
                        .unwrap(),
                    "{}",
                    clock.kind
                );
                assert!(
                    !store
                        .account_transaction(|tx| tx
                            .claim_fence_rechecks(std::slice::from_ref(clock), clock.due_at()))
                        .unwrap(),
                    "{}",
                    clock.kind
                );
            }
        }
    }

    #[test]
    fn direct_pool_acquire_admits_exactly_one_concurrent_probe() {
        let scratch = cortexkit_test_support::ScratchDir::new("pool-probe-concurrency");
        let store = std::sync::Arc::new(
            SqliteRouteStateStore::open(&crate::dev_descriptor(
                "pool-probe-concurrency",
                scratch.path().join("store.db").to_string_lossy(),
            ))
            .unwrap(),
        );
        store
            .pool_wall(
                "p",
                "apikey:p",
                crate::PoolWallKind::Billing,
                86_400_000,
                "empty",
                0,
            )
            .unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let keys = vec![prefrontal_routing_core::key_pool::PoolKey::new(
                        "apikey:p",
                        Some("a"),
                        "active",
                    )];
                    barrier.wait();
                    store
                        .pool_acquire(&crate::PoolAcquire {
                            provider: "p",
                            session_id: &format!("session-{i}"),
                            keys: &keys,
                            now_ms: NON_QUOTA_FENCE_MAX_MS,
                            lease_ms: 120_000,
                            affinity_idle_ms: 120_000,
                        })
                        .unwrap()
                })
            })
            .collect();
        assert_eq!(
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .filter(|outcome| matches!(outcome, crate::PoolAcquireOutcome::Leased(_)))
                .count(),
            1
        );
        store
            .pool_wall(
                "p",
                "apikey:p",
                crate::PoolWallKind::Billing,
                NON_QUOTA_FENCE_MAX_MS + 10_000,
                "still empty",
                NON_QUOTA_FENCE_MAX_MS + 1,
            )
            .unwrap();
        let clocks = store.fence_rechecks().unwrap();
        assert_eq!(
            clocks[0].due_at(),
            NON_QUOTA_FENCE_MAX_MS + 10_000,
            "a repeat refusal preserves a shorter retry"
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

    #[test]
    fn quota_rechecks_every_fifteen_minutes_and_reset_only_adds_a_check() {
        for reset in [0, 60_000, 2 * QUOTA_FENCE_RECHECK_MS] {
            let scratch = cortexkit_test_support::ScratchDir::new("quota-recheck");
            let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
                "quota-recheck",
                scratch.path().join("store.db").to_string_lossy(),
            ))
            .unwrap();
            store
                .record_account_route_fence(&crate::AccountRouteFence {
                    provider: "p".into(),
                    credential_id: "c".into(),
                    model: "m".into(),
                    until_ms: reset,
                    decision_id: "d".into(),
                    recorded_at: 0,
                })
                .unwrap();
            let clocks = store.fence_rechecks().unwrap();
            assert_eq!(clocks[0].next_recheck_at, QUOTA_FENCE_RECHECK_MS);
            let first = if reset == 60_000 {
                reset
            } else {
                QUOTA_FENCE_RECHECK_MS
            };
            assert!(!store
                .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, first - 1))
                .unwrap());
            assert!(store
                .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, first))
                .unwrap());
            assert!(!store
                .account_transaction(|tx| tx.claim_fence_rechecks(&clocks, first))
                .unwrap());
            assert_eq!(
                store
                    .account_route_fences(100 * QUOTA_FENCE_RECHECK_MS)
                    .unwrap()
                    .len(),
                1,
                "a date never expires quota evidence"
            );
            let updated = store.fence_rechecks().unwrap();
            assert_eq!(updated[0].next_recheck_at, first + QUOTA_FENCE_RECHECK_MS);
            assert_eq!(updated[0].evidence_at, 0);
            if reset > first {
                assert_eq!(updated[0].due_at(), reset);
            } else {
                assert_eq!(updated[0].reset_check_at, None);
            }
        }
    }

    #[test]
    fn migration_82_converts_quota_now_and_caps_non_quota_once() {
        let scratch = cortexkit_test_support::ScratchDir::new("fence-migration");
        let descriptor = crate::dev_descriptor(
            "fence-migration",
            scratch.path().join("store.db").to_string_lossy(),
        );
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 81).unwrap();
        let now = crate::now_ms();
        old.store.with_conn(|conn| conn.execute_batch(&format!(
            "INSERT INTO account_route_fence VALUES ('p','c','m',{},'d',{}, {},'quota');
             INSERT INTO credential_entitlement_fences VALUES ('p','c','ENTITLEMENT_ERROR',NULL,{});
             INSERT INTO pool_key_state(provider,credential_id,walled_until,cooldown_until,updated_at)
               VALUES ('p','key',{},{},{});",
            now+99_000_000,now-100,now-100,now-100,now+99_000_000,now+10_000,now-100))).unwrap();
        drop(old);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        let clocks = store.fence_rechecks().unwrap();
        let quota = clocks
            .iter()
            .find(|clock| clock.kind == "account_route")
            .unwrap();
        assert!(quota.next_recheck_at <= now && quota.next_recheck_at >= now - 1000);
        let billing = clocks
            .iter()
            .find(|clock| clock.kind == "pool_billing")
            .unwrap();
        assert!(billing.next_recheck_at <= now + NON_QUOTA_FENCE_MAX_MS);
        assert!(billing.next_recheck_at >= now + NON_QUOTA_FENCE_MAX_MS - 1000);
        let short = clocks
            .iter()
            .find(|clock| clock.kind == "pool_rate")
            .unwrap();
        assert_eq!(short.next_recheck_at, now + 10_000);
        let first: Vec<_> = clocks
            .iter()
            .map(|clock| (clock.kind.clone(), clock.next_recheck_at))
            .collect();
        drop(store);
        let store = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert_eq!(
            first,
            store
                .fence_rechecks()
                .unwrap()
                .iter()
                .map(|clock| (clock.kind.clone(), clock.next_recheck_at))
                .collect::<Vec<_>>()
        );
        drop(store);
        let error = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 81)
            .err()
            .unwrap();
        assert!(error.to_string().contains("store_ahead"));
        assert!(error.to_string().contains("82"));
    }

    #[test]
    fn strict_probe_claim_is_atomic_across_distinct_connections() {
        for case in ["credential", "account", "combined"] {
            let scratch = cortexkit_test_support::ScratchDir::new("fence-concurrency");
            let path = scratch.path().join("store.db");
            let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
                "fence-concurrency",
                path.to_string_lossy(),
            ))
            .unwrap();
            store.store.with_conn(|conn| {
            if case!="account" {conn.execute("INSERT INTO credential_entitlement_fences VALUES ('p','c','ENTITLEMENT_ERROR',NULL,0)",[])?;}
            if case!="credential" {conn.execute("INSERT INTO account_fence(fence_id,provider,account_id,scope_json,fence_json,source,observed_at,principal,created_at)
                VALUES ('f','p','a','null','{\"reset\":{\"kind\":\"missing\"}}','broca_account_limit',0,'test',0)",[])?;}
            if case=="combined" {conn.execute("INSERT INTO account_route_fence VALUES ('p','c','m',0,'d',0,0,'quota')",[])?;}
            Ok(())
        }).unwrap();
            let origins = store.fence_rechecks().unwrap();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    let path = path.clone();
                    let barrier = barrier.clone();
                    let origins = origins.clone();
                    std::thread::spawn(move || {
                        let mut conn = Connection::open(path).unwrap();
                        conn.busy_timeout(std::time::Duration::from_secs(10))
                            .unwrap();
                        barrier.wait();
                        let tx = conn
                            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                            .unwrap();
                        let won = claim(&tx, &origins, NON_QUOTA_FENCE_MAX_MS).unwrap();
                        tx.commit().unwrap();
                        won
                    })
                })
                .collect();
            assert_eq!(
                threads
                    .into_iter()
                    .map(|thread| thread.join().unwrap() as usize)
                    .sum::<usize>(),
                1
            );
            if case != "account" {
                assert_eq!(
                    store.credential_entitlement_fences().unwrap()[0].observed_at_ms,
                    0,
                    "claim preserves refusal evidence"
                );
            }
        }
    }
}
