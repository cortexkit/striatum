//! Durable, per-origin retry clocks. Selection reads these clocks without
//! consuming them; admission claims every applicable clock in one transaction.

use crate::{err, AccountTx, SqliteRouteStateStore};
use prefrontal_routing_core::RouteStateResult;
use rusqlite::{params, Connection};

pub const QUOTA_FENCE_RECHECK_MS: i64 = 15 * 60_000;
pub const NON_QUOTA_FENCE_MAX_MS: i64 = 60 * 60_000;

#[derive(Clone, Debug)]
pub struct FenceRecheck {
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
}

impl FenceRecheck {
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
    }
}

pub(crate) fn sync(conn: &Connection) -> rusqlite::Result<()> {
    // A changed evidence value is new evidence, not a probe. Pool evidence
    // omits the shared last-used timestamp so leasing a key cannot extend its
    // retry deadline merely by changing that timestamp.
    conn.execute(
        "INSERT INTO fence_recheck (kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
          evidence,evidence_at,quota,next_recheck_at,reset_check_at)
         SELECT kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
          evidence,evidence_at,COALESCE(quota,0),
          CASE WHEN quota=1 THEN evidence_at+900000 ELSE MIN(COALESCE(deadline,evidence_at+3600000),evidence_at+3600000) END,
          CASE WHEN quota=1 AND reset_at>evidence_at THEN reset_at END
         FROM fence_origin WHERE true
         ON CONFLICT(kind,origin_key) DO UPDATE SET evidence=excluded.evidence,evidence_at=excluded.evidence_at,
          quota=excluded.quota,next_recheck_at=excluded.next_recheck_at,reset_check_at=excluded.reset_check_at,
          last_claimed_at=NULL WHERE fence_recheck.evidence!=excluded.evidence",
        [],
    )?;
    // Observation evidence is append-only and read through a bounded sparse
    // index. Its clock survives that lookback until a successful probe heals it.
    conn.execute("DELETE FROM fence_recheck WHERE kind!='observation' AND NOT EXISTS
        (SELECT 1 FROM fence_origin o WHERE o.kind=fence_recheck.kind AND o.origin_key=fence_recheck.origin_key)", [])?;
    Ok(())
}

pub(crate) fn read(conn: &Connection) -> rusqlite::Result<Vec<FenceRecheck>> {
    let mut statement = conn.prepare("SELECT kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
        evidence_at,quota,next_recheck_at,reset_check_at FROM fence_recheck ORDER BY kind,origin_key")?;
    let rows = statement.query_map([], |row| {
        Ok(FenceRecheck {
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
        })
    })?;
    rows.collect()
}

pub(crate) fn claim(
    conn: &Connection,
    origins: &[FenceRecheck],
    now: i64,
) -> rusqlite::Result<bool> {
    // Check the entire conjunction before updating any clock. BEGIN IMMEDIATE
    // serializes distinct connections; a lost race consumes none of the clocks.
    for origin in origins {
        let due: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM fence_recheck WHERE kind=?1 AND origin_key=?2
            AND evidence_at=?3 AND MIN(next_recheck_at,COALESCE(reset_check_at,next_recheck_at))<=?4)",
            params![origin.kind,origin.origin_key,origin.evidence_at,now], |row| row.get(0))?;
        if !due {
            return Ok(false);
        }
    }
    for origin in origins {
        let interval = if origin.quota {
            QUOTA_FENCE_RECHECK_MS
        } else {
            NON_QUOTA_FENCE_MAX_MS
        };
        conn.execute("UPDATE fence_recheck SET next_recheck_at=?3,last_claimed_at=?4,
            reset_check_at=CASE WHEN reset_check_at>?4 THEN reset_check_at END WHERE kind=?1 AND origin_key=?2",
            params![origin.kind,origin.origin_key,now.saturating_add(interval),now])?;
    }
    Ok(true)
}

impl SqliteRouteStateStore {
    pub fn fence_rechecks(&self) -> RouteStateResult<Vec<FenceRecheck>> {
        self.account_transaction(|tx| tx.fence_rechecks())
    }
}

impl AccountTx<'_> {
    pub fn fence_rechecks(&self) -> RouteStateResult<Vec<FenceRecheck>> {
        sync(self.conn).map_err(err)?;
        read(self.conn).map_err(err)
    }

    pub fn claim_fence_rechecks(
        &self,
        origins: &[FenceRecheck],
        now: i64,
    ) -> RouteStateResult<bool> {
        sync(self.conn).map_err(err)?;
        claim(self.conn, origins, now).map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let scratch = cortexkit_test_support::ScratchDir::new("fence-concurrency");
        let path = scratch.path().join("store.db");
        let store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "fence-concurrency",
            path.to_string_lossy(),
        ))
        .unwrap();
        store.store.with_conn(|conn| conn.execute("INSERT INTO credential_entitlement_fences VALUES ('p','c','ENTITLEMENT_ERROR',NULL,0)",[])).unwrap();
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
        assert_eq!(
            store.credential_entitlement_fences().unwrap()[0].observed_at_ms,
            0,
            "claim preserves refusal evidence"
        );
    }
}
