//! Durable, per-origin retry clocks. Selection reads these clocks without
//! consuming them; admission claims every applicable clock in one transaction.

use crate::{err, AccountTx, SqliteRouteStateStore};
use prefrontal_routing_core::RouteStateResult;
use rusqlite::{params, Connection};

pub use prefrontal_routing_core::fence_lifetimes::{
    NON_QUOTA_FENCE_MAX_MS, QUOTA_FENCE_RECHECK_MS,
};

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
          last_claimed_at=NULL,classified=0,cleared_at=NULL WHERE fence_recheck.evidence!=excluded.evidence",
        [],
    )?;
    // Observation evidence is append-only and read through a bounded sparse
    // index. Its clock survives that lookback until a successful probe heals it.
    conn.execute("DELETE FROM fence_recheck WHERE kind!='observation' AND NOT EXISTS
        (SELECT 1 FROM fence_origin o WHERE o.kind=fence_recheck.kind AND o.origin_key=fence_recheck.origin_key)", [])?;
    // Older writers stored account-local errors on model/provider rows.
    // Snapshot reads ignore those rows as model/provider walls. Exclude their
    // retry clocks too, so one account's error cannot hide healthy siblings.
    for (table,key,kind) in [("model_route_state","route_key","route"),("provider_cooldown_state","provider_key","provider")] {
        let mut statement=conn.prepare(&format!("SELECT {key},last_error,wall_class FROM {table}"))?;
        let rows=statement.query_map([],|row|Ok((row.get::<_,String>(0)?,row.get::<_,Option<String>>(1)?,row.get::<_,Option<String>>(2)?)))?;
        for row in rows {
            let (key,error,class)=row?;
            let class=class.and_then(|class|prefrontal_routing_core::RouteWallClass::parse(&class).ok());
            if prefrontal_routing_core::account_wall::account_scoped_wall(error.as_deref(),class) {
                conn.execute("DELETE FROM fence_recheck WHERE kind=?1 AND origin_key=?2",params![kind,serde_json::json!([key]).to_string()])?;
            }
        }
    }
    Ok(())
}

pub(crate) fn read(conn: &Connection) -> rusqlite::Result<Vec<FenceRecheck>> {
    let mut statement = conn.prepare("SELECT kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,
        evidence_at,quota,next_recheck_at,reset_check_at FROM fence_recheck WHERE cleared_at IS NULL ORDER BY kind,origin_key")?;
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
        let origins = self.fence_rechecks()?;
        for origin in origins.iter().filter(|origin| {
            origin.applies(provider, account, credential, model)
                && origin.evidence_at < admitted_at
                && origin.principal.is_none()
                && origin
                    .size_class
                    .is_none_or(|class| Some(class as u32) == size)
        }) {
            let key: Vec<serde_json::Value> =
                serde_json::from_str(&origin.origin_key).map_err(err)?;
            match origin.kind.as_str() {
                "account" => {
                    self.conn.execute("UPDATE account_fence SET cleared_at=?2,cleared_reason='run_succeeded' WHERE fence_id=?1 AND observed_at<?3",params![key[0].as_str(),now,admitted_at]).map_err(err)?;
                }
                "credential" => {
                    self.conn.execute("DELETE FROM credential_entitlement_fences WHERE provider_id=?1 AND credential_id=?2 AND observed_at_ms<?3",params![provider,credential,admitted_at]).map_err(err)?;
                }
                "account_route" | "non_quota_route" => {
                    let table = if origin.kind == "account_route" {
                        "account_route_fence"
                    } else {
                        "account_non_quota_route_fence"
                    };
                    self.conn.execute(&format!("DELETE FROM {table} WHERE provider=?1 AND credential_id=?2 AND model=?3 AND COALESCE(last_reported_at,recorded_at)<?4"),params![provider,credential,model,admitted_at]).map_err(err)?;
                }
                "size" => {
                    self.conn.execute("DELETE FROM account_size_refusal WHERE provider=?1 AND credential_id=?2 AND model=?3 AND size_class=?4 AND observed_at<?5",params![provider,credential,model,origin.size_class,admitted_at]).map_err(err)?;
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
                    self.conn.execute("UPDATE model_route_state SET
                        cooldown_until=CASE WHEN cooldown_until<=(SELECT cooldown_until FROM provider_cooldown_state WHERE provider_key=?1) THEN NULL ELSE cooldown_until END,
                        quota_exhausted_until=CASE WHEN quota_exhausted_until<=(SELECT quota_exhausted_until FROM provider_cooldown_state WHERE provider_key=?1) THEN NULL ELSE quota_exhausted_until END
                        WHERE provider_id=?1",[provider]).map_err(err)?;
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
        sync(self.conn).map_err(err)?;
        self.conn.execute("UPDATE fence_recheck SET quota=?3,classified=1,next_recheck_at=MIN(next_recheck_at,evidence_at+CASE WHEN ?3 THEN 900000 ELSE 3600000 END)
            WHERE kind='credential' AND provider=?1 AND credential_id=?2 AND classified=0",params![provider,credential,quota]).map_err(err)?;
        self.conn.query_row("SELECT quota FROM fence_recheck WHERE kind='credential' AND provider=?1 AND credential_id=?2",params![provider,credential],|row|row.get(0)).map_err(err)
    }
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
    fn success_heals_only_serving_identity_and_evidence_older_than_admission() {
        let scratch=cortexkit_test_support::ScratchDir::new("fence-success-boundary");
        let store=SqliteRouteStateStore::open(&crate::dev_descriptor("fence-success-boundary",scratch.path().join("store.db").to_string_lossy())).unwrap();
        for credential in ["served","sibling"] {
            store.record_account_route_fence(&crate::AccountRouteFence {provider:"p".into(),credential_id:credential.into(),model:"m".into(),until_ms:0,decision_id:"d".into(),recorded_at:100}).unwrap();
            store.record_transient_account_route_fence(&crate::AccountRouteFence {provider:"p".into(),credential_id:credential.into(),model:"m".into(),until_ms:10_000,decision_id:"d".into(),recorded_at:100}).unwrap();
        }
        store.account_transaction(|tx|tx.heal_serving_fences("p","a","served","m",None,99,200)).unwrap();
        assert_eq!(store.account_route_fences(200).unwrap().len(),2);
        store.account_transaction(|tx|tx.heal_serving_fences("p","a","served","m",None,101,200)).unwrap();
        assert_eq!(store.account_route_fences(200).unwrap()[0].credential_id,"sibling");
        assert_eq!(store.transient_account_route_fences(200).unwrap()[0].credential_id,"sibling");
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
