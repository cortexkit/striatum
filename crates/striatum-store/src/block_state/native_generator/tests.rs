//! Deterministic source writes against the block and related state tables.
use super::*;
use crate::block_storage::{self, Deadline, StoredBlock};
use std::collections::BTreeMap;

type Origin = (String, String);
#[derive(Clone, Debug)]
/// Evidence from a row whose probe clock was already registered (`managed=1`)
/// immediately before a fixture write, followed by the source evidence left
/// by that write. The block row does not keep separate old/current signatures.
pub(super) struct RenewalWitness {
    managed_before: Option<String>,
    source_set: String,
}
pub(super) type Witnesses = BTreeMap<Origin, Vec<RenewalWitness>>;

fn signatures(conn: &Connection, managed: bool) -> rusqlite::Result<BTreeMap<Origin, String>> {
    let clause = if managed {
        "managed=1 AND is_origin=1 AND (source_present=1 OR kind='observation')"
    } else {
        "source_present=1 AND is_origin=1"
    };
    conn.prepare(&format!(
        "SELECT kind,origin_key,evidence FROM block WHERE {clause}
        AND (cleared_by IS NULL OR cleared_by='kept')"
    ))?
    .query_map([], |row| Ok(((row.get(0)?, row.get(1)?), row.get(2)?)))?
    .collect()
}

fn write(rig: &Rig, change: impl Fn(&Connection) -> rusqlite::Result<()> + Send + 'static) {
    let owner = rig.owner.clone();
    rig.run(move |conn| {
        let clocks = signatures(conn, true)?;
        let before = signatures(conn, false)?;
        change(conn)?;
        let after = signatures(conn, false)?;
        let mut witnesses = owner.renewal_witnesses.lock().unwrap();
        for key in before.keys().filter(|key| !after.contains_key(*key)) {
            witnesses.remove(key);
        }
        for (key, evidence) in after {
            if before.get(&key) != Some(&evidence) {
                witnesses
                    .entry(key.clone())
                    .or_default()
                    .push(RenewalWitness {
                        managed_before: clocks.get(&key).cloned(),
                        source_set: evidence,
                    });
            }
        }
        Ok(())
    });
}

fn sql(rig: &Rig, statement: String) {
    write(rig, move |conn| conn.execute_batch(&statement));
}

/// Count only a prior managed generation replaced by the final source value.
/// The register-alone caller separately requires that registration wrote rows.
pub(super) fn renewed(witnesses: &Witnesses, kind: &str, key: &str, current: &str) -> bool {
    witnesses
        .get(&(kind.to_owned(), key.to_owned()))
        .is_some_and(|events| {
            events
                .last()
                .is_some_and(|event| event.source_set == current)
                && events.iter().any(|event| {
                    event
                        .managed_before
                        .as_deref()
                        .is_some_and(|old| old != current)
                })
        })
}

fn put(conn: &Connection, row: &StoredBlock, replace: bool) -> rusqlite::Result<()> {
    let changed = block_storage::write_source(conn, row)?;
    if changed != 0 && replace {
        // Replacement must move the row id too, so the sequence still tests
        // removal of the old physical slot rather than only an in-place edit.
        conn.execute("UPDATE block SET source_order=(SELECT COALESCE(MAX(source_order),0)+1 FROM block WHERE kind=?1)
            WHERE kind=?1 AND origin_key=?2", rusqlite::params![row.block.kind, row.block.origin_key])?;
        let columns = conn
            .prepare("SELECT name FROM pragma_table_xinfo('block') WHERE hidden=0 ORDER BY cid")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .join(",");
        conn.execute(&format!("INSERT OR REPLACE INTO block({columns}) SELECT {columns} FROM block WHERE kind=?1 AND origin_key=?2"),
            rusqlite::params![row.block.kind, row.block.origin_key])?;
    }
    Ok(())
}

fn literal(value: &str) -> Option<&str> {
    (value != "NULL").then(|| &value[1..value.len() - 1])
}
fn time(value: &str) -> Option<i64> {
    value.parse().ok()
}

fn pool_source(
    provider: &str,
    credential: &str,
    kind: &str,
    until: i64,
    reason: &str,
    at: i64,
) -> rusqlite::Result<StoredBlock> {
    let mut row = StoredBlock::from_origin(fence_rechecks::ViewOrigin {
        kind: kind.into(),
        origin_key: serde_json::json!([provider, credential]).to_string(),
        provider: provider.into(),
        credential_id: Some(credential.into()),
        evidence: serde_json::json!([until, reason, at]).to_string(),
        evidence_at: at,
        quota: Some(false),
        deadline: Some(until as f64),
        ..Default::default()
    })?;
    row.deadline = Some(Deadline::Integer(until));
    row.observed_at = Some(at);
    row.reason = Some(reason.into());
    Ok(row)
}

pub(super) fn step(rig: &Rig, rng: &mut Rng, now: i64) -> &'static str {
    rig.owner.renewal_witnesses.lock().unwrap().clear();
    let p = rng.pick(&PROVIDERS);
    let c = rng.pick(&CREDENTIALS);
    let m = rng.pick(&MODELS);
    let offset = rng.below(4);
    let at = rng.time(now);
    let until = rng.time(now) + 3_600_000;
    match rng.below(44) {
        0 => {
            let id = if rng.below(2) == 0 {
                format!("f{}", rng.below(4))
            } else {
                format!("credential-quota:[\"{p}\",\"{c}\"]:{}", rng.below(3))
            };
            let pool = match rng.below(3) {
                0 => None,
                key => Some(serde_json::from_str(&pool_key(key as u32)).unwrap()),
            };
            let recovery = if rng.below(2) == 0 {
                Vec::new()
            } else {
                vec![
                    serde_json::from_str(&pool_key(1)).unwrap(),
                    serde_json::from_str(&pool_key(2)).unwrap(),
                ]
            };
            write(rig, move |conn| {
                let row = block_storage::account_source(
                    conn,
                    &crate::AccountPoolFenceRow {
                        evidence: crate::AccountFenceRow {
                            fence_id: id.clone(),
                            provider: p.into(),
                            account_id: "a0".into(),
                            scope_json: "{}".into(),
                            fence_json: serde_json::json!({"reset":{"at_ms":until}}).to_string(),
                            source: "broca_account_limit".into(),
                            observed_at: at,
                            principal: "x".into(),
                            admission_id: None,
                            observation_id: None,
                            created_at: at,
                            cleared_at: None,
                            cleared_reason: None,
                        },
                        pool_key: pool.clone(),
                        recovery_set: recovery.clone(),
                    },
                )?;
                put(conn, &row, false)
            });
            "insert account fence"
        }
        1 => {
            sql(rig, format!("UPDATE block SET source_present=0,cleared_at={now},cleared_by='kept'
                WHERE kind='account' AND rowid IN (SELECT rowid FROM block WHERE kind='account' AND cleared_at IS NULL ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "clear account fence"
        }
        2 => {
            write(rig, move |conn| {
                let prior: Option<(String, Option<String>)> = conn
                    .query_row(
                        "SELECT code,listing_fingerprint FROM block WHERE kind='credential'
                    AND origin_key=json_array(?1,?2) AND source_present=1",
                        rusqlite::params![p, c],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                let (code, fingerprint) =
                    prior.unwrap_or_else(|| ("billing_exhausted".into(), None));
                put(
                    conn,
                    &block_storage::credential_source(
                        &striatum_core::route_state_store::CredentialEntitlementFence {
                            provider_id: p.into(),
                            credential_id: c.into(),
                            code,
                            listing_fingerprint: fingerprint,
                            observed_at_ms: at,
                        },
                    )?,
                    false,
                )
            });
            "upsert credential fence"
        }
        3 => {
            write(rig, move |conn| {
                put(
                    conn,
                    &block_storage::credential_source(
                        &striatum_core::route_state_store::CredentialEntitlementFence {
                            provider_id: p.into(),
                            credential_id: c.into(),
                            code: "model_unsupported".into(),
                            listing_fingerprint: None,
                            observed_at_ms: at,
                        },
                    )?,
                    true,
                )
            });
            "replace credential fence"
        }
        4 => {
            let class = rng.pick(&["quota", "transient"]);
            write(rig, move |conn| {
                let key = serde_json::json!([p, c, m]).to_string();
                let recorded=conn.query_row("SELECT recorded_at FROM block WHERE kind='account_route' AND origin_key=?1 AND source_present=1",
                    [&key],|row|row.get::<_,i64>(0)).optional()?.unwrap_or(at);
                put(
                    conn,
                    &block_storage::account_route_source(
                        &crate::AccountRouteFence {
                            provider: p.into(),
                            credential_id: c.into(),
                            model: m.into(),
                            until_ms: until,
                            decision_id: "d".into(),
                            recorded_at: recorded,
                        },
                        "account_route",
                        class,
                        Some(at),
                    )?,
                    false,
                )
            });
            "upsert account route fence"
        }
        5 => {
            let kind = rng.pick(&["billing", "transient"]);
            write(rig, move |conn| {
                put(
                    conn,
                    &block_storage::account_route_source(
                        &crate::AccountRouteFence {
                            provider: p.into(),
                            credential_id: c.into(),
                            model: m.into(),
                            until_ms: until,
                            decision_id: "d".into(),
                            recorded_at: at,
                        },
                        if kind == "billing" {
                            "non_quota_route"
                        } else {
                            "transient_route"
                        },
                        kind,
                        Some(at),
                    )?,
                    true,
                )
            });
            "replace non-quota route fence"
        }
        6 => {
            let (wall, cooldown) = (
                rng.maybe_time(now + 3_600_000),
                rng.maybe_time(now + 3_600_000),
            );
            write(rig, move |conn| {
                conn.execute("INSERT INTO pool_key_state(provider,credential_id,wall_observed_at,cooldown_observed_at,updated_at)
                    VALUES (?1,?2,?3,?3,?3) ON CONFLICT(provider,credential_id) DO UPDATE SET updated_at=excluded.updated_at",rusqlite::params![p,c,at])?;
                for (kind, value, reason, column) in [
                    ("pool_billing", time(&wall), "billing", "wall_observed_at"),
                    ("pool_rate", time(&cooldown), "rate", "cooldown_observed_at"),
                ] {
                    if let Some(until) = value {
                        let observed=conn.query_row(&format!("SELECT COALESCE({column},updated_at) FROM pool_key_state WHERE provider=?1 AND credential_id=?2"),rusqlite::params![p,c],|row|row.get(0))?;
                        put(
                            conn,
                            &pool_source(p, c, kind, until, reason, observed)?,
                            false,
                        )?;
                    } else {
                        conn.execute(
                            "DELETE FROM block WHERE kind=?1 AND origin_key=json_array(?2,?3)",
                            rusqlite::params![kind, p, c],
                        )?;
                    }
                }
                Ok(())
            });
            "upsert pool key"
        }
        7 => {
            sql(rig,format!("UPDATE pool_key_state SET last_used_at={at},updated_at={at} WHERE rowid IN (SELECT rowid FROM pool_key_state ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "touch pool key"
        }
        8 => {
            let (cooldown, quota) = (
                rng.maybe_time(now + 3_600_000),
                rng.maybe_time(now + 3_600_000),
            );
            let error = rng.pick(&ERRORS);
            let unscoped = rng.below(2);
            let class = rng.pick(&["NULL", "'quota'", "'billing'"]);
            write(rig, move |conn| {
                conn.execute("INSERT OR REPLACE INTO provider_cooldown_state(provider_key,last_error,last_error_at,updated_at,wall_observed_at)
                    VALUES (?1,?2,?3,?3,?3)",rusqlite::params![p,literal(error),at])?;
                if !fence_rechecks::quarantined_row(unscoped != 0, literal(error), literal(class)) {
                    if let Some(row) = block_storage::wall_source(
                        conn,
                        block_storage::WallSource {
                            kind: "provider",
                            key: p,
                            provider: p,
                            model: None,
                            cooldown: time(&cooldown).map(|v| v as f64),
                            exhausted: time(&quota).map(|v| v as f64),
                            wall_observed: Some(at),
                            observed_at: at,
                            error: literal(error),
                            class: literal(class),
                            unscoped: unscoped != 0,
                        },
                    )? {
                        return put(conn, &row, true);
                    }
                }
                conn.execute(
                    "DELETE FROM block WHERE kind='provider' AND origin_key=json_array(?1)",
                    [p],
                )?;
                Ok(())
            });
            "replace provider row"
        }
        9 => {
            // The provider's cooldown and quota deadlines hide only the
            // matching route origin; the route's own deadline is preserved.
            let copy = rng.below(2) == 0;
            let shorter = if rng.below(2) == 0 { "-1" } else { "" };
            if copy {
                write(rig, move |conn| {
                    let meta:Option<(Option<String>,Option<i64>,Option<i64>,i64)>=conn.query_row(
                        "SELECT last_error,last_error_at,wall_observed_at,updated_at FROM provider_cooldown_state WHERE provider_key=?1",[p],
                        |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional()?;
                    let Some((error, error_at, observed, updated)) = meta else {
                        return Ok(());
                    };
                    let parent = source(conn, "provider", &serde_json::json!([p]).to_string())?;
                    let cooldown = parent
                        .as_ref()
                        .and_then(|row| row.cooldown_until)
                        .map(|v| v.as_f64() - if shorter.is_empty() { 0.0 } else { 1.0 });
                    let quota = parent
                        .as_ref()
                        .and_then(|row| row.quota_exhausted_until)
                        .map(|v| v.as_f64());
                    let class = parent.as_ref().and_then(|row| row.wall_class.as_deref());
                    let unscoped = parent.as_ref().is_some_and(|row| row.wall_unscoped);
                    let key = format!("{p}/{m}");
                    conn.execute("INSERT OR REPLACE INTO model_route_state(route_key,capacity_key,provider_id,model_id,last_error,last_error_at,updated_at,wall_observed_at)
                        VALUES (?1,?1,?2,?3,?4,?5,?6,?7)",rusqlite::params![key,p,m,error,error_at,updated,observed])?;
                    if !fence_rechecks::quarantined_row(unscoped, error.as_deref(), class) {
                        if let Some(row) = block_storage::wall_source(
                            conn,
                            block_storage::WallSource {
                                kind: "route",
                                key: &key,
                                provider: p,
                                model: Some(m),
                                cooldown,
                                exhausted: quota,
                                wall_observed: observed,
                                observed_at: observed.or(error_at).unwrap_or(updated),
                                error: error.as_deref(),
                                class,
                                unscoped,
                            },
                        )? {
                            put(conn, &row, true)?;
                        } else {
                            remove(conn, "route", &key)?;
                        }
                    } else {
                        remove(conn, "route", &key)?;
                    }
                    remove(conn, "failure", &key)
                });
            } else {
                let (cooldown, quota, failure) = (
                    rng.maybe_time(now + 3_600_000),
                    rng.maybe_time(now + 3_600_000),
                    rng.maybe_time(now + 3_600_000),
                );
                let error = rng.pick(&ERRORS);
                let unscoped = rng.below(2);
                write(rig, move |conn| {
                    let key = format!("{p}/{m}");
                    let prior = source(conn, "route", &serde_json::json!([key]).to_string())?;
                    let prior_failure =
                        source(conn, "failure", &serde_json::json!([key]).to_string())?;
                    let class = prior
                        .as_ref()
                        .or(prior_failure.as_ref())
                        .and_then(|row| row.wall_class.as_deref());
                    conn.execute("INSERT INTO model_route_state(route_key,capacity_key,provider_id,model_id,wall_observed_at,last_error,last_error_at,updated_at,failure_observed_at)
                        VALUES (?1,?1,?2,?3,?4,?5,?4,?4,?4) ON CONFLICT(route_key) DO UPDATE SET wall_observed_at=excluded.wall_observed_at,last_error=excluded.last_error",
                        rusqlite::params![key,p,m,at,literal(error)])?;
                    if !fence_rechecks::quarantined_row(unscoped != 0, literal(error), class) {
                        if let Some(row) = block_storage::wall_source(
                            conn,
                            block_storage::WallSource {
                                kind: "route",
                                key: &key,
                                provider: p,
                                model: Some(m),
                                cooldown: time(&cooldown).map(|v| v as f64),
                                exhausted: time(&quota).map(|v| v as f64),
                                wall_observed: Some(at),
                                observed_at: at,
                                error: literal(error),
                                class,
                                unscoped: unscoped != 0,
                            },
                        )? {
                            put(conn, &row, false)?;
                        } else {
                            remove(conn, "route", &key)?;
                        }
                    } else {
                        remove(conn, "route", &key)?;
                    }
                    let failure_at=conn.query_row("SELECT COALESCE(failure_observed_at,last_error_at,updated_at) FROM model_route_state WHERE route_key=?1",[&key],|row|row.get(0))?;
                    if let Some(row) = block_storage::failure_source(
                        conn,
                        block_storage::FailureSource {
                            key: &key,
                            provider: p,
                            model: m,
                            deadline: time(&failure).map(|v| v as f64),
                            reason: prior_failure
                                .as_ref()
                                .and_then(|row| row.reason.as_deref())
                                .or(Some("error")),
                            observed_at: failure_at,
                            error: literal(error),
                            class,
                            unscoped: unscoped != 0,
                        },
                    )? {
                        put(conn, &row, false)?;
                    } else {
                        remove(conn, "failure", &key)?;
                    }
                    Ok(())
                });
            }
            "write route row"
        }
        10 => {
            sql(rig,format!("UPDATE model_route_state SET latency_ema_ms={offset},updated_at={at} WHERE rowid IN (SELECT rowid FROM model_route_state ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "route data only"
        }
        11 => {
            let size = 16 + rng.below(2);
            let table = rng.pick(&["account_size_refusal", "account_non_quota_size_refusal"]);
            write(rig, move |conn| {
                put(
                    conn,
                    &block_storage::size_source(
                        &crate::AccountSizeRefusal {
                            provider: p.into(),
                            credential_id: c.into(),
                            model: m.into(),
                            size_class: size as u32,
                            expires_at: until,
                            observed_at: at,
                        },
                        table == "account_size_refusal",
                    )?,
                    true,
                )
            });
            "size refusal"
        }
        12 => {
            sql(rig,format!("UPDATE OR IGNORE block SET size_class=size_class+1,origin_key=json_array(provider,credential_id,model,size_class+1)
                WHERE kind='size' AND rowid IN (SELECT rowid FROM block WHERE kind='size' AND source_present=1 ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "move a size refusal's key"
        }
        13 => {
            write(rig, move |conn| {
                put(
                    conn,
                    &block_storage::principal_source(
                        conn,
                        &crate::AccountExclusionRow {
                            principal: "x".into(),
                            admission_id: format!("adm{offset}"),
                            credential_id: c.into(),
                            runner_view_digest: "runner".into(),
                            roster_view_digest: "roster".into(),
                            observation_id: None,
                            recorded_at: at,
                        },
                    )?,
                    false,
                )
            });
            "principal exclusion"
        }
        14 => {
            write(rig, move |conn| {
                let key = serde_json::json!([p, format!("a{offset}")]).to_string();
                let previous = source(conn, "observation", &key)?;
                let evidence_at = previous.as_ref().map_or(at, |row| row.block.evidence_at);
                let mut row = StoredBlock::from_origin(fence_rechecks::ViewOrigin {
                    kind: "observation".into(),
                    origin_key: key,
                    provider: p.into(),
                    account_id: Some(format!("a{offset}")),
                    evidence: serde_json::json!([format!("k{at}"), "[]"]).to_string(),
                    evidence_at,
                    quota: Some(false),
                    deadline: Some(until as f64),
                    ..Default::default()
                })?;
                row.deadline = Some(Deadline::Integer(until));
                put(conn, &row, false)
            });
            "transient backoff"
        }
        15 => {
            let table = rng.pick(&[
                "account_fence",
                "credential_entitlement_fences",
                "account_route_fence",
                "account_non_quota_route_fence",
                "pool_key_state",
                "provider_cooldown_state",
                "model_route_state",
                "account_size_refusal",
                "account_principal_exclusion",
                "account_transient_backoff",
                "account_eligibility",
            ]);
            write(rig, move |conn| {
                match table {
                    "pool_key_state" => {
                        let key:Option<(String,String)>=conn.query_row("SELECT provider,credential_id FROM pool_key_state ORDER BY rowid LIMIT 1 OFFSET ?1",[offset],|row|Ok((row.get(0)?,row.get(1)?))).optional()?;
                        if let Some((provider, credential)) = key {
                            conn.execute(
                                "DELETE FROM pool_key_state WHERE provider=?1 AND credential_id=?2",
                                rusqlite::params![provider, credential],
                            )?;
                            conn.execute("DELETE FROM block WHERE kind IN ('pool_billing','pool_rate') AND origin_key=json_array(?1,?2)",rusqlite::params![provider,credential])?;
                        }
                    }
                    "provider_cooldown_state" | "model_route_state" => {
                        let (column, kinds) = if table == "provider_cooldown_state" {
                            ("provider_key", "'provider'")
                        } else {
                            ("route_key", "'route','failure'")
                        };
                        let key: Option<String> = conn
                            .query_row(
                                &format!(
                                    "SELECT {column} FROM {table} ORDER BY rowid LIMIT 1 OFFSET ?1"
                                ),
                                [offset],
                                |row| row.get(0),
                            )
                            .optional()?;
                        if let Some(key) = key {
                            conn.execute(
                                &format!("DELETE FROM {table} WHERE {column}=?1"),
                                [&key],
                            )?;
                            conn.execute(&format!("DELETE FROM block WHERE kind IN ({kinds}) AND origin_key=json_array(?1)"),[&key])?;
                        }
                    }
                    "account_eligibility" => {
                        conn.execute("DELETE FROM account_eligibility WHERE rowid IN (SELECT rowid FROM account_eligibility ORDER BY rowid LIMIT 1 OFFSET ?1)",[offset])?;
                    }
                    _ => {
                        let kinds = match table {
                            "account_fence" => "'account'",
                            "credential_entitlement_fences" => "'credential'",
                            "account_route_fence" => "'account_route'",
                            "account_non_quota_route_fence" => {
                                "'non_quota_route','transient_route'"
                            }
                            "account_size_refusal" => "'size'",
                            "account_principal_exclusion" => "'principal'",
                            _ => "'observation'",
                        };
                        if table == "account_transient_backoff" {
                            conn.execute("UPDATE block SET source_present=0 WHERE kind='observation' AND rowid IN (SELECT rowid FROM block WHERE kind='observation' AND source_present=1 ORDER BY rowid LIMIT 1 OFFSET ?1)",[offset])?;
                        } else {
                            conn.execute(&format!("DELETE FROM block WHERE rowid IN (SELECT rowid FROM block WHERE kind IN ({kinds}) ORDER BY rowid LIMIT 1 OFFSET ?1)"),[offset])?;
                        }
                    }
                }
                Ok(())
            });
            "delete a source row"
        }
        16 | 17 => {
            rig.store
                .account_transaction(|tx| fence_rechecks::sync_on(tx, crate::now_ms()).map_err(err))
                .unwrap();
            "sync"
        }
        18 => {
            let owner = owner_name(rng.below(3));
            rig.run(move |conn| {
                let origins = fence_rechecks::read(conn)?;
                if let Some(origin) = origins.get(offset as usize) {
                    let claimed_at = now + 4 * 3_600_000;
                    fence_rechecks::claim_with_authorization(
                        conn,
                        std::slice::from_ref(origin),
                        claimed_at,
                        &owner,
                    )?;
                }
                Ok(())
            });
            "claim"
        }
        19 => {
            sql(rig,format!("UPDATE block SET cleared_at={now},cleared_by='kept',claim_owner=NULL WHERE rowid IN
                (SELECT rowid FROM block WHERE managed=1 AND is_origin=1 ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "heal a clock"
        }
        20 => {
            sql(rig,format!("UPDATE block SET quota=1-quota,classified=1 WHERE kind='credential' AND rowid IN
                (SELECT rowid FROM block WHERE kind='credential' AND managed=1 ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "classify a credential"
        }
        21 => {
            sql(rig,format!("UPDATE block SET cleared_models=CASE WHEN EXISTS(SELECT 1 FROM json_each(cleared_models) WHERE value='{m}')
                THEN cleared_models ELSE json_insert(cleared_models,'$[#]','{m}') END WHERE rowid IN
                (SELECT rowid FROM block WHERE kind IN ('provider','route') AND managed=1 ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "partial route clear"
        }
        22 => {
            sql(rig, "UPDATE block SET cleared_models='[]';".into());
            "delete every route clear"
        }
        23 => {
            sql(rig,format!("DELETE FROM probe_authorization WHERE rowid IN (SELECT rowid FROM probe_authorization ORDER BY rowid LIMIT 1 OFFSET {offset});"));
            "release an authorization"
        }
        24 => {
            let key = pool_key(1 + rng.below(2) as u32);
            if rng.below(2) == 0 {
                sql(rig,format!("INSERT OR REPLACE INTO success_proof SELECT json_extract(origin_key,'$[0]'),'{key}',MIN(observed_at,created_at),{now}
                    FROM block WHERE kind='account' ORDER BY rowid LIMIT 1 OFFSET {offset};"));
            } else {
                sql(rig,format!("INSERT OR REPLACE INTO success_proof VALUES ('f{offset}','{key}',{at},{now});"));
            }
            "success pool"
        }
        25 => {
            let update = RouteStateUpdate {
                observed_at_ms: Some(at),
                cooldown_until: Some(until as f64),
                last_error: Some("provider capacity".into()),
                ..Default::default()
            };
            rig.store
                .record_provider_cooldown(p, &format!("{p}/{m}"), &format!("{p}/{m}"), &update)
                .unwrap();
            "record provider cooldown"
        }
        26 => {
            let update = RouteStateUpdate {
                observed_at_ms: Some(at),
                quota_exhausted_until: (rng.below(2) == 0).then_some(until as f64),
                latency_ema_ms: Some(10.0),
                increment_failure: rng.below(2) == 0,
                ..Default::default()
            };
            rig.store
                .update_route_state(&format!("{p}/{m}"), &format!("{p}/{m}"), &update)
                .unwrap();
            "update route state"
        }
        27 => {
            rig.store
                .upsert_fact(&fact(p, &format!("{m}-{}", rng.below(4))), FactSource::Seed)
                .unwrap();
            "catalogue insert"
        }
        28 => {
            rig.store
                .clear_route_cooldowns(&format!("{p}/{m}"))
                .unwrap();
            "operator clear"
        }
        29 => {
            let model = if rng.below(3) == 0 { "" } else { m };
            let fingerprint = rng.pick(&["NULL", "'l0'", "'l1'"]);
            sql(rig,format!("INSERT INTO account_eligibility VALUES ('{p}','{c}','{model}',{fingerprint},'model_unsupported',{at})
                ON CONFLICT(provider,credential_id,model) DO UPDATE SET listing_fingerprint=excluded.listing_fingerprint,observed_at=excluded.observed_at;"));
            "eligibility fact"
        }
        30 => {
            sql(
                rig,
                format!(
                    "DELETE FROM account_eligibility WHERE provider='{p}' AND credential_id='{c}';"
                ),
            );
            "replace a listing's eligibility"
        }
        31..=42 => decide_both(rig, rng, now),
        _ => {
            let result=rig.store.account_command(AccountCommandKind::Maintenance,move|tx| {
                tx.pool_sql(|conn| {
                    let mut row=StoredBlock::from_origin(fence_rechecks::ViewOrigin {kind:"observation".into(),origin_key:serde_json::json!([p,"failed"]).to_string(),
                        provider:p.into(),account_id:Some("failed".into()),evidence:serde_json::json!(["k","[]"]).to_string(),evidence_at:at,
                        quota:Some(false),deadline:Some(until as f64),..Default::default()})?;
                    row.deadline=Some(Deadline::Integer(until));
                    put(conn,&row,true)?;
                    conn.execute("UPDATE block SET managed=0,claim_owner=NULL,last_claimed_at=NULL,claim_generation=0 WHERE rowid IN
                        (SELECT rowid FROM block WHERE managed=1 AND is_origin=1 LIMIT 1)",[])?;
                    Ok(())
                })?;
                Err::<(),_>(err("planned failure"))
            });
            assert!(result.is_err());
            "failed command"
        }
    }
}

fn source(conn: &Connection, kind: &str, key: &str) -> rusqlite::Result<Option<StoredBlock>> {
    conn.query_row(
        &format!(
            "{} WHERE kind=?1 AND origin_key=?2 AND source_present=1",
            block_storage::select()
        ),
        rusqlite::params![kind, key],
        block_storage::decode,
    )
    .optional()
}
fn remove(conn: &Connection, kind: &str, key: &str) -> rusqlite::Result<()> {
    conn.execute(
        "DELETE FROM block WHERE kind=?1 AND origin_key=json_array(?2)",
        rusqlite::params![kind, key],
    )
    .map(|_| ())
}
