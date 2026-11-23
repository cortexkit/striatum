//! Read-only schema-93 oracle and its fixture reconciliation, never compiled in production.
#![allow(dead_code)]
use super::*;
pub(crate) fn enabled(conn: &Connection) -> rusqlite::Result<bool> {
    conn.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='fence_recheck')",
    )?
    .query_row([], |row| row.get(0))
}

/// Whether the stored recheck evidence differs from the current fence origins,
/// checked without materializing the writer's TEMP snapshot. The writer must
/// check again before synchronizing: evidence may change after this read
/// snapshot is released.
pub(crate) fn needs_sync(conn: &Connection) -> rusqlite::Result<bool> {
    if !enabled(conn)? {
        return Ok(false);
    }
    let quarantined = quarantined_origins(conn)?;
    let mut statement = conn.prepare("SELECT kind,origin_key,evidence FROM fence_recheck")?;
    let mut clocks = statement
        .query_map([], |row| {
            Ok((
                (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<std::collections::BTreeMap<_, _>>>()?;
    if quarantined.iter().any(|key| clocks.contains_key(key)) {
        return Ok(true);
    }
    // Scan the UNION view once rather than correlating it against every clock.
    let mut statement = conn.prepare("SELECT kind,origin_key,evidence FROM fence_origin")?;
    for row in statement.query_map([], |row| {
        Ok((
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
            row.get::<_, String>(2)?,
        ))
    })? {
        let (key, evidence) = row?;
        if quarantined.contains(&key) {
            continue;
        }
        if clocks.remove(&key).as_deref() != Some(evidence.as_str()) {
            return Ok(true);
        }
    }
    if clocks.keys().any(|(kind, _)| kind != "observation") {
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn sync(conn: &Connection) -> rusqlite::Result<()> {
    sync_at(conn, crate::now_ms())
}

/// The expired, unmanaged origins `sync` retires at `now`: their non-quota
/// deadline passed and no clock holds their evidence.
const RETIRING: &str =
    "SELECT o.kind,o.origin_key,o.evidence,o.evidence_at,?1 FROM current_fence_origin o
    WHERE o.quota=0 AND o.deadline<?1 AND NOT EXISTS (SELECT 1 FROM fence_recheck r
        WHERE r.kind=o.kind AND r.origin_key=o.origin_key AND r.evidence=o.evidence)";

/// Archive the expired, unmanaged origins of the current snapshot at `now`,
/// which makes `fence_retire_enforcement` end their source evidence.
/// Returns how many it archived. An archive row for the same generation is
/// replaced: deleted first, so the insert that fires the trigger resolves no
/// conflict. (A conflict policy on that insert would also apply to every
/// statement of the trigger, which writes the block tables.) The archive
/// table has no delete trigger.
pub(crate) fn retire_expired(conn: &Connection, now: i64) -> rusqlite::Result<usize> {
    conn.execute(
        &format!(
            "DELETE FROM fence_retired_origin WHERE (kind,origin_key,evidence) IN
            (SELECT kind,origin_key,evidence FROM ({RETIRING}))"
        ),
        [now],
    )?;
    conn.execute(
        &format!("INSERT INTO fence_retired_origin {RETIRING}"),
        [now],
    )
}

/// [`sync`] at a given time.
pub(crate) fn sync_at(conn: &Connection, now: i64) -> rusqlite::Result<()> {
    if !enabled(conn)? {
        return Ok(());
    }
    snapshot_origins(conn)?;
    let retired = retire_expired(conn, now)?;
    if retired > 0 {
        snapshot_origins(conn)?;
    }
    upsert_clocks(conn, None, None)?;
    // Observation evidence is append-only. Its clock survives the source
    // reader's observation lookback until a successful probe heals it.
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

/// Install a new refusal's clock at receipt, even if its shorter deadline is
/// already due. Only the named origin is registered; historical rows are not
/// mistaken for a new refusal merely because another account reported one.
pub(crate) fn register(conn: &Connection, kind: &str, key: &str) -> rusqlite::Result<()> {
    if !enabled(conn)? {
        return Ok(());
    }
    snapshot_origins(conn)?;
    upsert_clocks(conn, Some(kind), Some(key))
}

pub(crate) fn snapshot_origins(conn: &Connection) -> rusqlite::Result<()> {
    // Materialize once and index the compound origin key. A correlated lookup
    // against the UNION view otherwise rebuilds its JSON keys for every clock.
    conn.execute_batch("CREATE TEMP TABLE IF NOT EXISTS current_fence_origin AS SELECT * FROM fence_origin WHERE 0;
        CREATE UNIQUE INDEX IF NOT EXISTS current_fence_origin_key ON current_fence_origin(kind,origin_key);
        DELETE FROM current_fence_origin;
        INSERT INTO current_fence_origin SELECT * FROM fence_origin;")
}

fn upsert_clocks(conn: &Connection, kind: Option<&str>, key: Option<&str>) -> rusqlite::Result<()> {
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
         FROM current_fence_origin WHERE (?1 IS NULL OR (kind=?1 AND origin_key=?2))
         ON CONFLICT(kind,origin_key) DO UPDATE SET evidence=excluded.evidence,evidence_at=excluded.evidence_at,
          quota=excluded.quota,
          next_recheck_at=CASE WHEN excluded.evidence_at>=COALESCE(fence_recheck.last_claimed_at,-9223372036854775808)
            THEN excluded.next_recheck_at ELSE MAX(fence_recheck.next_recheck_at,excluded.next_recheck_at) END,
          reset_check_at=CASE WHEN excluded.reset_check_at>COALESCE(fence_recheck.last_claimed_at,-9223372036854775808) THEN excluded.reset_check_at END,
          classified=0,cleared_at=NULL,claim_owner=NULL WHERE fence_recheck.evidence!=excluded.evidence",
        params![kind,key],
    )?;
    Ok(())
}

pub(crate) fn quarantined_origins(conn: &Connection) -> rusqlite::Result<Vec<(String, String)>> {
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
            if quarantined_row(false, error.as_deref(), class.as_deref()) {
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
        evidence_at,quota,next_recheck_at,reset_check_at,last_claimed_at,claim_owner,evidence,classified FROM fence_recheck WHERE cleared_at IS NULL ORDER BY kind,origin_key")?;
    let rows = statement.query_map([], |row| {
        Ok(FenceRecheck {
            evidence: row.get(15)?,
            kind: row.get(0)?,
            origin_key: row.get(1)?,
            provider: row.get(2)?,
            account_id: row.get(3)?,
            credential_id: row.get(4)?,
            console_credit_micros_at_refusal: None,
            model: row.get(5)?,
            size_class: row.get(6)?,
            principal: row.get(7)?,
            admission_id: row.get(8)?,
            evidence_at: row.get(9)?,
            quota: row.get(10)?,
            classified: row.get(16)?,
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
    read_effective_at(conn, crate::now_ms())
}

pub(crate) fn read_effective_at(
    conn: &Connection,
    now: i64,
) -> rusqlite::Result<Vec<FenceRecheck>> {
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
        if !quota && deadline.is_some_and(|at| at < now) {
            continue;
        }
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
            console_credit_micros_at_refusal: None,
            model: row.get(5)?,
            size_class: row.get(6)?,
            principal: row.get(7)?,
            admission_id: row.get(8)?,
            evidence_at,
            quota,
            classified: false,
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

pub(crate) fn load_blocks(conn: &Connection, now: i64) -> rusqlite::Result<LoadedBlocks> {
    if !enabled(conn)? {
        return Ok(LoadedBlocks::default());
    }
    let mut clocks = Clocks::new();
    let mut statement = conn.prepare(&format!("SELECT {CLOCK_COLUMNS} FROM fence_recheck"))?;
    for row in statement.query_map([], |r| clock_row(r, 0))? {
        let (block, cleared) = row?;
        clocks.insert(
            (block.kind.clone(), block.origin_key.clone()),
            (block, cleared),
        );
    }
    let mut statement = conn.prepare(
        "SELECT kind,origin_key,evidence,model FROM fence_recheck_route_clear ORDER BY model",
    )?;
    let clears = statement
        .query_map([], |r| {
            Ok(RouteClear {
                kind: r.get(0)?,
                origin_key: r.get(1)?,
                evidence: r.get(2)?,
                model: r.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    attach_route_clears(&mut clocks, &clears);
    let quarantined = quarantined_origins(conn)?;
    let mut statement = conn.prepare("SELECT kind,origin_key,provider,account_id,credential_id,model,size_class,principal,admission_id,evidence,evidence_at,quota,deadline,reset_at FROM fence_origin")?;
    let origins = statement
        .query_map([], |r| view_origin(r, 0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    derive_blocks(clocks, &quarantined, origins, now)
}
