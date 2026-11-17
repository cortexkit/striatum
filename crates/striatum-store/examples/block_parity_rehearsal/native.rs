use rusqlite::Connection;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::path::Path;
use std::time::Instant;
use striatum_core::RouteWallClass;
use striatum_store::{dev_descriptor, BlockParity, FenceRecheck, SqliteRouteStateStore};

#[path = "domain.rs"]
mod domain;

type Key = (String, String, String);
type Scope = (String, String);
const KINDS: [&str; 14] = [
    "account",
    "credential",
    "account_route",
    "non_quota_route",
    "transient_route",
    "pool_billing",
    "pool_rate",
    "provider",
    "route",
    "failure",
    "size",
    "non_quota_size",
    "principal",
    "observation",
];

struct Before {
    rows: BTreeMap<Scope, FenceRecheck>,
    tombstones: BTreeSet<Key>,
    quarantine_metadata: BTreeMap<String, usize>,
    quarantine_origins: BTreeMap<String, usize>,
    future_provider: usize,
    future_provider_changed_status: usize,
    cleared_observations: Vec<Key>,
    old_observation_authorizations: usize,
}

fn triples(conn: &Connection, sql: &str) -> rusqlite::Result<BTreeSet<Key>> {
    conn.prepare(sql)?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect()
}

fn canonical(rows: Vec<FenceRecheck>) -> BTreeMap<Scope, FenceRecheck> {
    rows.into_iter()
        .map(|mut row| {
            row.cleared_models.sort();
            ((row.kind.clone(), row.origin_key.clone()), row)
        })
        .collect()
}

fn kinds<'a>(values: impl IntoIterator<Item = &'a str>) -> BTreeMap<String, usize> {
    let mut counts: BTreeMap<_, _> = KINDS
        .into_iter()
        .map(|kind| (kind.to_string(), 0))
        .collect();
    for kind in values {
        *counts.entry(kind.to_string()).or_default() += 1;
    }
    counts
}

fn before(store: &SqliteRouteStateStore, now: i64) -> Result<Before, Box<dyn Error>> {
    let rows = canonical(store.effective_blocks_oracle_for_test(now)?);
    store.with_reader_for_test(|conn| {
        let mut quarantine=BTreeSet::new();
        let mut metadata=BTreeMap::from([("route".to_string(),0usize),("provider".to_string(),0usize)]);
        let mut origins=metadata.clone();
        let mut future_provider=0;
        let mut changed=0;
        let mut query=conn.prepare("SELECT 'route',json_array(route_key),wall_unscoped,last_error,wall_class,
            MAX(COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0)),cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL
            FROM model_route_state UNION ALL
            SELECT 'provider',json_array(provider_key),wall_unscoped,last_error,wall_class,
            MAX(COALESCE(cooldown_until,0),COALESCE(quota_exhausted_until,0)),cooldown_until IS NOT NULL OR quota_exhausted_until IS NOT NULL
            FROM provider_cooldown_state")?;
        for value in query.query_map([],|row|Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,bool>(2)?,row.get::<_,Option<String>>(3)?,row.get::<_,Option<String>>(4)?,row.get::<_,f64>(5)?,row.get::<_,bool>(6)?)))? {
            let (kind,key,unscoped,error,class,deadline,is_origin)=value?;
            let class=class.as_deref().and_then(|value|RouteWallClass::parse(value).ok());
            if !unscoped && striatum_core::account_wall::account_scoped_wall(error.as_deref(),class) {
                *metadata.get_mut(&kind).unwrap()+=1;
                if is_origin { *origins.get_mut(&kind).unwrap()+=1; }
                if kind=="provider" && deadline>now as f64 {
                    future_provider+=1;
                    if !error.as_deref().is_some_and(|error|striatum_core::account_wall::credential_refusal_in(error).is_some()) { changed+=1; }
                }
                quarantine.insert((kind,key));
            }
        }
        let mut tombstones=triples(conn,"SELECT kind,origin_key,evidence FROM fence_retired_origin")?;
        tombstones.extend(triples(conn,"SELECT 'account',json_array(fence_id),fence_json FROM account_fence WHERE cleared_at IS NOT NULL")?);
        let clock_tombs=triples(conn,"SELECT kind,origin_key,evidence FROM fence_recheck WHERE cleared_at IS NOT NULL")?;
        tombstones.extend(clock_tombs.into_iter().filter(|(kind,key,_)|!quarantine.contains(&(kind.clone(),key.clone()))));
        let source=triples(conn,"SELECT kind,origin_key,evidence FROM fence_origin")?;
        for (kind,key,evidence) in &source {
            if !quarantine.contains(&(kind.clone(),key.clone())) && !rows.contains_key(&(kind.clone(),key.clone())) {
                tombstones.insert((kind.clone(),key.clone(),evidence.clone()));
            }
        }
        // Removed non-observation recheck rows remain stored for audit.
        // Observation rechecks outlive source pruning and still govern retries.
        let orphan=triples(conn,"SELECT r.kind,r.origin_key,r.evidence FROM fence_recheck r WHERE r.kind!='observation'
            AND NOT EXISTS(SELECT 1 FROM fence_origin o WHERE o.kind=r.kind AND o.origin_key=r.origin_key)
            AND NOT EXISTS(SELECT 1 FROM account_route_fence f WHERE r.kind='account_route' AND r.origin_key=json_array(f.provider,f.credential_id,f.model))")?;
        tombstones.extend(orphan.into_iter().filter(|(kind,key,_)|!quarantine.contains(&(kind.clone(),key.clone()))));
        let cleared_observations:Vec<_>=triples(conn,"SELECT o.kind,o.origin_key,o.evidence FROM fence_origin o JOIN fence_recheck r
            ON r.kind=o.kind AND r.origin_key=o.origin_key AND r.evidence=o.evidence WHERE o.kind='observation' AND r.cleared_at IS NOT NULL")?.into_iter().collect();
        let mut authorizations=0;
        for (kind,key,evidence) in &cleared_observations {
            authorizations+=conn.query_row("SELECT count(*) FROM fence_probe_authorization WHERE kind=?1 AND origin_key=?2 AND evidence=?3",rusqlite::params![kind,key,evidence],|row|row.get::<_,usize>(0))?;
        }
        Ok(Before {rows,tombstones,quarantine_metadata:metadata,quarantine_origins:origins,future_provider,future_provider_changed_status:changed,cleared_observations,old_observation_authorizations:authorizations})
    }).map_err(Into::into)
}

fn key_description(key: &Key) -> serde_json::Value {
    json!({"kind":key.0,"origin_key":key.1,"evidence_blake3":blake3::hash(key.2.as_bytes()).to_hex().to_string()})
}

pub(super) fn rehearse(path: &Path, now: i64) -> Result<BlockParity, Box<dyn Error>> {
    let version = Connection::open(path)?.query_row(
        "SELECT MAX(version) FROM cortexkit_schema_version WHERE namespace='route_state'",
        [],
        |row| row.get::<_, u32>(0),
    )?;
    let descriptor = dev_descriptor("block-storage-rehearsal", path.to_string_lossy());
    let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, version)?;
    let before = before(&old, now)?;
    let identities = old.block_parity_identities()?;
    let times = old.block_parity_times(now)?;
    // Keep the metrics returned by old.block_parity(now). Report the native
    // loader and physical tombstone counts separately below.
    let mut report = old.block_parity(now)?;
    drop(old);
    let started = Instant::now();
    let native = SqliteRouteStateStore::open(&descriptor)?;
    let migration_duration = started.elapsed();
    let started = Instant::now();
    let loaded = native.load_blocks(now)?;
    let native_load_duration = started.elapsed();
    let actual = canonical(loaded.blocks.iter().map(FenceRecheck::from).collect());
    let mut row_mismatches = 0;
    for key in before
        .rows
        .keys()
        .chain(actual.keys())
        .collect::<BTreeSet<_>>()
    {
        if before.rows.get(key) != actual.get(key) {
            row_mismatches += 1;
            if report.differences.len() < 100 {
                report.differences.push(format!(
                    "native effective row differs for {key:?}: SQL={:?}, native={:?}",
                    before.rows.get(key),
                    actual.get(key)
                ));
            }
        }
    }
    let evaluation_mismatches = domain::compare(
        &identities,
        &times,
        &before.rows,
        &loaded.blocks,
        &mut report,
    );
    let (stored_tombs,stored_active,record_only,generation_mismatches,observations_retired,remaining_authorizations,new_version)=native.with_reader_for_test(|conn| {
        let tombs=triples(conn,"SELECT kind,origin_key,evidence FROM block WHERE is_origin=1 AND cleared_at IS NOT NULL
            UNION SELECT kind,origin_key,evidence FROM retirement_tombstone")?;
        let mut active=kinds(std::iter::empty());
        for row in conn.prepare("SELECT kind,count(*) FROM block WHERE is_origin=1 AND cleared_at IS NULL GROUP BY kind")?.query_map([],|row|Ok((row.get::<_,String>(0)?,row.get::<_,usize>(1)?)))? {
            let(kind,count)=row?;active.insert(kind,count);
        }
        let record_only=conn.query_row("SELECT count(*) FROM block WHERE is_origin=0",[],|row|row.get::<_,usize>(0))?;
        let generation=conn.query_row("SELECT count(*) FROM block b JOIN fence_recheck r
            ON r.kind=b.kind AND r.origin_key=b.origin_key AND r.evidence=b.evidence
            WHERE b.is_origin=1 AND b.managed=1 AND b.cleared_by IS NOT 'removed' AND b.cleared_by IS NOT 'retired'
            AND b.claim_generation!=r.claim_generation",[],|row|row.get::<_,usize>(0))?;
        let mut retired=0;
        let mut authorized=0;
        for(kind,key,evidence) in &before.cleared_observations {
            retired+=conn.query_row("SELECT count(*) FROM retirement_tombstone WHERE kind=?1 AND origin_key=?2 AND evidence=?3",rusqlite::params![kind,key,evidence],|row|row.get::<_,usize>(0))?;
            authorized+=conn.query_row("SELECT count(*) FROM probe_authorization WHERE kind=?1 AND origin_key=?2 AND evidence=?3",rusqlite::params![kind,key,evidence],|row|row.get::<_,usize>(0))?;
        }
        let version=conn.query_row("SELECT MAX(version) FROM cortexkit_schema_version WHERE namespace='route_state'",[],|row|row.get::<_,u32>(0))?;
        Ok((tombs,active,record_only,generation,retired,authorized,version))
    })?;
    let missing: Vec<_> = before
        .tombstones
        .difference(&stored_tombs)
        .map(key_description)
        .collect();
    let extra: Vec<_> = stored_tombs
        .difference(&before.tombstones)
        .map(key_description)
        .collect();
    let expected_active = kinds(before.rows.values().map(|row| row.kind.as_str()));
    let loaded_active = kinds(loaded.blocks.iter().map(|row| row.kind.as_str()));
    if expected_active != stored_active || expected_active != loaded_active {
        report
            .differences
            .push("per-kind native active parity differs".into());
    }
    if !missing.is_empty() || !extra.is_empty() {
        report.differences.push(format!(
            "tombstone parity differs: {} missing, {} extra",
            missing.len(),
            extra.len()
        ));
    }
    if generation_mismatches != 0 {
        report.differences.push(format!(
            "{generation_mismatches} claimed generations differ"
        ));
    }
    if observations_retired != before.cleared_observations.len() || remaining_authorizations != 0 {
        report
            .differences
            .push("pre-fix healed observation normalization differs".into());
    }
    let metrics = json!({
        "fixed_now_ms":now,"source_schema_version":version,"migrated_schema_version":new_version,
        "migration_open_us":migration_duration.as_micros(),"native_loader_us":native_load_duration.as_micros(),
        "expected_active_per_kind":expected_active,"stored_active_per_kind":stored_active,"loaded_active_per_kind":loaded_active,
        "record_only_rows":record_only,"effective_row_mismatches":row_mismatches,"generation_mismatches":generation_mismatches,
        "expected_tombstones":before.tombstones.len(),"stored_tombstones_distinct":stored_tombs.len(),
        "expected_tombstones_per_kind":kinds(before.tombstones.iter().map(|key|key.0.as_str())),
        "stored_tombstones_per_kind":kinds(stored_tombs.iter().map(|key|key.0.as_str())),
        "loader_cleared_evidence":loaded.cleared_evidence.len(),"missing_tombstones":missing,"extra_tombstones":extra,
        "quarantined_metadata_rows_per_kind":before.quarantine_metadata,"quarantined_origins_per_kind":before.quarantine_origins,
        "quarantined_provider_future_deadline_rows":before.future_provider,"quarantined_provider_future_noncredential_status_changes":before.future_provider_changed_status,
        "evaluation_identities":report.identities,"evaluation_boundary_times":times.len(),"evaluation_combinations_per_identity":report.combinations_per_identity,
        "evaluation_comparisons":report.function_comparisons,"evaluation_mismatches":evaluation_mismatches,
        "old_healed_observation_origins":before.cleared_observations.len(),"old_observation_authorizations":before.old_observation_authorizations,
        "normalized_observation_retirements":observations_retired,"remaining_normalized_observation_authorizations":remaining_authorizations,
        "differences":&report.differences,
        "timing_scope":"reduced-copy migration open and block-only loader; not full history or 5.5GB database timing"
    });
    std::fs::write(
        path.with_file_name("block-storage-rehearsal.json"),
        serde_json::to_vec_pretty(&metrics)?,
    )?;
    println!("block_storage_rehearsal_json={metrics}");
    Ok(report)
}
