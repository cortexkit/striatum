//! Raw rows the module's in-memory admission state is built from. The store
//! only reads and returns them; the module derives every index itself. The
//! loader queries run inside the writer's single deferred load transaction;
//! the keyed rechecks run inside a pruning command's write transaction.

use super::*;

/// One admission row that is still open (`reserved`, `started` or
/// `expired`), with the probe-scope columns and its SQLite rowid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionStateRow {
    pub row: AccountAdmissionRow,
    pub input_size_class: Option<u32>,
    pub model_decision_id: Option<String>,
    pub rowid: i64,
}

/// The newest admission of one conversation, ordered exactly like
/// `parent_used_account`: `committed_at DESC, rowid DESC`, finished and
/// released rows included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestAdmissionRow {
    pub principal: String,
    pub conversation_hash: String,
    pub provider: String,
    pub admission_id: String,
    pub committed_at: i64,
    pub rowid: i64,
    pub pin_id: String,
    pub pin_generation: u64,
    pub state: String,
    pub admission_valid_until: i64,
    pub lease_valid_until: i64,
    pub account_id: String,
}

/// One charged, unreleased admission's demand, in any other state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemandStateRow {
    pub provider: String,
    pub account_id: String,
    pub committed_at: i64,
    pub principal: String,
    pub admission_id: String,
    pub state: String,
    pub request_bytes: u64,
}

/// Started admissions on a conversation's current affinity pin and
/// generation, finished ones included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinStartedRow {
    pub principal: String,
    pub conversation_hash: String,
    pub provider: String,
    pub started: u64,
}

/// Identity of the admission a continuation offer reserved, read from that
/// admission row in whatever state it is in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservationRef {
    pub pin_id: String,
    pub account_id: String,
    pub credential_id: String,
}

/// One continuation row that carries an offer (decision, reservation and
/// deadline), without its request and answer bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuationStateRow {
    pub rowid: i64,
    pub principal: String,
    pub selection_id: String,
    pub run_id: String,
    pub admission_id: String,
    pub previous_decision_id: String,
    pub decision_id: String,
    pub reservation_id: String,
    pub valid_until_ms: i64,
    pub used_at_ms: Option<i64>,
    pub released_at_ms: Option<i64>,
    pub reservation: Option<ReservationRef>,
}

/// An admission row a retention pass deleted, with the fields the derived
/// indexes are keyed by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrunedAdmission {
    pub rowid: i64,
    pub principal: String,
    pub admission_id: String,
    pub conversation_hash: String,
    pub provider: String,
    pub account_id: String,
    pub pin_id: String,
    pub pin_generation: u64,
    pub state: String,
    pub charged: bool,
    pub committed_at: i64,
    pub started: bool,
}

/// A continuation row a retention pass deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrunedContinuation {
    pub rowid: i64,
    pub principal: String,
    pub selection_id: String,
    pub admission_id: String,
    pub run_id: String,
    pub previous_decision_id: String,
    pub decision_id: Option<String>,
    pub used: bool,
}

/// Everything the admission group's state loads, in loader order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdmissionGroupRows {
    pub open: Vec<AdmissionStateRow>,
    pub latest: Vec<LatestAdmissionRow>,
    pub demand: Vec<DemandStateRow>,
    pub affinities: Vec<AccountAffinityRow>,
    pub pin_started: Vec<PinStartedRow>,
    /// Offers that are neither used nor released.
    pub offers: Vec<ContinuationStateRow>,
    /// Every offer row whose original admission is still open, in rowid order.
    pub chains: Vec<ContinuationStateRow>,
}

const OPEN_STATES: &str = "('reserved', 'started', 'expired')";

const LATEST_COLUMNS: &str = "principal, conversation_hash, provider, admission_id, committed_at, \
     rowid, pin_id, pin_generation, state, admission_valid_until, lease_valid_until, account_id";

fn latest_from_row(row: &Row<'_>) -> rusqlite::Result<LatestAdmissionRow> {
    Ok(LatestAdmissionRow {
        principal: row.get(0)?,
        conversation_hash: row.get(1)?,
        provider: row.get(2)?,
        admission_id: row.get(3)?,
        committed_at: row.get(4)?,
        rowid: row.get(5)?,
        pin_id: row.get(6)?,
        pin_generation: to_u64(row.get(7)?),
        state: row.get(8)?,
        admission_valid_until: row.get(9)?,
        lease_valid_until: row.get(10)?,
        account_id: row.get(11)?,
    })
}

const CONTINUATION_COLUMNS: &str = "c.rowid, c.principal, c.selection_id, c.run_id, \
     c.admission_id, c.previous_decision_id, c.decision_id, c.reservation_id, c.valid_until_ms, \
     c.used_at_ms, c.released_at_ms, r.pin_id, r.account_id, r.credential_id";

fn continuation_from_row(row: &Row<'_>) -> rusqlite::Result<ContinuationStateRow> {
    let pin_id: Option<String> = row.get(11)?;
    Ok(ContinuationStateRow {
        rowid: row.get(0)?,
        principal: row.get(1)?,
        selection_id: row.get(2)?,
        run_id: row.get(3)?,
        admission_id: row.get(4)?,
        previous_decision_id: row.get(5)?,
        decision_id: row.get(6)?,
        reservation_id: row.get(7)?,
        valid_until_ms: row.get(8)?,
        used_at_ms: row.get(9)?,
        released_at_ms: row.get(10)?,
        reservation: match pin_id {
            Some(pin_id) => Some(ReservationRef {
                pin_id,
                account_id: row.get(12)?,
                credential_id: row.get(13)?,
            }),
            None => None,
        },
    })
}

fn continuations_exist(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='account_continue')",
        [],
        |row| row.get(0),
    )
}

fn collect<T>(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
    map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<Vec<T>> {
    let mut statement = conn.prepare(sql)?;
    let rows = statement.query_map(params, map)?;
    rows.collect()
}

impl AccountTx<'_> {
    /// Rowid of the row the previous INSERT on this connection created. The
    /// admission and continuation indexes order by it exactly as SQL does.
    pub fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }

    /// Load the admission group's rows for the module's state. Must run in
    /// the writer's read-only load transaction.
    pub fn load_admission_group(&self) -> RouteStateResult<AdmissionGroupRows> {
        (|| -> rusqlite::Result<AdmissionGroupRows> {
            let conn = self.conn;
            // The probe-scope columns arrive with the fence-recheck schema.
            let scope = if crate::fence_rechecks::enabled(conn)? {
                "input_size_class, model_decision_id"
            } else {
                "NULL, NULL"
            };
            let open = collect(
                conn,
                &format!(
                    "SELECT {ADMISSION_COLUMNS}, {scope}, rowid FROM account_admission \
                     WHERE state IN {OPEN_STATES} ORDER BY rowid"
                ),
                [],
                |row| {
                    Ok(AdmissionStateRow {
                        row: admission_from_row(row)?,
                        input_size_class: row
                            .get::<_, Option<i64>>(25)?
                            .map(|size| u32::try_from(size).unwrap_or(u32::MAX)),
                        model_decision_id: row.get(26)?,
                        rowid: row.get(27)?,
                    })
                },
            )?;
            let latest = collect(
                conn,
                &format!(
                    "SELECT {LATEST_COLUMNS} FROM (SELECT {LATEST_COLUMNS}, ROW_NUMBER() OVER ( \
                       PARTITION BY principal, conversation_hash, provider \
                       ORDER BY committed_at DESC, rowid DESC) AS position \
                     FROM account_admission) WHERE position = 1"
                ),
                [],
                latest_from_row,
            )?;
            let demand = collect(
                conn,
                "SELECT provider, account_id, committed_at, principal, admission_id, state, \
                   request_bytes FROM account_admission \
                 WHERE charged = 1 AND state != 'released' \
                 ORDER BY provider, account_id, committed_at, principal, admission_id",
                [],
                |row| {
                    Ok(DemandStateRow {
                        provider: row.get(0)?,
                        account_id: row.get(1)?,
                        committed_at: row.get(2)?,
                        principal: row.get(3)?,
                        admission_id: row.get(4)?,
                        state: row.get(5)?,
                        request_bytes: to_u64(row.get(6)?),
                    })
                },
            )?;
            let affinities = collect(
                conn,
                &format!("SELECT {AFFINITY_COLUMNS} FROM account_affinity"),
                [],
                affinity_from_row,
            )?;
            let pin_started = collect(
                conn,
                "SELECT f.principal, f.conversation_hash, f.provider, COUNT(*) \
                 FROM account_affinity f JOIN account_admission a \
                   ON a.principal = f.principal AND a.conversation_hash = f.conversation_hash \
                  AND a.provider = f.provider AND a.pin_id = f.pin_id \
                  AND a.pin_generation = f.pin_generation AND a.started_at IS NOT NULL \
                 GROUP BY f.principal, f.conversation_hash, f.provider",
                [],
                |row| {
                    Ok(PinStartedRow {
                        principal: row.get(0)?,
                        conversation_hash: row.get(1)?,
                        provider: row.get(2)?,
                        started: to_u64(row.get(3)?),
                    })
                },
            )?;
            let (offers, chains) = if continuations_exist(conn)? {
                let offers = collect(
                    conn,
                    &format!(
                        "SELECT {CONTINUATION_COLUMNS} FROM account_continue c \
                         LEFT JOIN account_admission r \
                           ON r.principal = c.principal AND r.admission_id = c.reservation_id \
                         WHERE c.decision_id IS NOT NULL AND c.used_at_ms IS NULL \
                           AND c.released_at_ms IS NULL ORDER BY c.rowid"
                    ),
                    [],
                    continuation_from_row,
                )?;
                let chains = collect(
                    conn,
                    &format!(
                        "SELECT {CONTINUATION_COLUMNS} FROM account_continue c \
                         JOIN account_admission o \
                           ON o.principal = c.principal AND o.admission_id = c.admission_id \
                          AND o.state IN {OPEN_STATES} \
                         LEFT JOIN account_admission r \
                           ON r.principal = c.principal AND r.admission_id = c.reservation_id \
                         WHERE c.decision_id IS NOT NULL ORDER BY c.rowid"
                    ),
                    [],
                    continuation_from_row,
                )?;
                (offers, chains)
            } else {
                (Vec::new(), Vec::new())
            };
            Ok(AdmissionGroupRows {
                open,
                latest,
                demand,
                affinities,
                pin_started,
                offers,
                chains,
            })
        })()
        .map_err(err)
    }

    /// Maintenance recheck after retention deleted a conversation's newest
    /// admission: the next newest row, by the same order as the loader.
    pub fn latest_admission_for_conversation(
        &self,
        principal: &str,
        conversation_hash: &str,
        provider: &str,
    ) -> RouteStateResult<Option<LatestAdmissionRow>> {
        self.conn
            .query_row(
                &format!(
                    "SELECT {LATEST_COLUMNS} FROM account_admission \
                     WHERE principal = ?1 AND conversation_hash = ?2 AND provider = ?3 \
                     ORDER BY committed_at DESC, rowid DESC LIMIT 1"
                ),
                params![principal, conversation_hash, provider],
                latest_from_row,
            )
            .optional()
            .map_err(err)
    }

    /// Maintenance recheck after retention deleted an original admission's
    /// newest used continuation: the next newest used row, as
    /// `latest_used_continuation` orders them.
    pub fn latest_used_continuation_row(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Option<(i64, String)>> {
        self.conn
            .query_row(
                "SELECT rowid, reservation_id FROM account_continue \
                 WHERE principal = ?1 AND admission_id = ?2 AND used_at_ms IS NOT NULL \
                   AND decision_id IS NOT NULL ORDER BY rowid DESC LIMIT 1",
                params![principal, admission_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(err)
    }
}

/// Marks the SQL text of a keyed-exception read. The module serves the
/// admission group from memory, which holds open admissions and open
/// continuation offers only; finished and released admissions and every
/// continuation's request and answer bodies stay in SQL. A request that names
/// one of those reads it by primary key, never by scan, and these reads are
/// counted apart from ordinary ones. The marker lets a statement trace tell
/// them apart.
pub const KEYED_EXCEPTION: &str = "/* keyed exception */";

impl AccountTx<'_> {
    /// Keyed exception: one admission by primary key, for a caller whose
    /// memory state does not hold it because it is closed or absent.
    pub fn admission_by_key(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Option<AccountAdmissionRow>> {
        self.conn
            .query_row(
                &format!(
                    "SELECT {ADMISSION_COLUMNS} FROM account_admission {KEYED_EXCEPTION} \
                     WHERE principal = ?1 AND admission_id = ?2"
                ),
                params![principal, admission_id],
                admission_from_row,
            )
            .optional()
            .map_err(err)
    }

    /// Keyed exception: a continuation's stored request and answer, which the
    /// memory state never holds.
    pub fn continuation_by_key(
        &self,
        principal: &str,
        selection: &str,
    ) -> RouteStateResult<Option<crate::V1Recorded>> {
        self.conn
            .query_row(
                &format!(
                    "SELECT request_json, answer_json FROM account_continue {KEYED_EXCEPTION} \
                     WHERE principal = ?1 AND selection_id = ?2"
                ),
                params![principal, selection],
                |row| {
                    Ok(crate::V1Recorded {
                        body_json: row.get(0)?,
                        answer_json: row.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(err)
    }

    /// Write the lifecycle columns of one admission exactly as the module's
    /// state computed them. Keyed by primary key and guarded by the state the
    /// row had, so a row the state misjudged is left alone and reported.
    pub fn set_admission_lifecycle(
        &self,
        row: &AccountAdmissionRow,
        from_state: &str,
    ) -> RouteStateResult<bool> {
        self.conn
            .execute(
                "UPDATE account_admission SET state = ?3, run_id = ?4, started_at = ?5, \
                   started_late = ?6, released_at = ?7, finished_at = ?8 \
                 WHERE principal = ?1 AND admission_id = ?2 AND state = ?9",
                params![
                    row.principal,
                    row.admission_id,
                    row.state,
                    row.run_id,
                    row.started_at,
                    i64::from(row.started_late),
                    row.released_at,
                    row.finished_at,
                    from_state,
                ],
            )
            .map(|changed| changed == 1)
            .map_err(err)
    }

    /// End one conversation's lease at `at`, by primary key.
    pub fn set_affinity_lease_end(
        &self,
        principal: &str,
        conversation_hash: &str,
        provider: &str,
        at: i64,
    ) -> RouteStateResult<()> {
        self.conn
            .execute(
                "UPDATE account_affinity SET lease_valid_until = ?4 \
                 WHERE principal = ?1 AND conversation_hash = ?2 AND provider = ?3",
                params![principal, conversation_hash, provider, at],
            )
            .map(|_| ())
            .map_err(err)
    }

    /// Mark one continuation offer used or released, by primary key.
    pub fn settle_continuation_offer(
        &self,
        principal: &str,
        selection: &str,
        used_at: Option<i64>,
        released_at: Option<i64>,
    ) -> RouteStateResult<()> {
        self.conn
            .execute(
                "UPDATE account_continue SET used_at_ms = COALESCE(?3, used_at_ms), \
                   released_at_ms = COALESCE(?4, released_at_ms) \
                 WHERE principal = ?1 AND selection_id = ?2",
                params![principal, selection, used_at, released_at],
            )
            .map(|_| ())
            .map_err(err)
    }
}
