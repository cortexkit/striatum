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

/// The columns of one admission row that the derived indexes need. The
/// loader reads every admission once and the module derives the newest row
/// per conversation, the charged demand and the started rows on each
/// conversation's current pin from this single scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionScanRow {
    pub latest: LatestAdmissionRow,
    pub charged: bool,
    pub request_bytes: u64,
    pub started: bool,
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
    /// Every admission row, in no particular order.
    pub admissions: Vec<AdmissionScanRow>,
    pub affinities: Vec<AccountAffinityRow>,
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

/// A continuation row that carries an offer. The load refuses a row with only
/// some of its decision, reservation and deadline (migration 088 cleared any
/// that existed), so a decision means a whole offer here, as in the SQL paths.
const OFFER_ROW: &str = "c.decision_id IS NOT NULL";

/// A continuation row with some but not all of its offer columns.
const PARTIAL_OFFER_ROW: &str = "NOT ((decision_id IS NULL AND reservation_id IS NULL \
     AND valid_until_ms IS NULL) OR (decision_id IS NOT NULL AND reservation_id IS NOT NULL \
     AND valid_until_ms IS NOT NULL))";

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
        if continuations_exist(self.conn).map_err(err)? {
            let partial: Option<(i64, String, String)> = self
                .conn
                .query_row(
                    &format!(
                        "SELECT rowid, principal, selection_id FROM account_continue \
                         WHERE {PARTIAL_OFFER_ROW} ORDER BY rowid LIMIT 1"
                    ),
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(err)?;
            if let Some((rowid, principal, selection)) = partial {
                return Err(err(format!(
                    "account_continue row {rowid} (principal {principal}, selection \
                     {selection}) has only some of decision_id, reservation_id and \
                     valid_until_ms; routing writes all three or none, so the account \
                     state refuses to load it"
                )));
            }
        }
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
            let admissions = collect(
                conn,
                &format!(
                    "SELECT {LATEST_COLUMNS}, charged, request_bytes, started_at IS NOT NULL \
                     FROM account_admission"
                ),
                [],
                |row| {
                    Ok(AdmissionScanRow {
                        latest: latest_from_row(row)?,
                        charged: row.get::<_, i64>(12)? == 1,
                        request_bytes: to_u64(row.get(13)?),
                        started: row.get(14)?,
                    })
                },
            )?;
            let affinities = collect(
                conn,
                &format!("SELECT {AFFINITY_COLUMNS} FROM account_affinity"),
                [],
                affinity_from_row,
            )?;
            let (offers, chains) = if continuations_exist(conn)? {
                let offers = collect(
                    conn,
                    &format!(
                        "SELECT {CONTINUATION_COLUMNS} FROM account_continue c \
                         LEFT JOIN account_admission r \
                           ON r.principal = c.principal AND r.admission_id = c.reservation_id \
                         WHERE {OFFER_ROW} AND c.used_at_ms IS NULL \
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
                         WHERE {OFFER_ROW} ORDER BY c.rowid"
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
                admissions,
                affinities,
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
                 ORDER BY rowid DESC LIMIT 1",
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

static KEYED_EXCEPTION_READS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Count one keyed-exception read, whether the store or the module made it.
pub fn note_keyed_exception() {
    KEYED_EXCEPTION_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Keyed-exception reads made by this process so far.
pub fn keyed_exception_reads() -> u64 {
    KEYED_EXCEPTION_READS.load(std::sync::atomic::Ordering::Relaxed)
}

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

    /// End one conversation's lease at `at`, by primary key, only while the
    /// row still has the pin, generation and lease end the caller saw.
    /// Returns whether it did.
    pub fn set_affinity_lease_end(
        &self,
        conversation: &(String, String, String),
        pin: (&str, u64),
        previous: Option<i64>,
        at: i64,
    ) -> RouteStateResult<bool> {
        self.conn
            .execute(
                "UPDATE account_affinity SET lease_valid_until = ?4 \
                 WHERE principal = ?1 AND conversation_hash = ?2 AND provider = ?3 \
                   AND pin_id = ?5 AND pin_generation = ?6 AND lease_valid_until IS ?7",
                params![
                    conversation.0,
                    conversation.1,
                    conversation.2,
                    at,
                    pin.0,
                    pin.1 as i64,
                    previous
                ],
            )
            .map(|changed| changed == 1)
            .map_err(err)
    }

    /// A closed admission and its success-proof metadata in one primary-key
    /// lookup. Open admissions supply these fields from the module's cache.
    pub fn admission_success_by_key(
        &self,
        principal: &str,
        id: &str,
    ) -> RouteStateResult<Option<AdmissionStateRow>> {
        let scope = if crate::fence_rechecks::enabled(self.conn).map_err(err)? {
            "input_size_class,model_decision_id"
        } else {
            "NULL,NULL"
        };
        note_keyed_exception();
        self.conn
            .query_row(
                &format!(
                    "SELECT {ADMISSION_COLUMNS},{scope},rowid
            FROM account_admission {KEYED_EXCEPTION} WHERE principal=?1 AND admission_id=?2"
                ),
                params![principal, id],
                |row| {
                    Ok(AdmissionStateRow {
                        row: admission_from_row(row)?,
                        input_size_class: row.get::<_, Option<i64>>(25)?
                            .map(|size| u32::try_from(size).unwrap_or(u32::MAX)),
                        model_decision_id: row.get(26)?,
                        rowid: row.get(27)?,
                    })
                },
            )
            .optional()
            .map_err(err)
    }

    /// The admissions (at most two, enough to tell an ambiguous run) that a
    /// completed report's decision and run name, each with its probe size
    /// class. Keyed exception: the module's state holds only open admissions
    /// and a report usually arrives after its admission finished; the model
    /// decision id is indexed, so this never scans.
    pub fn admissions_for_run(
        &self,
        decision: &str,
        run: Option<&str>,
    ) -> RouteStateResult<Vec<(AccountAdmissionRow, Option<u32>)>> {
        note_keyed_exception();
        collect(
            self.conn,
            &format!(
                "SELECT {ADMISSION_COLUMNS}, input_size_class FROM account_admission \
                 {KEYED_EXCEPTION} WHERE model_decision_id = ?1 AND run_id = ?2 LIMIT 2"
            ),
            params![decision, run],
            |row| {
                Ok((
                    admission_from_row(row)?,
                    row.get::<_, Option<i64>>(25)?
                        .map(|size| u32::try_from(size).unwrap_or(u32::MAX)),
                ))
            },
        )
        .map_err(err)
    }

    /// Whether an admission is in one of `states`, without reading it: a
    /// primary-key UPDATE that leaves the row as it is and counts the match.
    /// The state uses it to confirm a transition it found not applicable, so
    /// the check writes nothing and reads no group data on the request path.
    pub fn admission_in_states(
        &self,
        principal: &str,
        admission_id: &str,
        states: &[String],
    ) -> RouteStateResult<bool> {
        let list = states.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let mut values: Vec<&dyn rusqlite::ToSql> = vec![&principal, &admission_id];
        values.extend(states.iter().map(|state| state as &dyn rusqlite::ToSql));
        self.conn
            .execute(
                &format!(
                    "UPDATE account_admission SET state = state \
                     WHERE principal = ?1 AND admission_id = ?2 AND state IN ({list})"
                ),
                values.as_slice(),
            )
            .map(|changed| changed > 0)
            .map_err(err)
    }

    /// Mark one continuation offer used or released, by primary key. Returns
    /// whether it was still an open offer.
    pub fn settle_continuation_offer(
        &self,
        principal: &str,
        selection: &str,
        used_at: Option<i64>,
        released_at: Option<i64>,
    ) -> RouteStateResult<bool> {
        self.conn
            .execute(
                "UPDATE account_continue SET used_at_ms = COALESCE(?3, used_at_ms), \
                   released_at_ms = COALESCE(?4, released_at_ms) \
                 WHERE principal = ?1 AND selection_id = ?2 \
                   AND used_at_ms IS NULL AND released_at_ms IS NULL",
                params![principal, selection, used_at, released_at],
            )
            .map(|changed| changed == 1)
            .map_err(err)
    }
}
