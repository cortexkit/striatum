//! Stored quota inputs, vault rosters, and the last announced route winners.
//! These checkpoints let the routing module resume from its last accepted
//! state after a restart without querying the selection history.

use super::*;

/// The last quota snapshot the routing module accepted, as it serialized it.
/// `observed_at` is the local poll clock (Unix milliseconds) at acceptance, so a
/// restarted module can report how old the reading it restored is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedQuotaSnapshot {
    pub windows_json: String,
    pub observed_at: i64,
}

/// One stored per-account quota reading. `entry_json` is the whole Insula entry
/// as published; `fetched_at` is that entry's producer date, kept verbatim so a
/// restart restores the original source time rather than a local stamp.
/// `received_at` and `tombstoned_at` are local Unix milliseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedAccountReading {
    pub provider: String,
    pub account_id: String,
    pub entry_json: String,
    pub fetched_at: Option<String>,
    pub received_at: i64,
    pub tombstoned_at: Option<i64>,
}

/// Per-provider Insula completeness, in local Unix milliseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedProviderCompleteness {
    pub provider: String,
    pub last_polled_at: i64,
    pub last_poll_complete: bool,
    pub last_complete_at: Option<i64>,
}

/// The last vault roster routing listed, serialized by the routing module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedVaultRoster {
    pub view: String,
    pub roster_json: String,
    pub fetched_at: i64,
}

impl SqliteRouteStateStore {
    /// The last announced route-winner ladder as the routing module serialized
    /// it, or `None` before the first ladder was ever recorded.
    pub fn route_winner_baseline(&self) -> RouteStateResult<Option<String>> {
        self.store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT ladder_json FROM route_winner_baseline WHERE singleton = 1",
                    [],
                    |row| row.get(0),
                )
                .optional()
            })
            .map_err(err)
    }

    /// Replace the stored route-winner ladder with `ladder_json`, recording the
    /// change that produced it.
    pub fn put_route_winner_baseline(
        &self,
        ladder_json: &str,
        trigger: &str,
        recorded_at: i64,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "INSERT INTO route_winner_baseline (singleton, ladder_json, trigger, recorded_at)
                     VALUES (1, ?1, ?2, ?3)
                     ON CONFLICT(singleton) DO UPDATE SET
                       ladder_json = excluded.ladder_json,
                       trigger = excluded.trigger,
                       recorded_at = excluded.recorded_at",
                    params![ladder_json, trigger, recorded_at],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    /// The last quota snapshot the routing module persisted, or `None` before the
    /// first accepted poll was ever recorded.
    pub fn quota_snapshot(&self) -> RouteStateResult<Option<PersistedQuotaSnapshot>> {
        self.store
            .with_read(|c: &Connection| {
                c.query_row(
                    "SELECT windows_json, observed_at FROM quota_snapshot WHERE singleton = 1",
                    [],
                    |row| {
                        Ok(PersistedQuotaSnapshot {
                            windows_json: row.get(0)?,
                            observed_at: row.get(1)?,
                        })
                    },
                )
                .optional()
            })
            .map_err(err)
    }

    /// Replace the stored quota snapshot with `windows_json`, accepted from a poll
    /// at `observed_at` (Unix milliseconds on the local poll clock).
    pub fn put_quota_snapshot(&self, windows_json: &str, observed_at: i64) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "INSERT INTO quota_snapshot (singleton, windows_json, observed_at)
                     VALUES (1, ?1, ?2)
                     ON CONFLICT(singleton) DO UPDATE SET
                       windows_json = excluded.windows_json,
                       observed_at = excluded.observed_at",
                    params![windows_json, observed_at],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    /// Every stored per-account quota reading, tombstoned ones included.
    pub fn account_quota_readings(&self) -> RouteStateResult<Vec<PersistedAccountReading>> {
        self.store
            .with_read(|c: &Connection| {
                let mut statement = c.prepare(
                    "SELECT provider, account_id, entry_json, fetched_at, received_at, tombstoned_at
                     FROM account_quota_reading ORDER BY provider, account_id",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok(PersistedAccountReading {
                        provider: row.get(0)?,
                        account_id: row.get(1)?,
                        entry_json: row.get(2)?,
                        fetched_at: row.get(3)?,
                        received_at: row.get(4)?,
                        tombstoned_at: row.get(5)?,
                    })
                })?;
                rows.collect()
            })
            .map_err(err)
    }

    /// Every stored provider completeness row.
    pub fn account_quota_completeness(
        &self,
    ) -> RouteStateResult<Vec<PersistedProviderCompleteness>> {
        self.store
            .with_read(|c: &Connection| {
                let mut statement = c.prepare(
                    "SELECT provider, last_polled_at, last_poll_complete, last_complete_at
                     FROM account_quota_completeness ORDER BY provider",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok(PersistedProviderCompleteness {
                        provider: row.get(0)?,
                        last_polled_at: row.get(1)?,
                        last_poll_complete: row.get::<_, i64>(2)? != 0,
                        last_complete_at: row.get(3)?,
                    })
                })?;
                rows.collect()
            })
            .map_err(err)
    }

    /// Upsert the readings and completeness rows one poll changed, in one
    /// transaction. Rows the poll did not name are left as they are: this never
    /// deletes a reading, so a partial poll cannot erase a stored sibling.
    pub fn put_account_quota_poll(
        &self,
        readings: &[PersistedAccountReading],
        completeness: &[PersistedProviderCompleteness],
    ) -> RouteStateResult<()> {
        self.apply_quota_delta(&[], readings, completeness)
            .map(|_| ())
    }

    /// Atomically write quota-reading log rows and the account poll from an event
    /// body or full `usage.get` reply. The quota consumer acknowledges an event
    /// only after both are durable. Poll-completeness flags and removed-account
    /// markers travel with the readings so partial polls cannot erase siblings
    /// on restart. Log pruning remains a separate background retention operation.
    pub fn apply_quota_delta(
        &self,
        rows: &[QuotaReadingLogRow],
        readings: &[PersistedAccountReading],
        completeness: &[PersistedProviderCompleteness],
    ) -> RouteStateResult<usize> {
        let rows = rows.to_vec();
        let readings = readings.to_vec();
        let completeness = completeness.to_vec();
        let split = self.history.is_some();
        self.account_command(AccountCommandKind::QuotaDelta, move |account| {
            let tx = account.conn;
            (|| -> rusqlite::Result<usize> {
                let inserted = history::append_quota(tx, split, &rows)?;
                for reading in &readings {
                    tx.execute(
                        "INSERT INTO account_quota_reading
                           (provider, account_id, entry_json, fetched_at, received_at, tombstoned_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                         ON CONFLICT(provider, account_id) DO UPDATE SET
                           entry_json = excluded.entry_json,
                           fetched_at = excluded.fetched_at,
                           received_at = excluded.received_at,
                           tombstoned_at = excluded.tombstoned_at",
                        params![
                            reading.provider,
                            reading.account_id,
                            reading.entry_json,
                            reading.fetched_at,
                            reading.received_at,
                            reading.tombstoned_at,
                        ],
                    )?;
                }
                for row in &completeness {
                    tx.execute(
                        "INSERT INTO account_quota_completeness
                           (provider, last_polled_at, last_poll_complete, last_complete_at)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(provider) DO UPDATE SET
                           last_polled_at = excluded.last_polled_at,
                           last_poll_complete = excluded.last_poll_complete,
                           last_complete_at = excluded.last_complete_at",
                        params![
                            row.provider,
                            row.last_polled_at,
                            i64::from(row.last_poll_complete),
                            row.last_complete_at,
                        ],
                    )?;
                }
                Ok(inserted)
            })().map_err(err)
        })
    }

    /// The last vault roster routing persisted, or `None` before the first listing.
    pub fn account_vault_roster(&self) -> RouteStateResult<Option<PersistedVaultRoster>> {
        self.store
            .with_read(|c: &Connection| {
                c.query_row(
                    "SELECT view, roster_json, fetched_at FROM account_vault_roster WHERE singleton = 1",
                    [],
                    |row| {
                        Ok(PersistedVaultRoster {
                            view: row.get(0)?,
                            roster_json: row.get(1)?,
                            fetched_at: row.get(2)?,
                        })
                    },
                )
                .optional()
            })
            .map_err(err)
    }

    /// Replace the stored vault roster.
    pub fn put_account_vault_roster(&self, roster: &PersistedVaultRoster) -> RouteStateResult<()> {
        let roster = roster.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.put_account_vault_roster(&roster)
        })
    }
}

impl crate::AccountTx<'_> {
    pub fn put_account_vault_roster(&self, roster: &PersistedVaultRoster) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::routing_checkpoints::put_account_vault_roster",
        );
        self.command_sql(|c: &Connection| {
            c.execute(
                "INSERT INTO account_vault_roster (singleton, view, roster_json, fetched_at)
                     VALUES (1, ?1, ?2, ?3)
                     ON CONFLICT(singleton) DO UPDATE SET
                       view = excluded.view,
                       roster_json = excluded.roster_json,
                       fetched_at = excluded.fetched_at",
                params![roster.view, roster.roster_json, roster.fetched_at],
            )
            .map(|_| ())
        })
        .map_err(err)
    }
}
