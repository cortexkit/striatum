//! Persistence for the routing/v1 role (migration 62): the binding of each v1
//! decision to the principal and consumer that selected it, the affinity
//! preference per `(principal, consumer, affinity)`, and the first body and
//! answer of every report and v1 admission, which make those ops idempotent.
//!
//! The decision itself is the row `route.select` writes into the decision
//! ledger; this module only keeps what v1 adds to it.

use rusqlite::{params, Connection, OptionalExtension};
use striatum_core::module_identity::alternate;
use striatum_core::{accounts::AllowedAccountClass, RouteStateResult};

use super::{err, SqliteRouteStateStore};

/// A failure report ends the affinity that still names its decision. The
/// partial index `routing_v1_affinity_by_decision` finds those rows by key;
/// the table's primary key leads with the affinity, not the decision.
pub(crate) const END_DECISION_AFFINITY_SQL: &str =
    "UPDATE routing_v1_affinity SET ended_at_ms=?2 WHERE decision_id=?1 AND ended_at_ms IS NULL";

const PANEL_READ_SQL: &str =
    "SELECT state_json FROM routing_v1_panel WHERE panel_id=?1 AND principal IN (?2, ?3)";
const AFFINITY_READ_SQL: &str = "SELECT decision_id FROM routing_v1_affinity
    WHERE principal IN (?1, ?4) AND consumer IN (?2, ?5) AND affinity = ?3
    AND ended_at_ms IS NULL ORDER BY updated_at_ms DESC, (principal=?6) DESC, (consumer=?7) DESC, rowid DESC LIMIT 1";
const ADMISSION_READ_SQL: &str = "SELECT body_json, answer_json FROM routing_v1_admission
    WHERE principal IN (?1, ?3) AND admission_id = ?2";
const BOUND_ACCOUNT_DECISION_SQL: &str = "SELECT d.decision_json
    FROM account_admission AS a
    LEFT JOIN account_decision AS d ON d.decision_id = a.decision_id
    WHERE a.model_decision_id = ?1 AND a.pin_id = ?2 AND a.admission_id = ?3
    LIMIT 2";

/// What a v1 decision is bound to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1Decision {
    pub decision_id: String,
    /// The route principal that selected it (`reserved:<module>`, ...).
    pub principal: String,
    pub consumer: String,
    pub allowed_account_class: AllowedAccountClass,
    pub project_id: Option<String>,
    pub provider: String,
    pub model: String,
    pub variant: Option<String>,
    /// The runner module the answer named.
    pub runner_module: String,
    pub affinity: Option<String>,
    pub created_at_ms: i64,
    pub expires_at_ms: i64,
}

/// The first body recorded under an idempotency key, and the answer it got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1Recorded {
    pub body_json: String,
    pub answer_json: String,
}

pub struct V1SelectionAdmission {
    pub principal: String,
    pub admission_id: String,
    pub answer_json: String,
}

impl SqliteRouteStateStore {
    pub fn v1_create_panel(
        &self,
        id: &str,
        principal: &str,
        state_json: &str,
        now_ms: i64,
    ) -> RouteStateResult<()> {
        self.store.with_conn(|c: &Connection| {
            c.execute("INSERT INTO routing_v1_panel(panel_id, principal, state_json, created_at_ms) VALUES (?1, ?2, ?3, ?4)",
                params![id, principal, state_json, now_ms]).map(|_| ())
        }).map_err(err)
    }

    pub fn v1_panel(&self, id: &str, principal: &str) -> RouteStateResult<Option<String>> {
        self.store
            .with_conn(|c: &Connection| {
                c.query_row(
                    PANEL_READ_SQL,
                    params![id, principal, alternate(principal).as_ref()],
                    |row| row.get(0),
                )
                .optional()
            })
            .map_err(err)
    }

    pub fn v1_update_panel(&self, id: &str, state_json: &str) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "UPDATE routing_v1_panel SET state_json=?2 WHERE panel_id=?1",
                    params![id, state_json],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    pub fn record_service_tier(
        &self,
        decision_id: &str,
        tier: &str,
        accounts: &[String],
    ) -> RouteStateResult<()> {
        let accounts = serde_json::to_string(accounts).map_err(err)?;
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "UPDATE model_route_decisions SET service_tier_chosen=?2,
                 service_tier_accounts_json=?3 WHERE id=?1",
                    params![decision_id, tier, accounts],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    /// Read on a pooled reader like [`Self::v1_decision`].
    pub fn decision_service_tier(
        &self,
        decision_id: &str,
    ) -> RouteStateResult<Option<(String, Vec<String>)>> {
        let row: Option<(String, String)> = self
            .store
            .with_read(|c: &Connection| {
                c.query_row(
                    "SELECT service_tier_chosen, service_tier_accounts_json
                 FROM model_route_decisions WHERE id=?1 AND service_tier_chosen IS NOT NULL",
                    params![decision_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
            })
            .map_err(err)?;
        row.map(|(tier, accounts)| {
            serde_json::from_str(&accounts)
                .map(|accounts| (tier, accounts))
                .map_err(err)
        })
        .transpose()
    }

    pub fn record_usage_service_tier(
        &self,
        usage_id: &str,
        applied: Option<&str>,
        served: Option<&str>,
        cost_usd: Option<f64>,
    ) -> RouteStateResult<()> {
        self.store
            .with_conn(|c: &Connection| {
                c.execute(
                    "UPDATE model_route_usage SET service_tier_applied=?2,
                 service_tier_served=?3, service_tier_price_status=?4,
                 service_tier_cost_usd=?5 WHERE id=?1",
                    params![
                        usage_id,
                        applied,
                        served,
                        if served.is_some() && cost_usd.is_none() {
                            Some("served_unpriced")
                        } else if served.is_some() {
                            Some("confirmed")
                        } else if applied.is_some() {
                            Some("unconfirmed_estimate")
                        } else {
                            None
                        },
                        cost_usd
                    ],
                )
                .map(|_| ())
            })
            .map_err(err)
    }

    /// Bind a decision and, when it carries an affinity key, make it the key's
    /// previous decision, in one transaction.
    pub fn v1_record_decision(&self, decision: &V1Decision) -> RouteStateResult<()> {
        let decision = decision.to_owned();
        self.account_command(crate::AccountCommandKind::Selection, move |tx| {
            tx.v1_record_decision(&decision)
        })
    }

    /// Persist the immutable selection answer with its decision binding.
    /// Account allocations deliberately have no selection snapshot.
    pub fn v1_record_selection(
        &self,
        decision: &V1Decision,
        selection_json: Option<&str>,
    ) -> RouteStateResult<()> {
        let decision = decision.to_owned();
        let selection_json = selection_json.map(str::to_owned);
        self.account_command(crate::AccountCommandKind::Selection, move |tx| {
            tx.v1_record_selection(&decision, selection_json.as_deref())
        })
    }

    /// Return selection-time facts, never a reconstruction from today's catalog.
    pub fn v1_selection_json(&self, decision_id: &str) -> RouteStateResult<Option<String>> {
        self.store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT selection_json FROM routing_v1_decision WHERE decision_id = ?1",
                    params![decision_id],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()
                .map(Option::flatten)
            })
            .map_err(err)
    }

    /// Latest admitted serving identity under a selection. Continuation offers
    /// carry previous_decision_id instead and do not move this binding.
    pub fn v1_selection_admission(
        &self,
        decision_id: &str,
    ) -> RouteStateResult<Option<V1SelectionAdmission>> {
        self.store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT principal, admission_id, answer_json FROM routing_v1_admission
                 WHERE json_extract(body_json, '$.decision_id') = ?1
                 ORDER BY created_at_ms DESC, rowid DESC LIMIT 1",
                    params![decision_id],
                    |row| {
                        Ok(V1SelectionAdmission {
                            principal: row.get(0)?,
                            admission_id: row.get(1)?,
                            answer_json: row.get(2)?,
                        })
                    },
                )
                .optional()
            })
            .map_err(err)
    }

    /// Explain a particular v1 admission, not another use of the same model
    /// decision or pin. Ambiguous matches do not establish provenance.
    pub fn v1_bound_account_decision(
        &self,
        model_decision_id: &str,
        pin_id: &str,
        admission_id: &str,
    ) -> RouteStateResult<Option<String>> {
        self.store
            .with_conn(|c: &Connection| {
                let mut stmt = c.prepare_cached(BOUND_ACCOUNT_DECISION_SQL)?;
                let mut rows = stmt.query(params![model_decision_id, pin_id, admission_id])?;
                let Some(row) = rows.next()? else {
                    return Ok(None);
                };
                let decision = row.get(0)?;
                Ok(if rows.next()?.is_none() {
                    decision
                } else {
                    None
                })
            })
            .map_err(err)
    }

    /// The binding of `decision_id`, whoever selected it. Callers check the
    /// principal or runner themselves so a refusal never says which it was.
    /// Read on a pooled reader: account requests call this before queuing
    /// their writer command, and the writer connection is held for whole
    /// batches.
    pub fn v1_decision(&self, decision_id: &str) -> RouteStateResult<Option<V1Decision>> {
        self.store
            .with_read(|c: &Connection| {
                c.query_row(
                    "SELECT decision_id, principal, consumer, project_id, provider, model, variant,
                            runner_module, affinity, created_at_ms, expires_at_ms, allowed_account_class
                     FROM routing_v1_decision WHERE decision_id = ?1",
                    params![decision_id],
                    |row| {
                        Ok(V1Decision {
                            decision_id: row.get(0)?,
                            principal: row.get(1)?,
                            consumer: row.get(2)?,
                            project_id: row.get(3)?,
                            provider: row.get(4)?,
                            model: row.get(5)?,
                            variant: row.get(6)?,
                            runner_module: row.get(7)?,
                            affinity: row.get(8)?,
                            created_at_ms: row.get(9)?,
                            expires_at_ms: row.get(10)?,
                            allowed_account_class: AllowedAccountClass::parse(&row.get::<_, String>(11)?)
                                .map_err(|error| rusqlite::Error::FromSqlConversionFailure(
                                    11, rusqlite::types::Type::Text, error.into()))?,
                        })
                    },
                )
                .optional()
            })
            .map_err(err)
    }

    /// The previous decision for a live route affinity key. A failure report
    /// ends that decision's preference, but never a newer decision's preference.
    pub fn v1_affinity_decision(
        &self,
        principal: &str,
        consumer: &str,
        affinity: &str,
    ) -> RouteStateResult<Option<V1Decision>> {
        let decision_id: Option<String> = self
            .store
            .with_conn(|c: &Connection| {
                c.query_row(
                    AFFINITY_READ_SQL,
                    params![
                        principal,
                        consumer,
                        affinity,
                        alternate(principal).as_ref(),
                        alternate(consumer).as_ref(),
                        striatum_core::module_identity::canonical(principal).as_ref(),
                        striatum_core::module_identity::canonical(consumer).as_ref()
                    ],
                    |row| row.get(0),
                )
                .optional()
            })
            .map_err(err)?;
        match decision_id {
            Some(id) => self.v1_decision(&id),
            None => Ok(None),
        }
    }

    /// The first report on `decision_id`, if one was recorded.
    pub fn v1_report(&self, decision_id: &str) -> RouteStateResult<Option<V1Recorded>> {
        self.store
            .with_conn(|c: &Connection| {
                c.query_row(
                    "SELECT body_json, answer_json FROM routing_v1_report WHERE decision_id = ?1",
                    params![decision_id],
                    |row| {
                        Ok(V1Recorded {
                            body_json: row.get(0)?,
                            answer_json: row.get(1)?,
                        })
                    },
                )
                .optional()
            })
            .map_err(err)
    }

    /// Record the first report on a decision. A failure ends route affinity
    /// only while it still names this decision; account affinity is separate.
    /// Returns the report
    /// that stands: this one, or one another caller recorded first.
    pub fn v1_record_report(
        &self,
        decision_id: &str,
        report_id: &str,
        recorded: &V1Recorded,
        failed: bool,
        now_ms: i64,
    ) -> RouteStateResult<V1Recorded> {
        let decision_id = decision_id.to_owned();
        let report_id = report_id.to_owned();
        let recorded = recorded.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.v1_record_report(&decision_id, &report_id, &recorded, failed, now_ms)
        })
    }

    /// The first v1 admission a runner made under `admission_id`, read on a
    /// pooled reader like [`Self::v1_decision`].
    pub fn v1_admission(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<Option<V1Recorded>> {
        self.store
            .with_read(|c: &Connection| {
                c.query_row(
                    ADMISSION_READ_SQL,
                    params![principal, admission_id, alternate(principal).as_ref()],
                    |row| {
                        Ok(V1Recorded {
                            body_json: row.get(0)?,
                            answer_json: row.get(1)?,
                        })
                    },
                )
                .optional()
            })
            .map_err(err)
    }

    /// Record a runner's first v1 admission under `admission_id`. Returns the
    /// admission that stands: this one, or one recorded first.
    pub fn v1_record_admission(
        &self,
        principal: &str,
        admission_id: &str,
        recorded: &V1Recorded,
        now_ms: i64,
    ) -> RouteStateResult<V1Recorded> {
        let principal = principal.to_owned();
        let admission_id = admission_id.to_owned();
        let recorded = recorded.to_owned();
        self.account_command(crate::AccountCommandKind::Maintenance, move |tx| {
            tx.v1_record_admission(&principal, &admission_id, &recorded, now_ms)
        })
    }
}

impl crate::AccountTx<'_> {
    /// Record the public replay answer in the same transaction as its grant.
    pub fn v1_record_admission(
        &self,
        principal: &str,
        admission_id: &str,
        recorded: &V1Recorded,
        now_ms: i64,
    ) -> RouteStateResult<V1Recorded> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::routing_v1::v1_record_admission");
        if alternate(principal) != principal {
            if let Some(first) = self
                .conn
                .query_row(
                    ADMISSION_READ_SQL,
                    params![principal, admission_id, alternate(principal).as_ref()],
                    |row| {
                        Ok(V1Recorded {
                            body_json: row.get(0)?,
                            answer_json: row.get(1)?,
                        })
                    },
                )
                .optional()
                .map_err(err)?
            {
                return Ok(first);
            }
        }
        self.conn
            .execute(
                "INSERT INTO routing_v1_admission
             (principal, admission_id, body_json, answer_json, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(principal, admission_id) DO NOTHING",
                params![
                    principal,
                    admission_id,
                    recorded.body_json,
                    recorded.answer_json,
                    now_ms
                ],
            )
            .map_err(err)?;
        self.conn
            .query_row(
                "SELECT body_json, answer_json FROM routing_v1_admission
             WHERE principal = ?1 AND admission_id = ?2",
                params![principal, admission_id],
                |row| {
                    Ok(V1Recorded {
                        body_json: row.get(0)?,
                        answer_json: row.get(1)?,
                    })
                },
            )
            .map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev_descriptor;

    fn store(label: &str) -> SqliteRouteStateStore {
        let dir = cortexkit_test_support::scratch_root().join(format!(
            "routing-v1-store-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        SqliteRouteStateStore::open(&dev_descriptor(
            "routing-v1-store-test",
            dir.join("store.db").to_string_lossy().into_owned(),
        ))
        .unwrap()
    }

    fn decision(id: &str, affinity: Option<&str>) -> V1Decision {
        V1Decision {
            decision_id: id.into(),
            principal: "reserved:consumer".into(),
            consumer: "summary".into(),
            allowed_account_class: AllowedAccountClass::NotPayPerToken,
            project_id: None,
            provider: "p".into(),
            model: "m".into(),
            variant: None,
            runner_module: "runner".into(),
            affinity: affinity.map(str::to_owned),
            created_at_ms: 1,
            expires_at_ms: 2,
        }
    }

    /// Account requests read their decision, its service tier and any earlier
    /// admission before queuing a writer command. Those reads must not wait
    /// for the writer connection, which the account writer holds for a whole
    /// batch (or a state reload).
    #[test]
    fn request_path_reads_do_not_wait_for_the_writer_connection() {
        let store = std::sync::Arc::new(store("pooled-reads"));
        store.v1_record_decision(&decision("held", None)).unwrap();
        let (holding_tx, holding) = std::sync::mpsc::channel();
        let (release_tx, release) = std::sync::mpsc::channel::<()>();
        let writer = {
            let store = store.clone();
            std::thread::spawn(move || {
                store
                    .store
                    .with_conn(|_| {
                        holding_tx.send(()).unwrap();
                        release.recv().ok();
                        Ok(())
                    })
                    .unwrap();
            })
        };
        holding.recv().unwrap();
        let (done_tx, done) = std::sync::mpsc::channel();
        let reader = {
            let store = store.clone();
            std::thread::spawn(move || {
                let decision = store.v1_decision("held").unwrap();
                let tier = store.decision_service_tier("held").unwrap();
                let admission = store.v1_admission("reserved:runner", "none").unwrap();
                done_tx
                    .send((decision.is_some(), tier.is_none(), admission.is_none()))
                    .unwrap();
            })
        };
        let answer = done.recv_timeout(std::time::Duration::from_secs(5));
        release_tx.send(()).unwrap();
        writer.join().unwrap();
        reader.join().unwrap();
        assert_eq!(
            answer.expect("the reads waited for the held writer connection"),
            (true, true, true)
        );
    }

    #[test]
    fn alias_principal_keyed_reads_keep_indexed_seeks() {
        let store = store("rename-plans");
        for (sql, index) in [
            (PANEL_READ_SQL, "sqlite_autoindex_routing_v1_panel_1"),
            (AFFINITY_READ_SQL, "sqlite_autoindex_routing_v1_affinity_1"),
            (
                ADMISSION_READ_SQL,
                "sqlite_autoindex_routing_v1_admission_1",
            ),
        ] {
            store
                .store
                .with_conn(|c| {
                    let mut stmt = c.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
                    let count = stmt.parameter_count();
                    let plan: Vec<String> = stmt
                        .query_map(
                            rusqlite::params_from_iter(
                                std::iter::repeat("reserved:prefrontal").take(count),
                            ),
                            |r| r.get(3),
                        )?
                        .collect::<rusqlite::Result<_>>()?;
                    assert!(
                        plan.iter()
                            .any(|s| s.contains("SEARCH") && s.contains(index)),
                        "{plan:?}"
                    );
                    assert!(!plan.iter().any(|s| s.starts_with("SCAN ")), "{plan:?}");
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn bound_account_explanation_uses_indexed_admission_and_decision_lookups() {
        let store = store("bound-explain-plan");
        store
            .store
            .with_conn(|c| {
                let mut stmt =
                    c.prepare(&format!("EXPLAIN QUERY PLAN {BOUND_ACCOUNT_DECISION_SQL}"))?;
                let plan: Vec<String> = stmt
                    .query_map(params!["selection", "pin", "admission"], |row| row.get(3))?
                    .collect::<rusqlite::Result<_>>()?;
                assert!(
                    plan.iter().any(|line| {
                        line.contains("SEARCH a USING INDEX")
                            && (line.contains("account_admission_model_decision")
                                || line.contains("account_admission_by_pin"))
                    }),
                    "{plan:?}"
                );
                assert!(
                    plan.iter().any(|line| line
                        .contains("SEARCH d USING INDEX sqlite_autoindex_account_decision_1")),
                    "{plan:?}"
                );
                assert!(
                    !plan.iter().any(|line| line.starts_with("SCAN ")),
                    "{plan:?}"
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn prefrontal_rename_decision_list_filter_reads_old_history() {
        use striatum_core::RouteStateStore;
        let store = store("rename-list");
        store
            .record_decision(&striatum_core::DecisionInput {
                target_agent: "prefrontal-core.mason".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            store
                .list_decisions(&crate::DecisionListFilter {
                    target_agent: Some("prefrontal.mason".into()),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn prefrontal_rename_panel_is_owned_across_the_cut() {
        let store = store("rename-panel");
        store
            .v1_create_panel("panel", "reserved:prefrontal-core", "{}", 1)
            .unwrap();
        assert_eq!(
            store
                .v1_panel("panel", "reserved:prefrontal")
                .unwrap()
                .as_deref(),
            Some("{}")
        );
        assert!(store.v1_panel("panel", "reserved:other").unwrap().is_none());
    }

    #[test]
    fn prefrontal_rename_affinity_reads_old_and_new_workload_and_newest_decision() {
        let store = store("rename-affinity");
        let mut old = decision("old", Some("same"));
        old.principal = "reserved:prefrontal-core".into();
        old.consumer = "prefrontal-core.mason".into();
        store.v1_record_decision(&old).unwrap();
        assert_eq!(
            store
                .v1_affinity_decision("reserved:prefrontal", "prefrontal.mason", "same")
                .unwrap()
                .unwrap()
                .decision_id,
            "old"
        );
        let mut new = old.clone();
        new.decision_id = "new".into();
        new.principal = "reserved:prefrontal".into();
        new.consumer = "prefrontal.mason".into();
        new.created_at_ms += 1;
        store.v1_record_decision(&new).unwrap();
        assert_eq!(
            store
                .v1_affinity_decision("reserved:prefrontal-core", "prefrontal-core.mason", "same")
                .unwrap()
                .unwrap()
                .decision_id,
            "new"
        );
        let raw: (i64, String, String) = store
            .store
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*),principal,consumer FROM routing_v1_affinity",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
            })
            .unwrap();
        assert_eq!(
            raw,
            (
                1,
                "reserved:prefrontal-core".into(),
                "prefrontal-core.mason".into()
            )
        );
        store
            .v1_record_report(
                "new",
                "failed",
                &V1Recorded {
                    body_json: "{}".into(),
                    answer_json: "{}".into(),
                },
                true,
                10,
            )
            .unwrap();
        assert!(store
            .v1_affinity_decision("reserved:prefrontal", "prefrontal.mason", "same")
            .unwrap()
            .is_none());
    }

    #[test]
    fn prefrontal_rename_parallel_affinities_choose_newest_then_old_on_ties() {
        for (new_at, chosen) in [(1, "reserved:prefrontal-core"), (2, "reserved:prefrontal")] {
            let store = store("rename-parallel-affinity");
            let mut old = decision("old", Some("same"));
            old.principal = "reserved:prefrontal-core".into();
            old.consumer = "prefrontal-core.mason".into();
            store.v1_record_decision(&old).unwrap();
            store.store.with_conn(|c| c.execute("INSERT INTO routing_v1_affinity(principal,consumer,affinity,decision_id,updated_at_ms) VALUES ('reserved:prefrontal','prefrontal.mason','same','old',?1)", [new_at])).unwrap();
            let mut new = old;
            new.principal = "reserved:prefrontal".into();
            new.consumer = "prefrontal.mason".into();
            new.decision_id = "new".into();
            new.created_at_ms = 3;
            store.v1_record_decision(&new).unwrap();
            let live: String = store
                .store
                .with_conn(|c| {
                    c.query_row(
                        "SELECT principal FROM routing_v1_affinity WHERE ended_at_ms IS NULL",
                        [],
                        |r| r.get(0),
                    )
                })
                .unwrap();
            assert_eq!(live, chosen);
            store
                .v1_record_report(
                    "new",
                    "failed",
                    &V1Recorded {
                        body_json: "{}".into(),
                        answer_json: "{}".into(),
                    },
                    true,
                    10,
                )
                .unwrap();
            assert!(store
                .v1_affinity_decision("reserved:prefrontal", "prefrontal.mason", "same")
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn prefrontal_rename_admission_replay_keeps_first_answer() {
        let store = store("rename-admission");
        let first = V1Recorded {
            body_json: "first".into(),
            answer_json: "answer".into(),
        };
        store
            .v1_record_admission("reserved:prefrontal-core", "same", &first, 1)
            .unwrap();
        assert_eq!(
            store.v1_admission("reserved:prefrontal", "same").unwrap(),
            Some(first.clone())
        );
        let second = V1Recorded {
            body_json: "changed".into(),
            answer_json: "changed".into(),
        };
        assert_eq!(
            store
                .v1_record_admission("reserved:prefrontal", "same", &second, 2)
                .unwrap(),
            first
        );
    }

    #[test]
    fn selection_snapshot_migrates_without_inventing_old_capacities_and_survives_reopen() {
        let path = cortexkit_test_support::scratch_root().join(format!(
            "routing-selection-migration-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let descriptor = dev_descriptor("selection-migration", path.to_string_lossy().into_owned());
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 76).unwrap();
        old.v1_record_decision(&decision("old", None)).unwrap();
        drop(old);
        let upgraded = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert!(upgraded.v1_decision("old").unwrap().is_some());
        assert_eq!(upgraded.v1_selection_json("old").unwrap(), None);
        let snapshot = r#"{"context_window":100000,"max_output_tokens":20000,"runner":{"module":"runner","harness":"program","auth":{"credential_id":"public-key-id"}}}"#;
        upgraded
            .v1_record_selection(&decision("new", Some("affinity")), Some(snapshot))
            .unwrap();
        upgraded
            .v1_record_decision(&decision("account-allocation", None))
            .unwrap();
        drop(upgraded);
        let reopened = SqliteRouteStateStore::open(&descriptor).unwrap();
        assert_eq!(
            reopened.v1_selection_json("new").unwrap().as_deref(),
            Some(snapshot)
        );
        assert_eq!(reopened.v1_selection_json("old").unwrap(), None);
        assert_eq!(
            reopened.v1_selection_json("account-allocation").unwrap(),
            None
        );
    }

    #[test]
    fn a_failure_ends_only_its_current_route_affinity() {
        let store = store("affinity");
        store
            .v1_record_decision(&decision("d1", Some("k")))
            .unwrap();
        store
            .v1_record_decision(&decision("d2", Some("k")))
            .unwrap();
        let previous = store
            .v1_affinity_decision("reserved:consumer", "summary", "k")
            .unwrap()
            .unwrap();
        assert_eq!(previous.decision_id, "d2");

        // A failure report on an older decision leaves the preference alone.
        let body = V1Recorded {
            body_json: "{}".into(),
            answer_json: "{}".into(),
        };
        store.v1_record_report("d1", "r1", &body, true, 5).unwrap();
        assert_eq!(
            store
                .v1_affinity_decision("reserved:consumer", "summary", "k")
                .unwrap()
                .unwrap()
                .decision_id,
            "d2"
        );

        // A failure report on the current decision ends route affinity.
        store.v1_record_report("d2", "r2", &body, true, 6).unwrap();
        assert!(store
            .v1_affinity_decision("reserved:consumer", "summary", "k")
            .unwrap()
            .is_none());

        // The next decision for the key starts the preference again.
        store
            .v1_record_decision(&decision("d3", Some("k")))
            .unwrap();
        assert_eq!(
            store
                .v1_affinity_decision("reserved:consumer", "summary", "k")
                .unwrap()
                .unwrap()
                .decision_id,
            "d3"
        );
        store.v1_record_report("d2", "r2", &body, true, 7).unwrap();
        store.v1_record_report("d3", "r3", &body, false, 8).unwrap();
        assert_eq!(
            store
                .v1_affinity_decision("reserved:consumer", "summary", "k")
                .unwrap()
                .unwrap()
                .decision_id,
            "d3"
        );
    }

    #[test]
    fn the_first_report_and_admission_stand() {
        let store = store("first-stands");
        store.v1_record_decision(&decision("d1", None)).unwrap();
        let first = V1Recorded {
            body_json: "first".into(),
            answer_json: "a1".into(),
        };
        let second = V1Recorded {
            body_json: "second".into(),
            answer_json: "a2".into(),
        };
        assert_eq!(
            store
                .v1_record_report("d1", "r1", &first, false, 1)
                .unwrap(),
            first
        );
        assert_eq!(
            store
                .v1_record_report("d1", "r2", &second, true, 2)
                .unwrap(),
            first
        );
        assert_eq!(store.v1_report("d1").unwrap(), Some(first.clone()));

        assert_eq!(
            store
                .v1_record_admission("reserved:runner", "adm", &first, 1)
                .unwrap(),
            first
        );
        assert_eq!(
            store
                .v1_record_admission("reserved:runner", "adm", &second, 2)
                .unwrap(),
            first
        );
        assert!(store
            .v1_admission("reserved:other", "adm")
            .unwrap()
            .is_none());
    }
}

impl crate::AccountTx<'_> {
    pub fn v1_record_decision(&self, decision: &V1Decision) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::routing_v1::v1_record_decision");
        self.v1_record_selection(decision, None)
    }

    pub fn v1_record_selection(
        &self,
        decision: &V1Decision,
        selection_json: Option<&str>,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::routing_v1::v1_record_selection");
        self.command_sql(|c: &Connection| {
                let tx = c;
                tx.execute(
                    "INSERT INTO routing_v1_decision
                       (decision_id, principal, consumer, project_id, provider, model, variant,
                         runner_module, affinity, created_at_ms, expires_at_ms, allowed_account_class)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    params![
                        decision.decision_id,
                        decision.principal,
                        decision.consumer,
                        decision.project_id,
                        decision.provider,
                        decision.model,
                        decision.variant,
                        decision.runner_module,
                        decision.affinity,
                        decision.created_at_ms,
                        decision.expires_at_ms,
                        decision.allowed_account_class.as_str(),
                    ],
                )?;
                if let Some(selection_json) = selection_json {
                    tx.execute("UPDATE routing_v1_decision SET selection_json=?2 WHERE decision_id=?1",
                        params![decision.decision_id, selection_json])?;
                }
                if let Some(affinity) = &decision.affinity {
                    // Reuse the newest matching affinity row across principal
                    // and consumer spellings. End the other matches before
                    // updating the chosen row so only one binding stays active.
                    let existing: Option<i64> = if alternate(&decision.principal) != decision.principal || alternate(&decision.consumer) != decision.consumer { tx.query_row(
                        "SELECT rowid FROM routing_v1_affinity WHERE principal IN (?1,?4) AND consumer IN (?2,?5) AND affinity=?3 ORDER BY updated_at_ms DESC, (principal=?6) DESC, (consumer=?7) DESC, rowid DESC LIMIT 1",
                        params![decision.principal, decision.consumer, affinity, alternate(&decision.principal).as_ref(), alternate(&decision.consumer).as_ref(), striatum_core::module_identity::canonical(&decision.principal).as_ref(), striatum_core::module_identity::canonical(&decision.consumer).as_ref()],
                        |row| row.get(0),
                    ).optional()? } else { None };
                    if let Some(rowid) = existing {
                        tx.execute("UPDATE routing_v1_affinity SET ended_at_ms=?6 WHERE principal IN (?1,?4) AND consumer IN (?2,?5) AND affinity=?3 AND rowid<>?7 AND ended_at_ms IS NULL", params![decision.principal, decision.consumer, affinity, alternate(&decision.principal).as_ref(), alternate(&decision.consumer).as_ref(), decision.created_at_ms, rowid])?;
                        tx.execute("UPDATE routing_v1_affinity SET decision_id=?2, updated_at_ms=?3, ended_at_ms=NULL WHERE rowid=?1", params![rowid, decision.decision_id, decision.created_at_ms])?;
                        return Ok(());
                    }
                    tx.execute(
                        "INSERT INTO routing_v1_affinity
                           (principal, consumer, affinity, decision_id, updated_at_ms, ended_at_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5, NULL)
                         ON CONFLICT(principal, consumer, affinity) DO UPDATE SET
                           decision_id = excluded.decision_id,
                           updated_at_ms = excluded.updated_at_ms,
                           ended_at_ms = NULL",
                        params![
                            decision.principal,
                            decision.consumer,
                            affinity,
                            decision.decision_id,
                            decision.created_at_ms,
                        ],
                    )?;
                }
                Ok(())
            })
            .map_err(err)
    }

    pub fn v1_record_report(
        &self,
        decision_id: &str,
        report_id: &str,
        recorded: &V1Recorded,
        failed: bool,
        now_ms: i64,
    ) -> RouteStateResult<V1Recorded> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry =
            crate::writer_entry_trace::enter("account_tx::routing_v1::v1_record_report");
        self.command_sql(|c: &Connection| {
            let tx = c;
            let inserted = tx.execute(
                "INSERT INTO routing_v1_report
                       (decision_id, report_id, body_json, answer_json, created_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(decision_id) DO NOTHING",
                params![
                    decision_id,
                    report_id,
                    recorded.body_json,
                    recorded.answer_json,
                    now_ms
                ],
            )?;
            if inserted == 1 && failed {
                tx.execute(END_DECISION_AFFINITY_SQL, params![decision_id, now_ms])?;
            }
            let standing = tx.query_row(
                "SELECT body_json, answer_json FROM routing_v1_report WHERE decision_id = ?1",
                params![decision_id],
                |row| {
                    Ok(V1Recorded {
                        body_json: row.get(0)?,
                        answer_json: row.get(1)?,
                    })
                },
            )?;

            Ok(standing)
        })
        .map_err(err)
    }
}
