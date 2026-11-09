use super::*;
use crate::V1Recorded;

impl AccountTx<'_> {
    pub fn has_expired_continuations(&self, now: i64) -> RouteStateResult<bool> {
        let exists=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='account_continue')", [], |row|row.get::<_,bool>(0)).map_err(err)?;
        if !exists {
            return Ok(false);
        }
        self.conn.query_row("SELECT EXISTS(SELECT 1 FROM account_continue WHERE used_at_ms IS NULL AND released_at_ms IS NULL AND valid_until_ms<=?1)",params![now],|row|row.get(0)).map_err(err)
    }

    pub fn continuation(
        &self,
        principal: &str,
        selection: &str,
    ) -> RouteStateResult<Option<V1Recorded>> {
        self.conn.query_row(
            "SELECT request_json, answer_json FROM account_continue WHERE principal=?1 AND selection_id=?2",
            params![principal, selection],
            |row| Ok(V1Recorded { body_json: row.get(0)?, answer_json: row.get(1)? }),
        ).optional().map_err(err)
    }

    /// The offer's reservation belongs to the original admission and run, not a new run.
    pub fn continued_reservation(
        &self,
        principal: &str,
        admission: &str,
        run: &str,
        decision: &str,
    ) -> RouteStateResult<Option<String>> {
        self.conn.query_row(
            "SELECT reservation_id FROM account_continue WHERE principal=?1 AND admission_id=?2 AND run_id=?3 AND decision_id=?4 AND released_at_ms IS NULL",
            params![principal, admission, run, decision], |row| row.get(0),
        ).optional().map_err(err)
    }

    pub fn continuation_has_used_successor(
        &self,
        principal: &str,
        admission: &str,
        run: &str,
        decision: &str,
    ) -> RouteStateResult<bool> {
        self.conn.query_row("SELECT EXISTS(SELECT 1 FROM account_continue WHERE principal=?1 AND admission_id=?2 AND run_id=?3 AND previous_decision_id=?4 AND used_at_ms IS NOT NULL)",
            params![principal,admission,run,decision],|row|row.get(0)).map_err(err)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_continuation(
        &self,
        principal: &str,
        selection: &str,
        record: &V1Recorded,
        run: &str,
        admission: &str,
        previous: &str,
        offer: Option<(&str, &str, i64)>,
    ) -> RouteStateResult<()> {
        self.conn.execute(
            "INSERT INTO account_continue (principal,selection_id,request_json,answer_json,run_id,admission_id,previous_decision_id,decision_id,reservation_id,valid_until_ms) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![principal,selection,record.body_json,record.answer_json,run,admission,previous,
                offer.map(|o|o.0),offer.map(|o|o.1),offer.map(|o|o.2)],
        ).map(|_|()).map_err(err)
    }

    /// Unused offers stop consuming admission demand at the deadline, even if never retried.
    pub fn release_expired_continuations(&self, now: i64) -> RouteStateResult<()> {
        if !self.conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='account_continue')", [], |row|row.get::<_,bool>(0)).map_err(err)? {
            return Ok(());
        }
        self.conn.execute(
            "UPDATE account_admission SET state='released', released_at=?1 WHERE state='reserved' AND (principal,admission_id) IN (SELECT principal,reservation_id FROM account_continue WHERE used_at_ms IS NULL AND released_at_ms IS NULL AND valid_until_ms<=?1)", params![now],
        ).map_err(err)?;
        self.conn.execute(
            "UPDATE account_continue SET released_at_ms=?1 WHERE used_at_ms IS NULL AND released_at_ms IS NULL AND valid_until_ms<=?1", params![now],
        ).map(|_|()).map_err(err)
    }

    pub fn use_continuation(&self, decision: &str, now: i64) -> RouteStateResult<()> {
        self.release_expired_continuations(now)?;
        self.conn.execute(
            "UPDATE account_admission SET state='started', started_at=?2, run_id=(SELECT run_id FROM account_continue WHERE decision_id=?1) WHERE state='reserved' AND (principal,admission_id) IN (SELECT principal,reservation_id FROM account_continue WHERE decision_id=?1 AND used_at_ms IS NULL AND released_at_ms IS NULL)", params![decision,now],
        ).map_err(err)?;
        self.conn.execute(
            "UPDATE account_continue SET used_at_ms=?2 WHERE decision_id=?1 AND used_at_ms IS NULL AND released_at_ms IS NULL", params![decision,now],
        ).map(|_|()).map_err(err)
    }

    /// Copy selection provenance and frozen pricing; only identity and lifecycle are new.
    pub fn link_continued_decision(
        &self,
        previous: &str,
        decision: &str,
        now: i64,
        until: i64,
    ) -> RouteStateResult<()> {
        // Copy every selection column so later pricing/profile additions follow switches too.
        let columns: Vec<String> = self
            .conn
            .prepare("PRAGMA table_info(model_route_decisions)")
            .map_err(err)?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(err)?
            .collect::<rusqlite::Result<_>>()
            .map_err(err)?;
        let values: Vec<String> = columns
            .iter()
            .map(|column| match column.as_str() {
                "id" => "?2".into(),
                "created_at" => "?3".into(),
                "previous_decision_id" => "?1".into(),
                "outcome" | "outcome_at" | "outcome_error" | "provider_identity_json" => {
                    "NULL".into()
                }
                _ => format!("\"{column}\""),
            })
            .collect();
        let names = columns
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(",");
        let copied=self.conn.execute(&format!("INSERT INTO model_route_decisions ({names}) SELECT {} FROM model_route_decisions WHERE id=?1",values.join(",")),params![previous,decision,now]).map_err(err)?;
        if copied != 1 {
            return Err(prefrontal_routing_core::RouteStateStoreError(
                "previous selection is missing".into(),
            ));
        }
        self.conn.execute(
            "INSERT INTO routing_v1_decision (decision_id,principal,consumer,project_id,provider,model,variant,runner_module,affinity,created_at_ms,expires_at_ms,allowed_account_class) SELECT ?2,principal,consumer,project_id,provider,model,variant,runner_module,affinity,?3,?4,allowed_account_class FROM routing_v1_decision WHERE decision_id=?1",
            params![previous,decision,now,until],
        ).map(|_|()).map_err(err)
    }
}
