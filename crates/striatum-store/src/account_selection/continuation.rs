use super::*;
use crate::V1Recorded;

impl AccountTx<'_> {
    /// Sign-in exclusions belong to the run, not to the global fence's clock.
    pub fn signed_out_credentials(
        &self,
        principal: &str,
        admission: &str,
        run: &str,
    ) -> RouteStateResult<Vec<String>> {
        let mut statement = self.conn.prepare(
            "SELECT DISTINCT json_extract(body_json,'$.exhausted.credential_id')
             FROM (SELECT observation_json AS body_json FROM account_observation
               WHERE principal=?1 AND admission_id=?2 AND bound=1 AND kind='signed_out')
             WHERE json_extract(body_json,'$.run_id')=?3",
        ).map_err(err)?;
        let rows = statement
            .query_map(params![principal, admission, run], |row| row.get(0))
            .map_err(err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(err)?;
        Ok(rows)
    }

    /// Remember an unusable vault state so a later usable listing lifts sign-in
    /// evidence even when the original active listing had the same record version.
    pub fn update_signed_out_fingerprint(
        &self,
        fence: &striatum_core::route_state_store::CredentialEntitlementFence,
        fingerprint: &str,
    ) -> RouteStateResult<()> {
        self.conn.execute(
            "UPDATE credential_entitlement_fences SET listing_fingerprint=?1
             WHERE provider_id=?2 AND credential_id=?3 AND code='signed_out'
             AND observed_at_ms=?4 AND listing_fingerprint IS ?5",
            params![fingerprint, fence.provider_id, fence.credential_id, fence.observed_at_ms, fence.listing_fingerprint],
        ).map_err(err)?;
        Ok(())
    }

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
        ).map_err(err)?;
        // Once a successor serves, competing offers from its predecessor cannot serve.
        let siblings: Vec<(String, String)> = self.conn.prepare(
            "SELECT c.principal,c.reservation_id FROM account_continue c JOIN account_continue used
             ON c.principal=used.principal AND c.admission_id=used.admission_id AND c.run_id=used.run_id
             AND c.previous_decision_id=used.previous_decision_id
             WHERE used.decision_id=?1 AND used.used_at_ms IS NOT NULL
             AND c.used_at_ms IS NULL AND c.released_at_ms IS NULL AND c.reservation_id IS NOT NULL",
        ).map_err(err)?.query_map([decision], |row| Ok((row.get(0)?,row.get(1)?)))
            .map_err(err)?.collect::<rusqlite::Result<_>>().map_err(err)?;
        for (principal, reservation) in siblings {
            self.release_continued_reservation(&principal, &reservation, now)?;
        }
        Ok(())
    }

    /// Observations keep the original admission identity even after the account moves.
    #[allow(clippy::too_many_arguments)]
    pub fn use_observed_continuation(
        &self,
        principal: &str,
        admission: &str,
        run: &str,
        account: &str,
        credential: Option<&str>,
        now: i64,
    ) -> RouteStateResult<bool> {
        let decision: Option<String> = self.conn.query_row(
            "SELECT c.decision_id FROM account_continue c
             JOIN account_admission original ON original.principal=c.principal AND original.admission_id=c.admission_id
             JOIN account_admission reservation ON reservation.principal=c.principal AND reservation.admission_id=c.reservation_id
             WHERE c.principal=?1 AND c.admission_id=?2 AND c.run_id=?3
             AND original.state='started' AND original.run_id=?3
             AND reservation.account_id=?4 AND (?5 IS NULL OR reservation.credential_id=?5)
             AND c.used_at_ms IS NULL AND c.released_at_ms IS NULL AND c.valid_until_ms>?6
             AND NOT EXISTS(SELECT 1 FROM account_continue successor
                 WHERE successor.principal=c.principal AND successor.admission_id=c.admission_id
                 AND successor.run_id=c.run_id AND successor.previous_decision_id=c.previous_decision_id
                 AND successor.used_at_ms IS NOT NULL)
             ORDER BY c.rowid DESC LIMIT 1",
            params![principal,admission,run,account,credential,now], |row| row.get(0),
        ).optional().map_err(err)?;
        let Some(decision) = decision else {
            return Ok(false);
        };
        self.use_continuation(&decision, now)?;
        Ok(true)
    }

    pub fn latest_used_continuation(
        &self,
        principal: &str,
        admission: &str,
    ) -> RouteStateResult<Option<AccountAdmissionRow>> {
        let reservation: Option<String> = self
            .conn
            .query_row(
                "SELECT reservation_id FROM account_continue WHERE principal=?1 AND admission_id=?2
             AND used_at_ms IS NOT NULL ORDER BY rowid DESC LIMIT 1",
                params![principal, admission],
                |row| row.get(0),
            )
            .optional()
            .map_err(err)?;
        reservation
            .map(|id| self.admission(principal, &id))
            .transpose()
            .map(Option::flatten)
    }

    fn release_continued_reservation(
        &self,
        principal: &str,
        reservation: &str,
        now: i64,
    ) -> RouteStateResult<()> {
        if let Some(row) = self.admission(principal, reservation)? {
            self.mark_admission_released(principal, reservation, now)?;
            // Sibling reservations can share a pin with the successor that started.
            let serving: bool = self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM account_admission WHERE pin_id=?1 AND state='started')",
                [&row.pin_id], |row| row.get(0),
            ).map_err(err)?;
            if !serving {
                self.end_lease(&row.pin_id, now)?;
            }
        }
        self.conn.execute(
            "UPDATE account_continue SET released_at_ms=?3 WHERE principal=?1 AND reservation_id=?2
             AND used_at_ms IS NULL AND released_at_ms IS NULL",
            params![principal,reservation,now],
        ).map(|_|()).map_err(err)
    }

    /// The original admission owns every reservation accumulated by the run.
    pub fn finish_continuations(
        &self,
        principal: &str,
        admission: &str,
        run: &str,
        now: i64,
    ) -> RouteStateResult<()> {
        let reservations: Vec<(String, bool)> = self
            .conn
            .prepare(
                "SELECT reservation_id,used_at_ms IS NOT NULL FROM account_continue
             WHERE principal=?1 AND admission_id=?2 AND run_id=?3 AND reservation_id IS NOT NULL",
            )
            .map_err(err)?
            .query_map(params![principal, admission, run], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(err)?
            .collect::<rusqlite::Result<_>>()
            .map_err(err)?;
        for (reservation, used) in reservations {
            if used {
                if let Some(row) = self.admission(principal, &reservation)? {
                    self.mark_admission_finished(principal, &reservation, now)?;
                    self.end_lease(&row.pin_id, now)?;
                }
            } else {
                self.release_continued_reservation(principal, &reservation, now)?;
            }
        }
        Ok(())
    }

    /// Keep an account-only offer under the run's original admission, not its reservation.
    pub fn bind_continued_account_decision(
        &self,
        decision: &str,
        admission: &str,
    ) -> RouteStateResult<()> {
        self.conn
            .execute(
                "UPDATE account_decision SET admission_id=?2 WHERE decision_id=?1",
                params![decision, admission],
            )
            .map(|_| ())
            .map_err(err)
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
            return Err(striatum_core::RouteStateStoreError(
                "previous selection is missing".into(),
            ));
        }
        self.conn.execute(
            "INSERT INTO routing_v1_decision (decision_id,principal,consumer,project_id,provider,model,variant,runner_module,affinity,created_at_ms,expires_at_ms,allowed_account_class) SELECT ?2,principal,consumer,project_id,provider,model,variant,runner_module,affinity,?3,?4,allowed_account_class FROM routing_v1_decision WHERE decision_id=?1",
            params![previous,decision,now,until],
        ).map(|_|()).map_err(err)
    }
}
