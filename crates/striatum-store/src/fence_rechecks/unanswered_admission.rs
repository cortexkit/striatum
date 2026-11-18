use super::*;
use crate::{block_state::ProbeAuthorization, AccountAdmissionRow};

impl AccountTx<'_> {
    /// A finished admission without a select decision has no later report to
    /// settle its test. If its observations supply no verdict, reopen only
    /// its exact non-quota claims; leave the refusal evidence and quota probe
    /// cadence in place.
    pub fn release_unanswered_admission_probes(
        &self,
        admission: &AccountAdmissionRow,
        now: i64,
    ) -> RouteStateResult<()> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::fence_rechecks::unanswered_admission::release_unanswered_admission_probes",
        );
        if !enabled(self.conn).map_err(err)? {
            return Ok(());
        }
        let owner = serde_json::json!([
            "account_admission",
            admission.principal,
            admission.admission_id
        ])
        .to_string();
        let claims = self.current_non_quota_admission_claims(&owner)?;
        if claims.is_empty() {
            return Ok(());
        }
        // The finish already removed the open row from the working state.
        // Look up the terminal row by primary key to check for a select decision.
        let decisionless_finish = self
            .admission_success_by_key(&admission.principal, &admission.admission_id)?
            .is_some_and(|row| row.row.state == "finished" && row.model_decision_id.is_none());
        if !decisionless_finish || self.admission_reported_verdict(admission)? {
            return Ok(());
        }
        for claim in claims {
            let changed = self.conn.execute(
                "UPDATE block SET next_recheck_at=?3, last_claimed_at=NULL, claim_owner=NULL,
                    claim_generation=claim_generation+1
                 WHERE kind=?1 AND origin_key=?2 AND is_origin=1 AND managed=1 AND cleared_at IS NULL AND quota=0
                   AND evidence=?4 AND evidence_at=?5 AND claim_generation=?6
                   AND (claim_owner IS NULL OR claim_owner=?7)",
                params![claim.kind, claim.origin_key, now, claim.evidence, claim.evidence_at,
                    claim.claim_generation, owner],
            ).map_err(err)?;
            crate::block_state::expect_changes(
                changed,
                1,
                format_args!(
                    "unanswered test of {} {} for {owner}",
                    claim.kind, claim.origin_key
                ),
            )
            .map_err(err)?;
            self.delete_authorization(&claim)?;
        }
        Ok(())
    }

    fn current_non_quota_admission_claims(
        &self,
        owner: &str,
    ) -> RouteStateResult<Vec<ProbeAuthorization>> {
        if let Some(blocks) = self.working_blocks()? {
            // A replay of a committed run_finished observation cannot retry
            // its release. Check both the test permission and its block.
            self.check_probe_inputs(&blocks, owner, true)?;
            return Ok(blocks
                .authorizations_of(owner)
                .into_iter()
                .filter(|row| {
                    blocks
                        .clock(&row.kind, &row.origin_key)
                        .is_some_and(|(clock, _)| clock.lifetime != LifetimeClass::Quota)
                        && test_is_current(&blocks, row, false)
                })
                .collect());
        }
        self.conn
            .prepare_cached(&format!(
            "SELECT a.kind,a.origin_key,a.evidence,a.evidence_at,a.claimed_at,a.claim_generation
             FROM probe_authorization a {} WHERE a.owner=?1 AND r.quota=0",
            current_test_join(2),
        ))
            .map_err(err)?
            .query_map(params![owner, false], |row| {
                Ok(ProbeAuthorization {
                    owner: owner.to_owned(),
                    kind: row.get(0)?,
                    origin_key: row.get(1)?,
                    evidence: row.get(2)?,
                    evidence_at: row.get(3)?,
                    claimed_at: row.get(4)?,
                    claim_generation: row.get(5)?,
                })
            })
            .map_err(err)?
            .collect::<rusqlite::Result<_>>()
            .map_err(err)
    }
}
