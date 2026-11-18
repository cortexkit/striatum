use super::*;

const FAILURE_KINDS: &str =
    "kind IN ('credential_refused', 'transient_backoff', 'account_limit_exhausted')";

fn verdict_sql(outcome_filter: &str) -> String {
    format!(
        "SELECT EXISTS(SELECT 1 FROM account_observation
        WHERE principal = ?1 AND admission_id = ?2 AND ({outcome_filter}))"
    )
}

fn admission_failure_sql() -> String {
    verdict_sql(FAILURE_KINDS)
}

fn admission_verdict_sql() -> String {
    verdict_sql(&format!(
        "{FAILURE_KINDS} OR CASE WHEN kind = 'attempt' AND bound = 1 AND json_valid(observation_json) THEN
            json_extract(observation_json, '$.attempt_outcome.status') IN ('served', 'served_empty')
            AND json_extract(observation_json, '$.served_account_id') = ?3
            AND json_extract(observation_json, '$.credential_id') = ?4 END"
    ))
}

impl AccountTx<'_> {
    /// A failed attempt must not be healed by its later run_finished event.
    pub fn admission_reported_failure(
        &self,
        principal: &str,
        admission_id: &str,
    ) -> RouteStateResult<bool> {
        #[cfg(any(test, feature = "test-support"))]
        let _writer_entry = crate::writer_entry_trace::enter(
            "account_tx::account_selection::admission_verdicts::admission_reported_failure",
        );
        self.conn
            .prepare_cached(&admission_failure_sql())
            .and_then(|mut stmt| stmt.query_row(params![principal, admission_id], |row| row.get(0)))
            .map_err(err)
    }

    /// An attempt with `served` or `served_empty` is a verdict even if it
    /// could not heal the refusal (for example, if it used another quota
    /// pool). Match the admitted account and credential as healing does.
    pub(crate) fn admission_reported_verdict(
        &self,
        admission: &AccountAdmissionRow,
    ) -> RouteStateResult<bool> {
        self.conn
            .prepare_cached(&admission_verdict_sql())
            .and_then(|mut stmt| {
                stmt.query_row(
                    params![
                        admission.principal,
                        admission.admission_id,
                        admission.account_id,
                        admission.credential_id
                    ],
                    |row| row.get(0),
                )
            })
            .map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_keyed_lookup(sql: &str, bindings: impl rusqlite::Params) {
        let scratch = cortexkit_test_support::ScratchDir::new("admission-verdict-plan");
        let path = scratch.path().join("store.db");
        let _store = SqliteRouteStateStore::open(&crate::dev_descriptor(
            "admission-verdict-plan",
            path.to_string_lossy(),
        ))
        .unwrap();
        let connection = Connection::open(path).unwrap();
        let details: Vec<String> = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map(bindings, |row| row.get(3))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert!(details.iter().any(|step| step.contains(
            "SEARCH account_observation USING INDEX account_observation_by_admission (principal=? AND admission_id=?)"
        )), "{details:?}");
        assert!(
            !details
                .iter()
                .any(|step| step.starts_with("SCAN account_observation")),
            "{details:?}"
        );
    }

    #[test]
    fn failure_lookup_searches_by_principal_and_admission() {
        assert_keyed_lookup(&admission_failure_sql(), ["principal", "admission"]);
    }

    #[test]
    fn verdict_lookup_searches_by_principal_and_admission() {
        assert_keyed_lookup(
            &admission_verdict_sql(),
            ["principal", "admission", "account", "credential"],
        );
    }
}
