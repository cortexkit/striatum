-- Read-only rehearsal query, valid both before and after migration 089.
-- Count attempts, not candidate admissions: ambiguous owner reports must not
-- refresh a runner's row even when a stored model_decision_id happens to match.
SELECT COUNT(*) AS left_out_attempts
FROM account_observation o
WHERE o.kind = 'attempt'
  AND (o.bound = 1 OR EXISTS (
    SELECT 1 FROM json_each(o.effects_json) e
    WHERE json_extract(e.value, '$.effect') = 'served_account_mismatch'
  ))
  AND NOT EXISTS (
    SELECT 1 FROM account_admission a
    WHERE a.principal = o.principal AND a.admission_id = o.admission_id
      AND (o.principal = 'reserved:' || o.runner
        OR json_extract(o.observation_json, '$.decision_id') IS NULL)
  );
