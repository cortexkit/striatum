ALTER TABLE account_admission ADD COLUMN last_observed_at INTEGER;

-- Only stored identity can prove which principal's admission an attempt found.
-- A decision id supplied by another principal is not proof of an owner lookup:
-- legacy callers could supply the same decision id without making that lookup.
WITH tied AS MATERIALIZED (
  SELECT a.principal, a.admission_id, MAX(o.received_at) AS at
  FROM account_observation o
  JOIN account_admission a
    ON a.principal = o.principal AND a.admission_id = o.admission_id
  WHERE o.kind = 'attempt'
    AND (o.bound = 1 OR EXISTS (
      SELECT 1 FROM json_each(o.effects_json) e
      WHERE json_extract(e.value, '$.effect') = 'served_account_mismatch'
    ))
    AND (o.principal = 'reserved:' || o.runner
      OR json_extract(o.observation_json, '$.decision_id') IS NULL)
  GROUP BY a.principal, a.admission_id
), reached AS (
  SELECT principal, admission_id, at FROM tied
  UNION ALL
  SELECT c.principal, c.reservation_id, t.at
  FROM tied t JOIN account_continue c
    ON c.principal = t.principal AND c.admission_id = t.admission_id
  WHERE c.used_at_ms IS NOT NULL
), latest AS (
  SELECT principal, admission_id, MAX(at) AS at FROM reached
  GROUP BY principal, admission_id
)
UPDATE account_admission AS a
SET last_observed_at = (
  SELECT MAX(at) FROM (
    SELECT a.started_at AS at
    UNION ALL
    SELECT l.at FROM latest l
    WHERE l.principal = a.principal AND l.admission_id = a.admission_id
  )
)
WHERE a.state = 'started';
