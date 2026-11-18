-- When a run finishes, routing checks that admission's own rows in
-- account_observation while holding the writer. Index both admission keys so
-- the lookup never walks a principal's whole observation history.
CREATE INDEX account_observation_by_admission
  ON account_observation (principal, admission_id)
  WHERE admission_id IS NOT NULL;
