-- A finish can check the admission's receipts while holding the writer. Use
-- both admission keys so the lookup never walks a principal's whole history.
CREATE INDEX account_observation_by_admission
  ON account_observation (principal, admission_id)
  WHERE admission_id IS NOT NULL;
