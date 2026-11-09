-- Migration 046: partial covering index for routing demand queries.
-- Eliminates table lookup and temporary b-tree sorts for non-released admissions.
DROP INDEX IF EXISTS account_admission_demand;
CREATE INDEX account_admission_demand
  ON account_admission (provider, committed_at, principal, admission_id, account_id, state, request_bytes)
  WHERE charged = 1 AND state != 'released';
