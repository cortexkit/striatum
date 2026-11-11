-- Migration 068: key the admission demand index by account, then commit time.
-- Demand is read per account. When an account's quota reading bounds what
-- the allocator charges, the read starts after that reading less the
-- reflection lag; otherwise (no reading, paid credits, a counting strategy)
-- the account's whole history is read. Putting account_id ahead of
-- committed_at lets each read be one covering range search in commit order;
-- with the old (provider, committed_at, ...) order the floor could only skip
-- rows older than the earliest floor of any account.
DROP INDEX IF EXISTS account_admission_demand;
CREATE INDEX account_admission_demand
  ON account_admission (provider, account_id, committed_at, principal, admission_id, state, request_bytes)
  WHERE charged = 1 AND state != 'released';
