-- Migration 52: retire account-selection shadow mode.
--
-- Shadow mode re-ran the allocator for nobody (on a timer, on every
-- observation and on refused requests) and recorded each result as an
-- `account_decision` row with mode 'shadow'. Routing no longer writes or reads
-- those rows.
--
-- This migration does not delete them. A long-running install holds over a
-- million shadow rows (about 4 GB of decision JSON): one DELETE of all of them
-- took about 26 seconds on a copy of such a store, all of it inside the
-- module's startup, before routing can answer anyone. The routing retention
-- pass deletes them instead, 256 rows per short transaction, starting one
-- minute after boot. Reclaiming the file space afterwards is an operator
-- VACUUM.
--
-- The `mode` CHECK constraint keeps allowing 'shadow': changing it means
-- rebuilding the table, and nothing writes that value any more.

-- Only the shadow timer's heartbeat ("the newest timer row for this provider
-- and family") read this index; it is otherwise maintained on every insert.
DROP INDEX IF EXISTS account_decision_latest;

-- Served history is now pruned too (see the store's retention.rs). Each of
-- these lets one bounded retention delete walk an index range instead of the
-- whole table, and `account_admission_by_pin` also serves the foreign-key
-- check SQLite runs on account_admission when a pin row is deleted.
CREATE INDEX IF NOT EXISTS account_admission_by_committed
  ON account_admission (committed_at);
CREATE INDEX IF NOT EXISTS account_admission_by_pin
  ON account_admission (pin_id);
CREATE INDEX IF NOT EXISTS account_affinity_by_expiry
  ON account_affinity (expires_at);
CREATE INDEX IF NOT EXISTS account_hold_by_created
  ON account_hold (created_at);
