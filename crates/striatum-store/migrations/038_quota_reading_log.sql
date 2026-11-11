-- Migration 38 (namespace "route_state"): immutable quota readings sampled at the
-- local poll clock. Source time remains separate so repeated producer readings can
-- be retained without pretending that quota elapsed.

CREATE TABLE IF NOT EXISTS quota_reading_log (
  account_key TEXT NOT NULL,
  window_key TEXT NOT NULL,
  observed_at INTEGER NOT NULL,
  used_percent REAL NOT NULL,
  raw_used_percent REAL,
  resets_at INTEGER,
  window_minutes INTEGER,
  banked_credit_count INTEGER,
  fetched_at INTEGER,
  is_stale INTEGER NOT NULL CHECK (is_stale IN (0, 1)),
  stale_since TEXT,
  stale_class TEXT,
  PRIMARY KEY (account_key, window_key, observed_at),
  CHECK (is_stale = 1 OR (stale_since IS NULL AND stale_class IS NULL))
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS quota_reading_log_observed_idx
  ON quota_reading_log(observed_at);

CREATE TRIGGER IF NOT EXISTS quota_reading_log_reject_update
BEFORE UPDATE ON quota_reading_log
BEGIN
  SELECT RAISE(ABORT, 'quota_reading_log is append-only');
END;
