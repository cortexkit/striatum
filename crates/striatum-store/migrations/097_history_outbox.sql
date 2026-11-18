-- Only pending rows live here. History is committed separately; a pending row
-- is acknowledged on the state writer only after that commit succeeds.
CREATE TABLE IF NOT EXISTS pending_account_hold (
  decision_id TEXT PRIMARY KEY NOT NULL,
  principal TEXT NOT NULL,
  conversation_hash TEXT NOT NULL,
  provider TEXT NOT NULL,
  pin_id TEXT NOT NULL,
  pin_generation INTEGER NOT NULL,
  account_id TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  basis TEXT NOT NULL,
  reset_at INTEGER NOT NULL,
  retry_after_s INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS pending_account_hold_by_pin ON pending_account_hold(pin_id);
CREATE INDEX IF NOT EXISTS pending_account_hold_by_created ON pending_account_hold(created_at);

CREATE TABLE IF NOT EXISTS pending_quota_reading_log (
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
CREATE INDEX IF NOT EXISTS pending_quota_reading_log_by_observed ON pending_quota_reading_log(observed_at);

CREATE TABLE IF NOT EXISTS quota_reading_moved (
  account_key TEXT NOT NULL,
  window_key TEXT NOT NULL,
  moved_through_observed_at INTEGER NOT NULL,
  PRIMARY KEY (account_key, window_key)
) WITHOUT ROWID;
