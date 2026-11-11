-- Migration 43 (namespace "route_state"): per-account quota readings, Insula
-- completeness and the vault roster that account selection reads, kept across
-- restarts. Nothing selects an account from them yet.
--
-- account_quota_reading holds one row per (Insula provider, vault account id):
-- the whole entry exactly as Insula published it. `fetched_at` repeats the
-- entry's own producer date, the source time its age is measured from.
-- `received_at` is the local clock (Unix milliseconds) when routing accepted the
-- entry. A reading is only ever replaced by a newer entry for the same account.
-- An account that a complete Insula reply no longer lists gets `tombstoned_at`
-- and keeps its row; an account that a partial reply omits is left untouched.
CREATE TABLE IF NOT EXISTS account_quota_reading (
  provider TEXT NOT NULL,
  account_id TEXT NOT NULL,
  entry_json TEXT NOT NULL,
  fetched_at TEXT,
  received_at INTEGER NOT NULL,
  tombstoned_at INTEGER,
  PRIMARY KEY (provider, account_id)
);

-- One row per Insula provider routing polls: when it last answered, and when a
-- reply last named the provider in `completeProviders`. Times are local Unix
-- milliseconds.
CREATE TABLE IF NOT EXISTS account_quota_completeness (
  provider TEXT PRIMARY KEY,
  last_polled_at INTEGER NOT NULL,
  last_poll_complete INTEGER NOT NULL CHECK (last_poll_complete IN (0, 1)),
  last_complete_at INTEGER
);

-- The last vault roster routing listed: identities coalesced by refresh adapter
-- and vault account id, the vault's view digest, and the local listing time.
CREATE TABLE IF NOT EXISTS account_vault_roster (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  view TEXT NOT NULL,
  roster_json TEXT NOT NULL,
  fetched_at INTEGER NOT NULL
);
