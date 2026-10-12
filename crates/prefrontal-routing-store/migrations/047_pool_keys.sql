-- Migration 47 (namespace "route_state"): the key pool for explicit-only
-- providers whose credentials are interchangeable API keys (the paid Anthropic
-- keys behind `anthropic-platform`). Keyed on the full vault credential id,
-- because these keys have no refresh adapter, are not in the subscription
-- account roster, and do not fit the account tables of migration 44.
--
-- Nothing is deleted. A lease stops counting as a run in flight when
-- `valid_until` passes or `released_at` is set; core renews leases while their
-- runs live, so a lease from a core that died expires on its own. Times are
-- local Unix milliseconds.

-- One lease per run-starting send: the key a session's run was pinned to.
CREATE TABLE IF NOT EXISTS pool_key_lease (
  lease_id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  session_id TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  workspace TEXT NOT NULL,
  acquired_at INTEGER NOT NULL,
  valid_until INTEGER NOT NULL,
  released_at INTEGER
);
CREATE INDEX IF NOT EXISTS pool_key_lease_by_key
  ON pool_key_lease (provider, credential_id, valid_until);
CREATE INDEX IF NOT EXISTS pool_key_lease_by_session
  ON pool_key_lease (provider, session_id, valid_until);

-- Per-key walls and last use. A billing wall holds until its re-probe time; a
-- rate-limit cooldown until the provider's retry-after.
CREATE TABLE IF NOT EXISTS pool_key_state (
  provider TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  last_used_at INTEGER,
  walled_until INTEGER,
  wall_reason TEXT,
  cooldown_until INTEGER,
  cooldown_reason TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (provider, credential_id)
);

-- Prompt-cache affinity: the workspace a session is bound to. `ended_at` is set
-- when the session ends; an ended or idle-expired row binds nothing.
CREATE TABLE IF NOT EXISTS pool_session_affinity (
  provider TEXT NOT NULL,
  session_id TEXT NOT NULL,
  workspace TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  bound_at INTEGER NOT NULL,
  last_used_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  ended_at INTEGER,
  PRIMARY KEY (provider, session_id)
);
