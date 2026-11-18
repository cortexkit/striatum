-- The whole history tables at route_state version 96. The retired-origin
-- enforcement trigger is not copied: its audit rows must not mutate live state.

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

CREATE TABLE IF NOT EXISTS "account_hold" (
  decision_id TEXT PRIMARY KEY,
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

CREATE INDEX IF NOT EXISTS account_hold_by_created
  ON account_hold (created_at);

CREATE TABLE IF NOT EXISTS "account_fence" (
  fence_id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  account_id TEXT NOT NULL,
  scope_json TEXT NOT NULL,
  fence_json TEXT NOT NULL,
  source TEXT NOT NULL,
  observed_at INTEGER NOT NULL,
  principal TEXT NOT NULL,
  admission_id TEXT,
  observation_id TEXT,
  created_at INTEGER NOT NULL,
  cleared_at INTEGER,
  cleared_reason TEXT
, pool_key_json TEXT, recovery_set_json TEXT NOT NULL DEFAULT '[]');

CREATE INDEX IF NOT EXISTS account_fence_by_account
  ON account_fence (provider, account_id);

CREATE INDEX IF NOT EXISTS account_fence_by_observation
  ON account_fence (observation_id);

CREATE TABLE IF NOT EXISTS "account_principal_exclusion" (
  principal TEXT NOT NULL,
  admission_id TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  runner_view_digest TEXT NOT NULL,
  roster_view_digest TEXT NOT NULL,
  observation_id TEXT,
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (principal, admission_id, credential_id)
);

CREATE INDEX IF NOT EXISTS account_exclusion_by_observation
  ON account_principal_exclusion (observation_id);

CREATE TABLE IF NOT EXISTS credential_entitlement_fences (
    provider_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    code TEXT NOT NULL,
    listing_fingerprint TEXT,
    observed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (provider_id, credential_id)
);

CREATE TABLE IF NOT EXISTS account_size_refusal (
  provider TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  model TEXT NOT NULL,
  size_class INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  observed_at INTEGER NOT NULL,
  PRIMARY KEY (provider, credential_id, model, size_class)
);

CREATE INDEX IF NOT EXISTS account_size_refusal_expiry ON account_size_refusal (expires_at);

CREATE TABLE IF NOT EXISTS account_route_fence (
  provider TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  model TEXT NOT NULL,
  until_ms INTEGER NOT NULL,
  decision_id TEXT NOT NULL,
  recorded_at INTEGER NOT NULL, last_reported_at INTEGER, wall_class TEXT NOT NULL DEFAULT 'quota'
  CHECK (wall_class IN ('quota', 'transient')),
  PRIMARY KEY (provider, credential_id, model)
);

CREATE INDEX IF NOT EXISTS account_route_fence_expiry ON account_route_fence (until_ms);

CREATE TABLE IF NOT EXISTS fence_recheck (
  kind TEXT NOT NULL,
  origin_key TEXT NOT NULL,
  provider TEXT NOT NULL,
  account_id TEXT,
  credential_id TEXT,
  model TEXT,
  size_class INTEGER,
  principal TEXT,
  admission_id TEXT,
  evidence TEXT NOT NULL,
  evidence_at INTEGER NOT NULL,
  quota INTEGER NOT NULL CHECK (quota IN (0,1)),
  classified INTEGER NOT NULL DEFAULT 0,
  next_recheck_at INTEGER NOT NULL,
  reset_check_at INTEGER,
  last_claimed_at INTEGER,
  claim_generation INTEGER NOT NULL DEFAULT 0,
  claim_owner TEXT,
  cleared_at INTEGER,
  PRIMARY KEY (kind, origin_key)
);

CREATE INDEX IF NOT EXISTS fence_recheck_scope ON fence_recheck(provider, credential_id, account_id, model);

CREATE TABLE IF NOT EXISTS fence_probe_authorization (
 owner TEXT NOT NULL,kind TEXT NOT NULL,origin_key TEXT NOT NULL,
 evidence TEXT NOT NULL,evidence_at INTEGER NOT NULL,claimed_at INTEGER NOT NULL,
 claim_generation INTEGER NOT NULL,
 PRIMARY KEY(owner,kind,origin_key)
);

CREATE TABLE IF NOT EXISTS fence_recheck_route_clear (
 kind TEXT NOT NULL,origin_key TEXT NOT NULL,provider TEXT NOT NULL,model TEXT NOT NULL,evidence TEXT NOT NULL,
 PRIMARY KEY(kind,origin_key,provider,model)
);

CREATE TABLE IF NOT EXISTS fence_success_pool (
 fence_id TEXT NOT NULL,
 pool_key_json TEXT NOT NULL,evidence_at INTEGER NOT NULL,healed_at INTEGER NOT NULL,
 PRIMARY KEY(fence_id,pool_key_json)
);

CREATE TABLE IF NOT EXISTS account_transient_backoff (
 provider TEXT NOT NULL,account_id TEXT NOT NULL,retry_at_ms INTEGER NOT NULL,
 evidence_at INTEGER NOT NULL,evidence_key TEXT NOT NULL,effects_json TEXT NOT NULL,
 PRIMARY KEY(provider,account_id)
);

CREATE TABLE IF NOT EXISTS account_non_quota_route_fence (
 provider TEXT NOT NULL, credential_id TEXT NOT NULL, model TEXT NOT NULL,
 until_ms INTEGER NOT NULL, decision_id TEXT NOT NULL, recorded_at INTEGER NOT NULL,
 last_reported_at INTEGER NOT NULL, wall_kind TEXT NOT NULL,
 PRIMARY KEY(provider,credential_id,model,wall_kind)
);

CREATE TABLE IF NOT EXISTS account_non_quota_size_refusal (
 provider TEXT NOT NULL,credential_id TEXT NOT NULL,model TEXT NOT NULL,
 size_class INTEGER NOT NULL,expires_at INTEGER NOT NULL,observed_at INTEGER NOT NULL,
 PRIMARY KEY(provider,credential_id,model,size_class)
);

CREATE TABLE IF NOT EXISTS fence_retired_origin (
 kind TEXT NOT NULL,origin_key TEXT NOT NULL,evidence TEXT NOT NULL,
 evidence_at INTEGER NOT NULL,retired_at INTEGER NOT NULL,
 PRIMARY KEY(kind,origin_key,evidence)
);

CREATE INDEX IF NOT EXISTS fence_retired_origin_by_retired ON fence_retired_origin(retired_at);

CREATE INDEX IF NOT EXISTS account_hold_by_pin ON account_hold(pin_id);

-- The first open or the offline split binds this archive to its state file.
CREATE TABLE IF NOT EXISTS history_binding (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  split_identity TEXT NOT NULL CHECK (length(split_identity) = 32),
  move_generation INTEGER NOT NULL CHECK (move_generation >= 0)
);
