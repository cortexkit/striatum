-- Migration 44 (namespace "route_state"): the account-selection store that
-- `route.account_for` and its reports write, and the shadow decisions routing
-- records before it serves any. Specification: docs/designs/account-selection.md
-- ("Proposed protocol", "Affinity, persistence and freeze semantics").
--
-- Every table is append-or-update: nothing here is ever deleted. What the design
-- says expires (an unstarted reservation after 60 s, conversation affinity after
-- seven idle days, a plugin lease after two minutes) expires by comparing the
-- stored time with the clock, never by removing the row. Times are local Unix
-- milliseconds.

-- The installation-local salt conversation keys are hashed with, so no stored
-- row carries a caller's raw conversation key. Created on first use.
CREATE TABLE IF NOT EXISTS account_selection_salt (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  salt TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

-- Every account decision routing made, served or shadow, with its whole
-- machine-readable explanation (`decision_json`). A shadow row is a decision
-- routing computed without answering anyone; `context_json` carries what
-- triggered it (timer, caller request or observation) and the roster gaps and
-- alarms it saw.
CREATE TABLE IF NOT EXISTS account_decision (
  decision_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL CHECK (mode IN ('serve', 'shadow')),
  trigger TEXT NOT NULL,
  principal TEXT NOT NULL,
  admission_id TEXT,
  request_mode TEXT,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  family TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('admit', 'hold', 'refuse')),
  code TEXT NOT NULL,
  account_id TEXT,
  credential_id TEXT,
  policy_version INTEGER NOT NULL,
  decision_json TEXT NOT NULL,
  context_json TEXT,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS account_decision_by_mode_time
  ON account_decision (mode, created_at);

-- Conversation affinity: one row per (principal, hashed conversation key,
-- provider), holding the current pin. The pin id and generation are the
-- compare-and-set token for renewal and reassignment; a write that names an
-- older generation changes nothing. `expires_at` is the idle expiry and
-- `lease_valid_until` the plugin lease; an expired row is kept and read as
-- "no live affinity".
CREATE TABLE IF NOT EXISTS account_affinity (
  principal TEXT NOT NULL,
  conversation_hash TEXT NOT NULL,
  provider TEXT NOT NULL,
  affinity_model TEXT NOT NULL,
  pin_id TEXT NOT NULL,
  pin_generation INTEGER NOT NULL CHECK (pin_generation >= 1),
  account_id TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  credential_binding_revision INTEGER NOT NULL,
  policy_version INTEGER NOT NULL,
  initial_demand_bytes INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  last_used_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  lease_valid_until INTEGER,
  PRIMARY KEY (principal, conversation_hash, provider)
);

-- Every pin generation ever issued, immutable. A conversation's generations
-- are unique and increase; `previous_pin_id` links a move to the pin it
-- replaced, which stays as historical identity.
CREATE TABLE IF NOT EXISTS account_pin (
  pin_id TEXT PRIMARY KEY,
  principal TEXT NOT NULL,
  conversation_hash TEXT NOT NULL,
  provider TEXT NOT NULL,
  pin_generation INTEGER NOT NULL CHECK (pin_generation >= 1),
  affinity_model TEXT NOT NULL,
  account_id TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  pin_mode TEXT NOT NULL CHECK (pin_mode IN ('run_frozen', 'conversation_lease')),
  previous_pin_id TEXT,
  decision_id TEXT NOT NULL REFERENCES account_decision (decision_id),
  created_at INTEGER NOT NULL,
  UNIQUE (principal, conversation_hash, provider, pin_generation)
);

-- Admissions, namespaced by the authenticated principal. The row is the
-- committed decision replayed to a duplicate request (`reply_json`) and the
-- demand it charged: pending demand is the sum of `request_bytes` over charged
-- admissions in every state but `released`, committed after the account's
-- observation point minus the reflection lag. `request_fingerprint` tells a
-- transport retry (same parameters) from a conflicting reuse of the id.
CREATE TABLE IF NOT EXISTS account_admission (
  principal TEXT NOT NULL,
  admission_id TEXT NOT NULL,
  request_fingerprint TEXT NOT NULL,
  request_mode TEXT NOT NULL CHECK (request_mode IN ('admit', 'renew', 'reassign')),
  decision_id TEXT NOT NULL REFERENCES account_decision (decision_id),
  conversation_hash TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  account_id TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  pin_id TEXT NOT NULL REFERENCES account_pin (pin_id),
  pin_generation INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('reserved', 'started', 'released', 'expired', 'finished')),
  charged INTEGER NOT NULL CHECK (charged IN (0, 1)),
  request_bytes INTEGER NOT NULL,
  request_bytes_estimated INTEGER NOT NULL CHECK (request_bytes_estimated IN (0, 1)),
  committed_at INTEGER NOT NULL,
  admission_valid_until INTEGER NOT NULL,
  lease_valid_until INTEGER NOT NULL,
  reply_json TEXT NOT NULL,
  run_id TEXT,
  started_at INTEGER,
  started_late INTEGER NOT NULL DEFAULT 0 CHECK (started_late IN (0, 1)),
  released_at INTEGER,
  finished_at INTEGER,
  PRIMARY KEY (principal, admission_id)
);
CREATE INDEX IF NOT EXISTS account_admission_demand
  ON account_admission (provider, account_id, committed_at);

-- Short holds routing answered (`account_hold`): the conversation stays on its
-- account and retries after the jittered Retry-After.
CREATE TABLE IF NOT EXISTS account_hold (
  decision_id TEXT PRIMARY KEY REFERENCES account_decision (decision_id),
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

-- Every observation a runner reported, bound or advisory, kept verbatim.
-- A runner's event id is reported once: a repeat is recognized and changes
-- nothing. `effects_json` records what the observation changed, if anything.
CREATE TABLE IF NOT EXISTS account_observation (
  observation_id TEXT PRIMARY KEY,
  principal TEXT NOT NULL,
  runner TEXT NOT NULL,
  event_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  admission_id TEXT,
  bound INTEGER NOT NULL CHECK (bound IN (0, 1)),
  observation_json TEXT NOT NULL,
  effects_json TEXT NOT NULL,
  received_at INTEGER NOT NULL,
  UNIQUE (principal, runner, event_id)
);

-- Confirmed exhaustion on one scope of one account. Only evidence bound to an
-- admission routing issued to the reporting principal creates a fence. A fence
-- is never deleted; fresh post-reset evidence may later mark it cleared.
CREATE TABLE IF NOT EXISTS account_fence (
  fence_id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  account_id TEXT NOT NULL,
  scope_json TEXT NOT NULL,
  fence_json TEXT NOT NULL,
  source TEXT NOT NULL,
  observed_at INTEGER NOT NULL,
  principal TEXT NOT NULL,
  admission_id TEXT,
  observation_id TEXT REFERENCES account_observation (observation_id),
  created_at INTEGER NOT NULL,
  cleared_at INTEGER,
  cleared_reason TEXT
);
CREATE INDEX IF NOT EXISTS account_fence_by_account
  ON account_fence (provider, account_id);

-- A runner's scoped not-found for one credential, excluding that credential
-- for the one admission it was reported against and for no other admission or
-- principal.
CREATE TABLE IF NOT EXISTS account_principal_exclusion (
  principal TEXT NOT NULL,
  admission_id TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  runner_view_digest TEXT NOT NULL,
  roster_view_digest TEXT NOT NULL,
  observation_id TEXT REFERENCES account_observation (observation_id),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (principal, admission_id, credential_id)
);
