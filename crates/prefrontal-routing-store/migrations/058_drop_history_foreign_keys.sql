-- Migration 58: drop the foreign keys into the account-selection history logs.
--
-- A backup may skip a table and restore it empty while every other table
-- keeps its rows. The two account-selection history logs are worth skipping
-- (on a long-running install `account_decision` is about 480 MB and
-- `account_observation` about 520 MB), but other tables still pointed at
-- them by foreign key:
--   account_pin.decision_id, account_admission.decision_id and
--   account_hold.decision_id                    -> account_decision
--   account_fence.observation_id and
--   account_principal_exclusion.observation_id  -> account_observation
-- Restored empty, those logs would leave every such reference dangling
-- (`PRAGMA foreign_key_check` fails), and a backup tool refuses to skip a
-- table that a table it keeps references. Nothing reads through these
-- references: the ids stay as plain columns naming a history row that may be
-- gone. The one remaining foreign key, account_admission.pin_id ->
-- account_pin, joins two tables every backup keeps, and is kept.
--
-- SQLite cannot drop a constraint in place, so each of the five tables is
-- rebuilt the way SQLite's ALTER TABLE documentation describes: create the
-- new table, copy every row, drop the old table, rename the new one, and
-- recreate its indexes (none of these tables has a trigger). Columns, CHECK
-- constraints, defaults, rows and index definitions are unchanged. On a copy
-- of a long-running store this took about 15 seconds, once, while routing
-- answers routing_not_ready.
--
-- The routing store runs this with foreign-key enforcement off (see the
-- store's `open`): with it on, dropping `account_pin` would first delete every
-- pin and fail on the admissions that name them. The last statements check
-- that the rebuilt tables pass the foreign-key and integrity checks; a
-- failure rolls the whole migration back and leaves the store as it was.

CREATE TABLE account_pin_rebuilt (
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
  decision_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  UNIQUE (principal, conversation_hash, provider, pin_generation)
);
INSERT INTO account_pin_rebuilt
  (pin_id, principal, conversation_hash, provider, pin_generation, affinity_model,
   account_id, credential_id, pin_mode, previous_pin_id, decision_id, created_at)
SELECT pin_id, principal, conversation_hash, provider, pin_generation, affinity_model,
       account_id, credential_id, pin_mode, previous_pin_id, decision_id, created_at
FROM account_pin;
DROP TABLE account_pin;
ALTER TABLE account_pin_rebuilt RENAME TO account_pin;
CREATE INDEX account_pin_by_decision
  ON account_pin (decision_id);

CREATE TABLE account_admission_rebuilt (
  principal TEXT NOT NULL,
  admission_id TEXT NOT NULL,
  request_fingerprint TEXT NOT NULL,
  request_mode TEXT NOT NULL CHECK (request_mode IN ('admit', 'renew', 'reassign')),
  decision_id TEXT NOT NULL,
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
INSERT INTO account_admission_rebuilt
  (principal, admission_id, request_fingerprint, request_mode, decision_id,
   conversation_hash, provider, model, account_id, credential_id, pin_id,
   pin_generation, state, charged, request_bytes, request_bytes_estimated,
   committed_at, admission_valid_until, lease_valid_until, reply_json, run_id,
   started_at, started_late, released_at, finished_at)
SELECT principal, admission_id, request_fingerprint, request_mode, decision_id,
       conversation_hash, provider, model, account_id, credential_id, pin_id,
       pin_generation, state, charged, request_bytes, request_bytes_estimated,
       committed_at, admission_valid_until, lease_valid_until, reply_json, run_id,
       started_at, started_late, released_at, finished_at
FROM account_admission;
DROP TABLE account_admission;
ALTER TABLE account_admission_rebuilt RENAME TO account_admission;
CREATE INDEX account_admission_demand
  ON account_admission (provider, committed_at, principal, admission_id, account_id, state, request_bytes)
  WHERE charged = 1 AND state != 'released';
CREATE INDEX account_admission_by_decision
  ON account_admission (decision_id);
CREATE INDEX account_admission_by_committed
  ON account_admission (committed_at);
CREATE INDEX account_admission_by_pin
  ON account_admission (pin_id);

CREATE TABLE account_hold_rebuilt (
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
INSERT INTO account_hold_rebuilt
  (decision_id, principal, conversation_hash, provider, pin_id, pin_generation,
   account_id, credential_id, basis, reset_at, retry_after_s, created_at)
SELECT decision_id, principal, conversation_hash, provider, pin_id, pin_generation,
       account_id, credential_id, basis, reset_at, retry_after_s, created_at
FROM account_hold;
DROP TABLE account_hold;
ALTER TABLE account_hold_rebuilt RENAME TO account_hold;
CREATE INDEX account_hold_by_created
  ON account_hold (created_at);

CREATE TABLE account_fence_rebuilt (
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
);
INSERT INTO account_fence_rebuilt
  (fence_id, provider, account_id, scope_json, fence_json, source, observed_at,
   principal, admission_id, observation_id, created_at, cleared_at, cleared_reason)
SELECT fence_id, provider, account_id, scope_json, fence_json, source, observed_at,
       principal, admission_id, observation_id, created_at, cleared_at, cleared_reason
FROM account_fence;
DROP TABLE account_fence;
ALTER TABLE account_fence_rebuilt RENAME TO account_fence;
CREATE INDEX account_fence_by_account
  ON account_fence (provider, account_id);
CREATE INDEX account_fence_by_observation
  ON account_fence (observation_id);

CREATE TABLE account_principal_exclusion_rebuilt (
  principal TEXT NOT NULL,
  admission_id TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  runner_view_digest TEXT NOT NULL,
  roster_view_digest TEXT NOT NULL,
  observation_id TEXT,
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (principal, admission_id, credential_id)
);
INSERT INTO account_principal_exclusion_rebuilt
  (principal, admission_id, credential_id, runner_view_digest, roster_view_digest,
   observation_id, recorded_at)
SELECT principal, admission_id, credential_id, runner_view_digest, roster_view_digest,
       observation_id, recorded_at
FROM account_principal_exclusion;
DROP TABLE account_principal_exclusion;
ALTER TABLE account_principal_exclusion_rebuilt RENAME TO account_principal_exclusion;
CREATE INDEX account_exclusion_by_observation
  ON account_principal_exclusion (observation_id);

-- Refuse to commit a rebuild that left a dangling reference or a damaged
-- table: each row inserted here must hold 0 problems, or the CHECK fails the
-- statement and the migration's transaction rolls back. The foreign-key check
-- covers the whole store (only account_admission.pin_id remains to check);
-- the integrity check covers the five rebuilt tables and their indexes.
CREATE TEMP TABLE migration_058_check (
  what TEXT NOT NULL,
  problems INTEGER NOT NULL CHECK (problems = 0)
);
INSERT INTO migration_058_check (what, problems)
SELECT 'foreign_key_check', COUNT(*) FROM pragma_foreign_key_check;
INSERT INTO migration_058_check (what, problems)
SELECT 'integrity_check ' || name, (
  SELECT COUNT(*) FROM pragma_integrity_check(name) WHERE integrity_check != 'ok'
)
FROM (SELECT 'account_pin' AS name UNION ALL SELECT 'account_admission'
      UNION ALL SELECT 'account_hold' UNION ALL SELECT 'account_fence'
      UNION ALL SELECT 'account_principal_exclusion');
DROP TABLE migration_058_check;
