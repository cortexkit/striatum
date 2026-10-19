-- Migration 56: recognise a re-sent runner event without the observation log.
--
-- A backup may skip a table and restore it empty while every other table
-- keeps its rows. `account_observation`, the verbatim runner observation log
-- (about 520 MB on a long-running install), is worth skipping, but routing
-- read it to recognise a runner reporting the same event twice. The table
-- below keeps just what that needs, for a short window, so it is small enough
-- for every backup to carry.
--
-- The other half of making the account-selection history logs skippable is
-- dropping the foreign keys other tables hold into `account_decision` and
-- `account_observation`. That is not done here: rebuilding the referencing
-- tables copies about 230 MB and took about 15 seconds on a copy of a live
-- store, all of it before routing can answer anyone. The store's `open` drops
-- those constraints in place instead (see `drop_history_foreign_keys`).

-- Which runner events routing already applied, and what it answered.
--
-- A runner may report the same event again (a transport retry); routing must
-- recognise it and answer with the first report's id, binding and effects
-- instead of applying it a second time. That used to be read from
-- `account_observation` alone, so restoring that log empty would make every
-- replayed event apply again. This table keeps only the key and the answer,
-- not the verbatim report, and only for a short window (the routing retention
-- pass prunes it), so it stays small enough to back up. `account_observation`
-- is still consulted when a key is not here, so recognising an older event in
-- a store that was never restored works as before.
--
-- Triggers keep it equal to the log in the same transaction as each write, so
-- no writer has to remember it. Retention prunes the two tables separately:
-- deleting a log row leaves its key.
CREATE TABLE IF NOT EXISTS account_observation_event (
  principal TEXT NOT NULL,
  runner TEXT NOT NULL,
  event_id TEXT NOT NULL,
  observation_id TEXT NOT NULL,
  bound INTEGER NOT NULL CHECK (bound IN (0, 1)),
  effects_json TEXT NOT NULL,
  received_at INTEGER NOT NULL,
  PRIMARY KEY (principal, runner, event_id)
);
CREATE INDEX IF NOT EXISTS account_observation_event_by_received
  ON account_observation_event (received_at);

CREATE TRIGGER IF NOT EXISTS account_observation_event_on_insert
AFTER INSERT ON account_observation
BEGIN
  INSERT OR REPLACE INTO account_observation_event
    (principal, runner, event_id, observation_id, bound, effects_json, received_at)
  VALUES (NEW.principal, NEW.runner, NEW.event_id, NEW.observation_id, NEW.bound,
          NEW.effects_json, NEW.received_at);
END;

CREATE TRIGGER IF NOT EXISTS account_observation_event_on_update
AFTER UPDATE OF bound, effects_json ON account_observation
BEGIN
  UPDATE account_observation_event
  SET bound = NEW.bound, effects_json = NEW.effects_json
  WHERE principal = NEW.principal AND runner = NEW.runner AND event_id = NEW.event_id
    AND observation_id = NEW.observation_id;
END;

-- Backfill the last six hours of events (the window the routing retention
-- pass keeps) through the received-time index rather than reading the whole
-- log. Older events are still recognised through `account_observation` until
-- a restore empties it.
INSERT OR IGNORE INTO account_observation_event
  (principal, runner, event_id, observation_id, bound, effects_json, received_at)
SELECT principal, runner, event_id, observation_id, bound, effects_json, received_at
FROM account_observation INDEXED BY account_observation_by_received
WHERE received_at >= (CAST(strftime('%s', 'now') AS INTEGER) - 6 * 3600) * 1000;
