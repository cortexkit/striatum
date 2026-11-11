-- Migration 54: a narrow, indexed copy of failed decision outcomes per head.
--
-- Every route.select that names its parent session asks "which of this
-- head's recent workers were refused?" (the content-policy withholding).
-- That question used to be answered from `model_route_decisions` itself,
-- which has no index on `parent_session_id` or `outcome_at`, so each launch
-- scanned the whole decision ledger while holding the routing store's single
-- connection. On a long-running install (a 9 GB store) one scan took about
-- eleven seconds of disk waits, and every other request queued behind it, so
-- core's worker launches timed out waiting for route.select.
--
-- An index on the ledger is not enough: the outcome columns sit after the
-- large JSON payloads, so reading them means walking each row's overflow
-- pages, and building such an index at startup would read the whole table.
-- Instead this table holds one small row per failed decision that names a
-- parent session. Triggers keep it equal to the ledger, in the same
-- transaction as each outcome write and each retention delete, so no writer
-- has to remember it.
CREATE TABLE IF NOT EXISTS model_route_parent_failures (
  decision_id TEXT PRIMARY KEY,
  parent_session_id TEXT NOT NULL,
  outcome_at INTEGER,
  selected_route_key TEXT,
  selected_provider_id TEXT,
  outcome_error TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS model_route_parent_failures_by_parent
  ON model_route_parent_failures (parent_session_id, outcome_at);

-- An outcome write (or any later change to the copied columns) replaces the
-- copy; a decision without an error or without a parent session has none.
CREATE TRIGGER IF NOT EXISTS model_route_parent_failures_on_update
AFTER UPDATE OF outcome_error, outcome_at, parent_session_id, selected_route_key,
  selected_provider_id ON model_route_decisions
BEGIN
  DELETE FROM model_route_parent_failures WHERE decision_id = OLD.id;
  INSERT INTO model_route_parent_failures
    (decision_id, parent_session_id, outcome_at, selected_route_key,
     selected_provider_id, outcome_error)
  SELECT NEW.id, NEW.parent_session_id, NEW.outcome_at, NEW.selected_route_key,
         NEW.selected_provider_id, NEW.outcome_error
  WHERE NEW.outcome_error IS NOT NULL AND NEW.parent_session_id IS NOT NULL;
END;

-- An insert that already carries an outcome is copied too (the routing
-- module records outcomes by update, but the copy must not depend on that).
CREATE TRIGGER IF NOT EXISTS model_route_parent_failures_on_insert
AFTER INSERT ON model_route_decisions
WHEN NEW.outcome_error IS NOT NULL AND NEW.parent_session_id IS NOT NULL
BEGIN
  INSERT OR REPLACE INTO model_route_parent_failures
    (decision_id, parent_session_id, outcome_at, selected_route_key,
     selected_provider_id, outcome_error)
  VALUES (NEW.id, NEW.parent_session_id, NEW.outcome_at, NEW.selected_route_key,
          NEW.selected_provider_id, NEW.outcome_error);
END;

-- Retention deletes ledger rows; their copies go with them. The lookup is by
-- the decision id alone, so a delete never reads the deleted row's payload.
CREATE TRIGGER IF NOT EXISTS model_route_parent_failures_on_delete
AFTER DELETE ON model_route_decisions
BEGIN
  DELETE FROM model_route_parent_failures WHERE decision_id = OLD.id;
END;

-- Backfill from decisions created in the last seven days, found through the
-- existing created_at index rather than a whole-ledger scan. The head
-- withholding only looks back one hour of outcomes, and a content-policy
-- refusal arrives when the worker's request is refused, normally minutes
-- after its decision was made. Only a decision older than a week whose
-- refusal landed in the hour before this upgrade is left out, and only for
-- that hour; every outcome written after the upgrade is copied by the
-- triggers above.
INSERT OR IGNORE INTO model_route_parent_failures
  (decision_id, parent_session_id, outcome_at, selected_route_key,
   selected_provider_id, outcome_error)
SELECT id, parent_session_id, outcome_at, selected_route_key,
       selected_provider_id, outcome_error
FROM model_route_decisions INDEXED BY model_route_decisions_created_at_idx
WHERE created_at >= (CAST(strftime('%s', 'now') AS INTEGER) - 7 * 86400) * 1000
  AND outcome_error IS NOT NULL
  AND parent_session_id IS NOT NULL;
