-- Recreate two copy triggers of migrations 054 and 056 so that no statement
-- in the schema resolves a conflict by REPLACE. Each trigger's insert names
-- every column of its target table, so an upsert that sets all of them
-- leaves the same values a replacing insert left (the row keeps its rowid).
-- Neither target table has triggers of its own, so updating a conflicting
-- row instead of deleting and inserting it fires nothing different.
DROP TRIGGER IF EXISTS model_route_parent_failures_on_insert;
CREATE TRIGGER model_route_parent_failures_on_insert
AFTER INSERT ON model_route_decisions
WHEN NEW.outcome_error IS NOT NULL AND NEW.parent_session_id IS NOT NULL
BEGIN
  INSERT INTO model_route_parent_failures
    (decision_id, parent_session_id, outcome_at, selected_route_key,
     selected_provider_id, outcome_error)
  VALUES (NEW.id, NEW.parent_session_id, NEW.outcome_at, NEW.selected_route_key,
          NEW.selected_provider_id, NEW.outcome_error)
  ON CONFLICT (decision_id) DO UPDATE SET
    parent_session_id = excluded.parent_session_id,
    outcome_at = excluded.outcome_at,
    selected_route_key = excluded.selected_route_key,
    selected_provider_id = excluded.selected_provider_id,
    outcome_error = excluded.outcome_error;
END;

DROP TRIGGER IF EXISTS account_observation_event_on_insert;
CREATE TRIGGER account_observation_event_on_insert
AFTER INSERT ON account_observation
BEGIN
  INSERT INTO account_observation_event
    (principal, runner, event_id, observation_id, bound, effects_json, received_at)
  VALUES (NEW.principal, NEW.runner, NEW.event_id, NEW.observation_id, NEW.bound,
          NEW.effects_json, NEW.received_at)
  ON CONFLICT (principal, runner, event_id) DO UPDATE SET
    observation_id = excluded.observation_id,
    bound = excluded.bound,
    effects_json = excluded.effects_json,
    received_at = excluded.received_at;
END;
