-- Migration 47: indexes the routing retention passes need so that each bounded
-- delete is a short index range instead of a table scan.
--
-- Routing opens SQLite with foreign keys on. Deleting an `account_decision` row
-- makes SQLite look for children in `account_pin` and `account_admission` by
-- `decision_id`, and deleting an `account_observation` row makes it look in
-- `account_fence` and `account_principal_exclusion` by `observation_id`.
-- Without these indexes every deleted parent row would scan each child table
-- in full. The retention queries also skip still-referenced parents with the
-- same lookups.
--
-- `account_decision` itself gets no new index: building one reads the whole
-- table (gigabytes on a long-running install) while the module is starting.
-- Its retention pass walks the existing `account_decision_by_mode_time` index.
CREATE INDEX IF NOT EXISTS account_pin_by_decision
  ON account_pin (decision_id);
CREATE INDEX IF NOT EXISTS account_admission_by_decision
  ON account_admission (decision_id);
CREATE INDEX IF NOT EXISTS account_fence_by_observation
  ON account_fence (observation_id);
CREATE INDEX IF NOT EXISTS account_exclusion_by_observation
  ON account_principal_exclusion (observation_id);
CREATE INDEX IF NOT EXISTS account_observation_by_received
  ON account_observation (received_at);
CREATE INDEX IF NOT EXISTS model_route_usage_observed_idx
  ON model_route_usage (observed_at);
