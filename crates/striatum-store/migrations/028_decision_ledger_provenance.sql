-- Migration 28 (namespace "route_state"): retain the launch join key and the
-- selection inputs that a decision actually consumed. Historical rows stay NULL;
-- the router never invents a send ID or provenance after the fact.
ALTER TABLE model_route_decisions ADD COLUMN send_id TEXT;
ALTER TABLE model_route_decisions ADD COLUMN quota_signal_json TEXT;
ALTER TABLE model_route_decisions ADD COLUMN policy_revision TEXT;

CREATE INDEX IF NOT EXISTS model_route_decisions_send_id_idx
  ON model_route_decisions(send_id)
  WHERE send_id IS NOT NULL;
