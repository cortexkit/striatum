-- Migration 20 (namespace "route_state"): decision retention scans by creation time.
-- FROZEN once applied: a later schema change must use a new migration.
CREATE INDEX IF NOT EXISTS model_route_decisions_created_at_idx
  ON model_route_decisions(created_at);
