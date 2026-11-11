-- Migration 30 (namespace "route_state"): retain the full join identity for new
-- decision rows. Historical rows stay NULL; provenance is never invented after the fact.
ALTER TABLE model_route_decisions ADD COLUMN harness TEXT NULL;
ALTER TABLE model_route_decisions ADD COLUMN project_root TEXT NULL;
