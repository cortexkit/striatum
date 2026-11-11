-- Migration 36 (namespace "route_state"): extend the existing per-message usage
-- row with the token profile and attribution used by quota-aware routing.

ALTER TABLE model_route_usage ADD COLUMN reasoning_tokens INTEGER;
ALTER TABLE model_route_usage ADD COLUMN cached_read_tokens INTEGER;
ALTER TABLE model_route_usage ADD COLUMN cache_write_tokens INTEGER;
ALTER TABLE model_route_usage ADD COLUMN workload_class TEXT;
ALTER TABLE model_route_usage ADD COLUMN account_key TEXT;

CREATE INDEX IF NOT EXISTS model_route_usage_workload_class_observed_idx
  ON model_route_usage(workload_class, observed_at);
