-- Migration 2 (namespace "route_state"): the observed-TPS usage table. The module
-- records one row per completed assistant message (idempotent by message id) and
-- overlays the aggregated observed output-tokens/sec onto each fact's speed at
-- selection time. Ported from the Alfonso TS plugin's model_route_usage table.
-- FROZEN once applied: a later change is a NEW migration.

CREATE TABLE IF NOT EXISTS model_route_usage (
  id TEXT PRIMARY KEY,
  task_id TEXT,
  route_key TEXT NOT NULL,
  capacity_key TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  input_tokens INTEGER,
  output_tokens INTEGER,
  duration_ms INTEGER,
  observed_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS model_route_usage_capacity_idx
  ON model_route_usage(capacity_key, observed_at);

CREATE INDEX IF NOT EXISTS model_route_usage_route_observed_idx
  ON model_route_usage(route_key, observed_at);
