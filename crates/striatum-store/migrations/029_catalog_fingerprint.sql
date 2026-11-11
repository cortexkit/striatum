-- Migration 29 (namespace "route_state"): catalog generations make every
-- decision reproducible against the exact scored fact set. Existing decision rows
-- intentionally remain NULL because their historical catalog cannot be recovered.
ALTER TABLE model_facts ADD COLUMN speed_multiplier REAL;
ALTER TABLE model_facts ADD COLUMN excluded INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS catalog_generation (
  fingerprint TEXT PRIMARY KEY,
  created_at INTEGER NOT NULL
);

ALTER TABLE model_route_decisions ADD COLUMN catalog_fingerprint TEXT;

CREATE INDEX IF NOT EXISTS model_route_decisions_catalog_fingerprint_idx
  ON model_route_decisions(catalog_fingerprint)
  WHERE catalog_fingerprint IS NOT NULL;
