-- Provider-wide cooldown persistence and route-state inheritance.
--
-- Provider cooldowns are durable source state. Existing route rows are stamped
-- by the provider recorder, while every route fact inserted after this migration
-- inherits the live provider horizons through the trigger below. The trigger is
-- intentionally INSERT-only: updating a fact must never clear route telemetry.

CREATE TABLE IF NOT EXISTS provider_cooldown_state (
  provider_key TEXT PRIMARY KEY,
  cooldown_until INTEGER,
  quota_exhausted_until INTEGER,
  quota_pressure REAL,
  last_error TEXT,
  last_error_at INTEGER,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS provider_cooldown_state_live_idx
  ON provider_cooldown_state(provider_key, cooldown_until, quota_exhausted_until);

CREATE INDEX IF NOT EXISTS model_route_state_provider_idx
  ON model_route_state(provider_id);

CREATE TRIGGER IF NOT EXISTS model_facts_inherit_provider_cooldown
AFTER INSERT ON model_facts
BEGIN
  INSERT OR IGNORE INTO model_route_state (
    route_key, capacity_key, provider_id, model_id, variant,
    cooldown_until, quota_exhausted_until, quota_pressure,
    latency_ema_ms, success_ema, failure_count, quota_error_count,
    last_error, last_error_at, updated_at
  )
  SELECT
    NEW.route_key, NEW.capacity_key, NEW.provider_id, NEW.model_id, NEW.variant,
    provider.cooldown_until, provider.quota_exhausted_until, provider.quota_pressure,
    NULL, NULL, 0, 0,
    provider.last_error, provider.last_error_at, NEW.updated_at
  FROM provider_cooldown_state AS provider
  WHERE provider.provider_key = NEW.provider_id;

  INSERT OR IGNORE INTO model_route_state (
    route_key, capacity_key, provider_id, model_id, variant,
    cooldown_until, quota_exhausted_until, quota_pressure,
    latency_ema_ms, success_ema, failure_count, quota_error_count,
    last_error, last_error_at, updated_at
  )
  SELECT
    NEW.route_key, NEW.capacity_key, NEW.provider_id, NEW.model_id, NEW.variant,
    NULL, NULL, NULL,
    NULL, NULL, 0, 0,
    NULL, NULL, NEW.updated_at
  WHERE NOT EXISTS (
    SELECT 1 FROM provider_cooldown_state AS provider
    WHERE provider.provider_key = NEW.provider_id
  );
END;

-- Existing databases already have facts but may have no route-state rows for
-- them. Materialize all catalog routes so provider fanout reaches every route
-- without adding a provider lookup or scan to admission.
INSERT OR IGNORE INTO model_route_state (
  route_key, capacity_key, provider_id, model_id, variant,
  cooldown_until, quota_exhausted_until, quota_pressure,
  latency_ema_ms, success_ema, failure_count, quota_error_count,
  last_error, last_error_at, updated_at
)
SELECT
  facts.route_key, facts.capacity_key, facts.provider_id, facts.model_id, facts.variant,
  provider.cooldown_until, provider.quota_exhausted_until, provider.quota_pressure,
  NULL, NULL, 0, 0,
  provider.last_error, provider.last_error_at, facts.updated_at
FROM model_facts AS facts
LEFT JOIN provider_cooldown_state AS provider
  ON provider.provider_key = facts.provider_id;
