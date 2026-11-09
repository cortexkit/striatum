-- Migration 39 (namespace "route_state"): retain the typed cause of a hard
-- route wall so break-glass can distinguish a retryable quota window from an
-- account that requires payment. Existing rows remain NULL and keep legacy behavior.

ALTER TABLE model_route_state
  ADD COLUMN wall_class TEXT CHECK (wall_class IN ('quota', 'billing'));

ALTER TABLE provider_cooldown_state
  ADD COLUMN wall_class TEXT CHECK (wall_class IN ('quota', 'billing'));

-- The provider row is the inheritance source for facts added after a provider-wide
-- wall. Recreate the insert trigger so those future route rows retain the cause too.
DROP TRIGGER IF EXISTS model_facts_inherit_provider_cooldown;

CREATE TRIGGER model_facts_inherit_provider_cooldown
AFTER INSERT ON model_facts
BEGIN
  INSERT OR IGNORE INTO model_route_state (
    route_key, capacity_key, provider_id, model_id, variant,
    cooldown_until, quota_exhausted_until, quota_pressure,
    latency_ema_ms, success_ema, failure_count, quota_error_count,
    last_error, last_error_at, updated_at, wall_class
  )
  SELECT
    NEW.route_key, NEW.capacity_key, NEW.provider_id, NEW.model_id, NEW.variant,
    provider.cooldown_until, provider.quota_exhausted_until, provider.quota_pressure,
    NULL, NULL, 0, 0,
    provider.last_error, provider.last_error_at, NEW.updated_at, provider.wall_class
  FROM provider_cooldown_state AS provider
  WHERE provider.provider_key = NEW.provider_id;

  INSERT OR IGNORE INTO model_route_state (
    route_key, capacity_key, provider_id, model_id, variant,
    cooldown_until, quota_exhausted_until, quota_pressure,
    latency_ema_ms, success_ema, failure_count, quota_error_count,
    last_error, last_error_at, updated_at, wall_class
  )
  SELECT
    NEW.route_key, NEW.capacity_key, NEW.provider_id, NEW.model_id, NEW.variant,
    NULL, NULL, NULL,
    NULL, NULL, 0, 0,
    NULL, NULL, NEW.updated_at, NULL
  WHERE NOT EXISTS (
    SELECT 1 FROM provider_cooldown_state AS provider
    WHERE provider.provider_key = NEW.provider_id
  );
END;
