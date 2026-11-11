-- Generation timings are observations, not another delivery of billed usage.
CREATE TABLE model_route_speed_samples (
  id TEXT PRIMARY KEY,
  route_key TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  model_id TEXT NOT NULL,
  variant TEXT,
  service_tier TEXT,
  speed TEXT,
  -- Derived population keys permit bounded indexed reads without rewriting raw words.
  population_service_tier TEXT,
  population_speed TEXT,
  output_tokens INTEGER NOT NULL,
  reasoning_tokens INTEGER,
  duration_ms INTEGER NOT NULL,
  observed_at INTEGER NOT NULL
);

CREATE INDEX model_route_speed_samples_route_tier_observed_idx
  ON model_route_speed_samples(route_key, population_service_tier, population_speed, observed_at);
