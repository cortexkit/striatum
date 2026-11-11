-- Keep only the newest evaluation of each engine/metric/reference-set identity.
-- Zero-padded decimal timestamps preserve the contract's full unsigned range
-- while retaining chronological order in SQLite's text comparison.
CREATE TABLE routing_engine_eval (
  kind TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  metric TEXT NOT NULL,
  set_id TEXT NOT NULL,
  score REAL NOT NULL CHECK (score >= 0 AND score <= 1),
  measured_at_ms TEXT NOT NULL,
  PRIMARY KEY (kind, provider, model, metric, set_id)
) WITHOUT ROWID;

-- The writer prunes each engine to its last twenty reports in the same transaction.
-- Counts use decimal text so even unsigned values above i64::MAX round-trip.
CREATE TABLE routing_engine_speed (
  sequence INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  outcome TEXT NOT NULL,
  duration_ms TEXT,
  wall_ms TEXT NOT NULL,
  first_output_ms TEXT
);
CREATE INDEX routing_engine_speed_engine_sequence
  ON routing_engine_speed(kind, provider, model, sequence DESC);

-- Deduplication lasts as long as the decision, even after its sample is pruned.
ALTER TABLE routing_v1_decision ADD COLUMN engine_reported INTEGER NOT NULL DEFAULT 0;
