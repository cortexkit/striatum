-- Migration 33 (namespace "route_state"): AA-inferred provisional scores.
-- Additive only: existing catalog rows keep their source stamp and scores.
-- aa_inferred never overwrites curated (`seed`) or override rows; that
-- precedence is enforced in upsert, not here.

ALTER TABLE model_facts ADD COLUMN aa_index REAL;
ALTER TABLE model_facts ADD COLUMN review_by_ms INTEGER;
ALTER TABLE model_facts ADD COLUMN eq_source TEXT;

-- Latest Artificial Analysis intelligence index per AA slug. Replaced wholesale
-- on a successful fetch; a failed fetch leaves this table untouched (fail-open).
CREATE TABLE IF NOT EXISTS aa_index_cache (
  aa_slug TEXT PRIMARY KEY,
  aa_index REAL NOT NULL,
  fetched_at INTEGER NOT NULL
);
