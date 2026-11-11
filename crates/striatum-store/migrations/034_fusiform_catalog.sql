-- Migration 34 (namespace "route_state"): durable Fusiform catalog cursor and
-- per-field fact provenance. The existing `source` column remains score
-- provenance; `facts_source` records the external factual authority separately.

ALTER TABLE model_facts ADD COLUMN facts_source TEXT;
ALTER TABLE model_facts ADD COLUMN fusiform_version INTEGER;
ALTER TABLE model_facts ADD COLUMN fusiform_present INTEGER;
ALTER TABLE model_facts ADD COLUMN context_window_source TEXT;
ALTER TABLE model_facts ADD COLUMN max_output_tokens_source TEXT;
ALTER TABLE model_facts ADD COLUMN cost_tier_source TEXT;
ALTER TABLE model_facts ADD COLUMN cost_per_mtok_out_source TEXT;
ALTER TABLE model_facts ADD COLUMN supports_vision_source TEXT;
ALTER TABLE model_facts ADD COLUMN supports_tool_use_source TEXT;

CREATE TABLE IF NOT EXISTS fusiform_catalog_cursor (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  catalog_version INTEGER NOT NULL,
  resolved_at_ms INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
