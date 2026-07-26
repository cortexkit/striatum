-- Migration 1 (namespace "route_state"): the route-owned schema + the curated
-- model-facts seed. Applied EXACTLY ONCE by cortexkit-store's namespace-keyed
-- migration runner. This file is FROZEN: a versioned migration must never change
-- after it has been applied to any DB (later schema/seed changes are NEW
-- migrations). Quota tables are NOT here; they belong to ai-provider-quota and
-- are consumed module-to-module.
--
-- The 33 seed INSERTs are generated from the canonical model-facts JSON
-- (alfonso-routing-core/src/model_facts_seed.json). Regeneration helper:
-- script/gen-seed-migration.sh (writes a NEW migration, never edits this one).

CREATE TABLE IF NOT EXISTS model_facts (
  route_key TEXT PRIMARY KEY,
  capacity_key TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  model_id TEXT NOT NULL,
  variant TEXT,
  scores_json TEXT NOT NULL,
  context_window INTEGER,
  cost_tier INTEGER,
  cost_per_mtok_out REAL,
  supports_vision INTEGER,
  supports_tool_use INTEGER,
  prompt_defaults_json TEXT,
  confidence TEXT NOT NULL,
  notes TEXT,
  source TEXT NOT NULL DEFAULT 'seed',
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS model_route_state (
  route_key TEXT PRIMARY KEY,
  capacity_key TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  model_id TEXT NOT NULL,
  variant TEXT,
  cooldown_until INTEGER,
  quota_exhausted_until INTEGER,
  in_flight_limit INTEGER,
  quota_pressure REAL,
  latency_ema_ms REAL,
  success_ema REAL,
  failure_count INTEGER NOT NULL DEFAULT 0,
  quota_error_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  last_error_at INTEGER,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS model_route_state_capacity_idx
  ON model_route_state(capacity_key);

CREATE TABLE IF NOT EXISTS model_route_reservations (
  id TEXT PRIMARY KEY,
  task_id TEXT,
  route_key TEXT NOT NULL,
  capacity_key TEXT NOT NULL,
  owner_epoch TEXT,
  parent_session_id TEXT,
  child_session_id TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  started_at INTEGER NOT NULL,
  released_at INTEGER,
  release_reason TEXT
);

CREATE INDEX IF NOT EXISTS model_route_reservations_active_idx
  ON model_route_reservations(capacity_key) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS model_route_reservations_task_idx
  ON model_route_reservations(task_id);
CREATE INDEX IF NOT EXISTS model_route_reservations_owner_idx
  ON model_route_reservations(owner_epoch) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS model_route_decisions (
  id TEXT PRIMARY KEY,
  task_id TEXT,
  parent_session_id TEXT,
  target_agent TEXT NOT NULL,
  requirements_json TEXT NOT NULL,
  selected_route_key TEXT,
  selected_provider_id TEXT,
  selected_model_id TEXT,
  selected_variant TEXT,
  score_json TEXT NOT NULL,
  rejected_json TEXT NOT NULL,
  explanation_json TEXT,
  anchor_version INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  outcome TEXT,
  outcome_at INTEGER
);

CREATE INDEX IF NOT EXISTS model_route_decisions_task_idx
  ON model_route_decisions(task_id);

-- Curated model-facts seed (33 rows). Run-once via the migration runner.
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('anthropic/claude-opus-4-5', 'anthropic/claude-opus-4-5', 'anthropic', 'claude-opus-4-5', NULL, '{"iq":49.7,"eq":1698,"speed":53}', 200000, 4, 25, 1, 1, '{"variant":"high"}', 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('anthropic/claude-opus-4-6', 'anthropic/claude-opus-4-6', 'anthropic', 'claude-opus-4-6', NULL, '{"iq":52.9,"eq":1751.7,"speed":48}', 1000000, 4, 25, 1, 1, '{"variant":"max"}', 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('anthropic/claude-opus-4-6-fast', 'anthropic/claude-opus-4-6-fast', 'anthropic', 'claude-opus-4-6-fast', NULL, '{"iq":52.9,"eq":1751.7,"speed":48}', 1000000, 6, 150, 1, 1, '{"variant":"max"}', 'curated', 'fast-mode route (priority tier); speed bump pending TPS observation', 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('anthropic/claude-opus-4-7', 'anthropic/claude-opus-4-7', 'anthropic', 'claude-opus-4-7', NULL, '{"iq":57.3,"eq":1949.8,"speed":52}', 1000000, 4, 25, 1, 1, '{"variant":"max"}', 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('anthropic/claude-opus-4-7-fast', 'anthropic/claude-opus-4-7-fast', 'anthropic', 'claude-opus-4-7-fast', NULL, '{"iq":57.3,"eq":1949.8,"speed":52}', 1000000, 6, 150, 1, 1, '{"variant":"max"}', 'curated', 'fast-mode route (priority tier); speed bump pending TPS observation', 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('anthropic/claude-opus-4-8', 'anthropic/claude-opus-4-8', 'anthropic', 'claude-opus-4-8', NULL, '{"iq":61.4,"eq":2115.5,"speed":57}', 1000000, 4, 25, 1, 1, '{"variant":"max"}', 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('anthropic/claude-opus-4-8-fast', 'anthropic/claude-opus-4-8-fast', 'anthropic', 'claude-opus-4-8-fast', NULL, '{"iq":61.4,"eq":2115.5,"speed":57}', 1000000, 5, 50, 1, 1, '{"variant":"max"}', 'curated', 'fast-mode route (priority tier); speed bump pending TPS observation', 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('anthropic/claude-sonnet-4-6', 'anthropic/claude-sonnet-4-6', 'anthropic', 'claude-sonnet-4-6', NULL, '{"iq":51.7,"eq":1752.4,"speed":61}', 1000000, 3, 15, 1, 1, '{"variant":"max"}', 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('google/antigravity-claude-opus-4-6-thinking', 'google/antigravity-claude-opus-4-6-thinking', 'google', 'antigravity-claude-opus-4-6-thinking', NULL, '{"iq":52.9,"eq":1751.7,"speed":48}', 1000000, 4, 25, 1, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('google/antigravity-claude-sonnet-4-6-thinking', 'google/antigravity-claude-sonnet-4-6-thinking', 'google', 'antigravity-claude-sonnet-4-6-thinking', NULL, '{"iq":51.7,"eq":1752.4,"speed":61}', 1000000, 3, 15, 1, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('google/antigravity-gemini-3.1-pro', 'google/antigravity-gemini-3.1-pro', 'google', 'antigravity-gemini-3.1-pro', NULL, '{"iq":57.2,"eq":1532.6,"speed":123}', 1048576, 3, 12, 1, 1, '{"variant":"high"}', 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('google/antigravity-gemini-3.5-flash', 'google/antigravity-gemini-3.5-flash', 'google', 'antigravity-gemini-3.5-flash', NULL, '{"iq":55.3,"eq":1490,"speed":222}', 1048576, 3, 9, 1, 1, '{"variant":"high"}', 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('openai/gpt-5.4', 'openai/gpt-5.4', 'openai', 'gpt-5.4', NULL, '{"iq":56.8,"eq":1585.7,"speed":80}', 1050000, 3, 15, 1, 1, '{"variant":"xhigh"}', 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('openai/gpt-5.4-fast', 'openai/gpt-5.4-fast', 'openai', 'gpt-5.4-fast', NULL, '{"iq":56.8,"eq":1585.7,"speed":80}', 1050000, 4, 30, 1, 1, '{"variant":"xhigh"}', 'curated', 'fast-mode route (priority tier); speed bump pending TPS observation', 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('openai/gpt-5.4-mini', 'openai/gpt-5.4-mini', 'openai', 'gpt-5.4-mini', NULL, '{"iq":48.9,"eq":1430,"speed":157}', 400000, 2, 4.5, 1, 1, '{"variant":"xhigh"}', 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('openai/gpt-5.4-mini-fast', 'openai/gpt-5.4-mini-fast', 'openai', 'gpt-5.4-mini-fast', NULL, '{"iq":48.9,"eq":1430,"speed":157}', 400000, 3, 9, 1, 1, '{"variant":"xhigh"}', 'curated', 'fast-mode route (priority tier); speed bump pending TPS observation', 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('openai/gpt-5.5', 'openai/gpt-5.5', 'openai', 'gpt-5.5', NULL, '{"iq":60.2,"eq":1596.5,"speed":58}', 1050000, 4, 30, 1, 1, '{"variant":"xhigh"}', 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('openai/gpt-5.5-fast', 'openai/gpt-5.5-fast', 'openai', 'gpt-5.5-fast', NULL, '{"iq":60.2,"eq":1596.5,"speed":58}', 1050000, 5, 75, 1, 1, '{"variant":"xhigh"}', 'curated', 'fast-mode route (priority tier); speed bump pending TPS observation', 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/deepseek-v4-flash', 'opencode-go/deepseek-v4-flash', 'opencode-go', 'deepseek-v4-flash', NULL, '{"iq":46.5,"eq":1494.2,"speed":113}', 1000000, 0, 0.28, NULL, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/deepseek-v4-pro', 'opencode-go/deepseek-v4-pro', 'opencode-go', 'deepseek-v4-pro', NULL, '{"iq":51.5,"eq":1599.8,"speed":48}', 1000000, 2, 3.48, NULL, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/glm-5', 'opencode-go/glm-5', 'opencode-go', 'glm-5', NULL, '{"iq":49.8,"eq":1533.2,"speed":81}', 202752, 2, 3.2, NULL, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/glm-5.1', 'opencode-go/glm-5.1', 'opencode-go', 'glm-5.1', NULL, '{"iq":51.4,"eq":1557,"speed":52}', 202752, 2, 4.4, NULL, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/kimi-k2.5', 'opencode-go/kimi-k2.5', 'opencode-go', 'kimi-k2.5', NULL, '{"iq":46.8,"eq":1541.7,"speed":35}', 262144, 1, 3, 1, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/kimi-k2.6', 'opencode-go/kimi-k2.6', 'opencode-go', 'kimi-k2.6', NULL, '{"iq":53.9,"eq":1575.8,"speed":42}', 262144, 2, 4, 1, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/minimax-m2.5', 'opencode-go/minimax-m2.5', 'opencode-go', 'minimax-m2.5', NULL, '{"iq":41.9,"eq":1490,"speed":206}', 204800, 1, 1.2, NULL, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/minimax-m2.7', 'opencode-go/minimax-m2.7', 'opencode-go', 'minimax-m2.7', NULL, '{"iq":49.6,"eq":1510,"speed":65}', 204800, 1, 1.2, NULL, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/minimax-m3', 'opencode-go/minimax-m3', 'opencode-go', 'minimax-m3', NULL, '{"iq":54.7,"eq":1520,"speed":44}', 512000, 1, 2.4, 1, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/qwen3.6-plus', 'opencode-go/qwen3.6-plus', 'opencode-go', 'qwen3.6-plus', NULL, '{"iq":50,"eq":1510,"speed":52}', 1000000, 1, 3, 1, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/qwen3.7-max', 'opencode-go/qwen3.7-max', 'opencode-go', 'qwen3.7-max', NULL, '{"iq":56.6,"eq":1690,"speed":117}', 1000000, 3, 7.5, NULL, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('opencode-go/qwen3.7-plus', 'opencode-go/qwen3.7-plus', 'opencode-go', 'qwen3.7-plus', NULL, '{"iq":53.3,"eq":1533,"speed":53}', 1000000, 1, 1.6, 1, 1, NULL, 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('xai/grok-4.3', 'xai/grok-4.3', 'xai', 'grok-4.3', NULL, '{"iq":53.2,"eq":1480,"speed":187}', 1000000, 1, 2.5, 1, 1, '{"variant":"high"}', 'curated', NULL, 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('xai/grok-build-0.1', 'xai/grok-build-0.1', 'xai', 'grok-build-0.1', NULL, '{"iq":54,"eq":1530,"speed":156}', NULL, NULL, NULL, NULL, 1, NULL, 'curated', 'xai subscription-only; iq/speed/eq user-curated (absent from AA + models.dev)', 'seed', 0);
INSERT INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, updated_at) VALUES ('xai/grok-composer-2.5-fast', 'xai/grok-composer-2.5-fast', 'xai', 'grok-composer-2.5-fast', NULL, '{"iq":56,"eq":1604,"speed":184}', NULL, NULL, NULL, NULL, 1, NULL, 'curated', 'xai subscription-only (Cursor composer-2.5); iq/speed/eq user-curated (absent from AA + models.dev)', 'seed', 0);
