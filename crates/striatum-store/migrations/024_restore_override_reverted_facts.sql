-- Restore three migration-seeded facts deleted by the config override-revert
-- path (2026-08-10). Mechanism: when a per-model override entry disappears
-- from alfonso-routing.jsonc, apply_overrides_from_config reverts the row to
-- the COMPILED JSON seed, and DELETES rows the JSON seed does not contain.
-- fable-5 (migration 008), opus-5/opus-5-fast (migration 018) exist only as
-- migrations, so removing their stale pre-Elo config overrides erased the
-- facts themselves - opus-5 then failed roster seating as missing_fact (spec
-- refire 5 ran a 1-seat panel). Values reproduce migration 018/008 rows with
-- 019's real prices and 022's Elo rescores already applied.
-- (qwen3.8-max-preview was also deleted by the same pass, correctly: it is
-- superseded by qwen3.8-max and stays gone.)
INSERT OR IGNORE INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, family, max_output_tokens, updated_at) VALUES
  ('anthropic/claude-fable-5', 'anthropic/claude-fable-5', 'anthropic', 'claude-fable-5', NULL, '{"elo":1911,"eq":2160,"speed":55}', 1000000, 3, 50.0, 1, 1, '{"variant":"high"}', 'curated', 'Restored by migration 024 after override-revert deletion; elo per 022, price per 019; pin-only by policy', 'seed', 'anthropic', NULL, strftime('%s','now') * 1000),
  ('anthropic/claude-opus-5', 'anthropic/claude-opus-5', 'anthropic', 'claude-opus-5', NULL, '{"elo":1905,"eq":2160,"speed":57}', 1000000, 3, 25.0, 1, 1, '{"variant":"xhigh"}', 'curated', 'Restored by migration 024 after override-revert deletion; elo per 022, price per 019', 'seed', 'anthropic', 64000, strftime('%s','now') * 1000),
  ('anthropic/claude-opus-5-fast', 'anthropic/claude-opus-5-fast', 'anthropic', 'claude-opus-5-fast', NULL, '{"elo":1905,"eq":2160,"speed":57}', 1000000, 3, 50.0, 1, 1, '{"variant":"xhigh"}', 'curated', 'Restored by migration 024 after override-revert deletion; elo per 022, price per 019', 'seed', 'anthropic', 64000, strftime('%s','now') * 1000);
