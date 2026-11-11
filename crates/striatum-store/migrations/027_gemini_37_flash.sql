-- Gemini 3.7 Flash, added 2026-08-14. Follows the 3.5->3.6 succession pattern
-- (migration 017): the new flash inherits its predecessor's scores until AA
-- publishes an index for it, and the OLD flash is excluded so selection
-- prefers the newest build (same-scores siblings would otherwise tie and
-- split on speed samples, which mean nothing across model versions).
--
-- Scores carried from antigravity-gemini-3.6-flash: elo 1410, eq 1490,
-- speed 222. Vision follows the gemini family verified-yes from migration
-- 025. Price and context carried from 3.6-flash.
INSERT OR IGNORE INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, family, max_output_tokens, updated_at) VALUES
  ('google/antigravity-gemini-3.7-flash', 'google/antigravity-gemini-3.7-flash', 'google', 'antigravity-gemini-3.7-flash', NULL, '{"elo":1410.0,"eq":1490.0,"speed":222.0}', 1000000, 1, 9.0, 1, 1, NULL, 'curated', 'Succession seed 2026-08-14: scores carried from 3.6-flash pending AA index; supersedes 3.6-flash in selection via the exclusion below', 'seed', 'google-gemini', 65536, 0);

-- Retirement of 3.6-flash happens where 017 retired 3.5-flash: the
-- model_routing.exclude list in alfonso-routing.jsonc (config, live-reloaded),
-- not here. The fact row stays for history and explicit pins.
