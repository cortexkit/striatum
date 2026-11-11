-- Gemini 3.6 Flash on the antigravity (cloudcode-pa) path, added 2026-07-22.
-- Scores carried verbatim from antigravity-gemini-3.5-flash: same capability
-- class, but faster, cheaper, and less token-hungry per Ufuk's call, so 3.5 is
-- simultaneously retired from auto-routing via model_routing.exclude in the
-- global alfonso.jsonc (config-side, like the pre-5.6 GPT retirement). The 3.5
-- fact row stays for family consistency and explicit pins.
INSERT OR IGNORE INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, family, updated_at) VALUES ('google/antigravity-gemini-3.6-flash', 'google/antigravity-gemini-3.6-flash', 'google', 'antigravity-gemini-3.6-flash', NULL, '{"iq":44.0,"eq":1490.0,"speed":222.0}', 1048576, 3, 9.0, 1, 1, '{"variant":"high"}', 'curated', 'Scores carried from antigravity-gemini-3.5-flash (retired to exclude list 2026-07-22)', 'seed', 'google-gemini', 0);
