-- fable-5 seed (added 2026-07-16). Deliberately ABSENT from the AA v4.1 rebase:
-- the user routes to it only via explicit pins (athena roster members, agent model
-- pins), never through automatic selection: the provider-wide "anthropic" entry in
-- model_routing.exclude keeps it out of the router either way. It still needs a
-- fact row because roster-mode panel seating resolves every member through
-- model_facts and drops pins without one (MissingFact). AA Intelligence Index
-- 2026-07-08: 59 (1st). eq curated above opus-4-8 (2115.5): fable's long-form and
-- prose judgment is the family's strongest. speed 55 ~ opus-class. Same $25/MTok
-- out bracket and 1M context as opus-4-8; reasoning variants low..max, default high.
INSERT OR IGNORE INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, family, updated_at) VALUES ('anthropic/claude-fable-5', 'anthropic/claude-fable-5', 'anthropic', 'claude-fable-5', NULL, '{"iq":59,"eq":2160,"speed":55}', 1000000, 3, 25, 1, 1, '{"variant":"high"}', 'curated', 'AA Intelligence Index 59 (2026-07-08); eq curated above opus-4-8; pin-only by policy', 'seed', 'anthropic', 0);
