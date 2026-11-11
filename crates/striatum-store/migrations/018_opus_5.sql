-- Claude Opus 5, added 2026-07-25 (Broca serves it already). Ufuk's curation:
-- one IQ point below Fable 5, same EQ. Remaining fields carry from Fable, which
-- is the closest sibling in the same family: same 1M context, same 64k output
-- ceiling, same $25/Mtok list, same vision/tool support.
--
-- Speed is carried from opus-4-8 (57) rather than Fable (55): Fable's figure is
-- the one we have the least traffic behind, and the 4-8 measurement is the
-- better-observed anchor for an opus-line model. It is a tiebreak input only.
--
-- Auto-routing: the provider-wide "anthropic" entry in model_routing.exclude
-- keeps every Anthropic route pin-only, so this row exists to make roster and
-- agent pins RESOLVE, not to put Opus 5 in the automatic pool. Fable stays
-- permanently pin-only per rule #9227; Opus 5 inherits the same treatment until
-- explicitly re-admitted with a negation entry.
--
-- The -fast sibling doubles list price exactly as opus-4-8-fast does.
INSERT OR IGNORE INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, family, max_output_tokens, updated_at) VALUES
  ('anthropic/claude-opus-5', 'anthropic/claude-opus-5', 'anthropic', 'claude-opus-5', NULL, '{"iq":58.0,"eq":2160.0,"speed":57.0}', 1000000, 3, 25.0, 1, 1, '{"variant":"xhigh"}', 'curated', 'Ufuk curation 2026-07-25: iq one below Fable 5, eq equal to Fable 5; speed carried from opus-4-8', 'seed', 'anthropic', 64000, 0),
  ('anthropic/claude-opus-5-fast', 'anthropic/claude-opus-5-fast', 'anthropic', 'claude-opus-5-fast', NULL, '{"iq":58.0,"eq":2160.0,"speed":57.0}', 1000000, 3, 50.0, 1, 1, '{"variant":"xhigh"}', 'curated', 'Fast sibling of claude-opus-5; list price doubles as it does for opus-4-8-fast', 'seed', 'anthropic', 64000, 0);
