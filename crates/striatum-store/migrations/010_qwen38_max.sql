-- alibaba-token-plan/qwen3.8-max-preview seed (added 2026-07-19, new Alibaba
-- token-plan subscription; already pinned in the athena and campaign rosters).
-- No AA score exists for the preview yet: curated iq 53 (peer of grok-4.5,
-- below opus/k3 at 55) pending real-work evidence, eq 1560 (grok-class prose,
-- unproven long-form judgment), speed conservative pending TPS observation.
-- 256k context per the Qwen3 Max line. cost_per_mtok_out is the metered list
-- price reference; the subscription entry in alfonso.jsonc zero-costs it for
-- routing. family "qwen" (existing lineage label) gives panel diversity a
-- distinct family.
INSERT OR IGNORE INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, family, updated_at) VALUES ('alibaba-token-plan/qwen3.8-max-preview', 'alibaba-token-plan/qwen3.8-max-preview', 'alibaba-token-plan', 'qwen3.8-max-preview', NULL, '{"iq":53,"eq":1560,"speed":45}', 256000, 3, 6, 0, 1, '{}', 'curated', 'curated grok-4.5-peer pending AA evidence; subscription-served', 'seed', 'qwen', 0);
