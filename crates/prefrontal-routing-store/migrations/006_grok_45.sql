-- grok-4.5 seed (added 2026-07-16). AA Intelligence Index 2026-07-08: 54 (4th,
-- behind fable-5/gpt-5.5/opus-4.8): already on the rescaled scale our v3 anchors
-- calibrate to. eq curated between grok-4.3 (1480) and composer (1604), corroborated
-- by its GDPval-AA v2 Elo of 1543. speed 120 = AA-measured 2026-07-13 (xai claims 80;
-- the TPS observer overlays real observations either way). Context 500k (halved from
-- 4.3's 1M). $2/$6 per MTok in/out => cost_per_mtok_out 6, same bracket as luna.
-- Reasoning effort low|medium|high, default high: same prompt-default shape as 4.3.
INSERT OR IGNORE INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, family, updated_at) VALUES ('xai/grok-4.5', 'xai/grok-4.5', 'xai', 'grok-4.5', NULL, '{"iq":54,"eq":1550,"speed":120}', 500000, 2, 6, 1, 1, '{"variant":"high"}', 'curated', 'AA Intelligence Index 54 (2026-07-08); eq curated; speed AA-measured 120 tok/s', 'seed', 'xai-grok', 0);

-- grok-4.3 iq was seeded pre-rescale (53.2 on the old scale); AA has it at 38 on the
-- current scale. It is routing-excluded by config, but keep the family internally
-- consistent. Guarded to only overwrite the stale seeded value, preserving any
-- deliberate user override.
UPDATE model_facts
SET scores_json = '{"iq":38,"eq":1480,"speed":187}'
WHERE route_key = 'xai/grok-4.3'
  AND scores_json = '{"iq":53.2,"eq":1480,"speed":187}';
