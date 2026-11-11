-- kimi-for-coding/k3 seed (added 2026-07-17, new Kimi coding subscription).
-- Vendor advertises opus-4.8-level intelligence; curated one point BELOW opus
-- (iq 55) until we see real work, and eq 200 below opus (1915.5): Kimi's
-- long-form judgment is unproven here. speed conservative pending TPS
-- observation. 256k context per the K3 announcement. cost_per_mtok_out is the
-- metered list price; the subscription entry in alfonso.jsonc zero-costs it
-- for routing. family "kimi" gives panel diversity a distinct lineage.
INSERT OR IGNORE INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, family, updated_at) VALUES ('kimi-for-coding/k3', 'kimi-for-coding/k3', 'kimi-for-coding', 'k3', NULL, '{"iq":55,"eq":1915.5,"speed":50}', 256000, 3, 8, 0, 1, '{}', 'curated', 'curated 1 iq below opus-4-8 pending real-work evidence; eq opus-200; subscription-served', 'seed', 'kimi', 0);
