-- alibaba-token-plan text-model expansion (2026-07-21). The token plan serves
-- the full open-weight stable alongside qwen3.8; scores CARRY OVER from the
-- ollama-cloud twins where those exist (same open-weight models, different
-- provider), and the plan-only entries are curated. Purpose: stop burning the
-- 2.4T qwen3.8 window on classify/gather - deepseek-v4-flash is the intended
-- cheap workhorse. Image models (qwen-image, wan2.7) are deliberately absent:
-- no text-routing use. cost_per_mtok_out is the metered list-price reference;
-- the subscription entry in alfonso.jsonc zero-costs the provider for routing.
-- Speeds are conservative pending TPS observation (the observer overlays).

-- Open-weight twins carried from ollama-cloud curations:
INSERT OR IGNORE INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, family, updated_at) VALUES
('alibaba-token-plan/deepseek-v4-flash', 'alibaba-token-plan/deepseek-v4-flash', 'alibaba-token-plan', 'deepseek-v4-flash', NULL, '{"iq":42,"eq":1494.2,"speed":80}', 128000, 1, 0.8, 0, 1, '{}', 'curated', 'carried from ollama-cloud twin; intended classify/gather workhorse', 'seed', 'deepseek', 0),
('alibaba-token-plan/deepseek-v4-pro', 'alibaba-token-plan/deepseek-v4-pro', 'alibaba-token-plan', 'deepseek-v4-pro', NULL, '{"iq":44,"eq":1599.8,"speed":40}', 128000, 2, 3, 0, 1, '{}', 'curated', 'carried from ollama-cloud twin', 'seed', 'deepseek', 0),
('alibaba-token-plan/glm-5.2', 'alibaba-token-plan/glm-5.2', 'alibaba-token-plan', 'glm-5.2', NULL, '{"iq":49,"eq":1565,"speed":60}', 200000, 2, 2.2, 0, 1, '{}', 'curated', 'carried from ollama-cloud twin', 'seed', 'glm', 0),
('alibaba-token-plan/glm-5.1', 'alibaba-token-plan/glm-5.1', 'alibaba-token-plan', 'glm-5.1', NULL, '{"iq":47,"eq":1557,"speed":60}', 200000, 1, 1.5, 0, 1, '{}', 'curated', 'carried from ollama-cloud twin', 'seed', 'glm', 0),
('alibaba-token-plan/kimi-k2.5', 'alibaba-token-plan/kimi-k2.5', 'alibaba-token-plan', 'kimi-k2.5', NULL, '{"iq":35,"eq":1541.7,"speed":70}', 256000, 1, 1, 0, 1, '{}', 'curated', 'carried from ollama-cloud twin', 'seed', 'kimi', 0),
('alibaba-token-plan/kimi-k2.6', 'alibaba-token-plan/kimi-k2.6', 'alibaba-token-plan', 'kimi-k2.6', NULL, '{"iq":44,"eq":1575.8,"speed":60}', 256000, 2, 2, 0, 1, '{}', 'curated', 'carried from ollama-cloud twin', 'seed', 'kimi', 0),
('alibaba-token-plan/kimi-k2.7-code', 'alibaba-token-plan/kimi-k2.7-code', 'alibaba-token-plan', 'kimi-k2.7-code', NULL, '{"iq":42,"eq":1570,"speed":60}', 256000, 2, 2, 0, 1, '{}', 'curated', 'carried from ollama-cloud twin', 'seed', 'kimi', 0),
('alibaba-token-plan/MiniMax-M2.5', 'alibaba-token-plan/MiniMax-M2.5', 'alibaba-token-plan', 'MiniMax-M2.5', NULL, '{"iq":36,"eq":1490,"speed":80}', 200000, 1, 0.6, 0, 1, '{}', 'curated', 'carried from ollama-cloud minimax-m2.5 twin (provider spells it MiniMax-M2.5)', 'seed', 'minimax', 0),
('alibaba-token-plan/deepseek-v3.2', 'alibaba-token-plan/deepseek-v3.2', 'alibaba-token-plan', 'deepseek-v3.2', NULL, '{"iq":38,"eq":1470,"speed":70}', 128000, 1, 0.6, 0, 1, '{}', 'curated', 'older deepseek generation, curated below v4-flash', 'seed', 'deepseek', 0),
('alibaba-token-plan/glm-5', 'alibaba-token-plan/glm-5', 'alibaba-token-plan', 'glm-5', NULL, '{"iq":44,"eq":1540,"speed":60}', 128000, 1, 1, 0, 1, '{}', 'curated', 'older glm generation, curated below 5.1/5.2', 'seed', 'glm', 0),
('alibaba-token-plan/qwen3.6-flash', 'alibaba-token-plan/qwen3.6-flash', 'alibaba-token-plan', 'qwen3.6-flash', NULL, '{"iq":34,"eq":1440,"speed":110}', 128000, 1, 0.4, 0, 1, '{}', 'curated', 'flash tier, fastest of the plan', 'seed', 'qwen', 0),
('alibaba-token-plan/qwen3.6-plus', 'alibaba-token-plan/qwen3.6-plus', 'alibaba-token-plan', 'qwen3.6-plus', NULL, '{"iq":40,"eq":1500,"speed":75}', 256000, 1, 0.8, 0, 1, '{}', 'curated', 'plus tier between flash and max', 'seed', 'qwen', 0),
('alibaba-token-plan/qwen3.7-plus', 'alibaba-token-plan/qwen3.7-plus', 'alibaba-token-plan', 'qwen3.7-plus', NULL, '{"iq":44,"eq":1530,"speed":70}', 256000, 2, 1.6, 0, 1, '{}', 'curated', 'plus tier, 3.7 generation', 'seed', 'qwen', 0),
('alibaba-token-plan/qwen3.7-max', 'alibaba-token-plan/qwen3.7-max', 'alibaba-token-plan', 'qwen3.7-max', NULL, '{"iq":50,"eq":1620,"speed":50}', 256000, 3, 5, 0, 1, '{}', 'curated', 'prior max generation, below 3.8-preview', 'seed', 'qwen', 0);
