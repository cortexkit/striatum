-- AA Intelligence Index v4.1 catalog rebase (2026-07-16).
-- Three parts in one migration:
--   1. Guarded iq UPDATEs (eq/speed unchanged): only rewrite scores_json when it
--      still equals the frozen pre-007 seed value so deliberate overrides survive
--      (same pattern as migration 006's grok-4.3 rescale).
--   2. DELETE all opencode-go/% rows (subscription lapsed).
--   3. INSERT ollama-cloud rows for the live entitlement (family per model_id).

-- ── Part 1: iq rebase (guarded) ─────────────────────────────────────────────

-- openai/gpt-5.6-sol family: AA v4.1 (xhigh) → 58
UPDATE model_facts SET scores_json = '{"iq":58,"eq":1610,"speed":55}'
WHERE route_key = 'openai/gpt-5.6-sol'
  AND scores_json = '{"iq":69.5,"eq":1610,"speed":55}';
UPDATE model_facts SET scores_json = '{"iq":58,"eq":1610,"speed":55}'
WHERE route_key = 'openai/gpt-5.6-sol-fast'
  AND scores_json = '{"iq":69.5,"eq":1610,"speed":55}';
UPDATE model_facts SET scores_json = '{"iq":58,"eq":1610,"speed":15}'
WHERE route_key = 'openai/gpt-5.6-sol-pro'
  AND scores_json = '{"iq":69.5,"eq":1610,"speed":15}';

-- anthropic/claude-opus-4-8 family: AA v4.1 (max) → 56
UPDATE model_facts SET scores_json = '{"iq":56,"eq":2115.5,"speed":57}'
WHERE route_key = 'anthropic/claude-opus-4-8'
  AND scores_json = '{"iq":61.4,"eq":2115.5,"speed":57}';
UPDATE model_facts SET scores_json = '{"iq":56,"eq":2115.5,"speed":57}'
WHERE route_key = 'anthropic/claude-opus-4-8-fast'
  AND scores_json = '{"iq":61.4,"eq":2115.5,"speed":57}';

-- openai/gpt-5.5 family: AA v4.1 (xhigh) → 56
UPDATE model_facts SET scores_json = '{"iq":56,"eq":1596.5,"speed":58}'
WHERE route_key = 'openai/gpt-5.5'
  AND scores_json = '{"iq":60.2,"eq":1596.5,"speed":58}';
UPDATE model_facts SET scores_json = '{"iq":56,"eq":1596.5,"speed":58}'
WHERE route_key = 'openai/gpt-5.5-fast'
  AND scores_json = '{"iq":60.2,"eq":1596.5,"speed":58}';

-- openai/gpt-5.6-terra family: user curation 2026-07-16 → 55
UPDATE model_facts SET scores_json = '{"iq":55,"eq":1590,"speed":95}'
WHERE route_key = 'openai/gpt-5.6-terra'
  AND scores_json = '{"iq":59.9,"eq":1590,"speed":95}';
UPDATE model_facts SET scores_json = '{"iq":55,"eq":1590,"speed":95}'
WHERE route_key = 'openai/gpt-5.6-terra-fast'
  AND scores_json = '{"iq":59.9,"eq":1590,"speed":95}';
UPDATE model_facts SET scores_json = '{"iq":55,"eq":1590,"speed":22}'
WHERE route_key = 'openai/gpt-5.6-terra-pro'
  AND scores_json = '{"iq":59.9,"eq":1590,"speed":22}';

-- xai/grok-4.5: user curation (AA 56 minus 3) → 53
UPDATE model_facts SET scores_json = '{"iq":53,"eq":1550,"speed":120}'
WHERE route_key = 'xai/grok-4.5'
  AND scores_json = '{"iq":54,"eq":1550,"speed":120}';

-- openai/gpt-5.6-luna family: user curation → 53
UPDATE model_facts SET scores_json = '{"iq":53,"eq":1555,"speed":160}'
WHERE route_key = 'openai/gpt-5.6-luna'
  AND scores_json = '{"iq":57.5,"eq":1555,"speed":160}';
UPDATE model_facts SET scores_json = '{"iq":53,"eq":1555,"speed":160}'
WHERE route_key = 'openai/gpt-5.6-luna-fast'
  AND scores_json = '{"iq":57.5,"eq":1555,"speed":160}';
UPDATE model_facts SET scores_json = '{"iq":53,"eq":1555,"speed":35}'
WHERE route_key = 'openai/gpt-5.6-luna-pro'
  AND scores_json = '{"iq":57.5,"eq":1555,"speed":35}';

-- anthropic/claude-opus-4-7 family: bridge estimate → 53
UPDATE model_facts SET scores_json = '{"iq":53,"eq":1949.8,"speed":52}'
WHERE route_key = 'anthropic/claude-opus-4-7'
  AND scores_json = '{"iq":57.3,"eq":1949.8,"speed":52}';
UPDATE model_facts SET scores_json = '{"iq":53,"eq":1949.8,"speed":52}'
WHERE route_key = 'anthropic/claude-opus-4-7-fast'
  AND scores_json = '{"iq":57.3,"eq":1949.8,"speed":52}';

-- openai/gpt-5.4 family: bridge estimate → 52
UPDATE model_facts SET scores_json = '{"iq":52,"eq":1585.7,"speed":80}'
WHERE route_key = 'openai/gpt-5.4'
  AND scores_json = '{"iq":56.8,"eq":1585.7,"speed":80}';
UPDATE model_facts SET scores_json = '{"iq":52,"eq":1585.7,"speed":80}'
WHERE route_key = 'openai/gpt-5.4-fast'
  AND scores_json = '{"iq":56.8,"eq":1585.7,"speed":80}';

-- anthropic/claude-opus-4-6 family + google hosted twin: bridge → 49
UPDATE model_facts SET scores_json = '{"iq":49,"eq":1751.7,"speed":48}'
WHERE route_key = 'anthropic/claude-opus-4-6'
  AND scores_json = '{"iq":52.9,"eq":1751.7,"speed":48}';
UPDATE model_facts SET scores_json = '{"iq":49,"eq":1751.7,"speed":48}'
WHERE route_key = 'anthropic/claude-opus-4-6-fast'
  AND scores_json = '{"iq":52.9,"eq":1751.7,"speed":48}';
UPDATE model_facts SET scores_json = '{"iq":49,"eq":1751.7,"speed":48}'
WHERE route_key = 'google/antigravity-claude-opus-4-6-thinking'
  AND scores_json = '{"iq":52.9,"eq":1751.7,"speed":48}';

-- google/antigravity-gemini-3.1-pro: bridge ~52 minus 4-point demotion → 48
UPDATE model_facts SET scores_json = '{"iq":48,"eq":1532.6,"speed":123}'
WHERE route_key = 'google/antigravity-gemini-3.1-pro'
  AND scores_json = '{"iq":57.2,"eq":1532.6,"speed":123}';

-- anthropic/claude-sonnet-4-6 + google hosted twin: bridge → 48
UPDATE model_facts SET scores_json = '{"iq":48,"eq":1752.4,"speed":61}'
WHERE route_key = 'anthropic/claude-sonnet-4-6'
  AND scores_json = '{"iq":51.7,"eq":1752.4,"speed":61}';
UPDATE model_facts SET scores_json = '{"iq":48,"eq":1752.4,"speed":61}'
WHERE route_key = 'google/antigravity-claude-sonnet-4-6-thinking'
  AND scores_json = '{"iq":51.7,"eq":1752.4,"speed":61}';

-- anthropic/claude-opus-4-5: bridge → 46
UPDATE model_facts SET scores_json = '{"iq":46,"eq":1698,"speed":53}'
WHERE route_key = 'anthropic/claude-opus-4-5'
  AND scores_json = '{"iq":49.7,"eq":1698,"speed":53}';

-- openai/gpt-5.4-mini family: bridge → 45
UPDATE model_facts SET scores_json = '{"iq":45,"eq":1430,"speed":157}'
WHERE route_key = 'openai/gpt-5.4-mini'
  AND scores_json = '{"iq":48.9,"eq":1430,"speed":157}';
UPDATE model_facts SET scores_json = '{"iq":45,"eq":1430,"speed":157}'
WHERE route_key = 'openai/gpt-5.4-mini-fast'
  AND scores_json = '{"iq":48.9,"eq":1430,"speed":157}';

-- xai/grok-composer-2.5-fast: user curation → 44
UPDATE model_facts SET scores_json = '{"iq":44,"eq":1604,"speed":184}'
WHERE route_key = 'xai/grok-composer-2.5-fast'
  AND scores_json = '{"iq":56,"eq":1604,"speed":184}';

-- google/antigravity-gemini-3.5-flash: user curation → 44
UPDATE model_facts SET scores_json = '{"iq":44,"eq":1490,"speed":222}'
WHERE route_key = 'google/antigravity-gemini-3.5-flash'
  AND scores_json = '{"iq":55.3,"eq":1490,"speed":222}';

-- xai/grok-build-0.1: user curation → 40
UPDATE model_facts SET scores_json = '{"iq":40,"eq":1530,"speed":156}'
WHERE route_key = 'xai/grok-build-0.1'
  AND scores_json = '{"iq":54,"eq":1530,"speed":156}';

-- xai/grok-4.3: post-006 frozen value is iq 38; v4.1 keeps ~34 → 34
UPDATE model_facts SET scores_json = '{"iq":34,"eq":1480,"speed":187}'
WHERE route_key = 'xai/grok-4.3'
  AND scores_json = '{"iq":38,"eq":1480,"speed":187}';

-- ── Part 2: drop lapsed opencode-go subscription rows ───────────────────────

DELETE FROM model_facts WHERE route_key LIKE 'opencode-go/%';

-- ── Part 3: seed ollama-cloud entitlement rows ──────────────────────────────
-- eq/speed/context/cost carried from the deleted opencode-go curation of the
-- same model where one existed (frozen in migration 001). family follows the
-- migration 005 CASE mapping; mistral-large-3 gets family 'mistral'.

INSERT OR IGNORE INTO model_facts (
  route_key, capacity_key, provider_id, model_id, variant, scores_json,
  context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use,
  prompt_defaults_json, confidence, notes, source, family, updated_at
) VALUES
  ('ollama-cloud/glm-5.2', 'ollama-cloud/glm-5.2', 'ollama-cloud', 'glm-5.2', NULL,
   '{"iq":49,"eq":1565,"speed":196}', 1000000, 2, 4.4, NULL, 1, NULL, 'curated',
   'user-curated iq (AA 51); eq curated above glm-5.1; cost carried from glm-5.1',
   'seed', 'glm', 0),
  ('ollama-cloud/minimax-m3', 'ollama-cloud/minimax-m3', 'ollama-cloud', 'minimax-m3', NULL,
   '{"iq":44,"eq":1520,"speed":44}', 512000, 1, 2.4, 1, 1, NULL, 'curated',
   'AA v4.1 44; context/cost carried from opencode-go/minimax-m3',
   'seed', 'minimax', 0),
  ('ollama-cloud/deepseek-v4-pro', 'ollama-cloud/deepseek-v4-pro', 'ollama-cloud', 'deepseek-v4-pro', NULL,
   '{"iq":44,"eq":1599.8,"speed":48}', 1000000, 2, 3.48, NULL, 1, NULL, 'curated',
   'AA v4.1 44; context/cost carried from opencode-go/deepseek-v4-pro',
   'seed', 'deepseek', 0),
  ('ollama-cloud/kimi-k2.6', 'ollama-cloud/kimi-k2.6', 'ollama-cloud', 'kimi-k2.6', NULL,
   '{"iq":44,"eq":1575.8,"speed":43}', 256000, 2, 4, 1, 1, NULL, 'curated',
   'AA v4.1 44; cost carried from opencode-go/kimi-k2.6',
   'seed', 'kimi', 0),
  ('ollama-cloud/kimi-k2.7-code', 'ollama-cloud/kimi-k2.7-code', 'ollama-cloud', 'kimi-k2.7-code', NULL,
   '{"iq":42,"eq":1570,"speed":45}', 256000, 2, 4, 1, 1, NULL, 'curated',
   'AA v4.1 42; eq curated near k2.6; cost like k2.6',
   'seed', 'kimi', 0),
  ('ollama-cloud/deepseek-v4-flash', 'ollama-cloud/deepseek-v4-flash', 'ollama-cloud', 'deepseek-v4-flash', NULL,
   '{"iq":42,"eq":1494.2,"speed":113}', 1000000, 0, 0.28, NULL, 1, NULL, 'curated',
   'bridge from old 46.5; context/cost carried from opencode-go/deepseek-v4-flash',
   'seed', 'deepseek', 0),
  ('ollama-cloud/glm-5.1', 'ollama-cloud/glm-5.1', 'ollama-cloud', 'glm-5.1', NULL,
   '{"iq":47,"eq":1557,"speed":52}', 202752, 2, 4.4, NULL, 1, NULL, 'curated',
   'bridge from old 51.4; context/cost carried from opencode-go/glm-5.1',
   'seed', 'glm', 0),
  ('ollama-cloud/minimax-m2.7', 'ollama-cloud/minimax-m2.7', 'ollama-cloud', 'minimax-m2.7', NULL,
   '{"iq":42,"eq":1510,"speed":65}', 204800, 1, 1.2, NULL, 1, NULL, 'curated',
   'bridge; context/cost carried from opencode-go/minimax-m2.7',
   'seed', 'minimax', 0),
  ('ollama-cloud/minimax-m2.5', 'ollama-cloud/minimax-m2.5', 'ollama-cloud', 'minimax-m2.5', NULL,
   '{"iq":36,"eq":1490,"speed":206}', 204800, 1, 1.2, NULL, 1, NULL, 'curated',
   'bridge; context/cost carried from opencode-go/minimax-m2.5',
   'seed', 'minimax', 0),
  ('ollama-cloud/kimi-k2.5', 'ollama-cloud/kimi-k2.5', 'ollama-cloud', 'kimi-k2.5', NULL,
   '{"iq":35,"eq":1541.7,"speed":35}', 256000, 1, 3, 1, 1, NULL, 'curated',
   'AA v4.1 ~35; cost carried from opencode-go/kimi-k2.5',
   'seed', 'kimi', 0),
  ('ollama-cloud/qwen3.5:397b', 'ollama-cloud/qwen3.5:397b', 'ollama-cloud', 'qwen3.5:397b', NULL,
   '{"iq":38,"eq":1500,"speed":50}', 262000, NULL, NULL, 1, 1, NULL, 'curated',
   'AA ~34-40 (reasoning); rough; no defensible cost datapoint',
   'seed', 'qwen', 0),
  ('ollama-cloud/mistral-large-3', 'ollama-cloud/mistral-large-3', 'ollama-cloud', 'mistral-large-3', NULL,
   '{"iq":16,"eq":1450,"speed":42}', 256000, NULL, NULL, NULL, 1, NULL, 'curated',
   'AA v4.1 16; below most floors by design',
   'seed', 'mistral', 0);
