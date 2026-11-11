-- Panel selection needs curated model lineage and an explicit decision shape.
-- Both additions preserve v4 rows: family remains unknown until curated below,
-- while historical decisions retain the singular selection interpretation.
ALTER TABLE model_facts ADD COLUMN family TEXT;
ALTER TABLE model_route_decisions
  ADD COLUMN decision_kind TEXT NOT NULL DEFAULT 'single';

UPDATE model_facts
SET family = CASE
  WHEN provider_id = 'anthropic' THEN 'anthropic'
  WHEN provider_id = 'openai' THEN 'openai-gpt'
  WHEN provider_id = 'google' AND model_id LIKE 'antigravity-claude-%-thinking' THEN 'anthropic'
  WHEN provider_id = 'google' AND (
    model_id LIKE 'gemini%' OR model_id LIKE 'antigravity-gemini-%'
  ) THEN 'google-gemini'
  WHEN provider_id = 'xai' THEN 'xai-grok'
  WHEN model_id LIKE 'deepseek-%' THEN 'deepseek'
  WHEN model_id LIKE 'glm-%' THEN 'glm'
  WHEN model_id LIKE 'kimi-%' THEN 'kimi'
  WHEN model_id LIKE 'qwen%' THEN 'qwen'
  WHEN model_id LIKE 'minimax-%' THEN 'minimax'
  ELSE NULL
END;
