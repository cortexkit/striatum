-- Provider output-token ceilings for fanout seats. NULL means the registry does not
-- know a provider-enforced completion limit, so core leaves its requested budget intact.
ALTER TABLE model_facts ADD COLUMN max_output_tokens INTEGER;

-- SOURCE: Anthropic models overview, https://platform.claude.com/docs/en/about-claude/models/overview
-- The operational ceiling for the current Claude 4 roster is 64k max_tokens.
UPDATE model_facts
SET max_output_tokens = 64000
WHERE provider_id = 'anthropic';

-- SOURCE: OpenAI GPT-5.4/5.5/5.6 model pages,
-- https://developers.openai.com/api/docs/models/gpt-5.4
-- https://developers.openai.com/api/docs/models/gpt-5.5
-- https://developers.openai.com/api/docs/models/gpt-5.6
UPDATE model_facts
SET max_output_tokens = 128000
WHERE provider_id = 'openai'
  AND (
    model_id LIKE 'gpt-5.6-%'
    OR model_id LIKE 'gpt-5.5%'
    OR model_id LIKE 'gpt-5.4%'
  );

-- SOURCE: xAI model catalog, https://docs.x.ai/developers/models
-- No higher per-model output ceiling is published for these roster seats, so use
-- the conservative 32k registry cap until xAI documents a replacement.
UPDATE model_facts
SET max_output_tokens = 32768
WHERE route_key IN (
  'xai/grok-4.5',
  'xai/grok-4.3',
  'xai/grok-build-0.1',
  'xai/grok-composer-2.5-fast'
);

-- SOURCE: models.dev K3 capability data and live api.kimi.com/coding probes.
-- K3 accepts requests above the retired 8k application guess; the registry cap
-- is its 128k output capability.
UPDATE model_facts
SET max_output_tokens = 131072
WHERE route_key = 'kimi-for-coding/k3';

-- SOURCE: Alibaba Qwen Max model catalog,
-- https://help.aliyun.com/zh/model-studio/models
-- OpenCode and BROCA provider overlays both report Qwen Max's 64k output cap.
UPDATE model_facts
SET max_output_tokens = 65536
WHERE route_key = 'alibaba-token-plan/qwen3.8-max-preview';

-- ollama-cloud and google/antigravity routes remain NULL: their engine/provider
-- limits are not stable registry facts, so applying a cap here would be unsafe.
