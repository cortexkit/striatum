-- Attachment capability needs an explicit fact before it can advertise a file upload.
-- The routing scorer deliberately treats a missing flag as conservative no, but a phone
-- capability response must preserve absence as cannot-verify rather than claim no.
UPDATE model_facts
SET supports_vision = 1
WHERE (provider_id = 'anthropic' AND model_id LIKE 'claude-%')
   OR (provider_id = 'openai' AND model_id LIKE 'gpt-5.6%')
   OR (provider_id = 'xai' AND model_id LIKE 'grok-4.5%')
   OR (provider_id = 'google' AND model_id LIKE '%gemini%');

-- Uncertain families must remain absent rather than being advertised as image-capable.
UPDATE model_facts
SET supports_vision = NULL
WHERE (provider_id = 'alibaba-token-plan' AND model_id LIKE 'qwen%')
   OR model_id LIKE 'kimi%'
   OR provider_id = 'ollama-cloud';
