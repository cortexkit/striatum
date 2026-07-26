-- Opus default variant: max -> xhigh. Measured in practice: on claude-opus-4-8
-- the max effort tier takes far longer while producing results indistinguishable
-- from xhigh, so auto-routed selections should not pay the latency. Explicit
-- pins (athena rosters, agent model pins) carry their own variant and are
-- unaffected by this default.
UPDATE model_facts
SET prompt_defaults_json = json_set(prompt_defaults_json, '$.variant', 'xhigh'),
    updated_at = strftime('%s', 'now') * 1000
WHERE provider_id = 'anthropic'
  AND model_id LIKE 'claude-opus-4-%'
  AND json_extract(prompt_defaults_json, '$.variant') = 'max';
