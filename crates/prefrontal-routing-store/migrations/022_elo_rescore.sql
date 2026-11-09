-- Clean cutover from the bounded model-side `iq` capability score to open Elo.
-- Caller requirements remain 0..100 and are mapped to a request-relative bar
-- by the routing core. EQ and speed are copied byte-for-byte.
--
-- Values come from the operator-approved resolved placement sheet. Fast/pro
-- siblings inherit their base route's placement. Rows absent from the sheet
-- are permanently excluded/dead catalog entries and use old iq * 33.

UPDATE model_facts
SET scores_json = json_object(
  'elo', CASE route_key
    WHEN 'alibaba-token-plan/qwen3.7-max' THEN 1254
    WHEN 'alibaba-token-plan/qwen3.7-plus' THEN 715
    WHEN 'alibaba-token-plan/qwen3.8-max' THEN 1721
    WHEN 'alibaba-token-plan/qwen3.8-max-preview' THEN 1721

    WHEN 'anthropic/claude-fable-5' THEN 1911
    WHEN 'anthropic/claude-opus-4-5' THEN 698
    WHEN 'anthropic/claude-opus-4-6' THEN 1130
    WHEN 'anthropic/claude-opus-4-7' THEN 1635
    WHEN 'anthropic/claude-opus-4-8' THEN 1728
    WHEN 'anthropic/claude-opus-5' THEN 1905
    WHEN 'anthropic/claude-opus-5-fast' THEN 1905
    WHEN 'anthropic/claude-sonnet-4-6' THEN 637

    WHEN 'google/antigravity-gemini-3.1-pro' THEN 1280
    WHEN 'google/antigravity-gemini-3.5-flash' THEN 1345
    WHEN 'google/antigravity-gemini-3.6-flash' THEN 1410

    WHEN 'kimi-for-coding/k3' THEN 1742

    WHEN 'ollama-cloud/deepseek-v4-flash' THEN 1180
    WHEN 'ollama-cloud/deepseek-v4-flash:0731' THEN 1610
    WHEN 'ollama-cloud/deepseek-v4-pro' THEN 1284
    WHEN 'ollama-cloud/glm-5.1' THEN 988
    WHEN 'ollama-cloud/glm-5.2' THEN 1660
    WHEN 'ollama-cloud/kimi-k2.5' THEN 599
    WHEN 'ollama-cloud/kimi-k2.6' THEN 1215
    WHEN 'ollama-cloud/kimi-k2.7-code' THEN 1102
    WHEN 'ollama-cloud/minimax-m2.5' THEN 520
    WHEN 'ollama-cloud/minimax-m2.7' THEN 984
    WHEN 'ollama-cloud/minimax-m3' THEN 1311
    WHEN 'ollama-cloud/mistral-large-3' THEN 520

    WHEN 'openai/gpt-5.4' THEN 1589
    WHEN 'openai/gpt-5.4-fast' THEN 1589
    WHEN 'openai/gpt-5.4-mini' THEN 956
    WHEN 'openai/gpt-5.4-mini-fast' THEN 956
    WHEN 'openai/gpt-5.5' THEN 1690
    WHEN 'openai/gpt-5.5-fast' THEN 1690
    WHEN 'openai/gpt-5.6-luna' THEN 1621
    WHEN 'openai/gpt-5.6-luna-fast' THEN 1621
    WHEN 'openai/gpt-5.6-luna-pro' THEN 1621
    WHEN 'openai/gpt-5.6-sol' THEN 1838
    WHEN 'openai/gpt-5.6-sol-fast' THEN 1838
    WHEN 'openai/gpt-5.6-sol-pro' THEN 1838
    WHEN 'openai/gpt-5.6-terra' THEN 1685
    WHEN 'openai/gpt-5.6-terra-fast' THEN 1685
    WHEN 'openai/gpt-5.6-terra-pro' THEN 1685

    WHEN 'xai/grok-4.3' THEN 659
    WHEN 'xai/grok-4.5' THEN 1649

    -- Excluded/dead rows: coarse bridge from the final bounded score.
    WHEN 'xai/grok-build-0.1' THEN 1320
    WHEN 'ollama-cloud/qwen3.5:397b' THEN 1617
    WHEN 'google/antigravity-claude-opus-4.5' THEN 1782
    WHEN 'google/antigravity-claude-opus-4.6-thinking' THEN 1848
    WHEN 'google/antigravity-claude-opus-4.6-thinking-high' THEN 1848
    WHEN 'google/antigravity-claude-sonnet-4.6' THEN 1947

    -- A catalog inserted by an out-of-tree migration still receives a valid
    -- bridge instead of retaining the deleted key.
    ELSE json_extract(scores_json, '$.iq') * 33
  END,
  'eq', json_extract(scores_json, '$.eq'),
  'speed', json_extract(scores_json, '$.speed')
);
