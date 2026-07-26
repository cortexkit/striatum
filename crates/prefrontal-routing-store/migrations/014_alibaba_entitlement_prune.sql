-- Prune migration 013's alibaba seed to the token plan's real entitlement
-- (2026-07-21). BROCA adjudicated from the wire on the live credential: the
-- $68 token plan entitles the QWEN FAMILY ONLY - deepseek/kimi/glm/minimax
-- verbatim ids return 403 AccessDenied.Unpurchased (while dialect variants
-- 404, proving the ids resolve and are refused on purchase grounds), and
-- qwen3.6-plus is 403 too. Serving set confirmed 200: qwen3.8-max-preview
-- (migration 010), qwen3.7-max, qwen3.7-plus, qwen3.6-flash.
DELETE FROM model_facts
WHERE provider_id = 'alibaba-token-plan'
  AND model_id IN (
    'deepseek-v4-flash', 'deepseek-v4-pro', 'deepseek-v3.2',
    'glm-5', 'glm-5.1', 'glm-5.2',
    'kimi-k2.5', 'kimi-k2.6', 'kimi-k2.7-code',
    'MiniMax-M2.5', 'qwen3.6-plus'
  );
