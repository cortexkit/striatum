-- ollama-cloud began serving Kimi K3; the catalog only knew k2.5/k2.6/k2.7-code
-- on that provider, so a roster seat naming ollama-cloud/kimi-k3 dropped at
-- admission as an unknown route (live: the cutover spec panel ran 2 seats while
-- the operator's added third seat silently never existed). Same model as
-- kimi-for-coding/k3, so capability scores carry over verbatim (elo 1742,
-- eq 1915.5); speed follows the ollama-cloud serving observed on the k2 line;
-- cost uses ollama-cloud's list (the provider rides the zero-cost subscription
-- list regardless).
INSERT OR IGNORE INTO model_facts (
    route_key, capacity_key, provider_id, model_id, variant,
    scores_json, context_window, cost_tier, cost_per_mtok_out,
    supports_vision, supports_tool_use, prompt_defaults_json,
    confidence, notes, source, updated_at, family, max_output_tokens
) VALUES (
    'ollama-cloud/kimi-k3', 'ollama-cloud/kimi-k3', 'ollama-cloud', 'kimi-k3', NULL,
    '{"elo":1742,"eq":1915.5,"speed":45}', 256000, 3, 4.0,
    0, 1, NULL,
    'curated', 'K3 served via ollama-cloud; scores mirror kimi-for-coding/k3', 'migration-023',
    strftime('%s','now') * 1000, 'kimi', 131072
);
