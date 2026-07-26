-- Two models Ufuk reported live on their providers 2026-08-05. Both were already
-- in OpenCode's catalog (so the availability set admits them) but had no facts
-- row, which makes a route UNSCOREABLE and therefore invisible to the router:
-- it can never be selected, and it never appears as a rejection either, so the
-- absence is silent in both directions.
--
-- Every field is CARRIED from the sibling rather than curated fresh, because we
-- have no measurement of either model yet. Carrying is the honest default: an
-- invented IQ would be a fabricated fact wearing the same shape as a measured
-- one, and the router cannot tell them apart.
--
-- qwen3.8-max is the stable release of qwen3.8-max-preview: same family, same
-- 256k context, same 64k output ceiling, same $6/Mtok list. The preview stays
-- admitted for now; whether the stable release should supersede it (the way
-- gemini-3.6-flash superseded 3.5) is a curation call, not a schema one, and
-- belongs in the exclude list once Ufuk rules on it.
--
-- deepseek-v4-flash:0731 is a materially stronger build than deepseek-v4-flash,
-- per Ufuk's hands-on judgement, so its IQ is curated to gemini-3.6-flash's 44
-- (the cheap-tier reference point) rather than carried from the 42 of the older
-- build. EQ and speed still carry: only capability was reported, and inventing
-- movement in the other two would dress an unmeasured guess as a measurement.
-- At $0.28/Mtok against gemini-flash's $9 it becomes the cheapest route at that
-- capability, which is the point of admitting it.
INSERT OR IGNORE INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, family, max_output_tokens, updated_at) VALUES
  ('alibaba-token-plan/qwen3.8-max', 'alibaba-token-plan/qwen3.8-max', 'alibaba-token-plan', 'qwen3.8-max', NULL, '{"iq":55.0,"eq":1895.0,"speed":45.0}', 256000, 3, 6.0, 1, 1, NULL, 'curated', 'Stable release of qwen3.8-max-preview; all fields carried from the preview sibling pending its own measurement', 'seed', 'qwen', 65536, 0),
  ('ollama-cloud/deepseek-v4-flash:0731', 'ollama-cloud/deepseek-v4-flash:0731', 'ollama-cloud', 'deepseek-v4-flash:0731', NULL, '{"iq":44.0,"eq":1494.2,"speed":113.0}', 1000000, 0, 0.28, 0, 1, NULL, 'curated', 'Ufuk curation 2026-08-05: materially more capable than deepseek-v4-flash, iq set to gemini-3.6-flash parity (44); eq and speed carried from the older build', 'seed', 'deepseek', NULL, 0);
