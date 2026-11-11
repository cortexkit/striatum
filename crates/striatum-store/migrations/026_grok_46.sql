-- Grok 4.6, added 2026-08-13. Ufuk's curation: elo 1798 (his call after "about
-- 4% below gpt-5.6-sol"; sol sits at 1838, so 1798 lands it above every
-- non-OpenAI route and below the sol/opus/fable band). EQ carried from
-- grok-4.5 +50 (the 4.5->4.6 jump is a reasoning-band upgrade, and the xhigh
-- default is the reasoning variant); speed carried from grok-4.5 pending
-- observed samples - it is a tiebreak input only.
--
-- Default variant is xhigh per Ufuk: the model ships with xhigh reasoning and
-- that is the shape he wants routed, matching how sol and opus rows pin their
-- top effort as the default.
--
-- Price carries grok-4.5's list ($6/Mtok out) until xai publishes 4.6 pricing;
-- the xai provider-level subscription multiplier already covers the effective
-- price, so a list correction later is a one-column update.
--
-- grok-4.5 stays IN the pool (not excluded here): 4.6 outranks it on elo at
-- equal cost, so selection migrates naturally; an explicit exclude can follow
-- once 4.6 has traffic behind it.
INSERT OR IGNORE INTO model_facts (route_key, capacity_key, provider_id, model_id, variant, scores_json, context_window, cost_tier, cost_per_mtok_out, supports_vision, supports_tool_use, prompt_defaults_json, confidence, notes, source, family, max_output_tokens, updated_at) VALUES
  ('xai/grok-4.6', 'xai/grok-4.6', 'xai', 'grok-4.6', NULL, '{"elo":1798.0,"eq":1600.0,"speed":120.0}', 500000, 2, 6.0, 1, 1, '{"variant":"xhigh"}', 'curated', 'Ufuk curation 2026-08-13: elo 1798 (~4% below sol per his sizing, value his call); eq grok-4.5+50; speed, context, output ceiling carried from grok-4.5', 'seed', 'xai-grok', 32768, 0);
