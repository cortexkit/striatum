-- Real Anthropic list prices, corrected 2026-07-25 (Ufuk, from the published
-- rate card). This matters now in a way it did not before: with route-derived
-- pricing, cost_per_mtok_out IS the ranking signal, so a wrong price is a
-- wrong route rather than a cosmetic field.
--
-- Fable 5: $10 in / $50 output. The catalog carried 25, which understated it by
-- half and made the most expensive model in the fleet look like an opus-line
-- peer.
-- Opus 4.7 / 4.8 / 5: $5 in / $25 output. Already correct at 25; left alone.
--
-- Cost tier is realigned with the corrected price so the tier and the number do
-- not disagree (fable moves up a tier, its -fast siblings follow their base).
UPDATE model_facts
   SET cost_per_mtok_out = 50.0,
       cost_tier = 5,
       notes = 'Real list price $10 in / $50 out (Ufuk, 2026-07-25)'
 WHERE route_key = 'anthropic/claude-fable-5';

-- The opus-5 sibling I seeded in 018 assumed the -fast premium doubles the base
-- the way opus-4-8-fast does. The base price is confirmed at 25, so the assumed
-- 50 for the sibling stands as an assumption, not a quoted price: mark it so a
-- later reader does not mistake it for a verified figure.
UPDATE model_facts
   SET notes = 'Fast sibling of claude-opus-5; price ASSUMED at 2x base pending a quoted rate'
 WHERE route_key = 'anthropic/claude-opus-5-fast';
