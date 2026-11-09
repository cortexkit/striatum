-- qwen3.8-max-preview rescore (2026-07-19, operator curation): iq 53 -> 55,
-- eq 1560 -> 1895. Ufuk places it in the k3/terra band (55) rather than the
-- grok-4.5 band the initial seed guessed, with near-kimi prose judgment
-- (1895 vs k3's 1915.5). Still curated pending an Artificial Analysis score
-- for the preview.
UPDATE model_facts
SET scores_json = '{"iq":55,"eq":1895,"speed":45}',
    notes = 'curated k3/terra-band per operator 2026-07-19 pending AA evidence; subscription-served'
WHERE route_key = 'alibaba-token-plan/qwen3.8-max-preview'
  AND source = 'seed';
