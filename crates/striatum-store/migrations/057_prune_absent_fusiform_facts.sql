-- Remove retired catalog facts that have not been selected in the last 30 days.
-- Decision rows remain as the score and provenance history for those routes.
-- NULL presence is left for the next complete catalog sync to resolve: this
-- migration has no durable snapshot with which to distinguish served from absent.
-- These two Argon rows are old operator overrides for retired upstream aliases;
-- their distinct model IDs bypassed the canonical Argon exclusion, so remove
-- only these known aliases despite their override scores and recent decisions.
DELETE FROM model_route_state
WHERE route_key IN (
  SELECT route_key FROM model_facts
  WHERE route_key IN (
    'google/antigravity-gemini-4-argon-high',
    'google/antigravity-gemini-4-argon-low'
  )
  OR (
    fusiform_present = 0
    AND source <> 'override'
    AND NOT EXISTS (
      SELECT 1
      FROM model_route_decisions
      WHERE model_route_decisions.selected_route_key = model_facts.route_key
        AND model_route_decisions.created_at >=
          CAST(strftime('%s', 'now') AS INTEGER) * 1000 - 30 * 24 * 60 * 60 * 1000
    )
  )
);

DELETE FROM model_facts
WHERE route_key IN (
  'google/antigravity-gemini-4-argon-high',
  'google/antigravity-gemini-4-argon-low'
)
OR (
  fusiform_present = 0
  AND source <> 'override'
  AND NOT EXISTS (
    SELECT 1
    FROM model_route_decisions
    WHERE model_route_decisions.selected_route_key = model_facts.route_key
      AND model_route_decisions.created_at >=
        CAST(strftime('%s', 'now') AS INTEGER) * 1000 - 30 * 24 * 60 * 60 * 1000
  )
);
