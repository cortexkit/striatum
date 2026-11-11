-- The model identity Fusiform serves as `model.canonical_id` (models.dev's
-- `canonical_model_id`, passed through verbatim). Routes with the same value
-- serve the same weights, so they share one Elo/EQ placement at read time.
-- NULL means the catalog states no identity for this route (or has not served
-- the row since the column existed); it never means the route is unique.
ALTER TABLE model_facts ADD COLUMN canonical_id TEXT;
CREATE INDEX model_facts_canonical_id_idx ON model_facts (canonical_id)
    WHERE canonical_id IS NOT NULL;
-- Fusiform's `model.status`, as models.dev publishes it: 'deprecated', 'beta',
-- 'alpha', or NULL when no status is stated. models.dev marks a model OpenCode
-- has withdrawn as 'deprecated' instead of deleting it, so routing refuses a
-- deprecated route while the row (and its score) stays in the catalog.
ALTER TABLE model_facts ADD COLUMN upstream_status TEXT;
