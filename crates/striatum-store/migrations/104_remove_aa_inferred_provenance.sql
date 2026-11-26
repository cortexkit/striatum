-- An inferred score is not a curated placement. Return these rows to the
-- same score state as a new, ungraded catalog row without losing other facts.
UPDATE model_facts
SET source = 'unscored', scores_json = '{"elo":0.0,"eq":0.0,"speed":0.0}', confidence = 'unknown'
WHERE source = 'aa_inferred';

ALTER TABLE model_facts DROP COLUMN aa_index;
ALTER TABLE model_facts DROP COLUMN review_by_ms;
ALTER TABLE model_facts DROP COLUMN eq_source;
