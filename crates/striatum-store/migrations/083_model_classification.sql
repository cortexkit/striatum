-- Catalog classifications are model facts, shared by every reasoning variant.
-- NULL retains legacy eligibility when the producer does not state a value.
ALTER TABLE model_facts ADD COLUMN model_type TEXT;
ALTER TABLE model_facts ADD COLUMN model_kind TEXT;
