-- Persist the remaining catalog rate card beside the existing output rate.
-- NULL means the producer did not serve a usable value; explicit zero remains REAL 0.
ALTER TABLE model_facts ADD COLUMN input_rate REAL;
ALTER TABLE model_facts ADD COLUMN cache_read_rate REAL;
ALTER TABLE model_facts ADD COLUMN cache_write_rate REAL;
ALTER TABLE model_facts ADD COLUMN reasoning_rate REAL;
