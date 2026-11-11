-- The reasoning-effort values Fusiform publishes for a model: the `values` of
-- every `{"type": "effort"}` entry in `capability.reasoning_options`, as a
-- JSON array. NULL means this row has not been read from the catalog since the
-- column existed. An empty array means the catalog publishes no effort values,
-- so no configured variant can be admitted for the model.
ALTER TABLE model_facts ADD COLUMN reasoning_efforts_json TEXT
    CHECK (reasoning_efforts_json IS NULL OR json_valid(reasoning_efforts_json));
