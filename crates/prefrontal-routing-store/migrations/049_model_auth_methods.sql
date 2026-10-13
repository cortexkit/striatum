-- NULL means Fusiform has not restricted this model's login methods. A JSON
-- empty array is a restriction that permits no credential, not a missing value.
ALTER TABLE model_facts ADD COLUMN auth_methods_json TEXT
    CHECK (auth_methods_json IS NULL OR json_valid(auth_methods_json));
