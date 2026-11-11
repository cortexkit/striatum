ALTER TABLE model_route_decisions ADD COLUMN allowed_providers_json TEXT
    CHECK (allowed_providers_json IS NULL OR json_valid(allowed_providers_json));
