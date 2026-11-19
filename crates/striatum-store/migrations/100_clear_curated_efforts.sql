-- Effort defaults now come from the model catalog. Keep other prompt defaults
-- and the identities of explicitly variant-qualified model fact rows.
UPDATE model_facts
SET prompt_defaults_json = json_remove(prompt_defaults_json, '$.variant')
WHERE json_type(prompt_defaults_json, '$.variant') IS NOT NULL;
