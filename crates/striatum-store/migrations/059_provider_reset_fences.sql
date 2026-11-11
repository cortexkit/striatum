ALTER TABLE model_route_decisions ADD COLUMN consumer TEXT;
ALTER TABLE model_route_decisions ADD COLUMN provider_identity_json TEXT;

-- Provider refusals apply to one request class and exact executed variant, not
-- to Insula's independently measured quota pools or general model health.
CREATE TABLE provider_reset_fences (
    route_key TEXT NOT NULL,
    consumer TEXT NOT NULL,
    account_ref TEXT NOT NULL,
    credential_id TEXT,
    resets_at_ms INTEGER NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (route_key, consumer, account_ref)
);
