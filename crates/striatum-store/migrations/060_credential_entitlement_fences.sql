-- A provider's permanent refusal of one vault credential: an entitlement error
-- (the account has no plan for the provider's models) or an empty paid balance
-- (billing_exhausted); `code` is the provider code Broca reported. Automatic
-- selection does not use the provider through that credential while a row
-- exists. The row ends when the credential's vault listing (provider ids, plan
-- tier, state) no longer matches listing_fingerprint, never on a timer.
-- listing_fingerprint is NULL when no listing named the credential at the time.
CREATE TABLE credential_entitlement_fences (
    provider_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    code TEXT NOT NULL,
    listing_fingerprint TEXT,
    observed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (provider_id, credential_id)
);
