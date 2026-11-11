-- Eligibility is a vault-listing fact, not quota exhaustion. Empty model means
-- every model on this provider. Replacing a listing removes its old facts.
CREATE TABLE account_eligibility (
  provider TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  model TEXT NOT NULL,
  listing_fingerprint TEXT,
  code TEXT NOT NULL,
  observed_at INTEGER NOT NULL,
  PRIMARY KEY (provider, credential_id, model)
);

-- A size refusal does not block an account or pool for all request sizes.
-- A successful request in
-- the same size band clears it; expiry also permits a trial without a report.
CREATE TABLE account_size_refusal (
  provider TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  model TEXT NOT NULL,
  size_class INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  observed_at INTEGER NOT NULL,
  PRIMARY KEY (provider, credential_id, model, size_class)
);
CREATE INDEX account_size_refusal_expiry ON account_size_refusal (expires_at);
