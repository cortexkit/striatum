-- A failed routing/v1 report naming a future reset fences one route on one
-- account: the account is not admitted for that provider and model until
-- `until_ms`. Other routes on the account and other accounts on the route are
-- untouched. The reset is the caller's claim, capped at 24 hours when it is
-- recorded; an expired row is deleted when the fences are next read.
CREATE TABLE account_route_fence (
  provider TEXT NOT NULL,
  credential_id TEXT NOT NULL,
  model TEXT NOT NULL,
  until_ms INTEGER NOT NULL,
  decision_id TEXT NOT NULL,
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (provider, credential_id, model)
);
CREATE INDEX account_route_fence_expiry ON account_route_fence (until_ms);
