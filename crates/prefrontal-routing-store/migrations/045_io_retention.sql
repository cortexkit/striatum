-- The heartbeat query must not walk observation shadows to find one timer row.
-- Ascending time also gives descending rowid tie order on the reverse scan.
CREATE INDEX account_decision_latest
  ON account_decision(mode, trigger, provider, family, created_at);
-- Usage payload is read for seven-day speed/class statistics and at most
-- fourteen-day quota intervals. Retired message identities remain permanent:
-- deleting a payload must not make a redrive count as a new message.
CREATE INDEX model_route_usage_retention ON model_route_usage(observed_at);
CREATE TABLE retired_usage_delivery (
  id TEXT PRIMARY KEY
) WITHOUT ROWID;
