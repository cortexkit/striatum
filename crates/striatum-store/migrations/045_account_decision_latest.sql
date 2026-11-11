-- The heartbeat query must not walk observation shadows to find one timer row.
-- Ascending time also gives descending rowid tie order on the reverse scan.
CREATE INDEX account_decision_latest
  ON account_decision(mode, trigger, provider, family, created_at);
