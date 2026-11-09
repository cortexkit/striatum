-- Keep the start of a fence for diagnosis, and its latest evidence time for
-- recovery: a reading between two failures must not heal the second failure.
ALTER TABLE account_route_fence ADD COLUMN last_reported_at INTEGER;
UPDATE account_route_fence SET last_reported_at = recorded_at;
