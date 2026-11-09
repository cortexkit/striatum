-- Active account-wide backoffs are sparse among the retained runner events.
-- Selection reads only their recent bound effects, not the whole event log.
CREATE INDEX account_observation_backoff_by_received
ON account_observation(received_at)
WHERE kind = 'transient_backoff' AND bound = 1;
