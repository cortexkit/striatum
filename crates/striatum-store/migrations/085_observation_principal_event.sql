-- Event de-duplication first finds a runner by principal and event id.
-- Keep the existing principal/runner/event uniqueness for exact replays.
CREATE INDEX account_observation_by_principal_event
    ON account_observation (principal, event_id, runner);
CREATE INDEX account_observation_event_by_principal_event
    ON account_observation_event (principal, event_id, runner);
