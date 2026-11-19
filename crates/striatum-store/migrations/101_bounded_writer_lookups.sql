-- Count only unreleased leases whose deadline has not passed.
CREATE INDEX pool_key_lease_live_by_expiry
  ON pool_key_lease (provider, valid_until)
  WHERE released_at IS NULL;

-- When a run used an offer to switch accounts, retention checks the
-- predecessor reservation and the run's admissions still in progress
-- (state 'started') without scanning the principal's entire history.
CREATE INDEX account_continue_by_reservation
  ON account_continue (principal, reservation_id)
  WHERE reservation_id IS NOT NULL;
CREATE INDEX account_admission_started_by_run
  ON account_admission (principal, run_id, provider, account_id)
  WHERE state = 'started';
