-- Indexes that let retention and report statements find their rows by range
-- or by key instead of walking a whole table. Each index repeats the exact
-- expression of the statement that uses it; SQLite only matches an
-- expression index to an identical expression.

-- Parent-failure retention deletes copies whose outcome time (a missing time
-- counts as 0) is before the decision window. The only other index leads with
-- the parent session, so without this one every batch walked that whole index,
-- even when nothing was due.
CREATE INDEX model_route_parent_failures_by_outcome
  ON model_route_parent_failures (COALESCE(outcome_at, 0));

-- A failed report ends the affinity that still points at its decision. Only
-- unended rows are ever looked up this way.
CREATE INDEX routing_v1_affinity_by_decision
  ON routing_v1_affinity (decision_id)
  WHERE ended_at_ms IS NULL;

-- Continuation retention looks for answers whose offer deadline, use and
-- release (a missing time counts as 0) are all before its cutoff: the latest
-- of the three is the row's own retention clock.
CREATE INDEX account_continue_by_last_time
  ON account_continue (MAX(COALESCE(valid_until_ms, 0), COALESCE(used_at_ms, 0),
    COALESCE(released_at_ms, 0)));

-- Key-pool retention: a lease stops counting when it is released, or else
-- when its deadline passes; a session binding when it ends, or else when it
-- idles out. That time is each row's retention clock.
CREATE INDEX pool_key_lease_by_end
  ON pool_key_lease (COALESCE(released_at, valid_until));
CREATE INDEX pool_session_affinity_by_end
  ON pool_session_affinity (COALESCE(ended_at, expires_at));
