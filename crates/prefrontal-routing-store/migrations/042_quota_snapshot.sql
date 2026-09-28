-- Migration 42 (namespace "route_state"): the last quota snapshot the routing
-- module accepted from insula, kept across restarts. Without it a restarted
-- module selects with no quota reading until its first poll completes, and in
-- that window the ordering can pick a different, scarcer model. One row only:
-- each accepted poll replaces the previous snapshot. `observed_at` is the local
-- poll clock (Unix milliseconds) at which the snapshot was accepted.

CREATE TABLE IF NOT EXISTS quota_snapshot (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  windows_json TEXT NOT NULL,
  observed_at INTEGER NOT NULL
);
