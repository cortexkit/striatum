-- Migration 40 (namespace "route_state"): the last announced route-winner
-- ladder. The routing module compares every newly computed ladder against this
-- row, including the first one after a restart, so a winner that moved while the
-- process was down is still announced once. One row only: a newer announcement
-- replaces the older one.

CREATE TABLE IF NOT EXISTS route_winner_baseline (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  ladder_json TEXT NOT NULL,
  trigger TEXT NOT NULL,
  recorded_at INTEGER NOT NULL
);
