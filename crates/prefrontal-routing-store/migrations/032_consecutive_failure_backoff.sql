-- Migration 32 (namespace "route_state"): durable route-local safety backoff for
-- terminal send failures that arrive without a provider-specific refusal class.
ALTER TABLE model_route_state
  ADD COLUMN consecutive_terminal_failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE model_route_state
  ADD COLUMN failure_backoff_until INTEGER;
ALTER TABLE model_route_state
  ADD COLUMN failure_backoff_reason TEXT;

-- Preserve the exact surfaced send error beside the terminal decision that fed
-- the counter. Historical outcomes remain NULL because their error is unknown.
ALTER TABLE model_route_decisions
  ADD COLUMN outcome_error TEXT;
