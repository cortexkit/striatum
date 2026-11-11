-- Continuation answers and their reservations commit together with the account choice.
CREATE TABLE account_continue (
    principal TEXT NOT NULL,
    selection_id TEXT NOT NULL,
    request_json TEXT NOT NULL,
    answer_json TEXT NOT NULL,
    run_id TEXT NOT NULL,
    admission_id TEXT NOT NULL,
    previous_decision_id TEXT NOT NULL,
    decision_id TEXT,
    reservation_id TEXT,
    valid_until_ms INTEGER,
    used_at_ms INTEGER,
    released_at_ms INTEGER,
    PRIMARY KEY (principal, selection_id)
);
CREATE UNIQUE INDEX account_continue_decision ON account_continue(decision_id)
    WHERE decision_id IS NOT NULL;
CREATE INDEX account_continue_expiry ON account_continue(valid_until_ms)
    WHERE used_at_ms IS NULL AND released_at_ms IS NULL;
ALTER TABLE model_route_decisions ADD COLUMN previous_decision_id TEXT;
