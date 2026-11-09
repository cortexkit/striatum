-- routing/v1 role state, kept beside the legacy decision ledger.
--
-- A v1 decision is a row in model_route_decisions (the ledger route.select has
-- always written) plus the binding below: the principal and consumer that
-- selected it, the runner it names and when it stops being current routing.
-- Only that principal may report it, and the runner it names is the only one
-- whose route.account_for may cite it.
CREATE TABLE routing_v1_decision (
    decision_id TEXT PRIMARY KEY,
    principal TEXT NOT NULL,
    consumer TEXT NOT NULL,
    -- The project the selecting route was bound to; NULL for an unscoped route.
    project_id TEXT,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    variant TEXT,
    runner_module TEXT NOT NULL,
    affinity TEXT,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL
);

-- The last decision answered for each affinity key. A failure report on that
-- decision ends the preference (ended_at_ms is set); the next decision for
-- the key starts it again.
CREATE TABLE routing_v1_affinity (
    principal TEXT NOT NULL,
    consumer TEXT NOT NULL,
    affinity TEXT NOT NULL,
    decision_id TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    ended_at_ms INTEGER,
    PRIMARY KEY (principal, consumer, affinity)
);

-- route.report is idempotent on decision_id: the first body and the answer it
-- got, so the same body again returns that answer and another body conflicts.
CREATE TABLE routing_v1_report (
    decision_id TEXT PRIMARY KEY,
    report_id TEXT NOT NULL,
    body_json TEXT NOT NULL,
    answer_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

-- route.account_for v1 is idempotent on (calling runner, admission_id) over
-- the whole request body with defaults applied, which is wider than the
-- legacy admission fingerprint, so the first body and answer are kept here.
CREATE TABLE routing_v1_admission (
    principal TEXT NOT NULL,
    admission_id TEXT NOT NULL,
    body_json TEXT NOT NULL,
    answer_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (principal, admission_id)
);
