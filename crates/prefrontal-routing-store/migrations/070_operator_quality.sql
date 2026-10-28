CREATE TABLE routing_operator_quality_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    active INTEGER NOT NULL
);
INSERT INTO routing_operator_quality_state VALUES (1, 0);

CREATE TABLE routing_operator_quality (
    route_key TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    elo REAL NOT NULL,
    eq REAL NOT NULL
);
CREATE INDEX routing_operator_quality_model ON routing_operator_quality(model);

CREATE TABLE routing_unscored_admissible_models (
    model TEXT PRIMARY KEY,
    first_seen_ms INTEGER NOT NULL,
    last_seen_ms INTEGER NOT NULL
);
