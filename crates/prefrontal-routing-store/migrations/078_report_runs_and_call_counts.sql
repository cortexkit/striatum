CREATE TABLE routing_v1_report_delivery (
    decision_id TEXT NOT NULL REFERENCES routing_v1_decision(decision_id) ON DELETE CASCADE,
    delivery_key TEXT NOT NULL,
    report_id TEXT NOT NULL,
    body_json TEXT NOT NULL,
    answer_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (decision_id, delivery_key)
);

INSERT INTO routing_v1_report_delivery
    SELECT decision_id,
           CASE WHEN json_type(body_json, '$.usage') = 'object' THEN 'usage' ELSE 'outcome' END,
           report_id, body_json, answer_json, created_at_ms
    FROM routing_v1_report
    WHERE decision_id IN (SELECT decision_id FROM routing_v1_decision);

CREATE TABLE routing_call_window (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    started_at_ms INTEGER NOT NULL
);
INSERT INTO routing_call_window VALUES (1, CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER));

CREATE TABLE routing_call_count (
    method TEXT NOT NULL,
    shape TEXT NOT NULL,
    principal TEXT NOT NULL,
    calls INTEGER NOT NULL CHECK (calls > 0),
    last_called_at_ms INTEGER NOT NULL,
    PRIMARY KEY (method, shape, principal)
);
