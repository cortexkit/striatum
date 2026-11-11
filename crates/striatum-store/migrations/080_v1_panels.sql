CREATE TABLE routing_v1_panel (
    panel_id TEXT PRIMARY KEY,
    principal TEXT NOT NULL,
    state_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX routing_v1_panel_created ON routing_v1_panel(created_at_ms);
