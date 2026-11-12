-- History retention: compact decision detail after a short window, and the
-- indexes the retention pass needs to prune tables that used to grow forever.

-- When the retention pass replaced this decision's per-candidate scoring and
-- quota signals with a compact summary (Unix ms). NULL means the row still
-- holds full detail: it is younger than the full-detail window, or it is in
-- the deterministic full-detail sample. Adding a nullable column rewrites no
-- existing row.
ALTER TABLE model_route_decisions ADD COLUMN detail_stripped_at INTEGER;

-- How far the compaction scan has walked the decision ledger, in
-- (created_at, id) order. The scan never revisits a row behind it, so rows
-- kept at full detail by the sample are read once, not on every pass.
CREATE TABLE model_route_decision_detail_cursor (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    created_at INTEGER NOT NULL,
    decision_id TEXT NOT NULL
);

CREATE INDEX routing_v1_decision_by_created ON routing_v1_decision(created_at_ms);
CREATE INDEX routing_v1_report_by_created ON routing_v1_report(created_at_ms);
CREATE INDEX routing_v1_report_delivery_by_created
    ON routing_v1_report_delivery(created_at_ms);
CREATE INDEX routing_v1_admission_by_created ON routing_v1_admission(created_at_ms);
CREATE INDEX routing_v1_affinity_by_updated ON routing_v1_affinity(updated_at_ms);
CREATE INDEX account_continue_previous ON account_continue(previous_decision_id);
CREATE INDEX model_route_speed_samples_by_observed ON model_route_speed_samples(observed_at);
CREATE INDEX routing_call_count_by_last_called ON routing_call_count(last_called_at_ms);
CREATE INDEX fence_retired_origin_by_retired ON fence_retired_origin(retired_at);
CREATE INDEX routing_unscored_admissible_models_by_last_seen
    ON routing_unscored_admissible_models(last_seen_ms);
