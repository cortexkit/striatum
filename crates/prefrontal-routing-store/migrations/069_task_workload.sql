-- Task identity uses the decision's consumer, not the coarse usage class used
-- by cold-start pricing. Existing usage stays unattributed; no backfill.
ALTER TABLE model_route_usage ADD COLUMN task_workload TEXT;

-- The build reads historical rows once but writes no entries for them, because
-- the new task_workload column is NULL on every pre-upgrade usage row.
CREATE INDEX model_route_usage_task_idx
    ON model_route_usage(task_workload, task_id, observed_at)
    WHERE task_id IS NOT NULL AND task_workload IS NOT NULL;
