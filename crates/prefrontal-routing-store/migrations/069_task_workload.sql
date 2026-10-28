-- Task identity uses the decision's consumer, not the coarse usage class used
-- by cold-start pricing. Existing usage stays unattributed; no backfill or scan.
ALTER TABLE model_route_usage ADD COLUMN task_workload TEXT;
