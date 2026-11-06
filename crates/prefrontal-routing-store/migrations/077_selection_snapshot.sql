-- Only new selections have a snapshot. Older decisions cannot reconstruct
-- selection-time capacities or runner credentials from the current catalog.
ALTER TABLE routing_v1_decision ADD COLUMN selection_json TEXT;
