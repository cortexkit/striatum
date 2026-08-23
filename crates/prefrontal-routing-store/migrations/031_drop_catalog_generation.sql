-- Migration 31 (namespace "route_state"): catalog generation rows were written
-- for every catalog change but never read. Retire the unused history table.
DROP TABLE IF EXISTS catalog_generation;
