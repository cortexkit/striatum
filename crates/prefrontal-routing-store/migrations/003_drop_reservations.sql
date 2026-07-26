-- Migration 3 (namespace "route_state"): remove the never-wired reservation table.
-- Manager concurrency moved to the M7 slot system, so router-side reservations are
-- dead complexity and their derived in-flight gate was deleted.

DROP TABLE IF EXISTS model_route_reservations;
