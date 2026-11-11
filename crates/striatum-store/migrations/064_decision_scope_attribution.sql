-- The project and session folder the daemon stamped on a decision's scope.
--
-- A route opened under a scope reaches this module with the daemon's stamp,
-- which names the Entorhinal project (attribution only, never authority) and
-- the session folder. Both are recorded beside the decision so a run is
-- attributed to its project. Historical rows and routes opened without a
-- scope keep NULL: neither value is ever derived from the request.
ALTER TABLE model_route_decisions ADD COLUMN project_id TEXT;
ALTER TABLE model_route_decisions ADD COLUMN workdir TEXT;
