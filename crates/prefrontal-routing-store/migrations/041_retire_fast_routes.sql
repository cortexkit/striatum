-- Retire invalid Broca -fast model IDs. Keep decision and usage history intact.
-- Route-local runtime telemetry has no FK, but must not outlive its fact.
DELETE FROM model_route_state
WHERE route_key IN (SELECT route_key FROM model_facts WHERE route_key LIKE '%-fast');
DELETE FROM model_facts WHERE route_key LIKE '%-fast';
