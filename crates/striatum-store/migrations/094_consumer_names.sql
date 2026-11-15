-- Consumer names read back for policy must use the module-owned namespaces.
-- The ledger's consumer stays historical. Its target_agent also supplies legacy
-- admission priority after the shorter-lived v1 binding has been pruned, so
-- rewrite the ledger's target_agent value even while its v1 binding still exists.
-- Live-copy rehearsal renamed 39,413 ledger targets (22,518 bound, 16,895
-- unbound), 22,518 v1 decision consumers and 1,453 panel consumers, with no
-- affinity renames/collisions. Measured about 2.6 s total: 2.24 s for the
-- ledger update and 0.31 s to commit. Routing is offline during startup migration.
CREATE TEMP TABLE routing_consumer_rename (old TEXT PRIMARY KEY, new TEXT NOT NULL);
INSERT INTO routing_consumer_rename VALUES
    ('mason', 'prefrontal-core.mason'),
    ('sidekick', 'prefrontal-core.sidekick'),
    ('athena', 'prefrontal-core.athena'),
    ('athena.gather', 'prefrontal-core.athena.gather'),
    ('synthesizer', 'prefrontal-core.athena.synthesizer'),
    ('lab', 'prefrontal-core.lab'),
    ('core', 'prefrontal-core.wake'),
    ('codemode', 'basal.codemode'),
    ('flow', 'basal.flow'),
    ('mc_summary', 'magic-context.summary'),
    ('mc_dream', 'magic-context.dream'),
    ('mc_dream_classify', 'magic-context.dream.classify-memories'),
    ('mc_dream_curate', 'magic-context.dream.curate'),
    ('mc_dream_compress_cues', 'magic-context.dream.compress-cues'),
    ('mc_dream_review_user', 'magic-context.dream.review-user-memories'),
    ('mc_dream_map', 'magic-context.dream.map-memories'),
    ('mc_dream_primers', 'magic-context.dream.refresh-primers'),
    ('mc_dream_retrospective', 'magic-context.dream.reflect-events'),
    -- The settings rename copied the old verify rules to magic-context.dream.verify
    -- and magic-context.dream.verify-broad. Store the direct successor, verify,
    -- and keep any dotted suffix on the stored name.
    ('mc_dream_verify', 'magic-context.dream.verify');

CREATE TEMP TABLE routing_consumer_names (old TEXT PRIMARY KEY, new TEXT NOT NULL);
INSERT INTO routing_consumer_names
SELECT name, (
    SELECT new || substr(name, length(old) + 1)
    FROM routing_consumer_rename
    WHERE name = old OR substr(name, 1, length(old) + 1) = old || '.'
    ORDER BY length(old) DESC LIMIT 1
)
FROM (
    SELECT consumer AS name FROM routing_v1_decision
    UNION SELECT consumer FROM routing_v1_affinity
    UNION SELECT json_extract(state_json, '$.request.consumer') FROM routing_v1_panel
        WHERE json_valid(state_json)
    UNION SELECT target_agent FROM model_route_decisions
)
WHERE EXISTS (
    SELECT 1 FROM routing_consumer_rename
    WHERE name = old OR substr(name, 1, length(old) + 1) = old || '.'
);

UPDATE routing_v1_decision SET consumer = (
    SELECT new FROM routing_consumer_names WHERE old = consumer
) WHERE consumer IN (SELECT old FROM routing_consumer_names);

-- An affinity can exist under both spellings. Keep the newest preference or
-- end marker, with the already-canonical row winning a timestamp tie.
INSERT INTO routing_v1_affinity
    (principal, consumer, affinity, decision_id, updated_at_ms, ended_at_ms)
SELECT principal, names.new, affinity, decision_id, updated_at_ms, ended_at_ms
FROM routing_v1_affinity JOIN routing_consumer_names AS names ON consumer = names.old
ORDER BY updated_at_ms, consumer DESC
ON CONFLICT (principal, consumer, affinity) DO UPDATE SET
    decision_id = excluded.decision_id,
    updated_at_ms = excluded.updated_at_ms,
    ended_at_ms = excluded.ended_at_ms
WHERE excluded.updated_at_ms > routing_v1_affinity.updated_at_ms;
DELETE FROM routing_v1_affinity WHERE consumer IN (SELECT old FROM routing_consumer_names);

UPDATE routing_v1_panel SET state_json = json_set(state_json, '$.request.consumer', (
    SELECT new FROM routing_consumer_names
    WHERE old = json_extract(state_json, '$.request.consumer')
)) WHERE json_valid(state_json) AND json_extract(state_json, '$.request.consumer')
    IN (SELECT old FROM routing_consumer_names);

UPDATE model_route_decisions AS ledger SET target_agent = (
    SELECT new FROM routing_consumer_names WHERE old = ledger.target_agent
) WHERE target_agent IN (SELECT old FROM routing_consumer_names);

DROP TABLE routing_consumer_names;
DROP TABLE routing_consumer_rename;
