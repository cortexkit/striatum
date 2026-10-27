-- Account-class authority is frozen with the decision, not looked up from the
-- current consumer policy when a runner later asks for an account.
ALTER TABLE model_route_decisions ADD COLUMN allowed_account_class TEXT NOT NULL
    DEFAULT 'not_pay_per_token'
    CHECK (allowed_account_class IN ('priced_per_token_only', 'not_pay_per_token'));
ALTER TABLE routing_v1_decision ADD COLUMN allowed_account_class TEXT NOT NULL
    DEFAULT 'not_pay_per_token'
    CHECK (allowed_account_class IN ('priced_per_token_only', 'not_pay_per_token'));
UPDATE model_route_decisions SET allowed_account_class = 'priced_per_token_only'
    WHERE consumer IN ('codemode', 'flow');
UPDATE routing_v1_decision SET allowed_account_class = 'priced_per_token_only'
    WHERE consumer IN ('codemode', 'flow');

-- Keep the original evidence and its timestamps. The old scope is retained in
-- the evidence JSON; the pool key is the durable identity used by pool blocks.
-- A NULL pool key denotes a whole-account block, whose recovery set is stored
-- independently so partial readings cannot forget a previously required pool.
ALTER TABLE account_fence ADD COLUMN pool_key_json TEXT;
ALTER TABLE account_fence ADD COLUMN recovery_set_json TEXT NOT NULL DEFAULT '[]';
UPDATE account_fence SET pool_key_json = json_object(
    'window_minutes', CASE
        WHEN json_extract(scope_json, '$.kind') = 'general' THEN CASE
            json_extract(scope_json, '$.value')
            WHEN 'five_hour' THEN 300
            WHEN 'seven_day' THEN 10080
            ELSE json_extract(scope_json, '$.value.reported') END
        ELSE NULL END,
    'scope', CASE WHEN json_extract(scope_json, '$.kind') = 'general'
        THEN json_object('kind', 'every_model')
        ELSE json_object('kind', 'family', 'value', json_extract(scope_json, '$.value')) END,
    'name', NULL)
WHERE json_valid(scope_json)
    AND json_extract(scope_json, '$.kind') IN ('general', 'family', 'unknown_scope');

-- Reservations and their exact replies, observations, affinities, reports and
-- credential refusals survive. Only holds are recomputed on the next request.
DELETE FROM account_hold;
DROP TABLE provider_reset_fences;
