-- Preserve the admitted tier and account set independently of later plan changes.
ALTER TABLE model_route_decisions ADD COLUMN service_tier_chosen TEXT;
ALTER TABLE model_route_decisions ADD COLUMN service_tier_accounts_json TEXT;
ALTER TABLE model_route_usage ADD COLUMN service_tier_applied TEXT;
ALTER TABLE model_route_usage ADD COLUMN service_tier_served TEXT;
ALTER TABLE model_route_usage ADD COLUMN service_tier_price_status TEXT;
ALTER TABLE model_route_usage ADD COLUMN service_tier_cost_usd REAL;
