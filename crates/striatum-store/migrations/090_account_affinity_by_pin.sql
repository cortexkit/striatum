-- Ending an admission's lease names its pin, not its conversation key.
-- Avoid scanning retained affinity history under the account writer.
CREATE INDEX account_affinity_by_pin ON account_affinity (pin_id);
