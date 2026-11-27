# Synthetic account-joined multiplier report

The `spend.multiplier.report` v2 reply joins twelve synthetic accounts to five
plans. Only `openai/pro_200` supplies an eligible multiplier. The Antigravity
account tests that the report's plan name (`google/ultra_5x`) is matched by
account reference, not inferred from the quota provider. The historical series
uses the same synthetic account references as `latest`.
