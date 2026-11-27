# Synthetic credential-joined multiplier report

The `spend.multiplier.report` v2 reply includes OpenCode Go and Kimi plans that
name a credential rather than an account. Both are ineligible and have null
multipliers. The tests edit in-memory copies to exercise eligibility,
fallbacks, and ambiguous or missing joins. The other five plans use a distinct
set of twelve synthetic accounts so they cannot join to the account-only roster.
