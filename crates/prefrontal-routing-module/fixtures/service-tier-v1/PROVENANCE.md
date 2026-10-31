# Service tier fixtures

`cases.json`, `SHA256SUMS` and `README.md` are byte-for-byte copies of
the CortexKit commons repository (<https://github.com/cortexkit/commons>),
`test-vectors/service-tier-v1` at
`1af6bfb593f979b4e9b500aa1ab82dec7b01aaf1`.
`route-select-baseline.json` is that revision's existing
`crates/cortexkit-role-routing/test-vectors/routing-v1/route-select.json`;
its pre-tier answers pin omission bytes.

`fusiform-served-payloads.json` is an unchanged copy of
`crates/fusiform-module/fixtures/served-payloads.json` at
`a00547d2bc0f3029ebae3034b232c31da4ac2ac3`. Tests read the provider-default
GPT-6.1 Sol model and curated `plan.prices` cases, including their mode rates
and exact-decimal included-usage and credit rates.
