# route.head_choices v1 (routing 0.7.11)

Each JSON file is a request/reply or request/refusal pair captured through the real routing handler.
Except for `broca-live-catalog.json`, the pairs use a stand-in catalog (version 41),
vault accounts, plans and runner declarations.
A reply pair has `request` and `reply` keys: `reply` is decoded from the handler's
Response frame body and contains the `result` envelope. A refusal pair has
`request` and `refusal` keys instead: `refusal` is the exact Error frame body,
including its field order, with `code`, `message` and `detail` at the top level
and no `result` envelope. Each pair is compact JSON without a trailing newline.

These samples use the routing 0.7.11 contract: `catalog_version` is a string,
`billing` is `subscription` or `per_token`, and arrays use bytewise ordering.
Every caller can read choices, without operator authorization. An unknown runner
or one that cannot start heads receives an answer with an empty `models` list.
The optional request `harness` narrows the runner's reach using its declared
`harness_limits`; a hosted harness without an entry inherits runner-level limits.
An unhosted harness receives `no_route` with detail reason `harness_unavailable`.
The contract is also pinned by
`crates/striatum-module/fixtures/replay/head-choices-difference-9.golden.json`;
that replay uses a different catalog and model set.

- `with-tiers.json`: declared runner, an automatic subscription entry, head reasoning default and standard/fast tiers; ultrafast is absent because the runner cannot apply it.
- `without-tiers.json`: the same model reach on a runner without tier support, so only standard is offered.
- `policy-excluded.json`: a runner reaches the blocked model, but head `not_for` policy leaves it out.
- `pin-only.json`: a person can pick the pin-only model, with no invented default reasoning level.
- `pin-only-pay-per-token.json`: a pin-only model on a runner that can use only the API key, not the subscription credential. The filename is retained from tag `route-head-choices-vectors-v1.1`; the wire billing value is now `per_token`.
- `unknown-runner.json`: an undeclared runner receives an empty `models` list.
- `non-starting-runner.json`: a declared runner with `starts_sessions: false` receives an empty `models` list.
- `harness-narrowed.json`: `harness-runner` hosts `opencode2` and `claude-code`, defaults to `opencode2`, and reaches both OpenAI and Anthropic at runner level. The request selects `claude-code`, whose `harness_limits` allow only the `anthropic.messages`/`oauth` pair and explicit provider `anthropic`, so only `anthropic/claude` is listed.
- `harness-runner-wide.json`: the same runner with `harness: "opencode2"`. It has no harness-specific limits, so both Anthropic and OpenAI models are listed.
- `harness-unavailable.json`: the same runner with `harness: "pi"`, which it does not host. The refusal pair captures `no_route` with `detail.reason: "harness_unavailable"`.
- `broca-live-catalog.json`: a phone picker sample with 25 models, generated from a frozen snapshot of 14 Nov 2026, the worker runner's real runner declaration and tier capabilities, and a redacted credential roster. It includes subscription and per-token entries, pin-only models, and fast tiers.
- `SHA256SUMS`: SHA-256 hashes of the eleven captured pairs (ten replies and one refusal).

The independent frozen inputs are in `crates/striatum-module/fixtures/head-choices-broca-live-inputs-20261114.json`.
They contain the selected stored model rows, relevant catalog and billing-plane facts,
policy, runner facts and a reduced roster with credential and account IDs replaced by
stable placeholders. No credential secrets are recorded. The catalog and billing-plane
facts were read at version `1794680117849`, after the stored rows and the reference
picker answer at version `1794678317842`; all 25 generated model entries match that
earlier answer field for field (provider, model, billing, pin-only, variants, default
variant and service tiers). The 208-model reference response was captured from live
routing for the comparison and is not published as a fixture. GPT-6.1 Sol and Terra
are included because the reference picker did not list GPT-6 Sol or Terra. The model
set, provider policy and credential roster are
subsets, and the fixture keeps only facts needed by routing; other consumers' policy
and non-head harness consumer settings are omitted. Google billing-plane reads were
not served, so those plane pages are absent. Plan rates come from the checked-in
`service-tier-v1/fusiform-served-payloads.json` `plan.prices — every curated row`
case, notably OpenAI `pro_100`, `pro_200` and `pro_500`, rather than a fresh plan
read. These reductions and the newer catalog version are the fixture's differences
from the live inputs.

Regenerate from the worktree root. When using CortexKit's `bash` tool with
`runon` for a remote build, set its `return_paths` argument to all eleven JSON
paths listed above and `SHA256SUMS` under `test-vectors/route-head-choices-v1/`. That argument
copies the generated files back to the same paths in the local worktree:

```sh
UPDATE_ROUTE_HEAD_CHOICES_VECTORS=1 cargo nextest run --locked -p striatum-module --bin ck-striatum -E 'test(=routing_v1::head_choices_tests::serving_bytes_match_tagged_vectors)'
```

Setting `UPDATE_ROUTE_HEAD_CHOICES_VECTORS=1` writes the generated files.
Without `UPDATE_ROUTE_HEAD_CHOICES_VECTORS`, the same test regenerates the pairs
in memory and fails on any byte or digest difference, with no normalization of
the published samples. Check the captured files with:

```sh
(cd test-vectors/route-head-choices-v1 && shasum -a 256 -c SHA256SUMS)
```
