# Concurrency-full refusals and panel shortfalls on routing/v1

These request/reply pairs are captured through routing's real frame handler
using synthetic fixtures and the pinned routing contract.
Requests contain `method` and `params`. Selection and account refusals are Error
frame bodies with no `result` or `error` envelope; panel replies are Response
frame bodies with a `result` envelope. JSON bytes are preserved without a trailing
newline. The original three pairs remain byte-identical to Git tag
`route-concurrency-full-v1.1`.

Each capture starts with a fresh store and deterministic replay IDs. At epoch
millisecond `1800000000000`, the runner reserves the one available slot on each
account to be filled through `route.account_for`. Enforcement is
enabled only in this fixture; the production default remains off. The clock then
advances to the fixed sample time `1800000048000`. Filled reservations expire at
`1800000060000`, so the reset is twelve seconds away, not the thirty-second
fallback. For the original three pairs, the quota reading remains fresh and
neither of the two accounts is blocked.

* `select.request.json` / `select.reply.json`: the owner of the worker consumer
  `mason` asks for a route. All adequate routes are full. The answer is `no_route`
  with reason `concurrency_full`, the reset time and the rejected route list.
  The complete diagnostics also retain seeded catalog routes rejected because
  they have no eligible account; only the synthetic route has eligible accounts.
* `account-for.request.json` / `account-for.reply.json`: a runner asks to admit a
  new conversation. The answer is `no_route` with reason `concurrency_full` and
  exactly the allocator's `reset_at_ms`. A separate legacy call in the test
  confirms the same reset. Routing emits no `is_wait` member: the pinned
  `cortexkit-role-routing` contract derives wait classification from the
  `concurrency_full` reason.
* `basal-flow-select.request.json` / `basal-flow-select.reply.json`: attested
  `reserved:basal` selects its declared consumer `basal.flow`, with Broca as
  its placement and without a request `detail` member. The request uses Basal's
  `requirements.iq`, `requirements.tools` and `eligible_routes` fields, with the
  single synthetic route substituted.
  The fixture removes all seeded model facts except `synthetic/model`; its two
  synthetic accounts are filled via the same real admissions as above. This keeps
  the public sample free of real catalog routes, including in refusal diagnostics.
* `panel-all-full.request.json` / `panel-all-full.reply.json`: attested
  `reserved:prefrontal` asks for an `audit` panel for consumer
  `prefrontal.athena`, matching the Athena caller and class in panel tests.
  The synthetic policy requests three seats, without extras. All three candidate
  accounts are full. Routing returns an answer with zero seats and a
  `size_reduced` shortfall, not a refusal.
* `panel-partial.request.json` / `panel-partial.reply.json`: the same Athena caller,
  consumer and class request three seats, with only two accounts filled. One seat
  stands and two candidates are rejected on concurrency, giving a `size_reduced`
  shortfall. Each cause has `reason: "no_route"`, a cause-level `reset_at_ms`, and
  a nested refusal whose `detail.reason` is `concurrency_full`.

Both panel fixtures remove the seeded catalog entirely and install only
`synthetic-0/panel-model`, `synthetic-1/panel-model` and `synthetic-2/panel-model`,
with distinct `claude`, `gpt` and `gemini` families so diversity is satisfiable.
Broca serves one API-key account per synthetic provider. The accounts are filled
by real `route.account_for` admissions, not injected reservation rows; enforcement
is enabled only in the fixture. No real catalog routes appear in either panel
pair, including nested diagnostics.

Core reads the panel result's `shortfall.detail.causes[i].reset_at_ms` and
`shortfall.detail.causes[i].detail.detail.reason` (the latter is
`causes[i].detail.detail.reason` relative to `shortfall.detail`). The reset belongs
to each cause, not directly to the shortfall. Both panel replies preserve
`1800000060000`, twelve seconds after the sample clock, for every rejected cause.

## Replacement when all candidates are full

`replace-all-full.request.json` / `replace-all-full.reply.json` capture a
replacement through `route.select_panel` with a `replace` parameter. A two-seat
panel is selected first; then all three synthetic accounts are filled through
real admissions. The response is an unseated `PanelReplacement` with cause
`no_candidate`. Each concurrency rejection carries `reset_at_ms` at
`result.detail.causes[i].reset_at_ms` and again in the nested refusal at
`result.detail.causes[i].detail.detail.reset_at_ms`. The refusal's
`blockingCauses[0].routes[0].detail` and `rejected[0].detail` also contain
JSON-encoded account diagnostics with `reset_at_ms`. Other causes describe
candidates excluded by panel history or family diversity.

## Regeneration and verification

From the worktree root, with writable sources:

```sh
UPDATE_CONCURRENCY_FULL_SAMPLES=1 cargo nextest run --locked -p striatum-module --bin ck-striatum -E 'test(=routing_v1::concurrency_samples_tests::handler_samples_regenerate_byte_for_byte)'
```

For remote builds with read-only synced sources, regenerate in a writable scratch
copy and return the twelve JSON files and `SHA256SUMS`, then install them locally.
The environment variable enables writes only for generated files, not this README.
Without it, regeneration happens in memory and any byte difference, missing file,
unexpected file or manifest drift fails the test. The additional handler tests
check refusal and panel-shortfall semantics independently of the recorded files.

`SHA256SUMS` covers the twelve JSON files and this README, in bytewise filename
order, excluding the manifest itself. Independently verify from the worktree root:

```sh
(cd test-vectors/route-concurrency-full-v1 && shasum -a 256 -c SHA256SUMS)
```
