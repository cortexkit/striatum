# route.select declared consumers v1 (routing 0.7.22)

This set uses separate request/reply files, like `route-report-attempt-v1`, rather
than the combined files of `route-head-choices-v1`. Each `<stem>.request.json` is
the compact `route.select` handler body with `method` and `params`; its
`<stem>.reply.json` is the Response frame body with the `result` envelope.
Neither JSON file has a trailing newline. The bytes are captured through the
real routing handler, without normalizing member order or generated fields.

The stand-ins are the routing conformance model catalog, vault accounts and
runner declarations, extended with the declarations below. Each pair gets a
fresh store, a clock fixed at `1800000000000` milliseconds since the Unix epoch,
and replay IDs starting at zero. The subscription multiplier is fixed at 22.
These are illustrative catalog rates, not current provider quotes.

Production routing advertises `consumer_declarations`, `decision_harness`,
`select_eligible_routes` and `select_price`. Capture stops if any is absent;
no sample uses a test-only capability override. Every answer passes the pinned
crate's `check_answer_serving` for its request, resolving the declared IQ default
before checking the Magic Context answer.

| Filename stem | Declaration / request | Answer |
| --- | --- | --- |
| `basal-flow` | Basal declares `basal.flow` with `placement: {module: "broca"}` and no demand default. Per-call IQ 50, `tools: "none"`, and exact `eligible_routes` narrow the call. | Broca, even though another runner appears first in settings. No `runner.harness`: today's harness-less Broca shape. |
| `magic-context-summary` | Magic Context declares `magic-context.summary`, IQ 40 and `placement: "caller_runner"`. The request omits IQ and sends `tools: "none"` and `runners: ["broca"]`. | The non-runner attested caller `reserved:magic-context` resolves to the single catalog runner Broca. Resolved IQ is 40. |
| `price-metered` | Pin `example-provider/model-a`. | `basis: "metered"`, catalog list rates, no multiplier. |
| `price-subscription-5m-1h` | Pin `example-subscription/model-c`. | `basis: "subscription_equivalent"`, multiplier 22, and `5m` / `1h` cache-write lifetimes. |
| `price-subscription-30m-12h` | The same subscription route, with different stand-in catalog rates. | Multiplier 22 and `30m` / `12h` lifetimes, preserving catalog lifetime names. |
| `price-unpriced` | Pin the metered route with all catalog rates removed. | No `price`, not a zero-price quote. A dollar-capped caller must refuse to send without a price. |
| `decision-harness` | Request `harness: "sample-harness"` on a runner declaring that harness. | `runner.module: "sample-harness-runner"`, `runner.harness: "sample-harness"`. |
| `broca-harness` | The same Basal demand as `basal-flow`, with Broca's real after-slot installed verbatim from `crates/striatum-module/src/routing_v1/runners/broca-runner-slot.after.json`. | `runner.module: "broca"`, `runner.harness: "broca"`. This is the shape core sees once Broca switches its harness facts on. |

The short-lifetime catalog quotes input 4, output 20, cache read 1, cache write 5,
and cache-write TTL rates `5m: 5`, `1h: 8` USD per million tokens. Metered rates
stay at list; subscription rates divide every bucket by 22. The long-lifetime
catalog quotes input 22, output 110, cache read 11, cache write 44, and TTL rates
`30m: 55`, `12h: 88`, again divided by 22. No task history is seeded, so no
`estimated_task_usd` is available. The price pairs use `example_summary`, the
stand-in summary consumer supplied by the routing conformance fixture; the
declared-consumer pairs exercise admission and placement directly.

`caller_runner` resolves an attested `reserved:<module>` caller to that module
when it is a catalog runner. Otherwise it resolves the single `runners` entry,
which must name a catalog runner. The Magic Context pair exercises the latter
case: the attested caller is not a runner. This identity comes from the bound
route, not from any request-body claim.

The annotated tag for this set is `route-select-declared-vectors-v1.0`, following
the `route-head-choices-vectors-v1.1` naming scheme. The maintainer sets it on
merged main during deployment checks. This slice does not create the tag.

## Regeneration and verification

From the worktree root:

```sh
UPDATE_ROUTE_SELECT_DECLARED_VECTORS=1 cargo nextest run --locked -p striatum-module --bin ck-striatum -E 'test(=routing_v1::select::declared_samples_tests::serving_bytes_match_tagged_vectors)'
```

For a remote build with read-only synced sources, run regeneration in a writable
scratch copy. Copy the sixteen JSON files and `SHA256SUMS` to a new output
directory, return those paths, then install the files here locally. Only the
update environment variable permits writing generated files; it does not rewrite
this README. Without it, the same test generates samples in memory and fails on
any byte or digest difference, and rejects unlisted files by name.

`SHA256SUMS` covers all sixteen JSON files and this README in bytewise filename
order, excluding only the manifest itself. Independently verify it with:

```sh
(cd test-vectors/route-select-declared-v1 && shasum -a 256 -c SHA256SUMS)
```
