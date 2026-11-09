# route.head_choices v1 (routing 0.7.1)

Each JSON file is a request/reply pair captured through the real routing handler
with a stand-in catalog (version 41), vault accounts, plans and runner declarations.
The reply is decoded from the handler's subc frame body; the pair is compact JSON
without a trailing newline.

These samples use the routing 0.7.1 contract: `catalog_version` is a string,
`billing` is `subscription` or `per_token`, and arrays use bytewise ordering.
Every caller can read choices, without operator authorization. An unknown runner
or one that cannot start heads receives an answer with an empty `models` list.
The contract is also pinned by
`crates/prefrontal-routing-module/fixtures/replay/head-choices-difference-9.golden.json`;
that replay uses a different catalog and model set.

- `with-tiers.json`: declared runner, an automatic subscription entry, head reasoning default and standard/fast tiers; ultrafast is absent because the runner cannot apply it.
- `without-tiers.json`: the same model reach on a runner without tier support, so only standard is offered.
- `policy-excluded.json`: a runner reaches the blocked model, but head `not_for` policy leaves it out.
- `pin-only.json`: a person can pick the pin-only model, with no invented default reasoning level.
- `pin-only-pay-per-token.json`: a pin-only model on a runner that can use only the API key, not the subscription credential. The filename is retained from tag `route-head-choices-vectors-v1.1`; the wire billing value is now `per_token`.
- `unknown-runner.json`: an undeclared runner receives an empty `models` list.
- `non-starting-runner.json`: a declared runner with `starts_sessions: false` receives an empty `models` list.
- `SHA256SUMS`: SHA-256 hashes of the seven captured request/reply pairs.

Regenerate from the worktree root. When using CortexKit's `bash` tool with
`runon` for a remote build, set its `return_paths` argument to all seven JSON
paths listed above and `SHA256SUMS` under `test-vectors/route-head-choices-v1/`. That argument
copies the generated files back to the same paths in the local worktree:

```sh
UPDATE_ROUTE_HEAD_CHOICES_VECTORS=1 cargo nextest run --locked -p prefrontal-routing-module --bin ck-prefrontal-routing -E 'test(=routing_v1::head_choices_tests::serving_bytes_match_tagged_vectors)'
```

Setting `UPDATE_ROUTE_HEAD_CHOICES_VECTORS=1` writes the generated files.
Without `UPDATE_ROUTE_HEAD_CHOICES_VECTORS`, the same test regenerates the pairs
in memory and fails on any byte or digest difference, with no normalization of
the published samples. Check the captured files with:

```sh
(cd test-vectors/route-head-choices-v1 && shasum -a 256 -c SHA256SUMS)
```
