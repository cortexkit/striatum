# route.head_choices v1

Each JSON file is a request/reply pair captured through the real routing handler
with a stand-in catalog (version 41), vault accounts, plans and runner declarations.
The reply is decoded from the handler's subc frame body; the pair is compact JSON
without a trailing newline.

- `with-tiers.json`: declared runner, head reasoning default and standard/fast tiers; ultrafast is absent because the runner cannot apply it.
- `without-tiers.json`: the same model reach on a runner without tier support, so only standard is offered.
- `policy-excluded.json`: a runner reaches the blocked model, but head `not_for` policy leaves it out.
- `pin-only.json`: a person can pick the pin-only model, with no invented default reasoning level.
- `SHA256SUMS`: SHA-256 hashes of the four captured request/reply pairs.

Regenerate from the worktree root. When using CortexKit's `bash` tool with
`runon` for a remote build, set its `return_paths` argument to the four JSON
paths and `SHA256SUMS` under `test-vectors/route-head-choices-v1/`. That argument
copies the generated files back to the same paths in the local worktree:

```sh
UPDATE_ROUTE_HEAD_CHOICES_VECTORS=1 cargo nextest run --locked -p prefrontal-routing-module --bin ck-prefrontal-routing -E 'test(=routing_v1::head_choices_tests::serving_bytes_match_tagged_vectors)'
```

Setting `UPDATE_ROUTE_HEAD_CHOICES_VECTORS=1` writes the generated files.
Without `UPDATE_ROUTE_HEAD_CHOICES_VECTORS`, the same test regenerates the pairs
in memory and fails on any byte or digest difference.
