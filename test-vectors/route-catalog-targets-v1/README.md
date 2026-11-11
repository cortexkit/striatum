# route.catalog_targets v1

Producer: `striatum-module`, through `select::handle_select_request`
and `catalog_targets::serve`. The JSON files are exact subc frame body bytes
(no trailing newline), not hand-written response examples.

- `resolved.json`: Response from an admitted `reserved:occipital` caller.
  Catalog snapshot version 41 contains `anthropic` and `openai` rows. The
  credential listing maps `anthropic-platform` to the sole catalog vendor
  `anthropic`. Ambiguous and unknown providers are absent, as are providers
  already present in the catalog.
- `warming.json`: Error before routing has a catalog snapshot or credential
  listing. `routing_not_ready` is retryable; it is not a successful empty map.
- `SHA256SUMS`: SHA-256 hashes of the two JSON bodies.

Regenerate from the worktree root with:

```sh
CARGO_INCREMENTAL=0 UPDATE_ROUTE_CATALOG_TARGETS_VECTORS=1 cargo nextest run -p striatum-module --bin ck-striatum -E 'test(=tests::catalog_targets_contract::serving_bytes_match_tagged_vectors)'
```

The same test, without the update variable, checks the producer bytes and hashes
against this tagged set. The other catalog-targets tests exercise registration,
snapshot changes, principal admission, and the two independent startup inputs.
