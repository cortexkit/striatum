# Read-only panel estimates

These samples contain request and reply bodies captured from the real frame
handler on a caller-owned route, at Unix time 1800000000000 ms. The audit policy
selects two seats ranked by model quality (`iq`) and one random extra, with seed
`fixed`. Three
distinct families have context windows of 200000, 200000 and 128000 tokens;
all reserve 32000 output tokens and 16000 prompt tokens. A 10000-token package
fits all seats; a 60000-token package leaves two seats and a typed package-fit
shortfall for the third. No panel or decision identifiers are issued.

Regenerate from the repository root:

```sh
UPDATE_ROUTE_SELECT_PANEL_ESTIMATE_VECTORS=1 cargo nextest run --locked \
  -p striatum-module --bin ck-striatum \
  -E 'test(panel_estimate_serving_bytes_match_vectors)'
```

Run the same command without the environment variable to verify exact byte
identity, SHA256SUMS, and the directory inventory. The generator does not rewrite
existing routing goldens or replay recordings.
