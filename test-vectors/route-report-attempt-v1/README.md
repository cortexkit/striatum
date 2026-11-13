# route.report attempt outcomes v1 (routing 0.7.21)

This set uses request/reply files per status rather than the combined form of the head-choices set; each file is the verbatim handler body.
`<status>.request.json` contains the compact `route.report` request with `method`
and `params`; `<status>.reply.json` contains the Response frame body with its
`result` envelope. Neither JSON file has a trailing newline. The bytes are
captured through routing's real handler, without normalizing member order or
replacing generated fields.

Each pair starts from a fresh stand-in model catalog, vault accounts and runner
declarations supplied by the routing conformance fixture. A real `route.select`
for its summary consumer pins `example-subscription/model-c` before the report.
The clock is fixed at `1800000000000` milliseconds since the Unix epoch, replay
IDs start from zero for each pair, and the subscription multiplier is 22.
Independent stores let the pairs share a deterministic decision ID without
conflicting with another status's report or inheriting its effects.

The `report_attempt_outcomes` capability is advertised by production routing;
these captures use no test-only capability override. The required legacy
`outcome` accompanies each named status and is echoed in the answer:

| Status / filename stem | Legacy `outcome` | Request detail |
| --- | --- | --- |
| `served` | `completed` | Successful attempt. |
| `served_empty` | `completed` | Successful attempt with no useful output. |
| `quota` | `failed` | Quota-limited attempt. |
| `throttled` | `failed` | Includes `attempt_outcome.retry_after_ms: 30000`. |
| `credential` | `failed` | Credential-rejected attempt. |
| `unavailable` | `failed` | Unavailable route. |
| `rejected` | `cancelled` | Neutral rejection; no route effects. |

These minimal requests omit credential and usage evidence. Additional account
effects remain conditional on the evidence a real caller supplies. Unknown
statuses are tested, not published as samples: see
`unknown_attempt_status_has_no_companion_check_and_echoes_legacy_outcome` and
`rejected_and_unknown_suppress_all_legacy_evidence_but_commit_cancelled` in
`crates/striatum-module/src/routing_v1/report/attempt_tests.rs`.

The annotated tag for this set is `route-report-attempt-vectors-v1.0`, following
the `route-head-choices-vectors-v1.1` naming scheme. The maintainer sets the tag
on merged main during deployment checks; sample generation does not create it.

## Regeneration and verification

From the worktree root:

```sh
UPDATE_ROUTE_REPORT_ATTEMPT_VECTORS=1 cargo nextest run --locked -p striatum-module --bin ck-striatum -E 'test(=routing_v1::report::attempt_samples_tests::serving_bytes_match_tagged_vectors)'
```

For a remote CortexKit build, set `return_paths` to all fourteen
`<status>.request.json` / `<status>.reply.json` files and `SHA256SUMS` in this
directory to copy the generated files back into the worktree.
If the server keeps synced files read-only, regenerate in a writable scratch
copy instead. Stage the generated files in a new output directory and return
those paths, then install them in this directory locally.
Setting the update variable permits writes to those generated files only.
Without it, the same test regenerates the samples in memory and fails on any
sample byte or digest difference, with no writes or normalization. It also
rejects extra files, naming the unexpected file.

`SHA256SUMS` covers all fourteen JSON files and this README in bytewise filename
order, excluding only the manifest itself. Independently verify it with:

```sh
(cd test-vectors/route-report-attempt-v1 && shasum -a 256 -c SHA256SUMS)
```
