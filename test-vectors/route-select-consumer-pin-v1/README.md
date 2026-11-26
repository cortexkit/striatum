# Consumer settings pin: `route.select` serving samples

These are raw request and reply bodies recorded from Striatum's real frame
handler. The catalog, credentials, clock and decision IDs are deterministic
test fixtures; the replies are not normalized or rewritten after capture.

The fixture has Opus 5.5 and Fable 5.2 on an eligible runner, with equal
capability and quality. Opus is `pin_only_for: ["prefrontal-core.athena"]`. The operator sets
`consumers["prefrontal-core.athena.synthesizer"].pin` to
`{model: "anthropic/claude-opus-5-5", variant: "medium"}`. No request carries
a pin.

| Pair | What the caller can rely on |
| --- | --- |
| `pinned-chair` | The chair consumer selects exactly Opus 5.5 medium through explicit pin admission. |
| `unpinned` | The panel consumer has no settings pin and selects Fable automatically; Opus remains pin-only. |
| `pin-unavailable` | Changing the settings pin to the absent `anthropic/missing` model refuses with `pin_unavailable`, reason `unknown_model`; Fable is not substituted. |

The answer explanation identifies the consumer's operator pin. Per
`cortexkit-role-routing` 0.7.23 CONTRACT §5.2, `pin_outcome` is present exactly
when the **request** pinned, so it is absent from these settings-only answers.
The successful samples pass that crate's `check_answer_serving` against the
original, unpinned requests. The caller should use the returned route and runner
as-is instead of selecting a different route for the chair.

Regenerate intentionally from the repository root:

```sh
UPDATE_ROUTE_SELECT_CONSUMER_PIN_VECTORS=1 cargo nextest run --locked -p striatum-module \
  -E 'test(consumer_pin_serving_bytes_match_vectors)'
```

Without the environment variable, the test regenerates in memory and compares
every byte, checks `SHA256SUMS` (including this README) and rejects extra files.
An independent checksum check is:

```sh
cd test-vectors/route-select-consumer-pin-v1
shasum -a 256 -c SHA256SUMS
```
