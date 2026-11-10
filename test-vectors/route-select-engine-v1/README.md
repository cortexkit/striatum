# route.select_engine v1

Each JSON file contains a `request`/`reply` pair captured through the routing
module's real frame dispatcher. Both members retain the exact compact JSON
body bytes produced by the harness and handler, including object-member order;
the enclosing pair has no trailing newline. These are request/reply bodies
only, not transport frames, route bindings, or module-address envelopes.

## Caller and fixture

The iPhone calls through Callosum using consumer `phone.media`. The fixture
declares that consumer in Callosum's nonce-verified `routing/v1` slot, so it is
admitted only from `reserved:callosum` on a current route binding. The principal
is transport-attested, not a request member. Successful samples use an unscoped
route; the foreign and direct caller samples intentionally fail ownership.

The clock is fixed at `1750000000000` milliseconds, generated IDs use the store's
deterministic test sequence, and the catalog snapshot is version 41. The
nonce-verified module `fixture-media` declares one remote engine of each named
kind. An active API key (`opaque-media-key`) serves `media-vendor` in the vault's
`media-provider` category, with a category `list` grant. No real credential or
network service is used. Each unavailability scenario starts with fresh state.
Decisions expire ten minutes after the fixed clock.

No sample names a routing module ID. The phone addresses routing by module ID
according to the rename caller rule: try `prefrontal-routing` first, then
`striatum` on `unknown_module`, `target_unavailable`, or `module_warming`.
Those are module-address/transport outcomes, not `route.select_engine` bodies.
The selected engine's `module` member identifies the media executor, not routing.

## Accepted and repeated requests

- `accepted-transcribe.json`: remote transcription (`transcribe`).
- `accepted-synthesize.json`: remote speech synthesis (`synthesize`).
- `accepted-image.json`: image generation (`image`).
- `accepted-video.json`: video generation (`video`).
- `accepted-stream_transcribe.json`: streaming transcription (`stream_transcribe`).
- `accepted-realtime.json`: realtime media (`realtime`).
- `accepted-caller-local.json`: on-phone transcription under `local_only`;
  `caller_local: true` and `index: 0` replace the remote `module` address.
- `accepted-transient-only-account.json`: the only media account has a transient
  backoff. Routing prefers other accounts when possible, but retains a transient
  account as a last resort rather than refusing the engine.
- `repeated-transcribe.json`: exactly the same request body as
  `accepted-transcribe.json`, sent again in the same fixture. The engine is the
  same, but the `decision_id` is new.

`route.select_engine` has **no idempotent replay key**. Repeating a request is a
fresh selection, not retrieval of the prior decision. The earlier decision
simply expires unused; a caller should use the `decision_id` from the reply it
received. An `effect_id` is not accepted, as `invalid-request.json` demonstrates.
The harness's deterministic ID/clock replay controls are test inputs only,
not a production idempotency mechanism.

## Refusals

The handler can return `invalid_request`, `invalid_requirements`,
`unknown_consumer`, `consumer_not_owned`, `no_engine`, and `store_error`.
The frame dispatcher also returns `flow_scope_not_supported` before selection
if the daemon stamped the route with a flow ID.
Several different causes share `no_engine`; its `detail.filter` names the first
filter that emptied the candidates, in order: `kind`, `requirements`, `privacy`,
`reachability`. Engine refusals do not include `retry_at_ms`.

- `no-engines.json`: no engine declarations; `no_engine`, filter `kind`.
- `unknown-kind.json`: the open kind enum accepts `future_kind`, but no declared
  engine matches; `no_engine`, filter `kind`, not an `unknown_kind` code.
- `requirements-unmet.json`: no engine declares French; filter `requirements`.
- `privacy-local-only.json`: only remote engines exist; filter `privacy`.
- `invalid-request.json`: unsupported `effect_id`; `invalid_request`.
- `invalid-requirements.json`: negative duration; `invalid_requirements`.
- `unknown-consumer.json`: consumer not in the registry; `unknown_consumer`.
- `caller-not-owner.json`: attested `reserved:stranger` cannot use `phone.media`;
  `consumer_not_owned`.
- `caller-direct.json`: a direct caller has no declaring module ownership;
  `consumer_not_owned`.
- `flow-scope-not-supported.json`: even a Callosum principal cannot select an
  engine through a flow-scoped route; the dispatcher returns
  `flow_scope_not_supported` before consumer admission.
- `route-cooldown.json`: the model route has a future cooldown.
- `route-quota-exhausted.json`: the model route has a future quota horizon.
- `route-failure-backoff.json`: repeated failures have left a future route backoff.
- `credential-entitlement-fenced.json`: the sole key is fenced by
  `ENTITLEMENT_ERROR`.
- `credential-billing-fenced.json`: the sole key is fenced by `billing_exhausted`.
- `credential-refused.json`: the sole key is fenced by the media admission's
  `credential_refused` observation. All credential fences are fresh, not due for
  a recheck probe.
- `pool-billing-wall.json`: the sole pooled key has a billing wall.
- `pool-rate-limit-wall.json`: the sole pooled key has a rate-limit wall.
- `provider-reset-account-route-fence.json`: the sole key/model has an account
  route fence: routing has recorded that the provider cannot serve this model
  through this key until a future quota reset. Other keys would remain eligible.
- `listing-missing.json`: the media credential list has not arrived.
- `listing-grant-missing.json`: a credential exists, but the media category list
  grant is absent.
- `listing-stale.json`: the last credential listing is too old to admit media.
- `listing-failed.json`: a failed listing invalidates previously cached readiness.
- `no-admissible-media-credential.json`: a ready media listing contains only an
  LLM-category key, which cannot authorize a media engine.
- `store-error.json`: a missing measurements table induces an actual store read
  failure; `store_error`. Its diagnostic text is a captured synthetic SQLite
  failure, not a promise that every storage failure has the same message.

Missing grants, unavailable listings, and absent/blocked media credentials
remove remote candidates at `reachability`. They do **not** return
`route.account_for`'s `auth_required` or `account_selection_unavailable` codes.
`SHA256SUMS` covers every JSON sample, in bytewise filename order.

## Regeneration and verification

From the worktree root:

```sh
UPDATE_ROUTE_SELECT_ENGINE_VECTORS=1 cargo nextest run --locked -p prefrontal-routing-module --bin ck-prefrontal-routing -E 'test(=routing_v1::select_engine_vectors_tests::serving_bytes_match_tagged_vectors)'
```

For a remote CortexKit build, include every JSON file in this directory and
`SHA256SUMS` in `return_paths` to copy the generated files back to the worktree.
Without the update variable, the same test regenerates the samples in memory
and fails if any committed sample byte or checksum differs. It also rejects
extra JSON files not covered by the generator. Independently check the manifest:

```sh
(cd test-vectors/route-select-engine-v1 && shasum -a 256 -c SHA256SUMS)
```
