# route.select_engine v2

These request/reply pairs are recorded through routing's real frame dispatcher,
not assembled as expected replies. The `request` and `reply` members preserve the
exact compact JSON body bytes, including object-member order; the enclosing pair
has no trailing newline. They are bodies, not transport frames or route bindings.

## Caller and fixture

The phone selects through the transport-attested `reserved:callosum` principal
on a current, unscoped route binding. The nonce-verified voice-module catalog entry
declares its `routing/v1` consumer exactly as follows:

```json
{"name":"colliculus.voice_message","kind":"transcribe","selectors":["callosum"]}
```

There is no `iq` member. The voice module owns the consumer; the phone operator is admitted as a
listed selector, not as its owner. The phone operator does not declare `phone.media` in this
fixture. Caller identity is transport metadata, not a request member. The foreign,
direct, and flow-scoped samples intentionally use different route bindings.

The voice module also declares fixture provider engines in its `media-provider/v1`
slot. These synthetic engines use provider `media-vendor` and models named
`fixture-<kind>`. An active vault key `opaque-media-key` in category
`media-provider`, plus a category `list` grant, supplies media account readiness.
No real credential or network service is used. Unavailability cases start from
fresh state. The catalog snapshot is version 41; the clock is fixed at
`1750000000000` ms, IDs follow the store's deterministic test sequence, and engine
decisions expire ten minutes later, at `1750000600000` ms.

No sample identifies routing's module address. The `engine.module` in a remote
pick identifies the executor (`colliculus`), not routing. Module addressing and
transport failures are outside these request/reply bodies.

## Phone and voice-module picks

- `accepted-transcribe.json`: the provider transcription engine wins, with
  `caller_local: false`, `local: false`, and `module: "colliculus"`.
- `accepted-caller-local.json`: the phone's transcription engine wins under
  `local_only`, with `caller_local: true` and `index: 0`, and no remote `module`.
- `repeated-transcribe.json`: the same request is sent twice in one fixture;
  the engine is unchanged but the decision ID is new. `route.select_engine`
  has no idempotent replay key. `invalid-request.json` shows that an `effect_id`
  is not accepted.
- `accepted-transient-only-account.json`: a transient account backoff does not
  exclude the only account; it remains a last resort.

## Expired decision use

The pinned `cortexkit-role-routing` 0.7.22 contract (tag `cortexkit-role-routing-v0.7.22`),
§13 (`route.select_engine`), specifies that a remote engine's executor redeems
its decision through `route.account_for`. After slot authorization, a **new**
admission at or after `expires_at_ms` receives `unknown_decision`. A replay of an
admission recorded before expiry returns its original answer even after expiry.
§14 (`route.engine_report`) reports an engine outcome; it is not the credential
admission operation on this path.

- `decision-expired.json`: a real `route.account_for` request from attested
  `reserved:colliculus`, the selected provider executor, at exactly the engine
  decision's expiry boundary. It uses the decision ID in
  `accepted-transcribe.json`, the same provider/model, and a new admission ID.
  The handler returns `unknown_decision`, not an invented `decision_expired`
  select error. The generator first admits a different admission ID before
  expiry and confirms its replay after expiry, so the refusal cannot be
  attributed to a wrong executor or an unauthorized provider/model slot.

The fixed engine-clock offset used to reach expiry is a test input only. The
pre-expiry admission and its replay are controls, not additional sample pairs.

## Refusals and unavailability

Selection errors have no `retry_at_ms`. Several causes share `no_engine`;
`detail.filter` identifies the first filter that emptied the candidates, in
order: `kind`, `requirements`, `privacy`, `reachability`.

- `no-engines.json`: no engine declarations; `no_engine`, filter `kind`.
- `kind-mismatch.json`: `future_kind` conflicts with the consumer's declared
  `transcribe` kind; `invalid_request` with `detail.field: "kind"`, before engine
  filtering. An open wire kind enum does not override a consumer's declared kind.
- `requirements-unmet.json`: no engine declares French; filter `requirements`.
- `privacy-local-only.json`: only remote engines are available; filter `privacy`.
- `invalid-request.json`: unsupported `effect_id`; `invalid_request`.
- `invalid-requirements.json`: negative duration; `invalid_requirements`.
- `unknown-consumer.json`: absent registry entry; `unknown_consumer`.
- `caller-not-owner.json`: attested `reserved:stranger` is neither the voice module
  nor its listed phone selector; `consumer_not_owned`.
- `caller-direct.json`: a direct caller is neither owner nor listed selector;
  `consumer_not_owned`.
- `flow-scope-not-supported.json`: even the phone operator cannot select through a
  flow-scoped route; the dispatcher refuses with `flow_scope_not_supported`
  before consumer admission.
- `route-cooldown.json`: a future route cooldown.
- `route-quota-exhausted.json`: a future route quota horizon.
- `route-failure-backoff.json`: a future route failure backoff.
- `credential-entitlement-fenced.json`: the key has an `ENTITLEMENT_ERROR` fence.
- `credential-billing-fenced.json`: the key has a `billing_exhausted` fence.
- `credential-refused.json`: the key has a `credential_refused` fence. All three
  fences are fresh, not due for recheck probes.
- `pool-billing-wall.json`: the sole pooled key has a billing wall.
- `pool-rate-limit-wall.json`: the sole pooled key has a rate-limit wall.
- `provider-reset-account-route-fence.json`: a future quota reset fences the
  key/model; other keys would remain eligible.
- `listing-missing.json`: no credential listing has arrived.
- `listing-grant-missing.json`: a credential exists but its category list grant
  is absent.
- `listing-stale.json`: the listing is too old to admit media.
- `listing-failed.json`: a failed listing invalidates cached readiness.
- `no-admissible-media-credential.json`: the listing contains only an
  LLM-category key, which cannot authorize media.
- `store-error.json`: dropping the measurements table causes a real store read
  failure; `store_error`. The captured synthetic SQLite message is not a promise
  that every storage failure has the same diagnostic.

All route, credential, pool, account-route, and listing unavailability cases
remove remote engines at `reachability`. They do not return `route.account_for`'s
`auth_required` or `account_selection_unavailable` selection codes.

## Migration from v1

There are **30 pairs**: 28 retain their v1 filenames and scenarios under the new
consumer, and two new pairs are `decision-expired.json` and `kind-mismatch.json`.
All 30 filenames are listed above. V1 contains 34 JSON pairs and remains
byte-identical for pinned readers.
These six v1 pairs have no v2 counterpart with the same scenario:

- `accepted-synthesize.json`
- `accepted-image.json`
- `accepted-video.json`
- `accepted-stream_transcribe.json`
- `accepted-realtime.json`

Those five successful selections are not applicable to a consumer declared with
kind `transcribe`. The real handler rejects a different requested kind before
selecting any engine; the fixture does not remove the declaration to manufacture
cross-kind successes.

- `unknown-kind.json`: the old `no_engine`/`kind` refusal depended on an unbound
  consumer kind. Under the new declaration, `future_kind` instead conflicts with
  `transcribe`. `kind-mismatch.json` records that different, real refusal rather
  than preserving an impossible `no_engine` outcome.

## Regeneration and verification

From the worktree root:

```sh
UPDATE_ROUTE_SELECT_ENGINE_V2_VECTORS=1 cargo nextest run --locked -p striatum-module --bin ck-striatum -E 'test(=routing_v1::select_engine_vectors_tests::serving_bytes_match_v2_vectors)'
```

When recording on a remote build server with read-only synced sources, use a
writable checkout and copy every generated JSON path and `SHA256SUMS` back into
this directory.
The v2 update variable does not update v1.
Without it, the same test regenerates replies in memory and compares every byte
and the exact checksum manifest. It rejects extra or missing directory entries,
including non-JSON files. Only the generated pairs, `README.md`, and `SHA256SUMS`
are allowed. `SHA256SUMS` covers every JSON pair in bytewise filename order.

```sh
(cd test-vectors/route-select-engine-v2 && shasum -a 256 -c SHA256SUMS)
```
