# Media `route.select` samples v1.2

This expansion is prepared for the Git release tag `route-select-media-v1.2`; all 21 previous pairs
are byte-identical. Production advertises `select_kind`
and `select_media`, using routing contract 0.7.28 and conformance-test crate
0.10.32. The regeneration test calls the real module frame dispatcher with
production's capabilities, without a test override.

Each JSON file contains the request body and the reply body exactly as captured,
without reserializing either body. The fixed clock is `1750000000000` ms; replay
inputs fix decision IDs. `SHA256SUMS` covers every JSON file, sorted by filename.
These are synthetic fixtures, not captures of private user requests or live keys.

## Fixture and cases

Colliculus declares `colliculus.voice_message` as `transcribe`, with Callosum
authorized to select for it, and `colliculus.speech` as `synthesize`. Both use
demand mode at level 40 initially. A synthetic, active media credential reaches
the remote engines, all declared by Colliculus under provider `media-vendor`.

The transcription and speech rate rows are copied from the checked-in pinned
Fusiform fixture (`openai/gpt-transcribe` and
`elevenlabs/eleven_flash_v2_5`). Only their fixture identities and operator grades
are synthetic: the priced engines have grade 80, and an unpriced transcription
engine has grade 100. A phone transcription row also has grade 80. It is not a
module engine declaration: only the caller-local sample offers it. The phone
inherits that catalog grade and has known-zero ranking cost, with no price.

| File | Selection context and expected reply |
| --- | --- |
| `voice-message-transcribe.json` | Callosum selects a 30-second voice message in demand mode: the priced, adequate grade-80 engine precedes the unpriced grade-100 engine. The reply quotes its per-second rate. |
| `speech.json` | Colliculus selects speech for 24 ASCII characters / 24 UTF-8 bytes. The reply quotes the per-character rate, using the current contract's explicit `input_characters` member. |
| `pinned.json` | The request pins the unpriced grade-100 engine after its model policy becomes `pin_only`. It is admitted without substitution or a `price`. |
| `ladder.json` | Settings switch voice messages to ladder mode: a missing entry and the pin-only model are skipped, then the priced engine wins. |
| `no-route.json` | The same ladder with `privacy: "local_only"` cannot use any engine. The reply is `no_route` with only the contract's reason in its detail. |
| `voice-message-caller-local.json` | Back in demand mode, Callosum offers its phone engine for the same 30-second voice message. Its catalog grade equals the remote engine's; known-zero cost wins, and the reply has `caller_local: true`, `index: 0`, and no module or price. |
| `voice-message-caller-local-en.json` | The phone's `apple/phone-transcriber` offers English (`en-US`), WAV, realtime transcription with a 600000-ms duration limit. With a 30-second English request, its grade 80 and known-zero cost win; the reply has `caller_local: true`, `index: 0`, and no price. |
| `voice-message-caller-local-fr.json` | The same phone offer cannot meet the request's `fr-FR` language. In this pair's isolated fixture, the priced `media-vendor/fixture-transcribe` module engine declares `fr` and wins instead. |

## Added refusal and redemption pairs

Each selection request below uses `route.select` with `kind`, not the older
`route.select_engine` operation. Caller identity and flow metadata come from the
bound route; they are not extra request members.

| File | Cause and expected reply |
| --- | --- |
| `unknown-consumer.json` | Callosum names an undeclared consumer: `unknown_consumer`. |
| `caller-not-owner.json` | `reserved:stranger` is neither the owner nor an authorized selector: `consumer_not_owned`. |
| `caller-direct.json` | A direct caller is neither the owner nor an authorized selector: `consumer_not_owned`. |
| `invalid-request.json` | An unsupported top-level `effect_id`: `invalid_request`, naming that field. |
| `kind-mismatch.json` | `synthesize` conflicts with the voice consumer's declared `transcribe` kind: `invalid_request`, naming `kind`. |
| `invalid-requirements.json` | Negative `duration_ms`: `invalid_requirements`, naming that field. |
| `flow-scope-not-supported.json` | Even the authorized selector Callosum cannot select on a flow-stamped route: `flow_scope_not_supported`. |
| `store-error.json` | Removing the actual `routing_engine_eval` table induces `store_error` through the real selection handler. |
| `reachability-unavailable.json` | All otherwise suitable remote routes are unavailable: `no_route`, with only `reason: no_available_model` in `detail`. The grouped causes below produce identical complete pairs. |
| `requirements-unmet.json` | No declared engine advertises the requested French language (`fr`): `no_route` / `no_available_model`. |
| `no-engines.json` | The consumer remains declared but no module engines are declared: `no_route` / `no_available_model`. |
| `missing-capability.json` | A video request requires a 1000-ms output duration, but the catalog has no `limit.output_duration_ms` fact: `no_route` / `missing_capability`, with the failed fact named in the message. |
| `decision-admitted.json` | Colliculus, the selected provider executor, redeems the decision from `voice-message-transcribe.json` before expiry. Admission is `reserved` and keeps the same `media-vendor` / `fixture-transcribe` binding. |
| `decision-caller-mismatch.json` | A new admission of that same decision from `reserved:stranger` fails the account caller check: `account_not_authorized`. The stranger has no runner declaration or authorized session scope; this is distinct from an authorized executor naming an unknown decision. |
| `decision-expired.json` | Colliculus requests a new admission 600001 ms after selection, past the ten-minute decision life: `unknown_decision`. It is an account redemption refusal, not a select-time expiry code. |

### Shared reachability shape

The regeneration test independently seeds each of these causes, invokes the real
handler, and asserts that the entire request/reply pair equals
`reachability-unavailable.json`, byte for byte:

- route cooldown;
- route quota exhaustion;
- route failure backoff;
- credential entitlement fence (`ENTITLEMENT_ERROR`);
- credential billing fence (`billing_exhausted`);
- credential refusal (`credential_refused`);
- pooled-key billing wall;
- pooled-key rate-limit wall;
- account route fence / provider-reset wall;
- vault listing missing, stale, failed, or without the `media-provider` list grant;
- a loaded listing containing only a non-media (LLM-only) credential.

Both transcription routes are fenced when a cause is route-specific, so another
declared engine cannot mask the refusal. These causes do **not** expose the older
engine selector's `detail.filter` or a retry timestamp. The existing `no-route.json`
also covers local-only privacy with no eligible local engine. Media selection's
`no_route` reasons are `no_available_model` and `missing_capability`; both are
represented.

Samples are read alongside the fixture and policy setup in
`crates/striatum-module/src/routing_v1/media_samples_tests.rs`; policy is not an
extra request member. Replies are validated with the pinned media contract and
round-tripped through its answer type to reject extra wire members.

## Regeneration and verification

Run from the repository root with the repository's Rust toolchain and nextest:

```sh
UPDATE_ROUTE_SELECT_MEDIA_VECTORS=1 cargo nextest run --locked -p striatum-module --bin ck-striatum -E 'test(=routing_v1::engines_tests::media::samples::serving_bytes_match_media_vectors)'
cargo nextest run --locked -p striatum-module --bin ck-striatum -E 'test(=routing_v1::engines_tests::media::samples::serving_bytes_match_media_vectors)'
(cd test-vectors/route-select-media-v1 && shasum -a 256 -c SHA256SUMS)
```

The ordinary test regenerates all 23 pairs in memory and compares every byte,
the checksum manifest and exact directory membership. The reachability pair is
additionally regenerated under all 14 causes listed above. Only the explicit
update command writes samples. Existing engine vectors and replay goldens are
untouched.
