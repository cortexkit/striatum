# Media `route.select` samples

These samples have no Git release tag yet. Production advertises `select_kind`
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

The ordinary test regenerates all six pairs in memory and compares every byte,
the checksum manifest and exact directory membership. Only the explicit update
command writes samples. Existing engine vectors and replay goldens are untouched.
