# Transcription bias `route.select` samples v1.5

Prepared for review before the `route-select-media-v1.5` Git tag is created.
These are synthetic, deterministic fixtures captured through the real module frame
dispatcher, not live credentials, private audio, or measured recognizer results.
Production code is unchanged. The `cortexkit-role-routing` contract crate is pinned
to 0.7.36.

Each JSON file wraps the original request and reply body bytes as
`{"request":...,"reply":...}`, without reserializing either body. The clock is
`1750000000000` ms and replay inputs fix decision IDs. `SHA256SUMS` covers all
three JSON files in filename order; the test also checks exact directory membership.

## Inputs and outcomes

All three requests derive from `voice-message-caller-local-en.json` in
`test-vectors/route-select-media-v1/`, byte-identical to the version at the
`route-select-media-v1.2` tag (commit `393e629b1b3b36f17ea5316e6bee8fc535b2e741`).
Callosum selects for `colliculus.voice_message`, kind `transcribe`, with a 30-second
English (`en-US`) WAV message and `remote_allowed` privacy. Its caller-local
`apple/phone-transcriber` advertises English, WAV, realtime latency, and a
600000-ms duration limit.

The added phone `bias_limit: {"unit":"phrases","max":100}` is **illustrative**,
not a statement of Apple's recognizer capacity. The selection request advertises
only `requirements.bias: true`; the actual word list is not sent to routing.
Routing does not validate word-list length against this limit.

| Case / file | Bias requirement | Phone advertises bias | Winning engine / reply |
| --- | --- | --- | --- |
| (a) `voice-message-bias-caller-local.json` | `true` | Illustrative phrases/100 | `apple/phone-transcriber`: `caller_local: true`, `index: 0`, no module or price. |
| (b) `voice-message-bias-remote.json` | `true` | No `bias_limit` | `colliculus`'s `openai/gpt-transcribe`: `caller_local: false`, with a metered `input_audio.per_second` rate of `0.000075` USD. The phone fails eligibility. |
| (c) `voice-message-no-bias-caller-local.json` | Absent | Illustrative phrases/100 | `apple/phone-transcriber`, identical engine/price shape to (a). |

Only the requested bias members differ between these request bodies. The replies
have distinct deterministic decision IDs because the three requests run in order.

## Declaration and ranking fixture

The test uses the unchanged real Colliculus declarations already loaded by the
media tests from `crates/striatum-module/fixtures/colliculus-5b/` (Colliculus commit
`5ccd00f69eb93d7515b4e3b45938ca751bf00098`). All ten engines and six consumers
are retained. Its transcription declarations are `openai/gpt-transcribe`
(`bias_limit` phrase/50) and `google/gemini-3.8-flash` (phrase/100). The latter is
not the separately measured `google/gemini-4-hydrogen` model. See that fixture's
provenance and checksum manifest for the imported declaration bytes.

A synthetic active media credential reaches both remote providers. The voice
consumer uses demand mode, level 40. The catalog has synthetic operator grades:
phone 80, OpenAI 80, Google 100. OpenAI's real pinned Fusiform rate row is copied
from `crates/striatum-module/fixtures/served-payloads.json`; Google's fixture row
has no price, preserving the older samples' priced/adequate versus
unpriced/higher-grade comparison. These catalog grades are not accuracy percentages.
The phone has known-zero ranking cost and no price: it wins (a)/(c) in the
adequate-grade tier. In (b), priced OpenAI precedes unpriced Google after the
phone is filtered. This is not an unconditional preference for caller-local;
a phone failing demand or another requirement does not win just by being free.

## Regeneration and verification

Run from the repository root on the Linux build server with four vCPUs (tool
setting `runon: "linux,4c"`):

```sh
UPDATE_TRANSCRIPTION_BIAS_VECTORS=1 cargo nextest run --locked -p striatum-module --bin ck-striatum -E 'test(=routing_v1::engines_tests::media::samples::serving_bytes_match_transcription_bias_vectors)'
cargo nextest run --locked -p striatum-module --bin ck-striatum -E 'test(=routing_v1::engines_tests::media::samples::serving_bytes_match_transcription_bias_vectors)'
(cd test-vectors/route-select-media-v1.5 && sha256sum -c SHA256SUMS)
```

Only the explicit update environment variable writes the new set. The ordinary
test recaptures the three pairs, validates answers against the pinned media
contract, asserts the winners and location/price shapes, and checks every byte
and checksum. Existing media vectors and replay goldens remain unchanged.
