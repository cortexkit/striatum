# Imported consumer fixtures

These byte-for-byte contract snapshots keep routing tests independent of the
former workspace. Source: prefrontal commit
`3de63a65ddde07b09128808f24d0f8a54da60072`. The root `test-vectors/route-*`
folders are routing-produced samples; these are frozen consumer inputs.

| Source path | SHA-256 |
| --- | --- |
| `test-vectors/select-panel-wire.json` | `9d4e7e7b2ed4df9a762894e1adbe784eba322509ee6163edac3b9c9c3b971326` |
| `test-vectors/broca-service-tiers-v1/role-describe-live.json` | `dd0ef1d533f267ea3422e508c7e16f7ece51239fcc93580924572c610bf8e581` |
| `test-vectors/broca-turn-system-v1/before/role-describe.response.json` | `8eaaed6189f1b126f6896e5de87471b9a03291f1b56dea50ab7ba6273d6b0572` |
| `test-vectors/broca-turn-system-v1/after/role-describe.response.json` | `e9f31acc936fd1dc3e5d81240baaa2b62e9d028beed05b2d60215394592676f1` |
| `test-vectors/broca-contract-0748-v1/role-describe/role_describe_reader.json` | `a5b25b2eb1323f66af9961913ecfafcb547740db2fe19cd5df98cf602a8d92dd` |
| `test-vectors/broca-contract-0748-v1/role-describe/role_describe.json` | `696198c703d4d2e613f90bd7c3b1415e7fb7892a11ed202fe08796e171ad46bb` |
| `test-vectors/broca-contract-0748-v1/role-describe/role_describe.head_additions.json` | `ea87d4ab7cdf518af3d9bc225364345b449fcec4a7980c3384e2b65df5c9fef6` |
| `test-vectors/broca-head-selection-v2/before/role.describe.json` | `a60bf6a845d06480de6c01bf2555e9b51972e1e43f5547873268df7669ee3d14` |
| `test-vectors/broca-head-selection-v2/before-routed/role.describe.json` | `8488a915fe0bb8a8ac828d9969a475728e7a8554ddee704802b600e698789f86` |
| `test-vectors/broca-head-selection-v2/after/role.describe.json` | `d6f2aaeb1a8b7421a11170f5fda7cd186d602e7fdc28bdf572cab3355984a52a` |
| `test-vectors/diagnostics-findings-v1/request-empty.json` | `85e09268d9303b5e1863c341c3577ea77ecbea4d46eaafc7419c513109b41680` |
| `test-vectors/diagnostics-findings-v1/request-window.json` | `01e32754c10783ceb1f1b9f2a61a0376888e772106de75b139d863bc55130042` |
| `test-vectors/diagnostics-findings-v1/request-max-window.json` | `f5b7c7eadac60e9eb0d41a9235ac95d53c21e095a384cec521a653c5cbb8f43e` |
| `test-vectors/diagnostics-findings-v1/reply-routing-findings.json` | `3556a3361cdf1b7cbba7cb30516a9e15452fa62a1329822b5630b4604037d33d` |
| `test-vectors/diagnostics-findings-v1/reply-routing-unscored-admissible-model.json` | `2004f223e3b8393ce9a74cebbeab9bc67118f8416b7368287458c3ebd88b7771` |
| `crates/prefrontal-core-module/fixtures/routing-wire/catalog-seed.json` | `248b2303db08ab319b6d3c25dc1f31fef8360e69dab367be0eb1cb3a3cb38284` |
| `crates/prefrontal-core-module/fixtures/routing-wire/states.json` | `7764f333ede371a21628f8b241d8ad21eb133c4ed90e654d9f8d3b60281018ef` |
| `crates/prefrontal-core-module/fixtures/routing-wire/panels.json` | `460ffaf99610412ea664e2a599c152cad5d63e024b7e500484f3b098b36b8540` |
| `test-vectors/broca-request-invalid-v1/run_status.request_invalid_openai.json` | `cb03a7ce9d9fa35ff201b6efc4ce3824ab1bdd18da3594bd5c130c8878a86f25` |

## Captured daemon catalog

`2026-11-10-daemon-catalog.json` is trimmed to only colliculus's complete module
entry, copied verbatim from the `ck --json catalog` capture supplied on
10 November 2026, around 13:06 UTC. The original source artifact is
`.cortexkit/internal-docs/research/2026-11-10-daemon-catalog.json`, with SHA-256
`2e15ab501a348b95e60d827394cd3b52fcc9ecb1609fc2e085af3c26917b36e9`.
The entry is wrapped in a full `catalog.list` reply envelope with fixture
generation 1 and an empty daemon-operation list (`subc_ops`). The engine
regression decodes that reply through the pinned catalog client's actual reply
type before replacing routing's catalog and selecting both colliculus
transcription engines. No other module entries are retained.

## Colliculus realtime dictation and voice-front declarations

`colliculus-5b/` preserves the supplied declaration snapshots byte for byte from
Colliculus commit `5ccd00f69eb93d7515b4e3b45938ca751bf00098`. The source directory was
`CortexKit-scratch/shared-striatum-readers/colliculus-5b/`. Its `PROVENANCE.txt`
records generation by `service::sets::tests::write_declaration_goldens`, using the
real handler and manifest with priced, keyed model rows,
including `gpt-realtime-2.1`.

Before import, the supplied `SHA256SUMS` was verified against the independently
provided digest below, then every artifact was checked against that manifest.
The manifest and provenance are retained alongside the JSON inputs.

| Imported path | SHA-256 |
| --- | --- |
| `colliculus-5b/routing-v1-declaration.json` | `be5266ca59b114e19dc9bebead17d816f7a30268c7bdce67b55b10068ac1a508` |
| `colliculus-5b/media-role-describe.json` | `4f9856a6c68ac918e645b2cef2419b17625469b61cfbc64b11723a6991d5e7cd` |
| `colliculus-5b/PROVENANCE.txt` | `34332247a2606f54598ae67a89929f990af41a79fdcaf0b84efa42fc55457b0c` |
| `colliculus-5b/SHA256SUMS` | `4c74fa6cff5438da8e0b98a87e80a1365e778aa2063d098d647a9d24ebcd574a` |

## Thalamus parent-linked phone-call forks

`thalamus-route-fork-of-v1/` copies the supplied relay request and hold-envelope
samples byte for byte from Thalamus commit
`07093f5525ca78b2341cf65dbc0e1c7268dfd328`. The source directory is
`.cortexkit/internal-docs/investigations/thalamus-route-fork-of-v1/`; its README
records capture by
`routing::tests::fork_of::fork_of_uses_the_newest_parent_and_inherits_its_head_turn`.
The data is synthetic. The request is the exact compact params body, not a
method envelope; the hold sample wraps the error body in `error`.

Before import, the manifest's own SHA-256 was checked against the prefix
`73031db3` that THALAMUS quoted in its handoff message, and then both files
were verified against the manifest.
The reader tests send the request bytes unchanged through routing's v1 handler.

| Imported path | SHA-256 |
| --- | --- |
| `thalamus-route-fork-of-v1/request.json` | `a96e194685b92151f7ceceed5e0dafc580ed3b0f5836b43d6d14fe5299151a36` |
| `thalamus-route-fork-of-v1/hold-answer.json` | `94b1c12960b67c9841db267888af00a66fe847bf9a2911ea0c85c7ff395c54f8` |
| `thalamus-route-fork-of-v1/SHA256SUMS` | `73031db341c99611a1a92e71b8fea6fdec1bf7ef72fdbcbe9d74b5b731ffe4ac` |

## Thalamus capacity wait

THALAMUS (Agent Runner) supplied the `route.account_for` bodies in the shared
scratch folder `CortexKit-scratch/shared-routing-capacity-v1/`.
`thalamus-capacity-wait-v1/` copies the bodies byte for byte, dropping the
`route.account_for.` prefix from the filenames. The source `SHA256SUMS` has SHA-256
`730b2a806ef90d1c03c7ca1a64bb9a73a4dc75fe5255bbcea06a4cb3392ab6d6`.
The OpenCode 2.3.2 run recorded a capacity refusal and the same run re-asking
after the reset with a fresh admission ID.

| Imported path | SHA-256 |
| --- | --- |
| `thalamus-capacity-wait-v1/request.json` | `0ea3b76afb2aecfe170c73fc4c90418ef89aabbc69107d02bb4e4b26c23647da` |
| `thalamus-capacity-wait-v1/re-ask.json` | `ebb7e14ab2bb4c0b4e48cca501ced65628f51164010f1a038cd89eb0227d796c` |

## Per-kind and caller-local media grades

`quality-grades-prefix-v1/` copies Fusiform's real-serializer samples byte for
byte, supplied by its owner from Fusiform commit `a4e36712`, source path
`crates/fusiform-module/fixtures/quality-grades-prefix-v1/`. The older-prefix
refusal is the owner's additional live older-Fusiform capture. The supplied
research copy was `.cortexkit/internal-docs/research/quality-grades-prefix-v1/`.
Its updated `SHA256SUMS` covers all four JSON files and has SHA-256
`8831211c65f0bfabe3e6d9fb6642659f07602108481b2a8e64325801b9c58fc8`.
The byte-pinned decoder tests retain the grade spelling `72.50` in the fixture.

`colliculus-voice-message-demand/routing-v1-declaration.json` is the supplied
output of Colliculus's own routing/v1 declaration serializer, copied unchanged
from `.cortexkit/internal-docs/research/colliculus-voice-message-demand/`, together
with its `SHA256SUMS`. Its SHA-256 is
`2fa96a8a79cdfa1213d8560e2df73bdcb200cd672fb47ee29029972feff466ba`.
The declaration sets `colliculus.voice_message` to IQ 75 without a mode member;
the approved-values ranking test loads these bytes through the real reader.

The earlier `served-payloads.json` capture has one explicit consumer fixture
port: `catalog.get — operator transcription grade` renames `quality.grade` to
`quality.grade.transcription`, preserving `87.500` and every other byte. The new
digest is `1ad59d65a4d7306f9462fc5f849c97bdc54cb58d902450f988787d56aa2bf4d1`.
`catalog_fixture_port_changes_only_the_media_grade_key` reverses that rename and
checks the original capture's digest, proving that no other fields changed.

## Broca route-fact observations

`broca-route-fact-1119/` contains byte-for-byte copies of the publications from
Broca's own publisher for `admission_started_body` and `run_finished_body` on
account `box_test`. They were supplied in
`.cortexkit/internal-docs/research/broca-route-fact-1119/`. Before import, the
manifest's SHA-256 was checked against the digest prefix `fe0c7bee24ea1441`
provided with the captures, then the original files were verified against the
manifest.

| Imported path | SHA-256 |
| --- | --- |
| `broca-route-fact-1119/admission-started.json` | `822af034833b1a778faba2874f88fe9a3b06e2ef3e9f7b703cc1627bd1cbba3b` |
| `broca-route-fact-1119/run-finished.json` | `6a610abf8ceea85ad3c953a7ea9f41fb844248a1f9cd7184f301867bbeeb9a87` |

## Thalamus route-fact observations

`thalamus-bus-route-fact-v1/` and `thalamus-bus-route-fact-v2/` copy the supplied
Thalamus bus observation bodies byte for byte from the matching directories
under `.cortexkit/internal-docs/research/`. Each body was verified against its
source `SHA256SUMS`; the tests use those body digests as the transport headers.

| Imported path | SHA-256 |
| --- | --- |
| `thalamus-bus-route-fact-v1/typed-success.json` | `bdb59cf0af248f2f669554ae1741e06bfd8f9b25db5c6d4ee8830161cc46b63e` |
| `thalamus-bus-route-fact-v2/mixed-headers.json` | `389e3b7dde17afc7995b98729ca2ac01d2a833b21f245faca63cd76bea0c4009` |
| `thalamus-bus-route-fact-v2/all-headers-dropped.json` | `880027a45706c78c2c9fafdab55c19c90e9672b93b304f0d5f96aa9a8a995ecb` |

## SUBC project settings views

`subc-project-views-v1/` preserves the byte-for-byte capture from a real
`ck-subc` built from commit `f55cc24e5062e46cb790dabcaf7430b82bbc4b93` on Linux.
The rig used the daemon's `crates/subc-client-rs/tests/real_daemon/project_config.rs`
helpers and an invented `example-router` module in scratch state. The capture
README identifies the exchange for each frame, and `SHA256SUMS` was verified
before import. The tests use the frames without rewriting their module or project
IDs.

## Claustrum scoped credential listing

`claustrum-list-scoped-v2/list-scoped-v2.json` copies the vault's
`credential.list_scoped` vectors byte for byte from Claustrum commit
`d85f4a01` (`list-scoped-vectors-v2.3`), as supplied under
`.cortexkit/internal-docs/contract-ref/`.
The `learned_org` case includes a top-level `org_id` on an Anthropic API key
with no account identity. SHA-256:
`4ff7e45c2839f71873c1ca12ffe5ea3e15d35897e3610bb884a26a6f0329b691`.
