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
| `test-vectors/project-resolve-v1/enumerate.json` | `89afedb88fa4ddec219f4932df22f26ca8395022c5c1dd399e3e50c9d50b94dd` |
| `test-vectors/project-resolve-v1/resolve-project-id.json` | `4af3d3d6dd6b279146da2e50dd48dd507674f2806f6ac35e8f04b380bc5e901e` |
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
