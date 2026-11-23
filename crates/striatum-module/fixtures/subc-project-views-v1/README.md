# Project settings wire captures

Daemon: `ck-subc` built on the Linux runner from `f55cc24e5062e46cb790dabcaf7430b82bbc4b93`, with the same test and desktop-approval features as the project-config integration rig. No daemon source was modified.

The throwaway integration test reused `crates/subc-client-rs/tests/real_daemon/project_config.rs` for daemon launch, the fake `projects.enumerate` registry, and scripted desktop approval. A small raw-frame fixture module (`example-router`) used the real protocol and captured the body bytes at `subc_transport::read_frame` / `write_frame`. Each daemon child was given separate scratch `XDG_DATA_HOME`, `XDG_RUNTIME_DIR`, `XDG_CONFIG_HOME`, `HOME`, connection file and approval store. No live daemon was contacted. The IDs and paths in the samples belong to the rig.

| File | Capture |
| --- | --- |
| `config-declare.json` | Module → daemon REQUEST after HELLO_ACK; user/project schema for the live integer setting `/count`, with user file `{"count":7}`. |
| `config-get-project-reply.unstamped.json` | Daemon → module RESPONSE to `config.get` for `sample-project` with project file `{"count":11}`. `active` is `unstamped`, generation 0; there is no attempt. |
| `config-get-unknown-project.json` | Daemon → module ERROR to `config.get` for `unknown-sample-project`, absent from the fake registry. |
| `config-changed-project.unstamped.json` | Daemon → module PUSH after changing the fetched project's file to `{"count":12}`. |
| `config-review-project.stamped.json` | Daemon → module REQUEST and matching module → daemon RESPONSE during the plan for the new project file `{"count":13}`. This file is a JSON wrapper with `request` and `reply` members; the original body bytes are embedded as-is. |
| `config-changed-project.stamped.json` | Daemon → module PUSH after approving the project plan. The rig removed the project file before approving the initial `config.stamp`, then created the new file after the user generation became active. |
| `config-get-project-reply.stamped.json` | Daemon → module RESPONSE to a project `config.get` prompted by the stamped push. It carries the user-only fallback in `active` and the project view in `pending` with generation 2 and attempt 2. |
| `config-applied-project.stamped.json` | Module → daemon REQUEST acknowledging that pending generation and attempt for the same project. The test checked that the daemon accepted the ACK and persisted the project tuple as active. |

Except for the review wrapper, each JSON file is exactly one frame body, with no frame header or added newline. `SHA256SUMS` covers the JSON files and this README.
