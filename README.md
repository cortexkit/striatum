# Striatum (AI Model Routing)

> [!WARNING]
> **Alpha.** Striatum is in early development and is not yet released for general use.
> Breaking changes will happen between versions, including changes to its settings,
> stored data and interfaces. Pin an exact version if you depend on it.

Striatum is CortexKit's AI model routing module. For each model call, it chooses the
provider/model route and the account that should serve it, using the model
catalog, configured policy, account availability, quota evidence, price and
observed outcomes. Consumers ask for a route and act on the answer rather than
duplicating routing policy.

The module registers as `striatum`, serves the `routing/v1` role and its
`route.*` operations, and runs as the `ck-striatum` binary under the `ck`
daemon. Its crates are `striatum-core`, `striatum-store` and `striatum-module`.
Decisions and routing state are stored in
`$XDG_DATA_HOME/cortexkit/striatum/store.db` (by default,
`~/.local/share/cortexkit/striatum/store.db`).

## Runtime requirements

A normal CortexKit installation uses `ck setup` to install and supervise the
daemon and modules. Routing has these companion-module dependencies:

- **Required for normal routing:** Fusiform supplies the model catalog and
  Claustrum (the vault) supplies credential metadata for available logins.
  Routing never reads login secrets. Catalog and credential-dependent
  operations need these inputs; a slow companion does not have to block module
  startup.
- **Optional:** Insula supplies quota readings, Astrocyte supplies measured
  subscription-plan values, and the CortexKit bus delivers asynchronous quota
  notices. Routing retains its configured fallbacks when these optional inputs
  are absent.

## Settings

The daemon manages settings and supplies workspace and project views to
routing. The optional user file is
`$XDG_CONFIG_HOME/cortexkit/striatum.jsonc` (by default,
`~/.config/cortexkit/striatum.jsonc`). Run `ck-striatum --check-config` to
validate and print the user file's path. Routing reads only `striatum.jsonc`;
it does not fall back to older filenames.

Workspace and project layers use `.cortexkit/striatum.jsonc` under their
respective roots. The daemon validates and merges defaults, user, workspace and
project settings. Routing selects the project view using the project identity
attested on the caller's daemon route; a request body cannot switch projects.
Only consumer runner placement and service-tier settings may vary by project.

For example, a project can choose an ordered runner list for a consumer and its
dotted descendants:

```jsonc
{
  "consumers": {
    "example.consumer": {
      "runners": [{"module": "example-runner"}]
    }
  }
}
```

Replace the example names with a registered consumer and a runner enrolled in
the user settings or auto-enrolled as first-party. An empty list allows no
runner. See [the routing specification](docs/specs/model-routing.md#runner-placement)
for validation and precedence.

Tracing warnings and errors go to stderr. The default threshold is `warn`.
`STRIATUM_LOG` overrides `RUST_LOG`; both accept global levels and
comma-separated `target=level` directives, such as
`warn,striatum_store=debug,ck_striatum=info`.

## Build and test

A clean checkout needs the Rust toolchain pinned in `rust-toolchain.toml`
(Rust 1.100.0, with rustfmt and Clippy), Python 3.10 or newer, and
[cargo-nextest](https://nexte.st/). Install the toolchain and nextest with your
usual Rust tooling, then run:

```sh
cargo build --locked --release -p striatum-module
scripts/rust-test-gate.sh
```

The gate runs the Python and Rust source checks before the complete standalone
test suite. It does not require sibling checkouts or a running daemon. Use
`cargo nextest run --locked` with a package and test filter for focused
development; for example:

```sh
cargo nextest run --locked -p striatum-module --bin ck-striatum \
  -E 'test(/route_fence_pool_tests::/)'
```

### Optional sibling end-to-end tests

Six real-daemon end-to-end tests are ignored by default. They are optional and
are not part of a normal clean-checkout build. They expect Subconscious and
Insula source checkouts plus the shared support checkout in the sibling
locations `../subconscious`, `../insula` and `../commons`.
`AI_PROVIDER_QUOTA_REPO` can override the Insula checkout location. On a machine
with those optional dependencies, run:

```sh
cargo nextest run --locked -p striatum-module --test it --run-ignored only \
  -E 'test(/^(both_modules_e2e|real_daemon_e2e)::/)'
```

## More documentation

See [ARCHITECTURE.md](ARCHITECTURE.md), [STRUCTURE.md](STRUCTURE.md), the
[routing specification](docs/specs/model-routing.md), and the
[`route.what_if` operator query](docs/route-what-if.md). Scripts under
`script/` include optional operator tooling; they are not needed to build or
test the module.

## Releases

[GitHub Releases](https://github.com/cortexkit/striatum/releases) will provide
`ck-striatum-<target>.zip` archives for `darwin-arm64`, `linux-x64`,
`linux-arm64`, `windows-x64` and `windows-arm64`. Each archive contains the `ck-striatum` binary
(`ck-striatum.exe` on Windows); the macOS binaries are signed with Developer ID
and notarized. Each release also includes a `release-manifest.json` with the
build's Git revision and the minimum supported core version. A release is only
published from a commit whose test suite passed natively on all five targets.

Download the ZIP and its `.zip.sha256` sidecar to the same directory, then verify
the ZIP before extracting it. For example:

```sh
shasum -a 256 -c ck-striatum-darwin-arm64.zip.sha256
```

## License

Striatum is available under the [MIT License](LICENSE).
