# Striatum

Model and account routing for the CortexKit fleet: which model and which account serve each request.

The repository is Striatum; the crates, `ck-prefrontal-routing` binary,
`prefrontal-routing` module id, `prefrontal-routing.jsonc` configuration and
`~/.local/share/cortexkit/prefrontal-routing/store.db` retain their existing names.

```sh
cargo build --locked --release -p prefrontal-routing-module
bash scripts/rust-test-gate.sh
# On the macOS deployment host; validates, signs, installs and restarts routing:
sh script/deploy-module.sh routing
```

The test gate runs the source checks and
`cargo nextest run --locked --lib --bins --tests`. Rust 1.99.0, Python 3.10+
and cargo-nextest are required. No sibling repository is needed for this gate.
Six ignored real-daemon tests additionally build `../subconscious` and
`../insula` with `../commons` available. Run them explicitly after preparing
those siblings:

```sh
cargo nextest run --locked -p prefrontal-routing-module --test it --run-ignored only \
  -E 'test(/^(both_modules_e2e|real_daemon_e2e)::/)'
```

Deployment keeps the existing signature, configuration, running-inode and
supervisor-health gates. It stamps the build with the checkout revision,
tracked dirty state and lock digest. It requires the installed `ck` and
`subc-probe` clients and the existing supervised routing module.

See [ARCHITECTURE.md](ARCHITECTURE.md), [STRUCTURE.md](STRUCTURE.md) and the
[routing specification](docs/specs/model-routing.md).

[TODO(user): Review the purpose wording and the operator deployment instructions.]
