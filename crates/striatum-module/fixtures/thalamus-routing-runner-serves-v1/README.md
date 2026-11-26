# Routing runner declares its served models v1

These samples pair a routing 0.8.0 runner declaration with an opt-in
llm-runner 0.10.0 `role.describe` reply. None has a trailing newline.

- `all-harnesses.json`: the exact `manifest.role_declarations["routing/v1"].runner`
  object from the shipping HELLO. HELLO names all three harnesses before the
  deployment settings are known.
- `opencode2-only.json`: the same builder and HELLO writer with only OpenCode 2
  passed to the builder. This exercises a manifest subset; it is not a deployment
  setting.
- `describe-serves.json`: the raw opt-in reply from the real OpenCode 2.4.1
  catalog test. Only OpenCode 2 is configured. Its `opencode-go` stand-in lists
  `probe` and `wide` alongside compatible bundled models. The catalog also
  contains `unsupported`, which is omitted because it uses the Anthropic driver.
  Pi and Claude Code are unconfigured and appear as empty lists.

The HELLO samples match the bytes emitted by the Linux shipping build of
Thalamus commit `ee2d2c451d3a7117de5a2f972902bd718a2d6085`. The real OpenCode
test captured the `role.describe` reply on Linux from Thalamus commit
`03456daac9592a277b7a4a20ea77b15ceaae6f2a`.

On Linux, capture the declaration with the shipping build (no `test-support`,
which adds a Pi capability):

```sh
out=$(mktemp -d)
THALAMUS_RECORD_ROUTING_RUNNER="$out" cargo test --locked \
  -p thalamus-harness-module --lib \
  wire::routing_tests::routing_hello_runner_entry_bytes -- --exact
```

Capture the description with the pinned OpenCode executable. Build the module's binary
with `test-support` so the private holder can use the test's in-memory vault:

```sh
bash probes/opencode-linux/fetch.sh "${TMPDIR:-/tmp}/oc2" 2.4.1
npm ci --ignore-scripts --no-audit --no-fund --cache "${TMPDIR:-/tmp}/npm-cache" \
  --prefix opencode2-plugin/thalamus
cargo build --locked -p thalamus-harness-module --features test-support --bin ck-thalamus-harness
THALAMUS_RECORD_ROUTING_RUNNER="$out" \
  THALAMUS_OPENCODE2_BIN="${TMPDIR:-/tmp}/oc2/opencode" \
  THALAMUS_TEST_HOLDER_IMAGE="$PWD/target/debug/ck-thalamus-harness" \
  cargo test --locked -p thalamus-harness-module --features test-support --lib \
  runner::tests_opencode2_real::serves::real_opencode2_serves_uses_the_catalog_and_excludes_an_unsupported_driver \
  -- --exact --ignored
```

Verify the files with `sha256sum -c SHA256SUMS` from this directory.
