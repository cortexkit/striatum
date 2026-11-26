# Broca routing 0.8.0 wire samples

The native-free `wire::wire_serves::role_describe_serves_tracks_complete_catalog_and_gateway_snapshots`
integration test emits these files from the actual module connection. It uses a synthetic Fusiform
catalog and one synthetic scoped vault row. Neither file contains production data or credential
material.

- `hello-routing-slot.json` is the raw `manifest.role_declarations["routing/v1"]` JSON value
  from HELLO, with no outer envelope or trailing newline.
- `role-describe-serves.json` is the raw `result` JSON value from the response to
  `role.describe {"serves": true}`, also without an envelope or trailing newline. The fixture
  advertises seven capped account providers and one uncapped provider. Its exclusions exercise
  a provider with both renderable and unsupported models and a Google model without an endpoint.
  The Google model with a Code Assist route remains served.

To recapture on Linux from the checkout root:

```sh
nice -n 10 env CARGO_INCREMENTAL=0 BROCA_SERVES_SAMPLE_DIR="$PWD/.cortexkit/internal-docs/samples/runner-serves-1126" \
  cargo test --locked -p broca-module-serve --test it --all-features \
  wire::wire_serves::role_describe_serves_tracks_complete_catalog_and_gateway_snapshots -- --exact
```

Run `sha256sum -c SHA256SUMS` in this directory to verify the files. This slot and describe
answer must be read by routing 0.8.0; they are not compatible with the previous routing reader.

## Default `role.describe` (no `serves` param)

- `role-describe-default-before.json` is broca 0.4.147's default answer (the `broca-wire`
  golden), and `role-describe-default-after.json` is the same answer with this change. The only
  difference is the added `"serves"` entry in the capability list, which llm-runner 0.10.0 §2.1
  requires in every answer. Any params other than `{"serves": true}` get this default answer.
