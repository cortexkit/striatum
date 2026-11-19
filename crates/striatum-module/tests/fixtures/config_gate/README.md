These JSON bodies are copied unchanged from `subc-protocol` 0.37.4,
`tests/golden/config_gate`, the module-delivery goldens used by the
`subc-client-rs` config tests. The routing integration test uses the same
envelopes and replaces the sample view's payload with routing's user-layer
settings for delivery. The upstream `vectors/config-settings-v1` corpus covers
operator requests, not these module hooks.
