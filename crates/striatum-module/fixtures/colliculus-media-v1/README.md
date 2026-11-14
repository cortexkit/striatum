# Colliculus media declarations at `04a55268b791547f9c1def9875f644e7fd6369ef`

Frozen, byte-for-byte inputs generated in the Colliculus repository by the
ignored test `service::sets::tests::write_declaration_goldens`, using its real
handler and manifest. `SHA256SUMS` preserves the verified upstream checksums.

- `media-role-describe.json`: nine engines, including
  `openai/gpt-live-transcribe` as both `stream_transcribe` and `realtime`.
- `routing-v1-declaration.json`: five consumers, including
  `colliculus.dictation_token` with kind `realtime` and selector `callosum`.

The media sample regeneration test reads these committed files, not the ignored
handoff directory. Its success fixture uses the declarations unchanged. Its
no-engine fixture removes only the `realtime` engine, retaining the distinct
`stream_transcribe` declaration for the same provider/model. Credentials and
catalog grades are synthetic; pricing comes from the pinned Fusiform fixture.
