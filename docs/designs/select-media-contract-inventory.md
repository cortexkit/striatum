# Media selection: pinned contract and main inventory

## Scope and evidence

This is a read-only inventory, not an advertisement or implementation of media
`route.select`. The owner must confirm the findings and the proposed pricing
pairs before dependent implementation work starts. No dependency pin is moved.

The references below use these fixed sources:

- **M**: striatum main `d07c6a8888f8fc720a6eacd856dc56742abea19d`.
- **E**: striatum `ef108e344b8e07ab8d02f2587b14c8041e05f296`, the revision of
  the reviewed specification's original line citations.
- **C**: published `cortexkit-role-routing` **0.7.25**, including its
  `CONTRACT.md`, Rust types, validators and tests. Paths prefixed `C:` are
  relative to that published crate, not striatum.

Repository paths without a prefix are relative to striatum. Line numbers labeled
M refer to the pinned main revision, not to later main or this report's delivery
base (`6c55a37ae6f651ace582695cf58c958a4d6eede3`). The owner authorized reading
these revisions from this worktree's git objects and the published local Cargo
registry because the supplied frozen-evidence manifest contained no files.
Read-only copies were extracted inside the worktree for comparison.

The reviewed specification is
`.cortexkit/prefrontal/specs/ct_00000000-0000-4245-98e6-dfbc03372b28.md`
(the campaign-provided local specification, intent items 1–10 and chair rulings).
Its Wire paragraph describes 0.7.24; C's changelog says 0.7.25 changes account
continuation behavior but preserves wire shapes and capabilities
(`C:CHANGELOG.md:3–11`). That does not by itself establish that the paragraph is
complete: the inventory below checks the published contract and source.

## Findings requiring owner confirmation

1. **The exact character-count member does not exist in C.** Synthesis has
   `input_bytes` (UTF-8 text bytes), not a character count. Image selection has
   no image-count member either. The original intent item 5 would leave every
   speech row unpriced. During this inventory the owner explicitly amended that
   rule: pair speech's `input.per_character` with `input_bytes` as a **ceiling**,
   not an exact charge estimate. UTF-8 bytes are never fewer than characters.
   This accepted exception resolves the original stop condition without
   inventing a request member or moving a pin.
2. **Ceiling provenance is internal, not a new contract basis.** The owner
   clarified that the answer uses `price.basis: "metered"`, the per-character
   unit rate, and `estimated_task_usd` containing the ceiling. The marker
   `bytes_as_characters_ceiling` belongs in routing's own stored decision pricing
   detail, not on the wire. C permits only `metered` and
   `subscription_equivalent` producer bases. No provenance question remains
   open under that ruling. A later contract's optional `input_characters` is an
   owner-reported follow-up with the routing contract's maintainers, **not a
   member of 0.7.25**; prefer it when a
   later pinned release actually supplies it.
3. **Contract-first differences outside the Wire paragraph need attention.**
   C orders an all-ungraded population by cost, then measurement, then identity,
   even with a level; intent item 3's no-bar order instead puts measurement
   before cost. C also retains ungraded routes after adequate graded routes
   under a level. Its parser returns `pin_unavailable` reason `invalid` for a
   variant-bearing request pin; intent item 4 calls variants `not_available`.
   These are reported differences, not changes made by this inventory
   (`C:CONTRACT.md:2376–2398`; `C:src/select_media.rs:263–272`).

The owner still confirms the report before any dependent dispatch. This document
implements none of the prospective behavior or tests.

## Published media request contract

### Job-size members and units

All six kinds reuse the same `EngineRequirements`; the parser does not supply a
different numeric-size struct for each kind. These are optional unsigned `u64`
members. `duration_ms` must fit `max_duration_ms`, and `input_bytes` must fit
`max_input_bytes`; a missing matching engine fact fails a stated requirement
(`C:src/select_engine.rs:53–78,330–334`; `C:CONTRACT.md:2060–2064,2341–2349`;
`C:src/select_media.rs:99–112,221–256`).

| Kind | Admitted job-size members | Units and limits of the definition |
|---|---|---|
| `transcribe` | `requirements.duration_ms`, `requirements.input_bytes` | Milliseconds of input; decoded audio bytes. |
| `stream_transcribe` | `requirements.duration_ms`, `requirements.input_bytes` | Milliseconds; decoded audio bytes, cumulative for a stream. |
| `synthesize` | `requirements.duration_ms`, `requirements.input_bytes` | Milliseconds (no synthesis-specific duration meaning supplied); **UTF-8 text bytes**, not characters. |
| `realtime` | `requirements.duration_ms`, `requirements.input_bytes` | Milliseconds and bytes. The common rule describes decoded audio bytes but does not separately define a realtime-specific quantity. |
| `image` | `requirements.duration_ms`, `requirements.input_bytes` | Milliseconds (no image-specific duration meaning supplied); summed reference bytes. No image count. |
| `video` | `requirements.duration_ms`, `requirements.input_bytes`, `requirements.output_duration_ms` | Common duration in milliseconds, summed reference bytes; video output duration in unsigned milliseconds, bounded by the catalog output-duration limit. |

`size` and `resolution` are strings, not image counts or numeric billed job
quantities. `reference_formats` is a format requirement, not a count.
`aspect_ratio` is a positive-integer `"W:H"` string and `audio` is a boolean,
not additional job quantities. `language`, `voice`, `format`, `bias`, `quality`,
`latency_class`, and `metric` are the other shared matching/ranking requirements
(`C:src/select_engine.rs:53–78`; `C:CONTRACT.md:2341–2362`).

**Characters and images:** C defines neither `input_characters`, `characters`,
`chars`, `text_length`, nor an image-count selection requirement. Its separate
`route.engine_report` has `input.chars`; that does not admit a selection member
(`C:CONTRACT.md:2119–2124`). For `synthesize`, the only text-length member is
`requirements.input_bytes`, in **bytes**. It is not an exact quantity in the
`per_character` rate's unit. The owner's ceiling ruling above is the explicit
exception used by the pricing proposal below.

### Eligibility, request pins, thresholds and refusals

- **`eligible_routes` is admitted** as an optional top-level
  `[{provider, model, variant?}]`, independently gated by
  `select_eligible_routes`. Omission adds no restriction; empty is a valid empty
  subset, not a parse error. Policy, pins and ladders cannot widen it; duplicates
  add nothing. The media answer validator tests the selected route against it
  (`C:CONTRACT.md:2316–2329`; `C:src/select_media.rs:132–147,441–459`;
  `C:src/select.rs:741–760`).
- **A request `pin` is admitted** as an optional top-level string. Media pins
  must be plain `provider/model`, not a variant or slot expression; invalid
  syntax/variants return `pin_unavailable` with reason `invalid` and no reset.
  `pin_fallback` is not an admitted media member. A module-model pin excludes
  caller-local offers; a caller-local pin/ladder entry must actually be offered
  for that kind (`C:src/select_media.rs:199–214,263–272`;
  `C:CONTRACT.md:2420–2428`).
- **An unmeasured route fails `metric_limit`.** Exactly one finite bound
  (`at_most` or `at_least`) is required. No comparable evaluation is a hard
  exclusion, not an unknown sorted after measured engines; `admits(None)` is
  false. The independent `requirements.metric` chooses the ranking metric
  (`C:CONTRACT.md:2356–2362`; `C:src/select_media.rs:62–93`;
  `C:tests/select_media.rs:60–68`).
- **`no_route` detail** is `{reason, reset_at_ms?}`, not the legacy engine
  operation's `{filter}`. Empty eligible media sets use `no_route`, except a
  pinned request uses `pin_unavailable`. Known `NoRouteReason` strings are
  `no_available_model`, `harness_unavailable`, `missing_capability`,
  `context_too_small`, `quota_or_cooldown`, `concurrency_full`, `all_inadequate`,
  and `accounts_blocked`. The reason set is open. Known-end waits carry
  `reset_at_ms`; `concurrency_full` always does. A plain exhaustion need not
  invent a reset (`C:src/errors.rs:119–143,364–370`;
  `C:CONTRACT.md:705–715,729–736,2399`).
- **`pin_unavailable` detail** is `{pin, reason, reset_at_ms?}` with the original
  pin text. Its known reasons are `invalid`, `unknown_slot`, `unknown_provider`,
  `unknown_model`, `invalid_variant`, `excluded`, `not_available`,
  `credential_unentitled`, `credential_billing_exhausted`, `cooldown`,
  `quota_exhausted`, and `backoff`. This set is also open, not exhaustive for
  consumers. `cooldown` and `backoff` are timed; `quota_exhausted` is timed when
  its reset is known. Intent item 4 chooses `excluded` for only-for/not-for and
  `not_available` for other *selection* failures, but that does not override
  the pinned parser's `invalid` request-pin refusal
  (`C:src/errors.rs:95–116,354–362,487–495`; `C:src/lib.rs:32–36`).

### Unit price and advertisement

`SelectMediaAnswer` is `{decision_id, expires_at_ms, engine, price?}`, with the
engine's `provider`, `model`, `local`, `caller_local`, optional `module` and
`index`, and the engine contract's ten-minute expiry. `price` is omitted, not
null, when absent (`C:CONTRACT.md:2400–2419`;
`C:src/select_media.rs:401–410`).

The full price object is
`{basis, multiplier?, rates: {key: {rate, step?, minimum?}}, estimated_task_usd?}`.
The rate is a unit price in USD per the role/unit named by the key, not per
million tokens and not an amount accompanied by a separate `per` denominator.
A Fusiform amount per N units must therefore be divided by N for a one-unit
answer rate (the intent's normalization); rate keys are copied verbatim, not
rewritten to a preferred unit spelling. The catalog fact prefix `rate.` is not
part of the answer's key: for example `rate.input.per_character` supplies
`input.per_character`. C's examples also include `input.per_audio_second`,
`output.per_image`, and `output.per_video_second`; those are examples, not
aliases for main's `input_audio.per_second` fact
(`C:CONTRACT.md:2410–2419`; `C:src/select_media.rs:302–340,368–398`).

**`step` and `minimum`:** these are optional numbers on one unit rate. The
pinned text and Rust define positivity for `step`, non-negativity for `minimum`
and `rate`, and finiteness for all three. They do **not** define a billing
formula, rounding order, whether `minimum` is money or quantity, or an explicit
mapping from Fusiform's tagged `increment`/`minimum_quantity`. Calling them a
proven billing quantum/floor in this release would go beyond its stated
semantics. The inventory therefore records that meaning as underspecified,
not as permission to invent rounding. Intent item 5 independently requires
omitting both for absent/unknown Fusiform shapes and failing closed for other
shapes outside transcription. The constructor/validator and the sole test
round-trip the numbers; they do not calculate a charge
(`C:src/select_media.rs:302–328,373–398`;
`C:tests/select_media.rs:110–129`; `C:CONTRACT.md:2410–2419`).

`basis: "metered"` carries list unit rates and **no multiplier**;
`subscription_equivalent` carries already-divided list rates and the positive
multiplier (never divide twice). `estimated_task_usd` is a value estimate, not
money spent. C requires omitting `price` when the job cannot be priced and an
unmatched role/unit stays unpriced. Intent item 5 additionally omits it for
token-priced/local/unpriced winners and emits only the measurable per-unit
rates. The owner's speech ceiling ruling retains the contract-valid metered
basis and puts the ceiling in `estimated_task_usd`
(`C:CONTRACT.md:2410–2419,2434–2464`;
`C:src/select_media.rs:331–398`).

**`kinds` is `role.describe.kinds`**, a top-level sibling of `capabilities`, not
a child of a `select_media` capability object. Its type is `Vec<placement::Kind>`
and an empty list is omitted. The capability string `select_media` requires
`select_kind`; the parser checks both and the advertised non-llm list
(`C:src/describe.rs:11–18`; `C:src/lib.rs:161–165`;
`C:src/select_media.rs:182–197`). The intended order remains
`["transcribe", "stream_transcribe", "synthesize", "realtime", "image", "video"]`.

### Wire paragraph versus the pinned release

There is **no contradictory wire assertion** in the specification's short Wire
paragraph (intent, line 116) when “lists `select_media` with `kinds`” is read as
shorthand for the two top-level members above. Its shared engine members,
video additions, `metric_limit`, optional media `iq`, required top-level
privacy, caller-local shape, llm/media separation, engine answer, and unit-rate
shape agree with C. There is no media wire change from 0.7.24 to 0.7.25.

For completeness, these are all the summary's omissions/ambiguities exposed by
the inventory, rather than new fields routing may invent:

- `kinds` is a top-level sibling, not capability metadata.
- The shown `price.rates` fragment omits required `basis`, optional `multiplier`
  and `estimated_task_usd`, rate validation, and the unpriceable omission rule.
- `"<role>.per_<unit>"` is a placeholder for verbatim catalog keys, not a
  renaming algorithm.
- The paragraph does not describe admitted `eligible_routes`, its independent
  capability gate and empty/subset rules, or admitted request `pin`, its plain
  route syntax, and the lack of media `pin_fallback`.
- It does not spell out the `no_route`/`pin_unavailable` detail shapes and
  reason sets, or missing-evaluation failure under `metric_limit`.
- It does not define any character-count or image-count member, nor the
  semantics of `step`/`minimum` beyond the constraints above; neither does C.

The request pin and all-ungraded ranking conflicts noted at the start are with
other intent paragraphs, **not** changes introduced by 0.7.25's Wire schema.

## Main's pinned media fixture rows

M's `crates/striatum-module/fixtures/served-payloads.pin.json:1–6` pins
`CortexKit/fusiform` commit `9591c735106d7d31f6ba917be96b00810be81af0`, path
`crates/fusiform-module/fixtures/served-payloads.json`, SHA-256
`afeac184c2481f67afe3c7dee03b5a96c069cfa86ffd7fb037f9674c5de4aa46`.

The following table inventories **each response row**, not a deduplicated live
catalog. In particular, the grade-only response for `openai/gpt-transcribe` is
listed separately from its rate-bearing response. `F` below means M's
`crates/striatum-module/fixtures/served-payloads.json`; all line ranges in this
section are in F. A request selector mentioning `model.type` is not a model row.
There are nine media-typed response rows, eight identities, and no `realtime`
row. Every actual rate entry in these rows has state `priced`.

`—` means **absent**, not zero or unknown. `unknown` means the actual object
`{"kind":"unknown"}`. Amount is `units / 10^exponent` USD; it is per the stated
`per` for unit rates, and per million tokens for the legacy token-rate keys
(not an inferred `per: 1000000` field).

| Provider/model; type; row lines | Rate key (and rate lines) | State | USD amount | `per` | `increment` | `minimum_quantity` |
|---|---|---|---:|---:|---|---|
| `elevenlabs/eleven_flash_v2_5`; speech; 1096–1128 | `rate.input.per_character` (1107–1122) | priced | 0.04 | 1000 | unknown | unknown |
| `openai/gpt-image-2.6-flare`; image; 1155–1212 | `rate.cache_read` (1171–1179) | priced | 1.25 | — | — | — |
| same row | `rate.input` (1180–1188) | priced | 5 | — | — | — |
| same row | `rate.input_image` (1189–1197) | priced | 8 | — | — | — |
| same row | `rate.output_image` (1198–1206) | priced | 30 | — | — | — |
| `google/gemini-3.8-flash-tts`; speech; 1489–1529 | `rate.input` (1506–1514) | priced | 0.50 | — | — | — |
| same row | `rate.output_audio` (1515–1523) | priced | 9 | — | — | — |
| `google/gemini-omni-1.2-flash`; video; 1556–1610 | `rate.input` (1578–1586) | priced | 1.50 | — | — | — |
| same row | `rate.output` (1587–1595) | priced | 9 | — | — | — |
| same row | `rate.output_video` (1596–1604) | priced | 12 | — | — | — |
| `google/gemini-3.5-transcribe-live`; transcription; 1637–1674 | `rate.input_audio` (1652–1660) | priced | 3.50 | — | — | — |
| same row | `rate.output` (1661–1669) | priced | 21 | — | — | — |
| `openai/gpt-transcribe`; transcription; 3669–3704 | `rate.input_audio.per_second` (3684–3699) | priced | 0.0045 | 60 | unknown | unknown |
| `openai/gpt-live-transcribe`; transcription; 4002–4037 | `rate.input_audio.per_second` (4017–4032) | priced | 0.017 | 60 | unknown | unknown |
| `openai/gpt-transcribe`; transcription; 4523–4526 | **No rate key** (partial `quality.grade` response) | — | — | — | — | — |
| `zai/synthetic-glm`; transcription; 7682–7732 | `rate.input` (7700–7709) | priced | 0.075 | — | — | — |
| same row | `rate.output` (7710–7718) | priced | 0.25 | — | — | — |

### Which rows can price a job?

“Priced” here assumes the request actually supplies the paired quantity and
the route passes admission; a rate's own `state: priced` is not enough to price
a media job. Intent item 5 is applied with its transcription exception and,
where noted, the owner's subsequent speech-ceiling amendment.

| Response row | Priced under original intent item 5? | After the accepted ceiling ruling; calculation / refusal to infer |
|---|---|---|
| Eleven Flash TTS (1096–1128) | **No**: no character-count member in C. | **Yes, ceiling only**, with `input_bytes`: `0.04 / 1000 × input_bytes = 0.00004 × input_bytes` USD. No other rate is incurred on this row. Unknown increment/minimum impose no rounding/floor. Without `input_bytes`, unpriced. |
| GPT Image Flare (1155–1212) | No. | No: all four rates are token rates, with no token-per-image/reference-byte fact. `rate.output_image` does not mean per image. |
| Gemini Flash TTS (1489–1529) | No. | No: input/output-audio token rates remain unmeasurable; `input_bytes` supplies neither token quantity. |
| Gemini Omni video (1556–1610) | No. | No: input/output/output-video token rates have no request-unit conversion. `output_duration_ms` cannot price token `rate.output_video`. |
| Gemini Transcribe Live (1637–1674) | **Yes**, with `duration_ms`. | Same `engine_cost`: `duration_ms / 1000 × 25 × 3.50 / 1_000_000`. The row states `limit.audio_tokens_per_second: 25` (1645). Output's 21 USD/MTok is deliberately ignored, not an all-or-nothing failure. No per-unit wire `price` from this token-priced estimate. |
| GPT Transcribe (3669–3704) | **Yes**, with `duration_ms`. | Same `engine_cost`: `duration_ms / 1000 × 0.0045 / 60` (0.000075 USD per second). |
| Live Transcribe (4002–4037) | **Yes**, with `duration_ms`. | Same `engine_cost`: `duration_ms / 1000 × 0.017 / 60` USD. |
| Grade-only GPT Transcribe (4523–4526) | No, as a standalone response row. | No rate is present; do not invent one from the grade. Its separate full response above supplies a price when that row is available. |
| Unmarked creator synthetic GLM (7682–7732) | No. | No: `engine_cost` finds no audio-input rate, regardless of the two token rates or the row's transcription label. |

M's unchanged `CatalogSnapshot::engine_cost` explicitly limits itself to
`transcribe|stream_transcribe`, requires duration, prefers the audio per-second
rate, otherwise requires both input-audio token rate and audio-tokens-per-second,
and ignores output-token rates
(`crates/striatum-module/src/fusiform_catalog.rs:335–357`). Outside those kinds,
every row rate is incurred except known `stated_zero`/`not_billed` zeroes; there
are no such zero-rate entries in the nine rows above. A mixed measurable and
unmeasurable row stays unpriced. Realtime's all-incurred rule remains the
specification's explicit assumption; there is no realtime fixture to confirm it.

### Proposed closed pair table for intent item 5

This is a table of the **served keys actually available in the pinned fixture**,
not a license to synthesize rates for units the fixture does not serve. Exact
role matching matters; bytes do not imply tokens, and `per_second` is not
automatically interchangeable with another role's seconds.

| Kind | Catalog rate role/unit | Request quantity and calculation | Status |
|---|---|---|---|
| `transcribe` | `input_audio.per_second` | `requirements.duration_ms / 1000`; normalize the amount by `per`. | Existing `engine_cost`, exact duration-based estimate. |
| `stream_transcribe` | `input_audio.per_second` | Same duration/normalization. | Existing `engine_cost`, unchanged. |
| both transcribe kinds | `input_audio` (per-million-token fallback) | `duration_ms / 1000 × limit.audio_tokens_per_second`, then rate / 1,000,000. | Existing exception, not a new token conversion or per-unit answer price. |
| `synthesize` | `input.per_character` | **`requirements.input_bytes`**, amount / `per` × bytes. | Accepted owner exception: **bytes-as-characters ceiling**, not an exact character quantity. Future `input_characters` takes precedence only after its contract release is pinned. |
| `realtime` | None in this fixture. | No served pair to propose. `duration_ms`/`input_bytes` exist but do not create a catalog rate. | No priced fixture row. |
| `image` | None measurable in this fixture. | No image count; reference bytes do not pair with `input_image`/`output_image` token rates. | Unpriced. |
| `video` | None measurable in this fixture. | `output_duration_ms` supplies output milliseconds, but the row serves token `output_video`, not a per-output-second rate. | Unpriced. |

The contract's illustrative `output.per_image` has no image-count request pair.
Its illustrative `output.per_video_second` could pair with
`output_duration_ms / 1000` if that key is actually served in a future fixture;
neither is a rate in this pinned fixture, so neither adds a currently served
pair. No `per_byte` rate is served. This keeps the proposed implemented list
closed while preserving the distinction between video input duration and
output duration (`C:CONTRACT.md:2343–2349,2410–2419`).

For the accepted speech exception, emit the one-character rate
`rates["input.per_character"].rate = 0.00004`, `basis: "metered"`, no
`step`/`minimum`, and `estimated_task_usd = 0.00004 × input_bytes`. Mark only
the **internal stored pricing detail** `bytes_as_characters_ceiling`. Main's
existing internal task-pricing evidence lives in
`model_route_decisions.score_json` at `objective.taskPricing`, distinct from
wire `price` (`crates/striatum-module/src/routing_v1/profile_tests.rs:10–24`;
`routing_v1/select.rs:1935–1947,1987–1995`; the latter path is under
`crates/striatum-module/src/`). The owner requested a future regression test
using multibyte UTF-8 text: estimated bytes × unit rate must be at least
characters × unit rate, and must be labeled a ceiling. This report adds no
production behavior or tests.

## Recheck of every specification line citation on main

The left column is the specification's **E citation**, including repeated
citations only once. The right column locates its named rule, test or fixture on
**M**, not merely the same numeric lines. **Unchanged** means the named rule is
retained at the cited location; **moved** means retained with shifted lines;
**different** means changed behavior or pinned data (the explanation separates
changed adjacent code from the retained rule). No cited rule has disappeared.

Path abbreviations in the tables are exact prefixes:

- `RV/` = `crates/striatum-module/src/routing_v1/`.
- `SM/` = `crates/striatum-module/src/`.
- `SC/` = `crates/striatum-core/src/`.
- `FIX/` = `crates/striatum-module/fixtures/`.

### Pins, catalog, fixtures and non-routing helpers

| E path and line citation | Status; M location and named rule |
|---|---|
| `Cargo.toml:28–30` | **Different**, M:28–30: routing `=0.7.23` → `=0.7.25`; conformance `=0.10.26` → `=0.10.28`. Only `crates/striatum-module/Cargo.toml:24,48` consumes the two workspace dependencies. This inventory changes neither. |
| `SM/fusiform_catalog.rs:306–323` | **Different**, M:311–329 (`CatalogSnapshot`): adds decommissioned state; catalog snapshot remains the join source. |
| `SM/fusiform_catalog.rs:326–348` | **Moved**, M:335–357 (`CatalogSnapshot::engine_cost`): same transcription-only duration/audio-input cost, same ignored output-token rate. |
| `SM/fusiform_catalog.rs:288–304` | **Moved**, M:287–309 (`CatalogRate`): same tagged rate states; this is not grade decoding. |
| `SM/fusiform_catalog.rs:385–405` | **Moved**, M:394–415 (`AudioInputPrice::decode`): same input-audio per-second amount/`per` and per-token fallback. No new character/image-unit decoder. |
| `SM/fusiform_catalog.rs:991–1012` | **Different**, M:1002–1034 (`decode_catalog_get`): newly decodes withdrawal independent of grading, then retains the `quality_scores.graded_by == "operator"` Elo/EQ gate. It still does not decode `quality.grade`. |
| `SM/fusiform_catalog.rs:997–1011` | **Moved**, M:1014–1033: the cited operator-only `quality.elo`/`quality.eq` branch itself is unchanged; the decommissioned branch is new immediately before it. |
| `SM/fusiform_catalog.rs:1023–1034` | **Different**, M:1045–1068: per-`provider/model` audio-price insertion remains independent of `model.type`; decoded facts now acquire `login_methods_documented`. Neither change adds a type filter to pricing. |
| `SM/fusiform_catalog.rs:896–898` | **Moved**, M:880–912 (`rate_cost`, zero-state arm at 905–907): `stated_zero` and `not_billed` still yield known zero. |
| `SM/fusiform_catalog.rs:905–920` | **Moved**, M:914–929 (`report_refused_rate_once`): refused rate logging remains once per model/rate. |
| `FIX/served-payloads.pin.json:1–6` | **Different**, M:1–6: Fusiform `7d604d1623b9085b3f3277c889d417d2ccb6f5e0` → `9591c735106d7d31f6ba917be96b00810be81af0`; SHA-256 `f9555ed07197b60119627a391d75ffa59a3f9f5647df28251220fbe19faee1fc` → `afeac184c2481f67afe3c7dee03b5a96c069cfa86ffd7fb037f9674c5de4aa46`. The old absence-of-unit/grade-fixture claim is no longer true: M's payload has per-character/per-second rates and `quality.grade` (table above; F:4510–4535). |
| `SM/account_selection.rs:624–637` | **Unchanged**, M:624–637 (`media_credential_serves`): active, media-provider-category API key, with explicit provider coverage; `login_methods` changes nothing in this function. See below. |
| `SM/account_selection.rs:821–847` | **Unchanged**, M:821–847 (`engine_has_eligible_credential`): listing readiness and transient credential/account eligibility remain distinct from static media-key coverage. |
| `SM/config_policy.rs:269–271` | **Unchanged**, M:267–271: pin remains a provider/model string, not a kind/origin structure. |
| `SM/config_policy.rs:638–661` | **Unchanged**, M:638–666 (`ConsumerPolicy`): same below-bar, ladder, pin and level fields. |
| `SM/config_policy.rs:642–645`; `694–696` | **Unchanged**, M:same ranges: the `mason` consumer and its dotted descendants default to `below_bar: refuse`; other consumers default to `best_effort`. |
| `SM/config_policy.rs:684–724` | **Unchanged**, M:684–724 (`resolve_consumer_policy`): same inherited settings/default resolution; media-specific interpretation remains future work. |
| `SM/config_policy.rs:727–733` | **Unchanged**, M:727–733: settings pin folds into a one-entry ladder. |
| `SM/config_overrides/new_shape.rs:344–348` | **Unchanged**, M:344–348 (`route_allowed`): pin bypasses pin-only, not exclusion/consumer restriction. |
| `SM/config_overrides/new_shape.rs:350–377` | **Unchanged**, M:350–377: metered/default refusal methods remain; media must avoid the llm-only default as intent requires. |
| `SM/config_overrides/new_shape.rs:510–512` | **Unchanged**, M:507–512: same built-in metered pin-only condition. |
| `SM/select_policy.rs:186–236` | **Unchanged**, M:184–236 (`select_in_order`): ladder does not fall through to automatic selection outside the ladder. |
| `SM/diagnostics_findings.rs:1091–1108` | **Unchanged**, M:1086–1118 (`flow_route_refusal`): only diagnostics.findings is admitted at flow scope. |
| `SM/main.rs:1541–1551` | **Unchanged**, M:1541–1551: flow refusal precedes routing-op dispatch. |
| `SC/routing_config.rs:14` | **Unchanged**, M:14: llm demand exponent is 0.35, not a linear media grade curve. |
| `SC/routing_config.rs:149–151` | **Unchanged**, M:149–151: top score excludes transient state by using statically routable facts. |
| `SC/routing_config.rs:165–170` | **Unchanged**, M:165–170: llm demand applies the exponent. |
| `SC/model_router.rs:968–972` | **Moved**, M:981–984: same statically-routable top population excludes transient account exhaustion; additional decommissioned rejection precedes it (M:961–980). |
| `test-vectors/route-select-engine-v1/flow-scope-not-supported.json:1` | **Unchanged**, M:1: required privacy is top-level and the flow-scoped request is refused. |

Two further named fixtures in the specification have no numeric line citation:
`test-vectors/route-select-engine-v1/README.md`'s `unknown-kind.json` entry is
**unchanged** at M:65–68 (`no_engine {filter:"kind"}`), and
`test-vectors/route-select-engine-v2/SHA256SUMS` is **unchanged** at M:1–30.

### Engine path, consumer admission and language-model selection

| E path and line citation | Status; M location and named rule |
|---|---|
| `RV/engines.rs:63–280` | **Different**, M:63–291 (`select_inner`): same engine path, plus decommissioned filtering before account loading (M:113–124). It still records engine decisions and does not become the llm path. |
| `RV/engines.rs:43–54` | **Unchanged**, M:43–54 (`is_decision`): stored answer's engine object identifies an engine decision. |
| `RV/engines.rs:84–89` | **Unchanged**, M:84–89: catalog snapshot is still optional. |
| `RV/engines.rs:113–122` | **Moved**, M:125–134 (`ClassContext::read_with_catalog`): engine selection still performs the live account load; new withdrawal filtering is just before it. |
| `RV/engines.rs:124–133` | **Moved**, M:135–145: candidate price still comes from optional catalog `engine_cost`. |
| `RV/engines.rs:134–150` | **Moved**, M:146–162: provider/model key and boolean route-fence state unchanged. |
| `RV/engines.rs:139–150` | **Moved**, M:151–162: cooldown/quota/provider-reset/backoff or credential-refusal fence remains a boolean, not a refusal reset timestamp. |
| `RV/engines.rs:151–162` | **Moved**, M:163–174: remote engines still bypass provider/model pin-only through pinned-style policy admission, keep consumer restrictions and require an eligible credential; local policy bypass remains. |
| `RV/engines.rs:170–181` | **Moved**, M:182–193: caller-local offers are excluded from stored-measurement identities. |
| `RV/engines.rs:182–189` | **Moved**, M:194–201: requested ranking metric, else `wer` for `transcribe` only. |
| `RV/engines.rs:244–278` | **Moved**, M:255–289: ledger kind `engine`, priced-per-token class, stored request/answer and engine decision persistence retained. |
| `RV/engines.rs:335–346` | **Moved**, M:336–356 (`report`): speed report still requires the stored engine request's kind to match. |
| `RV/engines_tests.rs:472`; `486–489` | **Moved**, M:479–539 (rate fixture at 490): transcription pricing test retains per-60-second rate with unknown increment/minimum and same expected costs. |
| `RV/engines_tests.rs:575–612` | **Moved**, M:592–630: automatic select_engine still ignores provider/model pin-only while enforcing consumer refusal. |
| `RV/select_engine_vectors_tests.rs:131–141` | **Unchanged**, M:131–141: 0.0045 USD per 60 seconds fixture, hence 0.000075 USD per second. |
| `RV/consumers.rs:43–51` | **Unchanged**, M:43–51 (`engine_declarations`): module `media-provider/v1` declarations remain the engine source. |
| `RV/consumers.rs:335–367` | **Unchanged**, M:334–370 (`defaults_for`): declaration defaults carry ladder but no pin, and inherit within the same owner. |
| `RV/consumers/kind.rs:6–26` | **Unchanged**, M:5–26 (`resolve`/canonicalization): declaration aliases canonicalize, omitted kind defaults, a declared/request mismatch refuses. |
| `RV/accounts.rs:277–295` | **Unchanged**, M:276–295: engine-decision provider/model/module binding and expiry gate. Media-key/category admission remains at M:341–364. |
| `RV/report.rs:66–69` | **Unchanged**, M:59–75: engine decisions skip consumer re-admission. |
| `RV/reads.rs:111–116` | **Unchanged**, M:111–117: engine decisions are unknown to explain. |
| `RV/diagnostics_tests.rs:180–200` | **Unchanged**, M:167–207 (same test): flow scope permits diagnostics.findings but refuses select/account/report/describe. |
| `RV/select.rs:771–773` | **Moved**, M:778–780: what-if request skips consumer ownership admission. |
| `RV/select.rs:778–784` | **Moved**, M:785–791: production still refuses media-declared consumers on route.select. |
| `RV/select.rs:785–787` | **Moved**, M:792–794: llm inherited requirement defaults are still merged here. |
| `RV/select.rs:791–798` | **Moved**, M:798–805: llm waits for billing/catalog readiness and can return routing_not_ready. Engine path does not acquire that wait. |
| `RV/select.rs:1474–1476` | **Moved**, M:1496–1498: settings IQ still outranks preview evaluation/request IQ. |
| `RV/select.rs:1652–1719` | **Moved**, M:1675–1742: same ladder candidate scoring and ordered fallback. Existing pin-slot/fallback handling precedes it on both E and M; that is not newly added media behavior. |
| `RV/select.rs:1660–1672` | **Moved**, M:1683–1695: ladder entries still score within the allowed candidate subset. |
| `RV/select.rs:2336–2342` | **Moved**, M:2365–2371: automatic candidates still call route_allowed with pinned=false. |
| `RV/select.rs:2516–2531` | **Different**, M:2543–2570 (`Selection::pin`): the original exclusion admission remains, but main first refuses a decommissioned model with reason `decommissioned` (2548–2557), an open-set reason. This is llm behavior; intent separately prescribes media `not_available`. |
| `RV/consumers_tests.rs:669–707` | **Unchanged**, M:668–707 (same test): declared ladder cannot widen model pin-only or not-for policy. |

### Advertisement, conformance, replay and preview

| E path and line citation | Status; M location and named rule |
|---|---|
| `RV/conformance_tests.rs:1576–1580` | **Different**, M:1578–1601 (`PENDING_READERS`): select_media and sixteen cases join select_kind/placement_select_kind. Both groups remain pending, not advertised. |
| `RV/conformance_tests.rs:1604–1634` | **Moved**, M:1627–1657 (`routing_v1_conformance_kit_matches_served_checks`): every known group must be advertised or explicitly pending; pending groups must not be advertised. Same rule, larger pending data. |
| `RV/conformance_tests.rs:1674–1681` | **Moved**, M:1700–1708: pending case IDs still must appear as not_applicable; now includes media's sixteen. |
| `RV/consumers_tests.rs:129–152` | **Unchanged**, M:129–152: production media-declared route.select refusal and no select_kind advertisement. |
| `RV/consumers_tests.rs:341–363` | **Unchanged**, M:341–363: the declaration-only candidate scope retains production media refusal, sent-kind refusal and no select_kind advertisement. |
| `RV/describe.rs:54–62` | **Unchanged**, M:54–62: production list unless a cfg(test) candidate override is in scope. |
| `RV/describe.rs:72–83` | **Unchanged**, M:72–83: candidate scope adds to the production capability list. |
| `RV/describe.rs:90–103` | **Unchanged**, M:90–103: withheld-capability test scope remains available. |
| `RV/conformance_tests.rs:1702–1718` | **Moved**, M:1725–1744: production parser still rejects kind llm; candidate capability scope admits it. |
| `FIX/replay/core.golden.json:167–230` | **Unchanged**, M:167–230: checked-in role.describe reply remains byte-identical, with neither media group nor kinds. |
| `RV/replay_tests.rs:183–248` | **Unchanged**, M:183–248: replay invokes the production handler/list. |
| `RV/replay_tests.rs:321–329` | **Unchanged**, M:321–329: both observed and expected answers undergo the additive describe projection before comparison. |
| `RV/replay_tests.rs:572–635` | **Unchanged**, M:572–635: fixed removal list still excludes neither select_kind nor select_media, and does not remove kinds. |
| `RV/select/declared_samples_tests.rs:38–51` | **Unchanged**, M:38–51 (`require_production_capabilities`): compares with production's own list, not an independent assertion that either media group is absent. |
| `RV/conformance_tests.rs:1760–1777` | **Moved**, M:1784–1808: production capability comparisons and candidate-scope cleanup still track production's list. |
| `RV/what_if.rs:16–28` | **Unchanged**, M:16–28 (`Params`): llm preview inputs only; no kind member yet. |
| `RV/what_if.rs:71–76` | **Unchanged**, M:71–76: operator-only preview guard. |
| `RV/what_if.rs:79–92` | **Unchanged**, M:79–92: llm preview requires iqs or panel, with the same sweep bounds/validation. |
| `RV/what_if.rs:142–147` | **Unchanged**, M:142–147: registered consumer required. |
| `RV/what_if.rs:153–178` | **Unchanged**, M:153–178: disposable credential and account snapshots, not live healing state. |
| `RV/what_if.rs:200–222` | **Unchanged**, M:200–222: same row/envelope and no decision persistence. |
| `RV/what_if_tests.rs:629` | **Moved**, M:646–655: same fractional IQ and Elo bar/top-routable assertion; media grade output remains future work. |

### What `login_methods` changed in `media_credential_serves`

**Nothing.** E and M have the same function at
`crates/striatum-module/src/account_selection.rs:624–637`. It requires all four:
`row.state == "active"`, category `media-provider`,
`row.auth_method == Some("apikey")`, and explicit `row.provider_ids` membership
for the requested provider. It neither reads a catalog login-method marker nor
admits OAuth/subscriptions/generic sign-ins as media keys.

The change is elsewhere: `CatalogGetReply` adds `login_methods`
(`SM/fusiform_catalog.rs:126–139`); catalog decoding merges those auth rows with
curated ones and marks documented models (`SM/fusiform_catalog.rs:978–990,
1045–1053`). Model-level sign-in coverage is then consulted on the llm-side
credential path (`SM/broca_credentials.rs:28–85,242–256`), with a new coverage
test in `SM/fusiform_catalog_login_methods_tests.rs:216–247`. None changes the
media predicate used to define intent item 3's static top population. Transient
readiness, credential fences, blocks on a shared credential pool, and account
route fences/backoff remain in the separate eligibility check
at `SM/account_selection.rs:821–847` and must not lower that top.

### Every main test naming `select_kind` or `select_media`

A literal scan of all M repository files finds only
`RV/consumers_tests.rs`, `RV/conformance_tests.rs`, and production
`RV/select.rs` containing either lowercase spelling. The production
`select_kind` function is not a test. These are the two literal-bearing tests:

| Test | Location on M | Assertion naming the group |
|---|---|---|
| `production_consumer_declarations_serve_defaults_admission_placement_and_kind` | `RV/consumers_tests.rs:94–160` | At 152, role.describe does **not** contain `"select_kind"`; media-declared consumer refuses at 129–141. |
| `declared_kind_select_retains_media_and_unknown_kinds_and_production_hold` | `RV/consumers_tests.rs:328–363` | At 362, capabilities do **not** contain `"select_kind"`; production media/sent-kind refusals retained. |

There is **no test name or body with literal `select_media`** on M. Its sixteen
names are data in the pending-reader constant, not sixteen implemented local
tests. Including symbolic references and tests consuming that constant adds:

- `candidate_capabilities_reach_readers_without_leaking_to_production_calls`,
  `RV/conformance_tests.rs:1710–1808`: names `SELECT_KIND` at 1716, accepts a
  candidate kind-aware parser and description, rejects production's sent kind,
  and checks candidate groups do not leak. It does not add `SELECT_MEDIA`.
- `routing_v1_conformance_kit_matches_served_checks`,
  `RV/conformance_tests.rs:1627–1708`: consumes both groups through
  `PENDING_READERS`, ensuring known/unadvertised/pending and not-applicable
  case accounting. It has no literal group string in its own body.

The related `serving_bytes_match_tagged_vectors` test
(`RV/select/declared_samples_tests.rs:456–466`) uses the production-list helper
at 38–51, but names neither group directly. The core replay projection and
golden obligations remain the ones in the citation ledger; advertising must
not re-record a golden to hide the new description.

The full pending list at `RV/conformance_tests.rs:1578–1601` is:

- `select_kind`: `placement_select_kind`.
- `select_media`: `select_media_privacy`, `select_media_requirements`,
  `select_media_video`, `select_media_ranking`, `select_media_ladder`,
  `select_media_llm_refuses_media`, `select_media_refuses_llm`,
  `select_media_metric_limit`, `select_media_unlisted_kind`,
  `select_media_answer`, `select_media_pin`, `select_media_price`,
  `select_media_demand`, `select_media_ungraded_level`, `select_media_policy`,
  `select_media_report_admission`.

The production capability list at `RV/describe.rs:16–52` advertises neither
group, and its current answer has no media kinds advertisement. The eventual
advertising change must serve both groups, remove both pending entries, run
all seventeen pending cases, and assert the exact top-level kinds list directly,
as the specification requires. This inventory does not advertise either group.
