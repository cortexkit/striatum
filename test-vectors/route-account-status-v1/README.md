# route.account_status v1

Producer: **routing** (`striatum`). Consumer: **the worker runner**, through the
same runner management route used for `route.account_for`.

Each JSON file contains a wire request and reply. The examples use routing's
default allocator policy, account selection enabled, and a vault-listed
Anthropic account. The source reading was observed at `2026-09-29T11:59:00Z`.
`warm_ok.json` and `within_reserve.json` are evaluated at `12:00:00Z`;
`reading_stale.json` is evaluated at `12:19:00Z`, exactly the cold-placement
excluded-age boundary. The reserve example spends 99.5% of the five-hour
window, leaving less than routing's 1% reserve.

The request takes the producer's `apiProvider` (routing's vendor id) and the
vault account id, not the feed's producer name (for example `openai`, not
`codex`). The query
reads only routing's in-memory quota snapshot: it does not poll, refresh,
reserve quota, write shadow decisions, or touch the store. The worker runner must stop
warming whenever `warm_ok` is false; it must not replicate reserve thresholds.

`reason` is a tagged object with a closed `code` vocabulary:

- `headroom`
- `within_reserve`, with `window` naming the limiting window's `kind`
- `reading_stale`
- `reading_missing`
- `account_unknown`
- `account_selection_disabled`

`observed_at` preserves the producer's source timestamp. `age_band` is routing's
`full`, `discounted`, or `excluded` band (null if no valid timestamp is known).
Freshness uses the allocator's cold-placement rules, including clock skew and
degradation disclosures; a discounted but usable reading can allow warming.
Absent/tombstoned readings or missing/invalid windows fail closed. Insula-only
accounts that the vault does not list are unknown. When disabled, warming is
denied even if shadow mode is on.

Window `kind` preserves the producer's `windowKind` when present (including
the outer extra-window field); otherwise general windows use `five_hour` or
`weekly` by duration, and extra windows use their producer id (here `Fable`).
An unnamed general window of unknown duration uses `reported`. Resets and
durations remain null when unreported. General periods come from the reading,
not a fixed provider table: a Codex account may report only a weekly window.
For pooled accounts, all named pools are checked and misleading aggregate
slots are ignored, using the allocator's pool topology and inferred durations.
All applicable reported windows must leave
strictly more headroom than the allocator reserve: known general windows use
`reserve_five_hour` / `reserve_seven_day`, mapped scoped windows use
`reserve_scoped`, and other producer-reported windows use the seven-day reserve.
This read is advisory, not an admission or an account authorization grant.

The routing module's `account_status_*` unit tests exercise these examples.
