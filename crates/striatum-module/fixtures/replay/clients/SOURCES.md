# Routing client request bodies (exact params)

Collected 5 November 2026 for the routing contract 0.7.1 recording. Each file is
the exact `params` JSON a client sent to one legacy routing operation. Client
names, paths, principals and module ids below are fixture or protocol
identifiers, not operational ownership records.

| Client or fixture | Op | File | Notes |
|---|---|---|---|
| `occipital` | `route.catalog_targets` | `occipital/route.catalog_targets-1.json` | The display client's only routing call. The whole request is `{"method":"route.catalog_targets","params":{}}`, at most once per 30-minute name-cache refresh, or every 60 seconds while the alias map cannot be read. It never calls `head_choices` or `explain`. |
| `magic-context` | `route.select` | `magic-context/route.select-1.json` | First model for a summary run, printed from build `88fe5eff`'s own encoder with synthetic ids. `runners` is the session's attested runner (`thalamus-harness`, `thalamus-pi` or `broca`). Unscoped route on the daemon's management surface, with bind identity `magic-context` / `routing`. |
| `magic-context` | `route.select` | `magic-context/route.select-2.json` | Fallback after a failed model (up to two per run). `exclude` holds `route_key()` values in `provider/model` form for tried routes; the shown value is a stand-in and the shape is exact. |
| `magic-context` | `route.report` | `magic-context/route.report-1.json` | Report with usage. `input_tokens` is prompt tokens minus cache reads and writes; unreported usage fields are omitted. One report per selected decision; a failed report is logged and dropped, never retried. |
| `magic-context` | `route.report` | `magic-context/route.report-2.json` | `failed` with no usage because no step reported usage. |
| `magic-context` | `route.report` | `magic-context/route.report-3.json` | `cancelled` with no usage because the prompt did not fit the model's window and was never sent. |
| `ckdesk` (desktop app) | `route.head_choices` | `ckdesk/route.head_choices-1.json` | Not live when collected: the encoder existed but no screen called it. The request has one key whose value is the selected agent session's runner module (`thalamus-harness`, `thalamus-pi` or `broca`). The desktop sends nothing else to routing. |
| `thalamus-harness`, `thalamus-pi` | `route.account_for` | `thalamus/route.account_for-1.json` | Admit with optional hold cap and caller decision. Both token-relay modules run the same relay code; only the attesting module differs. Printed through a recording stand-in transport with synthetic ids and values and exact keys and types. No `route.report` was sent. |
| `thalamus-harness`, `thalamus-pi` | `route.account_for` | `thalamus/route.account_for-2.json` | Plain admit, with no cap or decision. |
| `thalamus-harness`, `thalamus-pi` | `route.account_observe` | `thalamus/route.account_observe-1.json` | Kind `admission_started`, on the first served report of an admission. |
| `thalamus-harness`, `thalamus-pi` | `route.account_observe` | `thalamus/route.account_observe-2.json` | Kind `attempt`. Header names are lowercased; `upstream_origin` is present only for Anthropic. Headers are whatever the auth plugin reports. |
| `thalamus-harness`, `thalamus-pi` | `route.account_observe` | `thalamus/route.account_observe-3.json` | Kind `account_limit_exhausted`. `pool`, `resets_at_ms` and `resets_at_source` are optional; `limit_window` is `five_hour` or `seven_day`. |
| `thalamus-harness`, `thalamus-pi` | `route.account_observe` | `thalamus/route.account_observe-4.json` | Kind `run_finished`. `event_id` is `<admission_id>:<sequence>:<kind>`; sequence is the auth plugin report number, or `end` for `run_finished`. |
| `broca` | `route.account_for`, `route.account_observe` | `broca/` (8 files + `SHA256SUMS`) | Produced by the worker runner's own builders (`account_for_params`, `admission_started_body`, `attempt_body`, `run_finished_body`, `scoped_not_found_body`) with synthetic values. Files are pretty-printed; compact wire JSON retains the same key order. `account_for` always uses mode `admit`: 01 plain, 02 every optional member. `account_observe` covers 03 `admission_started`, 04 `attempt` with rate-limit headers and `view_digest`, 05 `attempt` without upstream, 06 `account_limit_exhausted`, 07 `run_finished`, and 08 `scoped_not_found`. The worker runner never sends `route.report`, `route.select` or `route.head_choices`. |
