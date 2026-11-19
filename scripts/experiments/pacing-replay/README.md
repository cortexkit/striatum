# Replay expiring quota pacing

Compare `max` and `expiry` with today's catalog, task profiles, settings and
credentials over historical quota states. `max` takes the highest weekly or
short-window pressure. `expiry` starts from the weekly pressure and lets a
short-window pool raise it only when less than 25% of its quota remains.
This is a weighted counterfactual
sweep, not a re-ranking of the historical decisions' candidate costs. The
original score writer records the winner's objective but only up to four
alternative names, not their costs (`model_router.rs`, `SelectedRouteScore`
and the `also_adequate` construction). Even full-detail records have this gap.

Requires Python 3.10+ and a running operator build with `route.what_if` quota
overrides. The Python commands neither contact that service nor write the
source databases. Run from the repository root:

```sh
python3 scripts/experiments/pacing-replay/run.py --days 7 --out /path/to/private/pacing
# Forward each requests.jsonl record through your operator what-if client.
python3 scripts/experiments/pacing-replay/run.py --out /path/to/private/pacing \
    --results /path/to/private/pacing/results.jsonl
```

Preparation opens `~/.local/share/cortexkit/striatum/{store,history}.db` with
SQLite URI `mode=ro`. Override paths with `PACING_ROUTING_STORE` and
`PACING_HISTORY_STORE`; pin the exclusive Unix-ms end with
`PACING_WINDOW_END_MS` (default: now). Use a fresh output directory. The
readers hold separate per-file SQLite snapshots, not a transaction across both
files. Treat output as private: it includes account and credential **IDs**, not
credentials. The script does not overwrite artifacts.

- `audit.md` shows score/detail retention, reconstructed-vs-recorded winner
  pressure, and quota coverage by account and UTC date. Read it before
  interpreting the sweep. `manifest.json` keeps mismatch examples, per-plan
  slot pressures, daily rate observations and the traffic-weighted cell map.
- `requests.jsonl` lines are `{ "id": "pacing-000000", "method": "route.what_if",
  "params": { ... } }`. Pass **only** the method and params to the operator
  client. Keep the `id` in the output envelope. Run the members of each pair
  together so live config, task profile and catalog refreshes are less likely
  to intervene.
- `results.jsonl` uses `{ "id": "pacing-000000", "result": { "consumer": ...,
  "results": [...] } }`, where `result` is the operation result itself, not a
  nested response envelope. On top-level refusal use `{ "id": "pacing-000000",
  "error": ... }`. Order is irrelevant; duplicate and unexpected IDs are
  errors. Missing rows and refusals do not count as unchanged picks. Analysis
  needs only the manifest and response file, not either source store.
- `sweep.md` and `sweep.json` report weighted changes by consumer, level band
  and model/plan transition, banked-credit sensitivity, and the wall proxy.

## Evidence and limits

- Every two-hour slot starts at the interval's left edge. Only successful
  single-model decisions with a recorded consumer and a numeric reasoning
  level (`requirements_json.iq`) supply traffic weights. `prefrontal-core.*`
  and `prefrontal.*` are folded together. For each
  tracked window the latest observation at or before the evaluation time is
  used; windows from different producer fetches are not spliced into the same
  entry. Freshness and stale-reset rules mirror `subscription_economics.rs`
  (`account` and `build`). Today’s roster and reading checkpoints identify
  the producer lanes, tiers and extra-window titles; historical identities
  and scope metadata not in those checkpoints can be missing. A windowless
  or missing snapshot is unknown, not an idle account. The quota log defaults
  to three days of retention; empty slots are counted and not replayed.
- `quota_poll.rs` and `quota_extract.rs` show what the log drops: saved-credit
  expiry and kind, spend balances, window models and in some layouts whole
  windows. We do not substitute current values for these historical inputs.
  In slots with positive banked counts, one case uses no banked resets and a
  second assumes the aggregate saved counts are eligible as pricing capacity;
  each runs both pacing rules. The report exposes disagreements (not just
  one optimistic figure). These cases are sensitivity estimates, not bounds
  on real account admission.
- The audit ports `subscription.rs` pressure, spendable points, and duration
  pooling. It subtracts the recorded keep-free margin (the weekly percentage
  points reserved on each account for use outside worker routing), skips applied or
  undisclosed banked resets, and reports pressure matches at 1% relative or
  0.0001 absolute tolerance. It also checks available points separately and
  prints diagnosis counts: different points suggest a quota/account mismatch;
  matching points with different pressure suggest a timing or topology issue.
  A short window can be priced at the economics-cache refresh rather than
  decision time (`subscription_economics.rs`, `overlay_pools`), so an as-of
  reconstruction need not match that cached value. Check mismatch examples
  and coverage before treating the sweep as representative.
- Plan moves use the exact returned subscription plan name when the priced
  tier identifies one, and provider/tier otherwise. The wall proxy groups by
  provider/tier (the account checkpoints do not retain the exact plan names
  that subscription pricing joins from the value report) and weekly reset
  timestamp. It fits the median and
  shows the min/max of observed weekly percentage-point rises per served
  admission on each account-day. It deduplicates admission IDs, uses only
  within-reset intervals, and skips account-days where several weekly meters
  cannot be assigned the same admission count. It sums **net** shifts against
  the observed baseline and distributes them across known reset cohorts by
  headroom, not by a simulated account allocator. Reported first walls mean
  the first sampled slot whose proxy headroom reached zero; they do not
  estimate within-slot timing or simulate the routing response to an early
  wall. Missing rates and unmatched cohorts stay unknown. Today's runner
  availability, consumer policy, fixed-route preferences, account restrictions
  and task profiles can all differ from those of the recorded decisions.

For a synthetic fixture with banked sensitivity, normalized consumers and a
known pressure mismatch, run:

```sh
python3 -B -m unittest discover -s scripts/experiments/pacing-replay -v
```
