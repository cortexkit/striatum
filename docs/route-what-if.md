# Operator selection what-if

`route.what_if` is a **query on the `striatum` module's own management
surface**, not an addition to the public `routing/v1` consumer contract. It runs
the v1 selection pipeline without owning the consumer or recording a selection.
It does not deploy, change settings, run a model, or reserve an account.

## Authority and invocation

The daemon-attested route principal must be `direct` (the local operator) or
`reserved:callosum` (the phone operator), exactly as for
`route.clear_credential_fences`. All other principals, including ordinary
unverified clients and other reserved modules, receive `not_permitted`. A JSON
parameter cannot grant operator authority.

Send the following management request to module `striatum` on an
operator route. An operator client configured with `moduleId: "striatum"` can
call `route.what_if` with these params. A plain client script that the daemon
labels `unverified` cannot make this call; it needs a daemon-attested `direct`
route instead.

Example policy preview:

```json
{
  "method": "route.what_if",
  "params": {
    "consumer": "mason",
    "iqs": [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100],
    "runner": "broca"
  }
}
```

As if Opus were not pin-only for the `mason` consumer:

```json
{
  "method": "route.what_if",
  "params": {
    "consumer": "mason",
    "iqs": [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100],
    "runner": "broca",
    "policy_patch": {
      "models": {
        "anthropic/claude-opus-5-5": {"pin_only_for": []}
      }
    }
  }
}
```

The empty array replaces that model's `pin_only_for` array for this call. It does
not clear a separate `pin_only: true`, `not_for`, provider exclusion or account-level quota, cooldown or credential restriction. If the live model uses `pin_only: true`, also patch `"pin_only": false`.

As if the worker runner published a proposed `routing/v1` runner slot, including
the head picker under that declaration:

```json
{
  "method": "route.what_if",
  "params": {
    "consumer": "mason",
    "iqs": [0, 50, 100],
    "runner": "broca",
    "runner_declarations": {
      "broca": {
        "runner": {
          "follows_account_choice": true,
          "pairs": [
            {"auth_method": "apikey", "wire_api": "anthropic.messages", "requires_wire_api": true},
            {"auth_method": "oauth", "wire_api": "anthropic.messages"}
          ]
        }
      }
    },
    "head_choices": {"runner_module": "broca"}
  }
}
```

The example deliberately admits only two pairs. To evaluate a runner's actual
proposal, use its entire `role_declarations["routing/v1"]` object as the value
under its module name, not the surrounding catalog entry or just the inner
`runner` object. Omit both new members for the live-policy baseline.

`requires_wire_api` on a runner pair controls whether that pair needs a served
`wire.api` endpoint in the model catalog. A pair's explicit value takes precedence
over the runner-level value; if neither is supplied, the requirement is false.
Members on `harness_limits` pairs do not change this requirement: harness limits
only narrow the runner's admitted pairs.

## Parameters

* `consumer`: a registered consumer name, including a registered dotted name.
* `iqs`: 1–101 finite numbers in 0–100, evaluated in supplied order; fractional
  demand is retained. It may be omitted only when `panel` is present, and then
  `results` is empty.
* `eq`: optional finite demand in 0–100. If absent, normal consumer defaults apply.
* `runner`: optional `"broca"`; omission uses the normal eligible runner set.
* `policy_patch.models`: optional map from provider/model keys to any combination
  of `pin_only_for`, `pin_only`, `not_for`, `only_for`. Arrays replace arrays;
  `false` replaces `true`; omitted fields retain live settings. Other policy
  fields, price/grade edits and unknown request members are rejected.
* `runner_declarations`: optional map from module names to complete `routing/v1`
  slot objects containing a `runner` declaration. Each slot is validated by the
  same role-routing parser used for catalog slots; invalid slots return its
  error. A supplied runner declaration replaces that module's catalog declaration
  for this call, or supplies one if the module has none. It does not register an
  unavailable runner or change operator settings, accounts or credentials.
* `head_choices`: optional object with required `runner_module` and optional
  `harness`, as in `route.head_choices`. Runs the picker under the call's policy
  and runner declaration overrides, alongside the IQ sweep.
* `workloads`: optional map from dotted consumer names to workload names. The
  most specific matching entry replaces the consumer's configured workload for
  this call only. An unknown workload is treated as none. Head picks do not use
  a workload.
* `pricing_patch`: optional object with only `overqualification` and
  `banked_resets`. `overqualification` is the mode string `"additive"` or
  `"cost_scaled"`; `banked_resets` maps provider ids to `"emergency"` or
  `"capacity"`. Omitted members retain the live setting. Unknown members,
  invalid provider ids and unknown modes are refused before snapshotting.
  This patch applies only to the evaluation's cloned settings, for both IQ
  sweeps and panel dry runs, against the same isolated account snapshot. It
  records no decisions, admissions or usage and changes neither live settings
  files nor the live settings cache. A mode-only `cost_scaled` preview uses
  the fixed core curve even if live additive settings have custom tuning;
  it does not reinterpret those tuning values. Persistent settings combining
  `cost_scaled` with tuning members are refused instead.
* `free_before_reset_preview`: optional boolean, default `false`. Add a
  comparison price and pick to the IQ sweep under the expiring-quota rule
  below. This does not change the ordinary pick or the panel dry run.
* `panel`: optional object `{class, package_tokens?, runners?, seed?}` that
  dry-runs `route.select_panel` for `consumer` (see [Panel dry run](#panel-dry-run)).
  `class` is one class name or a list of 1–16 names; each is seated separately.
  The members are passed to `route.select_panel`'s own request parser, so a
  malformed member, and a class routing has no policy for, is refused with the
  same error `route.select_panel` returns. `runners` defaults to `[runner]` when
  the top-level `runner` is given. Unknown members are rejected.

Consumer policy, global exclusions, configured service tiers, grades, pricing
(except the explicit request-local `pricing_patch` and comparison preview),
runner availability and account eligibility all remain in force. As with
`route.select`, an operator-configured consumer IQ/EQ overrides request demand;
the returned `bar` is the effective demand bar, not an independently calculated
IQ scale. This op accepts no project/workspace context, pin, affinity, task
reference, caller exclusion or concurrency-slot injection. A project pin supplied
by the operator route's bound scope still applies, just as it does to `route.select`.

## Result

The management response's `result` contains `consumer`, `results`, `anchor_version`, `snapshot_ms`,
`elapsed_ms` and `concurrency`, plus `head_choices` when requested and `panels`
when `panel` is supplied. The public picker shape is unchanged; in what-if,
each head model adds the diagnostic fields `effort_source`, `variant` and
`grade` to that shape. Each `results` row contains:

```json
{
  "iq": 50,
  "bar": {"elo": 1500, "eq": 0},
  "top_routable": {"elo": 1900, "eq": 1700},
  "adequate": [{
    "route": "provider/model",
    "fit": {"elo": {"requirement": 50, "fact": 1800, "target": 1500, "fit": 1}},
    "adequacy": 1,
    "cost_usd": 0.012,
    "normalized_cost": 0.08
  }],
  "pick": {"route": "provider/model", "variant": null, "cost_usd": 0.012},
  "excluded": [{"route": "provider/other-model", "reason": "below_floor"}]
}
```

`anchor_version` identifies the evaluation's scoring formula: `5` for additive
(including the default) and `7` for cost-scaled, whether selected by persistent
settings or this call's `pricing_patch`. It describes both the sweep and panels;
no decision is written. A banked-reset patch changes the pressure input rather
than score semantics, so it does not change the anchor by itself.

Numbers above are illustrative. `fit` is the scorer's metric audit, with no
entry for a zero-demand metric. `adequate` includes every policy-allowed,
account-eligible route clearing the scorer's adequacy floor, not only the four
alternatives retained in normal decision records; its order is not a ranking.
`excluded` includes hard-admission reasons, below-floor routes, and the full
settings rule explaining a `pin_only`, `pin_only_for`, `not_for` or `only_for`
rejection. Below-floor routes may still be chosen by routing's normal best-effort
fallback when no route clears the floor.

`cost_usd` is the scorer's estimated economic **dollars per task**, including
subscription value (the configured dollar value of consumed subscription quota)
and pressure (the surcharge for scarce remaining quota), when task-profile pricing
is available. It is
`null` for unknown prices or cold-start message/million-output-token pricing:
those units must not be presented as a task estimate. `normalized_cost` remains
the scorer's actual comparison cost, before quota and overqualification terms.
Each adequate candidate's `task_pricing` contains the arithmetic behind that
estimate. As in live decisions, subscription entries include `weeklyPressure`
and, when a short-window pool has less than 25% of its quota left,
`shortWindowPressure`. `pressure` is the greater of those values, or the weekly
value alone when no short-window pool meets that threshold.

A refused row has `pick: null` and `refusal: {code, detail}`; the other fields
remain present. If selection stops before model admission (for example no
runner is available), bar/frontier objects and candidate arrays can be empty.
Malformed requests and unauthorized callers are top-level errors, not rows.

### Quota free before reset

With `free_before_reset_preview: true`, each IQ row also carries
`free_before_reset_pick`. Each adequate subscription candidate adds a
`free_before_reset_preview` object; its ordinary `task_pricing` is unchanged.

The object reports `window` (the producer's slot or named scope),
`window_minutes`, `resets_at_ms`, `pace_points_per_hour`, `hours_to_reset`,
`projected_leftover_points`, and `free_before_reset`. A window is free only
within 24 hours of its reset, with more than five percentage points projected
to expire and at least one hour of observations from the last six hours.
Unavailable observations appear as `null`. A pool is free only if every
spendable member qualifies. The preview breaks zero-cost ties by earliest
qualifying reset, then list price.

`binding_short_windows` explains the short-window pools under 25% remaining.
They can keep the plan's `pressure` positive even when `weekly_pressure` is
zero; `short_window_pressure` shows their contribution. `final_cost_usd` uses
the same pricing unit as the candidate's ordinary `task_pricing`.

### Account concurrency snapshot

Both model sweeps and media previews include `concurrency`, independently of
the enforcement switch:

```json
{
  "concurrency": {
    "enforcement_enabled": false,
    "accounts": [{
      "provider": "example-provider",
      "account_id": "vault-account-id",
      "cap_counts": [
        {"scope": "account", "model": null, "holds_in_flight": 2, "effective_cap": 6},
        {"scope": "account_model", "model": "special-model", "holds_in_flight": 1, "effective_cap": 2}
      ]
    }]
  }
}
```

`enforcement_enabled` is the captured `concurrency.enforcement_enabled` setting
(default `false`). The report shows counts and caps, not a refusal; it never
refuses a caller for concurrency.

With enforcement on, a count that live waiters wait on also carries
`waiters`, ordered by `order`:

```json
{"scope": "account", "model": null, "holds_in_flight": 0, "effective_cap": 1,
 "waiters": [{"class": "blocking", "order": 3}, {"class": "worker", "order": 5}]}
```

A waiter is a conversation that `account_for` refused `concurrency_full`.
`class` is the priority class it was refused with (`interactive`, `blocking`,
`worker` or `background`) and `order` the writer's counter for new waiters,
which ranks waiters of one class. A waiter is listed on every count of its
latest refusal and lasts until it takes a slot on one of them or until 30 s
after the retry time it was given. A count no one waits on has no `waiters`
member, and with enforcement off none is listed. Waiters are held in memory
only, so a routing restart forgets them.

`accounts` is ordered by `provider`, then `account_id`. It includes known cap
accounts, listed credential accounts and accounts retained by the in-flight
ledger, including uncapped accounts. Each account's `cap_counts` starts with
the shared `account` count (`model: null`), followed by its `account_model`
counts in model-id order. Per-model counts cover models in the catalog, live
ledger entries and model overrides whose effective scope is `account_model`.
Models sharing the account count are not duplicated as per-model counts.

`effective_cap` is a positive integer, or `null` when no cap applies to that
count. An account whose default scope is `account_model` has an uncapped
shared count; each model's limit is reported on its own count. A model override
is counted apart from a shared cap even when that override lifts its limit.
`holds_in_flight` is exactly the ledger count compared against that cap:
started runs, unstarted reservations and live continuation-offer reservations,
grouped and lapsed by the ledger's rules. Each row contributes to only its
effective cap count, never both the shared and a per-model count.

The report retains an immutable published account-state snapshot (including
its in-flight ledger and waiters) and a resolved-cap snapshot before the
sweep. Counts and waiters are evaluated at one captured routing-clock instant,
not reloaded from SQL or recalculated from the eligibility projection.
Reporting changes neither the published ledger nor cap policy.

## Panel dry run

Panels are not seated by single select. `route.select_panel` ranks top-K seats
by the class policy's `by` order (IQ, EQ or cost), then fills extra seats by
`extra_by` (random, IQ, EQ or cost), one route per family and per canonical
model. The class floor (a demand bar relative to the top routable route) gates
every automatic top seat and extra seat, regardless of ranking order. Each
scored route is then checked for runner and account admission. Class policies
come from operator `panels` settings, or from the class's built-in default.
Audit, spec and diagnose default to floor 80; design_review, evaluate and
explain default to 70. The floor gates IQ admission; it does not
replace EQ or cost ordering. Lab uses the separate lab consumer and remains
floorless by default. An operator's complete class policy still takes precedence,
including an omitted or lower floor.

With `panel`, the what-if runs that same code path for each listed class,
under the call's `policy_patch` and `runner_declarations`, against the same
account snapshot as the IQ sweep. For example, as if two routes were not for
the `athena` consumer:

```json
{
  "method": "route.what_if",
  "params": {
    "consumer": "athena",
    "panel": {"class": ["audit", "spec", "diagnose"], "seed": "task-123"},
    "policy_patch": {
      "models": {
        "openai/gpt-5-5": {"not_for": ["athena"]},
        "google/gemini-3-pro": {"not_for": ["athena"]}
      }
    }
  }
}
```

`seed` makes the random extra seat deterministic, as it does for
`route.select_panel`. Without it the dry run uses the empty seed, which is what
`route.select_panel` draws with when a request carries none; each row reports
the seed used and `seed_source` (`request` or `default`). The same seed and
inputs seat the same routes as `route.select_panel` would.

The result gains `panels`, one row per class in request order:

```json
{
  "class": "audit",
  "policy": {"source": "settings", "top_k": 2, "by": "iq", "extra": 1,
             "extra_by": "random", "floor": 80, "width": 3},
  "seed": "task-123",
  "seed_source": "request",
  "seats": [{
    "slot": 0,
    "phase": "top",
    "route": "provider/model",
    "variant": "high",
    "family": "claude",
    "rank": 1,
    "cost_usd": null,
    "chosen_by": "top-2 seat ranked 1 by iq; clears class floor 80"
  }],
  "shortfall": null,
  "funnel": {"catalog": 12, "scored": 12, "operatorAdmitted": 10, "...": 0}
}
```

* `policy.source` is `settings` when operator settings name the class's policy
  and `default` for its built-in policy.
* `seats` are in slot order. `phase` is `top` or `extra`; `rank` is the scorer's
  position in that phase's order, absent for a random extra draw; `chosen_by`
  states the rule in words. `variant` and `cost_usd` are as for the IQ sweep's
  `pick`.
* `shortfall` is `null` when every seat of the policy's width was filled.
  Otherwise it is `route.select_panel`'s shortfall: `code` and a `detail` whose
  `causes` list each rejected candidate with its typed reason (for example
  `excluded_by_operator`, `family_duplicate`, `package_too_large`, or a final
  admission refusal such as `account_hold`).
* `funnel` is the scorer's candidate funnel, with camelCase keys: cumulative
  route counts at each admission gate, named in `gateOrder`.
* A class whose panel fails for another reason (for example routing has not yet
  loaded its first prices, refused as `routing_not_ready`) has empty `seats` and
  `refusal: {code, detail}`.

## Isolation

The op holds live configuration snapshots for the sweep and overlays policy in
memory only. Runner declaration overrides are applied to a call-local catalog
snapshot shared by the sweep and optional head picker; no live catalog entry is
changed. It uses the same preparation, scorer, selection ordering and variant
policy as `route.select`, returning before decision persistence or pool-key
acquisition. It also skips the counters of routing calls by method, request shape
and principal. A panel dry run writes no panel row, records no seat decision and
takes no account hold: each seat is checked by the same evaluation path as the
IQ sweep.

Account preparation normally reconciles stale eligibility/fences and expired
continuations. The what-if reconciles only an isolated in-memory projection of
the account evidence tables, copied once through the store's read-only pooled
transaction. It copies neither decision nor usage history, takes no live write
lock, creates no lease file, reserves no probe, and never copies anything back.
`snapshot_ms` measures configuration capture and evidence copying; `elapsed_ms`
measures the whole call so the operator can check latency against the production
database's current account-evidence volume. Separate
calls may see different live quota, catalog or configuration refreshes.
The concurrency report uses the separate immutable module-owned account-state
and cap snapshots described above, not SQL reads for counts.

## Compare dormant pricing

Both changes are dormant by default: `overqualification.mode` is `"additive"`
and `providers.<id>.banked_resets` is `"emergency"`. To compare live inputs
without switching live picks, send an ordinary sweep, then the same request
with the following patch (each call takes its own isolated snapshot):

```json
{
  "consumer": "mason",
  "iqs": [10,20,30,40,50,60,70,80,90,100],
  "pricing_patch": {
    "overqualification": "cost_scaled",
    "banked_resets": {"openai": "capacity"}
  }
}
```

Use `"sidekick"` for the second consumer. Compare picks and estimated task
costs, keeping in mind that separate calls can see newly refreshed live quota.
A banked capacity preview only changes pressure pricing: an account already
at its live quota wall is still unavailable until its owner applies a reset.
No saved reset is applied by this operation.
