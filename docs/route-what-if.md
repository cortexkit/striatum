# Operator selection what-if

`route.what_if` is a **query on the `prefrontal-routing` module's own management
surface**, not an addition to the public `routing/v1` consumer contract. It runs
the v1 selection pipeline without owning the consumer or recording a selection.
It does not deploy, change settings, run a model, or reserve an account.

## Authority and invocation

The daemon-attested route principal must be `direct` (the local operator) or
`reserved:callosum` (the phone operator), exactly as for
`route.clear_credential_fences`. All other principals, including ordinary
unverified clients and other reserved modules, receive `not_permitted`. A JSON
parameter cannot grant operator authority.

Send the following management request to module `prefrontal-routing` on an
operator route. For example, the head session can use its `AlfonsoCoreClient`
configured with `moduleId: "prefrontal-routing"` to call `route.what_if` with
these params. A plain client script that the daemon labels `unverified` cannot
make this call; it needs a daemon-attested `direct` route instead.

Today's policy:

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

As if Opus were not pin-only for masons:

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

As if Broca published a proposed `routing/v1` runner slot, including the head
picker under that declaration:

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
            {"auth_method": "apikey", "wire_api": "anthropic.messages"},
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

## Parameters

* `consumer`: a registered consumer name, including a registered dotted name.
* `iqs`: 1–101 finite numbers in 0–100, evaluated in supplied order; fractional
  demand is retained.
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
  `harness`, as in `route.head_choices`. Requests the same picker response under
  the call's policy and runner declaration overrides, alongside the IQ sweep.

Consumer policy, global exclusions, configured service tiers, grades, pricing,
runner availability and account eligibility all remain in force. As with
`route.select`, an operator-configured consumer IQ/EQ overrides request demand;
the returned `bar` is the effective demand bar, not an independently calculated
IQ scale. This op accepts no project/workspace context, pin, affinity, task
reference, caller exclusion or concurrency-slot injection. A project pin supplied
by the operator route's bound scope still applies, just as it does to `route.select`.

## Result

The management response's `result` contains `consumer`, `results`, `snapshot_ms`
and `elapsed_ms`, plus `head_choices` when requested (the unchanged
`route.head_choices` response shape). Each `results` row contains:

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

A refused row has `pick: null` and `refusal: {code, detail}`; the other fields
remain present. If selection stops before model admission (for example no
runner is available), bar/frontier objects and candidate arrays can be empty.
Malformed requests and unauthorized callers are top-level errors, not rows.

## Isolation

The op holds live configuration snapshots for the sweep and overlays policy in
memory only. Runner declaration overrides are applied to a call-local catalog
snapshot shared by the sweep and optional head picker; no live catalog entry is
changed. It uses the same preparation, scorer, selection ordering and variant
policy as `route.select`, returning before decision persistence or pool-key
acquisition. It also skips the counters of routing calls by method, request shape
and principal.

Account preparation normally reconciles stale eligibility/fences and expired
continuations. The what-if reconciles only an isolated in-memory projection of
the account evidence tables, copied once through the store's read-only pooled
transaction. It copies neither decision nor usage history, takes no live write
lock, creates no lease file, reserves no probe, and never copies anything back.
`snapshot_ms` measures configuration capture and evidence copying; `elapsed_ms`
measures the whole call so the operator can check latency against the production
database's current account-evidence volume. Separate
calls may see different live quota, catalog or configuration refreshes.
