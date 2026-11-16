# Local RoutingConsumer test bodies

Source revision: `428d689f6330b53ae2e23bfb39f68d179db4782b` in this worktree.
These JSON params are transcribed from checked-in tests, not captured traffic.
Undefined JavaScript members are omitted as they are by `JSON.stringify`.

| Body | Source in `packages/opencode/src/features/model-routing/routing-consumer.test.ts` |
|---|---|
| `route.model_facts-1.json` | lines 38–70, durable catalog wire/cache tests |
| `route.select-1.json` | lines 72–117, exact asserted JSON string |
| `route.select-2.json` | lines 119–125, head-pin forwarding test and the current select encoder |
| `route.resolve_policy-1.json` | lines 165–185, asserted `core.calls` |
| `route.record_usage-1.json` | lines 187–214, asserted params |
| `route.record_usage-2.json` | lines 216–250, first (`primary`) account-key case |
| `route.record_cooldown-1.json` | lines 260–269, caller input passed through the current cooldown encoder |
| `route.set_decision_outcome-1.json` | lines 271–277, real module rejection test |

The test imports the re-export in `packages/opencode/.../routing-consumer.ts`,
which points to `packages/core/src/features/model-routing/routing-consumer.ts`.
The latter's methods construct these routing requests at lines 189–439. It
contains no `route.explain` call. `manager.concurrency_slots` belongs to core,
not routing, so it is not a routing corpus operation.

The implementation also exposes `route.clear_failure_backoff`. The plugin's test suite
has no test that asserts or captures a request body for that method. It remains an
explicit blocked method rather than an invented example. The fixture uses a
synthetic core-owned route; it does not document which principal the live plugin
used or which session and project identity fields it sent.
