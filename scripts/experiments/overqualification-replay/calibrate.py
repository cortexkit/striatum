#!/usr/bin/env python3
"""Fit the per-revision operator fence the decision rows do not record in full.

A decision row lists at most twelve rejections inline, so most of a revision's
operator exclusions survive only as a count. replay.rs starts from today's
fence and re-admits every route that won under the revision. This script then
looks for routes the baseline replay picks INSTEAD of the recorded winner while
never having won a single decision under that revision: such a route was
fenced (or not served) then, or it would have won. Each round adds those routes
to the revision's exclusions and replays the baseline again, until nothing
changes. The fitted list is printed and written into world.json so the report
can show it.
"""

import json
import subprocess
import sys
from collections import Counter

BIN = sys.argv[1]
STORE, DECISIONS, WORLD, OUT = "/tmp/overq/store.db", "/tmp/overq/decisions.jsonl", "/tmp/overq/world.json", "/tmp/overq/baseline.jsonl"
MIN_COUNT = 2

policy = {}
for line in open(DECISIONS):
    d = json.loads(line)
    if d.get("kind") == "single":
        policy[d["id"]] = d.get("policy")

world = json.load(open(WORLD))
world.setdefault("epoch_excludes", {})
for round_no in range(1, 9):
    json.dump(world, open(WORLD, "w"), indent=1)
    subprocess.run([BIN, STORE, DECISIONS, WORLD, OUT], check=True, env={"OVERQ_BASELINE_ONLY": "1"})
    rows = [json.loads(l) for l in open(OUT)]
    ok = sum(1 for r in rows if r.get("baseline") == r["recorded"])
    wrong = Counter(
        (policy[r["id"]], r["baseline"])
        for r in rows
        if r.get("baseline") and r["baseline"] != r["recorded"] and policy.get(r["id"])
        and r["baseline"] not in world["epoch_winners"].get(policy[r["id"]], [])
    )
    added = [(p, k, n) for (p, k), n in wrong.items() if n >= MIN_COUNT]
    print(f"round {round_no}: reproduced {ok}/{len(rows)}; adding {len(added)}", file=sys.stderr)
    for p, k, n in sorted(added):
        print(f"  {p[:15]} {k} ({n})", file=sys.stderr)
        world["epoch_excludes"].setdefault(p, []).append(k)
    if not added:
        break
json.dump(world, open(WORLD, "w"), indent=1)
