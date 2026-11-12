#!/usr/bin/env python3
"""Token volume per mason decision, for the token-weighted quota estimate.

Joins model_route_decisions -> manager_task (routing_json.decisionID) ->
OpenCode session messages and sums the assistant messages' recorded tokens.
Only OpenCode-hosted masons have an OpenCode session; Broca and Pi masons and
every explore decision have no per-decision token record, so the report
weights those by decision count.
"""

import json
import os
import sqlite3
import sys

HOME = os.path.expanduser("~")
CORE = f"{HOME}/.local/share/cortexkit/prefrontal-core/store.db"
OPENCODE = f"{HOME}/.local/share/opencode/opencode.db"
DECISIONS = sys.argv[1] if len(sys.argv) > 1 else "/tmp/overq/decisions.jsonl"
OUT = sys.argv[2] if len(sys.argv) > 2 else "/tmp/overq/mason_tokens.json"


def ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


core = ro(CORE)
oc = ro(OPENCODE)
out = {}
for line in open(DECISIONS):
    d = json.loads(line)
    bg = d.get("mason_task")
    if not bg:
        continue
    row = core.execute("SELECT session_id FROM manager_task WHERE background_task_id = ?", (bg,)).fetchone()
    if not row or not row[0]:
        continue
    total = {"input": 0, "output": 0, "reasoning": 0, "cache_read": 0, "cache_write": 0, "messages": 0}
    for (data,) in oc.execute(
        "SELECT data FROM message WHERE session_id = ? AND json_extract(data,'$.role') = 'assistant'",
        (row[0],),
    ):
        t = (json.loads(data).get("tokens") or {})
        cache = t.get("cache") or {}
        total["input"] += t.get("input") or 0
        total["output"] += t.get("output") or 0
        total["reasoning"] += t.get("reasoning") or 0
        total["cache_read"] += cache.get("read") or 0
        total["cache_write"] += cache.get("write") or 0
        total["messages"] += 1
    if total["messages"]:
        out[d["id"]] = total
json.dump(out, open(OUT, "w"))
print(f"{len(out)} mason decisions with OpenCode token totals -> {OUT}", file=sys.stderr)
