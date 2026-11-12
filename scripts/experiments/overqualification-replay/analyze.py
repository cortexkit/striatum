#!/usr/bin/env python3
"""Aggregate the replay output into the tables of
reports/2026-10-02-overqualification-replay.md. Prints markdown to stdout."""

import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict

DECISIONS = "/tmp/overq/decisions.jsonl"
REPLAY = os.environ.get("OVERQ_REPLAY", "/tmp/overq/replay.jsonl")
# OVERQ_ALL=1: every decision is in the population (the today-fence replay, whose
# baseline is not expected to reproduce the recorded winner).
ALL = bool(os.environ.get("OVERQ_ALL"))
TOKENS = "/tmp/overq/mason_tokens.json"
HOME = os.path.expanduser("~")
CORE = f"{HOME}/.local/share/cortexkit/prefrontal-core/store.db"
FOCUS = sys.argv[1:] or []

decisions = {}
for line in open(DECISIONS):
    d = json.loads(line)
    decisions[d["id"]] = d
replay = [json.loads(l) for l in open(REPLAY)]
tokens = json.load(open(TOKENS)) if os.path.exists(TOKENS) else {}


def group(d):
    agent, harness, iq = d["agent"], d.get("harness"), d.get("iq", 0)
    if agent == "mason":
        return "mason"
    if agent != "explore":
        return agent
    if d.get("task_id"):
        return "explore iq 30, Broca, consult/spec/campaign gatherers"
    if iq == 50:
        return "explore iq 50, sidekick request route"
    if iq == 30 and harness in ("opencode", "pi"):
        return "explore iq 30, host (gather_context, knowhow)"
    if iq == 30:
        return "explore iq 30, Broca (core one-shots, sidekick research parts)"
    return "explore, other demand"


def band(iq):
    for hi, label in ((30, "<=30"), (50, "31-50"), (60, "51-60"), (70, "61-70"), (80, "71-80"), (90, "81-90")):
        if iq <= hi:
            return label
    return "91-100"


BANDS = ["<=30", "31-50", "51-60", "61-70", "71-80", "81-90", "91-100"]


def provider(route):
    return route.split("/")[0] if route else None


def short(route):
    return route.split("/", 1)[1] if route else "-"


def pct(a, b):
    return f"{100.0 * a / b:.1f}%" if b else "-"


rows = []
for r in replay:
    d = decisions[r["id"]]
    rows.append((r, d))

print("## Fidelity\n")
total = len(rows)
ok = [(r, d) for r, d in rows if (r.get("baseline") is not None if ALL else r.get("baseline") == r["recorded"])]
err = [(r, d) for r, d in rows if r.get("baseline") is None]
print(f"- single-route decisions replayed: {total}")
print(f"- baseline replay reproduces the recorded winner: {len(ok)} ({pct(len(ok), total)})")
print(f"- baseline replay found no route: {len(err)}")
mm = sum(r.get("reimpl_mismatch", 0) for r, _ in rows)
bad_base = sum(1 for r, _ in rows if r.get("baseline") and not r.get("reimpl_baseline_ok"))
print(f"- re-implemented ordering vs library: {mm} disagreements over {total} x 27 penalised runs; {bad_base} baseline disagreements\n")
print("| group | decisions | reproduced | rate |\n|---|---:|---:|---:|")
g_all, g_ok = Counter(group(d) for _, d in rows), Counter(group(d) for _, d in ok)
for g, n in g_all.most_common():
    print(f"| {g} | {n} | {g_ok[g]} | {pct(g_ok[g], n)} |")
print()
print("| policy revision | first decision (UTC) | decisions | reproduced | rate |\n|---|---|---:|---:|---:|")
pol = defaultdict(list)
for r, d in rows:
    pol[d.get("policy") or "-"].append((r, d))
import datetime as dt
for p, items in sorted(pol.items(), key=lambda kv: min(d["ts"] for _, d in kv[1])):
    first = dt.datetime.utcfromtimestamp(min(d["ts"] for _, d in items) / 1000).strftime("%m-%d %H:%M")
    k = sum(1 for r, _ in items if r.get("baseline") == r["recorded"])
    print(f"| {p[:15]} | {first} | {len(items)} | {k} | {pct(k, len(items))} |")
print()
miss = Counter((r["recorded"], r.get("baseline")) for r, d in rows if r.get("baseline") != r["recorded"])
print("Top recorded -> baseline-replay disagreements:\n\n| recorded | replay | n |\n|---|---|---:|")
for (a, b), n in miss.most_common(12):
    print(f"| {a} | {b} | {n} |")
print()

grid_keys = list(rows[0][0]["variants"].keys()) if rows else []
VARIANTS = ("pre", "post", "guard")


def changed(r, key, variant):
    v = r["variants"][key][variant]
    return v is not None and v != r["baseline"]


print(f"## Grid: decisions whose winner changes ({'all decisions' if ALL else 'reproduced decisions only'})\n")
print("Cells: changes under `pre` / `post` / `guard` (see the report for the three placements).\n")
groups = [g for g, _ in g_all.most_common()]
hdr = "| X | h | base | all | " + " | ".join(groups) + " | Opus wins kept at iq<=50 | Opus wins changed at iq>=70 |"
print(hdr)
print("|" + "---|" * (3) + "---:|" * (len(groups) + 3))
for key in grid_keys:
    x, h, b = key.split("|")
    cells = []
    for variant in ("pre", "post"):
        pass
    allc = "/".join(str(sum(1 for r, _ in ok if changed(r, key, v))) for v in VARIANTS)
    per = []
    for g in groups:
        per.append("/".join(str(sum(1 for r, d in ok if group(d) == g and changed(r, key, v))) for v in VARIANTS))
    opus_low = [r for r, d in ok if r["baseline"] == "anthropic/claude-opus-5-5" and d["iq"] <= 50]
    kept = "/".join(str(sum(1 for r in opus_low if not changed(r, key, v))) for v in VARIANTS)
    opus_hi = [r for r, d in ok if r["baseline"] == "anthropic/claude-opus-5-5" and d["iq"] >= 70]
    hi = "/".join(str(sum(1 for r in opus_hi if changed(r, key, v))) for v in VARIANTS)
    print(f"| {x} | {h} | {b} | {allc} | " + " | ".join(per) + f" | {kept} (of {len(opus_low)}) | {hi} (of {len(opus_hi)}) |")
print()

if not FOCUS:
    sys.exit(0)

# Failure history: mason settles over the 30 days before the window end, and
# explore decision outcomes inside the window.
core = sqlite3.connect(f"file:{CORE}?mode=ro", uri=True)
mason_fail = defaultdict(lambda: [0, 0])
for model_json, outcome, wq in core.execute(
    """SELECT t.model_json, ev.outcome, ev.work_quality FROM manager_task t
         JOIN manager_task_evaluation ev USING (background_task_id)
        WHERE t.agent='mason' AND t.time_created > ? AND ev.outcome IN ('finalized','rejected')""",
    (1790967123678 - 30 * 86_400_000,),
):
    m = json.loads(model_json or "{}")
    if not m.get("providerID"):
        continue
    key = f"{m['providerID']}/{m['modelID']}"
    mason_fail[key][0] += 1
    if outcome == "rejected" or (wq is not None and wq <= 70):
        mason_fail[key][1] += 1
explore_fail = defaultdict(lambda: [0, 0])
for r, d in rows:
    if d["agent"] == "explore" and d.get("outcome"):
        k = (group(d), d["winner"])
        explore_fail[k][0] += 1
        if d["outcome"] in ("error", "launch_failed", "session_error"):
            explore_fail[k][1] += 1

# The motivating window: the six hours before the window end.
SIX = 1790967123678 - 6 * 3_600_000
six = [(r, d) for r, d in rows if d["ts"] > SIX and r["recorded"] == "anthropic/claude-opus-5-5" and d["iq"] <= 50]
print(f"## Last six hours: recorded Opus wins at iq <= 50 ({len(six)}; baseline replay reproduces {sum(1 for r, _ in six if r.get('baseline') == r['recorded'])})\n")
print("| X | h | base | variant | moved off Opus | destinations |\n|---|---|---|---|---:|---|")
for key in FOCUS:
    x, h, b = key.split("|")
    for v in VARIANTS:
        dest = Counter((r["variants"][key][v] or "-") for r, _ in six if ALL or r.get("baseline") == r["recorded"])
        moved = sum(n for k, n in dest.items() if k != "anthropic/claude-opus-5-5")
        print(f"| {x} | {h} | {b} | {v} | {moved} | " + ", ".join(f"{short(k)} {n}" for k, n in dest.most_common()) + " |")
print()

def exact(r, d, route):
    s = next((x for x in r.get("shortlist", []) if x["k"] == route), None)
    return s is not None and s["iq"] >= d["iq"] - 1e-9 and (d["eq"] <= 0 or s["eqd"] >= d["eq"] - 1e-9)

for key in FOCUS:
    x, h, b = key.split("|")
    for variant in VARIANTS:
        print(f"## X={x}, h={h}, base={b}, `{variant}`\n")
        print("### Changes by group and demand band\n")
        print("| group | " + " | ".join(BANDS) + " |\n|---|" + "---:|" * len(BANDS))
        for g in groups:
            cells = []
            for bd in BANDS:
                n = sum(1 for r, d in ok if group(d) == g and band(d["iq"]) == bd)
                c = sum(1 for r, d in ok if group(d) == g and band(d["iq"]) == bd and changed(r, key, variant))
                cells.append(f"{c}/{n}" if n else "")
            print(f"| {g} | " + " | ".join(cells) + " |")
        print()
        print("### Transitions (baseline -> penalised)\n\n| group | from | to | n |\n|---|---|---|---:|")
        tr = Counter((group(d), r["baseline"], r["variants"][key][variant]) for r, d in ok if changed(r, key, variant))
        for (g, a, bb), n in tr.most_common(20):
            print(f"| {g} | {short(a)} | {short(bb)} | {n} |")
        print()
        print("### Model share (reproduced decisions)\n\n| route | baseline | penalised | delta |\n|---|---:|---:|---:|")
        base_c = Counter(r["baseline"] for r, _ in ok)
        new_c = Counter((r["variants"][key][variant] or r["baseline"]) for r, _ in ok)
        for route in sorted(set(base_c) | set(new_c), key=lambda k: -(abs(new_c[k] - base_c[k]))):
            if new_c[route] != base_c[route]:
                print(f"| {route} | {base_c[route]} | {new_c[route]} | {new_c[route] - base_c[route]:+d} |")
        print()
        print("### Quota shift\n")
        print("| provider | decisions baseline | decisions penalised | delta | mason tokens moved in (M) | mason tokens moved out (M) |\n|---|---:|---:|---:|---:|---:|")
        pb = Counter(provider(r["baseline"]) for r, _ in ok)
        pn = Counter(provider(r["variants"][key][variant] or r["baseline"]) for r, _ in ok)
        tin, tout = Counter(), Counter()
        for r, d in ok:
            if d["agent"] == "mason" and changed(r, key, variant) and r["id"] in tokens:
                t = tokens[r["id"]]
                vol = t["input"] + t["output"] + t["reasoning"] + t["cache_read"] + t["cache_write"]
                tin[provider(r["variants"][key][variant])] += vol
                tout[provider(r["baseline"])] += vol
        for p in sorted(set(pb) | set(pn), key=lambda k: -abs(pn[k] - pb[k])):
            print(f"| {p} | {pb[p]} | {pn[p]} | {pn[p] - pb[p]:+d} | {tin[p] / 1e6:.1f} | {tout[p] / 1e6:.1f} |")
        print()
        print("### Recent failures of the model the penalty picks\n")
        print("| group | new winner | changed decisions | that model's record for this workload |\n|---|---|---:|---|")
        nw = Counter((group(d), r["variants"][key][variant]) for r, d in ok if changed(r, key, variant))
        for (g, m), n in nw.most_common(20):
            if g == "mason":
                tot, fails = mason_fail.get(m, [0, 0])
                rec = f"mason settles 30d: {fails}/{tot} failed ({pct(fails, tot)})"
            else:
                tot, fails = explore_fail.get((g, m), [0, 0])
                rec = f"decision outcomes 7d: {fails}/{tot} errored ({pct(fails, tot)})"
            print(f"| {g} | {m} | {n} | {rec} |")
        print()
        # Mason decisions in the window whose own settle failed, split by whether
        # the penalty would have moved them.
        moved_fail = [(r, d) for r, d in ok if d["agent"] == "mason" and changed(r, key, variant)]
        f_known = [x for x in moved_fail if x[1].get("fail") is not None]
        print(f"Moved mason decisions: {len(moved_fail)}; with a settle: {len(f_known)}; of those failed on the baseline model: {sum(1 for _, d in f_known if d['fail'])}.\n")
        moved = [(r, d) for r, d in ok if changed(r, key, variant)]
        inexact = Counter(group(d) for r, d in moved if not exact(r, d, r["variants"][key][variant]))
        print(f"Moved decisions whose new route does NOT clear every demanded axis exactly (fit < 1 on iq or eq): {sum(inexact.values())} of {len(moved)} ({dict(inexact)}).\n")
