#!/usr/bin/env python3
"""Build the per-decision replay input for the over-qualification penalty study.

Reads (read-only):
  ~/.local/share/cortexkit/striatum/store.db        model_route_decisions
  ~/.local/share/cortexkit/prefrontal-core/store.db  manager_task, manager_task_evaluation

Writes one JSON object per full-detail decision of the window to
/tmp/overq/decisions.jsonl. Every field is copied or parsed from what the
decision row recorded; nothing here re-derives routing state. The Rust replay
(replay.rs) turns these into SelectModelRouteInput values.

Routing retention compacts a decision's scoring detail (the full candidate
list and the per-capacity quota signals) after two days, except for a 1-in-20
sample chosen by a hash of the decision id, which keeps full detail for the
whole 30-day decision window. Only full-detail rows (`detail_stripped_at IS
NULL`: the sample plus the last two days) are written, since the replay needs
their quota pressure. Compacted rows still feed the per-provider abundance
series, which reads only the explanation lines every row keeps. Each written
row says whether it is in the sample (`full_detail_sample`).
"""

import bisect
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict

HOME = os.path.expanduser("~")
ROUTING = os.environ.get("OVERQ_ROUTING_STORE", f"{HOME}/.local/share/cortexkit/striatum/store.db")
CORE = f"{HOME}/.local/share/cortexkit/prefrontal-core/store.db"
OUT = sys.argv[1] if len(sys.argv) > 1 else "/tmp/overq/decisions.jsonl"
# Pin OVERQ_WINDOW_END_MS so a rerun covers the same rows a report describes.
# The default window is the whole 30-day decision window, so the sample counts.
WINDOW_END_MS = int(os.environ.get("OVERQ_WINDOW_END_MS", str(int(time.time() * 1000))))
WINDOW_MS = int(os.environ.get("OVERQ_WINDOW_DAYS", "30")) * 86_400_000
# Routing's default full-detail sample: one decision in this many.
FULL_DETAIL_SAMPLE_ONE_IN = int(os.environ.get("OVERQ_SAMPLE_ONE_IN", "20"))
DEMAND_EXPONENT = 0.35
ABUNDANCE_MAX_AGE_MS = 2 * 3_600_000
WALL_REASONS = {"quota_exhausted", "cooldown_active", "backoff_active"}


def ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def decision_id_hash(decision_id):
    """FNV-1a 64-bit hash of the id's UTF-8 bytes, as routing computes it."""
    value = 0xCBF29CE484222325
    for byte in decision_id.encode("utf-8"):
        value = ((value ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return value


def in_full_detail_sample(decision_id, one_in=FULL_DETAIL_SAMPLE_ONE_IN):
    return one_in != 0 and decision_id_hash(decision_id) % one_in == 0


def jload(text, default=None):
    if text is None:
        return default
    try:
        return json.loads(text)
    except ValueError:
        return default


ABUNDANCE_RE = re.compile(r"^quota abundance ([0-9.]+) \(window ([^)]+)\)$")
POLICY_RE = re.compile(r"^policy sources: operator (sha256:[0-9a-f]+)")
WINDOW_PROVIDER = {"codex": "openai", "claude": "anthropic", "grok": "xai"}


def window_provider(window):
    # codexbar:<service>:<window>
    parts = window.split(":")
    return WINDOW_PROVIDER.get(parts[1]) if len(parts) >= 2 else None


def task_routing_rows(core, since, page_size=100):
    """Explicit offline full-payload reads, bounded to at most 100 rows per page."""
    if not 1 <= page_size <= 100:
        raise ValueError("archive_list_page_required")
    columns = {row[1] for row in core.execute("PRAGMA table_info(manager_task)")}
    reference = "t.routing_archive_sha256" if "routing_archive_sha256" in columns else "NULL"
    archive = None
    offset = 0
    owns_snapshot = not core.in_transaction
    if owns_snapshot:
        core.execute("BEGIN")
    try:
        while True:
            rows = core.execute(
                f"""SELECT t.background_task_id,t.routing_json,coalesce(t.substrate,'opencode'),
                           ev.outcome,ev.work_quality,{reference}
                      FROM manager_task t
                      LEFT JOIN manager_task_evaluation ev USING (background_task_id)
                     WHERE t.agent='mason' AND t.time_created>?
                     ORDER BY t.background_task_id,ev.rowid LIMIT ? OFFSET ?""",
                (since, page_size, offset),
            ).fetchall()
            if not rows:
                return
            for bg, routing_json, substrate, outcome, quality, digest in rows:
                if digest is not None:
                    if archive is None:
                        path = next(row[2] for row in core.execute("PRAGMA database_list") if row[1] == "main")
                        try:
                            archive = ro(os.path.join(os.path.dirname(path), "archive.db"))
                        except sqlite3.Error as error:
                            raise ValueError(f"archive_open_failed: task={bg} sha256={digest}") from error
                    blob = archive.execute("SELECT bytes FROM blob WHERE sha256=?", (digest,)).fetchone()
                    if blob is None:
                        raise ValueError(f"archive_blob_missing: task={bg} field=routing_json sha256={digest}")
                    if hashlib.sha256(blob[0]).hexdigest() != digest:
                        raise ValueError(f"archive_blob_hash_mismatch: task={bg} field=routing_json sha256={digest}")
                    routing_json = blob[0].decode("utf-8")
                yield bg, routing_json, substrate, outcome, quality
            offset += len(rows)
    finally:
        if archive is not None:
            archive.close()
        if owns_snapshot:
            core.rollback()


def main():
    routing = ro(ROUTING)
    core = ro(CORE)
    since = WINDOW_END_MS - WINDOW_MS

    # Mason launches carry the host's available list and the settle outcome.
    tasks = {}
    for bg, routing_json, substrate, outcome, wq in task_routing_rows(core, since - 86_400_000):
        r = jload(routing_json, {}) or {}
        dec = r.get("decisionID")
        if not dec:
            continue
        judged = outcome in ("finalized", "rejected")
        tasks[dec] = {
            "bg": bg,
            "substrate": substrate,
            "available": r.get("availableModels") or [],
            "settle": outcome,
            "wq": wq,
            "fail": (outcome == "rejected" or (wq is not None and wq <= 70)) if judged else None,
        }

    columns = {row[1] for row in routing.execute("PRAGMA table_info(model_route_decisions)")}
    # Stores from before compaction existed hold full detail in every row.
    full = "detail_stripped_at IS NULL" if "detail_stripped_at" in columns else "1"
    rows = routing.execute(
        f"""SELECT id, task_id, target_agent, requirements_json, selected_route_key,
                  selected_provider_id, score_json, rejected_json, explanation_json,
                  created_at, outcome, outcome_error, quota_signal_json, harness, decision_kind,
                  {full}
             FROM model_route_decisions
            WHERE created_at > ? AND created_at <= ?
            ORDER BY created_at""",
        (since, WINDOW_END_MS),
    ).fetchall()

    # Per provider, the abundance the winners recorded, as a time series. A
    # winner of a provider that prints no abundance line had none at that
    # moment (the router prints the line whenever the value is present), so it
    # is recorded as an explicit None observation rather than skipped.
    abundance_series = defaultdict(list)
    for row in rows:
        lines = jload(row[8], []) or []
        found = False
        for line in lines if isinstance(lines, list) else []:
            m = ABUNDANCE_RE.match(line) if isinstance(line, str) else None
            if m:
                provider = window_provider(m.group(2))
                if provider:
                    abundance_series[provider].append((row[9], float(m.group(1)), m.group(2)))
                    found = True
        if not found and row[5] in WINDOW_PROVIDER.values() and row[6] and row[6] != "null":
            abundance_series[row[5]].append((row[9], None, None))
    for series in abundance_series.values():
        series.sort()

    def abundance_at(provider, ts):
        series = abundance_series.get(provider)
        if not series:
            return None
        times = [s[0] for s in series]
        i = bisect.bisect_right(times, ts) - 1
        best = None
        if i >= 0 and ts - series[i][0] <= ABUNDANCE_MAX_AGE_MS:
            best = series[i]
        elif i + 1 < len(series) and series[i + 1][0] - ts <= ABUNDANCE_MAX_AGE_MS:
            best = series[i + 1]
        return None if best is None or best[1] is None else {"value": best[1], "window": best[2]}

    out = open(OUT, "w")
    counts = defaultdict(int)
    for (dec_id, task_id, agent, req_json, winner, winner_provider, score_json, rejected_json,
         explanation_json, created_at, outcome, outcome_error, quota_json, harness, kind,
         full_detail) in rows:
        if not full_detail:
            counts["compacted (skipped)"] += 1
            continue
        counts[kind] += 1
        req = jload(req_json, {}) or {}
        score = jload(score_json)
        explanation = jload(explanation_json, []) or []
        rejected = jload(rejected_json, []) or []
        rec = {
            "id": dec_id,
            "ts": created_at,
            "agent": agent,
            "kind": kind,
            "harness": harness,
            "task_id": task_id,
            "outcome": outcome,
            "outcome_error": (outcome_error or "")[:160],
            "winner": winner,
            "full_detail_sample": in_full_detail_sample(dec_id),
        }
        if kind != "single":
            rec["requirements"] = req
            out.write(json.dumps(rec) + "\n")
            continue
        rec["iq"] = float(req.get("iq") or 0.0)
        rec["eq"] = float(req.get("eq") or 0.0)
        if isinstance(explanation, list):
            rec["best_effort"] = any(isinstance(l, str) and l.startswith("best-effort") for l in explanation)
            rec["break_glass"] = any(isinstance(l, str) and l.startswith("break-glass") for l in explanation)
            rec["basket"] = any(isinstance(l, str) and "basket pricing" in l for l in explanation)
            policy = [POLICY_RE.match(l) for l in explanation if isinstance(l, str)]
            policy = [m.group(1) for m in policy if m]
            rec["policy"] = policy[0] if policy else None
            rec["availability_line"] = next(
                (l for l in explanation if isinstance(l, str) and l.startswith("availability")), None
            )
            rec["covered"] = next(
                (
                    ("covered" if "subscription-covered" in l else "metered")
                    for l in explanation
                    if isinstance(l, str) and l.startswith("cost +")
                ),
                None,
            )
        if score:
            metrics = score.get("metrics") or {}
            elo = metrics.get("elo") or {}
            eqm = metrics.get("eq") or {}
            obj = score.get("objective") or {}
            rec["rec_selection_cost"] = score.get("selectionCost")
            rec["rec_cost"] = obj.get("cost")
            rec["rec_quota"] = obj.get("quota")
            rec["rec_speed"] = obj.get("speedScore")
            rec["rec_adequacy"] = score.get("adequacy")
            rec["rec_elo_fact"] = elo.get("fact")
            rec["rec_eq_fact"] = eqm.get("fact")
            if elo.get("target") and rec["iq"] > 0:
                rec["rec_top_elo"] = elo["target"] / (rec["iq"] / 100.0) ** DEMAND_EXPONENT
            if eqm.get("target") and rec["eq"] > 0:
                rec["rec_top_eq"] = eqm["target"] / (rec["eq"] / 100.0) ** DEMAND_EXPONENT
        walls, op_excluded, caller_excluded, not_available = [], [], [], []
        for entry in rejected:
            if not isinstance(entry, dict):
                continue
            if "truncated" in entry:
                not_available.extend(entry.get("notAvailable") or [])
                continue
            reason, key = entry.get("reason"), entry.get("routeKey")
            if reason in WALL_REASONS:
                walls.append(key)
            elif reason == "excluded_by_operator":
                op_excluded.append(key)
            elif reason == "excluded":
                caller_excluded.append(key)
            elif reason in ("not_available", "unavailable"):
                not_available.append(key)
        rec["walls"] = walls
        rec["listed_operator_excluded"] = op_excluded
        rec["caller_excluded"] = caller_excluded
        rec["not_available"] = not_available
        quota = jload(quota_json, {}) or {}
        rec["pressure"] = {
            s["capacityKey"]: s["pressure"]
            for s in (quota.get("signals") or [])
            if isinstance(s, dict) and s.get("pressure") is not None
        }
        rec["abundance"] = {
            p: a for p in ("openai", "anthropic", "xai") if (a := abundance_at(p, created_at))
        }
        task = tasks.get(dec_id)
        if task:
            rec["mason_task"] = task["bg"]
            rec["mason_substrate"] = task["substrate"]
            rec["available"] = task["available"]
            rec["settle"] = task["settle"]
            rec["wq"] = task["wq"]
            rec["fail"] = task["fail"]
        out.write(json.dumps(rec) + "\n")
    out.close()
    print(dict(counts), "->", OUT, file=sys.stderr)


if __name__ == "__main__":
    main()
