"""Exercise both CLI stages with no live store or what-if client."""

import contextlib
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from analyze import band, read_results
from history import DAY, MINUTE, Identity, reconstruct, utc
from run import main, read_only

AT = 1_795_104_000_000  # 19 Nov 2026, 16:00 UTC
END = AT + 120 * MINUTE


def setup(root):
    routing, history = root / "store?synthetic.db", root / "history.db"
    with contextlib.closing(sqlite3.connect(routing)) as db, db:
        db.executescript("""
            CREATE TABLE account_vault_roster (singleton INTEGER, roster_json TEXT);
            CREATE TABLE account_quota_reading (provider TEXT, account_id TEXT, entry_json TEXT, tombstoned_at INTEGER);
            CREATE TABLE model_route_decisions (id TEXT, created_at INTEGER, decision_kind TEXT, consumer TEXT,
                requirements_json TEXT, selected_provider_id TEXT, selected_route_key TEXT, score_json TEXT,
                detail_stripped_at INTEGER);
        """)
        roster = {"identities": [{"account_id": "close-a", "plan_tier": "plus", "first_seen_ms": END - DAY}],
                  "listing": {"credentials": [{"id": "kimi-a", "account_id": None, "plan_tier": "pro"}]}}
        db.execute("INSERT INTO account_vault_roster VALUES (1, ?)", (json.dumps(roster),))
        for account, lane, vendor in [("kimi-a", "kimi-lane", "kimi"), ("close-a", "other-lane", "other")]:
            db.execute("INSERT INTO account_quota_reading VALUES (?, ?, ?, NULL)", (lane, account, json.dumps({
                "provider": lane, "apiProvider": vendor, "usage": {},
                **({"credentialRef": account} if vendor == "kimi" else {"account": account}),
            })))
        for id, at, consumer, price, compact in [
            ("covered-1", AT + MINUTE, "prefrontal-core.mason", 59 / 300, None),
            ("covered-2", AT + MINUTE, "prefrontal.mason", 42, AT + 10 * MINUTE),
            ("before-retention", AT - DAY // 2, "prefrontal.mason", None, None),
        ]:
            objective = {} if price is None else {"subscriptionPricing": {
                "planTier": "pro", "planValues": [{"planTier": "pro", "plan": "kimi/pro-current"}],
                "availablePoints": 88.8, "planPressure": price, "keepFreePercent": 5, "bankedResetsApplied": 0,
                "shortWindowPressure": price,
            }}
            db.execute("INSERT INTO model_route_decisions VALUES (?, ?, 'single', ?, ?, 'kimi', 'kimi/k3', ?, ?)",
                       (id, at, consumer, '{"iq":50}', json.dumps({"objective": objective, "alsoAdequate": ["sol/gpt"]}), compact))
    with contextlib.closing(sqlite3.connect(history)) as db, db:
        db.executescript("""
            CREATE TABLE quota_reading_log (account_key TEXT, window_key TEXT, observed_at INTEGER, used_percent REAL,
                raw_used_percent REAL, resets_at INTEGER, window_minutes INTEGER, banked_credit_count INTEGER,
                fetched_at INTEGER, is_stale INTEGER, stale_since TEXT, stale_class TEXT);
            CREATE TABLE account_decision (provider TEXT, account_id TEXT, created_at INTEGER, mode TEXT,
                outcome TEXT, admission_id TEXT, decision_id TEXT);
        """)
        for account, lane, name, fetched, used, reset, minutes, banked in [
            ("kimi/", "kimi-lane", "primary", AT - 10 * MINUTE, 4, END, 10080, 0),
            ("kimi/", "kimi-lane", "primary", AT - MINUTE, 6.2, END, 10080, 1),
            ("kimi/", "kimi-lane", "secondary", AT - MINUTE, 0, AT + 60 * MINUTE, 300, 1),
            ("other/close-a", "other-lane", "primary", AT - MINUTE, 90, AT + MINUTE, 10080, 0),
            ("other/close-a", "other-lane", "secondary", AT - MINUTE, 73, AT + 120 * MINUTE, 300, 0),
            # The slot and both decisions must not see the future reading.
            ("kimi/", "kimi-lane", "primary", AT + 3 * MINUTE, 8, END, 10080, 1),
        ]:
            db.execute("INSERT INTO quota_reading_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL)",
                       (account, f"codexbar:{lane}:{name}", fetched, used, used, reset, minutes, banked, fetched))
        for decision in ("admit-1", "admit-1-duplicate"):
            db.execute("INSERT INTO account_decision VALUES ('kimi', 'kimi-a', ?, 'serve', 'admit', 'lease-1', ?)",
                       (AT - 7 * MINUTE, decision))
        db.execute("INSERT INTO account_decision VALUES ('kimi', 'kimi-a', ?, 'serve', 'admit', 'lease-2', 'admit-2')",
                   (AT + 2 * MINUTE,))
    return routing, history


def run_fixture(root):
    routing, history = setup(root)
    out = root / "out"
    env = {"PACING_ROUTING_STORE": str(routing), "PACING_HISTORY_STORE": str(history), "PACING_WINDOW_END_MS": str(END)}
    text = io.StringIO()
    with patch.dict(os.environ, env), contextlib.redirect_stdout(text):
        main(["--days", "1", "--out", str(out)])
    requests = [json.loads(line) for line in (out / "requests.jsonl").read_text().splitlines()]
    answers = out / "results.jsonl"
    with answers.open("w") as stream:
        for request in requests:
            params = request["params"]
            capacity = any("savedResets" in a for a in params["quota_override"])
            pick = {"route": "sol/gpt", "variant": None}
            if params["pacing"] == "expiry" and not capacity:
                pick = {"route": "kimi/k3", "variant": None,
                        "subscription_plan": {"provider": "kimi", "tier": "pro", "plan": "kimi/pro-current"}}
            result = {"consumer": params["consumer"], "results": [{"iq": iq, "pick": pick} for iq in params["iqs"]]}
            stream.write(json.dumps({"id": request["id"], "result": result}) + "\n")
    with patch.dict(os.environ, {"PACING_ROUTING_STORE": "/not-used", "PACING_HISTORY_STORE": "/not-used"}):
        with contextlib.redirect_stdout(text):
            main(["--out", str(out), "--results", str(answers)])
    return routing, out, requests, text.getvalue()


class ReplayFixture(unittest.TestCase):
    def test_last_good_reading_cannot_survive_a_past_short_reset(self):
        identity = Identity("kimi", "account", "pro", {"provider": "kimi", "apiProvider": "kimi"}, None, None)
        stale = {"observed_at": AT, "fetched_at": AT - 18 * MINUTE, "stale_class": "upstream_failed",
                 "stale_since": utc(AT - MINUTE), "is_stale": 1, "used_percent": 5,
                 "raw_used_percent": None, "banked_credit_count": 0}
        rows = {"primary": {**stale, "resets_at": END, "window_minutes": 10080},
                "secondary": {**stale, "resets_at": AT + MINUTE, "window_minutes": 300}}
        self.assertIsNotNone(reconstruct(identity, rows, AT).account)
        self.assertIsNone(reconstruct(identity, rows, AT + MINUTE))

    def test_read_only_prepare_and_reconcile_weighted_fixture(self):
        scratch = Path("target/scratch")
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch, prefix="pacing-") as directory:
            routing, out, requests, report = run_fixture(Path(directory))
            with contextlib.closing(read_only(routing)) as db:
                with self.assertRaisesRegex(sqlite3.OperationalError, "readonly"):
                    db.execute("DELETE FROM model_route_decisions")
            self.assertEqual(len(requests), 4)
            self.assertTrue(all(row["params"]["consumer"] == "prefrontal.mason" for row in requests))
            self.assertTrue(all(row["params"]["quota_override_at_ms"] == AT for row in requests))
            self.assertTrue(all(row["params"]["iqs"] == [50] for row in requests))
            self.assertEqual(sum(any("savedResets" in a for a in row["params"]["quota_override"]) for row in requests), 2)
            states = [a for a in requests[0]["params"]["quota_override"] if a.get("credentialRef") == "kimi-a"]
            self.assertEqual(states[0]["usage"]["primary"]["usedPercent"], 6.2)
            self.assertEqual(states[0]["fetchedAt"], utc(AT - MINUTE))
            manifest = json.loads((out / "manifest.json").read_text())
            self.assertEqual(manifest["cells"][0]["weight"], 2)
            self.assertEqual(len(manifest["audit"]["examples"]), 1)
            self.assertIn("1/2 matched (50.0%)", report)
            self.assertEqual(manifest["excluded_traffic"]["slot lacks all reconstructable quota history"], 1)
            group = next(g for g in manifest["slots"][-1]["groups"] if g["provider"] == "other")
            self.assertTrue(group["pressures"]["none"]["low_headroom_discount"])
            result = json.loads((out / "sweep.json").read_text())
            self.assertEqual(result["scenarios"]["none"]["changed_weight"], 2)
            self.assertEqual(result["scenarios"]["none"]["by_route"], [["sol/gpt", None, "kimi/k3", "kimi/pro-current", 2]])
            self.assertEqual(result["scenarios"]["capacity"]["changed_weight"], 0)
            self.assertEqual(result["sensitivity"][0]["differ"], 2)
            week = next(row for row in result["scenarios"]["none"]["plan_weeks"] if row["plan"] == "kimi/pro")
            self.assertAlmostEqual(week["max_min_headroom_percent"], 93.8)
            self.assertAlmostEqual(week["expiry_min_headroom_percent"], 89.8)

    def test_level_band_boundaries(self):
        self.assertEqual([band(iq) for iq in (0, 10, 10.5, 20, 20.01, 100)],
                         ["0–10", "0–10", ">10–20", ">10–20", ">20–30", ">90–100"])

    def test_duplicate_response_is_rejected(self):
        scratch = Path("target/scratch")
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            path = Path(directory) / "response.jsonl"
            path.write_text('{"id":"duplicate"}\n{"id":"duplicate"}\n')
            with self.assertRaisesRegex(ValueError, "duplicate response id"):
                read_results(path)


if __name__ == "__main__":
    unittest.main()
