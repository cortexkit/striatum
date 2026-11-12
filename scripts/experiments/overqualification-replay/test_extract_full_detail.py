import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("overqualification_extract", Path(__file__).with_name("extract.py"))
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class FullDetailExtractTests(unittest.TestCase):
    def setUp(self):
        Path("target/scratch").mkdir(parents=True, exist_ok=True)
        self.scratch = tempfile.TemporaryDirectory(dir="target/scratch", prefix="full-detail-")
        root = Path(self.scratch.name)
        self.routing_path = root / "routing.db"
        self.core_path = root / "core.db"
        self.out_path = root / "decisions.jsonl"
        routing = sqlite3.connect(self.routing_path)
        routing.executescript("""
            CREATE TABLE model_route_decisions(id TEXT PRIMARY KEY, task_id TEXT, target_agent TEXT,
                requirements_json TEXT, selected_route_key TEXT, selected_provider_id TEXT,
                score_json TEXT, rejected_json TEXT, explanation_json TEXT, created_at INTEGER,
                outcome TEXT, outcome_error TEXT, quota_signal_json TEXT, harness TEXT,
                decision_kind TEXT, detail_stripped_at INTEGER);
        """)
        full_quota = json.dumps({"signals": [{"capacityKey": "p", "pressure": 0.4}]})
        for decision_id, stripped_at in (("full", None), ("compacted", 5)):
            routing.execute(
                "INSERT INTO model_route_decisions VALUES (?,NULL,'mason','{\"iq\":70}','p/m','p',"
                "'{}','[]','[]',?,NULL,NULL,?,NULL,'single',?)",
                (decision_id, module.WINDOW_END_MS - 1000, full_quota, stripped_at),
            )
        routing.commit()
        routing.close()
        core = sqlite3.connect(self.core_path)
        core.executescript("""
            CREATE TABLE manager_task(background_task_id TEXT PRIMARY KEY, routing_json TEXT,
                substrate TEXT, agent TEXT, time_created INTEGER);
            CREATE TABLE manager_task_evaluation(background_task_id TEXT, outcome TEXT, work_quality INTEGER);
        """)
        core.close()

    def tearDown(self):
        self.scratch.cleanup()

    def test_only_full_detail_rows_are_written(self):
        module.ROUTING, module.CORE, module.OUT = self.routing_path, self.core_path, self.out_path
        module.main()
        rows = [json.loads(line) for line in self.out_path.read_text().splitlines()]
        self.assertEqual([row["id"] for row in rows], ["full"])
        self.assertEqual(rows[0]["pressure"], {"p": 0.4})
        self.assertEqual(rows[0]["full_detail_sample"], module.in_full_detail_sample("full"))

    def test_the_sample_hash_matches_routing(self):
        # The FNV-1a 64-bit vectors routing's own tests pin.
        self.assertEqual(module.decision_id_hash(""), 0xCBF29CE484222325)
        self.assertEqual(module.decision_id_hash("a"), 0xAF63DC4C8601EC8C)
        self.assertEqual(module.decision_id_hash("foobar"), 0x85944171F73967E8)
        self.assertTrue(module.in_full_detail_sample("anything", 1))
        self.assertFalse(module.in_full_detail_sample("anything", 0))


if __name__ == "__main__":
    unittest.main()
