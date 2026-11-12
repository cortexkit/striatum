import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("overqualification_extract", Path(__file__).with_name("extract.py"))
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class ArchivedRoutingTests(unittest.TestCase):
    def setUp(self):
        Path("target/scratch").mkdir(parents=True, exist_ok=True)
        self.scratch = tempfile.TemporaryDirectory(dir="target/scratch", prefix="archive-routing-")
        self.core = sqlite3.connect(str(Path(self.scratch.name) / "store.db"))
        self.core.execute("PRAGMA journal_mode=WAL")
        self.core.executescript("""
            CREATE TABLE manager_task(background_task_id TEXT PRIMARY KEY,routing_json TEXT,
                routing_archive_sha256 TEXT,substrate TEXT,agent TEXT,time_created INTEGER);
            CREATE TABLE manager_task_evaluation(background_task_id TEXT,outcome TEXT,work_quality INTEGER);
        """)
        self.archive = sqlite3.connect(str(Path(self.scratch.name) / "archive.db"))
        self.archive.execute("CREATE TABLE blob(sha256 TEXT PRIMARY KEY,bytes BLOB NOT NULL)")

    def tearDown(self):
        self.archive.close()
        self.core.close()
        self.scratch.cleanup()

    def insert(self, task, raw, archived=True):
        digest = hashlib.sha256(raw.encode()).hexdigest() if archived else None
        if archived:
            self.archive.execute("INSERT OR IGNORE INTO blob VALUES (?,?)", (digest, raw.encode()))
            self.archive.commit()
        self.core.execute("INSERT INTO manager_task VALUES (?,?,?,'broca','mason',1)",
                          (task, None if archived else raw, digest))
        self.core.execute("INSERT INTO manager_task_evaluation VALUES (?,'finalized',90)", (task,))

    def test_full_offline_inputs_are_resolved_in_bounded_pages(self):
        raw = ' {"decisionID":"decision","availableModels":["one","two"]} \n'
        for n in range(202):
            self.insert(f"task-{n:03}", raw, archived=n != 201)
        queries = []
        self.core.set_trace_callback(queries.append)
        rows = list(module.task_routing_rows(self.core, 0))
        self.assertEqual(len(rows), 202)
        self.assertTrue(all(row[1] == raw for row in rows))
        pages = [sql for sql in queries if "ORDER BY t.background_task_id,ev.rowid LIMIT" in sql]
        self.assertEqual(len(pages), 4)
        self.assertTrue(all("LIMIT 100 OFFSET" in sql for sql in pages))
        self.assertEqual(json.loads(rows[0][1])["availableModels"], ["one", "two"])

    def test_missing_and_corrupt_payloads_are_named_and_page_limit_is_enforced(self):
        self.insert("task", '{"decisionID":"decision"}')
        self.archive.execute("UPDATE blob SET bytes=x'00'")
        self.archive.commit()
        with self.assertRaisesRegex(ValueError, "archive_blob_hash_mismatch: task=task"):
            list(module.task_routing_rows(self.core, 0))
        self.archive.execute("DELETE FROM blob")
        self.archive.commit()
        with self.assertRaisesRegex(ValueError, "archive_blob_missing: task=task"):
            list(module.task_routing_rows(self.core, 0))
        with self.assertRaisesRegex(ValueError, "archive_list_page_required"):
            list(module.task_routing_rows(self.core, 0, page_size=101))

    def test_pages_share_one_read_snapshot_and_restore_transaction_state(self):
        self.insert("task-b", '{"decisionID":"b"}', archived=False)
        self.insert("task-c", '{"decisionID":"c"}', archived=False)
        self.core.commit()
        rows = module.task_routing_rows(self.core, 0, page_size=1)
        self.assertEqual(next(rows)[0], "task-b")
        self.assertTrue(self.core.in_transaction)
        writer = sqlite3.connect(str(Path(self.scratch.name) / "store.db"))
        try:
            writer.execute("INSERT INTO manager_task VALUES ('task-a','{}',NULL,'broca','mason',1)")
            writer.commit()
        finally:
            writer.close()
        self.assertEqual([row[0] for row in rows], ["task-c"])
        self.assertFalse(self.core.in_transaction)
        self.assertEqual(self.core.execute("SELECT count(*) FROM manager_task").fetchone()[0], 3)


if __name__ == "__main__":
    unittest.main()
