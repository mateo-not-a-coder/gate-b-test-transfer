"""Gate B staging-only regression checks. Never point at /data/state.db."""
import concurrent.futures
import sqlite3
import tempfile
import unittest
from pathlib import Path

import gate_b_state as gate

ROOT = Path(__file__).resolve().parent
SCHEMA_DB = ROOT / "gate-b-test.db"


class GateBTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="gate-b-suite-", dir=str(ROOT))
        self.addCleanup(self.tmp.cleanup)
        self.db = str(Path(self.tmp.name) / "test.db")
        with sqlite3.connect(str(SCHEMA_DB)) as src:
            sqls = [row[0] for row in src.execute(
                "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL "
                "AND (name LIKE ? OR name = ?) ORDER BY "
                "CASE WHEN type='table' THEN 0 ELSE 1 END",
                ("client_takeover_%", "idx_gate_b_pending")
            )]
        self.assertEqual(len(sqls), 4, "Expected three staging tables plus index")
        with sqlite3.connect(self.db) as conn:
            for sql in sqls:
                conn.execute(sql)

    def state(self):
        with sqlite3.connect(self.db) as conn:
            return conn.execute(
                "SELECT generation,owner_last_at_ms,takeover_active "
                "FROM client_takeover_state WHERE conversation_key='a'"
            ).fetchone()

    def test_owner_dedup(self):
        self.assertEqual(gate.record_owner_action(self.db, "a", "o1", 1000), ("SUPPRESS", 1))
        self.assertEqual(gate.record_owner_action(self.db, "a", "o1", 3000), ("DUPLICATE", 1))
        self.assertEqual(self.state(), (1, 1000, 1))

    def test_exact_boundary_and_generation_fence(self):
        gate.record_owner_action(self.db, "a", "o1", 1000)
        self.assertEqual(gate.admit_client_input(self.db, "a", "early", 1800999), ("SUPPRESS", 1))
        self.assertEqual(gate.admit_client_input(self.db, "a", "resume", 1801000), ("ALLOW_NEW_TURN", 2))
        self.assertEqual(self.state(), (2, 1000, 0))
        self.assertFalse(gate.read_delivery_permission(self.db, "a", 1))
        self.assertTrue(gate.read_delivery_permission(self.db, "a", 2))
        gate.record_owner_action(self.db, "a", "o2", 1900000)
        self.assertFalse(gate.read_delivery_permission(self.db, "a", 2))

    def test_late_owner_does_not_rewind(self):
        gate.record_owner_action(self.db, "a", "o1", 2000)
        gate.record_owner_action(self.db, "a", "o2", 1000)
        self.assertEqual(self.state(), (2, 2000, 1))

    def test_owner_parallel_distinct(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            jobs = [pool.submit(gate.record_owner_action, self.db, "a", "o"+str(i), 1000) for i in range(4)]
            result = [job.result() for job in jobs]
        self.assertEqual(sum(x[0] == "SUPPRESS" for x in result), 4)
        self.assertEqual(self.state(), (4, 1000, 1))

    def test_owner_parallel_duplicate(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            jobs = [pool.submit(gate.record_owner_action, self.db, "a", "same", 1000) for _ in range(6)]
            result = [job.result() for job in jobs]
        self.assertEqual(sum(x[0] == "SUPPRESS" for x in result), 1)
        self.assertEqual(sum(x[0] == "DUPLICATE" for x in result), 5)
        self.assertEqual(self.state(), (1, 1000, 1))

    def test_client_duplicate_across_connections(self):
        gate.record_owner_action(self.db, "a", "o1", 1000)
        self.assertEqual(gate.admit_client_input(self.db, "a", "c1", 1001), ("SUPPRESS", 1))
        self.assertEqual(gate.admit_client_input(self.db, "a", "c1", 1002), ("DUPLICATE", 1))
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM client_takeover_pending_inputs").fetchone()[0], 1)

    def test_mixed_owner_client_parallel(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            one = pool.submit(gate.record_owner_action, self.db, "a", "o1", 1000)
            two = pool.submit(gate.admit_client_input, self.db, "a", "c1", 1001)
            owner, client = one.result(), two.result()
        self.assertEqual(owner, ("SUPPRESS", 1))
        self.assertIn(client[0], ("SUPPRESS", "ALLOW_NEW_TURN"))
        self.assertEqual(self.state(), (1, 1000, 1))
        self.assertFalse(gate.read_delivery_permission(self.db, "a", 0))

    def test_missing_db_fails_closed(self):
        path = str(Path(self.tmp.name) / "missing-dir" / "db.sqlite")
        self.assertEqual(gate.record_owner_action(path, "a", "o1", 1000)[0], "ERROR_SUPPRESS")
        self.assertEqual(gate.admit_client_input(path, "a", "c1", 1000)[0], "ERROR_SUPPRESS")
        self.assertFalse(gate.read_delivery_permission(path, "a", 0))

    def test_invalid_identity_fails_closed(self):
        self.assertEqual(gate.record_owner_action(self.db, "", "o1", 1000)[0], "ERROR_SUPPRESS")
        self.assertEqual(gate.admit_client_input(self.db, "a", "", 1000)[0], "ERROR_SUPPRESS")
        self.assertFalse(gate.read_delivery_permission(self.db, "", 0))


if __name__ == "__main__":
    unittest.main(verbosity=2)