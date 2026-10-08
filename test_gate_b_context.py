"""Staging-only tests: Gate B context never becomes a runnable request."""
import contextlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

import gate_b_state as state
import gate_b_context as ctx

ROOT=Path(__file__).resolve().parent


class ContextTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=str(ROOT),prefix="gate-b-context-")
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"test.db")
        with contextlib.closing(sqlite3.connect(str(ROOT/"gate-b-test.db"))) as src:
            tables=[row[0] for row in src.execute(
                "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL "
                "AND (name LIKE ? OR name=?) ORDER BY CASE WHEN type='table' THEN 0 ELSE 1 END",
                ("client_takeover_%","idx_gate_b_pending"))]
        with contextlib.closing(sqlite3.connect(self.path)) as db:
            for sql in tables:
                db.execute(sql)
            db.commit()
        ctx.initialize_context_schema(self.path)

    def prepare(self):
        self.assertEqual(state.record_owner_action(self.path,"a","owner-1",1000),("SUPPRESS",1))
        for i in range(3):
            self.assertEqual(state.admit_client_input(self.path,"a","old-"+str(i),1001+i,
                '{"text":"historical '+str(i)+'"}'),("SUPPRESS",1))
        self.assertEqual(state.admit_client_input(self.path,"a","resume",1801000,
            '{"text":"fresh trigger"}'),("ALLOW_NEW_TURN",2))

    def rows(self):
        with contextlib.closing(sqlite3.connect(self.path)) as db:
            return db.execute("SELECT event_key,consumed_by_event_key,consumed_at_ms "
                              "FROM client_takeover_pending_inputs ORDER BY arrival_seq").fetchall()

    def test_retain_once_and_trigger_excluded(self):
        self.prepare()
        result,rows=ctx.claim_pending_context(self.path,"a","resume",2,"turn-1")
        self.assertEqual(result,"CONTEXT_ONLY")
        self.assertEqual([x[0] for x in rows],["old-0","old-1","old-2"])
        self.assertTrue(all("historical" in x[1] for x in rows))
        self.assertEqual(self.rows()[-1],("resume",None,None))

    def test_repeat_claim_after_connection_reopen(self):
        self.prepare()
        a=ctx.claim_pending_context(self.path,"a","resume",2,"turn-1")
        b=ctx.claim_pending_context(self.path,"a","resume",2,"turn-1")
        self.assertEqual(a,b)
        self.assertEqual(len(a[1]),3)
        self.assertEqual(ctx.claim_pending_context(self.path,"a","resume",2,"another-turn")[0],"ERROR_SUPPRESS")

    def test_interrupted_processing_recovers(self):
        self.prepare()
        a=ctx.claim_pending_context(self.path,"a","resume",2,"turn-1")
        self.assertEqual(sum(x[2] is not None for x in self.rows()),0)
        # Simulate process termination by making a new call with a fresh SQLite connection.
        b=ctx.claim_pending_context(self.path,"a","resume",2,"turn-1")
        self.assertEqual(a,b)
        self.assertEqual(ctx.commit_context_consumption(self.path,"a","resume",2,"turn-1",1802000),"CONSUMED")
        self.assertEqual(ctx.commit_context_consumption(self.path,"a","resume",2,"turn-1",1803000),"DUPLICATE_COMMIT")
        self.assertEqual(sum(x[2] is not None for x in self.rows()),3)

    def test_fail_closed_and_generation(self):
        self.prepare()
        self.assertEqual(ctx.claim_pending_context(self.path,"a","resume",1,"turn-1")[0],"ERROR_SUPPRESS")
        self.assertEqual(ctx.claim_pending_context(self.path,"a","missing",2,"turn-1")[0],"ERROR_SUPPRESS")
        self.assertEqual(ctx.commit_context_consumption(self.path,"a","resume",2,"turn-1",1802000),"ERROR_SUPPRESS")
        missing=str(Path(self.tmp.name)/"missing"/"state.db")
        self.assertEqual(ctx.claim_pending_context(missing,"a","resume",2,"turn-1")[0],"ERROR_SUPPRESS")
        self.assertEqual(ctx.commit_context_consumption(missing,"a","resume",2,"turn-1",1802000),"ERROR_SUPPRESS")

    def test_new_owner_invalidates_unclaimed_generation(self):
        self.prepare()
        state.record_owner_action(self.path,"a","owner-2",1801001)
        self.assertEqual(ctx.claim_pending_context(self.path,"a","resume",2,"turn-1")[0],"ERROR_SUPPRESS")


if __name__=="__main__":
    unittest.main(verbosity=2)