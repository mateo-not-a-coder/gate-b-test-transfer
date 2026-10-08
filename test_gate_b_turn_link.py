"""Step 79 staging-only tests for atomic historical-context turn linkage."""
import concurrent.futures
import contextlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

import gate_b_state as state
import gate_b_context as context
import gate_b_turn_link as link

ROOT=Path(__file__).resolve().parent


class TurnLinkTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix="gate-b-turn-",dir=str(ROOT))
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"test.db")
        with contextlib.closing(sqlite3.connect(str(ROOT/"gate-b-test.db"))) as src:
            ddl=[x[0] for x in src.execute(
                "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND (name LIKE ? OR name=?) "
                "ORDER BY CASE WHEN type='table' THEN 0 ELSE 1 END",
                ("client_takeover_%","idx_gate_b_pending"))]
        with contextlib.closing(sqlite3.connect(self.path)) as db:
            for stmt in ddl:
                db.execute(stmt)
            db.commit()
        context.initialize_context_schema(self.path)
        link.initialize_turn_schema(self.path)

    def prepare(self):
        self.assertEqual(state.record_owner_action(self.path,"a","owner",1000),("SUPPRESS",1))
        for n in range(3):
            self.assertEqual(state.admit_client_input(self.path,"a","hist-"+str(n),1001+n,
                '{"message":"prior"}'),("SUPPRESS",1))
        self.assertEqual(state.admit_client_input(self.path,"a","trigger",1801000,
            '{"message":"new"}'),("ALLOW_NEW_TURN",2))

    def counts(self):
        with contextlib.closing(sqlite3.connect(self.path)) as db:
            return (db.execute("SELECT COUNT(*) FROM client_takeover_resumed_turns").fetchone()[0],
                db.execute("SELECT COUNT(*) FROM client_takeover_pending_inputs WHERE consumed_at_ms IS NOT NULL").fetchone()[0])

    def test_atomic_ready_and_excludes_trigger(self):
        self.prepare()
        status,history=link.associate_resumed_turn(self.path,"a","trigger",2,"turn-1",1802000)
        self.assertEqual(status,"READY")
        self.assertEqual([x["event_key"] for x in history],["hist-0","hist-1","hist-2"])
        self.assertTrue(all(x["role"]=="historical_client_context_only" for x in history))
        self.assertEqual(self.counts(),(1,3))

    def test_restart_retry_returns_same_link(self):
        self.prepare()
        first=link.associate_resumed_turn(self.path,"a","trigger",2,"turn-1",1802000)
        again=link.associate_resumed_turn(self.path,"a","trigger",2,"turn-1",1900000)
        self.assertEqual(first[0],"READY")
        self.assertEqual(again,("EXISTING",first[1]))
        self.assertEqual(self.counts(),(1,3))

    def test_claim_then_interruption_is_recoverable(self):
        self.prepare()
        self.assertEqual(context.claim_pending_context(self.path,"a","trigger",2,"turn-1")[0],"CONTEXT_ONLY")
        self.assertEqual(self.counts(),(0,0))
        self.assertEqual(link.associate_resumed_turn(self.path,"a","trigger",2,"turn-1",1802000)[0],"READY")
        self.assertEqual(self.counts(),(1,3))

    def test_duplicate_turn_and_generation_rejected(self):
        self.prepare()
        self.assertEqual(link.associate_resumed_turn(self.path,"a","trigger",2,"turn-1",1802000)[0],"READY")
        self.assertEqual(link.associate_resumed_turn(self.path,"a","trigger",2,"turn-2",1803000)[0],"ERROR_SUPPRESS")
        self.assertEqual(link.associate_resumed_turn(self.path,"a","trigger",1,"turn-1",1803000)[0],"ERROR_SUPPRESS")
        self.assertEqual(self.counts(),(1,3))

    def test_concurrent_association_one_ready(self):
        self.prepare()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            jobs=[pool.submit(link.associate_resumed_turn,self.path,"a","trigger",2,"turn-1",1802000+i) for i in range(3)]
            result=[j.result() for j in jobs]
        self.assertEqual(sum(x[0]=="READY" for x in result),1)
        self.assertEqual(sum(x[0]=="EXISTING" for x in result),2)
        self.assertEqual(self.counts(),(1,3))

    def test_owner_intervention_revokes_claim(self):
        self.prepare()
        state.record_owner_action(self.path,"a","owner-again",1801001)
        self.assertEqual(link.associate_resumed_turn(self.path,"a","trigger",2,"turn-1",1802000)[0],"ERROR_SUPPRESS")
        self.assertEqual(self.counts(),(0,0))

    def test_missing_db_fails_closed(self):
        missing=str(Path(self.tmp.name)/"no-directory"/"test.db")
        self.assertEqual(link.associate_resumed_turn(missing,"a","trigger",2,"turn-1",1802000)[0],"ERROR_SUPPRESS")


if __name__=="__main__":
    unittest.main(verbosity=2)