"""Staging-only adapter tests with simulated Hermes transaction boundary."""
import contextlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from test_gate_b_persistence_adapter import FakeDB,FakeStore,authorize_session,authorize_database
from gate_b_persistence_adapter import record_owner_action
from gate_b_client_adapter import admit_client,link_resumed_turn

ROOT=Path(__file__).resolve().parent


class ClientAdapterTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix="gate-b-client-adapter-",dir=str(ROOT))
        self.addCleanup(self.tmp.cleanup)
        self.dbs={}
        for key in ("trusted:a","trusted:b"):
            path=str(Path(self.tmp.name)/(key.replace(":","_")+".db"))
            with contextlib.closing(sqlite3.connect(str(ROOT/"gate-b-test.db"))) as src:
                ddl=[row[0] for row in src.execute(
                    "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND (name LIKE ? OR name=?) "
                    "ORDER BY CASE WHEN type='table' THEN 0 ELSE 1 END",
                    ("client_takeover_%","idx_gate_b_pending"))]
            with contextlib.closing(sqlite3.connect(path)) as conn:
                for query in ddl:
                    conn.execute(query)
                conn.execute("""CREATE TABLE client_takeover_resumed_turns (
                    conversation_key TEXT NOT NULL,resume_event_key TEXT NOT NULL,
                    generation INTEGER NOT NULL,turn_key TEXT NOT NULL,
                    context_json TEXT NOT NULL,linked_at_ms INTEGER NOT NULL,
                    PRIMARY KEY(conversation_key,resume_event_key),UNIQUE(conversation_key,turn_key))""")
                conn.commit()
            db=FakeDB(path)
            db.profile=key
            self.dbs[key]=db
        self.store=FakeStore(self.dbs)

    def owner(self,key="trusted:a",event="o1",ms=1000):
        return record_owner_action(self.store,key,"client-x",event,ms,authorize_session,authorize_database)

    def client(self,event,ms,key="trusted:a",content='{"text":"hello"}'):
        return admit_client(self.store,key,"client-x",event,ms,content,authorize_session,authorize_database)

    def link(self,event="resume",generation=2,turn="turn-1",key="trusted:a"):
        return link_resumed_turn(self.store,key,"client-x",event,generation,turn,1802000,
                                authorize_session,authorize_database)

    def prepare(self):
        self.assertEqual(self.owner(),("SUPPRESS",1))
        self.assertEqual(self.client("old",1001),("SUPPRESS",1))
        self.assertEqual(self.client("resume",1801000),("ALLOW_NEW_TURN",2))

    def test_context_only_and_resume(self):
        self.prepare()
        result,history=self.link()
        self.assertEqual(result,"READY")
        self.assertEqual([x["event_key"] for x in history],["old"])
        self.assertEqual(history[0]["role"],"historical_client_context_only")

    def test_duplicate_resume_does_not_start_again(self):
        self.prepare()
        self.assertEqual(self.client("resume",1803000)[0],"DUPLICATE")
        first=self.link()
        second=self.link()
        self.assertEqual(first[0],"READY")
        self.assertEqual(second,("EXISTING",first[1]))

    def test_duplicate_active_event_is_durable(self):
        self.assertEqual(self.client("active-msg",1000),("ALLOW_NEW_TURN",0))
        self.assertEqual(self.client("active-msg",1001)[0],"DUPLICATE")

    def test_owner_race_revokes_stale_resume(self):
        self.prepare()
        self.assertEqual(self.owner(event="o2",ms=1801001),("SUPPRESS",3))
        self.assertEqual(self.link()[0],"ERROR_SUPPRESS")

    def test_wrong_profile_rejects_and_does_not_write(self):
        self.assertEqual(self.client("x",1000,key="unknown")[0],"ERROR_SUPPRESS")
        fake=FakeStore({"trusted:a":self.dbs["trusted:b"]})
        result=admit_client(fake,"trusted:a","client-x","x",1000,"{}",
                            authorize_session,authorize_database)
        self.assertEqual(result[0],"ERROR_SUPPRESS")

    def test_failure_does_not_admit_client(self):
        self.dbs["trusted:a"].fail=True
        self.assertEqual(self.client("x",1000)[0],"ERROR_SUPPRESS")

    def test_concurrent_duplicate_client(self):
        self.owner()
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs=[pool.submit(self.client,"x",1001) for _ in range(4)]
            results=[f.result() for f in jobs]
        self.assertEqual(sum(x[0]=="SUPPRESS" for x in results),1)
        self.assertEqual(sum(x[0]=="DUPLICATE" for x in results),3)

    def test_concurrent_link_is_idempotent(self):
        self.prepare()
        with ThreadPoolExecutor(max_workers=3) as pool:
            jobs=[pool.submit(self.link) for _ in range(3)]
            results=[f.result() for f in jobs]
        self.assertEqual(sum(x[0]=="READY" for x in results),1)
        self.assertEqual(sum(x[0]=="EXISTING" for x in results),2)

    def test_invalid_json_fails_closed(self):
        self.assertEqual(self.client("x",1000,content="{invalid")[0],"ERROR_SUPPRESS")

    def test_old_time_fails_closed(self):
        self.owner(ms=1000)
        self.assertEqual(self.client("delayed",999)[0],"ERROR_SUPPRESS")


if __name__=="__main__":
    unittest.main(verbosity=2)