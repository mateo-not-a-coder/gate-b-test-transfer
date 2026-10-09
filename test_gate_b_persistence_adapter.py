"""Safe fake SessionStore/SessionDB tests. Uses disposable temp SQLite only."""
import contextlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
import gate_b_persistence_adapter as adapter

ROOT=Path(__file__).resolve().parent


class FakeDB:
    def __init__(self,path,fail=False):
        self.path=path
        self.fail=fail
        self.attempts=0

    def _execute_write(self,fn,patience_s=None):
        self.attempts+=1
        if self.fail:
            raise sqlite3.OperationalError("simulated failure")
        with contextlib.closing(sqlite3.connect(self.path,isolation_level=None)) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                result=fn(conn)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise


class FakeStore:
    def __init__(self,dbmap):
        self.dbmap=dbmap
        self.lookups=[]

    def _db_for_key(self,key):
        self.lookups.append(key)
        return self.dbmap.get(key)


def authorize_session(key):
    return key in ("trusted:a","trusted:b")


def authorize_database(key,db):
    # In the live gateway this MUST resolve independently from trusted profile routing.
    return db is not None and getattr(db,"profile",None)==key


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=str(ROOT),prefix="gate-b-adapter-")
        self.addCleanup(self.tmp.cleanup)
        self.dbs={}
        for profile in ("trusted:a","trusted:b"):
            dbpath=str(Path(self.tmp.name)/(profile.replace(":","_")+".sqlite"))
            with contextlib.closing(sqlite3.connect(dbpath)) as conn:
                conn.executescript("""CREATE TABLE client_takeover_state(
                    conversation_key TEXT PRIMARY KEY,generation INTEGER NOT NULL DEFAULT 0,
                    owner_last_at_ms INTEGER,takeover_active INTEGER NOT NULL DEFAULT 0,updated_at_ms INTEGER NOT NULL DEFAULT 0);
                    CREATE TABLE client_takeover_owner_events(
                    conversation_key TEXT NOT NULL,event_key TEXT NOT NULL,generation INTEGER NOT NULL,
                    received_at_ms INTEGER NOT NULL,PRIMARY KEY(conversation_key,event_key));""")
                conn.commit()
            db=FakeDB(dbpath)
            db.profile=profile
            self.dbs[profile]=db
        self.store=FakeStore(self.dbs)

    def owner(self,key="trusted:a",conversation="client-x",event="owner-1",ms=1000,store=None):
        return adapter.record_owner_action(store or self.store,key,conversation,event,ms,authorize_session,authorize_database)

    def test_profile_isolation_and_dedup(self):
        self.assertEqual(self.owner(),("SUPPRESS",1))
        self.assertEqual(self.owner(ms=3000),("DUPLICATE",1))
        self.assertEqual(self.owner(key="trusted:b"),("SUPPRESS",1))
        self.assertEqual(self.dbs["trusted:a"].attempts,2)
        self.assertEqual(self.dbs["trusted:b"].attempts,1)

    def test_unknown_profile_never_resolves(self):
        self.assertEqual(self.owner(key="unknown"),("ERROR_SUPPRESS",None))
        self.assertNotIn("unknown",self.store.lookups)

    def test_missing_profile_fails_closed(self):
        store=FakeStore({})
        self.assertEqual(self.owner(store=store),("ERROR_SUPPRESS",None))

    def test_cross_profile_db_rejected(self):
        store=FakeStore({"trusted:a":self.dbs["trusted:b"]})
        self.assertEqual(self.owner(store=store),("ERROR_SUPPRESS",None))
        self.assertEqual(self.dbs["trusted:b"].attempts,0)

    def test_pinned_wrong_db_rejected(self):
        store=FakeStore({"trusted:a":self.dbs["trusted:b"]})
        self.assertEqual(self.owner(store=store),("ERROR_SUPPRESS",None))

    def test_transaction_failure_suppresses(self):
        self.dbs["trusted:a"].fail=True
        self.assertEqual(self.owner(),("ERROR_SUPPRESS",None))

    def test_missing_authorizers_suppress(self):
        result=adapter.record_owner_action(self.store,"trusted:a","client-x","owner-1",1000)
        self.assertEqual(result,("ERROR_SUPPRESS",None))
        self.assertEqual(self.store.lookups,[])

    def test_duplicate_transaction_callback_retry(self):
        self.assertEqual(self.owner(),("SUPPRESS",1))
        # Simulate Hermes invoking same logical action after commit acknowledgment lost.
        self.assertEqual(self.owner(),("DUPLICATE",1))
        with contextlib.closing(sqlite3.connect(self.dbs["trusted:a"].path)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM client_takeover_owner_events").fetchone()[0],1)

    def test_delivery_denied_during_takeover(self):
        self.assertEqual(self.owner(),("SUPPRESS",1))
        self.assertFalse(adapter.read_delivery_permission(self.store,"trusted:a","client-x",1,authorize_session,authorize_database))
        self.assertFalse(adapter.read_delivery_permission(self.store,"trusted:b","client-x",1,authorize_session,authorize_database))
        self.assertFalse(adapter.read_delivery_permission(self.store,"unknown","client-x",0,authorize_session,authorize_database))


if __name__=="__main__":
    unittest.main(verbosity=2)