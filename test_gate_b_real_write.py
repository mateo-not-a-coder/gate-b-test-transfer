"""Staging-only compatibility test of the actual Hermes _execute_write method.

Reads ONLY the isolated staged hermes_state.py source. No Hermes runtime import.
No reference to /data/state.db and no production service initialization.
"""
import ast
import contextlib
import pathlib
import random
import sqlite3
import threading
import time
import tempfile
import typing
import unittest

ROOT=pathlib.Path(__file__).resolve().parent
SOURCE=ROOT/"hermes_state.py"


def load_method():
    tree=ast.parse(SOURCE.read_text(encoding="utf-8"))
    klass=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=="SessionDB")
    method=next(n for n in klass.body if isinstance(n,ast.FunctionDef) and n.name=="_execute_write")
    isolated=ast.Module(body=[method],type_ignores=[])
    ast.fix_missing_locations(isolated)
    class SessionCompressionInProgressError(Exception):
        pass
    ns={
        "Callable":typing.Callable,"Optional":typing.Optional,"T":typing.TypeVar("T"),
        "sqlite3":sqlite3,"time":time,
        "SessionCompressionInProgressError":SessionCompressionInProgressError,
        "_is_no_more_rows":lambda e:False,
        "_DISK_IO_ERROR_MARKER":"disk i/o error",
        "log_write_lock_holders":lambda *args:None,
        "is_malformed_db_error":lambda e:False,
    }
    exec(compile(isolated,str(SOURCE),"exec"),ns)
    return ns["_execute_write"]


class MinimalWriter:
    _WRITE_PATIENCE_S=0.15
    _COMPRESSION_BUSY_WAIT_S=0.15
    _CHECKPOINT_EVERY_N_WRITES=100000
    _FTS_MERGE_EVERY_N_WRITES=100000

    def __init__(self,db_path,method):
        self.db_path=pathlib.Path(db_path)
        self._conn=sqlite3.connect(str(db_path),timeout=0,isolation_level=None,check_same_thread=False)
        self._lock=threading.Lock()
        self._write_count=0
        self._execute_write=method.__get__(self)
        self.checkpoints=0

    def _raise_if_db_corrupt(self):pass
    def _raise_if_db_replaced(self):pass
    def _sleep_before_write_retry(self,deadline,patience_s):
        if time.monotonic()>=deadline:
            return False
        time.sleep(.005)
        return True
    def _try_wal_checkpoint(self):self.checkpoints+=1
    def _try_incremental_merge_fts(self):pass
    def _reopen_after_close_locked(self,context):raise AssertionError("unexpected reopen")
    def _is_fts_write_corruption_error(self,exc):return False
    def _enter_fts_fail_open(self,exc):return False
    def _is_structural_corruption_error(self,exc):return False
    def _halt_db_corrupt(self,exc):raise AssertionError("unexpected corruption")

    def close(self):self._conn.close()


class RealWriteMethodTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix="gate-b-realwrite-",dir=str(ROOT))
        self.addCleanup(self.temp.cleanup)
        self.path=str(pathlib.Path(self.temp.name)/"test.db")
        self.writer=MinimalWriter(self.path,load_method())
        self.addCleanup(self.writer.close)
        self.writer._conn.execute("CREATE TABLE events(id TEXT PRIMARY KEY,value INTEGER)")

    def count(self):
        return self.writer._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    def test_real_method_commit(self):
        result=self.writer._execute_write(lambda c:c.execute("INSERT INTO events VALUES (?,?)",("x",1)).rowcount)
        self.assertEqual(result,1)
        self.assertEqual(self.count(),1)

    def test_real_method_rollback_callback_error(self):
        def fail(conn):
            conn.execute("INSERT INTO events VALUES (?,?)",("x",1))
            raise ValueError("stop")
        with self.assertRaisesRegex(ValueError,"stop"):
            self.writer._execute_write(fail)
        self.assertEqual(self.count(),0)

    def test_real_method_idempotent_duplicate(self):
        def mutate(conn):
            conn.execute("INSERT OR IGNORE INTO events VALUES (?,?)",("same",1))
            return conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        self.assertEqual(self.writer._execute_write(mutate),1)
        self.assertEqual(self.writer._execute_write(mutate),1)
        self.assertEqual(self.count(),1)

    def test_real_method_lock_exhaustion(self):
        with contextlib.closing(sqlite3.connect(self.path,timeout=0,isolation_level=None)) as locker:
            locker.execute("BEGIN IMMEDIATE")
            with self.assertRaises(sqlite3.OperationalError):
                self.writer._execute_write(lambda c:c.execute("INSERT INTO events VALUES (?,?)",("x",1)),patience_s=.025)
            locker.rollback()
        self.assertEqual(self.count(),0)

    def test_real_method_serialized_concurrent_writes(self):
        from concurrent.futures import ThreadPoolExecutor
        def write(n):
            return self.writer._execute_write(lambda c:c.execute("INSERT INTO events VALUES (?,?)",(str(n),n)).rowcount)
        with ThreadPoolExecutor(max_workers=4) as pool:
            result=list(pool.map(write,range(8)))
        self.assertEqual(result,[1]*8)
        self.assertEqual(self.count(),8)


if __name__=="__main__":
    unittest.main(verbosity=2)