"""Staging-only Gate B context claiming. Requires the existing Gate B three-table schema."""
import contextlib
import json
import sqlite3

MAX_ITEMS = 50
MAX_CONTEXT_BYTES = 32768


def _valid(value):
    return isinstance(value, str) and bool(value.strip()) and len(value) <= 512


def initialize_context_schema(path):
    """Staging additive schema; call only on disposable staging DBs."""
    with contextlib.closing(sqlite3.connect(path, timeout=5)) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS client_takeover_context_claims (
            conversation_key TEXT NOT NULL,
            resume_event_key TEXT NOT NULL,
            generation INTEGER NOT NULL,
            turn_key TEXT NOT NULL,
            linked_at_ms INTEGER,
            PRIMARY KEY(conversation_key,resume_event_key),
            UNIQUE(conversation_key,turn_key)
        )""")
        db.commit()


def claim_pending_context(path, conversation_key, resume_event_key, generation, turn_key):
    """Return (status, [(event_key, content_json), ...]); never return executable events."""
    if not all(map(_valid, (conversation_key,resume_event_key,turn_key))) or type(generation) is not int or generation < 0:
        return ("ERROR_SUPPRESS", [])
    db = None
    try:
        db = sqlite3.connect(path, timeout=5, isolation_level=None)
        db.execute("BEGIN IMMEDIATE")
        state = db.execute(
            "SELECT generation,takeover_active FROM client_takeover_state WHERE conversation_key=?",
            (conversation_key,)).fetchone()
        if state != (generation, 0):
            db.rollback()
            return ("ERROR_SUPPRESS", [])
        trigger = db.execute(
            "SELECT 1 FROM client_takeover_pending_inputs WHERE conversation_key=? AND event_key=?",
            (conversation_key,resume_event_key)).fetchone()
        if not trigger:
            db.rollback()
            return ("ERROR_SUPPRESS", [])
        claim = db.execute(
            "SELECT generation,turn_key,linked_at_ms FROM client_takeover_context_claims "
            "WHERE conversation_key=? AND resume_event_key=?",
            (conversation_key,resume_event_key)).fetchone()
        if claim and (claim[0] != generation or claim[1] != turn_key):
            db.rollback()
            return ("ERROR_SUPPRESS", [])
        if not claim:
            db.execute(
                "INSERT INTO client_takeover_context_claims"
                "(conversation_key,resume_event_key,generation,turn_key) VALUES (?,?,?,?)",
                (conversation_key,resume_event_key,generation,turn_key))
            rows = db.execute(
                "SELECT arrival_seq,content_json FROM client_takeover_pending_inputs "
                "WHERE conversation_key=? AND event_key<>? AND owner_generation<? "
                "AND consumed_by_event_key IS NULL AND consumed_at_ms IS NULL "
                "ORDER BY arrival_seq LIMIT ?",
                (conversation_key,resume_event_key,generation,MAX_ITEMS)).fetchall()
            total = 0
            accepted = []
            for seq,raw in rows:
                obj = json.loads(raw)
                if not isinstance(obj,(dict,list)):
                    raise ValueError("Invalid context JSON")
                size=len(raw.encode("utf-8"))
                if total+size > MAX_CONTEXT_BYTES:
                    break
                total+=size
                accepted.append(seq)
            for seq in accepted:
                db.execute(
                    "UPDATE client_takeover_pending_inputs SET consumed_by_event_key=? "
                    "WHERE arrival_seq=? AND consumed_by_event_key IS NULL",
                    (resume_event_key,seq))
        rows=db.execute(
            "SELECT event_key,content_json FROM client_takeover_pending_inputs "
            "WHERE conversation_key=? AND consumed_by_event_key=? AND consumed_at_ms IS NULL "
            "ORDER BY arrival_seq",
            (conversation_key,resume_event_key)).fetchall()
        db.commit()
        return ("ALREADY_LINKED" if claim and claim[2] is not None else "CONTEXT_ONLY", rows)
    except (sqlite3.Error,OSError,ValueError,TypeError):
        if db is not None and db.in_transaction:
            db.rollback()
        return ("ERROR_SUPPRESS", [])
    finally:
        if db is not None:
            db.close()


def commit_context_consumption(path, conversation_key, resume_event_key, generation, turn_key, linked_at_ms):
    """Finalize only once durable association of this turn with its history is confirmed."""
    if not all(map(_valid,(conversation_key,resume_event_key,turn_key))) or type(generation) is not int or type(linked_at_ms) is not int or linked_at_ms < 0:
        return "ERROR_SUPPRESS"
    db=None
    try:
        db=sqlite3.connect(path,timeout=5,isolation_level=None)
        db.execute("BEGIN IMMEDIATE")
        claim=db.execute(
            "SELECT generation,turn_key,linked_at_ms FROM client_takeover_context_claims "
            "WHERE conversation_key=? AND resume_event_key=?",
            (conversation_key,resume_event_key)).fetchone()
        if not claim or claim[:2]!=(generation,turn_key):
            db.rollback()
            return "ERROR_SUPPRESS"
        if claim[2] is not None:
            db.commit()
            return "DUPLICATE_COMMIT"
        # Caller guarantees turn association has been durably persisted before this call.
        db.execute(
            "UPDATE client_takeover_context_claims SET linked_at_ms=? "
            "WHERE conversation_key=? AND resume_event_key=? AND linked_at_ms IS NULL",
            (linked_at_ms,conversation_key,resume_event_key))
        db.execute(
            "UPDATE client_takeover_pending_inputs SET consumed_at_ms=? "
            "WHERE conversation_key=? AND consumed_by_event_key=? AND consumed_at_ms IS NULL",
            (linked_at_ms,conversation_key,resume_event_key))
        db.commit()
        return "CONSUMED"
    except (sqlite3.Error,OSError):
        if db is not None and db.in_transaction:
            db.rollback()
        return "ERROR_SUPPRESS"
    finally:
        if db is not None:
            db.close()