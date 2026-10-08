"""Staging-only durable Gate B turn association; NOT executable queue integration."""
import contextlib
import json
import sqlite3

from gate_b_context import _valid, claim_pending_context


def initialize_turn_schema(path):
    with contextlib.closing(sqlite3.connect(path,timeout=5)) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS client_takeover_resumed_turns (
            conversation_key TEXT NOT NULL,
            resume_event_key TEXT NOT NULL,
            generation INTEGER NOT NULL,
            turn_key TEXT NOT NULL,
            context_json TEXT NOT NULL,
            linked_at_ms INTEGER NOT NULL,
            PRIMARY KEY(conversation_key,resume_event_key),
            UNIQUE(conversation_key,turn_key)
        )""")
        db.commit()


def associate_resumed_turn(path,conversation_key,resume_event_key,generation,turn_key,now_ms):
    """Durably link historical snapshot and one trigger, in one SQLite transaction.

    Returns READY or EXISTING and an immutable history snapshot. No dispatch takes
    place here; gateway MUST use unique turn_key to avoid second execution.
    """
    if not all(map(_valid,(conversation_key,resume_event_key,turn_key))) or type(generation) is not int or type(now_ms) is not int or generation<0 or now_ms<0:
        return ("ERROR_SUPPRESS",[])
    # A prior committed claim can be recovered by the same turn identity.
    status, _ = claim_pending_context(path,conversation_key,resume_event_key,generation,turn_key)
    if status=="ERROR_SUPPRESS":
        return ("ERROR_SUPPRESS",[])
    db=None
    try:
        db=sqlite3.connect(path,timeout=5,isolation_level=None)
        db.execute("BEGIN IMMEDIATE")
        state=db.execute("SELECT generation,takeover_active FROM client_takeover_state WHERE conversation_key=?",(conversation_key,)).fetchone()
        if state!=(generation,0):
            db.rollback()
            return ("ERROR_SUPPRESS",[])
        prior=db.execute("SELECT generation,turn_key,context_json FROM client_takeover_resumed_turns WHERE conversation_key=? AND resume_event_key=?",(conversation_key,resume_event_key)).fetchone()
        if prior:
            if prior[0]!=generation or prior[1]!=turn_key:
                db.rollback()
                return ("ERROR_SUPPRESS",[])
            db.commit()
            return ("EXISTING",json.loads(prior[2]))
        claim=db.execute("SELECT generation,turn_key,linked_at_ms FROM client_takeover_context_claims WHERE conversation_key=? AND resume_event_key=?",(conversation_key,resume_event_key)).fetchone()
        if not claim or claim[0]!=generation or claim[1]!=turn_key or claim[2] is not None:
            db.rollback()
            return ("ERROR_SUPPRESS",[])
        trigger=db.execute("SELECT 1 FROM client_takeover_pending_inputs WHERE conversation_key=? AND event_key=?",(conversation_key,resume_event_key)).fetchone()
        if not trigger:
            db.rollback()
            return ("ERROR_SUPPRESS",[])
        rows=db.execute("SELECT event_key,content_json FROM client_takeover_pending_inputs WHERE conversation_key=? AND consumed_by_event_key=? AND consumed_at_ms IS NULL ORDER BY arrival_seq",(conversation_key,resume_event_key)).fetchall()
        history=[{"event_key":k,"content":json.loads(v),"role":"historical_client_context_only"} for k,v in rows]
        payload=json.dumps(history,ensure_ascii=False,separators=(",",":"))
        db.execute("INSERT INTO client_takeover_resumed_turns(conversation_key,resume_event_key,generation,turn_key,context_json,linked_at_ms) VALUES (?,?,?,?,?,?)",(conversation_key,resume_event_key,generation,turn_key,payload,now_ms))
        db.execute("UPDATE client_takeover_context_claims SET linked_at_ms=? WHERE conversation_key=? AND resume_event_key=? AND linked_at_ms IS NULL",(now_ms,conversation_key,resume_event_key))
        db.execute("UPDATE client_takeover_pending_inputs SET consumed_at_ms=? WHERE conversation_key=? AND consumed_by_event_key=? AND consumed_at_ms IS NULL",(now_ms,conversation_key,resume_event_key))
        db.commit()
        return ("READY",history)
    except (sqlite3.Error,OSError,ValueError,TypeError):
        if db is not None and db.in_transaction:
            db.rollback()
        return ("ERROR_SUPPRESS",[])
    finally:
        if db is not None:
            db.close()