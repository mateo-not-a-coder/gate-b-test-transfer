"""Staging-only Hermes callback integration for client admission and durable resume.

No Hermes runtime imports. Caller must provide trusted session and verified database
scope authorizers. SessionDB._execute_write owns BEGIN/COMMIT/ROLLBACK.
"""
import json
from gate_b_persistence_adapter import _resolve

WINDOW_MS=1800000
MAX_CONTEXT_ITEMS=50
MAX_CONTEXT_BYTES=32768


def _inputs_ok(session,conversation,event,now):
    return all(isinstance(x,str) and 0<len(x.strip())<=512 for x in (session,conversation,event)) and type(now) is int and now>=0


def admit_client(store,session_key,conversation_key,event_key,now_ms,content_json,
                 authorize_session=None,authorize_database=None):
    """Return (SUPPRESS | ALLOW_NEW_TURN | DUPLICATE | ERROR_SUPPRESS, generation).

    Every event gets a durable unique ledger row, including active-mode messages.
    For active-mode messages the ledger is marked consumed immediately; downstream
    delivery still needs gateway dispatch identity and output fencing.
    """
    if not _inputs_ok(session_key,conversation_key,event_key,now_ms):
        return ("ERROR_SUPPRESS",None)
    if not isinstance(content_json,str) or len(content_json.encode("utf-8"))>8192:
        return ("ERROR_SUPPRESS",None)
    try:
        content=json.loads(content_json)
        if not isinstance(content,(dict,list)):
            return ("ERROR_SUPPRESS",None)
        db=_resolve(store,session_key,authorize_session,authorize_database)
        if db is None:
            return ("ERROR_SUPPRESS",None)

        def mutation(conn):
            conn.execute("INSERT OR IGNORE INTO client_takeover_state(conversation_key) VALUES (?)",(conversation_key,))
            previous=conn.execute(
                "SELECT generation,owner_last_at_ms,takeover_active,updated_at_ms "
                "FROM client_takeover_state WHERE conversation_key=?",(conversation_key,)).fetchone()
            generation,owner_ms,active,updated_ms=previous
            old=conn.execute(
                "SELECT owner_generation,kind FROM client_takeover_pending_inputs "
                "WHERE conversation_key=? AND event_key=?",(conversation_key,event_key)).fetchone()
            if old is not None:
                return ("DUPLICATE",generation)
            if now_ms<updated_ms:
                return ("ERROR_SUPPRESS",None)
            if active and (owner_ms is None or now_ms<owner_ms):
                return ("ERROR_SUPPRESS",None)
            waiting=bool(active and now_ms < owner_ms+WINDOW_MS)
            resumed=bool(active and not waiting)
            if resumed:
                generation+=1
                conn.execute("UPDATE client_takeover_state SET generation=?,takeover_active=0,updated_at_ms=? "
                             "WHERE conversation_key=?",(generation,now_ms,conversation_key))
            kind="historical_context_only" if waiting else "new_turn_trigger"
            consumed_at=now_ms if (not waiting and not resumed) else None
            conn.execute(
                "INSERT INTO client_takeover_pending_inputs "
                "(conversation_key,event_key,received_at_ms,owner_generation,kind,content_json,consumed_at_ms) "
                "VALUES (?,?,?,?,?,?,?)",
                (conversation_key,event_key,now_ms,generation,kind,content_json,consumed_at))
            return ("SUPPRESS" if waiting else "ALLOW_NEW_TURN",generation)
        return db._execute_write(mutation)
    except Exception:
        return ("ERROR_SUPPRESS",None)


def link_resumed_turn(store,session_key,conversation_key,resume_event_key,generation,turn_key,now_ms,
                      authorize_session=None,authorize_database=None):
    """Single transaction: claim bounded historical rows, snapshot, durable link and consume.

    Returns READY with historical data, EXISTING with prior snapshot, or ERROR_SUPPRESS.
    Does not launch an agent run; actual gateway dispatcher must fence by turn_key.
    """
    if not _inputs_ok(session_key,conversation_key,resume_event_key,now_ms):
        return ("ERROR_SUPPRESS",[])
    if not isinstance(turn_key,str) or not turn_key.strip() or len(turn_key)>512 or type(generation) is not int or generation<0:
        return ("ERROR_SUPPRESS",[])
    try:
        db=_resolve(store,session_key,authorize_session,authorize_database)
        if db is None:
            return ("ERROR_SUPPRESS",[])

        def mutation(conn):
            state=conn.execute("SELECT generation,takeover_active FROM client_takeover_state "
                               "WHERE conversation_key=?",(conversation_key,)).fetchone()
            if state!=(generation,0):
                return ("ERROR_SUPPRESS",[])
            event=conn.execute("SELECT kind FROM client_takeover_pending_inputs "
                               "WHERE conversation_key=? AND event_key=?",
                               (conversation_key,resume_event_key)).fetchone()
            if event!=("new_turn_trigger",):
                return ("ERROR_SUPPRESS",[])
            prior=conn.execute("SELECT generation,turn_key,context_json FROM client_takeover_resumed_turns "
                               "WHERE conversation_key=? AND resume_event_key=?",
                               (conversation_key,resume_event_key)).fetchone()
            if prior:
                if prior[0]!=generation or prior[1]!=turn_key:
                    return ("ERROR_SUPPRESS",[])
                return ("EXISTING",json.loads(prior[2]))
            rows=conn.execute("SELECT arrival_seq,event_key,content_json FROM client_takeover_pending_inputs "
                              "WHERE conversation_key=? AND kind='historical_context_only' "
                              "AND consumed_at_ms IS NULL AND consumed_by_event_key IS NULL "
                              "AND owner_generation<? ORDER BY arrival_seq LIMIT ?",
                              (conversation_key,generation,MAX_CONTEXT_ITEMS)).fetchall()
            selected=[]
            used=0
            for seq,event_id,raw in rows:
                obj=json.loads(raw)
                if not isinstance(obj,(dict,list)):
                    raise ValueError("invalid history")
                size=len(raw.encode("utf-8"))
                if used+size>MAX_CONTEXT_BYTES:
                    break
                used+=size
                selected.append((seq,{"role":"historical_client_context_only","event_key":event_id,"content":obj}))
            history=[item for _,item in selected]
            conn.execute("INSERT INTO client_takeover_resumed_turns "
                         "(conversation_key,resume_event_key,generation,turn_key,context_json,linked_at_ms) "
                         "VALUES (?,?,?,?,?,?)",
                         (conversation_key,resume_event_key,generation,turn_key,json.dumps(history),now_ms))
            for seq,_ in selected:
                conn.execute("UPDATE client_takeover_pending_inputs "
                             "SET consumed_by_event_key=?,consumed_at_ms=? WHERE arrival_seq=? "
                             "AND consumed_at_ms IS NULL",(resume_event_key,now_ms,seq))
            return ("READY",history)
        return db._execute_write(mutation)
    except Exception:
        return ("ERROR_SUPPRESS",[])