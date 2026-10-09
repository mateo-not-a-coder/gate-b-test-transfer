"""Gate B staging-only adapter for Hermes-like SessionStore and SessionDB.

No gateway imports, no schema migration, and no production effects.
The caller must supply independently trusted session identity and DB-scope authority.
"""
import sqlite3


def _allowed_identity(session_key, authorize_session, authorize_database, db):
    if not isinstance(session_key,str) or not session_key.strip():
        return False
    if not callable(authorize_session) or not callable(authorize_database):
        return False
    if db is None or not callable(getattr(db, "_execute_write", None)):
        return False
    # Exceptions are handled by caller; fail closed.
    return authorize_session(session_key) is True and authorize_database(session_key,db) is True


def _resolve(store, session_key, authorize_session, authorize_database):
    if not isinstance(session_key,str) or not session_key.strip():
        return None
    if not callable(authorize_session) or authorize_session(session_key) is not True:
        return None
    db = store._db_for_key(session_key)
    if not _allowed_identity(session_key,authorize_session,authorize_database,db):
        return None
    return db


def record_owner_action(store,session_key,conversation_key,event_key,now_ms,
                        authorize_session=None,authorize_database=None):
    """Owner intake as a single Hermes _execute_write callback.

    Caller must have classified this as an intentional, trusted owner action.
    """
    if not all(isinstance(v,str) and v.strip() for v in (session_key,conversation_key,event_key)):
        return ("ERROR_SUPPRESS",None)
    if type(now_ms) is not int or now_ms < 0:
        return ("ERROR_SUPPRESS",None)
    try:
        db=_resolve(store,session_key,authorize_session,authorize_database)
        if db is None:
            return ("ERROR_SUPPRESS",None)

        def mutation(conn):
            conn.execute("INSERT OR IGNORE INTO client_takeover_state(conversation_key) VALUES (?)",(conversation_key,))
            prev=conn.execute("SELECT generation,owner_last_at_ms FROM client_takeover_state WHERE conversation_key=?",(conversation_key,)).fetchone()
            duplicate=conn.execute("SELECT generation FROM client_takeover_owner_events WHERE conversation_key=? AND event_key=?",(conversation_key,event_key)).fetchone()
            if duplicate:
                return ("DUPLICATE",duplicate[0])
            generation=prev[0]+1
            effective_ms=max(now_ms,prev[1]) if prev[1] is not None else now_ms
            conn.execute("INSERT INTO client_takeover_owner_events(conversation_key,event_key,generation,received_at_ms) VALUES (?,?,?,?)",
                         (conversation_key,event_key,generation,now_ms))
            conn.execute("UPDATE client_takeover_state SET generation=?,owner_last_at_ms=?,takeover_active=1,updated_at_ms=? WHERE conversation_key=?",
                         (generation,effective_ms,effective_ms,conversation_key))
            return ("SUPPRESS",generation)
        return db._execute_write(mutation)
    except (Exception,):
        return ("ERROR_SUPPRESS",None)


def read_delivery_permission(store,session_key,conversation_key,expected_generation,
                             authorize_session=None,authorize_database=None):
    """Conservative staging guard; uses Hermes callback API to read state.

    Production integration should use a dedicated read-only SessionDB API and
    authorization token tied to trusted gateway intake; NEVER chat-ID alone.
    """
    if not isinstance(conversation_key,str) or not conversation_key.strip() or type(expected_generation) is not int:
        return False
    try:
        db=_resolve(store,session_key,authorize_session,authorize_database)
        if db is None:
            return False
        def query(conn):
            row=conn.execute("SELECT generation,takeover_active FROM client_takeover_state WHERE conversation_key=?",(conversation_key,)).fetchone()
            return row is not None and row==(expected_generation,0)
        return db._execute_write(query) is True
    except (Exception,):
        return False