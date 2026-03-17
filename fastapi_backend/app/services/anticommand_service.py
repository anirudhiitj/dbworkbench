"""Anti-command service — store and retrieve anti-commands.

The *generation* logic for anti-commands is NOT implemented here.
Your friend provides that; this module only exposes storage + retrieval.
"""

from __future__ import annotations

from fastapi_backend.app.db.connection import get_connection, release_connection
from fastapi_backend.app.db import metadata_queries as mq


def store_anti_command(commit_id: str, step_id: int, anti_sql: str) -> dict:
    """
    Persist a single anti-command for a commit step.

    Parameters
    ----------
    commit_id : str (UUID)
    step_id   : int
    anti_sql  : str   — the inverse SQL statement

    Returns
    -------
    dict with ``id``, ``commit_id``, ``step_id``, ``anti_sql``.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(mq.INSERT_ANTI_COMMAND, (commit_id, step_id, anti_sql))
        anti_id = cur.fetchone()[0]
        conn.commit()
        return {
            "id": anti_id,
            "commit_id": commit_id,
            "step_id": step_id,
            "anti_sql": anti_sql,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        release_connection(conn)


def get_anti_commands_for_commit(commit_id: str) -> list[dict]:
    """Return all anti-commands associated with a commit, ordered by step."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(mq.SELECT_ANTI_COMMANDS_BY_COMMIT, (commit_id,))
        return [
            {
                "id": r[0],
                "commit_id": str(r[1]),
                "step_id": r[2],
                "anti_sql": r[3],
            }
            for r in cur.fetchall()
        ]
    finally:
        release_connection(conn)
