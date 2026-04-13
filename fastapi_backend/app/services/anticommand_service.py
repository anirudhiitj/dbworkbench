<<<<<<< HEAD
"""Anti-command service — store and retrieve anti-commands.

The *generation* logic for anti-commands is NOT implemented here.
Your friend provides that; this module only exposes storage + retrieval.
=======
"""Anti-command service — retrieve inverse operations via Django ORM.

Storage is handled atomically inside record_commit() (Django).
This module only exposes retrieval for the UI / debugging.
>>>>>>> integration
"""

from __future__ import annotations

<<<<<<< HEAD
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
=======
from core.models import InverseOperation


def get_inverse_for_version(user_id: int, version_id: str) -> dict | None:
    """Return the inverse operation for a given version_id, or None."""
    try:
        inv = InverseOperation.objects.get(
            version_id=version_id,
            commit__user_id=user_id,
        )
    except InverseOperation.DoesNotExist:
        return None

    return {
        "version_id": inv.version_id,
        "inverse_sql": inv.inverse_sql,
        "commit_version_id": inv.commit.version_id,
    }


def get_inverses_for_profile(user_id: int, connection_profile_id: int) -> list[dict]:
    """Return all inverse operations for a user+profile."""
    inverses = InverseOperation.objects.filter(
        commit__user_id=user_id,
        commit__connection_profile_id=connection_profile_id,
    ).select_related("commit").order_by("commit__timestamp")

    return [
        {
            "version_id": inv.version_id,
            "inverse_sql": inv.inverse_sql,
            "commit_version_id": inv.commit.version_id,
        }
        for inv in inverses
    ]
>>>>>>> integration
