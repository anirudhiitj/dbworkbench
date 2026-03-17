"""Commit service — create, list, and retrieve versioned commits.

A commit wraps one or more SQL steps.  Each commit is assigned a
sequential commit_number, a SHA-256 hash, and an optional message.
After creating a commit the service checks if the snapshot frequency
threshold has been hit and triggers an auto-snapshot if so.
"""

from __future__ import annotations

from fastapi_backend.app.db.connection import get_connection, release_connection
from fastapi_backend.app.db import metadata_queries as mq
from fastapi_backend.app.utils.hashing import generate_commit_hash
from fastapi_backend.app.services.snapshot_service import (
    get_snapshot_frequency,
    create_snapshot,
)


def create_commit(steps: list[dict], message: str | None = None) -> dict:
    """
    Execute every SQL step, persist the commit + steps, hash it, and
    optionally trigger a snapshot.

    Parameters
    ----------
    steps : list[dict]
        Each dict has ``sql`` (str) and ``step_type`` (str, default "DML").
    message : str | None
        Optional human-readable commit message.

    Returns
    -------
    dict  with commit_id, commit_number, hash, message, steps, created_at.
    """
    conn = get_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        # 1. Placeholder hash — we update it after we know commit_number + timestamp
        cur.execute(mq.INSERT_COMMIT, ("pending", message))
        commit_id, commit_number, created_at = cur.fetchone()

        # 2. Execute and record each step
        step_results: list[dict] = []
        for idx, step in enumerate(steps, start=1):
            sql = step["sql"]
            step_type = step.get("step_type", "DML")

            # Execute the actual user SQL on the database
            cur.execute(sql)

            # Record metadata
            cur.execute(mq.INSERT_COMMIT_STEP, (str(commit_id), idx, sql, step_type))
            step_id = cur.fetchone()[0]
            step_results.append(
                {
                    "step_id": step_id,
                    "step_order": idx,
                    "sql_command": sql,
                    "step_type": step_type,
                }
            )

        # 3. Generate deterministic hash and update the commit row
        sql_list = [s["sql"] for s in steps]
        commit_hash = generate_commit_hash(commit_number, str(created_at), sql_list)
        cur.execute(
            "UPDATE commits SET hash = %s WHERE commit_id = %s",
            (commit_hash, str(commit_id)),
        )

        # 4. Auto-snapshot if we've hit the frequency threshold
        frequency = get_snapshot_frequency(cur=cur)
        if commit_number % frequency == 0:
            create_snapshot(conn=conn, commit_number=commit_number)

        conn.commit()
        return {
            "commit_id": str(commit_id),
            "commit_number": commit_number,
            "hash": commit_hash,
            "message": message,
            "steps": step_results,
            "created_at": created_at.isoformat(),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = True
        release_connection(conn)


def list_commits() -> list[dict]:
    """Return every commit ordered by commit_number."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(mq.SELECT_ALL_COMMITS)
        return [
            {
                "commit_id": str(r[0]),
                "commit_number": r[1],
                "hash": r[2],
                "message": r[3],
                "created_at": r[4].isoformat(),
            }
            for r in cur.fetchall()
        ]
    finally:
        release_connection(conn)


def get_commit(commit_id: str) -> dict | None:
    """Return a single commit with its steps."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(mq.SELECT_COMMIT_BY_ID, (commit_id,))
        row = cur.fetchone()
        if row is None:
            return None

        cur.execute(mq.SELECT_STEPS_BY_COMMIT, (commit_id,))
        steps = [
            {
                "step_id": s[0],
                "step_order": s[2],
                "sql_command": s[3],
                "step_type": s[4],
            }
            for s in cur.fetchall()
        ]

        return {
            "commit_id": str(row[0]),
            "commit_number": row[1],
            "hash": row[2],
            "message": row[3],
            "steps": steps,
            "created_at": row[4].isoformat(),
        }
    finally:
        release_connection(conn)
