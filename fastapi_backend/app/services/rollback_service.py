"""Rollback service — restore database to a previous commit.

Algorithm
---------
1. Look up the target commit's ``commit_number``.
2. Find the nearest snapshot whose ``commit_number ≤ target``.
3. Restore that snapshot via ``psql``.
4. Fetch every anti-command for commits **after** the target (in reverse
   order) and execute them to undo the later changes.
5. Return a summary.
"""

from __future__ import annotations

from fastapi_backend.app.db.connection import get_connection, release_connection
from fastapi_backend.app.db import metadata_queries as mq
from fastapi_backend.app.services.snapshot_service import restore_snapshot


def rollback_to_commit(target_commit_id: str) -> dict:
    """
    Roll the database back to the state it was in *after* the given commit.

    Parameters
    ----------
    target_commit_id : str (UUID)
        The commit to roll back to.  The user picks this from the UI.

    Returns
    -------
    dict  with rolled_back_to, snapshot_restored, anti_commands_applied, status.
    """
    conn = get_connection()
    snapshot_info: str | None = None
    applied = 0
    try:
        cur = conn.cursor()

        # 1. Resolve target commit
        cur.execute(mq.SELECT_COMMIT_BY_ID, (target_commit_id,))
        target = cur.fetchone()
        if target is None:
            raise ValueError(f"Commit {target_commit_id} not found")

        target_number = target[1]  # commit_number

        # 2. Ensure we're not already at or ahead of the target
        cur.execute(mq.SELECT_LATEST_COMMIT_NUMBER)
        current_number = cur.fetchone()[0]
        if target_number >= current_number:
            raise ValueError(
                f"Target commit #{target_number} is not behind the current "
                f"commit #{current_number}"
            )

        # 3. Find nearest snapshot ≤ target
        cur.execute(mq.SELECT_NEAREST_SNAPSHOT_BEFORE, (target_number,))
        snap_row = cur.fetchone()

        # We are done with metadata lookups on this connection. End the
        # transaction and release before performing any out-of-band restore.
        conn.commit()
        cur.close()
        release_connection(conn)
        conn = None

        # Perform the snapshot restore out-of-band (via psql, etc.).
        if snap_row is not None:
            snapshot_s3_key = snap_row[2]
            restore_snapshot(snapshot_s3_key)
            snapshot_info = snapshot_s3_key

        # Obtain a fresh connection for fetching and applying anti-commands
        conn = get_connection()
        cur = conn.cursor()

        # 4. Fetch anti-commands for commits AFTER the target, reversed
        cur.execute(mq.SELECT_ANTI_COMMANDS_FOR_ROLLBACK, (target_number,))
        anti_rows = cur.fetchall()

        for row in anti_rows:
            anti_sql = row[3]
            cur.execute(anti_sql)
            applied += 1

        conn.commit()

        return {
            "rolled_back_to": target_commit_id,
            "snapshot_restored": snapshot_info,
            "anti_commands_applied": applied,
            "status": "success",
        }
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if conn is not None:
            release_connection(conn)
