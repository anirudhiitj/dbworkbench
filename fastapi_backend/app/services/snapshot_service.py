"""Snapshot service — create, restore, list snapshots and manage frequency.

Snapshots are pg_dump SQL files uploaded to S3.  The snapshot frequency
(1–5 commits) is stored in the ``snapshot_config`` table so it can be
changed at runtime by the user.
"""

from __future__ import annotations

import os
import subprocess
import tempfile

from fastapi_backend.app.db.connection import get_connection, release_connection
from fastapi_backend.app.db import metadata_queries as mq
from fastapi_backend.app.utils.s3_utils import upload_snapshot, download_snapshot
from fastapi_backend.app.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    SNAPSHOT_FREQUENCY_DEFAULT,
)


# ── Frequency management ──────────────────────────────────────────────────────

def get_snapshot_frequency(cur=None) -> int:
    """
    Read the current snapshot frequency from the DB.

    If an existing cursor is provided it is reused (useful inside an
    ongoing transaction); otherwise a fresh connection is obtained.
    """
    if cur is not None:
        cur.execute(mq.SELECT_SNAPSHOT_FREQUENCY)
        row = cur.fetchone()
        return row[0] if row else SNAPSHOT_FREQUENCY_DEFAULT

    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute(mq.SELECT_SNAPSHOT_FREQUENCY)
        row = c.fetchone()
        return row[0] if row else SNAPSHOT_FREQUENCY_DEFAULT
    finally:
        release_connection(conn)


def set_snapshot_frequency(frequency: int) -> int:
    """Update the snapshot frequency (1–5) and return the new value."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(mq.UPDATE_SNAPSHOT_FREQUENCY, (frequency,))
        conn.commit()
        return frequency
    except Exception:
        conn.rollback()
        raise
    finally:
        release_connection(conn)


# ── Snapshot creation / restoration ───────────────────────────────────────────

def create_snapshot(
    conn=None,
    commit_number: int | None = None,
) -> dict:
    """
    Run ``pg_dump``, upload the result to S3, and record snapshot metadata.

    If *conn* is provided the metadata INSERT reuses that connection (the
    caller manages the transaction).  Otherwise a new connection is used.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    tmp_path = None
    try:
        # Resolve commit_number if not given
        if commit_number is None:
            cur = conn.cursor()
            cur.execute(mq.SELECT_LATEST_COMMIT_NUMBER)
            commit_number = cur.fetchone()[0]

        s3_key = f"snapshots/v{commit_number}.sql"

        # pg_dump to a temp file
        with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as tmp:
            tmp_path = tmp.name

        env = os.environ.copy()
        env["PGPASSWORD"] = DB_PASSWORD
        subprocess.run(
            [
                "pg_dump",
                "-h", DB_HOST,
                "-p", str(DB_PORT),
                "-U", DB_USER,
                "-d", DB_NAME,
                "-f", tmp_path,
            ],
            env=env,
            check=True,
        )

        upload_snapshot(tmp_path, s3_key)

        # Record metadata
        cur = conn.cursor()
        cur.execute(mq.INSERT_SNAPSHOT, (commit_number, s3_key))
        row = cur.fetchone()

        if own_conn:
            conn.commit()

        return {
            "id": row[0],
            "commit_number": commit_number,
            "s3_key": s3_key,
            "created_at": row[1].isoformat(),
        }
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
        if own_conn:
            release_connection(conn)


def restore_snapshot(s3_key: str):
    """Download a snapshot from S3 and restore it via ``psql``."""
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as tmp:
            tmp_path = tmp.name

        download_snapshot(s3_key, tmp_path)

        env = os.environ.copy()
        env["PGPASSWORD"] = DB_PASSWORD
        subprocess.run(
            [
                "psql",
                "-h", DB_HOST,
                "-p", str(DB_PORT),
                "-U", DB_USER,
                "-d", DB_NAME,
                "-f", tmp_path,
            ],
            env=env,
            check=True,
        )
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass


# ── Listing ───────────────────────────────────────────────────────────────────

def list_snapshots() -> list[dict]:
    """Return all snapshot metadata records."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(mq.SELECT_ALL_SNAPSHOTS)
        return [
            {
                "id": r[0],
                "commit_number": r[1],
                "s3_key": r[2],
                "created_at": r[3].isoformat(),
            }
            for r in cur.fetchall()
        ]
    finally:
        release_connection(conn)
