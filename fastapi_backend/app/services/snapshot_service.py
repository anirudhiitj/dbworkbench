<<<<<<< HEAD
"""Snapshot service — create, restore, list snapshots and manage frequency.

Snapshots are pg_dump SQL files uploaded to S3.  The snapshot frequency
(1–5 commits) is stored in the ``snapshot_config`` table so it can be
changed at runtime by the user.
=======
"""Snapshot service — create, restore, list snapshots on the USER's database.

Snapshots are stored as pickle+gzip archives containing:
  - schema_ddl: full DDL from pg_dump --schema-only --clean --if-exists
  - tables: {table_name: {"columns": [...], "rows": [(...), ...]}}

pg_dump (schema-only) and psql target the user's external database (via
ConnectionProfile credentials), never WEAVE-DB's internal database.
Metadata is managed via Django ORM (Snapshot, SnapshotPolicy).
>>>>>>> integration
"""

from __future__ import annotations

<<<<<<< HEAD
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
=======
import gzip
import logging
import os
import pickle
import shutil
import subprocess
import tempfile
import uuid

from psycopg2 import sql as pg_sql
from psycopg2.extras import execute_values

from connections.models import ConnectionProfile
from core.models import Snapshot, SnapshotPolicy

from fastapi_backend.app.db.connection import get_user_connection
from fastapi_backend.app.utils.s3_utils import upload_snapshot, download_snapshot
from fastapi_backend.app.config import SNAPSHOT_FREQUENCY_DEFAULT

logger = logging.getLogger(__name__)


def _find_pg_bin(name: str) -> str:
    """Return the full path to a PostgreSQL binary (pg_dump / psql)."""
    found = shutil.which(name)
    if found:
        return found
    # Windows default install location
    for ver in ("18", "17", "16", "15", "14"):
        candidate = os.path.join(
            os.environ.get("ProgramFiles", r"C:\Program Files"),
            "PostgreSQL", ver, "bin", f"{name}.exe",
        )
        if os.path.isfile(candidate):
            return candidate
    raise FileNotFoundError(f"'{name}' not found on PATH or in standard PostgreSQL install directories")


# -- Frequency management ------------------------------------------------------

def get_snapshot_frequency(connection_profile_id: int) -> int:
    """Read the snapshot frequency for a connection profile."""
    try:
        policy = SnapshotPolicy.objects.get(connection_profile_id=connection_profile_id)
        return policy.frequency
    except SnapshotPolicy.DoesNotExist:
        return SNAPSHOT_FREQUENCY_DEFAULT


def set_snapshot_frequency(user_id: int, connection_profile_id: int, frequency: int) -> int:
    """Update (or create) the snapshot frequency for a connection profile."""
    profile = ConnectionProfile.objects.get(id=connection_profile_id, user_id=user_id)
    policy, _ = SnapshotPolicy.objects.update_or_create(
        connection_profile=profile,
        defaults={"frequency": frequency},
    )
    return policy.frequency


# -- Snapshot data upload (pickle → S3) ----------------------------------------

def upload_snapshot_data(connection_profile: ConnectionProfile, s3_key: str) -> None:
    """Capture the user's DB as a pickle snapshot and upload to S3.

    1. pg_dump --schema-only for DDL (indexes, constraints, sequences, etc.)
    2. SELECT * from every public table → Python tuples
    3. pickle.dumps({schema_ddl, tables}) → gzip → S3
    """
    # 1. Schema DDL
    password = connection_profile.get_decrypted_password()
    env = os.environ.copy()
    env["PGPASSWORD"] = password
    result = subprocess.run(
        [
            _find_pg_bin("pg_dump"),
            "--schema-only", "--clean", "--if-exists",
            "-h", connection_profile.host,
            "-p", str(connection_profile.port),
            "-U", connection_profile.db_username,
            "-d", connection_profile.database_name,
        ],
        env=env, capture_output=True, text=True, check=True,
    )
    schema_ddl = result.stdout

    # 2. Table data via psycopg2
    conn = get_user_connection(connection_profile)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        )
        tables = [row[0] for row in cur.fetchall()]

        table_data = {}
        for tbl in tables:
            cur.execute(pg_sql.SQL("SELECT * FROM {}").format(pg_sql.Identifier(tbl)))
            columns = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            table_data[tbl] = {"columns": columns, "rows": rows}
    finally:
        conn.close()

    # 3. Pickle + gzip + upload
    snapshot = {"schema_ddl": schema_ddl, "tables": table_data}
    compressed = gzip.compress(
        pickle.dumps(snapshot, protocol=pickle.HIGHEST_PROTOCOL)
    )

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pkl.gz", delete=False) as tmp:
            tmp_path = tmp.name
            tmp.write(compressed)
        upload_snapshot(tmp_path, s3_key)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


# -- Snapshot restore (S3 → pickle → user DB) ---------------------------------

def restore_snapshot_data(s3_key: str, connection_profile: ConnectionProfile) -> None:
    """Download a pickle snapshot from S3 and restore it on the user's DB.

    1. Download .pkl.gz from S3 → decompress → unpickle
    2. Execute schema DDL via psql (drops + recreates all objects)
    3. Bulk-insert table data via psycopg2
    4. Reset sequences to match inserted data
    """
    # 1. Download and unpickle
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pkl.gz", delete=False) as tmp:
            tmp_path = tmp.name
        download_snapshot(s3_key, tmp_path)
        with open(tmp_path, "rb") as f:
            compressed = f.read()
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

    snapshot = pickle.loads(gzip.decompress(compressed))

    # 2. Restore schema DDL via psql (DROP IF EXISTS + CREATE)
    schema_path = None
    try:
        with tempfile.NamedTemporaryFile(
            suffix=".sql", delete=False, mode="w", encoding="utf-8",
        ) as tmp:
            schema_path = tmp.name
            tmp.write(snapshot["schema_ddl"])

        password = connection_profile.get_decrypted_password()
        env = os.environ.copy()
        env["PGPASSWORD"] = password
        subprocess.run(
            [
                _find_pg_bin("psql"),
                "-h", connection_profile.host,
                "-p", str(connection_profile.port),
                "-U", connection_profile.db_username,
                "-d", connection_profile.database_name,
                "-f", schema_path,
            ],
            env=env, check=True,
        )
    finally:
        if schema_path and os.path.exists(schema_path):
            os.unlink(schema_path)

    # 3. Insert table data
    conn = get_user_connection(connection_profile)
    try:
        cur = conn.cursor()
        for tbl, info in snapshot["tables"].items():
            if not info["rows"]:
                continue
            cols = pg_sql.SQL(", ").join(
                pg_sql.Identifier(c) for c in info["columns"]
            )
            stmt = pg_sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                pg_sql.Identifier(tbl), cols,
            )
            execute_values(cur, stmt, info["rows"])

        # 4. Reset sequences to match inserted data
        cur.execute("""
            SELECT s.relname, t.relname, a.attname
            FROM pg_class s
            JOIN pg_depend d ON d.objid = s.oid AND d.deptype = 'a'
            JOIN pg_class t ON t.oid = d.refobjid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
            WHERE s.relkind = 'S'
        """)
        for seq_name, table_name, col_name in cur.fetchall():
            try:
                cur.execute(
                    pg_sql.SQL(
                        "SELECT setval({}, COALESCE((SELECT MAX({}) FROM {}), 1))"
                    ).format(
                        pg_sql.Literal(seq_name),
                        pg_sql.Identifier(col_name),
                        pg_sql.Identifier(table_name),
                    )
                )
            except Exception:
                logger.debug("Failed to reset sequence %s", seq_name, exc_info=True)

        conn.commit()
>>>>>>> integration
    except Exception:
        conn.rollback()
        raise
    finally:
<<<<<<< HEAD
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
=======
        conn.close()


# -- Listing -------------------------------------------------------------------

def list_snapshots_for_profile(user_id: int, connection_profile_id: int) -> list[dict]:
    """Return all snapshot metadata records for a user+profile."""
    snapshots = Snapshot.objects.filter(
        connection_profile_id=connection_profile_id,
        connection_profile__user_id=user_id,
    ).order_by("-created_at")

    return [
        {
            "snapshot_id": str(s.snapshot_id),
            "version_id": s.version_id,
            "s3_key": s.s3_key,
            "created_at": s.created_at,
            "connection_profile_id": s.connection_profile_id,
        }
        for s in snapshots
    ]
>>>>>>> integration
