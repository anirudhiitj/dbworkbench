<<<<<<< HEAD
"""Commit service — create, list, and retrieve versioned commits.

A commit wraps one or more SQL steps.  Each commit is assigned a
sequential commit_number, a SHA-256 hash, and an optional message.
After creating a commit the service checks if the snapshot frequency
threshold has been hit and triggers an auto-snapshot if so.
=======

"""Commit service — execute SQL on the user's DB and record via Django ORM.

Flow:
1. Validate the SQL command
2. Open connection to user's external database
3. Generate inverse command via InverseEngine (captures before-image)
4. Execute the forward SQL on the user's database
5. Finalize INSERT inverse if needed (captures RETURNING rows)
6. Call Django's record_commit() to atomically persist
   CommitEvent + InverseOperation + conditional Snapshot
7. If a snapshot was created, dispatch via Kafka for async pg_dump + S3
   upload.  Falls back to synchronous upload if Kafka is unavailable.
8. Produce an audit event to the commit-logs topic.
>>>>>>> integration
"""

from __future__ import annotations

<<<<<<< HEAD
import re

from fastapi_backend.app.db.connection import get_connection, release_connection
from fastapi_backend.app.db import metadata_queries as mq
from fastapi_backend.app.utils.hashing import generate_commit_hash
from fastapi_backend.app.services.snapshot_service import (
    get_snapshot_frequency,
    create_snapshot,
)

_ALLOWED_SQL_KEYWORDS = {
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    # Add other safe/expected keywords here if needed, e.g. "ALTER", "CREATE", etc.
}


def _validate_sql_step(sql: str, step_type: str) -> None:
    """
    Perform basic validation of a user-provided SQL step before execution.

    Currently it:
    - Ensures there is at least one non-empty token.
    - Ensures the first token is in an allowlist of SQL keywords.
    - Rejects multiple statements separated by semicolons (except an optional
      trailing semicolon).
    """
    if not isinstance(sql, str):
        raise ValueError("SQL step must be a string.")

    stripped = sql.strip()
    if not stripped:
        raise ValueError("SQL step may not be empty.")

    # Disallow stacked statements like "UPDATE ...; DELETE ...;"
    parts = [p for p in stripped.split(";") if p.strip()]
    if len(parts) > 1:
        raise ValueError("Only a single SQL statement per step is allowed.")

    # Grab the first word (keyword) and validate it.
    match = re.match(r"^([a-zA-Z]+)", stripped)
    if not match:
        raise ValueError("Could not determine SQL command keyword.")

    keyword = match.group(1).upper()
    if keyword not in _ALLOWED_SQL_KEYWORDS:
        raise ValueError(f"SQL command '{keyword}' is not allowed for commit steps.")


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

            # Validate and then execute the actual user SQL on the database
            _validate_sql_step(sql=sql, step_type=step_type)
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
=======
import logging
import re
import uuid

from authentication.models import User
from connections.models import ConnectionProfile
from core.models import CommitEvent, Snapshot
from core.services import record_commit

from fastapi_backend.app.db.connection import get_user_connection
from fastapi_backend.app.services.inverse_engine import (
    InverseEngine,
    CommandCategory,
)
from fastapi_backend.app.services.snapshot_service import upload_snapshot_data

from fastapi_backend.app.kafka import producer as kafka_producer
from fastapi_backend.app.kafka.topics import SNAPSHOT_TASKS, COMMIT_LOGS
from fastapi_backend.app.kafka.schemas import build_snapshot_task, build_commit_log

logger = logging.getLogger(__name__)


_WRITE_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "DROP", "TRUNCATE"}


def _validate_write_sql(sql: str) -> None:
    """Ensure the SQL is a single write statement."""
    stripped = sql.strip()
    if not stripped:
        raise ValueError("SQL command may not be empty")

    first_token = stripped.split(None, 1)[0].upper()
    if first_token == "SELECT":
        raise ValueError("SELECT queries are not tracked — use /query/execute instead")
    if first_token not in _WRITE_KEYWORDS:
        raise ValueError(f"SQL command '{first_token}' is not allowed for commits")

    parts = [p for p in stripped.split(";") if p.strip()]
    if len(parts) > 1:
        raise ValueError("Only a single SQL statement per commit is allowed")


def _append_returning_star(sql: str) -> str:
    """Append RETURNING * to an INSERT statement if it doesn't already have one."""
    if re.search(r"\bRETURNING\b", sql, re.IGNORECASE):
        return sql
    stripped = sql.rstrip().rstrip(";")
    return stripped + " RETURNING *"


def _rows_to_dicts(cursor) -> list[dict]:
    """Convert cursor results to list of dicts using cursor.description."""
    rows = cursor.fetchall()
    if not rows or cursor.description is None:
        return []
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in rows]


def create_commit(
    user_id: int,
    connection_profile_id: int,
    sql_command: str,
) -> dict:
    """Execute SQL on user's DB, auto-generate inverse, then persist commit atomically."""
    user = User.objects.get(id=user_id)
    profile = ConnectionProfile.objects.get(id=connection_profile_id, user=user)

    _validate_write_sql(sql_command)
    version_id = str(uuid.uuid4())

    conn = get_user_connection(profile)
    try:
        # 1. Generate inverse BEFORE executing (captures before-image)
        engine = InverseEngine(conn)
        inv = engine.generate(sql_command)

        # 2. Execute forward SQL on the user's database
        cur = conn.cursor()
        if inv.category == CommandCategory.INSERT:
            # For INSERTs, use RETURNING * so we can finalize the inverse
            exec_sql = _append_returning_star(sql_command)
            cur.execute(exec_sql)
            returned_rows = _rows_to_dicts(cur)
            engine.finalize_insert(inv, returned_rows)
        else:
            cur.execute(sql_command)

        conn.commit()
>>>>>>> integration
    except Exception:
        conn.rollback()
        raise
    finally:
<<<<<<< HEAD
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
=======
        conn.close()

    # 3. Build inverse_sql string from the generated steps
    inverse_sql = "\n".join(inv.steps) if inv.steps else ""

    # 4. Record atomically via Django (CommitEvent + InverseOperation + Snapshot)
    commit = record_commit(
        version_id=version_id,
        sql_command=sql_command,
        inverse_sql=inverse_sql,
        user=user,
        connection_profile=profile,
        status="success",
    )

    # 5. If record_commit() created a snapshot record, upload to S3 synchronously
    #    and also notify Kafka for audit/downstream consumers.
    snapshot = Snapshot.objects.filter(
        version_id=version_id,
        connection_profile=profile,
    ).first()
    if snapshot:
        # Always upload synchronously — cannot rely on a consumer being up
        upload_snapshot_data(profile, snapshot.s3_key)
        logger.info("Snapshot uploaded to S3: %s", snapshot.s3_key)

        # Notify Kafka (fire-and-forget, non-critical)
        try:
            key, value = build_snapshot_task(
                connection_profile_id=profile.id,
                s3_key=snapshot.s3_key,
                version_id=version_id,
                user_id=user.id,
            )
            kafka_producer.produce(SNAPSHOT_TASKS, key=key, value=value)
        except Exception:
            logger.debug("Failed to produce snapshot task to Kafka", exc_info=True)

    # 6. Produce audit log (fire-and-forget, non-critical)
    try:
        log_key, log_value = build_commit_log(
            version_id=commit.version_id,
            seq=commit.seq,
            sql_command=commit.sql_command,
            user_id=user.id,
            connection_profile_id=profile.id,
            status=commit.status,
        )
        kafka_producer.produce(COMMIT_LOGS, key=log_key, value=log_value)
    except Exception:
        logger.debug("Failed to produce commit audit log", exc_info=True)

    return {
        "version_id": commit.version_id,
        "seq": commit.seq,
        "sql_command": commit.sql_command,
        "commit_hash": commit.commit_hash,
        "status": commit.status,
        "timestamp": commit.timestamp,
        "connection_profile_id": profile.id,
    }


def list_commits(user_id: int, connection_profile_id: int) -> list[dict]:
    """Return all commits for a user+profile, ordered by seq."""
    commits = CommitEvent.objects.filter(
        user_id=user_id,
        connection_profile_id=connection_profile_id,
    ).order_by("seq")

    return [
        {
            "version_id": c.version_id,
            "seq": c.seq,
            "sql_command": c.sql_command,
            "commit_hash": c.commit_hash,
            "status": c.status,
            "timestamp": c.timestamp,
        }
        for c in commits
    ]


def get_commit(user_id: int, version_id: str) -> dict | None:
    """Return a single commit by version_id, or None."""
    try:
        c = CommitEvent.objects.get(version_id=version_id, user_id=user_id)
    except CommitEvent.DoesNotExist:
        return None

    return {
        "version_id": c.version_id,
        "seq": c.seq,
        "sql_command": c.sql_command,
        "commit_hash": c.commit_hash,
        "status": c.status,
        "timestamp": c.timestamp,
        "connection_profile_id": c.connection_profile_id,
    }
>>>>>>> integration
