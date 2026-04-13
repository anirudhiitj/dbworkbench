"""Message schemas for Kafka topics.

All messages are JSON-serialised dicts.  These helper functions
provide consistent structure and serialisation for producers and
deserialisation for consumers.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _serialize(payload: Dict[str, Any]) -> bytes:
    """JSON-encode a dict to UTF-8 bytes (Kafka message value)."""
    return json.dumps(payload, default=str).encode("utf-8")


def _deserialize(raw: bytes) -> Dict[str, Any]:
    """Decode a Kafka message value back to a dict."""
    return json.loads(raw.decode("utf-8"))


# ---------------------------------------------------------------------------
# commit-logs messages
# ---------------------------------------------------------------------------

def build_commit_log(
    *,
    version_id: str,
    seq: int,
    sql_command: str,
    user_id: int,
    connection_profile_id: int,
    status: str,
) -> tuple[bytes, bytes]:
    """Return ``(key, value)`` for a commit-log message.

    Key = connection_profile_id (ensures per-DB ordering within a partition).
    """
    key = str(connection_profile_id).encode("utf-8")
    value = _serialize({
        "event_type": "commit_created",
        "version_id": version_id,
        "seq": seq,
        "sql_command": sql_command,
        "user_id": user_id,
        "connection_profile_id": connection_profile_id,
        "status": status,
        "produced_at": _now_iso(),
    })
    return key, value


# ---------------------------------------------------------------------------
# snapshot-tasks messages
# ---------------------------------------------------------------------------

def build_snapshot_task(
    *,
    connection_profile_id: int,
    s3_key: str,
    version_id: str,
    user_id: int,
) -> tuple[bytes, bytes]:
    """Return ``(key, value)`` for a snapshot-task message.

    Key = connection_profile_id (serialises snapshots per DB).
    """
    key = str(connection_profile_id).encode("utf-8")
    value = _serialize({
        "task_type": "create_snapshot",
        "connection_profile_id": connection_profile_id,
        "s3_key": s3_key,
        "version_id": version_id,
        "user_id": user_id,
        "produced_at": _now_iso(),
    })
    return key, value


# ---------------------------------------------------------------------------
# events messages
# ---------------------------------------------------------------------------

def build_event(
    *,
    event_type: str,
    user_id: int,
    connection_profile_id: int,
    details: Optional[Dict[str, Any]] = None,
) -> tuple[bytes, bytes]:
    """Return ``(key, value)`` for a status-event message.

    Key = user_id (allows per-user consumption / filtering).
    """
    key = str(user_id).encode("utf-8")
    value = _serialize({
        "event_type": event_type,
        "user_id": user_id,
        "connection_profile_id": connection_profile_id,
        "details": details or {},
        "produced_at": _now_iso(),
    })
    return key, value


# Re-export deserialiser for consumers
deserialize = _deserialize
