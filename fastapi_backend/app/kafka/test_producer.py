"""Standalone test producer — push dummy messages to all WEAVE-DB topics.

Sends a batch of realistic sample messages to each Kafka topic so you can
verify the entire pipeline end-to-end (producer → broker → consumer).

Usage (from ANY directory):

    python fastapi_backend/app/kafka/test_producer.py          # from project root
    python test_producer.py                                     # from kafka/ dir
    python -m fastapi_backend.app.kafka.test_producer           # module form

Options:

    --count N     Number of messages per topic (default: 3)
    --dry-run     Print messages to stdout instead of sending to Kafka
"""

from __future__ import annotations

import json
import sys
import uuid
from argparse import ArgumentParser
from pathlib import Path

# Ensure project root is on sys.path for standalone execution
_PROJECT_ROOT = str(Path(__file__).resolve().parents[3])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from confluent_kafka import Producer

from fastapi_backend.app.kafka.config import get_producer_config
from fastapi_backend.app.kafka.schemas import (
    build_commit_log,
    build_snapshot_task,
    build_event,
)
from fastapi_backend.app.kafka.topics import COMMIT_LOGS, SNAPSHOT_TASKS, EVENTS


# ---------------------------------------------------------------------------
# Sample data generators
# ---------------------------------------------------------------------------

_SAMPLE_SQL = [
    "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);",
    "ALTER TABLE users ADD COLUMN email VARCHAR(255) UNIQUE;",
    "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');",
    "CREATE INDEX idx_users_email ON users (email);",
    "UPDATE users SET name = 'Bob' WHERE id = 1;",
    "DELETE FROM users WHERE id = 1;",
    "DROP INDEX idx_users_email;",
    "CREATE TABLE orders (id SERIAL, user_id INT REFERENCES users(id), total NUMERIC(10,2));",
]

_EVENT_TYPES = [
    "snapshot_completed",
    "snapshot_failed",
    "rollback_completed",
    "commit_applied",
    "connection_test_ok",
    "connection_test_failed",
]


def _generate_commit_logs(count: int) -> list[tuple[str, bytes, bytes]]:
    """Return (topic, key, value) tuples for commit-log messages."""
    messages = []
    for i in range(count):
        key, value = build_commit_log(
            version_id=str(uuid.uuid4()),
            seq=i + 1,
            sql_command=_SAMPLE_SQL[i % len(_SAMPLE_SQL)],
            user_id=1,
            connection_profile_id=1,
            status="applied",
        )
        messages.append((COMMIT_LOGS, key, value))
    return messages


def _generate_snapshot_tasks(count: int) -> list[tuple[str, bytes, bytes]]:
    """Return (topic, key, value) tuples for snapshot-task messages."""
    messages = []
    for i in range(count):
        vid = str(uuid.uuid4())
        key, value = build_snapshot_task(
            connection_profile_id=1,
            s3_key=f"snapshots/profile-1/{vid}.sql.gz",
            version_id=vid,
            user_id=1,
        )
        messages.append((SNAPSHOT_TASKS, key, value))
    return messages


def _generate_events(count: int) -> list[tuple[str, bytes, bytes]]:
    """Return (topic, key, value) tuples for status-event messages."""
    messages = []
    for i in range(count):
        etype = _EVENT_TYPES[i % len(_EVENT_TYPES)]
        key, value = build_event(
            event_type=etype,
            user_id=1,
            connection_profile_id=1,
            details={
                "version_id": str(uuid.uuid4()),
                "info": f"Dummy event #{i + 1} of type '{etype}'",
            },
        )
        messages.append((EVENTS, key, value))
    return messages


# ---------------------------------------------------------------------------
# Delivery helpers
# ---------------------------------------------------------------------------

_delivered = 0
_failed = 0


def _on_delivery(err, msg):
    global _delivered, _failed
    if err:
        _failed += 1
        print(f"  FAIL  [{msg.topic()}] {err}", file=sys.stderr)
    else:
        _delivered += 1
        print(
            f"  OK    [{msg.topic()}] "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def _pretty(value: bytes) -> str:
    try:
        return json.dumps(json.loads(value), indent=2)
    except Exception:
        return value.decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = ArgumentParser(description="WEAVE-DB Kafka test producer")
    parser.add_argument(
        "--count",
        type=int,
        default=3,
        help="Number of messages per topic (default: 3)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print messages to stdout without sending to Kafka",
    )
    args = parser.parse_args()

    # Build all messages
    all_messages: list[tuple[str, bytes, bytes]] = []
    all_messages.extend(_generate_commit_logs(args.count))
    all_messages.extend(_generate_snapshot_tasks(args.count))
    all_messages.extend(_generate_events(args.count))

    total = len(all_messages)
    print(f"Generated {total} test messages ({args.count} per topic)\n")

    if args.dry_run:
        for topic, key, value in all_messages:
            print(f"--- [{topic}] key={key.decode()} ---")
            print(_pretty(value))
            print()
        print("(dry-run mode — nothing was sent to Kafka)")
        return

    # Real produce
    try:
        config = get_producer_config()
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print(
            "Hint: set KAFKA_CONFIG_PATH or ensure config.ini exists "
            "in fastapi_backend/app/kafka/",
            file=sys.stderr,
        )
        sys.exit(1)

    producer = Producer(config)
    print(f"Connected to Kafka ({config.get('bootstrap.servers', '???')})\n")

    for topic, key, value in all_messages:
        producer.produce(topic, key=key, value=value, callback=_on_delivery)
        producer.poll(0)  # trigger delivery callbacks

    print(f"\nFlushing {total} message(s)...")
    remaining = producer.flush(timeout=15.0)

    print(f"\nResults: {_delivered} delivered, {_failed} failed", end="")
    if remaining:
        print(f", {remaining} still in queue (timed out)")
    else:
        print()

    if _failed or remaining:
        sys.exit(1)


if __name__ == "__main__":
    main()
