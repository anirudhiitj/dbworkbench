"""Kafka topic name constants.

Three topics — optimised for a single Confluent Cloud cluster:

1. commit-logs   – Audit trail of all versioned write operations.
                   Partition key: connection_profile_id (preserves per-DB ordering).

2. snapshot-tasks – Async work queue for pg_dump + S3 upload.
                   Partition key: connection_profile_id (serialises snapshots per DB).

3. events        – Status / notification events (task completed, failed, etc.).
                   Partition key: user_id (allows per-user consumption).
"""

COMMIT_LOGS = "dbworkbench.commit-logs"
SNAPSHOT_TASKS = "dbworkbench.snapshot-tasks"
EVENTS = "dbworkbench.events"

ALL_TOPICS = [COMMIT_LOGS, SNAPSHOT_TASKS, EVENTS]
