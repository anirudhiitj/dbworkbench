"""Kafka consumer worker — processes async tasks produced by the API.

Run as a standalone process:

    python -m fastapi_backend.app.kafka.consumer_worker

The worker subscribes to ``dbworkbench.snapshot-tasks`` and processes
snapshot creation (pg_dump → S3) outside the API request path.  This
is the primary mechanism for reducing commit-endpoint latency and
freeing API thread-pool slots for concurrent users.

Architecture notes:
- Django ORM is bootstrapped on import (same pattern as the FastAPI
  process) so the worker can resolve ConnectionProfile credentials.
- Offset commits are manual (at-least-once semantics).  Snapshot
  creation is idempotent — re-uploading the same pg_dump is safe.
- A status event is produced to ``dbworkbench.events`` after each
  task so that downstream consumers (WebSocket gateway, UI, etc.)
  can notify users in real-time.
"""

from __future__ import annotations

# Bootstrap Django BEFORE any model imports
import fastapi_backend.app.django_setup  # noqa: F401, E402

import json
import logging
import signal
import sys

from confluent_kafka import Consumer, Producer, KafkaException

from fastapi_backend.app.kafka.config import get_consumer_config, get_producer_config
from fastapi_backend.app.kafka.topics import SNAPSHOT_TASKS, EVENTS
from fastapi_backend.app.kafka.schemas import deserialize, build_event

logger = logging.getLogger(__name__)

# Graceful shutdown flag
_running = True


def _signal_handler(signum, frame):
    global _running
    logger.info("Received signal %s — shutting down consumer", signum)
    _running = False


def _process_snapshot_task(payload: dict, event_producer: Producer) -> None:
    """Handle a single snapshot-task message.

    Steps:
    1. Resolve the ConnectionProfile via Django ORM.
    2. Call upload_snapshot_data (pg_dump → S3).
    3. Produce a status event (success or failure).
    """
    from connections.models import ConnectionProfile
    from fastapi_backend.app.services.snapshot_service import upload_snapshot_data

    cpid = payload["connection_profile_id"]
    s3_key = payload["s3_key"]
    version_id = payload["version_id"]
    user_id = payload["user_id"]

    logger.info(
        "Processing snapshot task: profile=%s version=%s s3_key=%s",
        cpid, version_id, s3_key,
    )

    try:
        profile = ConnectionProfile.objects.get(id=cpid)
        upload_snapshot_data(profile, s3_key)

        # Publish success event
        key, value = build_event(
            event_type="snapshot_completed",
            user_id=user_id,
            connection_profile_id=cpid,
            details={
                "version_id": version_id,
                "s3_key": s3_key,
            },
        )
        event_producer.produce(EVENTS, key=key, value=value)
        event_producer.poll(0)

        logger.info("Snapshot completed: profile=%s s3_key=%s", cpid, s3_key)

    except Exception:
        logger.exception("Snapshot task failed: profile=%s s3_key=%s", cpid, s3_key)

        # Publish failure event
        key, value = build_event(
            event_type="snapshot_failed",
            user_id=user_id,
            connection_profile_id=cpid,
            details={
                "version_id": version_id,
                "s3_key": s3_key,
            },
        )
        event_producer.produce(EVENTS, key=key, value=value)
        event_producer.poll(0)


def run_consumer() -> None:
    """Main consumer loop.  Blocks until SIGINT / SIGTERM."""
    global _running

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    consumer_config = get_consumer_config(group_id="dbworkbench_snapshot_workers")
    consumer = Consumer(consumer_config)
    consumer.subscribe([SNAPSHOT_TASKS])

    # Separate producer for emitting status events
    event_producer = Producer(get_producer_config())

    logger.info(
        "Snapshot consumer started — group=%s topic=%s",
        consumer_config.get("group.id"),
        SNAPSHOT_TASKS,
    )

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            try:
                raw = msg.value()
                if raw is None:
                    continue
                payload = deserialize(raw)
                _process_snapshot_task(payload, event_producer)
            except (json.JSONDecodeError, KeyError) as exc:
                logger.error(
                    "Malformed message on %s [%s]: %s",
                    msg.topic(), msg.partition(), exc,
                )
            finally:
                # Manual commit after processing (at-least-once)
                consumer.commit(asynchronous=False)

    except KafkaException as exc:
        logger.error("Fatal Kafka error: %s", exc)
        sys.exit(1)
    finally:
        # Drain event producer
        event_producer.flush(timeout=10)
        consumer.close()
        logger.info("Snapshot consumer shut down")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_consumer()
