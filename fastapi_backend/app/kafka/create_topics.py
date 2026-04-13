"""One-time topic provisioning for Confluent Cloud.

Run once to create the required topics:

    python -m fastapi_backend.app.kafka.create_topics

If topics already exist the script is idempotent (prints a warning
and exits cleanly).

Topic design rationale (single-cluster optimisation):
- 3 topics with 3 partitions each = 9 total partitions
  (well within Confluent Cloud free-tier limits).
- 3 partitions per topic allows up to 3 parallel consumer instances
  per consumer group while keeping cluster overhead minimal.
"""

from __future__ import annotations

import logging
import sys

from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from fastapi_backend.app.kafka.config import get_admin_config
from fastapi_backend.app.kafka.topics import ALL_TOPICS

logger = logging.getLogger(__name__)

# Number of partitions per topic.  3 is a good default for a single
# cluster: it allows moderate parallelism without excessive overhead.
NUM_PARTITIONS = 3

# Replication factor.  Confluent Cloud typically requires 3.
REPLICATION_FACTOR = 3


def create_topics() -> None:
    admin = AdminClient(dict(get_admin_config()))  # type: ignore[arg-type]

    new_topics = [
        NewTopic(
            topic,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
        )
        for topic in ALL_TOPICS
    ]

    futures = admin.create_topics(new_topics)

    for topic, future in futures.items():
        try:
            future.result()  # blocks until topic is created
            logger.info("Created topic: %s", topic)
        except Exception as exc:
            if "TopicExistsException" in str(type(exc).__name__) or "TOPIC_ALREADY_EXISTS" in str(exc):
                logger.info("Topic already exists: %s", topic)
            else:
                logger.error("Failed to create topic %s: %s", topic, exc)
                sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    create_topics()
    print("Topic provisioning complete.")
