"""Standalone debug consumer — subscribe to all WEAVE-DB topics and print.

Usage (from project root):

    python -m fastapi_backend.app.kafka.debug_consumer                   # all topics
    python -m fastapi_backend.app.kafka.debug_consumer --topics dbworkbench.commit-logs  # single topic

Uses the same kafka/config.ini as the rest of the application.
"""

from __future__ import annotations

import json
import sys
from argparse import ArgumentParser
from pathlib import Path

# Allow running as a standalone script (`python debug_consumer.py`) by
# ensuring the project root is on sys.path so absolute imports resolve.
_PROJECT_ROOT = str(Path(__file__).resolve().parents[3])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from confluent_kafka import Consumer

from fastapi_backend.app.kafka.config import get_consumer_config
from fastapi_backend.app.kafka.topics import ALL_TOPICS


def _pretty(raw: bytes) -> str:
    """Try to pretty-print JSON, fall back to raw string."""
    try:
        return json.dumps(json.loads(raw), indent=2)
    except (json.JSONDecodeError, TypeError):
        return raw.decode("utf-8", errors="replace")


def main() -> None:
    parser = ArgumentParser(description="WEAVE-DB Kafka debug consumer")
    parser.add_argument(
        "--topics",
        nargs="*",
        default=None,
        help="Topic names to subscribe to (default: all WEAVE-DB topics)",
    )
    args = parser.parse_args()

    # Use a separate group so we never steal offsets from production consumers
    config = get_consumer_config(group_id="dbworkbench_debug")
    config["enable.auto.commit"] = "true"  # fine for debugging

    consumer = Consumer(config)

    topics = args.topics or ALL_TOPICS
    consumer.subscribe(topics)
    print(f"Subscribed to: {', '.join(topics)}")
    print("Waiting for messages (Ctrl+C to quit)...\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"ERROR: {msg.error()}", file=sys.stderr)
                continue

            print(
                f"--- [{msg.topic()}] partition={msg.partition()} "
                f"offset={msg.offset()} key={msg.key()} ---"
            )
            raw = msg.value()
            print(_pretty(raw) if raw is not None else "<empty>")
            print()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
