"""Thread-safe singleton Kafka producer for the FastAPI process.

Design decisions:
- **Singleton**: One Producer instance is shared across all request threads.
  confluent_kafka.Producer is thread-safe and maintains its own internal
  send buffer + background I/O thread, so sharing is both safe and optimal.
- **Graceful degradation**: If Kafka is unavailable the producer enters a
  *disabled* state and ``produce()`` becomes a no-op.  The caller (e.g.
  commit_service) can detect this and fall back to synchronous behaviour.
- **Delivery callbacks**: Errors are logged; the API never blocks waiting
  for broker acknowledgement (fire-and-forget from the request thread).
- **Flush on shutdown**: The lifespan hook calls ``shutdown()`` to drain
  the internal buffer before the process exits.
"""

from __future__ import annotations

import logging
import threading
from typing import Optional

from confluent_kafka import Producer, KafkaException

from fastapi_backend.app.kafka.config import get_producer_config

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_producer: Optional[Producer] = None
_enabled: bool = False


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

def _delivery_callback(err, msg):
    """Called once per message when the broker acknowledges (or rejects) it."""
    if err is not None:
        logger.error(
            "Kafka delivery failed: topic=%s key=%s err=%s",
            msg.topic(),
            msg.key(),
            err,
        )
    else:
        logger.debug(
            "Kafka delivered: topic=%s partition=%s offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def init_producer() -> bool:
    """Initialise the singleton Producer.  Returns True if successful."""
    global _producer, _enabled

    with _lock:
        if _producer is not None:
            return _enabled
        try:
            config = get_producer_config()
            _producer = Producer(config)
            _enabled = True
            logger.info("Kafka producer initialised (%s)", config.get("bootstrap.servers"))
            return True
        except (KafkaException, FileNotFoundError) as exc:
            logger.warning("Kafka producer disabled — %s", exc)
            _enabled = False
            return False


def shutdown(timeout: float = 10.0) -> None:
    """Flush pending messages and tear down the producer."""
    global _producer, _enabled

    with _lock:
        if _producer is None:
            return
        remaining = _producer.flush(timeout)
        if remaining > 0:
            logger.warning("Kafka shutdown: %d message(s) still in queue", remaining)
        _producer = None
        _enabled = False
        logger.info("Kafka producer shut down")


def is_enabled() -> bool:
    """Return True if the producer is ready to send."""
    return _enabled


# ---------------------------------------------------------------------------
# Public produce API
# ---------------------------------------------------------------------------

def produce(topic: str, key: bytes, value: bytes) -> bool:
    """Enqueue a message.  Non-blocking; returns False if Kafka is disabled.

    The caller should treat a ``False`` return as a signal to fall back
    to synchronous processing (e.g. inline snapshot upload).
    """
    if not _enabled or _producer is None:
        return False

    try:
        _producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=_delivery_callback,
        )
        # Service delivery callbacks without blocking
        _producer.poll(0)
        return True
    except BufferError:
        logger.warning("Kafka producer buffer full — falling back to sync")
        # Try to drain before giving up
        _producer.poll(1)
        return False
    except KafkaException as exc:
        logger.error("Kafka produce error: %s", exc)
        return False
