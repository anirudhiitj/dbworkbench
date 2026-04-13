"""Kafka configuration loader.

Reads ``kafka/config.ini`` from the project root and returns
ready-to-use dicts for the confluent_kafka Producer / Consumer.

The [default] section contains shared settings (bootstrap servers,
SASL credentials).  [producer] and [consumer] sections hold
client-specific overrides.
"""

from __future__ import annotations

import os
from configparser import ConfigParser
from pathlib import Path
from typing import Dict


def _find_config_ini() -> Path:
    """Locate ``config.ini``.

    Search order:
    1. ``KAFKA_CONFIG_PATH`` environment variable (absolute path).
    2. Same directory as this module (``fastapi_backend/app/kafka/config.ini``).
    3. Walk up from this file to the project root looking for ``kafka/config.ini``.
    """
    env_path = os.getenv("KAFKA_CONFIG_PATH")
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return p
        raise FileNotFoundError(f"KAFKA_CONFIG_PATH={env_path} does not exist")

    # Check sibling file first (same directory as this module)
    sibling = Path(__file__).resolve().parent / "config.ini"
    if sibling.is_file():
        return sibling

    # Walk upward looking for kafka/config.ini at the project root
    current = Path(__file__).resolve().parent
    for _ in range(6):
        candidate = current / "kafka" / "config.ini"
        if candidate.is_file():
            return candidate
        current = current.parent

    raise FileNotFoundError(
        "config.ini not found. Set KAFKA_CONFIG_PATH or place the file "
        "next to this module"
    )


def _load_config_parser() -> ConfigParser:
    path = _find_config_ini()
    parser = ConfigParser()
    parser.read(str(path))
    return parser


def get_producer_config() -> Dict[str, str]:
    """Return a config dict suitable for ``confluent_kafka.Producer``.

    Merges [default] + [producer] (if present) + hardened defaults.
    """
    parser = _load_config_parser()
    config: Dict[str, str] = dict(parser["default"])

    if parser.has_section("producer"):
        config.update(dict(parser["producer"]))

    # Sensible defaults for low-latency, reliable production
    config.setdefault("linger.ms", "5")
    config.setdefault("batch.num.messages", "100")
    config.setdefault("compression.type", "lz4")
    config.setdefault("acks", "all")
    config.setdefault("retries", "3")
    config.setdefault("retry.backoff.ms", "200")
    config.setdefault("delivery.timeout.ms", "30000")

    return config


def get_consumer_config(group_id: str | None = None) -> Dict[str, str]:
    """Return a config dict suitable for ``confluent_kafka.Consumer``.

    Merges [default] + [consumer] + optional group_id override.
    """
    parser = _load_config_parser()
    config: Dict[str, str] = dict(parser["default"])

    if parser.has_section("consumer"):
        config.update(dict(parser["consumer"]))

    if group_id:
        config["group.id"] = group_id

    # At-least-once: disable auto-commit so we control offsets
    config.setdefault("enable.auto.commit", "false")
    config.setdefault("auto.offset.reset", "earliest")

    return config


def get_admin_config() -> Dict[str, str]:
    """Return a config dict suitable for ``confluent_kafka.admin.AdminClient``."""
    parser = _load_config_parser()
    return dict(parser['default'])
