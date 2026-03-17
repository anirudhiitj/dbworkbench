"""Application configuration loaded from environment variables."""

import os


# ── PostgreSQL ────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# Connection pool
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "2"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))

# ── AWS S3 ────────────────────────────────────────────────────────────────────
S3_BUCKET = os.getenv("S3_BUCKET", "db-snapshots")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

# ── Snapshot defaults ─────────────────────────────────────────────────────────
# This is only the *initial* value seeded into the snapshot_config table.
# At runtime the frequency is read from the DB so users can change it (1–5).
SNAPSHOT_FREQUENCY_DEFAULT = int(os.getenv("SNAPSHOT_FREQUENCY_DEFAULT", "5"))
