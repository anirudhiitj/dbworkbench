<<<<<<< HEAD
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
=======
"""Application configuration loaded from environment variables.

Database credentials are NOT stored here — FastAPI connects dynamically
to each user's external PostgreSQL via ConnectionProfile (Django ORM).
"""

import os

# JWT — must match Django's SIMPLE_JWT['SIGNING_KEY']
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
JWT_ALGORITHM = "HS256"

# AWS S3
S3_BUCKET = os.getenv("S3_BUCKET", "db-snapshots")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

# Snapshot defaults (fallback when no SnapshotPolicy exists for a profile)
SNAPSHOT_FREQUENCY_DEFAULT = int(os.getenv("SNAPSHOT_FREQUENCY_DEFAULT", "5"))

# Django base URL — used by the auth proxy routes to forward token/register requests
DJANGO_BASE_URL = os.getenv("DJANGO_BASE_URL", "http://127.0.0.1:8000")
>>>>>>> integration
