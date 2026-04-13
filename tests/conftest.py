"""Shared fixtures for the WEAVE-DB test suite.

Provides reusable factories for User, ConnectionProfile, CommitEvent,
InverseOperation, Snapshot, and SnapshotPolicy.  Also sets up environment
variables and the FastAPI async test client.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Environment ΓÇö must be set BEFORE Django is configured
# ---------------------------------------------------------------------------
_django_backend_dir = Path(__file__).resolve().parent.parent / "django_backend"

# Force-set test credentials so real .env values are NEVER used in tests.
os.environ["SECRET_KEY"] = "test-secret-key-for-pytest"
os.environ["DB_NAME"] = "weavedb_internal"
os.environ["DB_USER"] = "postgres"
os.environ["DB_PASSWORD"] = "postgres"
os.environ["DB_HOST"] = "localhost"
os.environ["DB_PORT"] = "5432"
os.environ["DJANGO_SETTINGS_MODULE"] = "django_backend.settings"

# Fernet key ΓÇö test-only, not a real secret
os.environ["FERNET_KEY"] = "fCYdNhBhGgcHeBl7f5fqRet1pfLQdaflzoZAOLoysvM="

# JWT secret shared between Django and FastAPI
os.environ["JWT_SECRET_KEY"] = "test-jwt-secret-for-pytest"

# Ensure django_backend is on the path
if str(_django_backend_dir) not in sys.path:
    sys.path.insert(0, str(_django_backend_dir))

import django  # noqa: E402

django.setup()

# Ensure Django's cached settings also use our test JWT key, even if
# django_setup.py loaded .env before conftest force-set the env vars.
from django.conf import settings as _dj_settings  # noqa: E402

if hasattr(_dj_settings, "SIMPLE_JWT"):
    _dj_settings.SIMPLE_JWT["SIGNING_KEY"] = os.environ["JWT_SECRET_KEY"]

# ---------------------------------------------------------------------------
# Django model imports (after setup)
# ---------------------------------------------------------------------------
from authentication.models import User  # noqa: E402
from connections.models import ConnectionProfile  # noqa: E402
from core.models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy  # noqa: E402


# ---------------------------------------------------------------------------
# Django fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def user(db):
    """Create and return a test user."""
    return User.objects.create_user(
        username="testuser",
        password="testpass123",
        email="test@example.com",
    )


@pytest.fixture
def other_user(db):
    """Create a second user for ownership / isolation tests."""
    return User.objects.create_user(
        username="otheruser",
        password="otherpass123",
        email="other@example.com",
    )


@pytest.fixture
def connection_profile(user):
    """Create and return a ConnectionProfile for the test user."""
    return ConnectionProfile.objects.create(
        name="Test DB",
        host="localhost",
        port=5432,
        database_name="testdb",
        db_username="dbuser",
        db_password="dbpassword",
        user=user,
    )


@pytest.fixture
def other_profile(other_user):
    """ConnectionProfile owned by another user."""
    return ConnectionProfile.objects.create(
        name="Other DB",
        host="localhost",
        port=5432,
        database_name="otherdb",
        db_username="dbuser2",
        db_password="dbpassword2",
        user=other_user,
    )


@pytest.fixture
def commit_event(user, connection_profile):
    """Create a single CommitEvent."""
    return CommitEvent.objects.create(
        version_id="v-test-001",
        seq=1,
        sql_command="INSERT INTO t(id) VALUES (1)",
        status="success",
        user=user,
        connection_profile=connection_profile,
    )


@pytest.fixture
def inverse_operation(commit_event):
    """Create an InverseOperation linked to commit_event."""
    return InverseOperation.objects.create(
        version_id=commit_event.version_id,
        inverse_sql="DELETE FROM t WHERE id = 1",
        commit=commit_event,
    )


@pytest.fixture
def snapshot(connection_profile, commit_event):
    """Create a Snapshot record."""
    return Snapshot.objects.create(
        version_id=commit_event.version_id,
        s3_key=f"snapshots/{connection_profile.id}/{commit_event.version_id}",
        connection_profile=connection_profile,
    )


@pytest.fixture
def snapshot_policy(connection_profile):
    """Create a SnapshotPolicy with frequency 5."""
    return SnapshotPolicy.objects.create(
        frequency=5,
        connection_profile=connection_profile,
    )


# ---------------------------------------------------------------------------
# JWT helper ΓÇö generate a valid token for FastAPI tests
# ---------------------------------------------------------------------------

@pytest.fixture
def auth_token(user):
    """Return a valid JWT access token string for the test user."""
    import jwt as pyjwt
    from datetime import datetime, timedelta, timezone

    payload = {
        "user_id": user.id,
        "username": user.username,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=15),
        "iat": datetime.now(timezone.utc),
    }
    return pyjwt.encode(payload, os.environ["JWT_SECRET_KEY"], algorithm="HS256")


@pytest.fixture
def auth_headers(auth_token):
    """Return Authorization headers dict for FastAPI requests."""
    return {"Authorization": f"Bearer {auth_token}"}


# ---------------------------------------------------------------------------
# FastAPI async test client
# ---------------------------------------------------------------------------

@pytest.fixture
def fastapi_client():
    """Return an httpx.AsyncClient wired to the FastAPI ASGI app."""
    import httpx
    from fastapi_backend.app.main import app

    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    )


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_psycopg2_connect():
    """Patch psycopg2.connect to return a mock connection + cursor."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
    mock_cursor.rowcount = 2
    mock_conn.cursor.return_value = mock_cursor
    with patch("fastapi_backend.app.db.connection.psycopg2.connect", return_value=mock_conn) as mock_connect:
        mock_connect._mock_conn = mock_conn
        mock_connect._mock_cursor = mock_cursor
        yield mock_connect


@pytest.fixture
def mock_s3():
    """Patch all S3 utility functions.

    The download mock writes a valid pickle+gzip snapshot so that
    restore_snapshot_data can unpickle it.
    """
    import gzip as _gzip
    import pickle as _pickle

    def _mock_download(s3_key, local_path):
        snapshot = {"schema_ddl": "-- test schema\n", "tables": {}}
        with open(local_path, "wb") as f:
            f.write(_gzip.compress(_pickle.dumps(snapshot)))

    with patch("fastapi_backend.app.utils.s3_utils._get_client") as mock_client, \
         patch("fastapi_backend.app.services.snapshot_service.upload_snapshot") as mock_upload, \
         patch("fastapi_backend.app.services.snapshot_service.download_snapshot",
               side_effect=_mock_download) as mock_download:
        yield {
            "client": mock_client,
            "upload": mock_upload,
            "download": mock_download,
        }


@pytest.fixture
def mock_subprocess():
    """Patch subprocess.run for pg_dump/psql calls."""
    with patch("fastapi_backend.app.services.snapshot_service.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="-- test schema DDL\n")
        yield mock_run


@pytest.fixture
def mock_snapshot_conn():
    """Patch get_user_connection inside snapshot_service for upload/restore."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value = mock_cursor
    with patch("fastapi_backend.app.services.snapshot_service.get_user_connection",
               return_value=mock_conn) as mock_get:
        mock_get._mock_conn = mock_conn
        mock_get._mock_cursor = mock_cursor
        yield mock_get


@pytest.fixture
def mock_kafka_producer():
    """Patch the Kafka producer module to simulate an enabled broker.

    Sets ``is_enabled()`` ΓåÆ True and ``produce()`` ΓåÆ True by default.
    Tests can override ``produce.return_value = False`` to simulate
    Kafka being unavailable (triggering sync fallback).
    """
    with patch("fastapi_backend.app.kafka.producer.is_enabled", return_value=True), \
         patch("fastapi_backend.app.kafka.producer.produce", return_value=True) as mock_produce:
        yield mock_produce
