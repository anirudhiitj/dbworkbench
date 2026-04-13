"""Connection service — CRUD for ConnectionProfile via Django ORM."""

from __future__ import annotations

import psycopg2

from authentication.models import User
from connections.models import ConnectionProfile


def _serialize(profile: ConnectionProfile) -> dict:
    return {
        "id": profile.id,
        "name": profile.name,
        "host": profile.host,
        "port": profile.port,
        "database_name": profile.database_name,
        "db_username": profile.db_username,
        "created_at": profile.created_at,
    }


def create_connection_profile(
    user_id: int,
    name: str,
    host: str,
    port: int,
    database_name: str,
    db_username: str,
    db_password: str,
) -> dict:
    """Create a new ConnectionProfile. Password is Fernet-encrypted on save."""
    # Validate connection before saving
    try:
        conn = psycopg2.connect(
            host=host, port=port, dbname=database_name,
            user=db_username, password=db_password, connect_timeout=5,
        )
        conn.close()
    except Exception as e:
        raise ValueError(f"Cannot connect to database: {e}")

    user = User.objects.get(id=user_id)
    profile = ConnectionProfile.objects.create(
        user=user,
        name=name,
        host=host,
        port=port,
        database_name=database_name,
        db_username=db_username,
        db_password=db_password,  # save() encrypts this automatically
    )
    return _serialize(profile)


def list_connection_profiles(user_id: int) -> list[dict]:
    """Return all ConnectionProfiles belonging to this user."""
    profiles = ConnectionProfile.objects.filter(user_id=user_id).order_by("created_at")
    return [_serialize(p) for p in profiles]


def get_connection_profile(user_id: int, profile_id: int) -> dict | None:
    """Return a single ConnectionProfile, or None if not found."""
    try:
        profile = ConnectionProfile.objects.get(id=profile_id, user_id=user_id)
        return _serialize(profile)
    except ConnectionProfile.DoesNotExist:
        return None


def update_connection_profile(
    user_id: int,
    profile_id: int,
    **fields,
) -> dict:
    """Update allowed fields on an existing ConnectionProfile."""
    profile = ConnectionProfile.objects.get(id=profile_id, user_id=user_id)

    allowed = {"name", "host", "port", "database_name", "db_username", "db_password"}
    for field, value in fields.items():
        if field in allowed and value is not None:
            setattr(profile, field, value)

    # save() will re-encrypt db_password if it was changed
    profile.save()
    return _serialize(profile)


def delete_connection_profile(user_id: int, profile_id: int) -> None:
    """Delete a ConnectionProfile. Cascades to CommitEvents, Snapshots, etc."""
    profile = ConnectionProfile.objects.get(id=profile_id, user_id=user_id)
    profile.delete()
