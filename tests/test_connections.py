"""Tests for ConnectionProfile CRUD operations.

Covers connection service, Fernet encryption, ownership isolation,
cascade behaviour, and edge cases.
"""

import os
import pytest
from cryptography.fernet import Fernet

from authentication.models import User
from connections.models import ConnectionProfile
from core.models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy
from fastapi_backend.app.services.connection_service import (
    create_connection_profile,
    list_connection_profiles,
    get_connection_profile,
    update_connection_profile,
    delete_connection_profile,
)


pytestmark = pytest.mark.django_db


class TestConnectionProfileModel:
    """Tests for the ConnectionProfile Django model."""

    def test_password_encrypted_on_save(self, user):
        """Verify that db_password is Fernet-encrypted on save."""
        profile = ConnectionProfile.objects.create(
            name="Encrypted Test",
            host="localhost",
            port=5432,
            database_name="encdb",
            db_username="encuser",
            db_password="plaintext_password",
            user=user,
        )
        profile.refresh_from_db()
        assert profile.db_password.startswith("gAAAAA")
        assert profile.db_password != "plaintext_password"

    def test_get_decrypted_password(self, connection_profile):
        """Verify that get_decrypted_password() returns the original plaintext."""
        assert connection_profile.get_decrypted_password() == "dbpassword"

    def test_already_encrypted_password_not_double_encrypted(self, user):
        """Verify that saving an already-encrypted password does not double-encrypt."""
        f = Fernet(os.environ["FERNET_KEY"].encode())
        encrypted = f.encrypt(b"mypassword").decode()
        profile = ConnectionProfile.objects.create(
            name="Pre-enc",
            host="localhost",
            port=5432,
            database_name="predb",
            db_username="preuser",
            db_password=encrypted,
            user=user,
        )
        profile.refresh_from_db()
        assert profile.get_decrypted_password() == "mypassword"

    def test_special_characters_in_password(self, user):
        """Verify that passwords with special characters survive encrypt/decrypt."""
        special_pw = "p@$$w0rd!#%^&*(){}[]|;':\",./<>?"
        profile = ConnectionProfile.objects.create(
            name="Special", host="localhost", port=5432,
            database_name="db", db_username="user",
            db_password=special_pw, user=user,
        )
        profile.refresh_from_db()
        assert profile.get_decrypted_password() == special_pw

    def test_unicode_password(self, user):
        """Verify unicode characters in password."""
        unicode_pw = "mot_de_passe_cafe"
        profile = ConnectionProfile.objects.create(
            name="Unicode", host="localhost", port=5432,
            database_name="db", db_username="user",
            db_password=unicode_pw, user=user,
        )
        profile.refresh_from_db()
        assert profile.get_decrypted_password() == unicode_pw

    def test_empty_password(self, user):
        """Verify empty password is handled (some DBs allow no password)."""
        profile = ConnectionProfile.objects.create(
            name="NoPW", host="localhost", port=5432,
            database_name="db", db_username="user",
            db_password="", user=user,
        )
        profile.refresh_from_db()
        assert profile.get_decrypted_password() == ""

    def test_created_at_auto_set(self, connection_profile):
        """Verify created_at is automatically populated."""
        assert connection_profile.created_at is not None


class TestConnectionService:
    """Tests for the connection_service CRUD functions."""

    def test_create_connection_profile(self, user):
        """Verify create returns correct data and persists to DB."""
        result = create_connection_profile(
            user_id=user.id,
            name="NewConn",
            host="db.example.com",
            port=5433,
            database_name="newdb",
            db_username="newuser",
            db_password="newpass",
        )
        assert result["name"] == "NewConn"
        assert result["host"] == "db.example.com"
        assert result["port"] == 5433
        assert "id" in result
        assert ConnectionProfile.objects.filter(id=result["id"]).exists()

    def test_create_password_not_in_response(self, user):
        """Verify that password is never returned in the serialized output."""
        result = create_connection_profile(
            user_id=user.id,
            name="Secret",
            host="h", port=5432, database_name="d",
            db_username="u", db_password="secret123",
        )
        assert "db_password" not in result
        assert "password" not in result

    def test_create_with_nonexistent_user_raises(self):
        """Verify creating a profile for a non-existent user raises."""
        with pytest.raises(User.DoesNotExist):
            create_connection_profile(
                user_id=99999,
                name="X", host="h", port=5432,
                database_name="d", db_username="u", db_password="p",
            )

    def test_list_connection_profiles(self, user, connection_profile):
        """Verify listing returns profiles for the correct user."""
        results = list_connection_profiles(user_id=user.id)
        assert len(results) >= 1
        assert any(r["name"] == "Test DB" for r in results)

    def test_list_connection_profiles_isolation(self, user, connection_profile, other_profile):
        """Verify that listing profiles for one user does not return another's."""
        results = list_connection_profiles(user_id=user.id)
        ids = [r["id"] for r in results]
        assert other_profile.id not in ids

    def test_list_empty_for_new_user(self, other_user):
        """Verify empty list for a user with no profiles (after other_profile not created)."""
        results = list_connection_profiles(user_id=other_user.id)
        assert results == []

    def test_list_ordered_by_created_at(self, user):
        """Verify profiles are returned in creation order."""
        p1 = create_connection_profile(
            user_id=user.id, name="First",
            host="h", port=5432, database_name="d1", db_username="u", db_password="p",
        )
        p2 = create_connection_profile(
            user_id=user.id, name="Second",
            host="h", port=5432, database_name="d2", db_username="u", db_password="p",
        )
        results = list_connection_profiles(user_id=user.id)
        names = [r["name"] for r in results]
        assert names.index("First") < names.index("Second")

    def test_get_connection_profile(self, user, connection_profile):
        """Verify get returns the correct profile."""
        result = get_connection_profile(user_id=user.id, profile_id=connection_profile.id)
        assert result is not None
        assert result["name"] == "Test DB"

    def test_get_connection_profile_not_found(self, user):
        """Verify get returns None for non-existent profile."""
        result = get_connection_profile(user_id=user.id, profile_id=99999)
        assert result is None

    def test_get_wrong_user_returns_none(self, user, other_profile):
        """Verify that accessing another user's profile returns None."""
        result = get_connection_profile(user_id=user.id, profile_id=other_profile.id)
        assert result is None

    def test_update_connection_profile(self, user, connection_profile):
        """Verify update modifies the profile fields."""
        result = update_connection_profile(
            user_id=user.id,
            profile_id=connection_profile.id,
            name="Updated DB",
            host="newhost.example.com",
        )
        assert result["name"] == "Updated DB"
        assert result["host"] == "newhost.example.com"

    def test_update_password_re_encrypts(self, user, connection_profile):
        """Verify updating db_password re-encrypts it."""
        update_connection_profile(
            user_id=user.id,
            profile_id=connection_profile.id,
            db_password="brandnewpassword",
        )
        connection_profile.refresh_from_db()
        assert connection_profile.get_decrypted_password() == "brandnewpassword"

    def test_update_partial_fields(self, user, connection_profile):
        """Verify updating only one field doesn't affect others."""
        original_host = connection_profile.host
        update_connection_profile(
            user_id=user.id,
            profile_id=connection_profile.id,
            name="Only Name Changed",
        )
        connection_profile.refresh_from_db()
        assert connection_profile.name == "Only Name Changed"
        assert connection_profile.host == original_host

    def test_update_nonexistent_raises(self, user):
        """Verify updating a nonexistent profile raises DoesNotExist."""
        with pytest.raises(ConnectionProfile.DoesNotExist):
            update_connection_profile(user_id=user.id, profile_id=99999, name="X")

    def test_update_wrong_user_raises(self, user, other_profile):
        """Verify that updating another user's profile raises DoesNotExist."""
        with pytest.raises(ConnectionProfile.DoesNotExist):
            update_connection_profile(
                user_id=user.id,
                profile_id=other_profile.id,
                name="Hacked",
            )

    def test_update_ignores_unknown_fields(self, user, connection_profile):
        """Verify that unknown fields in kwargs are silently ignored."""
        result = update_connection_profile(
            user_id=user.id,
            profile_id=connection_profile.id,
            nonexistent_field="ignored",
        )
        assert result["name"] == connection_profile.name

    def test_delete_connection_profile(self, user, connection_profile):
        """Verify delete removes the profile."""
        pid = connection_profile.id
        delete_connection_profile(user_id=user.id, profile_id=pid)
        assert not ConnectionProfile.objects.filter(id=pid).exists()

    def test_delete_cascades_commits(self, user, connection_profile, commit_event, inverse_operation):
        """Verify deleting a profile cascades to commits and inverse operations."""
        pid = connection_profile.id
        cid = commit_event.id
        delete_connection_profile(user_id=user.id, profile_id=pid)
        assert not CommitEvent.objects.filter(id=cid).exists()
        assert not InverseOperation.objects.filter(commit_id=cid).exists()

    def test_delete_cascades_snapshots(self, user, connection_profile, snapshot):
        """Verify deleting a profile cascades to snapshots."""
        sid = snapshot.id
        delete_connection_profile(user_id=user.id, profile_id=connection_profile.id)
        assert not Snapshot.objects.filter(id=sid).exists()

    def test_delete_cascades_snapshot_policy(self, user, connection_profile, snapshot_policy):
        """Verify deleting a profile cascades to snapshot policy."""
        pid = snapshot_policy.id
        delete_connection_profile(user_id=user.id, profile_id=connection_profile.id)
        assert not SnapshotPolicy.objects.filter(id=pid).exists()

    def test_delete_nonexistent_raises(self, user):
        """Verify deleting a nonexistent profile raises DoesNotExist."""
        with pytest.raises(ConnectionProfile.DoesNotExist):
            delete_connection_profile(user_id=user.id, profile_id=99999)

    def test_delete_wrong_user_raises(self, user, other_profile):
        """Verify that deleting another user's profile raises DoesNotExist."""
        with pytest.raises(ConnectionProfile.DoesNotExist):
            delete_connection_profile(user_id=user.id, profile_id=other_profile.id)
