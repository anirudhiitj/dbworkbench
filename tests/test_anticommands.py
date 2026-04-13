"""Tests for anticommand (inverse operation) service.

Covers retrieval of stored inverse operations, ownership isolation,
edge cases for empty/missing data, and cross-profile isolation.
"""

import uuid

import pytest

from core.models import CommitEvent, InverseOperation
from core.services import record_commit
from fastapi_backend.app.services.anticommand_service import (
    get_inverse_for_version,
    get_inverses_for_profile,
)


pytestmark = pytest.mark.django_db


class TestGetInverseForVersion:
    """Tests for get_inverse_for_version()."""

    def test_returns_inverse_for_existing_version(self, user, connection_profile):
        """Verify retrieving inverse operation by version_id."""
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        result = get_inverse_for_version(user_id=user.id, version_id=vid)
        assert result is not None
        assert result["version_id"] == vid
        assert result["inverse_sql"] == "DELETE FROM t WHERE id = 1"
        assert result["commit_version_id"] == vid

    def test_returns_none_for_nonexistent_version(self, user):
        """Verify None is returned for a non-existent version_id."""
        result = get_inverse_for_version(user_id=user.id, version_id="nonexistent")
        assert result is None

    def test_ownership_isolation(self, user, other_user, connection_profile):
        """Verify user cannot access another user's inverse operations."""
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        result = get_inverse_for_version(user_id=other_user.id, version_id=vid)
        assert result is None

    def test_returns_empty_inverse_sql(self, user, connection_profile):
        """Verify that an empty inverse_sql is returned correctly (irreversible op)."""
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command="DROP TABLE t",
            inverse_sql="",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        result = get_inverse_for_version(user_id=user.id, version_id=vid)
        assert result is not None
        assert result["inverse_sql"] == ""

    def test_multiline_inverse_sql_returned(self, user, connection_profile):
        """Verify multi-line inverse SQL is returned intact."""
        vid = str(uuid.uuid4())
        inverse = "DELETE FROM t WHERE id = 1\nDELETE FROM t WHERE id = 2"
        record_commit(
            version_id=vid,
            sql_command="INSERT INTO t VALUES (1), (2)",
            inverse_sql=inverse,
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        result = get_inverse_for_version(user_id=user.id, version_id=vid)
        assert result["inverse_sql"] == inverse


class TestGetInversesForProfile:
    """Tests for get_inverses_for_profile()."""

    def test_returns_all_inverses_for_profile(self, user, connection_profile):
        """Verify all inverse operations for a profile are returned."""
        for i in range(3):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )
        results = get_inverses_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        assert len(results) == 3

    def test_ordered_by_commit_timestamp(self, user, connection_profile):
        """Verify results are ordered by commit timestamp."""
        vids = []
        for i in range(3):
            vid = str(uuid.uuid4())
            vids.append(vid)
            record_commit(
                version_id=vid,
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )
        results = get_inverses_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        result_vids = [r["version_id"] for r in results]
        assert result_vids == vids

    def test_empty_for_no_commits(self, user, connection_profile):
        """Verify empty list for profile with no commits."""
        results = get_inverses_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        assert results == []

    def test_cross_profile_isolation(self, user, connection_profile, other_user, other_profile):
        """Verify inverses from one profile don't appear in another's listing."""
        vid1 = str(uuid.uuid4())
        record_commit(
            version_id=vid1,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        vid2 = str(uuid.uuid4())
        record_commit(
            version_id=vid2,
            sql_command="INSERT INTO t(id) VALUES (2)",
            inverse_sql="DELETE FROM t WHERE id = 2",
            user=other_user,
            connection_profile=other_profile,
            status="success",
        )
        results_p1 = get_inverses_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        results_p2 = get_inverses_for_profile(
            user_id=other_user.id,
            connection_profile_id=other_profile.id,
        )
        assert len(results_p1) == 1
        assert len(results_p2) == 1
        assert results_p1[0]["version_id"] == vid1
        assert results_p2[0]["version_id"] == vid2

    def test_wrong_user_returns_empty(self, user, other_user, connection_profile):
        """Verify that querying inverses with the wrong user returns empty."""
        record_commit(
            version_id=str(uuid.uuid4()),
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        results = get_inverses_for_profile(
            user_id=other_user.id,
            connection_profile_id=connection_profile.id,
        )
        assert results == []

    def test_response_fields_present(self, user, connection_profile):
        """Verify all expected fields are present in the response."""
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        results = get_inverses_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        result = results[0]
        assert "version_id" in result
        assert "inverse_sql" in result
        assert "commit_version_id" in result
