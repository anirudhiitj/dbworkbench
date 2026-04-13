"""Tests for snapshot service ΓÇö frequency management, listing, upload/restore.

Covers: TC_15, TC_19, TC_20, TC_21, TC_36, TC_46, TC_60
Plus edge cases: pg_dump/psql failures, ownership, large frequency, temp file cleanup.
"""

import uuid
from unittest.mock import patch, MagicMock, call
import subprocess

import pytest

from core.models import CommitEvent, Snapshot, SnapshotPolicy
from core.services import record_commit
from connections.models import ConnectionProfile
from fastapi_backend.app.services.snapshot_service import (
    get_snapshot_frequency,
    set_snapshot_frequency,
    list_snapshots_for_profile,
    upload_snapshot_data,
    restore_snapshot_data,
)


pytestmark = pytest.mark.django_db


class TestSnapshotFrequency:
    """Tests for snapshot frequency get/set."""

    def test_default_frequency_when_no_policy(self, connection_profile):
        """TC_19 ΓÇö Verify that when no SnapshotPolicy exists, the default frequency of 5 is returned."""
        freq = get_snapshot_frequency(connection_profile.id)
        assert freq == 5

    def test_set_valid_frequency(self, user, connection_profile):
        """TC_21 ΓÇö Verify that setting a valid snapshot frequency (e.g., 3) is accepted and persisted."""
        result = set_snapshot_frequency(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            frequency=3,
        )
        assert result == 3
        assert get_snapshot_frequency(connection_profile.id) == 3

    def test_update_existing_frequency(self, user, connection_profile, snapshot_policy):
        """Verify that updating an existing policy works."""
        result = set_snapshot_frequency(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            frequency=10,
        )
        assert result == 10
        assert get_snapshot_frequency(connection_profile.id) == 10

    def test_frequency_zero_rejected_by_schema(self):
        """TC_20 ΓÇö Verify that setting snapshot frequency to 0 is rejected."""
        from pydantic import ValidationError
        from fastapi_backend.app.models.schemas import SnapshotFrequencyRequest

        with pytest.raises(ValidationError):
            SnapshotFrequencyRequest(connection_profile_id=1, frequency=0)

    def test_frequency_negative_rejected_by_schema(self):
        """TC_20 ΓÇö Verify that setting snapshot frequency to negative is rejected."""
        from pydantic import ValidationError
        from fastapi_backend.app.models.schemas import SnapshotFrequencyRequest

        with pytest.raises(ValidationError):
            SnapshotFrequencyRequest(connection_profile_id=1, frequency=-1)

    def test_set_frequency_large_value(self, user, connection_profile):
        """Verify that a very large frequency value is accepted."""
        result = set_snapshot_frequency(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            frequency=100000,
        )
        assert result == 100000

    def test_set_frequency_wrong_user_raises(self, user, other_profile):
        """Verify that setting frequency for another user's profile raises."""
        with pytest.raises(ConnectionProfile.DoesNotExist):
            set_snapshot_frequency(
                user_id=user.id,
                connection_profile_id=other_profile.id,
                frequency=5,
            )

    def test_frequency_one_is_valid(self, user, connection_profile):
        """Verify that frequency=1 (snapshot every commit) is accepted."""
        result = set_snapshot_frequency(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            frequency=1,
        )
        assert result == 1


class TestSnapshotRetrieval:
    """Tests for snapshot retrieval/listing."""

    def test_snapshot_retrievable_for_profile(self, user, connection_profile, snapshot):
        """TC_15 ΓÇö Verify that a snapshot can be retrieved by querying snapshots for a connection profile."""
        results = list_snapshots_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        assert len(results) >= 1
        snap = results[0]
        assert "snapshot_id" in snap
        assert "version_id" in snap
        assert "s3_key" in snap
        assert "created_at" in snap
        assert snap["connection_profile_id"] == connection_profile.id

    def test_list_empty_when_no_snapshots(self, user, connection_profile):
        """Verify empty list when no snapshots exist."""
        results = list_snapshots_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        assert results == []

    def test_list_ordered_by_created_at_desc(self, user, connection_profile, commit_event):
        """Verify snapshots are returned newest first."""
        s1 = Snapshot.objects.create(
            version_id="v-s1", s3_key="snapshots/1/v-s1",
            connection_profile=connection_profile,
        )
        s2 = Snapshot.objects.create(
            version_id="v-s2", s3_key="snapshots/1/v-s2",
            connection_profile=connection_profile,
        )
        results = list_snapshots_for_profile(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
        )
        # Newest first
        assert results[0]["version_id"] == "v-s2"

    def test_list_ownership_isolation(self, user, other_user, connection_profile, other_profile):
        """Verify user cannot list another user's snapshots."""
        Snapshot.objects.create(
            version_id="v-other", s3_key="snapshots/x/v-other",
            connection_profile=other_profile,
        )
        results = list_snapshots_for_profile(
            user_id=user.id,
            connection_profile_id=other_profile.id,
        )
        assert results == []


class TestSnapshotLinearGrowth:
    """Tests for snapshot storage growth."""

    def test_snapshot_count_grows_linearly(self, user, connection_profile):
        """TC_36 ΓÇö Verify that snapshot storage grows linearly with the number of commits."""
        SnapshotPolicy.objects.create(frequency=3, connection_profile=connection_profile)

        for i in range(9):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )

        snapshots = Snapshot.objects.filter(connection_profile=connection_profile)
        assert snapshots.count() == 3

    def test_efficient_snapshot_count_with_policy(self, user, connection_profile):
        """TC_60 ΓÇö Verify efficient storage: creating many commits with frequency policy results in expected snapshot count."""
        SnapshotPolicy.objects.create(frequency=5, connection_profile=connection_profile)

        for i in range(20):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )

        snapshots = Snapshot.objects.filter(connection_profile=connection_profile)
        assert snapshots.count() == 4


class TestSnapshotUploadRestore:
    """Tests for upload_snapshot_data and restore_snapshot_data with mocks."""

    def test_upload_snapshot_data(self, connection_profile, mock_subprocess, mock_s3, mock_snapshot_conn):
        """TC_46 ΓÇö Verify that snapshot upload calls pg_dump and S3 upload."""
        upload_snapshot_data(connection_profile, "snapshots/1/v1")
        mock_subprocess.assert_called_once()
        call_args = mock_subprocess.call_args
        assert "pg_dump" in call_args[0][0][0]  # substring check on executable path
        mock_s3["upload"].assert_called_once()

    def test_restore_snapshot_data(self, connection_profile, mock_subprocess, mock_s3, mock_snapshot_conn):
        """Verify that snapshot restore calls S3 download and psql."""
        restore_snapshot_data("snapshots/1/v1", connection_profile)
        mock_s3["download"].assert_called_once()
        mock_subprocess.assert_called_once()
        call_args = mock_subprocess.call_args
        assert "psql" in call_args[0][0][0]  # substring check on executable path

    def test_upload_passes_correct_credentials(self, connection_profile, mock_subprocess, mock_s3, mock_snapshot_conn):
        """Verify pg_dump is called with the correct host, port, username, database."""
        upload_snapshot_data(connection_profile, "snapshots/1/v1")
        call_args = mock_subprocess.call_args[0][0]
        assert connection_profile.host in call_args
        assert str(connection_profile.port) in call_args
        assert connection_profile.db_username in call_args
        assert connection_profile.database_name in call_args

    def test_restore_passes_correct_credentials(self, connection_profile, mock_subprocess, mock_s3, mock_snapshot_conn):
        """Verify psql is called with the correct host, port, username, database."""
        restore_snapshot_data("snapshots/1/v1", connection_profile)
        call_args = mock_subprocess.call_args[0][0]
        assert connection_profile.host in call_args
        assert str(connection_profile.port) in call_args
        assert connection_profile.db_username in call_args
        assert connection_profile.database_name in call_args

    def test_upload_pgpassword_in_env(self, connection_profile, mock_subprocess, mock_s3, mock_snapshot_conn):
        """Verify PGPASSWORD is set in the environment for pg_dump."""
        upload_snapshot_data(connection_profile, "snapshots/1/v1")
        call_env = mock_subprocess.call_args[1]["env"]
        assert "PGPASSWORD" in call_env
        assert call_env["PGPASSWORD"] == connection_profile.get_decrypted_password()

    def test_upload_pg_dump_failure_raises(self, connection_profile, mock_s3, mock_snapshot_conn):
        """Verify that a pg_dump failure propagates as an exception."""
        with patch("fastapi_backend.app.services.snapshot_service.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, "pg_dump")
            with pytest.raises(subprocess.CalledProcessError):
                upload_snapshot_data(connection_profile, "snapshots/1/v1")

    def test_restore_psql_failure_raises(self, connection_profile, mock_s3, mock_snapshot_conn):
        """Verify that a psql failure propagates as an exception."""
        with patch("fastapi_backend.app.services.snapshot_service.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, "psql")
            with pytest.raises(subprocess.CalledProcessError):
                restore_snapshot_data("snapshots/1/v1", connection_profile)

    def test_upload_check_true(self, connection_profile, mock_subprocess, mock_s3, mock_snapshot_conn):
        """Verify pg_dump is called with check=True to catch failures."""
        upload_snapshot_data(connection_profile, "snapshots/1/v1")
        assert mock_subprocess.call_args[1]["check"] is True

    def test_restore_check_true(self, connection_profile, mock_subprocess, mock_s3, mock_snapshot_conn):
        """Verify psql is called with check=True to catch failures."""
        restore_snapshot_data("snapshots/1/v1", connection_profile)
        assert mock_subprocess.call_args[1]["check"] is True
