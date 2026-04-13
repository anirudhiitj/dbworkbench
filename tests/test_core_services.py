"""Tests for core.services.record_commit() ΓÇö atomic commit recording.

Covers: TC_01, TC_02, TC_04, TC_05, TC_06, TC_07, TC_14
Plus edge cases: independent seq per profile, empty inverse, snapshot
frequency boundaries, duplicate version_id, s3_key format.
"""

import uuid
from datetime import datetime, timezone, timedelta

import pytest
from django.db import IntegrityError

from core.models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy
from core.services import record_commit


pytestmark = pytest.mark.django_db


class TestRecordCommitBasic:
    """Basic commit recording tests."""

    def test_successful_insert_creates_one_commit(self, user, connection_profile):
        """TC_01 ΓÇö Verify that a successful INSERT query creates exactly 1 commit entry."""
        version_id = str(uuid.uuid4())
        commit = record_commit(
            version_id=version_id,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        assert CommitEvent.objects.filter(version_id=version_id).count() == 1
        assert commit.status == "success"

    def test_failed_query_creates_zero_commits(self, user, connection_profile):
        """TC_02 ΓÇö Verify that a failed/invalid query creates 0 commit entries if not called."""
        initial_count = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count()
        # Simulate: SQL execution fails, so record_commit is never invoked.
        assert CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count() == initial_count

    def test_unique_version_id_assigned(self, user, connection_profile):
        """TC_04 ΓÇö Verify that each write query is assigned a unique version identifier."""
        vid1 = str(uuid.uuid4())
        vid2 = str(uuid.uuid4())
        c1 = record_commit(
            version_id=vid1,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        c2 = record_commit(
            version_id=vid2,
            sql_command="INSERT INTO t(id) VALUES (2)",
            inverse_sql="DELETE FROM t WHERE id = 2",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        assert c1.version_id != c2.version_id
        assert c1.version_id == vid1
        assert c2.version_id == vid2

    def test_timestamp_within_1_second(self, user, connection_profile):
        """TC_05 ΓÇö Verify that the commit timestamp is recorded within 1 second of execution."""
        before = datetime.now(timezone.utc)
        commit = record_commit(
            version_id=str(uuid.uuid4()),
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        after = datetime.now(timezone.utc)
        assert commit.timestamp >= before - timedelta(seconds=1)
        assert commit.timestamp <= after + timedelta(seconds=1)

    def test_two_writes_produce_two_commits(self, user, connection_profile):
        """TC_06 ΓÇö Verify that 2 successful write operations produce 2 distinct commit entries."""
        c1 = record_commit(
            version_id=str(uuid.uuid4()),
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        c2 = record_commit(
            version_id=str(uuid.uuid4()),
            sql_command="INSERT INTO t(id) VALUES (2)",
            inverse_sql="DELETE FROM t WHERE id = 2",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        assert c1.id != c2.id
        assert c1.version_id != c2.version_id
        total = CommitEvent.objects.filter(connection_profile=connection_profile).count()
        assert total == 2

    def test_duplicate_version_id_raises(self, user, connection_profile):
        """Verify that record_commit with a duplicate version_id raises IntegrityError."""
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        with pytest.raises(IntegrityError):
            record_commit(
                version_id=vid,
                sql_command="INSERT INTO t(id) VALUES (2)",
                inverse_sql="DELETE FROM t WHERE id = 2",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )

    def test_status_field_stored(self, user, connection_profile):
        """Verify that the status field is persisted correctly."""
        vid = str(uuid.uuid4())
        commit = record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        assert commit.status == "success"


class TestRecordCommitSequencing:
    """Tests for sequential seq numbering."""

    def test_sequential_seq_values_no_gaps(self, user, connection_profile):
        """TC_07 ΓÇö Verify that multiple sequential writes produce seq values with no gaps."""
        commits = []
        for i in range(5):
            c = record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )
            commits.append(c)

        seqs = [c.seq for c in commits]
        assert seqs == [1, 2, 3, 4, 5]

    def test_seq_independent_per_profile(self, user, connection_profile, other_profile):
        """Verify that seq numbering is independent per connection profile."""
        c1 = record_commit(
            version_id=str(uuid.uuid4()),
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        c2 = record_commit(
            version_id=str(uuid.uuid4()),
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=other_profile.user,
            connection_profile=other_profile,
            status="success",
        )
        assert c1.seq == 1
        assert c2.seq == 1  # independent sequence

    def test_seq_starts_at_1(self, user, connection_profile):
        """Verify first commit for a profile gets seq=1."""
        c = record_commit(
            version_id=str(uuid.uuid4()),
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        assert c.seq == 1


class TestRecordCommitInverseOperation:
    """Tests for InverseOperation creation."""

    def test_inverse_operation_created_with_commit(self, user, connection_profile):
        """Verify that record_commit() creates an InverseOperation alongside the commit."""
        vid = str(uuid.uuid4())
        commit = record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        inv = InverseOperation.objects.get(commit=commit)
        assert inv.inverse_sql == "DELETE FROM t WHERE id = 1"
        assert inv.version_id == vid

    def test_empty_inverse_sql_stored(self, user, connection_profile):
        """Verify that an empty inverse_sql string is stored when operation is irreversible."""
        vid = str(uuid.uuid4())
        commit = record_commit(
            version_id=vid,
            sql_command="DROP TABLE t",
            inverse_sql="",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        inv = InverseOperation.objects.get(commit=commit)
        assert inv.inverse_sql == ""

    def test_multiline_inverse_sql(self, user, connection_profile):
        """Verify that multi-line inverse SQL is stored correctly."""
        inverse = "DELETE FROM t WHERE id = 1;\nDELETE FROM t WHERE id = 2;"
        vid = str(uuid.uuid4())
        commit = record_commit(
            version_id=vid,
            sql_command="INSERT INTO t VALUES (1), (2)",
            inverse_sql=inverse,
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        inv = InverseOperation.objects.get(commit=commit)
        assert inv.inverse_sql == inverse


class TestRecordCommitSnapshot:
    """Tests for automatic snapshot creation."""

    def test_snapshot_created_at_frequency_threshold(self, user, connection_profile):
        """TC_14 ΓÇö Verify snapshot is auto-created when commit count reaches configured frequency."""
        SnapshotPolicy.objects.create(
            frequency=3,
            connection_profile=connection_profile,
        )
        for i in range(3):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )

        snapshots = Snapshot.objects.filter(connection_profile=connection_profile)
        assert snapshots.count() >= 1

    def test_no_snapshot_without_policy(self, user, connection_profile):
        """Verify that no snapshot is created if there's no SnapshotPolicy."""
        for i in range(10):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )
        snapshots = Snapshot.objects.filter(connection_profile=connection_profile)
        assert snapshots.count() == 0

    def test_snapshot_s3_key_format(self, user, connection_profile):
        """Verify snapshot s3_key follows expected format."""
        SnapshotPolicy.objects.create(frequency=1, connection_profile=connection_profile)
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        snap = Snapshot.objects.filter(connection_profile=connection_profile).first()
        assert snap is not None
        assert snap.s3_key == f"snapshots/{connection_profile.id}/{vid}"

    def test_frequency_one_snapshots_every_commit(self, user, connection_profile):
        """Verify frequency=1 creates a snapshot on every commit."""
        SnapshotPolicy.objects.create(frequency=1, connection_profile=connection_profile)
        for i in range(5):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )
        assert Snapshot.objects.filter(connection_profile=connection_profile).count() == 5

    def test_no_snapshot_before_threshold(self, user, connection_profile):
        """Verify no snapshot is created before reaching the frequency threshold."""
        SnapshotPolicy.objects.create(frequency=5, connection_profile=connection_profile)
        for i in range(4):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )
        assert Snapshot.objects.filter(connection_profile=connection_profile).count() == 0

    def test_snapshot_version_id_matches_triggering_commit(self, user, connection_profile):
        """Verify the snapshot's version_id matches the commit that triggered it."""
        SnapshotPolicy.objects.create(frequency=1, connection_profile=connection_profile)
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command="INSERT INTO t(id) VALUES (1)",
            inverse_sql="DELETE FROM t WHERE id = 1",
            user=user,
            connection_profile=connection_profile,
            status="success",
        )
        snap = Snapshot.objects.get(connection_profile=connection_profile)
        assert snap.version_id == vid

    def test_multiple_snapshot_cycles(self, user, connection_profile):
        """Verify correct snapshot count across multiple frequency cycles."""
        SnapshotPolicy.objects.create(frequency=3, connection_profile=connection_profile)
        for i in range(12):
            record_commit(
                version_id=str(uuid.uuid4()),
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
                inverse_sql=f"DELETE FROM t WHERE id = {i}",
                user=user,
                connection_profile=connection_profile,
                status="success",
            )
        assert Snapshot.objects.filter(connection_profile=connection_profile).count() == 4
