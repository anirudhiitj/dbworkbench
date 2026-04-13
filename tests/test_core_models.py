"""Tests for Django ORM models ΓÇö CommitEvent, InverseOperation, Snapshot, SnapshotPolicy.

Covers: TC_08, TC_09
Plus edge cases: constraints, cascades, defaults, isolation, NULL handling.
"""

import time
import uuid
import pytest

from django.db import IntegrityError

from core.models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy
from connections.models import ConnectionProfile


pytestmark = pytest.mark.django_db


class TestCommitEvent:
    """Tests for the CommitEvent model."""

    def test_commit_persists_after_creation(self, commit_event):
        """TC_08 ΓÇö Verify that commit history persists in the database (survives ORM re-query)."""
        fetched = CommitEvent.objects.get(version_id=commit_event.version_id)
        assert fetched.sql_command == commit_event.sql_command
        assert fetched.status == "success"
        assert fetched.seq == commit_event.seq

    def test_commit_survives_fresh_queryset(self, user, connection_profile):
        """TC_08 ΓÇö Verify commit data survives a fresh ORM query session (simulated restart)."""
        CommitEvent.objects.create(
            version_id="v-persist-test",
            seq=1,
            sql_command="INSERT INTO t(id) VALUES (99)",
            status="success",
            user=user,
            connection_profile=connection_profile,
        )
        # Simulate fresh query (new QuerySet, no cache)
        fresh_qs = CommitEvent.objects.all()
        assert fresh_qs.filter(version_id="v-persist-test").exists()
        fetched = CommitEvent.objects.get(version_id="v-persist-test")
        assert fetched.sql_command == "INSERT INTO t(id) VALUES (99)"

    def test_commit_retrieved_within_2_seconds(self, commit_event):
        """TC_09 ΓÇö Verify that a specific commit can be retrieved by version_id within 2 seconds."""
        start = time.time()
        fetched = CommitEvent.objects.get(version_id=commit_event.version_id)
        elapsed = time.time() - start
        assert fetched is not None
        assert elapsed < 2.0

    def test_version_id_unique_constraint(self, user, connection_profile):
        """Verify that duplicate version_ids raise IntegrityError."""
        CommitEvent.objects.create(
            version_id="v-dup",
            seq=1,
            sql_command="INSERT INTO a(x) VALUES (1)",
            status="success",
            user=user,
            connection_profile=connection_profile,
        )
        with pytest.raises(IntegrityError):
            CommitEvent.objects.create(
                version_id="v-dup",
                seq=2,
                sql_command="INSERT INTO a(x) VALUES (2)",
                status="success",
                user=user,
                connection_profile=connection_profile,
            )

    def test_seq_unique_per_profile_constraint(self, user, connection_profile):
        """Verify that duplicate (connection_profile, seq) raises IntegrityError."""
        CommitEvent.objects.create(
            version_id="v-seq1",
            seq=10,
            sql_command="INSERT INTO a(x) VALUES (1)",
            status="success",
            user=user,
            connection_profile=connection_profile,
        )
        with pytest.raises(IntegrityError):
            CommitEvent.objects.create(
                version_id="v-seq2",
                seq=10,
                sql_command="INSERT INTO a(x) VALUES (2)",
                status="success",
                user=user,
                connection_profile=connection_profile,
            )

    def test_auto_timestamp(self, commit_event):
        """Verify that timestamp is auto-set on creation."""
        assert commit_event.timestamp is not None

    def test_same_seq_allowed_for_different_profiles(self, user, connection_profile, other_profile):
        """Verify that two different profiles can have the same seq number."""
        CommitEvent.objects.create(
            version_id="v-p1-s1", seq=1,
            sql_command="SELECT 1", status="success",
            user=user, connection_profile=connection_profile,
        )
        c2 = CommitEvent.objects.create(
            version_id="v-p2-s1", seq=1,
            sql_command="SELECT 1", status="success",
            user=other_profile.user, connection_profile=other_profile,
        )
        assert c2.seq == 1  # no IntegrityError

    def test_related_name_commit_events(self, user, connection_profile):
        """Verify the related_name='commit_events' FK lookup from profile."""
        CommitEvent.objects.create(
            version_id="v-rel", seq=1,
            sql_command="SELECT 1", status="success",
            user=user, connection_profile=connection_profile,
        )
        assert connection_profile.commit_events.count() == 1

    def test_long_sql_command_stored(self, user, connection_profile):
        """Verify that very long SQL commands are stored without truncation."""
        long_sql = "INSERT INTO t(data) VALUES ('" + "x" * 10000 + "')"
        c = CommitEvent.objects.create(
            version_id="v-long", seq=1,
            sql_command=long_sql, status="success",
            user=user, connection_profile=connection_profile,
        )
        c.refresh_from_db()
        assert len(c.sql_command) == len(long_sql)

    def test_cascade_delete_from_user(self, user, connection_profile, commit_event):
        """Verify that deleting a user cascades through profile to commits."""
        cid = commit_event.id
        user.delete()
        assert not CommitEvent.objects.filter(id=cid).exists()

    def test_cascade_delete_from_profile(self, commit_event, connection_profile):
        """Verify that deleting a profile cascades to its commits."""
        cid = commit_event.id
        connection_profile.delete()
        assert not CommitEvent.objects.filter(id=cid).exists()


class TestInverseOperation:
    """Tests for the InverseOperation model."""

    def test_inverse_linked_to_commit(self, commit_event, inverse_operation):
        """Verify inverse_operation is linked to the correct commit."""
        assert inverse_operation.commit == commit_event
        assert inverse_operation.version_id == commit_event.version_id

    def test_inverse_cascades_on_commit_delete(self, commit_event, inverse_operation):
        """Verify deleting a commit cascades to its inverse operation."""
        inv_id = inverse_operation.id
        commit_event.delete()
        assert not InverseOperation.objects.filter(id=inv_id).exists()

    def test_one_to_one_constraint(self, commit_event, inverse_operation):
        """Verify only one inverse per commit (OneToOneField)."""
        with pytest.raises(IntegrityError):
            InverseOperation.objects.create(
                version_id="v-dup-inv",
                inverse_sql="SELECT 1",
                commit=commit_event,
            )

    def test_reverse_access_from_commit(self, commit_event, inverse_operation):
        """Verify commit.inverse_operation reverse accessor works."""
        assert commit_event.inverse_operation == inverse_operation

    def test_long_inverse_sql(self, user, connection_profile):
        """Verify that very long inverse SQL is stored correctly."""
        long_inv = "DELETE FROM t WHERE id IN (" + ",".join(str(i) for i in range(5000)) + ")"
        c = CommitEvent.objects.create(
            version_id="v-longinv", seq=1,
            sql_command="TRUNCATE t", status="success",
            user=user, connection_profile=connection_profile,
        )
        inv = InverseOperation.objects.create(
            version_id=c.version_id, inverse_sql=long_inv, commit=c,
        )
        inv.refresh_from_db()
        assert inv.inverse_sql == long_inv


class TestSnapshot:
    """Tests for the Snapshot model."""

    def test_snapshot_has_uuid(self, snapshot):
        """Verify snapshot_id is a valid UUID."""
        assert snapshot.snapshot_id is not None
        uuid.UUID(str(snapshot.snapshot_id))  # should not raise

    def test_snapshot_linked_to_profile(self, snapshot, connection_profile):
        """Verify snapshot is linked to the correct connection profile."""
        assert snapshot.connection_profile == connection_profile

    def test_snapshot_auto_timestamp(self, snapshot):
        """Verify created_at is automatically set."""
        assert snapshot.created_at is not None

    def test_snapshot_cascade_delete_from_profile(self, snapshot, connection_profile):
        """Verify deleting a profile cascades to its snapshots."""
        sid = snapshot.id
        connection_profile.delete()
        assert not Snapshot.objects.filter(id=sid).exists()

    def test_multiple_snapshots_per_profile(self, connection_profile, commit_event):
        """Verify multiple snapshots can exist for the same profile."""
        Snapshot.objects.create(
            version_id="v-snap-1", s3_key="snapshots/1/v-snap-1",
            connection_profile=connection_profile,
        )
        Snapshot.objects.create(
            version_id="v-snap-2", s3_key="snapshots/1/v-snap-2",
            connection_profile=connection_profile,
        )
        assert Snapshot.objects.filter(connection_profile=connection_profile).count() >= 2

    def test_snapshot_s3_key_stored(self, snapshot, connection_profile, commit_event):
        """Verify s3_key is stored and retrievable."""
        expected = f"snapshots/{connection_profile.id}/{commit_event.version_id}"
        assert snapshot.s3_key == expected


class TestSnapshotPolicy:
    """Tests for the SnapshotPolicy model."""

    def test_policy_frequency(self, snapshot_policy):
        """Verify policy stores the correct frequency."""
        assert snapshot_policy.frequency == 5

    def test_policy_one_to_one(self, connection_profile, snapshot_policy):
        """Verify only one policy per connection profile."""
        with pytest.raises(IntegrityError):
            SnapshotPolicy.objects.create(
                frequency=10,
                connection_profile=connection_profile,
            )

    def test_policy_auto_timestamp(self, snapshot_policy):
        """Verify last_updated is auto-set."""
        assert snapshot_policy.last_updated is not None

    def test_policy_cascade_delete_from_profile(self, snapshot_policy, connection_profile):
        """Verify deleting a profile cascades to its policy."""
        pid = snapshot_policy.id
        connection_profile.delete()
        assert not SnapshotPolicy.objects.filter(id=pid).exists()

    def test_policy_different_profiles_independent(self, connection_profile, other_profile):
        """Verify each profile can have its own policy."""
        p1 = SnapshotPolicy.objects.create(frequency=3, connection_profile=connection_profile)
        p2 = SnapshotPolicy.objects.create(frequency=10, connection_profile=other_profile)
        assert p1.frequency == 3
        assert p2.frequency == 10
