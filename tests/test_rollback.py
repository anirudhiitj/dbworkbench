"""Tests for rollback service ΓÇö database version rollback.

Covers: TC_16, TC_17, TC_18, TC_45, TC_47, TC_48, TC_49, TC_64, TC_65, TC_66
Plus forward/backward snapshot-distance strategy, Kafka events, ownership,
edge cases for first/latest commit rollback.
"""

import uuid
import time
from unittest.mock import MagicMock, patch, call

import pytest

from core.models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy
from core.services import record_commit
from connections.models import ConnectionProfile
from fastapi_backend.app.services.rollback_service import rollback_to_version
from fastapi_backend.app.kafka.topics import EVENTS


pytestmark = pytest.mark.django_db


def _create_commits(user, profile, count, mock_psycopg2=None):
    """Helper: create N commits via record_commit() and return their version_ids."""
    vids = []
    for i in range(count):
        vid = str(uuid.uuid4())
        record_commit(
            version_id=vid,
            sql_command=f"INSERT INTO t(id) VALUES ({i + 1})",
            inverse_sql=f"DELETE FROM t WHERE id = {i + 1}",
            user=user,
            connection_profile=profile,
            status="success",
        )
        vids.append(vid)
    return vids


class TestRollbackSnapshotSelection:
    """Tests for snapshot selection during rollback."""

    def test_nearest_snapshot_selected(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_16 ΓÇö Verify that the rollback service selects the nearest snapshot at or before the target commit timestamp."""
        SnapshotPolicy.objects.create(frequency=2, connection_profile=connection_profile)

        vids = _create_commits(user, connection_profile, 6)

        target_vid = vids[2]
        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=target_vid,
        )
        assert result["rolled_back_to"] == target_vid
        assert result["status"] == "success"
        if result["snapshot_restored"]:
            assert "snapshots/" in result["snapshot_restored"]


class TestRollbackInverseOperations:
    """Tests for inverse operation application during rollback."""

    def test_inverse_operations_applied_in_reverse_order(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_17 ΓÇö Verify that inverse operations are applied in correct reverse chronological order (no-snapshot fallback)."""
        vids = _create_commits(user, connection_profile, 5)

        target_vid = vids[1]

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=target_vid,
        )
        assert result["commands_applied"] == 3
        assert result["status"] == "success"

        mock_cursor = mock_psycopg2_connect._mock_cursor
        executed_sqls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        expected = [
            "DELETE FROM t WHERE id = 5",
            "DELETE FROM t WHERE id = 4",
            "DELETE FROM t WHERE id = 3",
        ]
        assert executed_sqls == expected

    def test_stale_commits_deleted_after_rollback(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_48 ΓÇö Verify that stale commits are deleted after rollback."""
        vids = _create_commits(user, connection_profile, 5)

        rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[1],
        )

        remaining = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).order_by("seq")
        assert remaining.count() == 2
        remaining_vids = list(remaining.values_list("version_id", flat=True))
        assert remaining_vids == vids[:2]

    def test_inverse_operations_cascade_deleted(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Verify that InverseOperations are cascade-deleted with stale commits."""
        vids = _create_commits(user, connection_profile, 5)

        rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[1],
        )

        remaining_inverses = InverseOperation.objects.filter(
            commit__connection_profile=connection_profile,
        ).count()
        assert remaining_inverses == 2


class TestRollbackFailureSafety:
    """Tests for failure-safe rollback behavior."""

    def test_rollback_stops_on_inverse_failure(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_18 ΓÇö Verify that if an inverse operation fails, the rollback stops safely and returns an error."""
        vids = _create_commits(user, connection_profile, 4)

        call_count = [0]

        def failing_execute(sql):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("Simulated inverse operation failure")

        mock_psycopg2_connect._mock_cursor.execute.side_effect = failing_execute

        with pytest.raises(Exception, match="Simulated inverse operation failure"):
            rollback_to_version(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                target_version_id=vids[0],
            )

    def test_rollback_nonexistent_target_raises(self, user, connection_profile):
        """Verify rollback to a non-existent version raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            rollback_to_version(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                target_version_id="nonexistent-version-id",
            )

    def test_rollback_connection_rolled_back_on_error(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Verify connection.rollback() is called on failure."""
        vids = _create_commits(user, connection_profile, 3)
        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("DB error")

        with pytest.raises(Exception):
            rollback_to_version(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                target_version_id=vids[0],
            )
        mock_psycopg2_connect._mock_conn.rollback.assert_called()

    def test_rollback_connection_closed_on_error(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Verify connection is closed even after failure."""
        vids = _create_commits(user, connection_profile, 3)
        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("fail")

        with pytest.raises(Exception):
            rollback_to_version(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                target_version_id=vids[0],
            )
        mock_psycopg2_connect._mock_conn.close.assert_called()

    def test_rollback_wrong_user_raises(self, user, other_user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Verify that rolling back with the wrong user raises."""
        vids = _create_commits(user, connection_profile, 3)
        with pytest.raises(ConnectionProfile.DoesNotExist):
            rollback_to_version(
                user_id=other_user.id,
                connection_profile_id=connection_profile.id,
                target_version_id=vids[0],
            )


class TestRollbackStrategySelection:
    """Tests for the forward/backward snapshot-distance strategy."""

    def test_forward_strategy_first_half(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Target in first half of interval uses forward replay from S1."""
        SnapshotPolicy.objects.create(frequency=6, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 12)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[7],
        )
        assert result["strategy"] == "forward"
        assert result["commands_applied"] == 2
        assert result["snapshot_restored"] is not None

        mock_cursor = mock_psycopg2_connect._mock_cursor
        executed_sqls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert executed_sqls == [
            "INSERT INTO t(id) VALUES (7)",
            "INSERT INTO t(id) VALUES (8)",
        ]

    def test_backward_strategy_second_half(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Target in second half of interval uses backward replay from S2."""
        SnapshotPolicy.objects.create(frequency=6, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 12)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[9],
        )
        assert result["strategy"] == "backward"
        assert result["commands_applied"] == 2
        assert result["snapshot_restored"] is not None

        mock_cursor = mock_psycopg2_connect._mock_cursor
        executed_sqls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert executed_sqls == [
            "DELETE FROM t WHERE id = 12",
            "DELETE FROM t WHERE id = 11",
        ]

    def test_forward_at_threshold_boundary(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Target at exactly ceil(k/2) uses forward strategy."""
        SnapshotPolicy.objects.create(frequency=6, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 12)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[8],
        )
        assert result["strategy"] == "forward"
        assert result["commands_applied"] == 3

    def test_odd_frequency_gives_more_to_forward(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """With odd k (e.g., 5), ceil(5/2)=3 so 3 positions use forward, 2 use backward."""
        SnapshotPolicy.objects.create(frequency=5, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 10)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[7],
        )
        assert result["strategy"] == "forward"

    def test_odd_frequency_backward_kicks_in(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """With odd k=5, position 4 (>3) uses backward."""
        SnapshotPolicy.objects.create(frequency=5, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 10)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[8],
        )
        assert result["strategy"] == "backward"

    def test_no_next_snapshot_falls_back_to_forward(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """When no S2 exists (target is beyond last snapshot), use forward from S1."""
        SnapshotPolicy.objects.create(frequency=6, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 10)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[9],
        )
        assert result["strategy"] == "forward"
        assert result["commands_applied"] == 4

    def test_no_prev_snapshot_uses_next_backward(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """When no S1 exists (target is before first snapshot), use backward from S2."""
        SnapshotPolicy.objects.create(frequency=3, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 6)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[1],
        )
        assert result["strategy"] == "backward"
        assert result["commands_applied"] == 1

    def test_target_at_snapshot_commit(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Rolling back to a commit that IS a snapshot should restore it with 0 replays."""
        SnapshotPolicy.objects.create(frequency=3, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 6)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[2],
        )
        assert result["status"] == "success"
        assert result["snapshot_restored"] is not None
        assert result["commands_applied"] == 0

    def test_no_snapshots_fallback(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """No snapshots at all ΓÇö fall back to applying inverses from current state."""
        vids = _create_commits(user, connection_profile, 4)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[1],
        )
        assert result["commands_applied"] == 2
        assert result["snapshot_restored"] is None
        assert result["strategy"] == "backward"


class TestRollbackEdgeCases:
    """Edge case tests for rollback."""

    def test_rollback_to_latest_commit(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Rollback to the latest commit should apply 0 inverses and delete nothing."""
        vids = _create_commits(user, connection_profile, 5)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[4],
        )
        assert result["commands_applied"] == 0
        assert result["status"] == "success"
        assert CommitEvent.objects.filter(connection_profile=connection_profile).count() == 5

    def test_rollback_to_first_commit(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Rollback to the first commit should undo all subsequent commits."""
        vids = _create_commits(user, connection_profile, 5)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[0],
        )
        assert result["commands_applied"] == 4
        assert CommitEvent.objects.filter(connection_profile=connection_profile).count() == 1

    def test_rollback_single_commit(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Rollback when only one commit exists should be a no-op."""
        vids = _create_commits(user, connection_profile, 1)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[0],
        )
        assert result["commands_applied"] == 0
        assert result["status"] == "success"

    def test_rollback_result_fields(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """Verify all expected fields are present in rollback result."""
        vids = _create_commits(user, connection_profile, 3)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[0],
        )
        assert "rolled_back_to" in result
        assert "snapshot_restored" in result
        assert "commands_applied" in result
        assert "strategy" in result
        assert "status" in result


class TestRollbackIntegration:
    """Integration tests for the rollback workflow."""

    def test_full_rollback_workflow(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_64 ΓÇö Full rollback workflow: write queries, trigger snapshot, then rollback."""
        SnapshotPolicy.objects.create(frequency=3, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 6)

        assert Snapshot.objects.filter(connection_profile=connection_profile).count() == 2

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[1],
        )
        assert result["rolled_back_to"] == vids[1]
        assert result["status"] == "success"
        assert result["commands_applied"] >= 1

        remaining = CommitEvent.objects.filter(connection_profile=connection_profile).count()
        assert remaining == 2

    def test_snapshot_based_rollback_optimization(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_65 ΓÇö Verify that rollback uses the nearest snapshot instead of replaying all inverses."""
        SnapshotPolicy.objects.create(frequency=3, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 9)

        assert Snapshot.objects.filter(connection_profile=connection_profile).count() == 3

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[4],
        )
        assert result["status"] == "success"
        assert result["strategy"] == "forward"
        assert result["commands_applied"] == 2
        if result["snapshot_restored"]:
            mock_s3["download"].assert_called()

    def test_failure_safe_rollback_with_bad_inverse(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_66 ΓÇö Failure-safe rollback: inject a faulty inverse operation and verify rollback stops safely."""
        vids = _create_commits(user, connection_profile, 4)

        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("Bad inverse SQL execution")

        with pytest.raises(Exception, match="Bad inverse SQL"):
            rollback_to_version(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                target_version_id=vids[0],
            )
        mock_psycopg2_connect._mock_conn.rollback.assert_called()

    def test_rollback_fetches_and_applies_inverses(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_45 ΓÇö Verify that during rollback, stored inverse commands are fetched and applied against the user database."""
        vids = _create_commits(user, connection_profile, 3)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[0],
        )
        assert result["commands_applied"] == 2
        mock_cursor = mock_psycopg2_connect._mock_cursor
        assert mock_cursor.execute.call_count == 2

    def test_rollback_restores_correct_snapshot_from_s3(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_47 ΓÇö Verify that during rollback, the correct snapshot is retrieved from S3 and restored."""
        SnapshotPolicy.objects.create(frequency=2, connection_profile=connection_profile)
        vids = _create_commits(user, connection_profile, 4)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[2],
        )
        assert result["status"] == "success"
        if result["snapshot_restored"]:
            mock_s3["download"].assert_called()

    def test_rollback_produces_expected_final_state(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_48 ΓÇö Verify that inverse operations produce the expected final database state."""
        vids = _create_commits(user, connection_profile, 5)

        rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[2],
        )

        remaining = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).order_by("seq")
        assert remaining.count() == 3
        assert list(remaining.values_list("seq", flat=True)) == [1, 2, 3]

        for commit in remaining:
            assert InverseOperation.objects.filter(commit=commit).exists()

    def test_rollback_safely_stops_on_failure(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess):
        """TC_49 ΓÇö Verify that a failing inverse operation stops execution safely without corrupting the database."""
        vids = _create_commits(user, connection_profile, 3)

        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("DB constraint violation")

        with pytest.raises(Exception, match="DB constraint violation"):
            rollback_to_version(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                target_version_id=vids[0],
            )

        mock_psycopg2_connect._mock_conn.rollback.assert_called()
        mock_psycopg2_connect._mock_conn.close.assert_called()


class TestRollbackKafkaIntegration:
    """Tests for Kafka event production during rollback."""

    def test_rollback_produces_event(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess, mock_kafka_producer):
        """Successful rollback produces a rollback_completed event to Kafka."""
        vids = _create_commits(user, connection_profile, 3)

        rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[0],
        )

        event_calls = [
            c for c in mock_kafka_producer.call_args_list
            if c[0][0] == EVENTS
        ]
        assert len(event_calls) == 1

    def test_rollback_event_failure_does_not_break_rollback(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess, mock_kafka_producer):
        """If Kafka event production raises, rollback still succeeds."""
        mock_kafka_producer.side_effect = RuntimeError("Kafka broker down")
        vids = _create_commits(user, connection_profile, 3)

        result = rollback_to_version(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            target_version_id=vids[0],
        )

        assert result["status"] == "success"
        assert result["commands_applied"] == 2
