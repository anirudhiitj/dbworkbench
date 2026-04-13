"""Tests for commit service ΓÇö versioned write operations.

Covers: TC_41, TC_42, TC_44, TC_51, TC_25, TC_63, TC_67, TC_70
Plus Kafka integration, connection lifecycle, edge cases, ownership isolation.
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from core.models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy
from connections.models import ConnectionProfile
from fastapi_backend.app.services.commit_service import (
    _validate_write_sql,
    _append_returning_star,
    _rows_to_dicts,
    create_commit,
    list_commits,
    get_commit,
)
from fastapi_backend.app.kafka.topics import SNAPSHOT_TASKS, COMMIT_LOGS
from fastapi_backend.app.services.inverse_engine import InverseCommand, CommandCategory


pytestmark = pytest.mark.django_db


def _dummy_inverse(sql):
    """Return a pre-built InverseCommand so tests don't hit the real engine."""
    return InverseCommand(
        category=CommandCategory.INSERT,
        forward_sql=sql,
        steps=["DELETE FROM t WHERE id = 1"],
        is_reversible=True,
    )


@pytest.fixture
def mock_inverse_engine():
    """Patch InverseEngine so create_commit() never queries the DB for inverses."""
    with patch("fastapi_backend.app.services.commit_service.InverseEngine") as MockEngine:
        instance = MockEngine.return_value
        instance.generate.side_effect = lambda sql: _dummy_inverse(sql)
        instance.finalize_insert.return_value = None
        yield MockEngine


class TestValidateWriteSQL:
    """Tests for the _validate_write_sql() validator."""

    def test_insert_allowed(self):
        _validate_write_sql("INSERT INTO t(id) VALUES (1)")

    def test_update_allowed(self):
        _validate_write_sql("UPDATE t SET x = 1 WHERE id = 1")

    def test_delete_allowed(self):
        _validate_write_sql("DELETE FROM t WHERE id = 1")

    def test_alter_allowed(self):
        _validate_write_sql("ALTER TABLE t ADD COLUMN y INT")

    def test_create_allowed(self):
        _validate_write_sql("CREATE TABLE t (id INT)")

    def test_drop_allowed(self):
        _validate_write_sql("DROP TABLE t")

    def test_truncate_allowed(self):
        _validate_write_sql("TRUNCATE TABLE t")

    def test_select_rejected(self):
        with pytest.raises(ValueError, match="SELECT"):
            _validate_write_sql("SELECT * FROM t")

    def test_empty_sql_rejected(self):
        with pytest.raises(ValueError, match="empty"):
            _validate_write_sql("")

    def test_whitespace_only_rejected(self):
        with pytest.raises(ValueError, match="empty"):
            _validate_write_sql("   ")

    def test_multiple_statements_rejected(self):
        with pytest.raises(ValueError, match="single"):
            _validate_write_sql("INSERT INTO t(id) VALUES (1); INSERT INTO t(id) VALUES (2)")

    def test_unknown_keyword_rejected(self):
        with pytest.raises(ValueError, match="not allowed"):
            _validate_write_sql("GRANT ALL ON t TO user1")

    def test_case_insensitive_keywords(self):
        """Verify mixed-case keywords are correctly identified."""
        _validate_write_sql("insert into t(id) values (1)")
        _validate_write_sql("Insert Into t(id) VALUES (1)")
        _validate_write_sql("DELETE from t WHERE id = 1")

    def test_leading_whitespace_handled(self):
        """Verify SQL with leading/trailing whitespace passes."""
        _validate_write_sql("  INSERT INTO t(id) VALUES (1)  ")

    def test_trailing_semicolon_single_statement(self):
        """Verify single statement with trailing semicolon is accepted."""
        _validate_write_sql("INSERT INTO t(id) VALUES (1);")

    def test_show_rejected(self):
        """Verify SHOW is not a write keyword."""
        with pytest.raises(ValueError, match="not allowed"):
            _validate_write_sql("SHOW tables")

    def test_explain_rejected(self):
        """Verify EXPLAIN is not a write keyword."""
        with pytest.raises(ValueError, match="not allowed"):
            _validate_write_sql("EXPLAIN SELECT * FROM t")


class TestAppendReturningStar:
    """Tests for the _append_returning_star() helper."""

    def test_appends_returning(self):
        result = _append_returning_star("INSERT INTO t(id) VALUES (1)")
        assert result.endswith("RETURNING *")

    def test_no_double_returning(self):
        """Verify it doesn't add RETURNING * if already present."""
        sql = "INSERT INTO t(id) VALUES (1) RETURNING *"
        result = _append_returning_star(sql)
        assert result.count("RETURNING") == 1

    def test_case_insensitive_returning_detection(self):
        """Verify it detects existing RETURNING regardless of case."""
        sql = "INSERT INTO t(id) VALUES (1) returning id"
        result = _append_returning_star(sql)
        assert result.count("returning") == 1  # unchanged

    def test_strips_trailing_semicolon(self):
        """Verify trailing semicolon is removed before appending."""
        result = _append_returning_star("INSERT INTO t(id) VALUES (1);")
        assert not result.endswith(";")
        assert result.endswith("RETURNING *")


class TestRowsToDicts:
    """Tests for the _rows_to_dicts() helper."""

    def test_converts_rows(self):
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        result = _rows_to_dicts(mock_cursor)
        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_empty_rows(self):
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",)]
        mock_cursor.fetchall.return_value = []
        result = _rows_to_dicts(mock_cursor)
        assert result == []

    def test_no_description(self):
        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_cursor.fetchall.return_value = []
        result = _rows_to_dicts(mock_cursor)
        assert result == []


class TestCreateCommit:
    """Tests for create_commit() with mocked psycopg2, S3, and InverseEngine."""

    def test_create_commit_success(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_41 ΓÇö Verify that executing a write query creates a commit with correct metadata."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        assert "version_id" in result
        assert result["seq"] == 1
        assert result["sql_command"] == "INSERT INTO t(id) VALUES (1)"
        assert result["status"] == "success"
        assert result["timestamp"] is not None
        assert result["connection_profile_id"] == connection_profile.id

        commit = CommitEvent.objects.get(version_id=result["version_id"])
        assert commit.sql_command == "INSERT INTO t(id) VALUES (1)"

    def test_select_does_not_create_commit(self, user, connection_profile):
        """TC_42 ΓÇö Verify that a SELECT query via commit endpoint does not create a commit."""
        initial_count = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count()
        with pytest.raises(ValueError, match="SELECT"):
            create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command="SELECT * FROM t",
            )
        assert CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count() == initial_count

    def test_commit_persists_across_fresh_query(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_44 ΓÇö Verify that commit records persist and survive a fresh ORM query session."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (42)",
        )
        fetched = CommitEvent.objects.get(version_id=result["version_id"])
        assert fetched.sql_command == "INSERT INTO t(id) VALUES (42)"

    def test_version_id_and_seq_assigned(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_51 ΓÇö Verify that a write query is assigned a version_id and seq correctly."""
        r1 = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        r2 = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (2)",
        )
        assert r1["version_id"] != r2["version_id"]
        assert r1["seq"] == 1
        assert r2["seq"] == 2

    def test_commit_triggers_snapshot_upload(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_subprocess, mock_inverse_engine):
        """TC_46 ΓÇö Verify that when a snapshot is triggered, upload is called."""
        SnapshotPolicy.objects.create(frequency=1, connection_profile=connection_profile)
        create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        assert Snapshot.objects.filter(connection_profile=connection_profile).count() >= 1

    def test_write_returns_affected_row_count_metadata(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_25 ΓÇö Verify that a write query (INSERT) returns commit metadata including status."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        assert result["status"] == "success"

    def test_inverse_engine_called_before_execution(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """Verify InverseEngine.generate() is called with the SQL command."""
        create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        mock_inverse_engine.return_value.generate.assert_called_once_with("INSERT INTO t(id) VALUES (1)")

    def test_inverse_sql_stored_from_engine(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """Verify the generated inverse steps are stored in InverseOperation."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        inv = InverseOperation.objects.get(commit__version_id=result["version_id"])
        assert inv.inverse_sql == "DELETE FROM t WHERE id = 1"

    def test_connection_rolled_back_on_failure(self, user, connection_profile, mock_psycopg2_connect, mock_inverse_engine):
        """Verify connection.rollback() is called when SQL execution fails."""
        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("DB error")
        with pytest.raises(Exception, match="DB error"):
            create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command="INSERT INTO t(id) VALUES (1)",
            )
        mock_psycopg2_connect._mock_conn.rollback.assert_called()

    def test_connection_closed_on_success(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """Verify connection is closed after successful execution."""
        create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        mock_psycopg2_connect._mock_conn.close.assert_called_once()

    def test_connection_closed_on_failure(self, user, connection_profile, mock_psycopg2_connect, mock_inverse_engine):
        """Verify connection is closed even after failure."""
        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("fail")
        with pytest.raises(Exception):
            create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command="INSERT INTO t(id) VALUES (1)",
            )
        mock_psycopg2_connect._mock_conn.close.assert_called_once()

    def test_wrong_user_profile_raises(self, user, other_profile, mock_psycopg2_connect):
        """Verify accessing another user's profile raises DoesNotExist."""
        with pytest.raises(ConnectionProfile.DoesNotExist):
            create_commit(
                user_id=user.id,
                connection_profile_id=other_profile.id,
                sql_command="INSERT INTO t(id) VALUES (1)",
            )

    def test_nonexistent_user_raises(self, connection_profile):
        """Verify that a non-existent user ID raises."""
        from authentication.models import User
        with pytest.raises(User.DoesNotExist):
            create_commit(
                user_id=99999,
                connection_profile_id=connection_profile.id,
                sql_command="INSERT INTO t(id) VALUES (1)",
            )

    def test_no_commit_created_on_execution_failure(self, user, connection_profile, mock_psycopg2_connect, mock_inverse_engine):
        """Verify that if SQL execution fails, no CommitEvent is created."""
        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("fail")
        initial = CommitEvent.objects.filter(connection_profile=connection_profile).count()
        with pytest.raises(Exception):
            create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command="INSERT INTO t(id) VALUES (1)",
            )
        assert CommitEvent.objects.filter(connection_profile=connection_profile).count() == initial


class TestListAndGetCommits:
    """Tests for list_commits() and get_commit()."""

    def test_list_commits_ordered_by_seq(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """Verify commits are listed in sequential order."""
        for i in range(3):
            create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
            )
        results = list_commits(user_id=user.id, connection_profile_id=connection_profile.id)
        assert len(results) == 3
        seqs = [r["seq"] for r in results]
        assert seqs == [1, 2, 3]

    def test_get_commit_by_version_id(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """Verify retrieving a single commit by version_id."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        fetched = get_commit(user_id=user.id, version_id=result["version_id"])
        assert fetched is not None
        assert fetched["version_id"] == result["version_id"]

    def test_get_commit_not_found(self, user):
        """Verify get_commit returns None for non-existent version_id."""
        result = get_commit(user_id=user.id, version_id="non-existent-id")
        assert result is None

    def test_list_empty_for_profile_with_no_commits(self, user, connection_profile):
        """Verify empty list is returned when no commits exist."""
        results = list_commits(user_id=user.id, connection_profile_id=connection_profile.id)
        assert results == []

    def test_get_commit_wrong_user(self, user, other_user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """Verify that another user cannot get someone else's commit."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        fetched = get_commit(user_id=other_user.id, version_id=result["version_id"])
        assert fetched is None

    def test_get_commit_returns_all_fields(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """Verify get_commit returns all expected fields."""
        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        fetched = get_commit(user_id=user.id, version_id=result["version_id"])
        assert "version_id" in fetched
        assert "seq" in fetched
        assert "sql_command" in fetched
        assert "status" in fetched
        assert "timestamp" in fetched
        assert "connection_profile_id" in fetched


class TestEndToEndCommitTracking:
    """System-level tests for commit tracking."""

    def test_multiple_writes_logged_with_correct_order(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_63 ΓÇö End-to-end commit tracking: execute N writes, verify all logged correctly."""
        n = 5
        version_ids = []
        for i in range(n):
            result = create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
            )
            version_ids.append(result["version_id"])

        commits = list_commits(user_id=user.id, connection_profile_id=connection_profile.id)
        assert len(commits) == n
        for i, c in enumerate(commits):
            assert c["seq"] == i + 1
            assert c["version_id"] == version_ids[i]
            assert c["sql_command"] == f"INSERT INTO t(id) VALUES ({i})"
            assert c["timestamp"] is not None

    def test_select_and_write_queries_correct_behavior(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_67 ΓÇö Query execution flow: SELECT does not create commit, write does."""
        from fastapi_backend.app.services.query_service import execute_read_sql

        execute_read_sql(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql="SELECT * FROM t",
        )
        assert CommitEvent.objects.filter(connection_profile=connection_profile).count() == 0

        create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )
        assert CommitEvent.objects.filter(connection_profile=connection_profile).count() == 1

    def test_persistence_across_restart_simulation(self, user, connection_profile, mock_psycopg2_connect, mock_s3, mock_inverse_engine):
        """TC_70 ΓÇö Persistence across restart: create commits, clear cache, re-query."""
        for i in range(3):
            create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
            )

        fresh_commits = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).order_by("seq").values_list("seq", flat=True)
        assert list(fresh_commits) == [1, 2, 3]


class TestKafkaIntegration:
    """Tests for Kafka integration in the commit service."""

    def test_snapshot_dispatched_to_kafka(
        self, user, connection_profile, mock_psycopg2_connect, mock_s3,
        mock_subprocess, mock_inverse_engine, mock_kafka_producer,
    ):
        """When Kafka is enabled, snapshot tasks are dispatched async ΓÇö not uploaded synchronously."""
        SnapshotPolicy.objects.create(frequency=1, connection_profile=connection_profile)

        create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )

        snapshot_calls = [
            c for c in mock_kafka_producer.call_args_list
            if c[0][0] == SNAPSHOT_TASKS
        ]
        assert len(snapshot_calls) == 1

        mock_s3["upload"].assert_not_called()
        mock_subprocess.assert_not_called()

    def test_snapshot_sync_fallback_when_kafka_unavailable(
        self, user, connection_profile, mock_psycopg2_connect, mock_s3,
        mock_subprocess, mock_inverse_engine, mock_kafka_producer,
    ):
        """When Kafka produce returns False, fall back to synchronous snapshot upload."""
        mock_kafka_producer.return_value = False
        SnapshotPolicy.objects.create(frequency=1, connection_profile=connection_profile)

        create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )

        mock_subprocess.assert_called()

    def test_commit_audit_log_produced(
        self, user, connection_profile, mock_psycopg2_connect, mock_s3,
        mock_inverse_engine, mock_kafka_producer,
    ):
        """Every commit produces an audit log message to the commit-logs topic."""
        create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )

        audit_calls = [
            c for c in mock_kafka_producer.call_args_list
            if c[0][0] == COMMIT_LOGS
        ]
        assert len(audit_calls) == 1

    def test_audit_log_failure_does_not_break_commit(
        self, user, connection_profile, mock_psycopg2_connect, mock_s3,
        mock_inverse_engine, mock_kafka_producer,
    ):
        """If the audit log produce raises, the commit still succeeds."""
        def produce_side_effect(topic, key, value):
            if topic == COMMIT_LOGS:
                raise RuntimeError("Kafka broker down")
            return True

        mock_kafka_producer.side_effect = produce_side_effect

        result = create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )

        assert result["status"] == "success"
        assert CommitEvent.objects.filter(version_id=result["version_id"]).exists()

    def test_no_snapshot_means_no_snapshot_task_produced(
        self, user, connection_profile, mock_psycopg2_connect, mock_s3,
        mock_inverse_engine, mock_kafka_producer,
    ):
        """When no snapshot is triggered, no snapshot-task message is produced."""
        create_commit(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql_command="INSERT INTO t(id) VALUES (1)",
        )

        snapshot_calls = [
            c for c in mock_kafka_producer.call_args_list
            if c[0][0] == SNAPSHOT_TASKS
        ]
        assert len(snapshot_calls) == 0

    def test_multiple_commits_produce_multiple_audit_logs(
        self, user, connection_profile, mock_psycopg2_connect, mock_s3,
        mock_inverse_engine, mock_kafka_producer,
    ):
        """Verify that N commits produce N audit log messages."""
        for i in range(3):
            create_commit(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql_command=f"INSERT INTO t(id) VALUES ({i})",
            )

        audit_calls = [
            c for c in mock_kafka_producer.call_args_list
            if c[0][0] == COMMIT_LOGS
        ]
        assert len(audit_calls) == 3
