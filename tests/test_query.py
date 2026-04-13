"""Tests for query service ΓÇö read-only SQL execution.

Covers: TC_03, TC_24, TC_25, TC_26, TC_27
Plus edge cases: case sensitivity, CTEs, connection errors, rowcount,
profile ownership, connection cleanup.
"""

from unittest.mock import MagicMock, patch

import pytest

from core.models import CommitEvent
from connections.models import ConnectionProfile
from fastapi_backend.app.services.query_service import _validate_read_only, execute_read_sql


pytestmark = pytest.mark.django_db


class TestValidateReadOnly:
    """Tests for the _validate_read_only() validator."""

    def test_select_allowed(self):
        """Verify SELECT statements pass validation."""
        _validate_read_only("SELECT * FROM users")

    def test_show_allowed(self):
        """Verify SHOW statements pass validation."""
        _validate_read_only("SHOW tables")

    def test_explain_allowed(self):
        """Verify EXPLAIN statements pass validation."""
        _validate_read_only("EXPLAIN SELECT * FROM users")

    def test_insert_rejected(self):
        """Verify INSERT statements are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("INSERT INTO t(id) VALUES (1)")

    def test_update_rejected(self):
        """Verify UPDATE statements are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("UPDATE t SET x = 1")

    def test_delete_rejected(self):
        """Verify DELETE statements are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("DELETE FROM t WHERE id = 1")

    def test_drop_rejected(self):
        """Verify DROP statements are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("DROP TABLE t")

    def test_empty_sql_rejected(self):
        """Verify empty SQL is rejected."""
        with pytest.raises(ValueError):
            _validate_read_only("")

    def test_whitespace_only_rejected(self):
        """Verify whitespace-only SQL is rejected."""
        with pytest.raises(ValueError):
            _validate_read_only("   ")

    def test_multiple_statements_rejected(self):
        """Verify multiple statements (semicolon-separated) are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("SELECT 1; SELECT 2")

    def test_trailing_semicolon_allowed(self):
        """Verify a single trailing semicolon is allowed."""
        _validate_read_only("SELECT * FROM users;")

    def test_case_insensitive_select(self):
        """Verify that mixed-case SELECT is accepted."""
        _validate_read_only("select * from users")
        _validate_read_only("Select * FROM users")
        _validate_read_only("SELECT * FROM users")

    def test_case_insensitive_reject(self):
        """Verify that mixed-case write keywords are still rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("insert into t values (1)")
        with pytest.raises(PermissionError):
            _validate_read_only("Delete from t")

    def test_alter_rejected(self):
        """Verify ALTER statements are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("ALTER TABLE t ADD COLUMN x INT")

    def test_create_rejected(self):
        """Verify CREATE statements are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("CREATE TABLE t (id INT)")

    def test_truncate_rejected(self):
        """Verify TRUNCATE statements are rejected."""
        with pytest.raises(PermissionError):
            _validate_read_only("TRUNCATE TABLE t")

    def test_leading_whitespace_handled(self):
        """Verify SQL with leading whitespace is validated correctly."""
        _validate_read_only("   SELECT * FROM users")

    def test_semicolon_in_middle_rejected(self):
        """Verify semicolon in the middle of SQL is rejected (injection attempt)."""
        with pytest.raises(PermissionError):
            _validate_read_only("SELECT 1; DROP TABLE t")

    def test_explain_analyze_allowed(self):
        """Verify EXPLAIN ANALYZE is allowed."""
        _validate_read_only("EXPLAIN ANALYZE SELECT * FROM users")


class TestExecuteReadSQL:
    """Tests for execute_read_sql() with mocked psycopg2."""

    def test_select_returns_columns_and_rows(self, user, connection_profile, mock_psycopg2_connect):
        """TC_24 ΓÇö Verify that a SELECT query returns tabular results (columns and rows)."""
        result = execute_read_sql(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql="SELECT * FROM users",
        )
        assert result["status"] == "success"
        assert result["columns"] == ["id", "name"]
        assert result["rows"] == [[1, "Alice"], [2, "Bob"]]
        assert result["rowcount"] == 2

    def test_select_does_not_create_commit(self, user, connection_profile, mock_psycopg2_connect):
        """TC_03 ΓÇö Verify that executing a SELECT query does not create any commit entry."""
        initial_count = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count()
        execute_read_sql(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql="SELECT * FROM users",
        )
        final_count = CommitEvent.objects.filter(
            connection_profile=connection_profile
        ).count()
        assert final_count == initial_count

    def test_insert_via_query_rejected(self, user, connection_profile):
        """TC_25 ΓÇö Verify that a write query through the read endpoint is rejected."""
        with pytest.raises(PermissionError):
            execute_read_sql(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql="INSERT INTO t(id) VALUES (1)",
            )

    def test_invalid_sql_returns_error(self, user, connection_profile, mock_psycopg2_connect):
        """TC_26 ΓÇö Verify that an invalid/malformed SQL query returns an error message."""
        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("syntax error")
        with pytest.raises(Exception, match="syntax error"):
            execute_read_sql(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql="SELECT *** FROM bad_syntax",
            )

    def test_multiple_valid_queries_no_crash(self, user, connection_profile, mock_psycopg2_connect):
        """TC_27 ΓÇö Verify that executing valid queries does not crash the system."""
        for i in range(5):
            result = execute_read_sql(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql=f"SELECT {i}",
            )
            assert result["status"] == "success"

    def test_query_with_no_description(self, user, connection_profile, mock_psycopg2_connect):
        """Verify handling of queries that return no cursor description."""
        mock_psycopg2_connect._mock_cursor.description = None
        mock_psycopg2_connect._mock_cursor.rowcount = 0
        result = execute_read_sql(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql="SHOW server_version",
        )
        assert result["columns"] == []
        assert result["rows"] == []

    def test_connection_closed_after_success(self, user, connection_profile, mock_psycopg2_connect):
        """Verify the connection is always closed after a successful query."""
        execute_read_sql(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql="SELECT 1",
        )
        mock_psycopg2_connect._mock_conn.close.assert_called_once()

    def test_connection_closed_after_failure(self, user, connection_profile, mock_psycopg2_connect):
        """Verify the connection is closed even after an execution failure."""
        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("DB error")
        with pytest.raises(Exception, match="DB error"):
            execute_read_sql(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql="SELECT 1",
            )
        mock_psycopg2_connect._mock_conn.close.assert_called_once()

    def test_rollback_called_on_failure(self, user, connection_profile, mock_psycopg2_connect):
        """Verify rollback is called when an exception occurs during execution."""
        mock_psycopg2_connect._mock_cursor.execute.side_effect = Exception("timeout")
        with pytest.raises(Exception):
            execute_read_sql(
                user_id=user.id,
                connection_profile_id=connection_profile.id,
                sql="SELECT 1",
            )
        mock_psycopg2_connect._mock_conn.rollback.assert_called_once()

    def test_wrong_profile_raises(self, user, other_profile):
        """Verify that accessing another user's profile raises DoesNotExist."""
        with pytest.raises(ConnectionProfile.DoesNotExist):
            execute_read_sql(
                user_id=user.id,
                connection_profile_id=other_profile.id,
                sql="SELECT 1",
            )

    def test_empty_result_set(self, user, connection_profile, mock_psycopg2_connect):
        """Verify handling of queries returning zero rows."""
        mock_psycopg2_connect._mock_cursor.description = [("id",)]
        mock_psycopg2_connect._mock_cursor.fetchall.return_value = []
        mock_psycopg2_connect._mock_cursor.rowcount = 0
        result = execute_read_sql(
            user_id=user.id,
            connection_profile_id=connection_profile.id,
            sql="SELECT * FROM empty_table",
        )
        assert result["columns"] == ["id"]
        assert result["rows"] == []
        assert result["rowcount"] == 0
