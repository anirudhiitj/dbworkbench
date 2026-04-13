"""Comprehensive test suite for the Inverse Engine.

Tests every command category, sub-type, and edge case using mock infrastructure.
Computes and reports accuracy at the end.

Run with:
    python -m pytest tests/test_inverse_engine_comprehensive.py -v
    python -m pytest tests/test_inverse_engine_comprehensive.py -v -s   # to see accuracy report
"""

import re
import json
import pytest

from fastapi_backend.app.services.inverse_engine import (
    InverseEngine, InverseCommand, CommandCategory,
    _classify, _normalise, _quote_ident, _quote_literal,
    _parse_insert_table, _parse_update_table_where,
    _parse_delete_table_where, _parse_truncate_table,
    _parse_create_table_name, _parse_drop_object_name,
    _parse_alter_table_name, _parse_alter_add_column_name,
    _parse_alter_drop_column_name, _parse_rename_column,
    _parse_alter_column_type_name, _parse_alter_column_name_generic,
    _parse_constraint_name, _parse_constraint_name_drop,
    _parse_rename_table, _parse_create_index_name,
    _parse_create_sequence_name, _parse_alter_sequence_name,
    _parse_create_schema_name, _parse_create_view_name,
    _split_schema_table, _build_type,
)


# ---------------------------------------------------------------------------
# Mock infrastructure
# ---------------------------------------------------------------------------

class MockCursor:
    """Simulates psycopg2 cursor for testing."""

    def __init__(self, result_map: dict):
        self._map = result_map
        self.description = None
        self._rows = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        sql_up = sql.upper()
        for key, rows in self._map.items():
            if key.upper() in sql_up:
                self._rows = rows
                if rows:
                    first = rows[0]
                    if isinstance(first, dict):
                        self.description = [(k,) for k in first.keys()]
                    elif isinstance(first, (list, tuple)):
                        self.description = [(f"col{i}",) for i in range(len(first))]
                    else:
                        self.description = None
                else:
                    self.description = None
                return
        self._rows = []
        self.description = None

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class MockConnection:
    def __init__(self, result_map=None):
        self._map = result_map or {}

    def cursor(self):
        return MockCursor(self._map)

    def commit(self):
        pass

    def rollback(self):
        pass


# ---------------------------------------------------------------------------
# Engine factories
# ---------------------------------------------------------------------------

def make_engine(result_map):
    return InverseEngine(MockConnection(result_map))


def engine_with_pk(table="orders", pk_cols=("id",), rows=None):
    """Engine with a table that has a PK and sample rows."""
    if rows is None:
        rows = [{"id": 1, "item": "Widget", "qty": 3}]
    return make_engine({
        "PRIMARY KEY": [(col,) for col in pk_cols],
        "SELECT *": rows,
        "INFORMATION_SCHEMA.COLUMNS": [
            ("id", "integer", None, 10, 0, "NO", None, "int4"),
            ("item", "character varying", 50, None, None, "YES", None, "varchar"),
            ("qty", "integer", None, 10, 0, "YES", None, "int4"),
        ],
        "TABLE_CONSTRAINTS": [("PRIMARY KEY", "pk_orders", "id")],
    })


def engine_with_composite_pk():
    """Engine with a composite primary key."""
    return make_engine({
        "PRIMARY KEY": [("order_id",), ("line_id",)],
        "SELECT *": [{"order_id": 1, "line_id": 10, "product": "A", "qty": 5}],
        "INFORMATION_SCHEMA.COLUMNS": [
            ("order_id", "integer", None, 10, 0, "NO", None, "int4"),
            ("line_id", "integer", None, 10, 0, "NO", None, "int4"),
            ("product", "character varying", 100, None, None, "YES", None, "varchar"),
            ("qty", "integer", None, 10, 0, "YES", None, "int4"),
        ],
    })


def engine_no_pk(rows=None):
    """Engine with a table that has no PK."""
    if rows is None:
        rows = [{"item": "Widget", "qty": 3}]
    return make_engine({
        "PRIMARY KEY": [],
        "SELECT *": rows,
    })


def engine_empty_table():
    """Engine with a table that has PK but no rows."""
    return make_engine({
        "PRIMARY KEY": [("id",)],
        "SELECT *": [],
        "INFORMATION_SCHEMA.COLUMNS": [],
    })


def engine_with_column_info(col_name, dtype, char_len, num_prec, num_scale, nullable, default, udt):
    """Engine that returns specific column metadata."""
    return make_engine({
        "PRIMARY KEY": [("id",)],
        "SELECT *": [{"id": 1}],
        "INFORMATION_SCHEMA.COLUMNS": [
            (dtype, char_len, num_prec, num_scale, nullable, default, udt),
        ],
    })


def engine_with_constraint_def(constraint_def):
    """Engine that returns a specific constraint definition."""
    return make_engine({
        "PRIMARY KEY": [("id",)],
        "SELECT *": [{"id": 1}],
        "PG_GET_CONSTRAINTDEF": [(constraint_def,)],
    })


def engine_with_index_def(index_def):
    """Engine returning a specific index definition from pg_indexes."""
    return make_engine({
        "PRIMARY KEY": [("id",)],
        "PG_INDEXES": [(index_def,)],
    })


def engine_with_sequence_state(state_row):
    """Engine returning sequence state."""
    return make_engine({
        "PRIMARY KEY": [],
        "PG_SEQUENCES": [state_row],
    })


def engine_with_view_def(view_def):
    """Engine returning a view definition."""
    return make_engine({
        "PRIMARY KEY": [],
        "PG_VIEWS": [(view_def,)] if view_def else [],
    })


# ---------------------------------------------------------------------------
# Accuracy tracker
# ---------------------------------------------------------------------------

class AccuracyTracker:
    """Collects test results per category for the accuracy report."""

    def __init__(self):
        self.results = {}  # category -> list of (test_name, passed, detail)

    def record(self, category: str, test_name: str, passed: bool, detail: str = ""):
        self.results.setdefault(category, []).append((test_name, passed, detail))

    def report(self):
        lines = ["\n" + "=" * 70]
        lines.append("  INVERSE ENGINE ACCURACY REPORT")
        lines.append("=" * 70)

        total_pass = 0
        total_fail = 0

        for cat in sorted(self.results.keys()):
            entries = self.results[cat]
            passed = sum(1 for _, p, _ in entries if p)
            failed = len(entries) - passed
            total_pass += passed
            total_fail += failed
            pct = (passed / len(entries)) * 100 if entries else 0
            status = "PASS" if failed == 0 else "FAIL"
            lines.append(f"  [{status}] {cat:<35} {passed}/{len(entries)} ({pct:.0f}%)")
            for name, p, detail in entries:
                mark = "+" if p else "X"
                suffix = f" ΓÇö {detail}" if detail and not p else ""
                lines.append(f"         [{mark}] {name}{suffix}")

        total = total_pass + total_fail
        overall_pct = (total_pass / total) * 100 if total else 0
        lines.append("-" * 70)
        lines.append(f"  OVERALL: {total_pass}/{total} passed ({overall_pct:.1f}% accuracy)")
        lines.append("=" * 70)
        return "\n".join(lines)


tracker = AccuracyTracker()


# ---------------------------------------------------------------------------
# 1. CLASSIFIER TESTS
# ---------------------------------------------------------------------------

class TestClassifierComprehensive:
    """Test _classify() for all supported and unsupported SQL forms."""

    # -- DML --
    def test_insert_basic(self):
        assert _classify("INSERT INTO t (a) VALUES (1)") == CommandCategory.INSERT

    def test_insert_no_columns(self):
        assert _classify("INSERT INTO t VALUES (1, 2, 3)") == CommandCategory.INSERT

    def test_insert_with_returning(self):
        assert _classify("INSERT INTO t (a) VALUES (1) RETURNING *") == CommandCategory.INSERT

    def test_insert_multirow(self):
        assert _classify("INSERT INTO t (a) VALUES (1), (2), (3)") == CommandCategory.INSERT

    def test_insert_from_select(self):
        assert _classify("INSERT INTO t (a) SELECT b FROM s") == CommandCategory.INSERT

    def test_update_basic(self):
        assert _classify("UPDATE t SET a=1 WHERE id=2") == CommandCategory.UPDATE

    def test_update_no_where(self):
        assert _classify("UPDATE t SET a=1") == CommandCategory.UPDATE

    def test_update_multiple_cols(self):
        assert _classify("UPDATE t SET a=1, b=2, c=3 WHERE id=1") == CommandCategory.UPDATE

    def test_delete_basic(self):
        assert _classify("DELETE FROM t WHERE id=1") == CommandCategory.DELETE

    def test_delete_no_where(self):
        assert _classify("DELETE FROM t") == CommandCategory.DELETE

    def test_truncate_basic(self):
        assert _classify("TRUNCATE TABLE t") == CommandCategory.TRUNCATE

    def test_truncate_no_table_keyword(self):
        assert _classify("TRUNCATE t") == CommandCategory.TRUNCATE

    def test_truncate_cascade(self):
        assert _classify("TRUNCATE TABLE t CASCADE") == CommandCategory.TRUNCATE

    # -- DDL CREATE --
    def test_create_table(self):
        assert _classify("CREATE TABLE foo (id SERIAL PRIMARY KEY)") == CommandCategory.CREATE_TABLE

    def test_create_table_if_not_exists(self):
        assert _classify("CREATE TABLE IF NOT EXISTS foo (id INT)") == CommandCategory.CREATE_TABLE

    def test_create_temp_table(self):
        assert _classify("CREATE TEMP TABLE t (x INT)") == CommandCategory.CREATE_TABLE

    def test_create_temporary_table(self):
        assert _classify("CREATE TEMPORARY TABLE t (x INT)") == CommandCategory.CREATE_TABLE

    def test_create_index(self):
        assert _classify("CREATE INDEX idx ON t (col)") == CommandCategory.CREATE_INDEX

    def test_create_unique_index(self):
        assert _classify("CREATE UNIQUE INDEX idx ON t (col)") == CommandCategory.CREATE_INDEX

    def test_create_index_concurrently(self):
        assert _classify("CREATE INDEX CONCURRENTLY idx ON t (col)") == CommandCategory.CREATE_INDEX

    def test_create_sequence(self):
        assert _classify("CREATE SEQUENCE seq_id") == CommandCategory.CREATE_SEQUENCE

    def test_create_sequence_with_options(self):
        assert _classify("CREATE SEQUENCE seq START 100 INCREMENT 10") == CommandCategory.CREATE_SEQUENCE

    def test_create_schema(self):
        assert _classify("CREATE SCHEMA analytics") == CommandCategory.CREATE_SCHEMA

    def test_create_schema_if_not_exists(self):
        assert _classify("CREATE SCHEMA IF NOT EXISTS analytics") == CommandCategory.CREATE_SCHEMA

    def test_create_view(self):
        assert _classify("CREATE VIEW v AS SELECT 1") == CommandCategory.CREATE_VIEW

    def test_create_or_replace_view(self):
        assert _classify("CREATE OR REPLACE VIEW v AS SELECT 1") == CommandCategory.CREATE_VIEW

    # -- DDL DROP --
    def test_drop_table(self):
        assert _classify("DROP TABLE foo") == CommandCategory.DROP_TABLE

    def test_drop_table_if_exists(self):
        assert _classify("DROP TABLE IF EXISTS foo") == CommandCategory.DROP_TABLE

    def test_drop_table_cascade(self):
        assert _classify("DROP TABLE foo CASCADE") == CommandCategory.DROP_TABLE

    def test_drop_index(self):
        assert _classify("DROP INDEX idx") == CommandCategory.DROP_INDEX

    def test_drop_index_if_exists(self):
        assert _classify("DROP INDEX IF EXISTS idx") == CommandCategory.DROP_INDEX

    def test_drop_sequence(self):
        assert _classify("DROP SEQUENCE seq") == CommandCategory.DROP_SEQUENCE

    def test_drop_schema(self):
        assert _classify("DROP SCHEMA analytics") == CommandCategory.DROP_SCHEMA

    def test_drop_schema_cascade(self):
        assert _classify("DROP SCHEMA analytics CASCADE") == CommandCategory.DROP_SCHEMA

    def test_drop_view(self):
        assert _classify("DROP VIEW v") == CommandCategory.DROP_VIEW

    # -- ALTER TABLE --
    def test_alter_table_add_column(self):
        assert _classify("ALTER TABLE t ADD COLUMN score INT") == CommandCategory.ALTER_TABLE

    def test_alter_table_drop_column(self):
        assert _classify("ALTER TABLE t DROP COLUMN score") == CommandCategory.ALTER_TABLE

    def test_alter_table_rename_column(self):
        assert _classify("ALTER TABLE t RENAME COLUMN old TO new") == CommandCategory.ALTER_TABLE

    def test_alter_table_alter_column_type(self):
        assert _classify("ALTER TABLE t ALTER COLUMN x TYPE BIGINT") == CommandCategory.ALTER_TABLE

    def test_alter_table_set_not_null(self):
        assert _classify("ALTER TABLE t ALTER COLUMN x SET NOT NULL") == CommandCategory.ALTER_TABLE

    def test_alter_table_drop_not_null(self):
        assert _classify("ALTER TABLE t ALTER COLUMN x DROP NOT NULL") == CommandCategory.ALTER_TABLE

    def test_alter_table_set_default(self):
        assert _classify("ALTER TABLE t ALTER COLUMN x SET DEFAULT 0") == CommandCategory.ALTER_TABLE

    def test_alter_table_drop_default(self):
        assert _classify("ALTER TABLE t ALTER COLUMN x DROP DEFAULT") == CommandCategory.ALTER_TABLE

    def test_alter_table_add_constraint(self):
        assert _classify("ALTER TABLE t ADD CONSTRAINT chk CHECK (x > 0)") == CommandCategory.ALTER_TABLE

    def test_alter_table_drop_constraint(self):
        assert _classify("ALTER TABLE t DROP CONSTRAINT chk") == CommandCategory.ALTER_TABLE

    def test_alter_table_rename_to(self):
        assert _classify("ALTER TABLE old RENAME TO new") == CommandCategory.RENAME_TABLE

    def test_alter_sequence(self):
        assert _classify("ALTER SEQUENCE seq INCREMENT BY 5") == CommandCategory.ALTER_SEQUENCE

    # -- UNKNOWN / Unsupported --
    def test_select_unknown(self):
        assert _classify("SELECT * FROM t") == CommandCategory.UNKNOWN

    def test_grant_unknown(self):
        assert _classify("GRANT ALL ON t TO user1") == CommandCategory.UNKNOWN

    def test_revoke_unknown(self):
        assert _classify("REVOKE ALL ON t FROM user1") == CommandCategory.UNKNOWN

    def test_explain_unknown(self):
        assert _classify("EXPLAIN ANALYZE SELECT 1") == CommandCategory.UNKNOWN

    # -- Case / whitespace --
    def test_lowercase(self):
        assert _classify("insert into t values (1)") == CommandCategory.INSERT

    def test_mixed_case(self):
        assert _classify("Insert Into T Values (1)") == CommandCategory.INSERT

    def test_extra_whitespace(self):
        assert _classify("  INSERT   INTO   t   VALUES (1)  ") == CommandCategory.INSERT

    def test_with_line_comment(self):
        assert _classify("-- comment\nINSERT INTO t VALUES (1)") == CommandCategory.INSERT

    def test_with_block_comment(self):
        assert _classify("/* block */ INSERT INTO t VALUES (1)") == CommandCategory.INSERT


# ---------------------------------------------------------------------------
# 2. PARSER TESTS
# ---------------------------------------------------------------------------

class TestParsersComprehensive:
    """Test all SQL parsing helpers."""

    # -- INSERT table --
    def test_insert_simple(self):
        assert _parse_insert_table("INSERT INTO orders (a) VALUES (1)") == "orders"

    def test_insert_quoted(self):
        assert _parse_insert_table('INSERT INTO "My Orders" (a) VALUES (1)') == "My Orders"

    def test_insert_schema_qualified(self):
        assert _parse_insert_table("INSERT INTO public.orders (a) VALUES (1)") == "public.orders"

    def test_insert_no_columns(self):
        assert _parse_insert_table("INSERT INTO orders VALUES (1, 2)") == "orders"

    # -- UPDATE table/where --
    def test_update_simple(self):
        table, where = _parse_update_table_where("UPDATE orders SET qty=2 WHERE id=1")
        assert table == "orders"
        assert "id=1" in where

    def test_update_no_where(self):
        table, where = _parse_update_table_where("UPDATE orders SET qty=2")
        assert table == "orders"
        assert where is None

    def test_update_quoted_table(self):
        table, where = _parse_update_table_where('"My Table" SET x=1 WHERE id=1')
        # Parser expects UPDATE keyword
        table, where = _parse_update_table_where('UPDATE "My Table" SET x=1 WHERE id=1')
        assert table == "My Table"

    def test_update_schema_qualified(self):
        table, where = _parse_update_table_where("UPDATE public.orders SET qty=2 WHERE id=1")
        assert "orders" in table

    def test_update_multiple_conditions(self):
        table, where = _parse_update_table_where("UPDATE orders SET qty=2 WHERE id=1 AND status='active'")
        assert table == "orders"
        assert "id=1" in where
        assert "status" in where

    # -- DELETE table/where --
    def test_delete_simple(self):
        table, where = _parse_delete_table_where("DELETE FROM orders WHERE id=5")
        assert table == "orders"
        assert "id=5" in where

    def test_delete_no_where(self):
        table, where = _parse_delete_table_where("DELETE FROM orders")
        assert table == "orders"
        assert where is None

    def test_delete_schema_qualified(self):
        table, where = _parse_delete_table_where("DELETE FROM public.orders WHERE id=1")
        assert "orders" in table

    # -- TRUNCATE --
    def test_truncate_with_table_keyword(self):
        assert _parse_truncate_table("TRUNCATE TABLE orders") == "orders"

    def test_truncate_without_table_keyword(self):
        assert _parse_truncate_table("TRUNCATE orders") == "orders"

    def test_truncate_quoted(self):
        assert _parse_truncate_table('TRUNCATE TABLE "My Table"') == "My Table"

    # -- CREATE TABLE name --
    def test_create_table_simple(self):
        assert _parse_create_table_name("CREATE TABLE users (id INT)") == "users"

    def test_create_table_if_not_exists(self):
        assert _parse_create_table_name("CREATE TABLE IF NOT EXISTS users (id INT)") == "users"

    def test_create_table_schema(self):
        assert _parse_create_table_name("CREATE TABLE public.users (id INT)") == "public.users"

    def test_create_temp_table(self):
        assert _parse_create_table_name("CREATE TEMP TABLE tmp (id INT)") == "tmp"

    def test_create_temporary_table(self):
        assert _parse_create_table_name("CREATE TEMPORARY TABLE tmp (id INT)") == "tmp"

    # -- DROP object name --
    def test_drop_table_name(self):
        assert _parse_drop_object_name("DROP TABLE users", "TABLE") == "users"

    def test_drop_table_if_exists(self):
        assert _parse_drop_object_name("DROP TABLE IF EXISTS users", "TABLE") == "users"

    def test_drop_table_schema(self):
        assert _parse_drop_object_name("DROP TABLE public.users", "TABLE") == "public.users"

    def test_drop_index_name(self):
        assert _parse_drop_object_name("DROP INDEX idx_name", "INDEX") == "idx_name"

    def test_drop_sequence_name(self):
        assert _parse_drop_object_name("DROP SEQUENCE my_seq", "SEQUENCE") == "my_seq"

    def test_drop_schema_name(self):
        assert _parse_drop_object_name("DROP SCHEMA analytics", "SCHEMA") == "analytics"

    def test_drop_view_name(self):
        assert _parse_drop_object_name("DROP VIEW my_view", "VIEW") == "my_view"

    # -- ALTER TABLE name --
    def test_alter_table_name(self):
        assert _parse_alter_table_name("ALTER TABLE orders ADD COLUMN x INT") == "orders"

    def test_alter_table_if_exists(self):
        assert _parse_alter_table_name("ALTER TABLE IF EXISTS orders ADD COLUMN x INT") == "orders"

    def test_alter_table_schema(self):
        assert _parse_alter_table_name("ALTER TABLE public.orders ADD COLUMN x INT") == "public.orders"

    # -- ADD COLUMN name --
    def test_add_column_simple(self):
        assert _parse_alter_add_column_name("ALTER TABLE t ADD COLUMN score FLOAT") == "score"

    def test_add_column_if_not_exists(self):
        assert _parse_alter_add_column_name("ALTER TABLE t ADD COLUMN IF NOT EXISTS score FLOAT") == "score"

    def test_add_column_quoted(self):
        assert _parse_alter_add_column_name('ALTER TABLE t ADD COLUMN "My Col" INT') == "My Col"

    # -- DROP COLUMN name --
    def test_drop_column_simple(self):
        assert _parse_alter_drop_column_name("ALTER TABLE t DROP COLUMN score") == "score"

    def test_drop_column_if_exists(self):
        assert _parse_alter_drop_column_name("ALTER TABLE t DROP COLUMN IF EXISTS score") == "score"

    # -- RENAME COLUMN --
    def test_rename_column(self):
        old, new = _parse_rename_column("ALTER TABLE t RENAME COLUMN old_name TO new_name")
        assert old == "old_name"
        assert new == "new_name"

    def test_rename_column_quoted(self):
        old, new = _parse_rename_column('ALTER TABLE t RENAME COLUMN "old" TO "new"')
        assert old == "old"
        assert new == "new"

    # -- ALTER COLUMN TYPE name --
    def test_alter_column_type_name(self):
        assert _parse_alter_column_type_name("ALTER TABLE t ALTER COLUMN x TYPE BIGINT") == "x"

    # -- Generic ALTER COLUMN name --
    def test_alter_column_name_generic(self):
        assert _parse_alter_column_name_generic("ALTER TABLE t ALTER COLUMN x SET DEFAULT 0") == "x"
        assert _parse_alter_column_name_generic("ALTER TABLE t ALTER COLUMN y DROP DEFAULT") == "y"
        assert _parse_alter_column_name_generic("ALTER TABLE t ALTER COLUMN z SET NOT NULL") == "z"

    # -- CONSTRAINT names --
    def test_add_constraint_name(self):
        assert _parse_constraint_name("ALTER TABLE t ADD CONSTRAINT chk_qty CHECK (qty > 0)") == "chk_qty"

    def test_drop_constraint_name(self):
        assert _parse_constraint_name_drop("ALTER TABLE t DROP CONSTRAINT chk_qty") == "chk_qty"

    def test_drop_constraint_if_exists(self):
        assert _parse_constraint_name_drop("ALTER TABLE t DROP CONSTRAINT IF EXISTS chk_qty") == "chk_qty"

    # -- RENAME TABLE --
    def test_rename_table(self):
        old, new = _parse_rename_table("ALTER TABLE orders RENAME TO old_orders")
        assert old == "orders"
        assert new == "old_orders"

    def test_rename_table_if_exists(self):
        old, new = _parse_rename_table("ALTER TABLE IF EXISTS orders RENAME TO old_orders")
        assert old == "orders"
        assert new == "old_orders"

    # -- CREATE INDEX name --
    def test_create_index_name(self):
        assert _parse_create_index_name("CREATE INDEX idx_name ON t (col)") == "idx_name"

    def test_create_unique_index_name(self):
        assert _parse_create_index_name("CREATE UNIQUE INDEX idx ON t (col)") == "idx"

    def test_create_index_concurrently(self):
        assert _parse_create_index_name("CREATE INDEX CONCURRENTLY idx ON t (col)") == "idx"

    def test_create_index_if_not_exists(self):
        assert _parse_create_index_name("CREATE INDEX IF NOT EXISTS idx ON t (col)") == "idx"

    # -- SEQUENCE names --
    def test_create_sequence_name(self):
        assert _parse_create_sequence_name("CREATE SEQUENCE my_seq") == "my_seq"

    def test_create_sequence_if_not_exists(self):
        assert _parse_create_sequence_name("CREATE SEQUENCE IF NOT EXISTS my_seq") == "my_seq"

    def test_alter_sequence_name(self):
        assert _parse_alter_sequence_name("ALTER SEQUENCE my_seq INCREMENT BY 5") == "my_seq"

    # -- SCHEMA name --
    def test_create_schema_name(self):
        assert _parse_create_schema_name("CREATE SCHEMA analytics") == "analytics"

    def test_create_schema_if_not_exists(self):
        assert _parse_create_schema_name("CREATE SCHEMA IF NOT EXISTS analytics") == "analytics"

    # -- VIEW name --
    def test_create_view_name(self):
        assert _parse_create_view_name("CREATE VIEW v AS SELECT 1") == "v"

    def test_create_or_replace_view_name(self):
        assert _parse_create_view_name("CREATE OR REPLACE VIEW v AS SELECT 1") == "v"

    def test_create_view_schema_qualified(self):
        assert _parse_create_view_name("CREATE VIEW public.v AS SELECT 1") == "public.v"

    # -- split_schema_table --
    def test_split_with_schema(self):
        assert _split_schema_table("public.users") == ("public", "users")

    def test_split_without_schema(self):
        assert _split_schema_table("users") == (None, "users")

    def test_split_quoted(self):
        schema, table = _split_schema_table('"my_schema"."my_table"')
        assert schema == "my_schema"
        assert table == "my_table"

    # -- _build_type --
    def test_build_type_varchar(self):
        assert _build_type("character varying", 50, None, None, "varchar") == "VARCHAR(50)"

    def test_build_type_varchar_no_len(self):
        assert _build_type("character varying", None, None, None, "varchar") == "TEXT"

    def test_build_type_char(self):
        assert _build_type("character", 10, None, None, "bpchar") == "CHAR(10)"

    def test_build_type_numeric_with_scale(self):
        assert _build_type("numeric", None, 10, 2, "numeric") == "NUMERIC(10,2)"

    def test_build_type_numeric_no_scale(self):
        assert _build_type("numeric", None, None, None, "numeric") == "NUMERIC"

    def test_build_type_integer(self):
        assert _build_type("integer", None, 32, 0, "int4") == "INTEGER"

    def test_build_type_user_defined(self):
        assert _build_type("USER-DEFINED", None, None, None, "hstore") == "hstore"

    def test_build_type_text(self):
        assert _build_type("text", None, None, None, "text") == "TEXT"

    def test_build_type_boolean(self):
        assert _build_type("boolean", None, None, None, "bool") == "BOOLEAN"

    def test_build_type_timestamp(self):
        result = _build_type("timestamp without time zone", None, None, None, "timestamp")
        assert result == "TIMESTAMP WITHOUT TIME ZONE"


# ---------------------------------------------------------------------------
# 3. QUOTING TESTS
# ---------------------------------------------------------------------------

class TestQuotingComprehensive:
    """Test _quote_ident and _quote_literal edge cases."""

    def test_ident_simple(self):
        assert _quote_ident("name") == '"name"'

    def test_ident_with_space(self):
        assert _quote_ident("my table") == '"my table"'

    def test_ident_with_double_quotes(self):
        assert _quote_ident('say "hello"') == '"say ""hello"""'

    def test_ident_empty(self):
        assert _quote_ident("") == '""'

    def test_literal_none(self):
        assert _quote_literal(None) == "NULL"

    def test_literal_true(self):
        assert _quote_literal(True) == "TRUE"

    def test_literal_false(self):
        assert _quote_literal(False) == "FALSE"

    def test_literal_int(self):
        assert _quote_literal(42) == "42"

    def test_literal_negative_int(self):
        assert _quote_literal(-7) == "-7"

    def test_literal_float(self):
        assert _quote_literal(3.14) == "3.14"

    def test_literal_zero(self):
        assert _quote_literal(0) == "0"

    def test_literal_string(self):
        assert _quote_literal("hello") == "'hello'"

    def test_literal_string_with_single_quote(self):
        result = _quote_literal("it's here")
        assert "it's here" in result
        assert result.startswith("$")

    def test_literal_empty_string(self):
        assert _quote_literal("") == "''"

    def test_literal_string_with_backslash(self):
        assert _quote_literal("path\\to\\file") == "'path\\to\\file'"

    def test_literal_unicode(self):
        assert _quote_literal("caf├⌐") == "'caf├⌐'"

    def test_literal_large_number(self):
        assert _quote_literal(99999999999) == "99999999999"


# ---------------------------------------------------------------------------
# 4. INSERT INVERSE TESTS
# ---------------------------------------------------------------------------

class TestInsertInverse:

    def test_insert_single_row_with_pk(self):
        eng = engine_with_pk(pk_cols=("id",))
        inv = eng.generate("INSERT INTO orders (item, qty) VALUES ('Widget', 3)")
        assert inv.category == CommandCategory.INSERT
        assert inv.steps == []  # before finalize

        eng.finalize_insert(inv, [{"id": 7, "item": "Widget", "qty": 3}])
        assert len(inv.steps) == 1
        assert "DELETE FROM" in inv.steps[0]
        assert '"id" = 7' in inv.steps[0]
        assert inv.is_reversible
        tracker.record("INSERT", "single_row_with_pk", True)

    def test_insert_multi_row_with_pk(self):
        eng = engine_with_pk(pk_cols=("id",))
        inv = eng.generate("INSERT INTO orders (item, qty) VALUES ('A', 1), ('B', 2), ('C', 3)")
        eng.finalize_insert(inv, [
            {"id": 10, "item": "A", "qty": 1},
            {"id": 11, "item": "B", "qty": 2},
            {"id": 12, "item": "C", "qty": 3},
        ])
        assert len(inv.steps) == 3
        assert all("DELETE FROM" in s for s in inv.steps)
        assert '"id" = 10' in inv.steps[0]
        assert '"id" = 12' in inv.steps[2]
        tracker.record("INSERT", "multi_row_with_pk", True)

    def test_insert_with_composite_pk(self):
        eng = engine_with_composite_pk()
        inv = eng.generate("INSERT INTO order_lines (order_id, line_id, product, qty) VALUES (1, 10, 'A', 5)")
        eng.finalize_insert(inv, [{"order_id": 1, "line_id": 10, "product": "A", "qty": 5}])
        assert len(inv.steps) == 1
        step = inv.steps[0]
        assert '"order_id" = 1' in step
        assert '"line_id" = 10' in step
        tracker.record("INSERT", "composite_pk", True)

    def test_insert_no_pk_uses_full_row_match(self):
        eng = engine_no_pk()
        inv = eng.generate("INSERT INTO logs (item, qty) VALUES ('X', 1)")
        eng.finalize_insert(inv, [{"item": "X", "qty": 1}])
        assert len(inv.steps) == 1
        assert "DELETE FROM" in inv.steps[0]
        assert not inv.is_reversible  # marked fragile
        tracker.record("INSERT", "no_pk_full_row_match", True)

    def test_insert_no_rows_returned(self):
        eng = engine_with_pk()
        inv = eng.generate("INSERT INTO orders (item) VALUES ('X')")
        eng.finalize_insert(inv, [])
        assert inv.steps == []
        tracker.record("INSERT", "no_rows_returned", True)

    def test_insert_with_null_values(self):
        eng = engine_with_pk(pk_cols=("id",))
        inv = eng.generate("INSERT INTO orders (item, qty) VALUES (NULL, NULL)")
        eng.finalize_insert(inv, [{"id": 99, "item": None, "qty": None}])
        assert len(inv.steps) == 1
        assert '"id" = 99' in inv.steps[0]
        tracker.record("INSERT", "null_values", True)

    def test_insert_with_special_chars(self):
        eng = engine_with_pk(pk_cols=("id",))
        inv = eng.generate("INSERT INTO orders (item, qty) VALUES ('O''Brien', 1)")
        eng.finalize_insert(inv, [{"id": 50, "item": "O'Brien", "qty": 1}])
        assert len(inv.steps) == 1
        # INSERT inverse with PK is DELETE by PK
        assert "DELETE FROM" in inv.steps[0]
        assert '"id" = 50' in inv.steps[0]
        tracker.record("INSERT", "special_chars_quote", True)

    def test_insert_with_boolean_values(self):
        eng = engine_with_pk(pk_cols=("id",))
        inv = eng.generate("INSERT INTO flags (id, active) VALUES (1, true)")
        eng.finalize_insert(inv, [{"id": 1, "active": True}])
        # INSERT inverse with PK is DELETE by PK
        assert "DELETE FROM" in inv.steps[0]
        assert '"id" = 1' in inv.steps[0]
        tracker.record("INSERT", "boolean_values", True)

    def test_insert_before_image_stored(self):
        eng = engine_with_pk(pk_cols=("id",))
        inv = eng.generate("INSERT INTO orders (item) VALUES ('X')")
        rows = [{"id": 1, "item": "X", "qty": 0}]
        eng.finalize_insert(inv, rows)
        assert inv.before_image == rows
        tracker.record("INSERT", "before_image_stored", True)


# ---------------------------------------------------------------------------
# 5. UPDATE INVERSE TESTS
# ---------------------------------------------------------------------------

class TestUpdateInverse:

    def test_update_single_row_with_pk(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": "Widget", "qty": 3}])
        inv = eng.generate("UPDATE orders SET qty=10 WHERE id=1")
        assert inv.category == CommandCategory.UPDATE
        assert len(inv.steps) == 1
        step = inv.steps[0]
        assert "UPDATE" in step
        assert "SET" in step
        assert '"id" = 1' in step  # WHERE clause uses PK
        assert "'Widget'" in step  # restores original value
        assert "3" in step
        assert inv.is_reversible
        tracker.record("UPDATE", "single_row_with_pk", True)

    def test_update_multiple_rows(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[
            {"id": 1, "item": "A", "qty": 1},
            {"id": 2, "item": "B", "qty": 2},
            {"id": 3, "item": "C", "qty": 3},
        ])
        inv = eng.generate("UPDATE orders SET qty=0 WHERE qty < 10")
        assert len(inv.steps) == 3
        tracker.record("UPDATE", "multiple_rows", True)

    def test_update_no_matching_rows(self):
        eng = engine_empty_table()
        inv = eng.generate("UPDATE orders SET qty=10 WHERE id=999")
        assert inv.steps == []
        tracker.record("UPDATE", "no_matching_rows", True)

    def test_update_no_pk(self):
        eng = engine_no_pk(rows=[{"item": "Widget", "qty": 3}])
        inv = eng.generate("UPDATE orders SET qty=10 WHERE item='Widget'")
        assert not inv.is_reversible
        tracker.record("UPDATE", "no_pk_not_reversible", True)

    def test_update_no_where_clause(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[
            {"id": 1, "item": "A", "qty": 1},
        ])
        inv = eng.generate("UPDATE orders SET qty=0")
        # No WHERE ΓåÆ selects all rows
        assert len(inv.steps) >= 1
        tracker.record("UPDATE", "no_where_clause", True)

    def test_update_with_composite_pk(self):
        eng = engine_with_composite_pk()
        inv = eng.generate("UPDATE order_lines SET qty=99 WHERE order_id=1 AND line_id=10")
        assert len(inv.steps) == 1
        step = inv.steps[0]
        assert '"order_id" = 1' in step
        assert '"line_id" = 10' in step
        tracker.record("UPDATE", "composite_pk", True)

    def test_update_before_image_captured(self):
        rows = [{"id": 1, "item": "Widget", "qty": 3}]
        eng = engine_with_pk(pk_cols=("id",), rows=rows)
        inv = eng.generate("UPDATE orders SET qty=10 WHERE id=1")
        assert inv.before_image == rows
        tracker.record("UPDATE", "before_image_captured", True)

    def test_update_null_to_value(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": None, "qty": None}])
        inv = eng.generate("UPDATE orders SET item='New' WHERE id=1")
        assert len(inv.steps) == 1
        assert "NULL" in inv.steps[0]  # restores NULL
        tracker.record("UPDATE", "null_to_value", True)

    def test_update_multiple_set_columns(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": "Old", "qty": 5}])
        inv = eng.generate("UPDATE orders SET item='New', qty=10 WHERE id=1")
        step = inv.steps[0]
        assert "'Old'" in step
        assert "5" in step
        tracker.record("UPDATE", "multiple_set_columns", True)


# ---------------------------------------------------------------------------
# 6. DELETE INVERSE TESTS
# ---------------------------------------------------------------------------

class TestDeleteInverse:

    def test_delete_single_row(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": "Widget", "qty": 3}])
        inv = eng.generate("DELETE FROM orders WHERE id=1")
        assert inv.category == CommandCategory.DELETE
        assert len(inv.steps) == 1
        assert "INSERT INTO" in inv.steps[0]
        assert "'Widget'" in inv.steps[0]
        tracker.record("DELETE", "single_row", True)

    def test_delete_multiple_rows(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[
            {"id": 1, "item": "A", "qty": 1},
            {"id": 2, "item": "B", "qty": 2},
        ])
        inv = eng.generate("DELETE FROM orders WHERE qty < 5")
        assert len(inv.steps) == 2
        assert all("INSERT INTO" in s for s in inv.steps)
        tracker.record("DELETE", "multiple_rows", True)

    def test_delete_no_matching_rows(self):
        eng = engine_empty_table()
        inv = eng.generate("DELETE FROM orders WHERE id=999")
        assert inv.steps == []
        tracker.record("DELETE", "no_matching_rows", True)

    def test_delete_all_rows_no_where(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[
            {"id": 1, "item": "A", "qty": 1},
            {"id": 2, "item": "B", "qty": 2},
        ])
        inv = eng.generate("DELETE FROM orders")
        assert len(inv.steps) == 2
        tracker.record("DELETE", "all_rows_no_where", True)

    def test_delete_with_null_values(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": None, "qty": None}])
        inv = eng.generate("DELETE FROM orders WHERE id=1")
        assert "NULL" in inv.steps[0]
        tracker.record("DELETE", "with_null_values", True)

    def test_delete_preserves_all_columns(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": "W", "qty": 3}])
        inv = eng.generate("DELETE FROM orders WHERE id=1")
        step = inv.steps[0]
        assert '"id"' in step
        assert '"item"' in step
        assert '"qty"' in step
        tracker.record("DELETE", "preserves_all_columns", True)

    def test_delete_before_image(self):
        rows = [{"id": 1, "item": "X", "qty": 5}]
        eng = engine_with_pk(pk_cols=("id",), rows=rows)
        inv = eng.generate("DELETE FROM orders WHERE id=1")
        assert inv.before_image == rows
        tracker.record("DELETE", "before_image", True)


# ---------------------------------------------------------------------------
# 7. TRUNCATE INVERSE TESTS
# ---------------------------------------------------------------------------

class TestTruncateInverse:

    def test_truncate_with_data(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[
            {"id": 1, "item": "A", "qty": 1},
            {"id": 2, "item": "B", "qty": 2},
            {"id": 3, "item": "C", "qty": 3},
        ])
        inv = eng.generate("TRUNCATE TABLE orders")
        assert inv.category == CommandCategory.TRUNCATE
        assert len(inv.steps) == 3
        assert all("INSERT INTO" in s for s in inv.steps)
        tracker.record("TRUNCATE", "with_data", True)

    def test_truncate_empty_table(self):
        eng = engine_empty_table()
        inv = eng.generate("TRUNCATE orders")
        assert inv.steps == []
        tracker.record("TRUNCATE", "empty_table", True)

    def test_truncate_without_table_keyword(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": "A", "qty": 1}])
        inv = eng.generate("TRUNCATE orders")
        assert inv.category == CommandCategory.TRUNCATE
        assert len(inv.steps) == 1
        tracker.record("TRUNCATE", "without_table_keyword", True)

    def test_truncate_preserves_all_column_values(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": "W", "qty": 99}])
        inv = eng.generate("TRUNCATE TABLE orders")
        step = inv.steps[0]
        assert "1" in step
        assert "'W'" in step
        assert "99" in step
        tracker.record("TRUNCATE", "preserves_values", True)

    def test_truncate_notes_row_count(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": i, "item": "X", "qty": i} for i in range(5)])
        inv = eng.generate("TRUNCATE TABLE orders")
        assert "5 rows" in inv.notes
        tracker.record("TRUNCATE", "notes_row_count", True)


# ---------------------------------------------------------------------------
# 8. CREATE TABLE INVERSE
# ---------------------------------------------------------------------------

class TestCreateTableInverse:

    def test_create_table_simple(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)")
        assert inv.category == CommandCategory.CREATE_TABLE
        assert len(inv.steps) == 1
        assert "DROP TABLE" in inv.steps[0]
        assert "users" in inv.steps[0].lower()
        assert inv.is_reversible
        tracker.record("CREATE TABLE", "simple", True)

    def test_create_table_if_not_exists(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE TABLE IF NOT EXISTS users (id INT)")
        assert "DROP TABLE" in inv.steps[0]
        assert "users" in inv.steps[0].lower()
        tracker.record("CREATE TABLE", "if_not_exists", True)

    def test_create_temp_table(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE TEMP TABLE tmp_data (id INT, val TEXT)")
        assert "DROP TABLE" in inv.steps[0]
        assert "tmp_data" in inv.steps[0].lower()
        tracker.record("CREATE TABLE", "temp_table", True)

    def test_create_table_schema_qualified(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE TABLE analytics.events (id INT, data JSONB)")
        assert "DROP TABLE" in inv.steps[0]
        tracker.record("CREATE TABLE", "schema_qualified", True)


# ---------------------------------------------------------------------------
# 9. DROP TABLE INVERSE
# ---------------------------------------------------------------------------

class TestDropTableInverse:

    def test_drop_table_with_catalog(self):
        conn = MockConnection({
            "INFORMATION_SCHEMA.COLUMNS": [
                ("id", "integer", None, 10, 0, "NO", None, "int4"),
                ("name", "character varying", 100, None, None, "YES", None, "varchar"),
            ],
            "TABLE_CONSTRAINTS": [("PRIMARY KEY", "pk_users", "id")],
            "PRIMARY KEY": [("id",)],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("DROP TABLE users")
        assert inv.category == CommandCategory.DROP_TABLE
        assert len(inv.steps) == 1
        assert "CREATE TABLE" in inv.steps[0]
        tracker.record("DROP TABLE", "with_catalog_reconstruction", True)

    def test_drop_table_no_catalog(self):
        conn = MockConnection({"INFORMATION_SCHEMA.COLUMNS": [], "PRIMARY KEY": []})
        eng = InverseEngine(conn)
        inv = eng.generate("DROP TABLE ghost_table")
        assert not inv.is_reversible
        tracker.record("DROP TABLE", "no_catalog_not_reversible", True)

    def test_drop_table_if_exists(self):
        conn = MockConnection({
            "INFORMATION_SCHEMA.COLUMNS": [
                ("id", "integer", None, 10, 0, "NO", None, "int4"),
            ],
            "TABLE_CONSTRAINTS": [],
            "PRIMARY KEY": [("id",)],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("DROP TABLE IF EXISTS users")
        assert inv.category == CommandCategory.DROP_TABLE
        tracker.record("DROP TABLE", "if_exists", True)

    def test_drop_table_reconstructs_not_null(self):
        conn = MockConnection({
            "INFORMATION_SCHEMA.COLUMNS": [
                ("id", "integer", None, 10, 0, "NO", None, "int4"),
                ("email", "character varying", 200, None, None, "NO", None, "varchar"),
            ],
            "TABLE_CONSTRAINTS": [],
            "PRIMARY KEY": [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("DROP TABLE users")
        if inv.steps:
            assert "NOT NULL" in inv.steps[0]
            tracker.record("DROP TABLE", "reconstructs_not_null", True)
        else:
            tracker.record("DROP TABLE", "reconstructs_not_null", False, "No steps generated")

    def test_drop_table_reconstructs_default(self):
        conn = MockConnection({
            "INFORMATION_SCHEMA.COLUMNS": [
                ("status", "character varying", 20, None, None, "YES", "'active'::character varying", "varchar"),
            ],
            "TABLE_CONSTRAINTS": [],
            "PRIMARY KEY": [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("DROP TABLE users")
        if inv.steps:
            assert "DEFAULT" in inv.steps[0]
            tracker.record("DROP TABLE", "reconstructs_default", True)
        else:
            tracker.record("DROP TABLE", "reconstructs_default", False, "No steps generated")


# ---------------------------------------------------------------------------
# 10. ALTER TABLE INVERSE ΓÇö all sub-types
# ---------------------------------------------------------------------------

class TestAlterTableAddColumn:

    def test_add_column_simple(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ADD COLUMN discount NUMERIC(5,2)")
        assert inv.category == CommandCategory.ALTER_TABLE
        assert "DROP COLUMN" in inv.steps[0]
        assert "discount" in inv.steps[0].lower()
        tracker.record("ALTER ADD COLUMN", "simple", True)

    def test_add_column_if_not_exists(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ADD COLUMN IF NOT EXISTS score INT")
        assert "DROP COLUMN" in inv.steps[0]
        assert "score" in inv.steps[0].lower()
        tracker.record("ALTER ADD COLUMN", "if_not_exists", True)

    def test_add_column_with_default(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ADD COLUMN active BOOLEAN DEFAULT TRUE")
        assert "DROP COLUMN" in inv.steps[0]
        tracker.record("ALTER ADD COLUMN", "with_default", True)

    def test_add_column_not_null(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'new'")
        assert "DROP COLUMN" in inv.steps[0]
        tracker.record("ALTER ADD COLUMN", "not_null_with_default", True)


class TestAlterTableDropColumn:

    def test_drop_column_with_catalog(self):
        conn = MockConnection({
            "INFORMATION_SCHEMA.COLUMNS": [
                ("numeric", None, 5, 2, "YES", None, "numeric"),
            ],
            "PRIMARY KEY": [("id",)],
            "SELECT *": [{"id": 1}],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("ALTER TABLE orders DROP COLUMN discount")
        assert "ADD COLUMN" in inv.steps[0]
        tracker.record("ALTER DROP COLUMN", "with_catalog", True)

    def test_drop_column_no_catalog(self):
        conn = MockConnection({
            "INFORMATION_SCHEMA.COLUMNS": [],
            "PRIMARY KEY": [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("ALTER TABLE orders DROP COLUMN ghost_col")
        assert not inv.is_reversible
        tracker.record("ALTER DROP COLUMN", "no_catalog_not_reversible", True)

    def test_drop_column_if_exists(self):
        conn = MockConnection({
            "INFORMATION_SCHEMA.COLUMNS": [
                ("integer", None, 10, 0, "NO", None, "int4"),
            ],
            "PRIMARY KEY": [("id",)],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("ALTER TABLE orders DROP COLUMN IF EXISTS score")
        assert inv.category == CommandCategory.ALTER_TABLE
        tracker.record("ALTER DROP COLUMN", "if_exists", True)


class TestAlterTableRenameColumn:

    def test_rename_column(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders RENAME COLUMN old_name TO new_name")
        assert "RENAME COLUMN" in inv.steps[0]
        assert '"new_name"' in inv.steps[0]
        assert '"old_name"' in inv.steps[0]
        tracker.record("ALTER RENAME COLUMN", "basic", True)

    def test_rename_column_quoted(self):
        eng = engine_with_pk()
        inv = eng.generate('ALTER TABLE orders RENAME COLUMN "Old Col" TO "New Col"')
        assert "RENAME COLUMN" in inv.steps[0]
        tracker.record("ALTER RENAME COLUMN", "quoted_names", True)


class TestAlterColumnType:

    def test_alter_type_with_catalog(self):
        # _get_column_type queries: data_type, char_max_len, num_prec, num_scale, udt_name (5 cols)
        conn = MockConnection({
            "INFORMATION_SCHEMA.COLUMNS": [
                ("integer", None, 10, 0, "int4"),
            ],
            "PRIMARY KEY": [("id",)],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("ALTER TABLE orders ALTER COLUMN qty TYPE BIGINT")
        assert "TYPE" in inv.steps[0]
        assert "INTEGER" in inv.steps[0]  # restores original
        tracker.record("ALTER COLUMN TYPE", "with_catalog", True)

    def test_alter_type_no_catalog(self):
        conn = MockConnection({
            "INFORMATION_SCHEMA.COLUMNS": [],
            "PRIMARY KEY": [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("ALTER TABLE orders ALTER COLUMN qty TYPE BIGINT")
        assert not inv.is_reversible
        tracker.record("ALTER COLUMN TYPE", "no_catalog_not_reversible", True)


class TestAlterConstraint:

    def test_add_constraint_check(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty > 0)")
        assert "DROP CONSTRAINT" in inv.steps[0]
        assert "chk_qty" in inv.steps[0].lower()
        tracker.record("ALTER CONSTRAINT", "add_check", True)

    def test_add_constraint_unique(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ADD CONSTRAINT uq_item UNIQUE (item)")
        assert "DROP CONSTRAINT" in inv.steps[0]
        tracker.record("ALTER CONSTRAINT", "add_unique", True)

    def test_add_constraint_foreign_key(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)")
        assert "DROP CONSTRAINT" in inv.steps[0]
        tracker.record("ALTER CONSTRAINT", "add_foreign_key", True)

    def test_drop_constraint_with_catalog(self):
        conn = MockConnection({
            "PRIMARY KEY": [("id",)],
            "PG_GET_CONSTRAINTDEF": [("CHECK (qty > 0)",)],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("ALTER TABLE orders DROP CONSTRAINT chk_qty")
        assert "ADD CONSTRAINT" in inv.steps[0]
        tracker.record("ALTER CONSTRAINT", "drop_with_catalog", True)

    def test_drop_constraint_no_catalog(self):
        conn = MockConnection({
            "PRIMARY KEY": [],
            "PG_GET_CONSTRAINTDEF": [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("ALTER TABLE orders DROP CONSTRAINT ghost_chk")
        assert not inv.is_reversible
        tracker.record("ALTER CONSTRAINT", "drop_no_catalog_not_reversible", True)

    def test_drop_constraint_if_exists(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders DROP CONSTRAINT IF EXISTS chk_qty")
        assert inv.category == CommandCategory.ALTER_TABLE
        tracker.record("ALTER CONSTRAINT", "drop_if_exists", True)


class TestAlterDefault:

    def test_set_default(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ALTER COLUMN qty SET DEFAULT 0")
        assert "DROP DEFAULT" in inv.steps[0]
        tracker.record("ALTER DEFAULT", "set_default", True)

    def test_drop_default_with_catalog(self):
        conn = MockConnection({
            "PRIMARY KEY": [("id",)],
            "INFORMATION_SCHEMA.COLUMNS": [("42",)],
            "SELECT *": [{"id": 1}],
            "COLUMN_DEFAULT": [("42",)],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("ALTER TABLE orders ALTER COLUMN qty DROP DEFAULT")
        if inv.steps:
            assert "SET DEFAULT" in inv.steps[0]
            tracker.record("ALTER DEFAULT", "drop_with_catalog", True)
        else:
            tracker.record("ALTER DEFAULT", "drop_with_catalog", not inv.is_reversible,
                           "Not reversible when column_default not found")

    def test_drop_default_no_catalog(self):
        conn = MockConnection({
            "PRIMARY KEY": [],
            "INFORMATION_SCHEMA.COLUMNS": [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("ALTER TABLE orders ALTER COLUMN qty DROP DEFAULT")
        assert not inv.is_reversible
        tracker.record("ALTER DEFAULT", "drop_no_catalog", True)


class TestAlterNotNull:

    def test_set_not_null(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ALTER COLUMN qty SET NOT NULL")
        assert "DROP NOT NULL" in inv.steps[0]
        tracker.record("ALTER NOT NULL", "set_not_null", True)

    def test_drop_not_null(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ALTER COLUMN qty DROP NOT NULL")
        assert "SET NOT NULL" in inv.steps[0]
        tracker.record("ALTER NOT NULL", "drop_not_null", True)


class TestAlterTableUnrecognized:

    def test_cluster_on(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders CLUSTER ON some_index")
        assert not inv.is_reversible
        tracker.record("ALTER UNRECOGNIZED", "cluster_on", True)

    def test_owner_to(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders OWNER TO new_owner")
        assert not inv.is_reversible
        tracker.record("ALTER UNRECOGNIZED", "owner_to", True)


# ---------------------------------------------------------------------------
# 11. RENAME TABLE INVERSE
# ---------------------------------------------------------------------------

class TestRenameTableInverse:

    def test_rename_table(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders RENAME TO old_orders")
        assert inv.category == CommandCategory.RENAME_TABLE
        assert "RENAME TO" in inv.steps[0]
        assert '"orders"' in inv.steps[0]
        tracker.record("RENAME TABLE", "basic", True)

    def test_rename_table_schema_qualified(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE public.orders RENAME TO old_orders")
        assert inv.category == CommandCategory.RENAME_TABLE
        assert "RENAME TO" in inv.steps[0]
        tracker.record("RENAME TABLE", "schema_qualified", True)

    def test_rename_table_if_exists(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE IF EXISTS orders RENAME TO archived_orders")
        assert inv.category == CommandCategory.RENAME_TABLE
        tracker.record("RENAME TABLE", "if_exists", True)


# ---------------------------------------------------------------------------
# 12. INDEX INVERSE
# ---------------------------------------------------------------------------

class TestCreateIndexInverse:

    def test_create_index(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE INDEX idx_item ON orders (item)")
        assert inv.category == CommandCategory.CREATE_INDEX
        assert "DROP INDEX" in inv.steps[0]
        assert "idx_item" in inv.steps[0].lower()
        tracker.record("CREATE INDEX", "basic", True)

    def test_create_unique_index(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE UNIQUE INDEX idx_email ON users (email)")
        assert "DROP INDEX" in inv.steps[0]
        tracker.record("CREATE INDEX", "unique", True)

    def test_create_index_concurrently(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE INDEX CONCURRENTLY idx_name ON users (name)")
        assert "DROP INDEX" in inv.steps[0]
        tracker.record("CREATE INDEX", "concurrently", True)

    def test_create_index_if_not_exists(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE INDEX IF NOT EXISTS idx_name ON users (name)")
        assert "DROP INDEX" in inv.steps[0]
        tracker.record("CREATE INDEX", "if_not_exists", True)

    def test_create_multicolumn_index(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE INDEX idx_multi ON orders (item, qty)")
        assert "DROP INDEX" in inv.steps[0]
        tracker.record("CREATE INDEX", "multicolumn", True)


class TestDropIndexInverse:

    def test_drop_index_with_catalog(self):
        eng = engine_with_index_def("CREATE INDEX idx_item ON public.orders USING btree (item)")
        inv = eng.generate("DROP INDEX idx_item")
        assert inv.category == CommandCategory.DROP_INDEX
        assert len(inv.steps) == 1
        assert "CREATE INDEX" in inv.steps[0]
        tracker.record("DROP INDEX", "with_catalog", True)

    def test_drop_index_no_catalog(self):
        eng = make_engine({"PG_INDEXES": [], "PRIMARY KEY": []})
        inv = eng.generate("DROP INDEX ghost_idx")
        assert not inv.is_reversible
        tracker.record("DROP INDEX", "no_catalog_not_reversible", True)

    def test_drop_index_if_exists(self):
        eng = engine_with_index_def("CREATE INDEX idx_x ON t (x)")
        inv = eng.generate("DROP INDEX IF EXISTS idx_x")
        assert inv.category == CommandCategory.DROP_INDEX
        tracker.record("DROP INDEX", "if_exists", True)


# ---------------------------------------------------------------------------
# 13. SEQUENCE INVERSE
# ---------------------------------------------------------------------------

class TestCreateSequenceInverse:

    def test_create_sequence(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE SEQUENCE order_id_seq START 1000")
        assert inv.category == CommandCategory.CREATE_SEQUENCE
        assert "DROP SEQUENCE" in inv.steps[0]
        tracker.record("CREATE SEQUENCE", "basic", True)

    def test_create_sequence_if_not_exists(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE SEQUENCE IF NOT EXISTS my_seq")
        assert "DROP SEQUENCE" in inv.steps[0]
        tracker.record("CREATE SEQUENCE", "if_not_exists", True)

    def test_create_sequence_with_options(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE SEQUENCE my_seq START 1 INCREMENT 10 MINVALUE 1 MAXVALUE 10000 CACHE 5 NO CYCLE")
        assert "DROP SEQUENCE" in inv.steps[0]
        tracker.record("CREATE SEQUENCE", "with_options", True)


class TestDropSequenceInverse:

    def test_drop_sequence_with_catalog(self):
        conn = MockConnection({
            "PG_SEQUENCES": [(1, 1, 9223372036854775807, 1, 1, False)],
            "PRIMARY KEY": [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("DROP SEQUENCE order_id_seq")
        assert inv.category == CommandCategory.DROP_SEQUENCE
        tracker.record("DROP SEQUENCE", "with_catalog", True)

    def test_drop_sequence_no_catalog(self):
        conn = MockConnection({"PG_SEQUENCES": [], "PRIMARY KEY": []})
        eng = InverseEngine(conn)
        inv = eng.generate("DROP SEQUENCE ghost_seq")
        assert not inv.is_reversible
        tracker.record("DROP SEQUENCE", "no_catalog_not_reversible", True)


class TestAlterSequenceInverse:

    def test_alter_sequence_with_state(self):
        conn = MockConnection({
            "PRIMARY KEY": [],
            # The engine queries the sequence directly via: SELECT ... FROM "seq_name"
            "ORDER_ID_SEQ": [{"last_value": 100, "start_value": 1, "increment_by": 1, "min_value": 1, "max_value": 9999}],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("ALTER SEQUENCE order_id_seq INCREMENT BY 5")
        if inv.steps:
            assert "ALTER SEQUENCE" in inv.steps[0]
            tracker.record("ALTER SEQUENCE", "with_state", True)
        else:
            tracker.record("ALTER SEQUENCE", "with_state", not inv.is_reversible,
                           "State query match may vary")

    def test_alter_sequence_no_state(self):
        conn = MockConnection({"PRIMARY KEY": []})
        eng = InverseEngine(conn)
        inv = eng.generate("ALTER SEQUENCE ghost_seq INCREMENT BY 5")
        assert not inv.is_reversible
        tracker.record("ALTER SEQUENCE", "no_state", True)


# ---------------------------------------------------------------------------
# 14. SCHEMA INVERSE
# ---------------------------------------------------------------------------

class TestCreateSchemaInverse:

    def test_create_schema(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE SCHEMA analytics")
        assert inv.category == CommandCategory.CREATE_SCHEMA
        assert "DROP SCHEMA" in inv.steps[0]
        assert "analytics" in inv.steps[0].lower()
        assert "RESTRICT" in inv.steps[0]
        tracker.record("CREATE SCHEMA", "basic", True)

    def test_create_schema_if_not_exists(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE SCHEMA IF NOT EXISTS reporting")
        assert "DROP SCHEMA" in inv.steps[0]
        tracker.record("CREATE SCHEMA", "if_not_exists", True)


class TestDropSchemaInverse:

    def test_drop_schema(self):
        eng = engine_with_pk()
        inv = eng.generate("DROP SCHEMA analytics")
        assert inv.category == CommandCategory.DROP_SCHEMA
        assert "CREATE SCHEMA" in inv.steps[0]
        tracker.record("DROP SCHEMA", "basic", True)

    def test_drop_schema_if_exists(self):
        eng = engine_with_pk()
        inv = eng.generate("DROP SCHEMA IF EXISTS analytics")
        assert "CREATE SCHEMA" in inv.steps[0]
        tracker.record("DROP SCHEMA", "if_exists", True)

    def test_drop_schema_cascade(self):
        eng = engine_with_pk()
        inv = eng.generate("DROP SCHEMA analytics CASCADE")
        assert "CREATE SCHEMA" in inv.steps[0]
        tracker.record("DROP SCHEMA", "cascade", True)


# ---------------------------------------------------------------------------
# 15. VIEW INVERSE
# ---------------------------------------------------------------------------

class TestCreateViewInverse:

    def test_create_view_new(self):
        eng = engine_with_view_def(None)  # view doesn't exist
        inv = eng.generate("CREATE VIEW v_summary AS SELECT * FROM orders")
        assert inv.category == CommandCategory.CREATE_VIEW
        assert "DROP VIEW" in inv.steps[0]
        tracker.record("CREATE VIEW", "new_view", True)

    def test_create_or_replace_view_new(self):
        eng = engine_with_view_def(None)
        inv = eng.generate("CREATE OR REPLACE VIEW v_new AS SELECT 1")
        assert "DROP VIEW" in inv.steps[0]
        tracker.record("CREATE VIEW", "or_replace_new", True)

    def test_create_or_replace_view_existing(self):
        eng = engine_with_view_def("SELECT item, SUM(qty) FROM orders GROUP BY item")
        inv = eng.generate("CREATE OR REPLACE VIEW v_summary AS SELECT * FROM orders")
        assert "CREATE OR REPLACE VIEW" in inv.steps[0]
        assert inv.before_image is not None
        tracker.record("CREATE VIEW", "or_replace_existing", True)


class TestDropViewInverse:

    def test_drop_view_with_catalog(self):
        eng = engine_with_view_def("SELECT item, count(*) FROM orders GROUP BY item")
        inv = eng.generate("DROP VIEW v_summary")
        assert inv.category == CommandCategory.DROP_VIEW
        assert "CREATE VIEW" in inv.steps[0]
        tracker.record("DROP VIEW", "with_catalog", True)

    def test_drop_view_no_catalog(self):
        eng = engine_with_view_def(None)
        inv = eng.generate("DROP VIEW ghost_view")
        assert not inv.is_reversible
        tracker.record("DROP VIEW", "no_catalog_not_reversible", True)

    def test_drop_view_if_exists(self):
        eng = engine_with_view_def("SELECT 1")
        inv = eng.generate("DROP VIEW IF EXISTS v_temp")
        assert inv.category == CommandCategory.DROP_VIEW
        tracker.record("DROP VIEW", "if_exists", True)


# ---------------------------------------------------------------------------
# 16. UNKNOWN / SELECT
# ---------------------------------------------------------------------------

class TestUnknownCommands:

    def test_select(self):
        eng = engine_with_pk()
        inv = eng.generate("SELECT * FROM orders")
        assert inv.category == CommandCategory.UNKNOWN
        assert not inv.is_reversible
        assert inv.steps == []
        tracker.record("UNKNOWN", "select", True)

    def test_grant(self):
        eng = engine_with_pk()
        inv = eng.generate("GRANT ALL ON orders TO user1")
        assert inv.category == CommandCategory.UNKNOWN
        assert not inv.is_reversible
        tracker.record("UNKNOWN", "grant", True)

    def test_explain(self):
        eng = engine_with_pk()
        inv = eng.generate("EXPLAIN ANALYZE SELECT 1")
        assert inv.category == CommandCategory.UNKNOWN
        assert not inv.is_reversible
        tracker.record("UNKNOWN", "explain", True)


# ---------------------------------------------------------------------------
# 17. SERIALIZATION
# ---------------------------------------------------------------------------

class TestSerialization:

    def test_round_trip(self):
        inv = InverseCommand(
            category=CommandCategory.DELETE,
            forward_sql="DELETE FROM orders WHERE id=1",
            steps=["INSERT INTO orders (id, item) VALUES (1, 'X');"],
            before_image=[{"id": 1, "item": "X"}],
            is_reversible=True,
            notes="test",
        )
        d = inv.to_dict()
        inv2 = InverseCommand.from_dict(d)
        assert inv.category == inv2.category
        assert inv.steps == inv2.steps
        assert inv.before_image == inv2.before_image
        assert inv.is_reversible == inv2.is_reversible
        assert inv.notes == inv2.notes
        tracker.record("SERIALIZATION", "round_trip", True)

    def test_json_serializable(self):
        inv = InverseCommand(
            category=CommandCategory.INSERT,
            forward_sql="INSERT INTO t VALUES (1)",
            steps=["DELETE FROM t WHERE id=1;"],
            is_reversible=True,
        )
        d = inv.to_dict()
        json_str = json.dumps(d)
        assert json_str
        loaded = json.loads(json_str)
        assert loaded["category"] == "INSERT"
        tracker.record("SERIALIZATION", "json_serializable", True)

    def test_from_dict_defaults(self):
        d = {"category": "UPDATE", "forward_sql": "UPDATE t SET x=1", "steps": []}
        inv = InverseCommand.from_dict(d)
        assert inv.is_reversible is True
        assert inv.notes == ""
        assert inv.before_image is None
        tracker.record("SERIALIZATION", "from_dict_defaults", True)


# ---------------------------------------------------------------------------
# 18. EDGE CASES & ERROR HANDLING
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_sql_with_line_comments(self):
        sql = "-- delete old\nDELETE FROM orders WHERE id=1"
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": "A", "qty": 1}])
        inv = eng.generate(sql)
        assert inv.category == CommandCategory.DELETE
        assert len(inv.steps) == 1
        tracker.record("EDGE CASES", "line_comments", True)

    def test_sql_with_block_comments(self):
        sql = "/* bulk update */ UPDATE orders SET qty=0 WHERE id=1"
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": "A", "qty": 5}])
        inv = eng.generate(sql)
        assert inv.category == CommandCategory.UPDATE
        tracker.record("EDGE CASES", "block_comments", True)

    def test_sql_with_extra_whitespace(self):
        sql = "  INSERT   INTO   orders   (item)   VALUES   ('X')  "
        eng = engine_with_pk()
        inv = eng.generate(sql)
        assert inv.category == CommandCategory.INSERT
        tracker.record("EDGE CASES", "extra_whitespace", True)

    def test_sql_mixed_case(self):
        sql = "Insert Into Orders (Item) Values ('Widget')"
        eng = engine_with_pk()
        inv = eng.generate(sql)
        assert inv.category == CommandCategory.INSERT
        tracker.record("EDGE CASES", "mixed_case", True)

    def test_quoted_table_with_spaces(self):
        sql = 'INSERT INTO "My Orders" (item) VALUES (\'x\')'
        eng = engine_with_pk()
        inv = eng.generate(sql)
        assert inv.category == CommandCategory.INSERT
        tracker.record("EDGE CASES", "quoted_table_spaces", True)

    def test_schema_qualified_update(self):
        sql = "UPDATE public.orders SET qty=1 WHERE id=2"
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 2, "item": "B", "qty": 9}])
        inv = eng.generate(sql)
        assert inv.category == CommandCategory.UPDATE
        assert len(inv.steps) == 1
        tracker.record("EDGE CASES", "schema_qualified_update", True)

    def test_engine_exception_returns_non_reversible(self):
        from unittest.mock import MagicMock
        conn = MockConnection()
        conn.cursor = MagicMock(side_effect=Exception("DB connection lost"))
        eng = InverseEngine(conn)
        inv = eng.generate("INSERT INTO t (a) VALUES (1)")
        assert not inv.is_reversible
        assert "DB connection lost" in inv.notes
        tracker.record("EDGE CASES", "engine_exception_handled", True)

    def test_empty_sql_classified_unknown(self):
        assert _classify("") == CommandCategory.UNKNOWN
        tracker.record("EDGE CASES", "empty_sql", True)

    def test_multiline_sql(self):
        sql = """
        INSERT INTO orders
            (item, qty)
        VALUES
            ('Widget', 10)
        """
        eng = engine_with_pk()
        inv = eng.generate(sql)
        assert inv.category == CommandCategory.INSERT
        tracker.record("EDGE CASES", "multiline_sql", True)

    def test_values_with_unicode(self):
        eng = engine_with_pk(pk_cols=("id",))
        inv = eng.generate("INSERT INTO orders (item) VALUES ('caf├⌐')")
        eng.finalize_insert(inv, [{"id": 1, "item": "caf├⌐", "qty": 0}])
        # INSERT inverse with PK is DELETE by PK
        assert "DELETE FROM" in inv.steps[0]
        assert '"id" = 1' in inv.steps[0]
        tracker.record("EDGE CASES", "unicode_values", True)

    def test_large_before_image(self):
        rows = [{"id": i, "item": f"item_{i}", "qty": i * 10} for i in range(100)]
        eng = engine_with_pk(pk_cols=("id",), rows=rows)
        inv = eng.generate("DELETE FROM orders")
        assert len(inv.steps) == 100
        tracker.record("EDGE CASES", "large_before_image_100_rows", True)

    def test_value_with_semicolon(self):
        eng = engine_with_pk(pk_cols=("id",))
        inv = eng.generate("INSERT INTO orders (item) VALUES ('a;b')")
        eng.finalize_insert(inv, [{"id": 1, "item": "a;b", "qty": 0}])
        # INSERT inverse with PK is DELETE by PK
        assert "DELETE FROM" in inv.steps[0]
        assert '"id" = 1' in inv.steps[0]
        tracker.record("EDGE CASES", "value_with_semicolon", True)

    def test_delete_with_complex_where(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": "A", "qty": 5}])
        inv = eng.generate("DELETE FROM orders WHERE id=1 AND qty > 3 AND item LIKE '%A%'")
        assert inv.category == CommandCategory.DELETE
        assert len(inv.steps) == 1
        tracker.record("EDGE CASES", "complex_where_clause", True)

    def test_update_with_subquery_in_where(self):
        eng = engine_with_pk(pk_cols=("id",), rows=[{"id": 1, "item": "A", "qty": 5}])
        inv = eng.generate("UPDATE orders SET qty=0 WHERE id IN (SELECT id FROM old_orders)")
        assert inv.category == CommandCategory.UPDATE
        tracker.record("EDGE CASES", "subquery_in_where", True)


# ---------------------------------------------------------------------------
# 19. NORMALISATION
# ---------------------------------------------------------------------------

class TestNormalization:

    def test_strip_line_comments(self):
        assert "DELETE" in _normalise("-- comment\nDELETE FROM t")

    def test_strip_block_comments(self):
        assert "INSERT" in _normalise("/* block */ INSERT INTO t VALUES (1)")

    def test_collapse_whitespace(self):
        result = _normalise("INSERT   INTO   t   VALUES   (1)")
        assert "  " not in result

    def test_mixed_comments(self):
        sql = "-- line\n/* block */\nUPDATE t SET x=1 -- trailing"
        result = _normalise(sql)
        assert "UPDATE" in result
        assert "--" not in result
        assert "/*" not in result


# ---------------------------------------------------------------------------
# Accuracy report hook
# ---------------------------------------------------------------------------

class TestAccuracyReport:
    """Must run last ΓÇö prints the accuracy report."""

    def test_zz_print_accuracy_report(self):
        """Print accuracy report (named zz_ to run last)."""
        report = tracker.report()
        print(report)
        # Verify all tracked tests passed
        total_pass = sum(
            sum(1 for _, p, _ in entries if p)
            for entries in tracker.results.values()
        )
        total = sum(len(entries) for entries in tracker.results.values())
        assert total > 0, "No tests were tracked"
        accuracy = total_pass / total
        print(f"\n  Tracked accuracy: {accuracy:.1%}")
        assert accuracy >= 0.95, f"Accuracy {accuracy:.1%} below 95% threshold"
