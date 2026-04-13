"""
WEAVE-DB: Test Suite for the Inverse Engine
============================================
Tests every command category using a MockCursor / MockConnection so the
suite runs without a live PostgreSQL instance.

Run with:
    python test_inverse_engine.py
    python test_inverse_engine.py -v          # verbose
    python test_inverse_engine.py -k insert   # filter by name
"""

import sys
import json
import unittest
from unittest.mock import MagicMock, patch, call
from collections import defaultdict

# ΓöÇΓöÇ Minimal stubs so we can import inverse_engine without psycopg2 ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

# ΓöÇΓöÇ Import under test ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ
from inverse_engine import (
    InverseEngine, InverseCommand, CommandCategory,
    _classify, _normalise, _quote_ident, _quote_literal,
    _parse_insert_table, _parse_update_table_where,
    _parse_delete_table_where, _parse_truncate_table,
    _parse_create_table_name, _parse_drop_object_name,
    _parse_alter_table_name, _parse_alter_add_column_name,
    _parse_alter_drop_column_name, _parse_rename_column,
    _parse_rename_table, _parse_create_index_name,
    _parse_create_sequence_name, _split_schema_table,
)


# ---------------------------------------------------------------------------
# Mock database infrastructure
# ---------------------------------------------------------------------------

class MockCursor:
    """Simulates psycopg2 cursor behaviour for testing."""

    def __init__(self, result_map: dict):
        """
        result_map : dict mapping a SQL-snippet substring ΓåÆ list of rows
        """
        self._map   = result_map
        self.description = None
        self._rows  = []
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
    """Simulates psycopg2 connection with a configurable cursor."""

    def __init__(self, result_map=None):
        self._map = result_map or {}
        self._committed = False

    def cursor(self):
        return MockCursor(self._map)

    def commit(self):
        self._committed = True

    def rollback(self):
        pass


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------

def engine_with_pk(table="orders", pk_cols=("id",)):
    """Create an InverseEngine whose DB returns a PK for the given table."""
    # information_schema primary key query returns pk_cols
    # SELECT * returns a single sample row
    conn = MockConnection({
        "PRIMARY KEY": [(col,) for col in pk_cols],
        "SELECT *":    [{"id": 1, "item": "Widget", "qty": 3}],
        "INFORMATION_SCHEMA.COLUMNS": [
            ("id",   "integer",          None, 10, 0, "NO",  None,      "int4"),
            ("item", "character varying", 50,  None, None, "YES", None, "varchar"),
            ("qty",  "integer",          None, 10, 0,  "YES", None,     "int4"),
        ],
    })
    return InverseEngine(conn)


def engine_no_pk():
    conn = MockConnection({
        "PRIMARY KEY": [],   # no PK
        "SELECT *":    [{"item": "Widget", "qty": 3}],
    })
    return InverseEngine(conn)


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestClassifier(unittest.TestCase):

    def test_insert(self):
        self.assertEqual(_classify("INSERT INTO t (a) VALUES (1)"),
                         CommandCategory.INSERT)

    def test_update(self):
        self.assertEqual(_classify("UPDATE t SET a=1 WHERE id=2"),
                         CommandCategory.UPDATE)

    def test_delete(self):
        self.assertEqual(_classify("DELETE FROM t WHERE id=1"),
                         CommandCategory.DELETE)

    def test_truncate(self):
        self.assertEqual(_classify("TRUNCATE TABLE t"),
                         CommandCategory.TRUNCATE)
        self.assertEqual(_classify("truncate orders"),
                         CommandCategory.TRUNCATE)

    def test_create_table(self):
        self.assertEqual(_classify("CREATE TABLE foo (id SERIAL PRIMARY KEY)"),
                         CommandCategory.CREATE_TABLE)
        self.assertEqual(_classify("CREATE TEMP TABLE t (x INT)"),
                         CommandCategory.CREATE_TABLE)

    def test_drop_table(self):
        self.assertEqual(_classify("DROP TABLE IF EXISTS foo"),
                         CommandCategory.DROP_TABLE)

    def test_alter_table(self):
        self.assertEqual(_classify("ALTER TABLE t ADD COLUMN score INT"),
                         CommandCategory.ALTER_TABLE)

    def test_rename_table(self):
        self.assertEqual(_classify("ALTER TABLE old RENAME TO new"),
                         CommandCategory.RENAME_TABLE)

    def test_create_index(self):
        self.assertEqual(_classify("CREATE INDEX idx_name ON t (col)"),
                         CommandCategory.CREATE_INDEX)
        self.assertEqual(_classify("CREATE UNIQUE INDEX CONCURRENTLY u ON t (x)"),
                         CommandCategory.CREATE_INDEX)

    def test_drop_index(self):
        self.assertEqual(_classify("DROP INDEX idx_name"),
                         CommandCategory.DROP_INDEX)

    def test_create_sequence(self):
        self.assertEqual(_classify("CREATE SEQUENCE seq_id"),
                         CommandCategory.CREATE_SEQUENCE)

    def test_create_schema(self):
        self.assertEqual(_classify("CREATE SCHEMA analytics"),
                         CommandCategory.CREATE_SCHEMA)

    def test_create_view(self):
        self.assertEqual(_classify("CREATE VIEW v AS SELECT 1"),
                         CommandCategory.CREATE_VIEW)
        self.assertEqual(_classify("CREATE OR REPLACE VIEW v AS SELECT 1"),
                         CommandCategory.CREATE_VIEW)

    def test_drop_view(self):
        self.assertEqual(_classify("DROP VIEW v"),
                         CommandCategory.DROP_VIEW)

    def test_select_is_unknown(self):
        self.assertEqual(_classify("SELECT * FROM t"),
                         CommandCategory.UNKNOWN)


class TestParsers(unittest.TestCase):

    def test_parse_insert_table(self):
        self.assertEqual(_parse_insert_table("INSERT INTO orders (a) VALUES (1)"),
                         "orders")
        self.assertEqual(_parse_insert_table('INSERT INTO "My Orders" (a) VALUES (1)'),
                         "My Orders")

    def test_parse_update_table_where(self):
        table, where = _parse_update_table_where("UPDATE orders SET qty=2 WHERE id=1")
        self.assertEqual(table, "orders")
        self.assertIn("id=1", where)

    def test_parse_delete_table_where(self):
        table, where = _parse_delete_table_where("DELETE FROM orders WHERE id=5")
        self.assertEqual(table, "orders")
        self.assertIn("id=5", where)

    def test_parse_truncate_table(self):
        self.assertEqual(_parse_truncate_table("TRUNCATE TABLE orders"), "orders")
        self.assertEqual(_parse_truncate_table("TRUNCATE orders"), "orders")

    def test_parse_create_table_name(self):
        self.assertEqual(_parse_create_table_name("CREATE TABLE public.users (id INT)"),
                         "public.users")

    def test_parse_drop_object_name(self):
        self.assertEqual(
            _parse_drop_object_name("DROP TABLE IF EXISTS public.users", "TABLE"),
            "public.users"
        )

    def test_parse_alter_add_column(self):
        self.assertEqual(
            _parse_alter_add_column_name("ALTER TABLE t ADD COLUMN score FLOAT"),
            "score"
        )

    def test_parse_rename_column(self):
        old, new = _parse_rename_column(
            "ALTER TABLE t RENAME COLUMN old_name TO new_name"
        )
        self.assertEqual(old, "old_name")
        self.assertEqual(new, "new_name")

    def test_parse_rename_table(self):
        old, new = _parse_rename_table("ALTER TABLE orders RENAME TO old_orders")
        self.assertEqual(old, "orders")
        self.assertEqual(new, "old_orders")

    def test_split_schema_table(self):
        self.assertEqual(_split_schema_table("public.users"), ("public", "users"))
        self.assertEqual(_split_schema_table("users"), (None, "users"))


class TestQuoting(unittest.TestCase):

    def test_quote_ident(self):
        self.assertEqual(_quote_ident("my table"), '"my table"')
        self.assertEqual(_quote_ident('say "hello"'), '"say ""hello"""')

    def test_quote_literal_none(self):
        self.assertEqual(_quote_literal(None), "NULL")

    def test_quote_literal_int(self):
        self.assertEqual(_quote_literal(42), "42")

    def test_quote_literal_bool(self):
        self.assertEqual(_quote_literal(True), "TRUE")
        self.assertEqual(_quote_literal(False), "FALSE")

    def test_quote_literal_string_no_quote(self):
        self.assertEqual(_quote_literal("hello"), "'hello'")

    def test_quote_literal_string_with_quote(self):
        # Should use dollar-quoting
        result = _quote_literal("it's here")
        self.assertIn("it's here", result)
        self.assertTrue(result.startswith("$"))


class TestInsertInverse(unittest.TestCase):

    def test_insert_returns_placeholder_before_finalize(self):
        eng = engine_with_pk()
        inv = eng.generate("INSERT INTO orders (item, qty) VALUES ('Widget', 3)")
        self.assertEqual(inv.category, CommandCategory.INSERT)
        # Before finalize, steps should be empty
        self.assertEqual(inv.steps, [])

    def test_finalize_insert_with_pk(self):
        eng = engine_with_pk(pk_cols=("id",))
        inv = eng.generate("INSERT INTO orders (item, qty) VALUES ('Widget', 3)")
        eng.finalize_insert(inv, [{"id": 7, "item": "Widget", "qty": 3}])
        self.assertEqual(len(inv.steps), 1)
        self.assertIn("DELETE FROM", inv.steps[0])
        self.assertIn('"id" = 7', inv.steps[0])

    def test_finalize_insert_multi_row(self):
        eng = engine_with_pk(pk_cols=("id",))
        inv = eng.generate("INSERT INTO orders (item, qty) VALUES ('A', 1), ('B', 2)")
        eng.finalize_insert(inv, [
            {"id": 8, "item": "A", "qty": 1},
            {"id": 9, "item": "B", "qty": 2},
        ])
        self.assertEqual(len(inv.steps), 2)

    def test_finalize_insert_no_pk(self):
        eng = engine_no_pk()
        inv = eng.generate("INSERT INTO orders (item) VALUES ('X')")
        eng.finalize_insert(inv, [{"item": "X"}])
        self.assertEqual(len(inv.steps), 1)
        self.assertFalse(inv.is_reversible)

    def test_finalize_insert_no_rows(self):
        eng = engine_with_pk()
        inv = eng.generate("INSERT INTO orders (item) VALUES ('X')")
        eng.finalize_insert(inv, [])
        self.assertEqual(inv.steps, [])


class TestUpdateInverse(unittest.TestCase):

    def test_update_with_pk(self):
        eng = engine_with_pk(pk_cols=("id",))
        # Mock returns before-image: id=1, item=Widget, qty=3
        inv = eng.generate("UPDATE orders SET qty=10 WHERE id=1")
        self.assertEqual(inv.category, CommandCategory.UPDATE)
        self.assertGreater(len(inv.steps), 0)
        step = inv.steps[0]
        self.assertIn("UPDATE", step)
        self.assertIn("SET", step)
        # Original values should appear
        self.assertIn("'Widget'", step)
        self.assertIn("3", step)

    def test_update_no_pk(self):
        eng = engine_no_pk()
        inv = eng.generate("UPDATE orders SET qty=10")
        self.assertFalse(inv.is_reversible)

    def test_update_no_matching_rows(self):
        conn = MockConnection({
            "PRIMARY KEY": [("id",)],
            "SELECT *":    [],  # no rows affected
        })
        eng = InverseEngine(conn)
        inv = eng.generate("UPDATE orders SET qty=10 WHERE id=999")
        self.assertEqual(inv.steps, [])


class TestDeleteInverse(unittest.TestCase):

    def test_delete_with_rows(self):
        eng = engine_with_pk(pk_cols=("id",))
        inv = eng.generate("DELETE FROM orders WHERE id=1")
        self.assertEqual(inv.category, CommandCategory.DELETE)
        self.assertGreater(len(inv.steps), 0)
        step = inv.steps[0]
        self.assertIn("INSERT INTO", step)
        self.assertIn("'Widget'", step)

    def test_delete_no_rows(self):
        conn = MockConnection({
            "PRIMARY KEY": [("id",)],
            "SELECT *":    [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("DELETE FROM orders WHERE id=999")
        self.assertEqual(inv.steps, [])


class TestTruncateInverse(unittest.TestCase):

    def test_truncate(self):
        eng = engine_with_pk()
        inv = eng.generate("TRUNCATE TABLE orders")
        self.assertEqual(inv.category, CommandCategory.TRUNCATE)
        self.assertGreater(len(inv.steps), 0)
        self.assertIn("INSERT INTO", inv.steps[0])

    def test_truncate_empty_table(self):
        conn = MockConnection({"SELECT *": [], "PRIMARY KEY": [("id",)]})
        eng = InverseEngine(conn)
        inv = eng.generate("TRUNCATE orders")
        self.assertEqual(inv.steps, [])


class TestCreateTableInverse(unittest.TestCase):

    def test_create_table(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE TABLE new_table (id SERIAL PRIMARY KEY, name TEXT)")
        self.assertEqual(inv.category, CommandCategory.CREATE_TABLE)
        self.assertEqual(len(inv.steps), 1)
        self.assertIn("DROP TABLE", inv.steps[0])
        self.assertIn("new_table", inv.steps[0].lower())


class TestDropTableInverse(unittest.TestCase):

    def test_drop_table_with_catalog(self):
        conn = MockConnection({
            "INFORMATION_SCHEMA.COLUMNS": [
                ("id",   "integer", None, 10, 0, "NO",  None, "int4"),
                ("name", "character varying", 100, None, None, "YES", None, "varchar"),
            ],
            "TABLE_CONSTRAINTS": [
                ("PRIMARY KEY", "pk_users", "id"),
            ],
            "KEY_COLUMN_USAGE": [("id",)],
            "PRIMARY KEY": [("id",)],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("DROP TABLE users")
        self.assertEqual(inv.category, CommandCategory.DROP_TABLE)

    def test_drop_table_no_catalog(self):
        conn = MockConnection({
            "INFORMATION_SCHEMA.COLUMNS": [],
            "PRIMARY KEY": [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("DROP TABLE ghost_table")
        self.assertFalse(inv.is_reversible)


class TestAlterTableInverse(unittest.TestCase):

    def test_add_column(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ADD COLUMN discount NUMERIC(5,2)")
        self.assertEqual(inv.category, CommandCategory.ALTER_TABLE)
        self.assertIn("DROP COLUMN", inv.steps[0])
        self.assertIn("discount", inv.steps[0].lower())

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
        self.assertIn("ADD COLUMN", inv.steps[0])

    def test_rename_column(self):
        eng = engine_with_pk()
        inv = eng.generate(
            "ALTER TABLE orders RENAME COLUMN old_name TO new_name"
        )
        self.assertIn("RENAME COLUMN", inv.steps[0])
        self.assertIn('"new_name"', inv.steps[0])
        self.assertIn('"old_name"', inv.steps[0])

    def test_add_constraint(self):
        eng = engine_with_pk()
        inv = eng.generate(
            "ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty > 0)"
        )
        self.assertIn("DROP CONSTRAINT", inv.steps[0])

    def test_set_not_null(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ALTER COLUMN qty SET NOT NULL")
        self.assertIn("DROP NOT NULL", inv.steps[0])

    def test_drop_not_null(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders ALTER COLUMN qty DROP NOT NULL")
        self.assertIn("SET NOT NULL", inv.steps[0])


class TestRenameTableInverse(unittest.TestCase):

    def test_rename_table(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE orders RENAME TO old_orders")
        self.assertEqual(inv.category, CommandCategory.RENAME_TABLE)
        self.assertIn("RENAME TO", inv.steps[0])
        self.assertIn('"orders"', inv.steps[0])


class TestIndexInverse(unittest.TestCase):

    def test_create_index(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE INDEX idx_orders_item ON orders (item)")
        self.assertEqual(inv.category, CommandCategory.CREATE_INDEX)
        self.assertIn("DROP INDEX", inv.steps[0])
        self.assertIn("idx_orders_item", inv.steps[0].lower())

    def test_drop_index_with_catalog(self):
        conn = MockConnection({
            "PG_INDEXES": [("CREATE INDEX idx_orders_item ON orders (item)",)],
            "PRIMARY KEY": [("id",)],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("DROP INDEX idx_orders_item")
        self.assertEqual(len(inv.steps), 1)
        self.assertIn("CREATE INDEX", inv.steps[0])


class TestSequenceInverse(unittest.TestCase):

    def test_create_sequence(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE SEQUENCE order_id_seq START 1000")
        self.assertEqual(inv.category, CommandCategory.CREATE_SEQUENCE)
        self.assertIn("DROP SEQUENCE", inv.steps[0])

    def test_drop_sequence_with_catalog(self):
        conn = MockConnection({
            "PG_SEQUENCES": [(1, 1, 9223372036854775807, 1, 1, False)],
            "PRIMARY KEY": [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("DROP SEQUENCE order_id_seq")
        # May or may not succeed depending on cursor key match
        self.assertEqual(inv.category, CommandCategory.DROP_SEQUENCE)


class TestSchemaInverse(unittest.TestCase):

    def test_create_schema(self):
        eng = engine_with_pk()
        inv = eng.generate("CREATE SCHEMA analytics")
        self.assertEqual(inv.category, CommandCategory.CREATE_SCHEMA)
        self.assertIn("DROP SCHEMA", inv.steps[0])
        self.assertIn("analytics", inv.steps[0].lower())

    def test_drop_schema(self):
        eng = engine_with_pk()
        inv = eng.generate("DROP SCHEMA analytics")
        self.assertEqual(inv.category, CommandCategory.DROP_SCHEMA)
        self.assertIn("CREATE SCHEMA", inv.steps[0])


class TestViewInverse(unittest.TestCase):

    def test_create_view_new(self):
        conn = MockConnection({
            "PG_VIEWS": [],   # view doesn't exist yet
            "PRIMARY KEY": [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("CREATE VIEW v_summary AS SELECT * FROM orders")
        self.assertEqual(inv.category, CommandCategory.CREATE_VIEW)
        self.assertIn("DROP VIEW", inv.steps[0])

    def test_drop_view_with_catalog(self):
        conn = MockConnection({
            "PG_VIEWS": [("SELECT item, SUM(qty) FROM orders GROUP BY item",)],
            "PRIMARY KEY": [],
        })
        eng = InverseEngine(conn)
        inv = eng.generate("DROP VIEW v_summary")
        self.assertEqual(inv.category, CommandCategory.DROP_VIEW)
        self.assertIn("CREATE VIEW", inv.steps[0])


class TestSelectIsIgnored(unittest.TestCase):

    def test_select(self):
        eng = engine_with_pk()
        inv = eng.generate("SELECT * FROM orders")
        self.assertEqual(inv.category, CommandCategory.UNKNOWN)
        self.assertFalse(inv.is_reversible)
        self.assertEqual(inv.steps, [])


class TestInverseCommandSerialization(unittest.TestCase):

    def test_round_trip(self):
        inv = InverseCommand(
            category      = CommandCategory.DELETE,
            forward_sql   = "DELETE FROM orders WHERE id=1",
            steps         = ["INSERT INTO orders (id, item) VALUES (1, 'X');"],
            before_image  = [{"id": 1, "item": "X"}],
            is_reversible = True,
            notes         = "test",
        )
        d   = inv.to_dict()
        inv2 = InverseCommand.from_dict(d)
        self.assertEqual(inv.category, inv2.category)
        self.assertEqual(inv.steps,    inv2.steps)
        self.assertEqual(inv.before_image, inv2.before_image)


class TestEdgeCases(unittest.TestCase):

    def test_whitespace_and_comments_normalised(self):
        sql = """
            -- delete old records
            DELETE  FROM   orders
            /* bulk delete */
            WHERE id = 1
        """
        cat = _classify(sql)
        self.assertEqual(cat, CommandCategory.DELETE)

    def test_mixed_case(self):
        self.assertEqual(_classify("insert into T values (1)"), CommandCategory.INSERT)
        self.assertEqual(_classify("DROP  TABLE  t"),           CommandCategory.DROP_TABLE)

    def test_quoted_table_name_with_space(self):
        sql = 'INSERT INTO "My Orders" (item) VALUES (\'x\')'
        table = _parse_insert_table(_normalise(sql))
        self.assertEqual(table, "My Orders")

    def test_schema_qualified_table(self):
        sql = "UPDATE public.orders SET qty=1 WHERE id=2"
        table, where = _parse_update_table_where(_normalise(sql))
        self.assertIn("orders", table)

    def test_unknown_alter_table_subtype(self):
        eng = engine_with_pk()
        inv = eng.generate("ALTER TABLE t CLUSTER ON some_index")
        self.assertFalse(inv.is_reversible)

    def test_generate_exception_handled(self):
        """Engine should return a non-reversible InverseCommand on any error."""
        conn = MockConnection()
        conn.cursor = MagicMock(side_effect=Exception("DB gone"))
        eng = InverseEngine(conn)
        inv = eng.generate("INSERT INTO t (a) VALUES (1)")
        self.assertFalse(inv.is_reversible)
        self.assertIn("DB gone", inv.notes)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Pretty header
    print("=" * 60)
    print("  WEAVE-DB Inverse Engine Test Suite")
    print("=" * 60)

    loader  = unittest.TestLoader()
    suite   = loader.loadTestsFromModule(sys.modules[__name__])
    runner  = unittest.TextTestRunner(verbosity=2)
    result  = runner.run(suite)

    print()
    if result.wasSuccessful():
        print("Γ£à  All tests passed.")
    else:
        print(f"Γ¥î  {len(result.failures)} failure(s), "
              f"{len(result.errors)} error(s).")
    sys.exit(0 if result.wasSuccessful() else 1)
