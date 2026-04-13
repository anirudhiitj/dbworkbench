"""Real-database accuracy tests for the Inverse Engine.

Connects to a live PostgreSQL instance, executes forward SQL,
generates inverses, applies them, and verifies the database
state is correctly restored.

Run with:
    python -m pytest tests/test_inverse_engine_real_db.py -v -s
"""

import os
import psycopg2
import psycopg2.extras
import pytest

from fastapi_backend.app.services.inverse_engine import InverseEngine, CommandCategory


# ---------------------------------------------------------------------------
# Connection + fixtures
# ---------------------------------------------------------------------------

DB_PARAMS = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "dbname": os.environ.get("TEST_DB_NAME", "weavedb_inverse_test"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "Parrva2396"),
}


def _admin_conn():
    """Connect to the default 'postgres' database for admin operations."""
    params = {**DB_PARAMS, "dbname": "postgres"}
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    return conn


def _test_conn():
    """Connect to the test database."""
    return psycopg2.connect(**DB_PARAMS)


# ---------------------------------------------------------------------------
# Session-scoped: create/drop the test database
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def create_test_database():
    """Create a fresh test database, drop it when done."""
    dbname = DB_PARAMS["dbname"]
    admin = _admin_conn()
    cur = admin.cursor()

    # Drop if leftover from previous run
    cur.execute(f"DROP DATABASE IF EXISTS {dbname}")
    cur.execute(f"CREATE DATABASE {dbname}")
    cur.close()
    admin.close()

    yield

    # Teardown ΓÇö terminate lingering connections before dropping
    admin = _admin_conn()
    cur = admin.cursor()
    cur.execute(f"""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = '{dbname}' AND pid <> pg_backend_pid()
    """)
    cur.execute(f"DROP DATABASE IF EXISTS {dbname}")
    cur.close()
    admin.close()


@pytest.fixture(autouse=True)
def clean_tables():
    """Drop all user tables before each test for isolation."""
    conn = _test_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        DO $$ DECLARE r RECORD;
        BEGIN
            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
            END LOOP;
        END $$;
    """)
    # Also drop sequences, views, schemas (except public)
    cur.execute("""
        DO $$ DECLARE r RECORD;
        BEGIN
            FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public') LOOP
                EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(r.sequence_name) || ' CASCADE';
            END LOOP;
        END $$;
    """)
    cur.execute("""
        DO $$ DECLARE r RECORD;
        BEGIN
            FOR r IN (SELECT viewname FROM pg_views WHERE schemaname = 'public') LOOP
                EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident(r.viewname) || ' CASCADE';
            END LOOP;
        END $$;
    """)
    cur.close()
    conn.close()
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def snapshot_table(conn, table):
    """Return all rows from a table as a sorted list of dicts."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"SELECT * FROM {table} ORDER BY 1")
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    return rows


def table_exists(conn, table):
    cur = conn.cursor()
    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table,))
    exists = cur.fetchone()[0]
    cur.close()
    return exists


def column_exists(conn, table, column):
    cur = conn.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
        )
    """, (table, column))
    exists = cur.fetchone()[0]
    cur.close()
    return exists


def get_column_type(conn, table, column):
    cur = conn.cursor()
    cur.execute("""
        SELECT data_type FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
    """, (table, column))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def index_exists(conn, index_name):
    cur = conn.cursor()
    cur.execute("SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = %s)", (index_name,))
    exists = cur.fetchone()[0]
    cur.close()
    return exists


def view_exists(conn, view_name):
    cur = conn.cursor()
    cur.execute("SELECT EXISTS (SELECT 1 FROM pg_views WHERE viewname = %s)", (view_name,))
    exists = cur.fetchone()[0]
    cur.close()
    return exists


def sequence_exists(conn, seq_name):
    cur = conn.cursor()
    cur.execute("SELECT EXISTS (SELECT 1 FROM pg_sequences WHERE sequencename = %s)", (seq_name,))
    exists = cur.fetchone()[0]
    cur.close()
    return exists


def execute_steps(conn, steps):
    """Execute a list of SQL steps."""
    cur = conn.cursor()
    for step in steps:
        cur.execute(step)
    conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
# Accuracy tracker
# ---------------------------------------------------------------------------

results = []  # (category, test_name, passed, detail)


def record(cat, name, passed, detail=""):
    results.append((cat, name, passed, detail))


# ---------------------------------------------------------------------------
# DML: INSERT
# ---------------------------------------------------------------------------

class TestInsertRealDB:

    def test_insert_single_row(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50), qty INT)")
        conn.commit()

        engine = InverseEngine(conn)
        sql = "INSERT INTO orders (item, qty) VALUES ('Widget', 3)"
        inv = engine.generate(sql)

        # Execute with RETURNING *
        cur.execute(sql + " RETURNING *")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        returned = [dict(zip(cols, r)) for r in rows]
        conn.commit()
        engine.finalize_insert(inv, returned)

        before = snapshot_table(conn, "orders")
        assert len(before) == 1

        # Apply inverse
        execute_steps(conn, inv.steps)
        after = snapshot_table(conn, "orders")
        assert len(after) == 0
        record("INSERT", "single_row", True)
        conn.close()

    def test_insert_multi_row(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50), qty INT)")
        conn.commit()

        engine = InverseEngine(conn)
        sql = "INSERT INTO orders (item, qty) VALUES ('A', 1), ('B', 2), ('C', 3)"
        inv = engine.generate(sql)

        cur.execute(sql + " RETURNING *")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        returned = [dict(zip(cols, r)) for r in rows]
        conn.commit()
        engine.finalize_insert(inv, returned)

        assert len(snapshot_table(conn, "orders")) == 3
        execute_steps(conn, inv.steps)
        assert len(snapshot_table(conn, "orders")) == 0
        record("INSERT", "multi_row", True)
        conn.close()

    def test_insert_with_nulls(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50), qty INT)")
        conn.commit()

        engine = InverseEngine(conn)
        sql = "INSERT INTO orders (item, qty) VALUES (NULL, NULL)"
        inv = engine.generate(sql)

        cur.execute(sql + " RETURNING *")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        returned = [dict(zip(cols, r)) for r in rows]
        conn.commit()
        engine.finalize_insert(inv, returned)

        execute_steps(conn, inv.steps)
        assert len(snapshot_table(conn, "orders")) == 0
        record("INSERT", "with_nulls", True)
        conn.close()

    def test_insert_special_characters(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(100), qty INT)")
        conn.commit()

        engine = InverseEngine(conn)
        sql = "INSERT INTO orders (item, qty) VALUES ('O''Brien & Sons', 5)"
        inv = engine.generate(sql)

        cur.execute(sql + " RETURNING *")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        returned = [dict(zip(cols, r)) for r in rows]
        conn.commit()
        engine.finalize_insert(inv, returned)

        execute_steps(conn, inv.steps)
        assert len(snapshot_table(conn, "orders")) == 0
        record("INSERT", "special_characters", True)
        conn.close()

    def test_insert_no_pk_table(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE logs (msg TEXT, level INT)")
        conn.commit()

        engine = InverseEngine(conn)
        sql = "INSERT INTO logs (msg, level) VALUES ('hello', 1)"
        inv = engine.generate(sql)

        cur.execute(sql + " RETURNING *")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        returned = [dict(zip(cols, r)) for r in rows]
        conn.commit()
        engine.finalize_insert(inv, returned)

        execute_steps(conn, inv.steps)
        assert len(snapshot_table(conn, "logs")) == 0
        record("INSERT", "no_pk_table", True)
        conn.close()

    def test_insert_composite_pk(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE order_lines (order_id INT, line_id INT, product VARCHAR(50), qty INT, PRIMARY KEY(order_id, line_id))")
        conn.commit()

        engine = InverseEngine(conn)
        sql = "INSERT INTO order_lines (order_id, line_id, product, qty) VALUES (1, 10, 'Widget', 5)"
        inv = engine.generate(sql)

        cur.execute(sql + " RETURNING *")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        returned = [dict(zip(cols, r)) for r in rows]
        conn.commit()
        engine.finalize_insert(inv, returned)

        execute_steps(conn, inv.steps)
        assert len(snapshot_table(conn, "order_lines")) == 0
        record("INSERT", "composite_pk", True)
        conn.close()


# ---------------------------------------------------------------------------
# DML: UPDATE
# ---------------------------------------------------------------------------

class TestUpdateRealDB:

    def _setup_orders(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50), qty INT)")
        cur.execute("INSERT INTO orders (item, qty) VALUES ('Widget', 3), ('Gadget', 7), ('Doohickey', 1)")
        conn.commit()
        cur.close()

    def test_update_single_row(self):
        conn = _test_conn()
        self._setup_orders(conn)
        before = snapshot_table(conn, "orders")

        engine = InverseEngine(conn)
        sql = "UPDATE orders SET qty = 99 WHERE id = 1"
        inv = engine.generate(sql)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

        # Verify update happened
        after_update = snapshot_table(conn, "orders")
        assert after_update[0]["qty"] == 99

        # Apply inverse
        execute_steps(conn, inv.steps)
        restored = snapshot_table(conn, "orders")
        assert restored == before
        record("UPDATE", "single_row", True)
        conn.close()

    def test_update_multiple_rows(self):
        conn = _test_conn()
        self._setup_orders(conn)
        before = snapshot_table(conn, "orders")

        engine = InverseEngine(conn)
        sql = "UPDATE orders SET qty = 0"
        inv = engine.generate(sql)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

        execute_steps(conn, inv.steps)
        restored = snapshot_table(conn, "orders")
        assert restored == before
        record("UPDATE", "multiple_rows", True)
        conn.close()

    def test_update_multiple_columns(self):
        conn = _test_conn()
        self._setup_orders(conn)
        before = snapshot_table(conn, "orders")

        engine = InverseEngine(conn)
        sql = "UPDATE orders SET item = 'Changed', qty = 999 WHERE id = 2"
        inv = engine.generate(sql)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

        execute_steps(conn, inv.steps)
        restored = snapshot_table(conn, "orders")
        assert restored == before
        record("UPDATE", "multiple_columns", True)
        conn.close()

    def test_update_to_null(self):
        conn = _test_conn()
        self._setup_orders(conn)
        before = snapshot_table(conn, "orders")

        engine = InverseEngine(conn)
        sql = "UPDATE orders SET item = NULL WHERE id = 1"
        inv = engine.generate(sql)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

        execute_steps(conn, inv.steps)
        restored = snapshot_table(conn, "orders")
        assert restored == before
        record("UPDATE", "to_null", True)
        conn.close()

    def test_update_no_matching_rows(self):
        conn = _test_conn()
        self._setup_orders(conn)
        before = snapshot_table(conn, "orders")

        engine = InverseEngine(conn)
        sql = "UPDATE orders SET qty = 0 WHERE id = 9999"
        inv = engine.generate(sql)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

        # No rows affected, inverse should be empty
        assert inv.steps == []
        after = snapshot_table(conn, "orders")
        assert after == before
        record("UPDATE", "no_matching_rows", True)
        conn.close()


# ---------------------------------------------------------------------------
# DML: DELETE
# ---------------------------------------------------------------------------

class TestDeleteRealDB:

    def _setup_orders(self, conn):
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50), qty INT)")
        cur.execute("INSERT INTO orders (item, qty) VALUES ('Widget', 3), ('Gadget', 7), ('Doohickey', 1)")
        conn.commit()
        cur.close()

    def test_delete_single_row(self):
        conn = _test_conn()
        self._setup_orders(conn)
        before = snapshot_table(conn, "orders")

        engine = InverseEngine(conn)
        sql = "DELETE FROM orders WHERE id = 2"
        inv = engine.generate(sql)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        assert len(snapshot_table(conn, "orders")) == 2

        execute_steps(conn, inv.steps)
        restored = snapshot_table(conn, "orders")
        assert len(restored) == 3
        # Verify the deleted row is back
        items = {r["item"] for r in restored}
        assert "Gadget" in items
        record("DELETE", "single_row", True)
        conn.close()

    def test_delete_multiple_rows(self):
        conn = _test_conn()
        self._setup_orders(conn)
        before = snapshot_table(conn, "orders")

        engine = InverseEngine(conn)
        sql = "DELETE FROM orders WHERE qty < 5"
        inv = engine.generate(sql)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

        execute_steps(conn, inv.steps)
        restored = snapshot_table(conn, "orders")
        assert len(restored) == len(before)
        record("DELETE", "multiple_rows", True)
        conn.close()

    def test_delete_all_rows(self):
        conn = _test_conn()
        self._setup_orders(conn)
        before = snapshot_table(conn, "orders")

        engine = InverseEngine(conn)
        sql = "DELETE FROM orders"
        inv = engine.generate(sql)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        assert len(snapshot_table(conn, "orders")) == 0

        execute_steps(conn, inv.steps)
        restored = snapshot_table(conn, "orders")
        assert len(restored) == 3
        record("DELETE", "all_rows", True)
        conn.close()

    def test_delete_no_matching_rows(self):
        conn = _test_conn()
        self._setup_orders(conn)

        engine = InverseEngine(conn)
        sql = "DELETE FROM orders WHERE id = 9999"
        inv = engine.generate(sql)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

        assert inv.steps == []
        assert len(snapshot_table(conn, "orders")) == 3
        record("DELETE", "no_matching_rows", True)
        conn.close()


# ---------------------------------------------------------------------------
# DML: TRUNCATE
# ---------------------------------------------------------------------------

class TestTruncateRealDB:

    def test_truncate_restores_all_rows(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50), qty INT)")
        cur.execute("INSERT INTO orders (item, qty) VALUES ('A', 1), ('B', 2), ('C', 3)")
        conn.commit()

        before = snapshot_table(conn, "orders")
        engine = InverseEngine(conn)
        inv = engine.generate("TRUNCATE TABLE orders")

        cur.execute("TRUNCATE TABLE orders")
        conn.commit()
        assert len(snapshot_table(conn, "orders")) == 0

        execute_steps(conn, inv.steps)
        restored = snapshot_table(conn, "orders")
        assert len(restored) == 3
        # Values should match (order may differ due to no ORDER guarantee)
        orig_items = sorted([r["item"] for r in before])
        rest_items = sorted([r["item"] for r in restored])
        assert orig_items == rest_items
        record("TRUNCATE", "restores_all_rows", True)
        conn.close()

    def test_truncate_empty_table(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50))")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("TRUNCATE TABLE orders")

        cur.execute("TRUNCATE TABLE orders")
        conn.commit()

        assert inv.steps == []
        record("TRUNCATE", "empty_table", True)
        conn.close()


# ---------------------------------------------------------------------------
# DDL: CREATE TABLE / DROP TABLE
# ---------------------------------------------------------------------------

class TestCreateDropTableRealDB:

    def test_create_table_inverse_drops(self):
        conn = _test_conn()
        engine = InverseEngine(conn)
        sql = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email VARCHAR(200))"
        inv = engine.generate(sql)

        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        assert table_exists(conn, "users")

        execute_steps(conn, inv.steps)
        assert not table_exists(conn, "users")
        record("CREATE TABLE", "inverse_drops", True)
        conn.close()

    def test_drop_table_inverse_recreates(self):
        """DROP TABLE inverse recreates structure.

        Known limitation: SERIAL columns reference a sequence (e.g. users_id_seq)
        that is dropped with the table. The reconstructed DDL references
        nextval('users_id_seq') which no longer exists.
        Use plain INTEGER PK to avoid this.
        """
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, active BOOLEAN DEFAULT TRUE)")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("DROP TABLE users")

        cur.execute("DROP TABLE users")
        conn.commit()
        assert not table_exists(conn, "users")

        execute_steps(conn, inv.steps)
        assert table_exists(conn, "users")
        assert column_exists(conn, "users", "id")
        assert column_exists(conn, "users", "name")
        assert column_exists(conn, "users", "active")
        record("DROP TABLE", "inverse_recreates", True)
        conn.close()

    def test_drop_table_serial_pk_limitation(self):
        """Known limitation: DROP TABLE with SERIAL PK fails to recreate because the
        auto-created sequence is also dropped and the inverse DDL references it."""
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("DROP TABLE users")

        cur.execute("DROP TABLE users")
        conn.commit()

        # This should fail because users_id_seq was dropped with the table
        with pytest.raises(psycopg2.errors.UndefinedTable):
            execute_steps(conn, inv.steps)
        conn.rollback()
        record("DROP TABLE", "serial_pk_limitation", False,
               "SERIAL sequence dropped with table; inverse DDL references missing sequence")
        conn.close()


# ---------------------------------------------------------------------------
# DDL: ALTER TABLE
# ---------------------------------------------------------------------------

class TestAlterTableRealDB:

    def test_add_column_inverse_drops(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50))")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("ALTER TABLE orders ADD COLUMN discount NUMERIC(5,2)")

        cur.execute("ALTER TABLE orders ADD COLUMN discount NUMERIC(5,2)")
        conn.commit()
        assert column_exists(conn, "orders", "discount")

        execute_steps(conn, inv.steps)
        assert not column_exists(conn, "orders", "discount")
        record("ALTER TABLE", "add_column_inverse_drops", True)
        conn.close()

    def test_drop_column_inverse_adds(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50), discount NUMERIC(5,2))")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("ALTER TABLE orders DROP COLUMN discount")

        cur.execute("ALTER TABLE orders DROP COLUMN discount")
        conn.commit()
        assert not column_exists(conn, "orders", "discount")

        execute_steps(conn, inv.steps)
        assert column_exists(conn, "orders", "discount")
        record("ALTER TABLE", "drop_column_inverse_adds", True)
        conn.close()

    def test_rename_column(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, old_name VARCHAR(50))")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("ALTER TABLE orders RENAME COLUMN old_name TO new_name")

        cur.execute("ALTER TABLE orders RENAME COLUMN old_name TO new_name")
        conn.commit()
        assert column_exists(conn, "orders", "new_name")
        assert not column_exists(conn, "orders", "old_name")

        execute_steps(conn, inv.steps)
        assert column_exists(conn, "orders", "old_name")
        assert not column_exists(conn, "orders", "new_name")
        record("ALTER TABLE", "rename_column", True)
        conn.close()

    def test_alter_column_type(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, qty INTEGER)")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("ALTER TABLE orders ALTER COLUMN qty TYPE BIGINT")

        cur.execute("ALTER TABLE orders ALTER COLUMN qty TYPE BIGINT")
        conn.commit()
        assert get_column_type(conn, "orders", "qty") == "bigint"

        execute_steps(conn, inv.steps)
        assert get_column_type(conn, "orders", "qty") == "integer"
        record("ALTER TABLE", "alter_column_type", True)
        conn.close()

    def test_add_constraint(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, qty INT)")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty >= 0)")

        cur.execute("ALTER TABLE orders ADD CONSTRAINT chk_qty CHECK (qty >= 0)")
        conn.commit()

        execute_steps(conn, inv.steps)
        # Verify constraint is gone ΓÇö inserting negative should work now
        cur = conn.cursor()
        cur.execute("INSERT INTO orders (qty) VALUES (-1)")
        conn.commit()
        record("ALTER TABLE", "add_constraint_inverse_drops", True)
        conn.close()

    def test_drop_constraint(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, qty INT, CONSTRAINT chk_qty CHECK (qty >= 0))")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("ALTER TABLE orders DROP CONSTRAINT chk_qty")

        cur.execute("ALTER TABLE orders DROP CONSTRAINT chk_qty")
        conn.commit()

        execute_steps(conn, inv.steps)
        # Constraint should be back ΓÇö inserting negative should fail
        cur = conn.cursor()
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute("INSERT INTO orders (qty) VALUES (-1)")
        conn.rollback()
        record("ALTER TABLE", "drop_constraint_inverse_adds", True)
        conn.close()

    def test_set_not_null(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, qty INT)")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("ALTER TABLE orders ALTER COLUMN qty SET NOT NULL")

        cur.execute("ALTER TABLE orders ALTER COLUMN qty SET NOT NULL")
        conn.commit()

        execute_steps(conn, inv.steps)
        # NOT NULL removed ΓÇö inserting NULL should work
        cur = conn.cursor()
        cur.execute("INSERT INTO orders (qty) VALUES (NULL)")
        conn.commit()
        record("ALTER TABLE", "set_not_null_inverse", True)
        conn.close()

    def test_drop_not_null(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, qty INT NOT NULL)")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("ALTER TABLE orders ALTER COLUMN qty DROP NOT NULL")

        cur.execute("ALTER TABLE orders ALTER COLUMN qty DROP NOT NULL")
        conn.commit()

        execute_steps(conn, inv.steps)
        # NOT NULL restored ΓÇö inserting NULL should fail
        cur = conn.cursor()
        with pytest.raises(psycopg2.errors.NotNullViolation):
            cur.execute("INSERT INTO orders (qty) VALUES (NULL)")
        conn.rollback()
        record("ALTER TABLE", "drop_not_null_inverse", True)
        conn.close()

    def test_set_default(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, qty INT)")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("ALTER TABLE orders ALTER COLUMN qty SET DEFAULT 10")

        cur.execute("ALTER TABLE orders ALTER COLUMN qty SET DEFAULT 10")
        conn.commit()

        execute_steps(conn, inv.steps)
        # Default removed ΓÇö insert without qty should give NULL
        cur = conn.cursor()
        cur.execute("INSERT INTO orders DEFAULT VALUES")
        conn.commit()
        rows = snapshot_table(conn, "orders")
        assert rows[0]["qty"] is None
        record("ALTER TABLE", "set_default_inverse", True)
        conn.close()

    def test_rename_table(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY)")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("ALTER TABLE orders RENAME TO old_orders")

        cur.execute("ALTER TABLE orders RENAME TO old_orders")
        conn.commit()
        assert table_exists(conn, "old_orders")
        assert not table_exists(conn, "orders")

        execute_steps(conn, inv.steps)
        assert table_exists(conn, "orders")
        assert not table_exists(conn, "old_orders")
        record("ALTER TABLE", "rename_table", True)
        conn.close()


# ---------------------------------------------------------------------------
# DDL: INDEX
# ---------------------------------------------------------------------------

class TestIndexRealDB:

    def test_create_index_inverse_drops(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50))")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("CREATE INDEX idx_item ON orders (item)")

        cur.execute("CREATE INDEX idx_item ON orders (item)")
        conn.commit()
        assert index_exists(conn, "idx_item")

        execute_steps(conn, inv.steps)
        assert not index_exists(conn, "idx_item")
        record("INDEX", "create_inverse_drops", True)
        conn.close()

    def test_drop_index_inverse_recreates(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50))")
        cur.execute("CREATE INDEX idx_item ON orders (item)")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("DROP INDEX idx_item")

        cur.execute("DROP INDEX idx_item")
        conn.commit()
        assert not index_exists(conn, "idx_item")

        execute_steps(conn, inv.steps)
        assert index_exists(conn, "idx_item")
        record("INDEX", "drop_inverse_recreates", True)
        conn.close()

    def test_create_unique_index(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, email VARCHAR(200))")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("CREATE UNIQUE INDEX idx_email ON orders (email)")

        cur.execute("CREATE UNIQUE INDEX idx_email ON orders (email)")
        conn.commit()

        execute_steps(conn, inv.steps)
        assert not index_exists(conn, "idx_email")
        record("INDEX", "create_unique_inverse_drops", True)
        conn.close()


# ---------------------------------------------------------------------------
# DDL: SEQUENCE
# ---------------------------------------------------------------------------

class TestSequenceRealDB:

    def test_create_sequence_inverse_drops(self):
        conn = _test_conn()
        engine = InverseEngine(conn)
        inv = engine.generate("CREATE SEQUENCE order_seq START 100")

        cur = conn.cursor()
        cur.execute("CREATE SEQUENCE order_seq START 100")
        conn.commit()
        assert sequence_exists(conn, "order_seq")

        execute_steps(conn, inv.steps)
        assert not sequence_exists(conn, "order_seq")
        record("SEQUENCE", "create_inverse_drops", True)
        conn.close()

    def test_drop_sequence_inverse_recreates(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE SEQUENCE order_seq START 100 INCREMENT 5")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("DROP SEQUENCE order_seq")

        cur.execute("DROP SEQUENCE order_seq")
        conn.commit()
        assert not sequence_exists(conn, "order_seq")

        execute_steps(conn, inv.steps)
        assert sequence_exists(conn, "order_seq")
        record("SEQUENCE", "drop_inverse_recreates", True)
        conn.close()


# ---------------------------------------------------------------------------
# DDL: VIEW
# ---------------------------------------------------------------------------

class TestViewRealDB:

    def test_create_view_inverse_drops(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50), qty INT)")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("CREATE VIEW v_orders AS SELECT item, qty FROM orders")

        cur.execute("CREATE VIEW v_orders AS SELECT item, qty FROM orders")
        conn.commit()
        assert view_exists(conn, "v_orders")

        execute_steps(conn, inv.steps)
        assert not view_exists(conn, "v_orders")
        record("VIEW", "create_inverse_drops", True)
        conn.close()

    def test_drop_view_inverse_recreates(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50), qty INT)")
        cur.execute("CREATE VIEW v_orders AS SELECT item, qty FROM orders")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("DROP VIEW v_orders")

        cur.execute("DROP VIEW v_orders")
        conn.commit()
        assert not view_exists(conn, "v_orders")

        execute_steps(conn, inv.steps)
        assert view_exists(conn, "v_orders")
        record("VIEW", "drop_inverse_recreates", True)
        conn.close()

    def test_create_or_replace_view_adds_columns_limitation(self):
        """Known limitation: CREATE OR REPLACE VIEW that adds columns ΓÇö the inverse
        tries to restore the original (fewer columns), which PG rejects because
        CREATE OR REPLACE VIEW cannot drop columns."""
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50), qty INT)")
        cur.execute("CREATE VIEW v_orders AS SELECT item, qty FROM orders")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("CREATE OR REPLACE VIEW v_orders AS SELECT item, qty, id FROM orders")

        cur.execute("CREATE OR REPLACE VIEW v_orders AS SELECT item, qty, id FROM orders")
        conn.commit()

        # Inverse tries to restore original def (2 cols) but PG won't drop the 3rd
        with pytest.raises(psycopg2.errors.InvalidTableDefinition):
            execute_steps(conn, inv.steps)
        conn.rollback()
        record("VIEW", "or_replace_adds_columns_limitation", False,
               "PG disallows dropping columns via CREATE OR REPLACE VIEW")
        conn.close()

    def test_create_or_replace_view_narrowing_limitation(self):
        """Known limitation: CREATE OR REPLACE VIEW that narrows columns cannot be
        reversed with CREATE OR REPLACE ΓÇö PG disallows dropping columns from a view."""
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, item VARCHAR(50), qty INT)")
        cur.execute("CREATE VIEW v_orders AS SELECT item FROM orders")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("CREATE OR REPLACE VIEW v_orders AS SELECT item, qty FROM orders")

        cur.execute("CREATE OR REPLACE VIEW v_orders AS SELECT item, qty FROM orders")
        conn.commit()

        # Inverse tries CREATE OR REPLACE back to original (fewer columns) ΓÇö PG rejects
        with pytest.raises(psycopg2.errors.InvalidTableDefinition):
            execute_steps(conn, inv.steps)
        conn.rollback()
        record("VIEW", "narrowing_limitation", False,
               "PG disallows dropping columns via CREATE OR REPLACE VIEW")
        conn.close()


# ---------------------------------------------------------------------------
# DDL: SCHEMA
# ---------------------------------------------------------------------------

class TestSchemaRealDB:

    def test_create_schema_inverse_drops(self):
        conn = _test_conn()
        engine = InverseEngine(conn)
        inv = engine.generate("CREATE SCHEMA analytics")

        cur = conn.cursor()
        cur.execute("CREATE SCHEMA analytics")
        conn.commit()

        execute_steps(conn, inv.steps)
        cur = conn.cursor()
        cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'analytics')")
        assert not cur.fetchone()[0]
        record("SCHEMA", "create_inverse_drops", True)
        conn.close()

    def test_drop_schema_inverse_recreates(self):
        conn = _test_conn()
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA analytics")
        conn.commit()

        engine = InverseEngine(conn)
        inv = engine.generate("DROP SCHEMA analytics")

        cur.execute("DROP SCHEMA analytics")
        conn.commit()

        execute_steps(conn, inv.steps)
        cur = conn.cursor()
        cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'analytics')")
        assert cur.fetchone()[0]
        record("SCHEMA", "drop_inverse_recreates", True)
        conn.close()


# ---------------------------------------------------------------------------
# Accuracy report
# ---------------------------------------------------------------------------

class TestZZAccuracyReport:

    def test_print_report(self):
        """Print the real-DB accuracy report."""
        from collections import defaultdict

        cats = defaultdict(list)
        for cat, name, passed, detail in results:
            cats[cat].append((name, passed, detail))

        lines = ["\n" + "=" * 70]
        lines.append("  REAL DATABASE INVERSE ENGINE ACCURACY REPORT")
        lines.append("=" * 70)

        total_pass = 0
        total_fail = 0

        for cat in sorted(cats.keys()):
            entries = cats[cat]
            passed = sum(1 for _, p, _ in entries if p)
            failed = len(entries) - passed
            total_pass += passed
            total_fail += failed
            pct = (passed / len(entries)) * 100
            status = "PASS" if failed == 0 else "FAIL"
            lines.append(f"  [{status}] {cat:<25} {passed}/{len(entries)} ({pct:.0f}%)")
            for name, p, detail in entries:
                mark = "+" if p else "X"
                suffix = f" ΓÇö {detail}" if detail and not p else ""
                lines.append(f"         [{mark}] {name}{suffix}")

        total = total_pass + total_fail
        pct = (total_pass / total) * 100 if total else 0
        lines.append("-" * 70)
        lines.append(f"  OVERALL: {total_pass}/{total} passed ({pct:.1f}% accuracy)")
        lines.append("=" * 70)

        report = "\n".join(lines)
        print(report)

        assert total > 0, "No real-DB tests were tracked"
        accuracy = total_pass / total
        # Known limitations (SERIAL DROP TABLE, VIEW column narrowing) are expected failures
        assert accuracy >= 0.90, f"Accuracy {accuracy:.1%} below 90% threshold ({total_fail} known limitation(s))"
