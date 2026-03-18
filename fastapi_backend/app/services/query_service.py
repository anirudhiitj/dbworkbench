"""Query service — execute raw SQL via psycopg2 and return results.

This endpoint is for fire-and-forget / ad-hoc queries (e.g. SELECT).
Versioned operations go through the commit service instead.
"""

from fastapi_backend.app.db.connection import get_connection, release_connection


def _ensure_safe_sql(sql: str) -> None:
    """
    Perform minimal validation on the raw SQL to reduce risk of misuse.
    This helper enforces a simple allow-list of read-only statements and
    rejects obviously dangerous input such as multiple statements.
    It is intentionally conservative.
    """
    stripped = sql.lstrip()
    if not stripped:
        raise ValueError("Empty SQL is not allowed")

    first_token = stripped.split(None, 1)[0].upper()

    # Allow only read-only / introspection style statements.
    allowed = {"SELECT", "SHOW", "EXPLAIN", "DESCRIBE"}
    if first_token not in allowed:
        raise PermissionError("Only read-only SQL statements are permitted")

    # Reject obviously multi-statement payloads.
    # A single trailing semicolon is tolerated, but anything more is blocked.
    semicolons = stripped.count(";")
    if semicolons > 1 or (semicolons == 1 and not stripped.rstrip().endswith(";")):
        raise PermissionError("Multiple SQL statements are not allowed")


def execute_raw_sql(sql: str) -> dict:
    """
    Execute a validated, read-only SQL statement and return the result.

    For SELECT-like queries the response contains columns + rows.
    For SHOW/EXPLAIN-like queries it returns any rows produced.
    """
    _ensure_safe_sql(sql)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)

        # If the statement returns rows (SELECT, RETURNING, etc.)
        if cur.description is not None:
            columns = [desc[0] for desc in cur.description]
            rows = [list(row) for row in cur.fetchall()]
            conn.commit()
            return {
                "columns": columns,
                "rows": rows,
                "rowcount": len(rows),
                "status": "success",
            }

        # DML / DDL with no result set
        rowcount = cur.rowcount
        conn.commit()
        return {
            "columns": [],
            "rows": [],
            "rowcount": rowcount,
            "status": "success",
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        release_connection(conn)
