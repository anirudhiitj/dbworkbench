"""Query service — execute raw SQL via psycopg2 and return results.

This endpoint is for fire-and-forget / ad-hoc queries (e.g. SELECT).
Versioned operations go through the commit service instead.
"""

from psycopg2.extras import RealDictCursor

from fastapi_backend.app.db.connection import get_connection, release_connection


def execute_raw_sql(sql: str) -> dict:
    """
    Execute an arbitrary SQL statement and return the result.

    For SELECT-like queries the response contains columns + rows.
    For DML/DDL the response contains the affected rowcount.
    """
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
