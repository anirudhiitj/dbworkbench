import psycopg2
from psycopg2.extras import RealDictCursor
import yaml
from contextlib import contextmanager
from pathlib import Path
from typing import List, Dict, Any, Optional, Union


def load_config():
    """Load database configuration from config.yaml"""
    config_path = (Path(__file__).resolve().parent.parent / "config.yaml")
    with config_path.open("r") as f:
        return yaml.safe_load(f)


@contextmanager
def get_db_connection(dict_cursor=False):
    """
    Context manager for database connections.
    Automatically handles connection cleanup.
    
    Args:
        dict_cursor: If True, returns results as dictionaries
    
    Usage:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
    """
    config = load_config()
    conn = None
    try:
        cursor_factory = RealDictCursor if dict_cursor else None
        conn = psycopg2.connect(
            host=config["POSTGRES_CREDS"]["HOST"],
            database=config["POSTGRES_CREDS"]["DATABASE"],
            user=config["POSTGRES_CREDS"]["USER"],
            password=config["POSTGRES_CREDS"]["PASSWORD"],
            port=config["POSTGRES_CREDS"]["PORT"],
            cursor_factory=cursor_factory
        )
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def read_query(query: str, params: Optional[tuple] = None, as_dict: bool = True) -> Union[List[Dict[str, Any]], List[tuple]]:
    """
    Execute a SELECT query and return results.
    
    Args:
        query: SQL SELECT query
        params: Optional tuple of parameters for parameterized queries
        as_dict: If True, returns list of dictionaries; if False, returns list of tuples
    
    Returns:
        List of results (dictionaries or tuples)
    
    Example:
        results = read_query("SELECT * FROM users WHERE id = %s", (1,))
    """
    with get_db_connection(dict_cursor=as_dict) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        return results


def read_one(query: str, params: Optional[tuple] = None, as_dict: bool = True) -> Union[Dict[str, Any], tuple, None]:
    """
    Execute a SELECT query and return single result.
    
    Args:
        query: SQL SELECT query
        params: Optional tuple of parameters for parameterized queries
        as_dict: If True, returns dictionary; if False, returns tuple
    
    Returns:
        Single result (dictionary or tuple) or None if no results
    
    Example:
        user = read_one("SELECT * FROM users WHERE id = %s", (1,))
    """
    with get_db_connection(dict_cursor=as_dict) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        result = cursor.fetchone()
        cursor.close()
        return result


def write_query(query: str, params: Optional[tuple] = None, return_id: bool = False) -> Optional[int]:
    """
    Execute an INSERT, UPDATE, or DELETE query.
    
    Args:
        query: SQL query (INSERT, UPDATE, DELETE)
        params: Optional tuple of parameters for parameterized queries
        return_id: If True, returns the ID of the inserted row (for INSERT with RETURNING)
    
    Returns:
        Row ID if return_id=True and query has RETURNING clause, else None
    
    Example:
        write_query("INSERT INTO users (name, email) VALUES (%s, %s)", ("John", "john@example.com"))
        user_id = write_query("INSERT INTO users (name) VALUES (%s) RETURNING id", ("Jane",), return_id=True)
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        result = None
        if return_id:
            result = cursor.fetchone()
            if result:
                result = result[0]
        
        cursor.close()
        return result


def insert_many(query: str, data: List[tuple]) -> int:
    """
    Execute batch INSERT operation.
    
    Args:
        query: SQL INSERT query with placeholders
        data: List of tuples, each containing values for one row
    
    Returns:
        Number of rows inserted
    
    Example:
        data = [("Alice", "alice@example.com"), ("Bob", "bob@example.com")]
        insert_many("INSERT INTO users (name, email) VALUES (%s, %s)", data)
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany(query, data)
        row_count = cursor.rowcount
        cursor.close()
        return row_count


def execute_query(query: str, params: Optional[tuple] = None) -> int:
    """
    Execute any SQL query and return number of affected rows.
    
    Args:
        query: SQL query
        params: Optional tuple of parameters for parameterized queries
    
    Returns:
        Number of affected rows
    
    Example:
        rows_updated = execute_query("UPDATE users SET active = true WHERE id = %s", (1,))
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        row_count = cursor.rowcount
        cursor.close()
        return row_count


if __name__ == "__main__":

    # write_query("INSERT INTO hello VALUES (%s, %s)", (2, 'parrva'))
    
    results = read_query("SELECT * FROM hello")
    for result in results:
        print(result)


# ---------------------------------------------------------------------------
# External interface functions consumed by the FastAPI backend.
# These are stubs — replace with real implementations.
# ---------------------------------------------------------------------------

def executeQuery(sql: str):
    """Execute a SQL statement. To be implemented by the SQL engine module."""
    raise NotImplementedError("executeQuery not yet implemented")


def validateSQL(sql: str):
    """Validate a SQL statement. To be implemented by the SQL parser module."""
    raise NotImplementedError("validateSQL not yet implemented")


def generateAntiInsert(sql: str) -> str:
    """Generate an anti-command for an INSERT. To be implemented externally."""
    raise NotImplementedError("generateAntiInsert not yet implemented")


def generateAntiUpdate(sql: str) -> str:
    """Generate an anti-command for an UPDATE. To be implemented externally."""
    raise NotImplementedError("generateAntiUpdate not yet implemented")


def generateAntiDelete(sql: str) -> str:
    """Generate an anti-command for a DELETE. To be implemented externally."""
    raise NotImplementedError("generateAntiDelete not yet implemented")


def validateAntiCommand(sql: str):
    """Validate an anti-command. To be implemented externally."""
    raise NotImplementedError("validateAntiCommand not yet implemented")