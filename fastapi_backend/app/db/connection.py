<<<<<<< HEAD
"""psycopg2 connection-pool management.

Provides init/close lifecycle hooks and get/release helpers used by every
service in the backend.
"""

from psycopg2 import pool

from fastapi_backend.app.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_POOL_MIN,
    DB_POOL_MAX,
)

_pool: pool.ThreadedConnectionPool | None = None


def init_pool():
    """Initialize the connection pool.  Call once at app startup."""
    global _pool
    _pool = pool.ThreadedConnectionPool(
        minconn=DB_POOL_MIN,
        maxconn=DB_POOL_MAX,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def get_connection():
    """Get a connection from the pool."""
    if _pool is None:
        raise RuntimeError("Connection pool not initialised — call init_pool() first")
    return _pool.getconn()


def release_connection(conn):
    """Return a connection back to the pool."""
    if _pool is not None:
        _pool.putconn(conn)


def close_pool():
    """Close all connections in the pool.  Call at app shutdown."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
=======
"""Dynamic per-user database connection management.

Connections are made to the user's external PostgreSQL database using
credentials stored (Fernet-encrypted) in their ConnectionProfile.
Django ORM never touches the user's database — only this module does.
"""

import psycopg2


def get_user_connection(connection_profile):
    """Open a psycopg2 connection to the user's external database.

    Parameters
    ----------
    connection_profile : connections.models.ConnectionProfile
        Django model instance with encrypted credentials.

    Returns
    -------
    psycopg2 connection — caller is responsible for closing it.
    """
    return psycopg2.connect(
        host=connection_profile.host,
        port=connection_profile.port,
        dbname=connection_profile.database_name,
        user=connection_profile.db_username,
        password=connection_profile.get_decrypted_password(),
    )
>>>>>>> integration
