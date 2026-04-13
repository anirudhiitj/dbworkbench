"""FastAPI application entry point.

<<<<<<< HEAD
Lifecycle
---------
Startup  → try to init connection pool + create metadata tables.
           If Postgres is unavailable the server still starts (with a warning).
Shutdown → close pool.
"""

=======
Django ORM is bootstrapped on import so that all services can use
Django models, the atomic record_commit() function, and Fernet
decryption from ConnectionProfile.
"""

# Bootstrap Django ORM BEFORE any other app imports
import fastapi_backend.app.django_setup  # noqa: F401, E402

>>>>>>> integration
import logging
import os
from contextlib import asynccontextmanager

<<<<<<< HEAD
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from fastapi_backend.app.db.connection import init_pool, close_pool, get_connection, release_connection
from fastapi_backend.app.db.metadata_queries import INIT_METADATA_TABLES

=======
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from django.db import close_old_connections
from django.db import connections as django_connections

from fastapi_backend.app.routes.auth_routes import router as auth_router
from fastapi_backend.app.routes.connection_routes import router as connection_router
>>>>>>> integration
from fastapi_backend.app.routes.query_routes import router as query_router
from fastapi_backend.app.routes.commit_routes import router as commit_router
from fastapi_backend.app.routes.anticommand_routes import router as anticommand_router
from fastapi_backend.app.routes.snapshot_routes import router as snapshot_router
from fastapi_backend.app.routes.rollback_routes import router as rollback_router
<<<<<<< HEAD

logger = logging.getLogger(__name__)

# Comma-separated list of allowed origins, e.g. "http://localhost:3000,https://myapp.com"
_cors_origins_env = os.getenv("BACKEND_CORS_ORIGINS", "").strip()
ALLOWED_ORIGINS = (
    [origin.strip() for origin in _cors_origins_env.split(",") if origin.strip()]
    or ["http://localhost:3000"]
)


def _init_metadata_tables():
    """Create metadata tables if they don't exist."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(INIT_METADATA_TABLES)
        conn.commit()
    finally:
        release_connection(conn)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: init pool + tables.  Shutdown: close pool.

    If Postgres is not reachable the server still starts — DB-dependent
    endpoints will fail at call time, but /docs and /health remain available.
    """
    try:
        init_pool()
        _init_metadata_tables()
        logger.info("Database connection pool initialised and metadata tables ready.")
    except Exception as exc:
        logger.warning(
            "Could not connect to PostgreSQL — the server will start but "
            "DB-dependent endpoints will fail until the database is available.  "
            "Error: %s",
            exc,
        )
    yield
    try:
        close_pool()
    except Exception:
        pass


app = FastAPI(
    title="DB Version Control",
    description=(
        "Database version-control backend with multi-step commits, "
        "anti-command storage, configurable snapshotting, and rollback."
    ),
    version="0.2.0",
    lifespan=lifespan,
)

# ── CORS (allow the webui frontend to call us) ───────────────────────────────
=======
from fastapi_backend.app.routes.terminal_routes import router as terminal_router

from fastapi_backend.app.kafka import producer as kafka_producer

logger = logging.getLogger(__name__)

_cors_origins_env = os.getenv("BACKEND_CORS_ORIGINS", "").strip()
ALLOWED_ORIGINS = (
    [origin.strip() for origin in _cors_origins_env.split(",") if origin.strip()]
    or ["http://localhost:3000", "http://localhost:5173", "http://localhost:8080"]
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle.

    - Initialise the Kafka producer (non-blocking; degrades gracefully).
    - No connection pool to init — connections are created dynamically
      per-user via ConnectionProfile.  Django ORM is already bootstrapped.
    """
    kafka_ok = kafka_producer.init_producer()
    logger.info(
        "FastAPI started — Django ORM bootstrapped, dynamic user connections ready. "
        "Kafka producer %s.",
        "enabled" if kafka_ok else "disabled (falling back to sync)",
    )
    yield
    kafka_producer.shutdown()


app = FastAPI(
    title="WEAVE-DB API",
    description=(
        "Database version-control backend with commit tracking, "
        "inverse operations, configurable snapshotting, and rollback. "
        "All endpoints require JWT authentication (issued by Django)."
    ),
    version="0.3.0",
    lifespan=lifespan,
)

# CORS — allow the web UI frontend to call us
>>>>>>> integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

<<<<<<< HEAD
# ── Routers ───────────────────────────────────────────────────────────────────
=======

@app.middleware("http")
async def reset_db_connections(request: Request, call_next):
    """Close stale Django ORM connections before each request."""
    close_old_connections()
    for conn in django_connections.all():
        conn.close_if_unusable_or_obsolete()
    response = await call_next(request)
    close_old_connections()
    return response


# Routers
app.include_router(auth_router)
app.include_router(connection_router)
>>>>>>> integration
app.include_router(query_router)
app.include_router(commit_router)
app.include_router(anticommand_router)
app.include_router(snapshot_router)
app.include_router(rollback_router)
<<<<<<< HEAD
=======
app.include_router(terminal_router)
>>>>>>> integration


@app.get("/health")
def health():
<<<<<<< HEAD
    """Simple liveness check."""
=======
    """Liveness check (no auth required)."""
>>>>>>> integration
    return {"status": "ok"}
