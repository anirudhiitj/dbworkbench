"""FastAPI application entry point.

Lifecycle
---------
Startup  → try to init connection pool + create metadata tables.
           If Postgres is unavailable the server still starts (with a warning).
Shutdown → close pool.
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from fastapi_backend.app.db.connection import init_pool, close_pool, get_connection, release_connection
from fastapi_backend.app.db.metadata_queries import INIT_METADATA_TABLES

from fastapi_backend.app.routes.query_routes import router as query_router
from fastapi_backend.app.routes.commit_routes import router as commit_router
from fastapi_backend.app.routes.anticommand_routes import router as anticommand_router
from fastapi_backend.app.routes.snapshot_routes import router as snapshot_router
from fastapi_backend.app.routes.rollback_routes import router as rollback_router

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
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(query_router)
app.include_router(commit_router)
app.include_router(anticommand_router)
app.include_router(snapshot_router)
app.include_router(rollback_router)


@app.get("/health")
def health():
    """Simple liveness check."""
    return {"status": "ok"}
