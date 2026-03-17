"""Routes for raw SQL execution (fire-and-forget, no versioning)."""

from fastapi import APIRouter, HTTPException

from fastapi_backend.app.models.schemas import ExecuteSQLRequest, ExecuteSQLResponse
from fastapi_backend.app.services.query_service import execute_raw_sql

router = APIRouter(prefix="/query", tags=["Query"])


@router.post("/execute", response_model=ExecuteSQLResponse)
def execute(request: ExecuteSQLRequest):
    """Execute a raw SQL statement and return the result.

    This is a non-versioned endpoint — useful for SELECT queries or
    ad-hoc operations that don't need to be tracked.
    """
    try:
        result = execute_raw_sql(request.sql)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
