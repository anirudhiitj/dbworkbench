<<<<<<< HEAD
"""Routes for raw SQL execution (fire-and-forget, no versioning)."""

from fastapi import APIRouter, HTTPException

from fastapi_backend.app.models.schemas import ExecuteSQLRequest, ExecuteSQLResponse
from fastapi_backend.app.services.query_service import execute_raw_sql
=======
"""Routes for raw SQL execution (read-only, no versioning)."""

from fastapi import APIRouter, Depends, HTTPException

from fastapi_backend.app.auth import get_current_user
from fastapi_backend.app.models.schemas import ExecuteSQLRequest, ExecuteSQLResponse
from fastapi_backend.app.services.query_service import execute_read_sql
>>>>>>> integration

router = APIRouter(prefix="/query", tags=["Query"])


@router.post("/execute", response_model=ExecuteSQLResponse)
<<<<<<< HEAD
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
=======
def execute(
    request: ExecuteSQLRequest,
    current_user: dict = Depends(get_current_user),
):
    """Execute a read-only SQL statement on the user's external database."""
    try:
        return execute_read_sql(
            user_id=current_user["user_id"],
            connection_profile_id=request.connection_profile_id,
            sql=request.sql,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
>>>>>>> integration
