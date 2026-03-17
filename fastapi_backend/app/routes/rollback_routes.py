"""Routes for database rollback."""

from fastapi import APIRouter, HTTPException

from fastapi_backend.app.models.schemas import RollbackRequest, RollbackResponse
from fastapi_backend.app.services.rollback_service import rollback_to_commit

router = APIRouter(prefix="/rollback", tags=["Rollback"])


@router.post("", response_model=RollbackResponse)
def rollback(request: RollbackRequest):
    """Roll the database back to the state after the specified commit.

    Steps:
    1. Find nearest snapshot ≤ target commit.
    2. Restore that snapshot.
    3. Apply anti-commands in reverse for all commits after the target.
    """
    try:
        result = rollback_to_commit(str(request.target_commit_id))
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
