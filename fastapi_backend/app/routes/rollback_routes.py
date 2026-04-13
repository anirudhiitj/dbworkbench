"""Routes for database rollback."""

<<<<<<< HEAD
from fastapi import APIRouter, HTTPException

from fastapi_backend.app.models.schemas import RollbackRequest, RollbackResponse
from fastapi_backend.app.services.rollback_service import rollback_to_commit
=======
from fastapi import APIRouter, Depends, HTTPException

from fastapi_backend.app.auth import get_current_user
from fastapi_backend.app.models.schemas import RollbackRequest, RollbackResponse
from fastapi_backend.app.services.rollback_service import rollback_to_version
>>>>>>> integration

router = APIRouter(prefix="/rollback", tags=["Rollback"])


@router.post("", response_model=RollbackResponse)
<<<<<<< HEAD
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
=======
def rollback(
    request: RollbackRequest,
    current_user: dict = Depends(get_current_user),
):
    """Roll the user's database back to the state after the specified commit.

    Steps:
    1. Find nearest snapshot at or before the target commit
    2. Restore that snapshot on the user's external database
    3. Apply inverse operations in reverse for all commits after the target
    """
    try:
        return rollback_to_version(
            user_id=current_user["user_id"],
            connection_profile_id=request.connection_profile_id,
            target_version_id=request.target_version_id,
        )
>>>>>>> integration
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
