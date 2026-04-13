<<<<<<< HEAD
"""Routes for anti-command storage and retrieval.

The *generation* logic lives elsewhere (your friend's code).
These endpoints only store and retrieve anti-commands.
"""

from fastapi import APIRouter, HTTPException

from fastapi_backend.app.models.schemas import (
    StoreAntiCommandRequest,
    AntiCommandResponse,
)
from fastapi_backend.app.services.anticommand_service import (
    store_anti_command,
    get_anti_commands_for_commit,
=======
"""Routes for inverse operation retrieval.

Storage is handled atomically inside record_commit() (Django).
These endpoints only retrieve inverse operations for display / debugging.
"""

from fastapi import APIRouter, Depends, HTTPException

from fastapi_backend.app.auth import get_current_user
from fastapi_backend.app.models.schemas import AntiCommandResponse
from fastapi_backend.app.services.anticommand_service import (
    get_inverse_for_version,
    get_inverses_for_profile,
>>>>>>> integration
)

router = APIRouter(prefix="/anticommands", tags=["Anti-Commands"])


<<<<<<< HEAD
@router.post("", response_model=AntiCommandResponse)
def store(request: StoreAntiCommandRequest):
    """Store an anti-command for a specific step in a commit.

    The caller (your friend's code) is responsible for generating the
    correct inverse SQL.  This endpoint merely persists it.
    """
    try:
        result = store_anti_command(
            commit_id=str(request.commit_id),
            step_id=request.step_id,
            anti_sql=request.anti_sql,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{commit_id}", response_model=list[AntiCommandResponse])
def get_for_commit(commit_id: str):
    """Retrieve all anti-commands for a given commit."""
    try:
        return get_anti_commands_for_commit(commit_id)
=======
@router.get("", response_model=list[AntiCommandResponse])
def get_all_for_profile(
    connection_profile_id: int,
    current_user: dict = Depends(get_current_user),
):
    """Retrieve all inverse operations for a connection profile."""
    try:
        return get_inverses_for_profile(
            user_id=current_user["user_id"],
            connection_profile_id=connection_profile_id,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{version_id}", response_model=AntiCommandResponse)
def get_for_version(
    version_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Retrieve the inverse operation for a specific commit version."""
    try:
        result = get_inverse_for_version(
            user_id=current_user["user_id"],
            version_id=version_id,
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Inverse operation for version {version_id} not found",
            )
        return result
    except HTTPException:
        raise
>>>>>>> integration
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
