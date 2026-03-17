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
)

router = APIRouter(prefix="/anticommands", tags=["Anti-Commands"])


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
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
