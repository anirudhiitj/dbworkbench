<<<<<<< HEAD
"""Routes for versioned commits (multi-step SQL)."""

from fastapi import APIRouter, HTTPException

=======
"""Routes for versioned commits (single SQL command per commit)."""

from fastapi import APIRouter, Depends, HTTPException

from fastapi_backend.app.auth import get_current_user
>>>>>>> integration
from fastapi_backend.app.models.schemas import (
    CreateCommitRequest,
    CommitResponse,
    CommitListItem,
)
from fastapi_backend.app.services.commit_service import (
    create_commit,
    list_commits,
    get_commit,
)

router = APIRouter(prefix="/commits", tags=["Commits"])


@router.post("", response_model=CommitResponse)
<<<<<<< HEAD
def make_commit(request: CreateCommitRequest):
    """Create a new versioned commit containing one or more SQL steps."""
    try:
        steps = [s.model_dump() for s in request.steps]
        result = create_commit(steps, request.message)
        return result
=======
def make_commit(
    request: CreateCommitRequest,
    current_user: dict = Depends(get_current_user),
):
    """Execute a SQL command on the user's DB and record it as a versioned commit."""
    try:
        return create_commit(
            user_id=current_user["user_id"],
            connection_profile_id=request.connection_profile_id,
            sql_command=request.sql_command,
        )
>>>>>>> integration
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("", response_model=list[CommitListItem])
<<<<<<< HEAD
def get_all_commits():
    """List every commit in chronological order."""
    try:
        return list_commits()
=======
def get_all_commits(
    connection_profile_id: int,
    current_user: dict = Depends(get_current_user),
):
    """List every commit for this connection profile in chronological order."""
    try:
        return list_commits(
            user_id=current_user["user_id"],
            connection_profile_id=connection_profile_id,
        )
>>>>>>> integration
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


<<<<<<< HEAD
@router.get("/{commit_id}", response_model=CommitResponse)
def get_single_commit(commit_id: str):
    """Return a single commit with all of its steps."""
    try:
        result = get_commit(commit_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Commit {commit_id} not found")
=======
@router.get("/{version_id}", response_model=CommitResponse)
def get_single_commit(
    version_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Return a single commit by version_id."""
    try:
        result = get_commit(
            user_id=current_user["user_id"],
            version_id=version_id,
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Commit {version_id} not found",
            )
>>>>>>> integration
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
