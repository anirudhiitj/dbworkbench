"""Routes for versioned commits (multi-step SQL)."""

from fastapi import APIRouter, HTTPException

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
def make_commit(request: CreateCommitRequest):
    """Create a new versioned commit containing one or more SQL steps."""
    try:
        steps = [s.model_dump() for s in request.steps]
        result = create_commit(steps, request.message)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("", response_model=list[CommitListItem])
def get_all_commits():
    """List every commit in chronological order."""
    try:
        return list_commits()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{commit_id}", response_model=CommitResponse)
def get_single_commit(commit_id: str):
    """Return a single commit with all of its steps."""
    try:
        result = get_commit(commit_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Commit {commit_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
