"""Routes for ConnectionProfile CRUD."""

from fastapi import APIRouter, Depends, HTTPException

from fastapi_backend.app.auth import get_current_user
from fastapi_backend.app.models.schemas import (
    CreateConnectionProfileRequest,
    UpdateConnectionProfileRequest,
    ConnectionProfileResponse,
)
from fastapi_backend.app.services.connection_service import (
    create_connection_profile,
    list_connection_profiles,
    get_connection_profile,
    update_connection_profile,
    delete_connection_profile,
)

router = APIRouter(prefix="/connections", tags=["Connections"])


@router.post("", response_model=ConnectionProfileResponse, status_code=201)
def create_profile(
    request: CreateConnectionProfileRequest,
    current_user: dict = Depends(get_current_user),
):
    """Create a new database connection profile for the current user."""
    try:
        return create_connection_profile(
            user_id=current_user["user_id"],
            name=request.name,
            host=request.host,
            port=request.port,
            database_name=request.database_name,
            db_username=request.db_username,
            db_password=request.db_password,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("", response_model=list[ConnectionProfileResponse])
def list_profiles(current_user: dict = Depends(get_current_user)):
    """List all connection profiles belonging to the current user."""
    try:
        return list_connection_profiles(user_id=current_user["user_id"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{profile_id}", response_model=ConnectionProfileResponse)
def get_profile(
    profile_id: int,
    current_user: dict = Depends(get_current_user),
):
    """Return a single connection profile."""
    try:
        result = get_connection_profile(
            user_id=current_user["user_id"],
            profile_id=profile_id,
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"Connection profile {profile_id} not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{profile_id}", response_model=ConnectionProfileResponse)
def update_profile(
    profile_id: int,
    request: UpdateConnectionProfileRequest,
    current_user: dict = Depends(get_current_user),
):
    """Update an existing connection profile."""
    try:
        return update_connection_profile(
            user_id=current_user["user_id"],
            profile_id=profile_id,
            **request.model_dump(exclude_none=True),
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{profile_id}", status_code=204)
def delete_profile(
    profile_id: int,
    current_user: dict = Depends(get_current_user),
):
    """Delete a connection profile and all associated data (commits, snapshots)."""
    try:
        delete_connection_profile(
            user_id=current_user["user_id"],
            profile_id=profile_id,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
