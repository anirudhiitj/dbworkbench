<<<<<<< HEAD
"""Routes for snapshot management and frequency configuration."""

from fastapi import APIRouter, HTTPException

=======
"""Routes for snapshot listing and frequency configuration."""

from fastapi import APIRouter, Depends, HTTPException

from fastapi_backend.app.auth import get_current_user
>>>>>>> integration
from fastapi_backend.app.models.schemas import (
    SnapshotResponse,
    SnapshotFrequencyRequest,
    SnapshotFrequencyResponse,
)
from fastapi_backend.app.services.snapshot_service import (
<<<<<<< HEAD
    create_snapshot,
    list_snapshots,
=======
    list_snapshots_for_profile,
>>>>>>> integration
    get_snapshot_frequency,
    set_snapshot_frequency,
)

router = APIRouter(prefix="/snapshots", tags=["Snapshots"])


<<<<<<< HEAD
@router.post("", response_model=SnapshotResponse)
def take_snapshot():
    """Manually trigger a snapshot of the current database state."""
    try:
        return create_snapshot()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=list[SnapshotResponse])
def get_all_snapshots():
    """List all snapshot metadata records."""
    try:
        return list_snapshots()
=======
@router.get("", response_model=list[SnapshotResponse])
def get_all_snapshots(
    connection_profile_id: int,
    current_user: dict = Depends(get_current_user),
):
    """List all snapshot metadata records for this connection profile."""
    try:
        return list_snapshots_for_profile(
            user_id=current_user["user_id"],
            connection_profile_id=connection_profile_id,
        )
>>>>>>> integration
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/frequency", response_model=SnapshotFrequencyResponse)
<<<<<<< HEAD
def get_frequency():
    """Return the current snapshot frequency (1–5)."""
    try:
        freq = get_snapshot_frequency()
=======
def get_frequency(
    connection_profile_id: int,
    current_user: dict = Depends(get_current_user),
):
    """Return the current snapshot frequency for this connection profile."""
    try:
        freq = get_snapshot_frequency(connection_profile_id)
>>>>>>> integration
        return {"frequency": freq}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/frequency", response_model=SnapshotFrequencyResponse)
<<<<<<< HEAD
def update_frequency(request: SnapshotFrequencyRequest):
    """Update the snapshot frequency (1–5).

    A frequency of 1 means every commit triggers a snapshot.
    A frequency of 5 means every 5th commit triggers a snapshot.
    """
    try:
        new_freq = set_snapshot_frequency(request.frequency)
=======
def update_frequency(
    request: SnapshotFrequencyRequest,
    current_user: dict = Depends(get_current_user),
):
    """Update the snapshot frequency for this connection profile."""
    try:
        new_freq = set_snapshot_frequency(
            user_id=current_user["user_id"],
            connection_profile_id=request.connection_profile_id,
            frequency=request.frequency,
        )
>>>>>>> integration
        return {"frequency": new_freq}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
