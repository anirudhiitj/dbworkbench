"""Routes for snapshot management and frequency configuration."""

from fastapi import APIRouter, HTTPException

from fastapi_backend.app.models.schemas import (
    SnapshotResponse,
    SnapshotFrequencyRequest,
    SnapshotFrequencyResponse,
)
from fastapi_backend.app.services.snapshot_service import (
    create_snapshot,
    list_snapshots,
    get_snapshot_frequency,
    set_snapshot_frequency,
)

router = APIRouter(prefix="/snapshots", tags=["Snapshots"])


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
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/frequency", response_model=SnapshotFrequencyResponse)
def get_frequency():
    """Return the current snapshot frequency (1–5)."""
    try:
        freq = get_snapshot_frequency()
        return {"frequency": freq}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/frequency", response_model=SnapshotFrequencyResponse)
def update_frequency(request: SnapshotFrequencyRequest):
    """Update the snapshot frequency (1–5).

    A frequency of 1 means every commit triggers a snapshot.
    A frequency of 5 means every 5th commit triggers a snapshot.
    """
    try:
        new_freq = set_snapshot_frequency(request.frequency)
        return {"frequency": new_freq}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
