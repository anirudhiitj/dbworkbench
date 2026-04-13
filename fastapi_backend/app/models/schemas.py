<<<<<<< HEAD
"""Pydantic request / response schemas for the API."""
=======
"""Pydantic request / response schemas for the API.

Aligned with Django's ORM models (CommitEvent, InverseOperation,
Snapshot, SnapshotPolicy, ConnectionProfile).
"""
>>>>>>> integration

from __future__ import annotations

from datetime import datetime
from typing import Any
<<<<<<< HEAD
from uuid import UUID
=======
>>>>>>> integration

from pydantic import BaseModel, Field


<<<<<<< HEAD
# ── Query (raw execute) ──────────────────────────────────────────────────────

class ExecuteSQLRequest(BaseModel):
    """Raw SQL to execute (SELECT, ad-hoc, etc.)."""
=======
# -- Auth ----------------------------------------------------------------------

class RegisterRequest(BaseModel):
    username: str
    password: str
    email: str = ""


class RegisterResponse(BaseModel):
    id: int
    username: str


class TokenObtainRequest(BaseModel):
    username: str
    password: str


class TokenRefreshRequest(BaseModel):
    refresh: str


# -- ConnectionProfile ---------------------------------------------------------

class CreateConnectionProfileRequest(BaseModel):
    name: str
    host: str
    port: int = 5432
    database_name: str
    db_username: str
    db_password: str


class UpdateConnectionProfileRequest(BaseModel):
    name: str | None = None
    host: str | None = None
    port: int | None = None
    database_name: str | None = None
    db_username: str | None = None
    db_password: str | None = None


class ConnectionProfileResponse(BaseModel):
    id: int
    name: str
    host: str
    port: int
    database_name: str
    db_username: str
    created_at: datetime


# -- Query (read-only, no versioning) -----------------------------------------

class ExecuteSQLRequest(BaseModel):
    connection_profile_id: int
>>>>>>> integration
    sql: str


class ExecuteSQLResponse(BaseModel):
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    rowcount: int = 0
    status: str = "success"


<<<<<<< HEAD
# ── Commit ────────────────────────────────────────────────────────────────────

class CommitStepInput(BaseModel):
    """A single SQL step inside a commit."""
    sql: str
    step_type: str = "DML"  # DML | DDL


class CreateCommitRequest(BaseModel):
    """Create a new commit with one or more SQL steps."""
    steps: list[CommitStepInput]
    message: str | None = None


class CommitStepResponse(BaseModel):
    step_id: int
    step_order: int
    sql_command: str
    step_type: str


class CommitResponse(BaseModel):
    commit_id: UUID
    commit_number: int
    hash: str
    message: str | None
    steps: list[CommitStepResponse] = Field(default_factory=list)
    created_at: datetime


class CommitListItem(BaseModel):
    commit_id: UUID
    commit_number: int
    hash: str
    message: str | None
    created_at: datetime


# ── Anti-command ──────────────────────────────────────────────────────────────

class StoreAntiCommandRequest(BaseModel):
    """Store an anti-command for a specific step in a commit."""
    commit_id: UUID
    step_id: int
    anti_sql: str


class AntiCommandResponse(BaseModel):
    id: int
    commit_id: UUID
    step_id: int
    anti_sql: str


# ── Snapshot ──────────────────────────────────────────────────────────────────

class SnapshotResponse(BaseModel):
    id: int
    commit_number: int
    s3_key: str
    created_at: datetime


class SnapshotFrequencyRequest(BaseModel):
    """Update snapshot frequency (1–5)."""
    frequency: int = Field(..., ge=1, le=5)
=======
# -- Commit (versioned write) -------------------------------------------------

class CreateCommitRequest(BaseModel):
    connection_profile_id: int
    sql_command: str


class CommitResponse(BaseModel):
    version_id: str
    seq: int
    sql_command: str
    commit_hash: str = ""
    status: str
    timestamp: datetime
    connection_profile_id: int


class CommitListItem(BaseModel):
    version_id: str
    seq: int
    sql_command: str
    commit_hash: str = ""
    status: str
    timestamp: datetime


# -- Anti-command (inverse operation retrieval) --------------------------------

class AntiCommandResponse(BaseModel):
    version_id: str
    inverse_sql: str
    commit_version_id: str


# -- Snapshot ------------------------------------------------------------------

class SnapshotResponse(BaseModel):
    snapshot_id: str
    version_id: str
    s3_key: str
    created_at: datetime
    connection_profile_id: int


class SnapshotFrequencyRequest(BaseModel):
    connection_profile_id: int
    frequency: int = Field(..., ge=1)
>>>>>>> integration


class SnapshotFrequencyResponse(BaseModel):
    frequency: int


<<<<<<< HEAD
# ── Rollback ──────────────────────────────────────────────────────────────────

class RollbackRequest(BaseModel):
    """Roll back to a specific commit."""
    target_commit_id: UUID
=======
# -- Rollback ------------------------------------------------------------------

class RollbackRequest(BaseModel):
    connection_profile_id: int
    target_version_id: str
>>>>>>> integration


class RollbackResponse(BaseModel):
    rolled_back_to: str
    snapshot_restored: str | None = None
<<<<<<< HEAD
    anti_commands_applied: int = 0
=======
    commands_applied: int = 0
>>>>>>> integration
    status: str = "success"
