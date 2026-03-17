"""Pydantic request / response schemas for the API."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


# ── Query (raw execute) ──────────────────────────────────────────────────────

class ExecuteSQLRequest(BaseModel):
    """Raw SQL to execute (SELECT, ad-hoc, etc.)."""
    sql: str


class ExecuteSQLResponse(BaseModel):
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    rowcount: int = 0
    status: str = "success"


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


class SnapshotFrequencyResponse(BaseModel):
    frequency: int


# ── Rollback ──────────────────────────────────────────────────────────────────

class RollbackRequest(BaseModel):
    """Roll back to a specific commit."""
    target_commit_id: UUID


class RollbackResponse(BaseModel):
    rolled_back_to: str
    snapshot_restored: str | None = None
    anti_commands_applied: int = 0
    status: str = "success"
