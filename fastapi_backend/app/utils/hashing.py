"""SHA-256 hashing utility for commit identification."""

import hashlib


def generate_commit_hash(commit_number: int, timestamp: str, sql_steps: list[str]) -> str:
    """
    Generate a deterministic SHA-256 hash for a commit.

    The hash combines the commit number, its timestamp, and every SQL
    statement in order — so two commits with the same steps at different
    times still get unique hashes.
    """
    raw = f"{commit_number}|{timestamp}|{'||'.join(sql_steps)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
