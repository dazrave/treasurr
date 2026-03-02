"""Quota calculation engine."""

from __future__ import annotations

from treasurr.db import Database
from treasurr.models import QuotaSummary


def get_user_quota(db: Database, user_id: int) -> QuotaSummary | None:
    """Calculate a user's current quota summary."""
    return db.get_quota_summary(user_id)


def has_sufficient_quota(db: Database, user_id: int, additional_bytes: int) -> bool:
    """Check if user has enough quota for additional content."""
    summary = db.get_quota_summary(user_id)
    if summary is None:
        return False
    return summary.available_bytes >= additional_bytes


def format_bytes(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    if size_bytes < 0:
        return f"-{format_bytes(-size_bytes)}"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"
