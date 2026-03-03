"""Quota calculation engine."""

from __future__ import annotations

import math

from treasurr.db import Database
from treasurr.models import QuotaSummary


def get_user_quota(
    db: Database, user_id: int, include_splits: bool = False
) -> QuotaSummary | None:
    """Calculate a user's current quota summary."""
    return db.get_quota_summary(user_id, include_splits=include_splits)


def has_sufficient_quota(
    db: Database, user_id: int, additional_bytes: int, include_splits: bool = False
) -> bool:
    """Check if user has enough quota for additional content."""
    summary = db.get_quota_summary(user_id, include_splits=include_splits)
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


def format_bytes_display(size_bytes: int, total_bytes: int, display_mode: str) -> str:
    """Format bytes according to admin display mode setting."""
    if display_mode == "percentage":
        if total_bytes == 0:
            return "0%"
        pct = (size_bytes / total_bytes) * 100
        return f"{pct:.0f}% of your space"
    if display_mode == "round_up":
        gb = size_bytes / (1024**3)
        if gb < 1:
            mb = math.ceil(size_bytes / (1024**2))
            return f"{mb} MB"
        return f"{math.ceil(gb)} GB"
    return format_bytes(size_bytes)
