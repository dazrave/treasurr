"""Frozen dataclasses for the Treasurr domain model."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class User:
    id: int
    plex_user_id: str
    plex_username: str
    email: str
    quota_bytes: int
    bonus_bytes: int = 0
    is_admin: bool = False
    created_at: str = ""
    auto_scuttle_days: int = 0
    onboarded: bool = False


@dataclass(frozen=True)
class Content:
    id: int
    title: str
    media_type: str  # 'movie' or 'show'
    tmdb_id: int
    sonarr_id: int | None = None
    radarr_id: int | None = None
    overseerr_request_id: int | None = None
    size_bytes: int = 0
    status: str = "active"  # 'active', 'deleting', 'deleted'
    added_at: str = ""
    poster_path: str | None = None


@dataclass(frozen=True)
class Season:
    id: int
    content_id: int
    season_number: int
    episode_count: int = 0
    size_bytes: int = 0


@dataclass(frozen=True)
class ContentOwnership:
    id: int
    content_id: int
    owner_user_id: int
    status: str = "owned"  # 'owned', 'promoted', 'released', 'plank', 'buried'
    owned_at: str = ""
    promoted_at: str | None = None
    plank_started_at: str | None = None
    buried_at: str | None = None


@dataclass(frozen=True)
class WatchEvent:
    id: int
    content_id: int
    user_id: int
    watched_at: str
    completed: bool = False


@dataclass(frozen=True)
class PromotionRecord:
    id: int
    content_id: int
    from_user_id: int
    unique_viewers: int
    size_freed_bytes: int
    promoted_at: str


@dataclass(frozen=True)
class DeletionRecord:
    id: int
    content_id: int
    deleted_by_user_id: int
    title: str
    size_bytes: int
    deleted_at: str


@dataclass(frozen=True)
class QuotaTransaction:
    id: int
    user_id: int
    change_bytes: int
    reason: str  # 'admin_grant', 'purchase', 'bonus'
    created_at: str


@dataclass(frozen=True)
class QuotaSummary:
    """Calculated view of a user's quota state."""
    user_id: int
    quota_bytes: int
    bonus_bytes: int
    used_bytes: int
    owned_count: int
    split_bytes: int = 0

    @property
    def total_bytes(self) -> int:
        return self.quota_bytes + self.bonus_bytes

    @property
    def total_used_bytes(self) -> int:
        return self.used_bytes + self.split_bytes

    @property
    def available_bytes(self) -> int:
        return self.total_bytes - self.total_used_bytes

    @property
    def usage_percent(self) -> float:
        if self.total_bytes == 0:
            return 0.0
        return round((self.total_used_bytes / self.total_bytes) * 100, 1)


@dataclass(frozen=True)
class QuotaSplit:
    """Per-user share of promoted content in split_the_loot mode."""
    id: int
    content_id: int
    user_id: int
    share_bytes: int
    created_at: str


@dataclass(frozen=True)
class ScuttleResult:
    """Result of a scuttle (delete) operation."""
    success: bool
    message: str
    freed_bytes: int = 0
    walked_plank: bool = False


@dataclass(frozen=True)
class RescueResult:
    """Result of a plank rescue operation."""
    success: bool
    message: str
    action: str = ""  # 'rescued', 'promoted', 'adopted'


@dataclass(frozen=True)
class OwnedContent:
    """Content with ownership info for display."""
    content: Content
    ownership: ContentOwnership
    unique_viewers: int = 0
