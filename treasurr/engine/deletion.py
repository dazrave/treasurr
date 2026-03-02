"""Deletion (scuttle) engine — handles content removal workflow."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from treasurr.config import Config
from treasurr.db import Database
from treasurr.sync.clients import RadarrClient, SonarrClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScuttleResult:
    success: bool
    message: str
    freed_bytes: int = 0


async def scuttle_content(
    db: Database,
    config: Config,
    content_id: int,
    user_id: int,
) -> ScuttleResult:
    """Delete content owned by the user (scuttle = sink the ship).

    Steps:
    1. Validate ownership
    2. Check deletion rate limit
    3. Unmonitor in Sonarr/Radarr
    4. Delete files via arr API
    5. Update status and log
    """
    content = db.get_content(content_id)
    if content is None:
        return ScuttleResult(success=False, message="Content not found")

    if content.status != "active":
        return ScuttleResult(success=False, message="Content is not active")

    ownership = db.get_ownership(content_id)
    if ownership is None:
        return ScuttleResult(success=False, message="Content has no owner")

    if ownership.owner_user_id != user_id:
        return ScuttleResult(success=False, message="You don't own this content")

    if ownership.status != "owned":
        return ScuttleResult(success=False, message="Content is already promoted — it's shared plunder now")

    # Rate limit check
    recent = db.count_recent_deletions(user_id)
    if recent >= config.safety.max_deletions_per_hour:
        return ScuttleResult(
            success=False,
            message=f"Rate limit reached ({config.safety.max_deletions_per_hour} deletions/hour). Try again later.",
        )

    # Mark as deleting
    db.update_content_status(content_id, "deleting")

    try:
        if content.media_type == "show" and content.sonarr_id:
            sonarr = SonarrClient(config.sonarr)
            try:
                await sonarr.unmonitor(content.sonarr_id)
            except Exception as e:
                logger.warning("Failed to unmonitor in Sonarr: %s", e)
            await sonarr.delete(content.sonarr_id, delete_files=True)

        elif content.media_type == "movie" and content.radarr_id:
            radarr = RadarrClient(config.radarr)
            try:
                await radarr.unmonitor(content.radarr_id)
            except Exception as e:
                logger.warning("Failed to unmonitor in Radarr: %s", e)
            await radarr.delete(content.radarr_id, delete_files=True)

        else:
            # No arr ID — can't delete files, but mark as deleted anyway
            logger.warning("No arr ID for content '%s' — marking deleted without file removal", content.title)

    except Exception as e:
        # Revert status on failure
        db.update_content_status(content_id, "active")
        logger.error("Failed to delete content '%s': %s", content.title, e)
        return ScuttleResult(success=False, message=f"Deletion failed: {e}")

    # Success — update records
    db.update_content_status(content_id, "deleted")
    db.release_content(content_id)
    db.log_deletion(content_id, user_id, content.title, content.size_bytes)

    logger.info("Scuttled '%s' (%d bytes freed for user %d)", content.title, content.size_bytes, user_id)
    return ScuttleResult(
        success=True,
        message=f"'{content.title}' has been scuttled!",
        freed_bytes=content.size_bytes,
    )
