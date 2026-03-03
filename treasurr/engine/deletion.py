"""Deletion (scuttle) engine  - handles content removal workflow."""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.models import ScuttleResult
from treasurr.sync.clients import RadarrClient, SonarrClient

logger = logging.getLogger(__name__)


def _get_plank_days(db: Database, config: Config) -> int:
    """Read plank_days from settings, falling back to config."""
    raw = db.get_setting("plank_days", "")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return config.quotas.plank_days


async def scuttle_content(
    db: Database,
    config: Config,
    content_id: int,
    user_id: int,
) -> ScuttleResult:
    """Delete content owned by the user (scuttle = sink the ship).

    If plank_days > 0, content enters the plank (grace period) instead of
    being deleted immediately. If plank_days == 0, instant delete (legacy).
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

    if ownership.status == "promoted":
        return ScuttleResult(success=False, message="Content is already promoted  - it's shared plunder now")

    if ownership.status == "plank":
        return ScuttleResult(success=False, message="Content is already walking the plank")

    if ownership.status != "owned":
        return ScuttleResult(success=False, message="Content cannot be scuttled in its current state")

    # Rate limit check
    recent = db.count_recent_deletions(user_id)
    if recent >= config.safety.max_deletions_per_hour:
        return ScuttleResult(
            success=False,
            message=f"Rate limit reached ({config.safety.max_deletions_per_hour} deletions/hour). Try again later.",
        )

    # Check if plank is enabled
    plank_days = _get_plank_days(db, config)
    if plank_days > 0:
        db.plank_content(content_id)
        logger.info(
            "Planked '%s' for %d days (user %d)", content.title, plank_days, user_id,
        )
        return ScuttleResult(
            success=True,
            message=f"'{content.title}' is walking the plank! The crew has {plank_days} days to save it.",
            walked_plank=True,
        )

    # Instant delete (plank_days == 0)
    return await _execute_deletion(db, config, content_id, user_id, content)


async def _execute_deletion(
    db: Database,
    config: Config,
    content_id: int,
    user_id: int,
    content=None,
) -> ScuttleResult:
    """Actually delete content via arr APIs and mark as deleted."""
    if content is None:
        content = db.get_content(content_id)
        if content is None:
            return ScuttleResult(success=False, message="Content not found")

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
            logger.warning("No arr ID for content '%s'  - marking deleted without file removal", content.title)

    except Exception as e:
        db.update_content_status(content_id, "active")
        logger.error("Failed to delete content '%s': %s", content.title, e)
        return ScuttleResult(success=False, message=f"Deletion failed: {e}")

    # Success  - update records
    db.update_content_status(content_id, "deleted")
    db.release_content(content_id)
    db.delete_splits_for_content(content_id)
    db.log_deletion(content_id, user_id, content.title, content.size_bytes)

    logger.info("Scuttled '%s' (%d bytes freed for user %d)", content.title, content.size_bytes, user_id)
    return ScuttleResult(
        success=True,
        message=f"'{content.title}' has been scuttled!",
        freed_bytes=content.size_bytes,
    )
