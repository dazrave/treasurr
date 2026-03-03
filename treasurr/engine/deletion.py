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


async def scuttle_season(
    db: Database,
    config: Config,
    content_id: int,
    season_number: int,
    user_id: int,
) -> ScuttleResult:
    """Delete all episode files for a specific season via Sonarr."""
    content = db.get_content(content_id)
    if content is None:
        return ScuttleResult(success=False, message="Content not found")

    if content.status != "active":
        return ScuttleResult(success=False, message="Content is not active")

    if content.media_type != "show":
        return ScuttleResult(success=False, message="Season scuttle only applies to shows")

    if not content.sonarr_id:
        return ScuttleResult(success=False, message="No Sonarr ID linked - cannot delete season files")

    ownership = db.get_ownership(content_id)
    if ownership is None:
        return ScuttleResult(success=False, message="Content has no owner")

    if ownership.owner_user_id != user_id:
        return ScuttleResult(success=False, message="You don't own this content")

    if ownership.status not in ("owned", "promoted"):
        return ScuttleResult(success=False, message="Content cannot be modified in its current state")

    # Rate limit check
    recent = db.count_recent_deletions(user_id)
    if recent >= config.safety.max_deletions_per_hour:
        return ScuttleResult(
            success=False,
            message=f"Rate limit reached ({config.safety.max_deletions_per_hour} deletions/hour). Try again later.",
        )

    sonarr = SonarrClient(config.sonarr)

    try:
        episodes = await sonarr.get_episodes(content.sonarr_id)
    except Exception as e:
        return ScuttleResult(success=False, message=f"Failed to fetch episodes from Sonarr: {e}")

    # Find episode files for this season
    file_ids = []
    freed_bytes = 0
    for ep in episodes:
        if ep.get("seasonNumber") != season_number:
            continue
        if ep.get("hasFile", False):
            ep_file = ep.get("episodeFile", {})
            fid = ep_file.get("id") or ep.get("episodeFileId")
            if fid:
                file_ids.append(fid)
                freed_bytes += ep_file.get("size", 0)

    if not file_ids:
        return ScuttleResult(success=False, message=f"No files found for season {season_number}")

    # Delete each episode file
    deleted_count = 0
    for fid in file_ids:
        try:
            await sonarr.delete_episode_file(fid)
            deleted_count += 1
        except Exception as e:
            logger.warning("Failed to delete episode file %d: %s", fid, e)

    if deleted_count == 0:
        return ScuttleResult(success=False, message="Failed to delete any episode files")

    # Update season record
    db.update_season_size(content_id, season_number, 0)

    # Update total content size
    new_total = max(0, content.size_bytes - freed_bytes)
    db.update_content_size(content_id, new_total)

    # Log the deletion
    db.log_deletion(content_id, user_id, f"{content.title} S{season_number:02d}", freed_bytes)

    logger.info(
        "Scuttled season %d of '%s' (%d files, %d bytes freed)",
        season_number, content.title, deleted_count, freed_bytes,
    )

    # Check if all seasons are now empty - if so, treat as full scuttle
    remaining_seasons = db.get_seasons(content_id)
    total_remaining = sum(s.size_bytes for s in remaining_seasons)
    if total_remaining == 0 and new_total == 0:
        logger.info("All seasons empty for '%s' - executing full deletion", content.title)
        return await _execute_deletion(db, config, content_id, user_id, content)

    return ScuttleResult(
        success=True,
        message=f"Season {season_number} of '{content.title}' has been scuttled! {deleted_count} files removed.",
        freed_bytes=freed_bytes,
    )


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
