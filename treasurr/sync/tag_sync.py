"""Sync content ownership from Sonarr/Radarr user tags."""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.sync.clients import RadarrClient, SonarrClient

logger = logging.getLogger(__name__)


def _build_tag_user_map(
    tags: list[dict],
    users_by_username: dict[str, int],
) -> dict[int, int]:
    """Build a mapping from tag ID to Treasurr user DB ID.

    Detects user tags by the Overseerr convention: '{overseerr_id} - {username}'.
    Falls back to matching plain username tags if no separator is found.
    """
    tag_to_user: dict[int, int] = {}

    for tag in tags:
        label = tag.get("label", "")
        tag_id = tag.get("id")
        if tag_id is None:
            continue

        # Try Overseerr format: "1 - dazrave"
        if " - " in label:
            username = label.split(" - ", 1)[1].strip().lower()
        else:
            username = label.strip().lower()

        if username in users_by_username:
            tag_to_user[tag_id] = users_by_username[username]

    return tag_to_user


async def sync_tag_ownership(db: Database, config: Config) -> dict:
    """Resolve content ownership from Sonarr/Radarr tags.

    Reads user tags from arr APIs and updates ownership for content
    that is unowned or owned by a different user. Never overrides
    promoted, plank, or buried content.

    Returns dict with resolved/updated/skipped counts.
    """
    settings = db.get_all_settings()
    enabled = settings.get("tag_ownership_enabled", "true")
    if enabled != "true":
        logger.debug("Tag ownership sync disabled")
        return {"resolved": 0, "updated": 0, "skipped": 0, "disabled": True}

    # Build username -> user ID lookup (case-insensitive)
    all_users = db.get_all_users()
    users_by_username: dict[str, int] = {
        u.plex_username.lower(): u.id for u in all_users
    }

    sonarr = SonarrClient(config.sonarr)
    radarr = RadarrClient(config.radarr)

    resolved = 0
    updated = 0
    skipped = 0

    # Process Sonarr series
    try:
        sonarr_tags = await sonarr.get_tags()
        sonarr_tag_map = _build_tag_user_map(sonarr_tags, users_by_username)

        if sonarr_tag_map:
            all_series = await sonarr.get_all_series()
            for series in all_series:
                user_tag_ids = [t for t in series.tags if t in sonarr_tag_map]
                if not user_tag_ids:
                    continue

                tag_user_id = sonarr_tag_map[user_tag_ids[0]]

                # Find content by sonarr_id
                content = db.get_content_by_arr_id(sonarr_id=series.id)
                if content is None:
                    skipped += 1
                    continue

                r = _apply_tag_ownership(db, content.id, tag_user_id)
                resolved += r.get("resolved", 0)
                updated += r.get("updated", 0)
                skipped += r.get("skipped", 0)
    except Exception as e:
        logger.warning("Sonarr tag sync failed: %s", e)

    # Process Radarr movies
    try:
        radarr_tags = await radarr.get_tags()
        radarr_tag_map = _build_tag_user_map(radarr_tags, users_by_username)

        if radarr_tag_map:
            all_movies = await radarr.get_all_movies()
            for movie in all_movies:
                user_tag_ids = [t for t in movie.tags if t in radarr_tag_map]
                if not user_tag_ids:
                    continue

                tag_user_id = radarr_tag_map[user_tag_ids[0]]

                content = db.get_content_by_arr_id(radarr_id=movie.id)
                if content is None:
                    skipped += 1
                    continue

                r = _apply_tag_ownership(db, content.id, tag_user_id)
                resolved += r.get("resolved", 0)
                updated += r.get("updated", 0)
                skipped += r.get("skipped", 0)
    except Exception as e:
        logger.warning("Radarr tag sync failed: %s", e)

    logger.info(
        "Tag ownership sync complete: %d resolved, %d updated, %d skipped",
        resolved, updated, skipped,
    )
    return {"resolved": resolved, "updated": updated, "skipped": skipped}


def _apply_tag_ownership(db: Database, content_id: int, tag_user_id: int) -> dict:
    """Apply tag-based ownership to a single content item.

    Returns dict indicating what happened.
    """
    ownership = db.get_ownership(content_id)

    if ownership is None:
        # No owner - assign from tag
        db.set_ownership(content_id, tag_user_id)
        return {"resolved": 1}

    if ownership.status in ("promoted", "plank", "buried"):
        # Don't change ownership for non-standard states
        return {"skipped": 1}

    if ownership.owner_user_id == tag_user_id:
        # Already correct
        return {"skipped": 0}

    # Different owner - tag is more authoritative
    db.set_ownership(content_id, tag_user_id)
    logger.info(
        "Tag ownership override: content %d from user %d to user %d",
        content_id, ownership.owner_user_id, tag_user_id,
    )
    return {"updated": 1}
