"""Sync file sizes and disk space from Sonarr and Radarr."""

from __future__ import annotations

import json
import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.sync.clients import RadarrClient, SonarrClient

logger = logging.getLogger(__name__)


async def sync_disk_space(db: Database, config: Config) -> dict:
    """Fetch server disk space from Radarr and store in settings. Returns disk info."""
    radarr = RadarrClient(config.radarr)

    try:
        disks = await radarr.get_diskspace()
    except Exception as e:
        logger.warning("Failed to fetch disk space from Radarr: %s", e)
        return {}

    if not disks:
        return {}

    # Deduplicate disks - Radarr reports the same physical disk multiple times
    # under different mount paths. Group by total_bytes (same-sized disks are
    # almost certainly the same physical device) and keep the largest free value.
    seen_totals: dict[int, dict] = {}
    for d in disks:
        t = d["total_bytes"]
        if t not in seen_totals or d["free_bytes"] > seen_totals[t]["free_bytes"]:
            seen_totals[t] = d
    unique_disks = list(seen_totals.values())

    total_bytes = sum(d["total_bytes"] for d in unique_disks)
    free_bytes = sum(d["free_bytes"] for d in unique_disks)

    disk_info = {
        "total_bytes": total_bytes,
        "free_bytes": free_bytes,
        "disks": disks,
    }

    db.set_setting("disk_space", json.dumps(disk_info))
    logger.info("Disk space synced: %d total, %d free", total_bytes, free_bytes)
    return disk_info


async def sync_sizes(db: Database, config: Config) -> int:
    """Update file sizes for all active content from arr APIs. Returns update count."""
    sonarr = SonarrClient(config.sonarr)
    radarr = RadarrClient(config.radarr)

    # Build lookup maps from arr APIs: {tmdb_id: (size_bytes, title)}
    sonarr_by_tmdb: dict[int, tuple[int, str]] = {}
    radarr_by_tmdb: dict[int, tuple[int, str]] = {}

    try:
        all_series = await sonarr.get_all_series()
        for s in all_series:
            sonarr_by_tmdb[s.tmdb_id] = (s.size_bytes, s.title)
    except Exception as e:
        logger.warning("Failed to fetch Sonarr series: %s", e)

    try:
        all_movies = await radarr.get_all_movies()
        for m in all_movies:
            radarr_by_tmdb[m.tmdb_id] = (m.size_bytes, m.title)
    except Exception as e:
        logger.warning("Failed to fetch Radarr movies: %s", e)

    updated = 0
    for content in db.get_all_active_content():
        new_size = 0
        arr_title = ""
        if content.media_type == "show" and content.tmdb_id in sonarr_by_tmdb:
            new_size, arr_title = sonarr_by_tmdb[content.tmdb_id]
        elif content.media_type == "movie" and content.tmdb_id in radarr_by_tmdb:
            new_size, arr_title = radarr_by_tmdb[content.tmdb_id]

        changed = False
        if new_size > 0 and new_size != content.size_bytes:
            db.update_content_size(content.id, new_size)
            changed = True

        # Fix "Unknown" titles using data from Sonarr/Radarr
        if arr_title and content.title in ("Unknown", ""):
            db.update_content_title(content.id, arr_title)
            changed = True
            logger.info("Fixed title for content %d: '%s'", content.id, arr_title)

        if changed:
            updated += 1
            logger.debug("Updated '%s': %d bytes", arr_title or content.title, new_size)

    logger.info("Size sync complete: %d content items updated", updated)
    return updated


async def sync_arr_ids(db: Database, config: Config) -> int:
    """Link content records to their Sonarr/Radarr IDs for deletion support."""
    sonarr = SonarrClient(config.sonarr)
    radarr = RadarrClient(config.radarr)

    sonarr_by_tmdb: dict[int, int] = {}
    radarr_by_tmdb: dict[int, int] = {}

    try:
        for s in await sonarr.get_all_series():
            sonarr_by_tmdb[s.tmdb_id] = s.id
    except Exception as e:
        logger.warning("Failed to fetch Sonarr series for ID mapping: %s", e)

    try:
        for m in await radarr.get_all_movies():
            radarr_by_tmdb[m.tmdb_id] = m.id
    except Exception as e:
        logger.warning("Failed to fetch Radarr movies for ID mapping: %s", e)

    updated = 0
    for content in db.get_all_active_content():
        if content.media_type == "show" and content.sonarr_id is None and content.tmdb_id in sonarr_by_tmdb:
            db.update_content_arr_ids(content.id, sonarr_id=sonarr_by_tmdb[content.tmdb_id])
            updated += 1
        elif content.media_type == "movie" and content.radarr_id is None and content.tmdb_id in radarr_by_tmdb:
            db.update_content_arr_ids(content.id, radarr_id=radarr_by_tmdb[content.tmdb_id])
            updated += 1

    logger.info("Arr ID sync complete: %d content items linked", updated)
    return updated
