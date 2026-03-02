"""Sync file sizes from Sonarr and Radarr into content records."""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.sync.clients import RadarrClient, SonarrClient

logger = logging.getLogger(__name__)


async def sync_sizes(db: Database, config: Config) -> int:
    """Update file sizes for all active content from arr APIs. Returns update count."""
    sonarr = SonarrClient(config.sonarr)
    radarr = RadarrClient(config.radarr)

    # Build lookup maps from arr APIs
    sonarr_by_tmdb: dict[int, int] = {}
    radarr_by_tmdb: dict[int, int] = {}

    try:
        all_series = await sonarr.get_all_series()
        for s in all_series:
            sonarr_by_tmdb[s.tmdb_id] = s.size_bytes
    except Exception as e:
        logger.warning("Failed to fetch Sonarr series: %s", e)

    try:
        all_movies = await radarr.get_all_movies()
        for m in all_movies:
            radarr_by_tmdb[m.tmdb_id] = m.size_bytes
    except Exception as e:
        logger.warning("Failed to fetch Radarr movies: %s", e)

    updated = 0
    for content in db.get_all_active_content():
        new_size = 0
        if content.media_type == "show" and content.tmdb_id in sonarr_by_tmdb:
            new_size = sonarr_by_tmdb[content.tmdb_id]
        elif content.media_type == "movie" and content.tmdb_id in radarr_by_tmdb:
            new_size = radarr_by_tmdb[content.tmdb_id]

        if new_size > 0 and new_size != content.size_bytes:
            db.update_content_size(content.id, new_size)
            updated += 1
            logger.debug("Updated size for '%s': %d bytes", content.title, new_size)

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
