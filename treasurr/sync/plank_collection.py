"""Plex collection sync for planked content."""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.sync.clients import PlexClient

logger = logging.getLogger(__name__)

COLLECTION_TITLE = "Walking the Plank"


async def sync_plank_collection(db: Database, config: Config) -> dict:
    """Sync a Plex collection with all currently planked content.

    Creates the collection if it doesn't exist, updates items if it does,
    and removes the collection if no planked items remain.

    Returns summary dict with counts.
    """
    results = {"synced": 0, "created": False, "removed": False}

    if not config.plex.url or not config.plex.token:
        return results

    plex = PlexClient(config.plex)

    # Get planked content titles
    plank_items = db.get_plank_content()
    plank_titles = {item.content.title: item.content for item in plank_items}

    if not plank_titles:
        # No planked items - remove collection if it exists
        try:
            libraries = await plex.get_libraries()
            for lib in libraries:
                existing = await plex.find_collection_by_title(lib["key"], COLLECTION_TITLE)
                if existing:
                    await plex.delete_collection(existing["ratingKey"])
                    results["removed"] = True
                    logger.info("Removed empty plank collection from library '%s'", lib["title"])
        except Exception as e:
            logger.warning("Failed to clean up plank collection: %s", e)
        return results

    # Find matching Plex items across all libraries
    try:
        libraries = await plex.get_libraries()
    except Exception as e:
        logger.error("Failed to get Plex libraries: %s", e)
        return results

    for lib in libraries:
        if lib["type"] not in ("movie", "show"):
            continue

        # Search for each planked title in this library
        matched_keys: list[str] = []
        for title in plank_titles:
            try:
                search_results = await plex.search_by_title(lib["key"], title)
                for result in search_results:
                    if result["title"] == title:
                        matched_keys.append(result["ratingKey"])
                        break
            except Exception as e:
                logger.warning("Failed to search for '%s' in Plex: %s", title, e)

        if not matched_keys:
            # No matches in this library - remove collection if it exists
            try:
                existing = await plex.find_collection_by_title(lib["key"], COLLECTION_TITLE)
                if existing:
                    await plex.delete_collection(existing["ratingKey"])
                    results["removed"] = True
            except Exception:
                pass
            continue

        # Find or create the collection
        try:
            existing = await plex.find_collection_by_title(lib["key"], COLLECTION_TITLE)
            if existing:
                await plex.update_collection_items(existing["ratingKey"], matched_keys)
                logger.info(
                    "Updated plank collection in '%s' with %d items",
                    lib["title"], len(matched_keys),
                )
            else:
                await plex.create_collection(lib["key"], COLLECTION_TITLE, matched_keys)
                results["created"] = True
                logger.info(
                    "Created plank collection in '%s' with %d items",
                    lib["title"], len(matched_keys),
                )
            results["synced"] += len(matched_keys)
        except Exception as e:
            logger.error("Failed to sync plank collection in '%s': %s", lib["title"], e)

    return results
