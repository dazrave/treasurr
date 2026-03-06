"""Plex/Jellyfin collection sync for planked content."""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.sync.clients import JellyfinClient, PlexClient

logger = logging.getLogger(__name__)

COLLECTION_TITLE = "Walking the Plank"


async def sync_plank_collection(db: Database, config: Config) -> dict:
    """Sync a collection/playlist with all currently planked content.

    For Plex: creates/updates a Plex collection.
    For Jellyfin: creates/updates a Jellyfin playlist.

    Returns summary dict with counts.
    """
    results = {"synced": 0, "created": False, "removed": False}

    use_jellyfin = config.media_server in ("jellyfin", "both")
    use_plex = config.media_server in ("plex", "both")

    if use_jellyfin and config.jellyfin.url and config.jellyfin.api_key:
        try:
            jf_results = await _sync_jellyfin_plank_playlist(db, config)
            results["synced"] += jf_results.get("synced", 0)
            results["created"] = results["created"] or jf_results.get("created", False)
        except Exception as e:
            logger.error("Jellyfin plank playlist sync failed: %s", e)

    if not use_plex:
        return results

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


async def _sync_jellyfin_plank_playlist(db: Database, config: Config) -> dict:
    """Sync planked content to a Jellyfin playlist."""
    results = {"synced": 0, "created": False}

    jf = JellyfinClient(config.jellyfin)

    # Get planked content
    plank_items = db.get_plank_content()
    if not plank_items:
        return results

    # Find Jellyfin item IDs for planked content by title
    matched_ids: list[str] = []
    for item in plank_items:
        try:
            search_results = await jf.search_by_title(item.content.title)
            for result in search_results:
                if result["name"].lower() == item.content.title.lower():
                    matched_ids.append(result["id"])
                    break
        except Exception as e:
            logger.warning("Failed to find '%s' in Jellyfin: %s", item.content.title, e)

    if not matched_ids:
        return results

    # Get the admin user ID for playlist creation
    users = await jf.get_users()
    admin_user = next((u for u in users if u.is_admin), None)
    if not admin_user:
        logger.warning("No Jellyfin admin user found for playlist creation")
        return results

    # Check if playlist already exists (via settings table)
    playlist_id = db.get_setting("jellyfin_plank_playlist_id")

    if playlist_id:
        try:
            await jf.update_playlist_items(playlist_id, matched_ids)
            results["synced"] = len(matched_ids)
            logger.info("Updated Jellyfin plank playlist with %d items", len(matched_ids))
        except Exception:
            # Playlist may have been deleted, create a new one
            playlist_id = None

    if not playlist_id:
        try:
            playlist_id = await jf.create_playlist(COLLECTION_TITLE, matched_ids, admin_user.user_id)
            if playlist_id:
                db.set_setting("jellyfin_plank_playlist_id", playlist_id)
                results["created"] = True
                results["synced"] = len(matched_ids)
                logger.info("Created Jellyfin plank playlist with %d items", len(matched_ids))
        except Exception as e:
            logger.error("Failed to create Jellyfin plank playlist: %s", e)
