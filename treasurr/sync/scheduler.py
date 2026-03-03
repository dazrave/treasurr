"""Background sync scheduler."""

from __future__ import annotations

import asyncio
import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.engine.plank import run_plank_checks
from treasurr.engine.promotion import run_promotions
from treasurr.engine.retention import run_retention_checks
from treasurr.sync.request_sync import sync_requests
from treasurr.sync.queue_sync import sync_download_queue
from treasurr.sync.size_sync import sync_arr_ids, sync_disk_space, sync_posters, sync_seasons, sync_sizes
from treasurr.sync.watch_sync import sync_users_from_tautulli, sync_watch_history

logger = logging.getLogger(__name__)


async def run_full_sync(db: Database, config: Config) -> dict:
    """Run all sync operations in sequence. Returns summary dict."""
    results = {}

    try:
        results["users"] = await sync_users_from_tautulli(db, config)
    except Exception as e:
        logger.error("User sync failed: %s", e)
        results["users_error"] = str(e)

    try:
        results["requests"] = await sync_requests(db, config)
    except Exception as e:
        logger.error("Request sync failed: %s", e)
        results["requests_error"] = str(e)

    try:
        results["arr_ids"] = await sync_arr_ids(db, config)
    except Exception as e:
        logger.error("Arr ID sync failed: %s", e)
        results["arr_ids_error"] = str(e)

    try:
        results["sizes"] = await sync_sizes(db, config)
    except Exception as e:
        logger.error("Size sync failed: %s", e)
        results["sizes_error"] = str(e)

    try:
        results["disk_space"] = await sync_disk_space(db, config)
    except Exception as e:
        logger.error("Disk space sync failed: %s", e)
        results["disk_space_error"] = str(e)

    try:
        results["seasons"] = await sync_seasons(db, config)
    except Exception as e:
        logger.error("Season sync failed: %s", e)
        results["seasons_error"] = str(e)

    try:
        results["posters"] = await sync_posters(db, config)
    except Exception as e:
        logger.error("Poster sync failed: %s", e)
        results["posters_error"] = str(e)

    try:
        results["download_queue"] = await sync_download_queue(db, config)
    except Exception as e:
        logger.error("Download queue sync failed: %s", e)
        results["download_queue_error"] = str(e)

    try:
        results["watches"] = await sync_watch_history(db, config)
    except Exception as e:
        logger.error("Watch sync failed: %s", e)
        results["watches_error"] = str(e)

    try:
        results["promotions"] = await run_promotions(db, config)
    except Exception as e:
        logger.error("Promotion engine failed: %s", e)
        results["promotions_error"] = str(e)

    try:
        results["retention"] = await run_retention_checks(db, config)
    except Exception as e:
        logger.error("Retention checks failed: %s", e)
        results["retention_error"] = str(e)

    try:
        results["plank"] = await run_plank_checks(db, config)
    except Exception as e:
        logger.error("Plank checks failed: %s", e)
        results["plank_error"] = str(e)

    # Cleanup expired sessions
    try:
        db.cleanup_expired_sessions()
    except Exception as e:
        logger.error("Session cleanup failed: %s", e)

    logger.info("Full sync complete: %s", results)
    return results


async def start_scheduler(db: Database, config: Config) -> None:
    """Run sync loop forever at the configured interval."""
    interval = config.sync_interval_seconds
    logger.info("Scheduler started, syncing every %d seconds", interval)

    while True:
        try:
            await run_full_sync(db, config)
        except Exception as e:
            logger.error("Sync cycle failed: %s", e)

        await asyncio.sleep(interval)
