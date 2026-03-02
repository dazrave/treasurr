"""Background sync scheduler."""

from __future__ import annotations

import asyncio
import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.engine.promotion import run_promotions
from treasurr.sync.request_sync import sync_requests
from treasurr.sync.size_sync import sync_arr_ids, sync_sizes
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
        results["watches"] = await sync_watch_history(db, config)
    except Exception as e:
        logger.error("Watch sync failed: %s", e)
        results["watches_error"] = str(e)

    try:
        results["promotions"] = await run_promotions(db, config)
    except Exception as e:
        logger.error("Promotion engine failed: %s", e)
        results["promotions_error"] = str(e)

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
