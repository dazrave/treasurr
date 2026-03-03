"""Retention engine  - auto-scuttles content based on user retention policies."""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.engine.deletion import scuttle_content

logger = logging.getLogger(__name__)


def _get_min_retention_days(db: Database, config: Config) -> int:
    """Read min_retention_days from settings, falling back to config."""
    raw = db.get_setting("min_retention_days", "")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return config.quotas.min_retention_days


def _get_stale_content_days(db: Database, config: Config) -> int:
    """Read stale_content_days from settings, falling back to 0 (disabled)."""
    raw = db.get_setting("stale_content_days", "")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return 0


async def run_retention_checks(db: Database, config: Config) -> int:
    """Auto-scuttle content for users with retention policies. Returns scuttle count."""
    min_retention = _get_min_retention_days(db, config)
    users = db.get_users_with_auto_scuttle()

    scuttled = 0
    for user in users:
        eligible = db.get_retention_eligible_content(
            user_id=user.id,
            scuttle_days=user.auto_scuttle_days,
            min_retention_days=min_retention,
        )

        for content in eligible:
            result = await scuttle_content(
                db=db,
                config=config,
                content_id=content.id,
                user_id=user.id,
            )

            if result.success:
                scuttled += 1
                logger.info(
                    "Auto-scuttled '%s' for user %s (retention: %d days, freed %d bytes)",
                    content.title,
                    user.plex_username,
                    user.auto_scuttle_days,
                    result.freed_bytes,
                )
            else:
                logger.warning(
                    "Failed to auto-scuttle '%s' for user %s: %s",
                    content.title,
                    user.plex_username,
                    result.message,
                )

    # Global stale content auto-plank (includes unclaimed content)
    stale_days = _get_stale_content_days(db, config)
    if stale_days > 0:
        stale_items = db.get_stale_content(stale_days)
        for content in stale_items:
            ownership = db.get_ownership(content.id)
            if ownership is None:
                # Unclaimed stale content - skip (no owner to scuttle for)
                continue
            result = await scuttle_content(
                db=db,
                config=config,
                content_id=content.id,
                user_id=ownership.owner_user_id,
            )
            if result.success:
                scuttled += 1
                logger.info(
                    "Stale auto-planked '%s' (unwatched for %d+ days, freed %d bytes)",
                    content.title,
                    stale_days,
                    result.freed_bytes,
                )

    if scuttled > 0:
        logger.info("Retention run complete: %d items auto-scuttled", scuttled)
    return scuttled
