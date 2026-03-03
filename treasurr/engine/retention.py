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

    if scuttled > 0:
        logger.info("Retention run complete: %d items auto-scuttled", scuttled)
    return scuttled
