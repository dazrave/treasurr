"""Promotion engine — detects and executes content promotion to shared plunder."""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database

logger = logging.getLogger(__name__)


async def run_promotions(db: Database, config: Config) -> int:
    """Check all owned content for promotion eligibility. Returns promotion count."""
    threshold = config.quotas.promotion_threshold
    candidates = db.get_owned_content_for_promotion()

    promoted = 0
    for item in candidates:
        if item.unique_viewers >= threshold:
            db.promote_content(item.content.id)
            db.log_promotion(
                content_id=item.content.id,
                from_user_id=item.ownership.owner_user_id,
                unique_viewers=item.unique_viewers,
                size_freed_bytes=item.content.size_bytes,
            )
            promoted += 1
            logger.info(
                "Promoted '%s' to shared plunder (%d viewers, freed %d bytes from user %d)",
                item.content.title,
                item.unique_viewers,
                item.content.size_bytes,
                item.ownership.owner_user_id,
            )

    if promoted > 0:
        logger.info("Promotion run complete: %d items promoted", promoted)
    return promoted
