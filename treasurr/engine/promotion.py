"""Promotion engine — detects and executes content promotion to shared plunder."""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database

logger = logging.getLogger(__name__)


def _get_promotion_mode(db: Database, config: Config) -> str:
    """Read promotion_mode from settings, falling back to config."""
    return db.get_setting("promotion_mode", config.quotas.promotion_mode)


def _get_shared_plunder_max_bytes(db: Database, config: Config) -> int:
    """Read shared plunder cap from settings, falling back to config."""
    raw = db.get_setting("shared_plunder_max_bytes", "")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return config.quotas.shared_plunder_max_bytes


def _would_exceed_plunder_cap(db: Database, config: Config, additional_bytes: int = 0) -> bool:
    """Check whether promoting additional content would exceed the shared plunder cap."""
    cap = _get_shared_plunder_max_bytes(db, config)
    if cap <= 0:
        return False
    return (db.get_total_promoted_bytes() + additional_bytes) > cap


async def run_promotions(db: Database, config: Config) -> int:
    """Check all owned content for promotion eligibility. Returns promotion count."""
    threshold = config.quotas.promotion_threshold
    mode = _get_promotion_mode(db, config)
    candidates = db.get_owned_content_for_promotion()

    promoted = 0
    for item in candidates:
        if item.unique_viewers < threshold:
            continue

        if _would_exceed_plunder_cap(db, config, item.content.size_bytes):
            logger.info(
                "Shared plunder cap would be exceeded by '%s' (%d bytes) — skipping",
                item.content.title,
                item.content.size_bytes,
            )
            continue

        db.promote_content(item.content.id)
        db.log_promotion(
            content_id=item.content.id,
            from_user_id=item.ownership.owner_user_id,
            unique_viewers=item.unique_viewers,
            size_freed_bytes=item.content.size_bytes,
        )

        if mode == "split_the_loot":
            viewer_ids = db.get_all_completed_viewer_ids(item.content.id)
            if viewer_ids:
                db.recalculate_splits(
                    item.content.id, viewer_ids, item.content.size_bytes
                )
                logger.info(
                    "Created quota splits for '%s': %d bytes across %d viewers",
                    item.content.title,
                    item.content.size_bytes,
                    len(viewer_ids),
                )

        promoted += 1
        logger.info(
            "Promoted '%s' to shared plunder (%d viewers, freed %d bytes from user %d, mode=%s)",
            item.content.title,
            item.unique_viewers,
            item.content.size_bytes,
            item.ownership.owner_user_id,
            mode,
        )

    # Second pass: recalculate splits for already-promoted content with new viewers
    if mode == "split_the_loot":
        _recalculate_existing_splits(db)

    if promoted > 0:
        logger.info("Promotion run complete: %d items promoted", promoted)
    return promoted


def _recalculate_existing_splits(db: Database) -> None:
    """Recalculate quota splits for already-promoted content that has gained new viewers."""
    promoted_content = db.get_promoted_content()

    for content in promoted_content:
        viewer_ids = db.get_all_completed_viewer_ids(content.id)
        if not viewer_ids:
            continue

        db.recalculate_splits(content.id, viewer_ids, content.size_bytes)
