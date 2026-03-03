"""Plank engine — manages the grace period before content deletion."""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.engine.deletion import _execute_deletion
from treasurr.models import RescueResult

logger = logging.getLogger(__name__)


def _get_plank_days(db: Database, config: Config) -> int:
    raw = db.get_setting("plank_days", "")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return config.quotas.plank_days


def _get_plank_mode(db: Database, config: Config) -> str:
    return db.get_setting("plank_mode", "") or config.quotas.plank_mode


def _get_rescue_action(db: Database, config: Config) -> str:
    return db.get_setting("plank_rescue_action", "") or config.quotas.plank_rescue_action


async def run_plank_checks(db: Database, config: Config) -> dict:
    """Process expired plank content and auto-rescue watched adrift content.

    Returns summary dict with counts of expired and rescued items.
    """
    results = {"expired": 0, "rescued": 0}

    plank_days = _get_plank_days(db, config)
    if plank_days <= 0:
        return results

    plank_mode = _get_plank_mode(db, config)

    # 1. In adrift mode: check for content that's been watched → auto-rescue
    if plank_mode == "adrift":
        plank_items = db.get_plank_content()
        for item in plank_items:
            viewers = db.get_all_completed_viewer_ids(item.content.id)
            non_owner_viewers = [v for v in viewers if v != item.ownership.owner_user_id]
            if non_owner_viewers:
                rescuer_id = non_owner_viewers[0]
                result = await rescue_content(db, config, item.content.id, rescuer_id)
                if result.success:
                    results["rescued"] += 1
                    logger.info(
                        "Auto-rescued '%s' by user %d (action: %s)",
                        item.content.title, rescuer_id, result.action,
                    )

    # 2. Delete expired plank content
    expired = db.get_expired_plank_content(plank_days)
    for item in expired:
        delete_result = await _execute_deletion(
            db, config, item.content.id, item.ownership.owner_user_id,
        )
        if delete_result.success:
            results["expired"] += 1
            logger.info(
                "Plank expired — scuttled '%s' (%d bytes)",
                item.content.title, item.content.size_bytes,
            )
        else:
            logger.warning(
                "Failed to expire planked content '%s': %s",
                item.content.title, delete_result.message,
            )

    if results["expired"] > 0 or results["rescued"] > 0:
        logger.info("Plank check complete: %d expired, %d rescued", results["expired"], results["rescued"])

    return results


async def rescue_content(
    db: Database,
    config: Config,
    content_id: int,
    user_id: int,
) -> RescueResult:
    """Rescue content from the plank."""
    content = db.get_content(content_id)
    if content is None:
        return RescueResult(success=False, message="Content not found")

    ownership = db.get_ownership(content_id)
    if ownership is None:
        return RescueResult(success=False, message="Content has no owner")

    if ownership.status != "plank":
        return RescueResult(success=False, message="Content is not on the plank")

    plank_mode = _get_plank_mode(db, config)

    # Owner can always rescue their own content
    if user_id == ownership.owner_user_id:
        db.rescue_content(content_id)
        logger.info("Owner rescued '%s' from the plank", content.title)
        return RescueResult(
            success=True,
            message=f"'{content.title}' has been rescued!",
            action="rescued",
        )

    # Non-owner can only rescue in adrift mode
    if plank_mode != "adrift":
        return RescueResult(
            success=False,
            message="Only the owner can rescue content in anchored mode",
        )

    rescue_action = _get_rescue_action(db, config)

    if rescue_action == "adopt":
        db.adopt_content(content_id, user_id)
        logger.info("User %d adopted '%s' from the plank", user_id, content.title)
        return RescueResult(
            success=True,
            message=f"'{content.title}' has been adopted!",
            action="adopted",
        )

    # Default: promote
    db.promote_content(content_id)
    db.log_promotion(
        content_id,
        from_user_id=ownership.owner_user_id,
        unique_viewers=db.get_unique_viewers(content_id),
        size_freed_bytes=content.size_bytes,
    )
    logger.info("'%s' rescued and promoted to shared plunder", content.title)
    return RescueResult(
        success=True,
        message=f"'{content.title}' has been rescued and promoted to shared plunder!",
        action="promoted",
    )
