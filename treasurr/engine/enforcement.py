"""Download queue quota enforcement.

Monitors the download queue and cancels downloads that would push users over quota.
"""

from __future__ import annotations

import json
import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.email import send_email
from treasurr.email_templates import download_cancelled_template
from treasurr.engine.quota import format_bytes, get_user_quota
from treasurr.sync.clients import OverseerrClient, RadarrClient, SonarrClient

logger = logging.getLogger(__name__)


async def enforce_download_quotas(db: Database, config: Config) -> int:
    """Check downloading items against owner quotas. Cancel if over quota.

    Returns count of cancelled downloads.
    """
    queue_raw = db.get_setting("download_queue", "[]")
    try:
        queue_items = json.loads(queue_raw)
    except (json.JSONDecodeError, TypeError):
        return 0

    if not queue_items:
        return 0

    cancelled = 0

    for item in queue_items:
        tmdb_id = item.get("tmdb_id", 0)
        if not tmdb_id:
            continue

        # Find the content in our database
        media_type = "show" if item["arr_type"] == "sonarr" else "movie"
        content = db.get_content_by_tmdb(tmdb_id, media_type)
        if not content:
            continue

        # Find the owner
        ownership = db.get_ownership(content.id)
        if not ownership:
            continue

        user = db.get_user(ownership.owner_user_id)
        if not user:
            continue

        # Check if current usage + download size exceeds quota
        quota = get_user_quota(db, user.id, include_splits=True)
        if quota is None:
            continue

        download_size = item.get("size_bytes", 0)
        if (quota.total_used_bytes + download_size) <= quota.total_bytes:
            continue

        # Over quota - cancel the download
        queue_id = item.get("queue_id")
        if not queue_id:
            continue

        logger.info(
            "Cancelling download '%s' for user %s (at %.1f%% + %s would exceed quota)",
            item["title"], user.plex_username, quota.usage_percent, format_bytes(download_size),
        )

        try:
            if item["arr_type"] == "sonarr" and config.sonarr:
                client = SonarrClient(config.sonarr)
                await client.delete_queue_item(queue_id)
            elif item["arr_type"] == "radarr" and config.radarr:
                client = RadarrClient(config.radarr)
                await client.delete_queue_item(queue_id)
            else:
                continue
        except Exception as e:
            logger.error("Failed to cancel queue item %s: %s", queue_id, e)
            continue

        # Try to decline the Overseerr request
        if config.overseerr and content.overseerr_request_id:
            try:
                overseerr = OverseerrClient(config.overseerr)
                await overseerr.decline_request(content.overseerr_request_id)
            except Exception as e:
                logger.warning("Failed to decline Overseerr request %s: %s", content.overseerr_request_id, e)

        # Email the user
        if user.email:
            reason = (
                f"Adding this download ({format_bytes(download_size)}) would push your "
                f"storage over quota ({format_bytes(quota.total_used_bytes)} used of "
                f"{format_bytes(quota.total_bytes)})."
            )
            subject, html, text = download_cancelled_template(
                username=user.plex_username,
                title=item["title"],
                reason=reason,
            )
            await send_email(db, user.email, subject, html, text)
            db.record_alert(user.id, "download_cancelled", content_title=item["title"])

        cancelled += 1

    if cancelled:
        logger.info("Enforcement: cancelled %d over-quota downloads", cancelled)

    return cancelled
