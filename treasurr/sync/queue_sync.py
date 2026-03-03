"""Sync download queue from Sonarr and Radarr."""

from __future__ import annotations

import json
import logging
import re

from treasurr.config import Config
from treasurr.db import Database
from treasurr.sync.clients import RadarrClient, SonarrClient

logger = logging.getLogger(__name__)


def _parse_timeleft(timeleft: str) -> str:
    """Convert Sonarr/Radarr timeleft string like '02:15:30' to human-readable."""
    if not timeleft:
        return ""
    match = re.match(r"(\d+):(\d+):(\d+)", timeleft)
    if not match:
        return timeleft
    hours, minutes, _ = int(match.group(1)), int(match.group(2)), int(match.group(3))
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


async def sync_download_queue(db: Database, config: Config) -> int:
    """Fetch download queues from Sonarr and Radarr, store as JSON setting."""
    queue_items: list[dict] = []

    if config.sonarr:
        try:
            sonarr = SonarrClient(config.sonarr)
            sonarr_queue = await sonarr.get_queue()
            for item in sonarr_queue:
                series = item.get("series", {})
                episode = item.get("episode", {})
                title = series.get("title", "Unknown")
                if episode.get("seasonNumber") and episode.get("episodeNumber"):
                    title += f" S{episode['seasonNumber']:02d}E{episode['episodeNumber']:02d}"
                size_total = item.get("size", 0)
                size_left = item.get("sizeleft", 0)
                progress = 0
                if size_total > 0:
                    progress = round(((size_total - size_left) / size_total) * 100, 1)
                queue_items.append({
                    "arr_type": "sonarr",
                    "arr_id": series.get("id"),
                    "title": title,
                    "size_bytes": int(size_total),
                    "sizeleft_bytes": int(size_left),
                    "progress": progress,
                    "eta": _parse_timeleft(item.get("timeleft", "")),
                    "status": item.get("status", ""),
                })
        except Exception as e:
            logger.error("Sonarr queue sync failed: %s", e)

    if config.radarr:
        try:
            radarr = RadarrClient(config.radarr)
            radarr_queue = await radarr.get_queue()
            for item in radarr_queue:
                movie = item.get("movie", {})
                size_total = item.get("size", 0)
                size_left = item.get("sizeleft", 0)
                progress = 0
                if size_total > 0:
                    progress = round(((size_total - size_left) / size_total) * 100, 1)
                queue_items.append({
                    "arr_type": "radarr",
                    "arr_id": movie.get("id"),
                    "title": movie.get("title", "Unknown"),
                    "size_bytes": int(size_total),
                    "sizeleft_bytes": int(size_left),
                    "progress": progress,
                    "eta": _parse_timeleft(item.get("timeleft", "")),
                    "status": item.get("status", ""),
                })
        except Exception as e:
            logger.error("Radarr queue sync failed: %s", e)

    db.set_setting("download_queue", json.dumps(queue_items))
    logger.info("Download queue synced: %d items", len(queue_items))
    return len(queue_items)
