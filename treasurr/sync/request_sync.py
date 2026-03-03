"""Sync Overseerr/Seer requests into content ownership."""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.sync.clients import OverseerrClient

logger = logging.getLogger(__name__)


async def sync_requests(db: Database, config: Config) -> int:
    """Sync all requests from Overseerr/Seer. Returns count of new ownership records."""
    client = OverseerrClient(config.overseerr)
    requests = await client.get_all_requests()

    created = 0
    for req in requests:
        if req.tmdb_id == 0:
            continue

        media_type = "show" if req.media_type == "tv" else "movie"

        # Resolve title - Overseerr's /request endpoint often omits it
        title = req.title
        if not title or title == "Unknown":
            try:
                title = await client.get_media_title(req.tmdb_id, req.media_type)
            except Exception:
                pass
            if not title:
                title = "Unknown"

        # Match existing user by username first (Tautulli creates users with real
        # Plex IDs, Overseerr only has its own internal IDs). Fall back to creating
        # a new user if no match exists yet.
        user = db.get_user_by_username(req.requested_by_username)
        if user is None:
            user = db.upsert_user(
                plex_user_id=str(req.requested_by_user_id),
                plex_username=req.requested_by_username,
                email=req.requested_by_email,
                quota_bytes=config.quotas.default_bytes,
            )

        # Upsert content (will update title if it changed from Unknown)
        content = db.upsert_content(
            title=title,
            media_type=media_type,
            tmdb_id=req.tmdb_id,
            overseerr_request_id=req.request_id,
        )

        # Set ownership if not already owned
        existing = db.get_ownership(content.id)
        if existing is None:
            db.set_ownership(content.id, user.id)
            created += 1
            logger.info("Assigned %s '%s' to %s", media_type, title, user.plex_username)

    logger.info("Request sync complete: %d new ownership records", created)
    return created
