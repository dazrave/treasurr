"""Sync watch history from Tautulli/Jellyfin and trigger promotions."""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.sync.clients import JellyfinClient, TautulliClient

logger = logging.getLogger(__name__)


async def sync_watch_history(db: Database, config: Config) -> int:
    """Pull watch history from Tautulli and record events. Returns new event count."""
    client = TautulliClient(config.tautulli)
    threshold = config.quotas.watch_completion_percent

    history = await client.get_history(length=1000)

    created = 0
    for record in history:
        # Find the user
        user = db.get_user_by_plex_id(record.user_id)
        if user is None:
            continue

        # For episodes, match against the show (grandparent)
        # For movies, match directly
        # We need to find content by looking up what we have in our DB
        # Tautulli uses rating_key which is Plex-specific, not TMDB
        # We match by title as a fallback  - the request sync populates TMDB IDs
        # This is imperfect but works for MVP; Phase 2 can add better mapping

        completed = record.percent_complete >= threshold

        # Try to find matching content in our DB
        content_items = db.get_all_active_content()
        matched_content = None
        for c in content_items:
            if record.media_type == "episode" and c.media_type == "show":
                # Match show title from the grandparent title
                if c.title.lower() in record.title.lower():
                    matched_content = c
                    break
            elif record.media_type == "movie" and c.media_type == "movie":
                if c.title.lower() == record.title.lower():
                    matched_content = c
                    break

        if matched_content is None:
            continue

        try:
            db.add_watch_event(
                content_id=matched_content.id,
                user_id=user.id,
                watched_at=record.watched_at,
                completed=completed,
            )
            created += 1
        except Exception:
            # Duplicate event, skip
            pass

    logger.info("Watch sync complete: %d events recorded", created)
    return created


async def sync_users_from_tautulli(db: Database, config: Config) -> int:
    """Sync Plex users from Tautulli. Returns count of new/updated users."""
    client = TautulliClient(config.tautulli)
    users = await client.get_users()

    count = 0
    for u in users:
        if not u.user_id or u.user_id == "0":
            continue
        db.upsert_user(
            plex_user_id=u.user_id,
            plex_username=u.username,
            email=u.email,
            quota_bytes=config.quotas.default_bytes,
        )
        count += 1

    logger.info("User sync from Tautulli: %d users", count)
    return count


async def sync_users_from_jellyfin(db: Database, config: Config) -> int:
    """Sync users from Jellyfin. Returns count of new/updated users."""
    if not config.jellyfin.url or not config.jellyfin.api_key:
        return 0

    client = JellyfinClient(config.jellyfin)
    users = await client.get_users()

    count = 0
    for u in users:
        if not u.user_id:
            continue
        db.upsert_jellyfin_user(
            jellyfin_user_id=u.user_id,
            username=u.username,
            quota_bytes=config.quotas.default_bytes,
            is_admin=u.is_admin,
        )
        count += 1

    logger.info("User sync from Jellyfin: %d users", count)
    return count


async def sync_watch_history_from_jellyfin(db: Database, config: Config) -> int:
    """Pull watch history from Jellyfin and record events. Returns new event count."""
    if not config.jellyfin.url or not config.jellyfin.api_key:
        return 0

    client = JellyfinClient(config.jellyfin)
    jf_users = await client.get_users()

    created = 0
    for jf_user in jf_users:
        # Find matching Treasurr user
        user = db.get_user_by_jellyfin_id(jf_user.user_id)
        if user is None:
            continue

        history = await client.get_watch_history(jf_user.user_id, limit=500)

        for record in history:
            if not record.played:
                continue

            # Match content by title
            content_items = db.get_all_active_content()
            matched_content = None
            for c in content_items:
                if record.media_type == "episode" and c.media_type == "show":
                    if c.title.lower() in record.title.lower():
                        matched_content = c
                        break
                elif record.media_type == "movie" and c.media_type == "movie":
                    if c.title.lower() == record.title.lower():
                        matched_content = c
                        break

            if matched_content is None:
                continue

            try:
                db.add_watch_event(
                    content_id=matched_content.id,
                    user_id=user.id,
                    watched_at=record.watched_at,
                    completed=True,
                )
                created += 1
            except Exception:
                pass

    logger.info("Watch sync from Jellyfin: %d events recorded", created)
    return created
