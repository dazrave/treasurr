"""Overseerr webhook endpoint for quota enforcement."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request

from treasurr.db import Database
from treasurr.email import send_email
from treasurr.email_templates import quota_exceeded_template
from treasurr.engine.quota import format_bytes, get_user_quota
from treasurr.sync.clients import OverseerrClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/webhook", tags=["webhook"])


def _get_db(request: Request) -> Database:
    return request.app.state.db


def _get_config(request: Request):
    return request.app.state.config


@router.post("/overseerr")
async def overseerr_webhook(request: Request) -> dict:
    """Handle Overseerr webhook notifications.

    No cookie auth - Overseerr uses X-Webhook-Secret header instead.
    Handles MEDIA_PENDING and MEDIA_AUTO_APPROVED notification types.
    """
    db = _get_db(request)
    config = _get_config(request)

    # Validate webhook secret if configured
    stored_secret = db.get_setting("webhook_secret", "")
    if stored_secret:
        header_secret = request.headers.get("X-Webhook-Secret", "")
        if header_secret != stored_secret:
            logger.warning("Webhook rejected: invalid secret")
            return {"action": "rejected", "reason": "Invalid webhook secret"}

    body = await request.json()
    notification_type = body.get("notification_type", "")

    if notification_type not in ("MEDIA_PENDING", "MEDIA_AUTO_APPROVED"):
        return {"action": "ignored", "reason": f"Unhandled notification type: {notification_type}"}

    # Extract request details
    media = body.get("media", {})
    extra = body.get("extra", [])
    requested_by = body.get("request", {}).get("requestedBy", {})

    tmdb_id = media.get("tmdbId", 0)
    media_type = media.get("media_type", "movie")
    request_id = body.get("request", {}).get("request_id", 0)

    username = requested_by.get("username", "") or requested_by.get("displayName", "")
    email = requested_by.get("email", "")

    # Resolve title from extra data or media
    title = ""
    for item in extra:
        if item.get("name") == "mediaTitle":
            title = item.get("value", "")
            break
    if not title:
        title = media.get("title", media.get("name", "Unknown"))

    logger.info(
        "Webhook received: type=%s, tmdb_id=%s, user=%s, request_id=%s",
        notification_type, tmdb_id, username, request_id,
    )

    # Find user by username or email
    user = db.get_user_by_username(username)
    if not user and email:
        user = db.get_user_by_email(email)

    if not user:
        logger.warning("Webhook: could not find user '%s' (email: %s)", username, email)
        return {"action": "allowed", "reason": "User not found in Treasurr, allowing through"}

    # Check quota
    quota = get_user_quota(db, user.id, include_splits=True)
    if quota is None:
        return {"action": "allowed", "reason": "No quota data available"}

    if quota.usage_percent < 100:
        logger.info("Webhook: user %s at %.1f%% quota, allowing", user.plex_username, quota.usage_percent)
        return {"action": "allowed", "reason": f"User at {quota.usage_percent:.1f}% quota"}

    # Over quota - decline the request
    logger.info("Webhook: user %s at %.1f%% quota, declining request %s", user.plex_username, quota.usage_percent, request_id)

    # Try to decline via Overseerr API
    if config.overseerr and request_id:
        try:
            client = OverseerrClient(config.overseerr)
            await client.decline_request(request_id)
            logger.info("Declined Overseerr request %d for user %s", request_id, user.plex_username)
        except Exception as e:
            logger.error("Failed to decline Overseerr request %d: %s", request_id, e)

    # Send notification email
    if user.email:
        used_display = format_bytes(quota.total_used_bytes)
        total_display = format_bytes(quota.total_bytes)
        subject, html, text = quota_exceeded_template(
            username=user.plex_username,
            title=title,
            usage_percent=quota.usage_percent,
            used_display=used_display,
            total_display=total_display,
        )
        await send_email(db, user.email, subject, html, text)
        db.record_alert(user.id, "quota_exceeded", content_title=title)

    return {
        "action": "declined",
        "reason": f"User {user.plex_username} at {quota.usage_percent:.1f}% quota",
    }
