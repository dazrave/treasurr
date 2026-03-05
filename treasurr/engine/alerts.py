"""Quota threshold email alerts.

Checks all users against 75% and 95% quota thresholds and sends warning emails.
"""

from __future__ import annotations

import logging

from treasurr.config import Config
from treasurr.db import Database
from treasurr.email import send_email
from treasurr.email_templates import quota_warning_template
from treasurr.engine.quota import format_bytes, get_user_quota

logger = logging.getLogger(__name__)

_THRESHOLDS = [
    (95, "quota_95"),
    (75, "quota_75"),
]


async def check_quota_alerts(db: Database, config: Config) -> int:
    """Check all users against quota thresholds. Send warning emails as needed.

    Returns count of alerts sent.
    """
    users = db.get_all_users()
    alerts_sent = 0

    for user in users:
        if not user.email:
            continue

        quota = get_user_quota(db, user.id, include_splits=True)
        if quota is None or quota.total_bytes == 0:
            continue

        for threshold, alert_type in _THRESHOLDS:
            if quota.usage_percent >= threshold:
                if not db.has_active_alert(user.id, alert_type):
                    used_display = format_bytes(quota.total_used_bytes)
                    total_display = format_bytes(quota.total_bytes)
                    subject, html, text = quota_warning_template(
                        username=user.plex_username,
                        threshold=threshold,
                        usage_percent=quota.usage_percent,
                        used_display=used_display,
                        total_display=total_display,
                    )
                    sent = await send_email(db, user.email, subject, html, text)
                    if sent:
                        db.record_alert(user.id, alert_type)
                        alerts_sent += 1
                        logger.info(
                            "Sent %d%% quota warning to %s (%.1f%% used)",
                            threshold, user.plex_username, quota.usage_percent,
                        )
                # Only process the highest matching threshold
                break
            else:
                # Below threshold - clear alert so it re-arms
                if db.has_active_alert(user.id, alert_type):
                    db.clear_alerts(user.id, alert_type)
                    logger.debug(
                        "Cleared %s alert for %s (now at %.1f%%)",
                        alert_type, user.plex_username, quota.usage_percent,
                    )

    if alerts_sent:
        logger.info("Alerts: sent %d quota warnings", alerts_sent)

    return alerts_sent
