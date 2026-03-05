"""SMTP email subsystem for Treasurr notifications."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import aiosmtplib
from email.message import EmailMessage

from treasurr.db import Database

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SmtpConfig:
    host: str
    port: int
    username: str
    password: str
    from_address: str
    use_tls: bool
    enabled: bool


def load_smtp_config(db: Database) -> SmtpConfig:
    """Load SMTP config from settings table, falling back to env vars."""
    settings = db.get_all_settings()

    enabled = settings.get("smtp_enabled", os.environ.get("TREASURR_SMTP_ENABLED", "false"))
    host = settings.get("smtp_host", os.environ.get("TREASURR_SMTP_HOST", ""))
    port_str = settings.get("smtp_port", os.environ.get("TREASURR_SMTP_PORT", "587"))
    username = settings.get("smtp_username", os.environ.get("TREASURR_SMTP_USERNAME", ""))
    password = settings.get("smtp_password", os.environ.get("TREASURR_SMTP_PASSWORD", ""))
    from_address = settings.get("smtp_from", os.environ.get("TREASURR_SMTP_FROM", ""))
    use_tls_str = settings.get("smtp_use_tls", os.environ.get("TREASURR_SMTP_USE_TLS", "true"))

    return SmtpConfig(
        host=host,
        port=int(port_str) if port_str else 587,
        username=username,
        password=password,
        from_address=from_address,
        use_tls=use_tls_str.lower() in ("true", "1", "yes"),
        enabled=enabled.lower() in ("true", "1", "yes"),
    )


async def send_email(db: Database, to: str, subject: str, html: str, text: str) -> bool:
    """Send an email via SMTP. Returns True on success, False on failure. Never raises."""
    try:
        smtp_config = load_smtp_config(db)

        if not smtp_config.enabled:
            logger.debug("Email not sent (SMTP disabled): %s -> %s", subject, to)
            return False

        if not smtp_config.host or not smtp_config.from_address:
            logger.warning("Email not sent (SMTP not configured): %s -> %s", subject, to)
            return False

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = smtp_config.from_address
        msg["To"] = to
        msg.set_content(text)
        msg.add_alternative(html, subtype="html")

        await aiosmtplib.send(
            msg,
            hostname=smtp_config.host,
            port=smtp_config.port,
            username=smtp_config.username or None,
            password=smtp_config.password or None,
            start_tls=smtp_config.use_tls,
        )

        logger.info("Email sent: %s -> %s", subject, to)
        return True
    except Exception as e:
        logger.error("Failed to send email to %s: %s", to, e)
        return False
