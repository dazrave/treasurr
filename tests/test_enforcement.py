"""Tests for email subsystem, webhook, enforcement engine, and alert thresholds."""

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from treasurr.app import create_app
from treasurr.config import ApiConfig, Config, QuotaConfig, SafetyConfig
from treasurr.db import Database
from treasurr.email import load_smtp_config, send_email
from treasurr.email_templates import (
    download_cancelled_template,
    quota_exceeded_template,
    quota_warning_template,
)
from treasurr.engine.alerts import check_quota_alerts
from treasurr.engine.enforcement import enforce_download_quotas


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(path)
    yield database
    os.unlink(path)


@pytest.fixture
def config():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return Config(
        db_path=path,
        quotas=QuotaConfig(default_bytes=500_000_000_000, promotion_threshold=2),
        safety=SafetyConfig(max_deletions_per_hour=10),
        overseerr=ApiConfig(url="http://localhost:5055/api/v1", key="test-key"),
        sonarr=ApiConfig(url="http://localhost:8989/api/v3", key="test-key"),
        radarr=ApiConfig(url="http://localhost:7878/api/v3", key="test-key"),
    )


@pytest.fixture
def app_and_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    app_config = Config(
        db_path=path,
        quotas=QuotaConfig(default_bytes=500_000_000_000, promotion_threshold=2),
        safety=SafetyConfig(max_deletions_per_hour=10),
        overseerr=ApiConfig(url="http://localhost:5055/api/v1", key="test-key"),
    )
    app = create_app(app_config)
    app_db = app.state.db

    # Create admin user
    admin = app_db.upsert_user(
        plex_user_id="admin_1",
        plex_username="captain",
        email="captain@sea.com",
        quota_bytes=500_000_000_000,
        is_admin=True,
    )
    expires = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    app_db.create_session("admin-token", admin.id, "plex-token", expires)

    # Create regular user
    user = app_db.upsert_user(
        plex_user_id="user_1",
        plex_username="pirate",
        email="pirate@sea.com",
        quota_bytes=1000,
    )

    yield app, app_db, user, admin, path
    os.unlink(path)


@pytest.fixture
def webhook_client(app_and_db):
    app, _, _, _, _ = app_and_db
    return TestClient(app)


@pytest.fixture
def admin_client(app_and_db):
    app, _, _, _, _ = app_and_db
    client = TestClient(app)
    client.cookies.set("treasurr_session", "admin-token")
    return client


# --- Email Template Tests ---


class TestEmailTemplates:
    def test_quota_warning_template(self):
        subject, html, text = quota_warning_template(
            username="pirate", threshold=75, usage_percent=76.3,
            used_display="381.5 GB", total_display="500.0 GB",
        )
        assert "75%" in subject
        assert "pirate" in html
        assert "76%" in html
        assert "381.5 GB" in html
        assert "pirate" in text
        assert "76%" in text

    def test_quota_exceeded_template(self):
        subject, html, text = quota_exceeded_template(
            username="pirate", title="The Matrix",
            usage_percent=105.2, used_display="526.0 GB", total_display="500.0 GB",
        )
        assert "The Matrix" in subject
        assert "Declined" in html
        assert "pirate" in text

    def test_download_cancelled_template(self):
        subject, html, text = download_cancelled_template(
            username="pirate", title="Breaking Bad S01E01",
            reason="Storage quota exceeded",
        )
        assert "Breaking Bad" in subject
        assert "Cancelled" in html
        assert "Storage quota exceeded" in text


# --- SMTP Config Tests ---


class TestSmtpConfig:
    def test_load_from_settings(self, db: Database):
        db.set_setting("smtp_enabled", "true")
        db.set_setting("smtp_host", "smtp.test.com")
        db.set_setting("smtp_port", "465")
        db.set_setting("smtp_from", "test@test.com")
        db.set_setting("smtp_username", "user")
        db.set_setting("smtp_password", "pass")
        db.set_setting("smtp_use_tls", "false")

        cfg = load_smtp_config(db)
        assert cfg.enabled is True
        assert cfg.host == "smtp.test.com"
        assert cfg.port == 465
        assert cfg.from_address == "test@test.com"
        assert cfg.username == "user"
        assert cfg.password == "pass"
        assert cfg.use_tls is False

    def test_defaults_when_no_settings(self, db: Database):
        cfg = load_smtp_config(db)
        assert cfg.enabled is False
        assert cfg.host == ""
        assert cfg.port == 587
        assert cfg.use_tls is True

    async def test_send_email_disabled(self, db: Database):
        result = await send_email(db, "test@test.com", "Test", "<p>Hi</p>", "Hi")
        assert result is False


# --- Database Alert Methods ---


class TestDatabaseAlerts:
    def test_has_active_alert_none(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=1000)
        assert db.has_active_alert(user.id, "quota_75") is False

    def test_record_and_check_alert(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=1000)
        db.record_alert(user.id, "quota_75")
        assert db.has_active_alert(user.id, "quota_75") is True
        assert db.has_active_alert(user.id, "quota_95") is False

    def test_clear_alerts(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=1000)
        db.record_alert(user.id, "quota_75")
        assert db.has_active_alert(user.id, "quota_75") is True
        db.clear_alerts(user.id, "quota_75")
        assert db.has_active_alert(user.id, "quota_75") is False

    def test_get_user_by_email(self, db: Database):
        db.upsert_user(plex_user_id="1", plex_username="pirate", email="pirate@sea.com", quota_bytes=1000)
        user = db.get_user_by_email("pirate@sea.com")
        assert user is not None
        assert user.plex_username == "pirate"
        assert db.get_user_by_email("nobody@sea.com") is None

    def test_get_user_by_username(self, db: Database):
        db.upsert_user(plex_user_id="1", plex_username="CaptainJack", quota_bytes=1000)
        user = db.get_user_by_username("captainjack")
        assert user is not None
        assert user.plex_username == "CaptainJack"
        assert db.get_user_by_username("nobody") is None


# --- Webhook Tests ---


class TestWebhook:
    def test_ignored_notification_type(self, webhook_client):
        resp = webhook_client.post("/api/webhook/overseerr", json={
            "notification_type": "MEDIA_AVAILABLE",
        })
        assert resp.status_code == 200
        assert resp.json()["action"] == "ignored"

    def test_invalid_secret(self, app_and_db, webhook_client):
        _, app_db, _, _, _ = app_and_db
        app_db.set_setting("webhook_secret", "correct-secret")
        resp = webhook_client.post(
            "/api/webhook/overseerr",
            json={"notification_type": "MEDIA_PENDING"},
            headers={"X-Webhook-Secret": "wrong-secret"},
        )
        assert resp.json()["action"] == "rejected"

    def test_valid_secret_passes(self, app_and_db, webhook_client):
        _, app_db, _, _, _ = app_and_db
        app_db.set_setting("webhook_secret", "my-secret")
        resp = webhook_client.post(
            "/api/webhook/overseerr",
            json={
                "notification_type": "MEDIA_PENDING",
                "media": {"tmdbId": 999, "media_type": "movie"},
                "request": {"request_id": 1, "requestedBy": {"username": "nobody"}},
            },
            headers={"X-Webhook-Secret": "my-secret"},
        )
        assert resp.status_code == 200
        # User not found, so allowed through
        assert resp.json()["action"] == "allowed"

    @patch("treasurr.api.webhook.send_email", new_callable=AsyncMock, return_value=True)
    @patch("treasurr.api.webhook.OverseerrClient")
    def test_over_quota_declines(self, mock_overseerr_cls, mock_send, app_and_db, webhook_client):
        _, app_db, user, _, _ = app_and_db
        # Fill user's quota
        c = app_db.upsert_content(title="Big Movie", media_type="movie", tmdb_id=100, size_bytes=1200)
        app_db.set_ownership(c.id, user.id)

        mock_client = AsyncMock()
        mock_overseerr_cls.return_value = mock_client

        resp = webhook_client.post("/api/webhook/overseerr", json={
            "notification_type": "MEDIA_PENDING",
            "media": {"tmdbId": 200, "media_type": "movie"},
            "request": {"request_id": 42, "requestedBy": {"username": "pirate", "email": "pirate@sea.com"}},
            "extra": [{"name": "mediaTitle", "value": "New Movie"}],
        })
        assert resp.status_code == 200
        assert resp.json()["action"] == "declined"
        mock_client.decline_request.assert_called_once_with(42)
        mock_send.assert_called_once()

    def test_under_quota_allows(self, app_and_db, webhook_client):
        _, app_db, user, _, _ = app_and_db
        # User has 1000 bytes quota and no content
        resp = webhook_client.post("/api/webhook/overseerr", json={
            "notification_type": "MEDIA_AUTO_APPROVED",
            "media": {"tmdbId": 300, "media_type": "movie"},
            "request": {"request_id": 10, "requestedBy": {"username": "pirate"}},
        })
        assert resp.status_code == 200
        assert resp.json()["action"] == "allowed"


# --- Enforcement Engine Tests ---


class TestEnforcement:
    @patch("treasurr.engine.enforcement.send_email", new_callable=AsyncMock, return_value=True)
    @patch("treasurr.engine.enforcement.SonarrClient")
    async def test_cancels_over_quota_download(self, mock_sonarr_cls, mock_send, db: Database, config):
        user = db.upsert_user(plex_user_id="1", plex_username="pirate", email="p@sea.com", quota_bytes=1000)
        c = db.upsert_content(title="Show", media_type="show", tmdb_id=50, size_bytes=800)
        db.set_ownership(c.id, user.id)

        # Queue item that would push over quota
        db.set_setting("download_queue", json.dumps([{
            "arr_type": "sonarr",
            "arr_id": 1,
            "queue_id": 99,
            "tmdb_id": 50,
            "title": "Show S01E02",
            "size_bytes": 300,
            "sizeleft_bytes": 200,
            "progress": 33.3,
            "eta": "1h 30m",
            "status": "downloading",
        }]))

        mock_client = AsyncMock()
        mock_sonarr_cls.return_value = mock_client

        cancelled = await enforce_download_quotas(db, config)
        assert cancelled == 1
        mock_client.delete_queue_item.assert_called_once_with(99)
        mock_send.assert_called_once()

    async def test_allows_within_quota_download(self, db: Database, config):
        user = db.upsert_user(plex_user_id="1", plex_username="pirate", quota_bytes=10000)
        c = db.upsert_content(title="Movie", media_type="movie", tmdb_id=60, size_bytes=100)
        db.set_ownership(c.id, user.id)

        db.set_setting("download_queue", json.dumps([{
            "arr_type": "radarr",
            "arr_id": 1,
            "queue_id": 50,
            "tmdb_id": 60,
            "title": "Movie",
            "size_bytes": 200,
            "sizeleft_bytes": 100,
            "progress": 50.0,
            "eta": "30m",
            "status": "downloading",
        }]))

        cancelled = await enforce_download_quotas(db, config)
        assert cancelled == 0

    async def test_empty_queue(self, db: Database, config):
        cancelled = await enforce_download_quotas(db, config)
        assert cancelled == 0

    async def test_unknown_tmdb_id_skipped(self, db: Database, config):
        db.set_setting("download_queue", json.dumps([{
            "arr_type": "radarr",
            "arr_id": 1,
            "queue_id": 10,
            "tmdb_id": 99999,
            "title": "Unknown",
            "size_bytes": 500,
            "sizeleft_bytes": 200,
            "progress": 60.0,
            "eta": "10m",
            "status": "downloading",
        }]))

        cancelled = await enforce_download_quotas(db, config)
        assert cancelled == 0


# --- Alert Threshold Tests ---


class TestAlerts:
    @patch("treasurr.engine.alerts.send_email", new_callable=AsyncMock, return_value=True)
    async def test_75_percent_alert(self, mock_send, db: Database, config):
        user = db.upsert_user(plex_user_id="1", plex_username="pirate", email="p@sea.com", quota_bytes=1000)
        c = db.upsert_content(title="Movie", media_type="movie", tmdb_id=1, size_bytes=760)
        db.set_ownership(c.id, user.id)

        sent = await check_quota_alerts(db, config)
        assert sent == 1
        assert db.has_active_alert(user.id, "quota_75") is True
        mock_send.assert_called_once()
        # Subject should mention 75%
        call_args = mock_send.call_args
        assert "75%" in call_args[0][2]  # subject

    @patch("treasurr.engine.alerts.send_email", new_callable=AsyncMock, return_value=True)
    async def test_95_percent_alert(self, mock_send, db: Database, config):
        user = db.upsert_user(plex_user_id="1", plex_username="pirate", email="p@sea.com", quota_bytes=1000)
        c = db.upsert_content(title="Movie", media_type="movie", tmdb_id=1, size_bytes=960)
        db.set_ownership(c.id, user.id)

        sent = await check_quota_alerts(db, config)
        assert sent == 1
        assert db.has_active_alert(user.id, "quota_95") is True
        # 95 takes priority over 75
        assert db.has_active_alert(user.id, "quota_75") is False

    @patch("treasurr.engine.alerts.send_email", new_callable=AsyncMock, return_value=True)
    async def test_no_alert_when_under_threshold(self, mock_send, db: Database, config):
        user = db.upsert_user(plex_user_id="1", plex_username="pirate", email="p@sea.com", quota_bytes=1000)
        c = db.upsert_content(title="Movie", media_type="movie", tmdb_id=1, size_bytes=500)
        db.set_ownership(c.id, user.id)

        sent = await check_quota_alerts(db, config)
        assert sent == 0
        mock_send.assert_not_called()

    @patch("treasurr.engine.alerts.send_email", new_callable=AsyncMock, return_value=True)
    async def test_no_duplicate_alert(self, mock_send, db: Database, config):
        user = db.upsert_user(plex_user_id="1", plex_username="pirate", email="p@sea.com", quota_bytes=1000)
        c = db.upsert_content(title="Movie", media_type="movie", tmdb_id=1, size_bytes=760)
        db.set_ownership(c.id, user.id)

        # First run sends alert
        sent1 = await check_quota_alerts(db, config)
        assert sent1 == 1
        # Second run should not send again
        sent2 = await check_quota_alerts(db, config)
        assert sent2 == 0

    @patch("treasurr.engine.alerts.send_email", new_callable=AsyncMock, return_value=True)
    async def test_alert_rearms_after_drop(self, mock_send, db: Database, config):
        user = db.upsert_user(plex_user_id="1", plex_username="pirate", email="p@sea.com", quota_bytes=1000)
        c = db.upsert_content(title="Movie", media_type="movie", tmdb_id=1, size_bytes=760)
        db.set_ownership(c.id, user.id)

        # Send initial alert
        await check_quota_alerts(db, config)
        assert db.has_active_alert(user.id, "quota_75") is True

        # Simulate dropping below threshold by reducing content size
        db.update_content_size(c.id, 500)

        # Check again - should clear the alert
        await check_quota_alerts(db, config)
        assert db.has_active_alert(user.id, "quota_75") is False

        # Now go back over 75%
        db.update_content_size(c.id, 760)
        sent = await check_quota_alerts(db, config)
        assert sent == 1  # Re-armed and fires again

    async def test_no_email_for_users_without_email(self, db: Database, config):
        user = db.upsert_user(plex_user_id="1", plex_username="pirate", email="", quota_bytes=1000)
        c = db.upsert_content(title="Movie", media_type="movie", tmdb_id=1, size_bytes=760)
        db.set_ownership(c.id, user.id)

        sent = await check_quota_alerts(db, config)
        assert sent == 0


# --- Admin Settings API Tests ---


class TestAdminEmailSettings:
    def test_get_settings_includes_smtp(self, admin_client):
        resp = admin_client.get("/api/admin/settings")
        assert resp.status_code == 200
        data = resp.json()
        assert "smtp_enabled" in data
        assert "smtp_host" in data
        assert "smtp_port" in data
        assert "smtp_password_set" in data
        assert "webhook_secret_set" in data
        assert data["smtp_enabled"] is False
        assert data["smtp_password_set"] is False

    def test_update_smtp_settings(self, admin_client):
        resp = admin_client.put("/api/admin/settings", json={
            "smtp_enabled": True,
            "smtp_host": "smtp.test.com",
            "smtp_port": 465,
            "smtp_from": "test@test.com",
            "smtp_username": "user",
            "smtp_password": "secret",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["smtp_enabled"] is True
        assert data["smtp_host"] == "smtp.test.com"
        assert data["smtp_port"] == 465
        assert data["smtp_password_set"] is True

    def test_webhook_secret_saved(self, admin_client):
        resp = admin_client.put("/api/admin/settings", json={
            "webhook_secret": "my-secret-key",
        })
        assert resp.status_code == 200
        assert resp.json()["webhook_secret_set"] is True

    @patch("treasurr.api.admin.send_email", new_callable=AsyncMock, return_value=False)
    def test_test_email_fails_gracefully(self, mock_send, admin_client):
        resp = admin_client.post("/api/admin/settings/test-email")
        assert resp.status_code == 500

    @patch("treasurr.api.admin.send_email", new_callable=AsyncMock, return_value=True)
    def test_test_email_success(self, mock_send, admin_client):
        resp = admin_client.post("/api/admin/settings/test-email")
        assert resp.status_code == 200
        assert "captain@sea.com" in resp.json()["message"]
