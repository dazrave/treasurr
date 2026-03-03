"""Tests for the settings system."""

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from treasurr.app import create_app
from treasurr.config import Config, QuotaConfig, SafetyConfig
from treasurr.db import Database


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(path)
    yield database
    os.unlink(path)


@pytest.fixture
def app_and_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    config = Config(
        db_path=path,
        quotas=QuotaConfig(
            promotion_mode="full_plunder",
            shared_plunder_max_bytes=0,
            min_retention_days=0,
            display_mode="exact",
        ),
        safety=SafetyConfig(max_deletions_per_hour=10),
    )
    app = create_app(config)
    db = app.state.db

    from datetime import datetime, timedelta, timezone

    expires = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()

    admin = db.upsert_user(
        plex_user_id="admin_1",
        plex_username="captain",
        email="cap@sea.com",
        quota_bytes=500_000_000_000,
        is_admin=True,
    )
    db.create_session("admin-token", admin.id, "plex-token-admin", expires)

    user = db.upsert_user(
        plex_user_id="user_1",
        plex_username="deckhand",
        email="deck@sea.com",
        quota_bytes=500_000_000_000,
    )
    db.create_session("user-token", user.id, "plex-token-user", expires)

    yield app, db, admin, user, path
    os.unlink(path)


@pytest.fixture
def admin_client(app_and_db):
    app, _, _, _, _ = app_and_db
    client = TestClient(app)
    client.cookies.set("treasurr_session", "admin-token")
    return client


@pytest.fixture
def user_client(app_and_db):
    app, _, _, _, _ = app_and_db
    client = TestClient(app)
    client.cookies.set("treasurr_session", "user-token")
    return client


class TestSettingsDB:
    def test_get_setting_default(self, db: Database):
        assert db.get_setting("nonexistent", "fallback") == "fallback"

    def test_set_and_get_setting(self, db: Database):
        db.set_setting("promotion_mode", "split_the_loot")
        assert db.get_setting("promotion_mode") == "split_the_loot"

    def test_set_overwrites(self, db: Database):
        db.set_setting("key", "value1")
        db.set_setting("key", "value2")
        assert db.get_setting("key") == "value2"

    def test_get_all_settings(self, db: Database):
        db.set_setting("a", "1")
        db.set_setting("b", "2")
        settings = db.get_all_settings()
        assert settings == {"a": "1", "b": "2"}

    def test_get_all_settings_empty(self, db: Database):
        assert db.get_all_settings() == {}


class TestSettingsAPI:
    def test_get_settings_defaults(self, admin_client):
        resp = admin_client.get("/api/admin/settings")
        assert resp.status_code == 200
        data = resp.json()
        assert data["promotion_mode"] == "full_plunder"
        assert data["shared_plunder_max_bytes"] == 0
        assert data["min_retention_days"] == 0
        assert data["display_mode"] == "exact"

    def test_update_promotion_mode(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"promotion_mode": "split_the_loot"},
        )
        assert resp.status_code == 200
        assert resp.json()["promotion_mode"] == "split_the_loot"

    def test_update_display_mode(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"display_mode": "round_up"},
        )
        assert resp.status_code == 200
        assert resp.json()["display_mode"] == "round_up"

    def test_invalid_promotion_mode(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"promotion_mode": "invalid"},
        )
        assert resp.status_code == 400

    def test_invalid_display_mode(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"display_mode": "invalid"},
        )
        assert resp.status_code == 400

    def test_negative_plunder_cap(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"shared_plunder_max_bytes": -1},
        )
        assert resp.status_code == 400

    def test_negative_retention_days(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"min_retention_days": -1},
        )
        assert resp.status_code == 400

    def test_update_plunder_cap(self, admin_client):
        cap = 100 * 1024**3  # 100 GB
        resp = admin_client.put(
            "/api/admin/settings",
            json={"shared_plunder_max_bytes": cap},
        )
        assert resp.status_code == 200
        assert resp.json()["shared_plunder_max_bytes"] == cap

    def test_update_min_retention(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"min_retention_days": 14},
        )
        assert resp.status_code == 200
        assert resp.json()["min_retention_days"] == 14

    def test_non_admin_cannot_access(self, user_client):
        resp = user_client.get("/api/admin/settings")
        assert resp.status_code == 403

    def test_non_admin_cannot_update(self, user_client):
        resp = user_client.put(
            "/api/admin/settings",
            json={"promotion_mode": "split_the_loot"},
        )
        assert resp.status_code == 403
