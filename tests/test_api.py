"""Tests for the FastAPI endpoints."""

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from treasurr.app import create_app
from treasurr.config import Config, QuotaConfig, SafetyConfig


@pytest.fixture
def app_and_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    config = Config(
        db_path=path,
        quotas=QuotaConfig(
            default_bytes=500_000_000_000,
            promotion_threshold=2,
        ),
        safety=SafetyConfig(max_deletions_per_hour=10),
    )
    app = create_app(config)
    db = app.state.db

    # Create a test user and session
    user = db.upsert_user(
        plex_user_id="test_plex_1",
        plex_username="testpirate",
        email="test@sea.com",
        quota_bytes=500_000_000_000,
    )
    from datetime import datetime, timedelta, timezone
    expires = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    db.create_session("test-token-123", user.id, "plex-token", expires)

    # Create an admin user
    admin = db.upsert_user(
        plex_user_id="admin_plex_1",
        plex_username="captain",
        email="captain@sea.com",
        quota_bytes=500_000_000_000,
        is_admin=True,
    )
    db.create_session("admin-token-123", admin.id, "plex-token-admin", expires)

    yield app, db, user, admin, path
    os.unlink(path)


@pytest.fixture
def client(app_and_db):
    app, _, _, _, _ = app_and_db
    return TestClient(app)


@pytest.fixture
def auth_client(app_and_db):
    app, _, _, _, _ = app_and_db
    client = TestClient(app)
    client.cookies.set("treasurr_session", "test-token-123")
    return client


@pytest.fixture
def admin_client(app_and_db):
    app, _, _, _, _ = app_and_db
    client = TestClient(app)
    client.cookies.set("treasurr_session", "admin-token-123")
    return client


class TestHealth:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestAuthRequired:
    def test_treasure_requires_auth(self, client):
        resp = client.get("/api/treasure")
        assert resp.status_code == 401

    def test_chest_requires_auth(self, client):
        resp = client.get("/api/treasure/chest")
        assert resp.status_code == 401

    def test_admin_requires_auth(self, client):
        resp = client.get("/api/admin/crew")
        assert resp.status_code == 401


class TestTreasure:
    def test_get_treasure_summary(self, auth_client, app_and_db):
        resp = auth_client.get("/api/treasure")
        assert resp.status_code == 200
        data = resp.json()
        assert data["username"] == "testpirate"
        assert data["used_bytes"] == 0
        assert data["usage_percent"] == 0

    def test_get_empty_chest(self, auth_client):
        resp = auth_client.get("/api/treasure/chest")
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []

    def test_get_chest_with_content(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)

        resp = auth_client.get("/api/treasure/chest")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["title"] == "Film"
        assert data["items"][0]["can_scuttle"] is True

    def test_get_plunder(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Popular Film", media_type="movie", tmdb_id=1, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)
        db.promote_content(content.id)

        resp = auth_client.get("/api/plunder")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["title"] == "Popular Film"

    def test_get_activity(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=1000)
        db.log_promotion(content.id, user.id, 3, 1000)

        resp = auth_client.get("/api/activity")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["events"]) == 1
        assert data["events"][0]["type"] == "promotion"


class TestAdmin:
    def test_non_admin_rejected(self, auth_client):
        resp = auth_client.get("/api/admin/crew")
        assert resp.status_code == 403

    def test_get_crew(self, admin_client, app_and_db):
        resp = admin_client.get("/api/admin/crew")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["crew"]) >= 2  # test user + admin

    def test_get_stats(self, admin_client):
        resp = admin_client.get("/api/admin/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_bytes" in data
        assert "user_count" in data

    def test_update_crew_quota(self, admin_client, app_and_db):
        _, _, user, _, _ = app_and_db
        resp = admin_client.put(
            f"/api/admin/crew/{user.id}",
            json={"quota_bytes": 1_000_000_000_000},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["quota_bytes"] == 1_000_000_000_000


class TestScuttle:
    def test_scuttle_unowned_content(self, auth_client, app_and_db):
        _, db, _, _, _ = app_and_db
        content = db.upsert_content(title="Not Mine", media_type="movie", tmdb_id=99)
        resp = auth_client.post(f"/api/treasure/{content.id}/scuttle")
        assert resp.status_code == 400

    def test_scuttle_promoted_content(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Promoted", media_type="movie", tmdb_id=99)
        db.set_ownership(content.id, user.id)
        db.promote_content(content.id)
        resp = auth_client.post(f"/api/treasure/{content.id}/scuttle")
        assert resp.status_code == 400

    def test_scuttle_nonexistent(self, auth_client):
        resp = auth_client.post("/api/treasure/9999/scuttle")
        assert resp.status_code == 400
