"""Tests for the external API v1 endpoints and API key management."""

import hashlib
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from treasurr.app import create_app
from treasurr.config import Config, QuotaConfig, QuotaTier, SafetyConfig


TEST_API_KEY = "test-api-key-for-tests"
TEST_API_KEY_HASH = hashlib.sha256(TEST_API_KEY.encode()).hexdigest()


@pytest.fixture
def app_and_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    config = Config(
        db_path=path,
        quotas=QuotaConfig(
            default_bytes=500_000_000_000,
            promotion_threshold=2,
            tiers=(
                QuotaTier(name="Small", bytes=100_000_000_000),
                QuotaTier(name="Medium", bytes=500_000_000_000),
                QuotaTier(name="Large", bytes=1_000_000_000_000),
            ),
        ),
        safety=SafetyConfig(max_deletions_per_hour=10),
    )
    app = create_app(config)
    db = app.state.db

    # Create test users
    user = db.upsert_user(
        plex_user_id="test_plex_1",
        plex_username="testpirate",
        email="test@sea.com",
        quota_bytes=500_000_000_000,
    )
    admin = db.upsert_user(
        plex_user_id="admin_plex_1",
        plex_username="captain",
        email="captain@sea.com",
        quota_bytes=500_000_000_000,
        is_admin=True,
    )

    # Create admin session for cookie auth
    expires = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    db.create_session("admin-token-123", admin.id, "plex-token-admin", expires)

    # Create test API key
    db.create_api_key("test-key", TEST_API_KEY_HASH)

    yield app, db, user, admin, path
    os.unlink(path)


@pytest.fixture
def api_client(app_and_db):
    app, _, _, _, _ = app_and_db
    client = TestClient(app)
    client.headers["Authorization"] = f"Bearer {TEST_API_KEY}"
    return client


@pytest.fixture
def admin_cookie_client(app_and_db):
    app, _, _, _, _ = app_and_db
    client = TestClient(app)
    client.cookies.set("treasurr_session", "admin-token-123")
    return client


@pytest.fixture
def unauth_client(app_and_db):
    app, _, _, _, _ = app_and_db
    return TestClient(app)


class TestApiKeyAuth:
    def test_no_auth_returns_401(self, unauth_client):
        resp = unauth_client.get("/api/v1/users")
        assert resp.status_code == 401
        assert resp.headers.get("WWW-Authenticate") == "Bearer"

    def test_bad_key_returns_401(self, unauth_client):
        resp = unauth_client.get(
            "/api/v1/users",
            headers={"Authorization": "Bearer bad-key-here"},
        )
        assert resp.status_code == 401

    def test_valid_key_works(self, api_client):
        resp = api_client.get("/api/v1/users")
        assert resp.status_code == 200

    def test_bearer_prefix_required(self, unauth_client):
        resp = unauth_client.get(
            "/api/v1/users",
            headers={"Authorization": TEST_API_KEY},
        )
        assert resp.status_code == 401

    def test_touch_updates_last_used(self, api_client, app_and_db):
        _, db, _, _, _ = app_and_db
        api_client.get("/api/v1/users")
        keys = db.list_api_keys()
        assert keys[0]["last_used_at"] is not None


class TestApiKeyManagement:
    def test_create_returns_key(self, admin_cookie_client):
        resp = admin_cookie_client.post(
            "/api/admin/api-keys",
            json={"name": "my-integration"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "key" in data
        assert data["name"] == "my-integration"
        assert "warning" in data

    def test_list_hides_hash(self, admin_cookie_client):
        resp = admin_cookie_client.get("/api/admin/api-keys")
        assert resp.status_code == 200
        data = resp.json()
        keys = data["keys"]
        assert len(keys) >= 1
        for key in keys:
            assert "key_hash" not in key
            assert "name" in key

    def test_revoke_works(self, admin_cookie_client, app_and_db):
        _, db, _, _, _ = app_and_db
        # Create a key to revoke
        resp = admin_cookie_client.post(
            "/api/admin/api-keys",
            json={"name": "to-revoke"},
        )
        key_id = resp.json()["id"]
        resp = admin_cookie_client.delete(f"/api/admin/api-keys/{key_id}")
        assert resp.status_code == 200

    def test_revoke_404_missing(self, admin_cookie_client):
        resp = admin_cookie_client.delete("/api/admin/api-keys/99999")
        assert resp.status_code == 404

    def test_name_required(self, admin_cookie_client):
        resp = admin_cookie_client.post(
            "/api/admin/api-keys",
            json={"name": ""},
        )
        assert resp.status_code == 400

    def test_non_admin_blocked(self, app_and_db):
        app, db, user, _, _ = app_and_db
        expires = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        db.create_session("user-token-123", user.id, "plex-token", expires)
        client = TestClient(app)
        client.cookies.set("treasurr_session", "user-token-123")
        resp = client.get("/api/admin/api-keys")
        assert resp.status_code == 403


class TestV1Users:
    def test_list_with_quotas(self, api_client):
        resp = api_client.get("/api/v1/users")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "total" in data
        assert data["total"] >= 2
        user_item = data["items"][0]
        assert "quota_bytes" in user_item
        assert "usage_percent" in user_item

    def test_get_single(self, api_client, app_and_db):
        _, _, user, _, _ = app_and_db
        resp = api_client.get(f"/api/v1/users/{user.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["username"] == "testpirate"
        assert "tier" in data

    def test_404_missing(self, api_client):
        resp = api_client.get("/api/v1/users/99999")
        assert resp.status_code == 404

    def test_get_user_content(self, api_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Test Movie", media_type="movie", tmdb_id=100, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)
        resp = api_client.get(f"/api/v1/users/{user.id}/content")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["title"] == "Test Movie"


class TestV1Content:
    def test_list_all(self, api_client, app_and_db):
        _, db, _, _, _ = app_and_db
        db.upsert_content(title="Movie A", media_type="movie", tmdb_id=200, size_bytes=1_000_000)
        db.upsert_content(title="Show B", media_type="show", tmdb_id=201, size_bytes=2_000_000)
        resp = api_client.get("/api/v1/content")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 2

    def test_filter_media_type(self, api_client, app_and_db):
        _, db, _, _, _ = app_and_db
        db.upsert_content(title="Movie F", media_type="movie", tmdb_id=300, size_bytes=1_000_000)
        db.upsert_content(title="Show F", media_type="show", tmdb_id=301, size_bytes=2_000_000)
        resp = api_client.get("/api/v1/content?media_type=movie")
        assert resp.status_code == 200
        data = resp.json()
        for item in data["items"]:
            assert item["media_type"] == "movie"

    def test_get_single(self, api_client, app_and_db):
        _, db, _, _, _ = app_and_db
        content = db.upsert_content(title="Single Movie", media_type="movie", tmdb_id=400, size_bytes=3_000_000)
        resp = api_client.get(f"/api/v1/content/{content.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["title"] == "Single Movie"
        assert "owner" in data

    def test_404_missing(self, api_client):
        resp = api_client.get("/api/v1/content/99999")
        assert resp.status_code == 404

    def test_leaving(self, api_client):
        resp = api_client.get("/api/v1/content/leaving")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data

    def test_latest(self, api_client, app_and_db):
        _, db, _, _, _ = app_and_db
        db.upsert_content(title="Latest Movie", media_type="movie", tmdb_id=500, size_bytes=1_000_000)
        resp = api_client.get("/api/v1/content/latest?limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 1

    def test_shared(self, api_client):
        resp = api_client.get("/api/v1/content/shared")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data


class TestV1Tiers:
    def test_returns_config_tiers(self, api_client):
        resp = api_client.get("/api/v1/tiers")
        assert resp.status_code == 200
        data = resp.json()
        tiers = data["tiers"]
        assert len(tiers) == 3
        assert tiers[0]["name"] == "Small"
        assert tiers[1]["name"] == "Medium"
        assert tiers[2]["name"] == "Large"
        assert "bytes" in tiers[0]
        assert "display" in tiers[0]


class TestV1Stats:
    def test_correct_shape(self, api_client):
        resp = api_client.get("/api/v1/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_bytes" in data
        assert "total_display" in data
        assert "owned_bytes" in data
        assert "promoted_bytes" in data
        assert "user_count" in data
        assert "content_count" in data


class TestV1UserWrite:
    def test_set_tier_valid(self, api_client, app_and_db):
        _, _, user, _, _ = app_and_db
        resp = api_client.put(
            f"/api/v1/users/{user.id}/tier",
            json={"tier": "Large"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["quota_bytes"] == 1_000_000_000_000
        assert data["tier"] == "Large"

    def test_set_tier_invalid(self, api_client, app_and_db):
        _, _, user, _, _ = app_and_db
        resp = api_client.put(
            f"/api/v1/users/{user.id}/tier",
            json={"tier": "NonExistent"},
        )
        assert resp.status_code == 400

    def test_set_quota_and_bonus(self, api_client, app_and_db):
        _, _, user, _, _ = app_and_db
        resp = api_client.put(
            f"/api/v1/users/{user.id}/quota",
            json={"quota_bytes": 200_000_000_000, "bonus_bytes": 50_000_000_000},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["quota_bytes"] == 200_000_000_000
        assert data["bonus_bytes"] == 50_000_000_000

    def test_negative_quota_rejected(self, api_client, app_and_db):
        _, _, user, _, _ = app_and_db
        resp = api_client.put(
            f"/api/v1/users/{user.id}/quota",
            json={"quota_bytes": -100},
        )
        assert resp.status_code == 400

    def test_reset_quota(self, api_client, app_and_db):
        _, db, user, _, _ = app_and_db
        # First set a non-default quota
        db.update_user_quota(user.id, quota_bytes=999_000_000_000, bonus_bytes=100_000)
        resp = api_client.delete(f"/api/v1/users/{user.id}/quota")
        assert resp.status_code == 200
        data = resp.json()
        assert data["quota_bytes"] == 500_000_000_000
        assert data["bonus_bytes"] == 0

    def test_404_missing_user(self, api_client):
        resp = api_client.put(
            "/api/v1/users/99999/tier",
            json={"tier": "Small"},
        )
        assert resp.status_code == 404
