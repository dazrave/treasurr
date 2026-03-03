"""Tests for admin bulk operations, tiers, and disk stats."""

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from treasurr.app import create_app
from treasurr.config import Config, QuotaConfig, QuotaTier, SafetyConfig


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
                QuotaTier(name="Bottle of Rum", bytes=536_870_912_000),
                QuotaTier(name="Case of Rum", bytes=2_199_023_255_552),
                QuotaTier(name="Barrel of Rum", bytes=5_497_558_138_880),
            ),
        ),
        safety=SafetyConfig(max_deletions_per_hour=10),
    )
    app = create_app(config)
    db = app.state.db

    from datetime import datetime, timedelta, timezone

    expires = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()

    user1 = db.upsert_user(
        plex_user_id="bulk_user_1",
        plex_username="pirate1",
        email="p1@sea.com",
        quota_bytes=500_000_000_000,
    )
    user2 = db.upsert_user(
        plex_user_id="bulk_user_2",
        plex_username="pirate2",
        email="p2@sea.com",
        quota_bytes=500_000_000_000,
    )
    user3 = db.upsert_user(
        plex_user_id="bulk_user_3",
        plex_username="pirate3",
        email="p3@sea.com",
        quota_bytes=500_000_000_000,
    )
    admin = db.upsert_user(
        plex_user_id="admin_bulk",
        plex_username="admiral",
        email="admiral@sea.com",
        quota_bytes=500_000_000_000,
        is_admin=True,
    )
    db.create_session("admin-bulk-token", admin.id, "plex-token-admin", expires)
    db.create_session("user1-token", user1.id, "plex-token-1", expires)

    yield app, db, [user1, user2, user3], admin, path
    os.unlink(path)


@pytest.fixture
def admin_client(app_and_db):
    app, _, _, _, _ = app_and_db
    client = TestClient(app)
    client.cookies.set("treasurr_session", "admin-bulk-token")
    return client


@pytest.fixture
def user_client(app_and_db):
    app, _, _, _, _ = app_and_db
    client = TestClient(app)
    client.cookies.set("treasurr_session", "user1-token")
    return client


class TestTiersEndpoint:
    def test_get_tiers(self, admin_client):
        resp = admin_client.get("/api/admin/tiers")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["tiers"]) == 3
        assert data["tiers"][0]["name"] == "Bottle of Rum"
        assert data["tiers"][0]["bytes"] == 536_870_912_000
        assert data["tiers"][1]["name"] == "Case of Rum"
        assert data["tiers"][2]["name"] == "Barrel of Rum"

    def test_tiers_require_admin(self, user_client):
        resp = user_client.get("/api/admin/tiers")
        assert resp.status_code == 403


class TestBulkUpdate:
    def test_bulk_update_quota(self, admin_client, app_and_db):
        _, db, users, _, _ = app_and_db
        user_ids = [u.id for u in users]
        new_quota = 2_199_023_255_552  # 2 TB

        resp = admin_client.put(
            "/api/admin/crew/bulk",
            json={"user_ids": user_ids, "quota_bytes": new_quota},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["updated"] == 3
        assert data["quota_bytes"] == new_quota

        # Verify all users were updated
        for uid in user_ids:
            user = db.get_user(uid)
            assert user.quota_bytes == new_quota

    def test_bulk_update_empty_ids(self, admin_client):
        resp = admin_client.put(
            "/api/admin/crew/bulk",
            json={"user_ids": [], "quota_bytes": 1_000_000},
        )
        assert resp.status_code == 400

    def test_bulk_update_missing_quota(self, admin_client, app_and_db):
        _, _, users, _, _ = app_and_db
        resp = admin_client.put(
            "/api/admin/crew/bulk",
            json={"user_ids": [users[0].id]},
        )
        assert resp.status_code == 400

    def test_bulk_update_require_admin(self, user_client, app_and_db):
        _, _, users, _, _ = app_and_db
        resp = user_client.put(
            "/api/admin/crew/bulk",
            json={"user_ids": [users[0].id], "quota_bytes": 1000},
        )
        assert resp.status_code == 403

    def test_bulk_creates_transactions(self, admin_client, app_and_db):
        _, db, users, _, _ = app_and_db
        user_ids = [users[0].id, users[1].id]

        admin_client.put(
            "/api/admin/crew/bulk",
            json={"user_ids": user_ids, "quota_bytes": 536_870_912_000},
        )

        # Check transactions were created
        with db.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM quota_transactions WHERE reason = 'admin_bulk_grant'"
            ).fetchall()
            assert len(rows) >= 2


class TestCrewActivity:
    def test_crew_includes_activity(self, admin_client, app_and_db):
        _, db, users, _, _ = app_and_db

        # Create some content ownership to generate activity
        content = db.upsert_content(
            title="Test Film", media_type="movie", tmdb_id=1, size_bytes=5_000_000_000
        )
        db.set_ownership(content.id, users[0].id)

        resp = admin_client.get("/api/admin/crew")
        assert resp.status_code == 200
        data = resp.json()

        # Find user1 in the response
        user1_data = next(c for c in data["crew"] if c["username"] == "pirate1")
        assert "last_request_at" in user1_data
        assert "request_count" in user1_data
        assert user1_data["request_count"] >= 1

    def test_crew_no_activity_shows_zero(self, admin_client, app_and_db):
        resp = admin_client.get("/api/admin/crew")
        data = resp.json()
        # Users without content should have 0 requests
        user3_data = next(c for c in data["crew"] if c["username"] == "pirate3")
        assert user3_data["request_count"] == 0
        assert user3_data["last_request_at"] is None


class TestDiskStats:
    def test_stats_include_disk_fields(self, admin_client):
        resp = admin_client.get("/api/admin/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "disk_total_bytes" in data
        assert "disk_free_bytes" in data
        assert "user_storage" in data

    def test_stats_disk_defaults_to_zero(self, admin_client):
        resp = admin_client.get("/api/admin/stats")
        data = resp.json()
        # No disk space synced yet, should be 0
        assert data["disk_total_bytes"] == 0
        assert data["disk_free_bytes"] == 0

    def test_stats_with_synced_disk(self, admin_client, app_and_db):
        _, db, _, _, _ = app_and_db
        import json

        disk_info = {
            "total_bytes": 30_000_000_000_000,
            "free_bytes": 10_000_000_000_000,
            "disks": [{"path": "/data", "total_bytes": 30_000_000_000_000, "free_bytes": 10_000_000_000_000}],
        }
        db.set_setting("disk_space", json.dumps(disk_info))

        resp = admin_client.get("/api/admin/stats")
        data = resp.json()
        assert data["disk_total_bytes"] == 30_000_000_000_000
        assert data["disk_free_bytes"] == 10_000_000_000_000

    def test_stats_user_storage_breakdown(self, admin_client, app_and_db):
        _, db, users, _, _ = app_and_db
        content = db.upsert_content(
            title="Big Movie", media_type="movie", tmdb_id=42, size_bytes=10_000_000_000
        )
        db.set_ownership(content.id, users[0].id)

        resp = admin_client.get("/api/admin/stats")
        data = resp.json()
        assert len(data["user_storage"]) >= 1
        pirate1_storage = next(
            (u for u in data["user_storage"] if u["username"] == "pirate1"), None
        )
        assert pirate1_storage is not None
        assert pirate1_storage["used_bytes"] == 10_000_000_000


class TestDatabaseBulk:
    def test_bulk_update_quota_db(self, app_and_db):
        _, db, users, _, _ = app_and_db
        user_ids = [users[0].id, users[1].id]
        updated = db.bulk_update_quota(user_ids, 999_000_000_000)
        assert updated == 2

        for uid in user_ids:
            user = db.get_user(uid)
            assert user.quota_bytes == 999_000_000_000

        # User 3 should be unchanged
        user3 = db.get_user(users[2].id)
        assert user3.quota_bytes == 500_000_000_000

    def test_bulk_update_empty_list(self, app_and_db):
        _, db, _, _, _ = app_and_db
        updated = db.bulk_update_quota([], 999_000_000_000)
        assert updated == 0

    def test_user_activity(self, app_and_db):
        _, db, users, _, _ = app_and_db
        content = db.upsert_content(
            title="Activity Test", media_type="movie", tmdb_id=100, size_bytes=1000
        )
        db.set_ownership(content.id, users[0].id)

        activity = db.get_user_activity()
        assert users[0].id in activity
        assert activity[users[0].id]["request_count"] == 1
        assert activity[users[0].id]["last_request_at"] is not None
