"""Tests for dashboard overhaul: seasons, posters, view-as, admin scuttle."""

import os
import tempfile
from datetime import datetime, timedelta, timezone

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
            default_bytes=500_000_000_000,
            promotion_threshold=2,
            plank_days=0,
        ),
        safety=SafetyConfig(max_deletions_per_hour=10),
    )
    app = create_app(config)
    db = app.state.db

    expires = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()

    user = db.upsert_user(
        plex_user_id="test_plex_1",
        plex_username="testpirate",
        email="test@sea.com",
        quota_bytes=500_000_000_000,
    )
    db.create_session("test-token-123", user.id, "plex-token", expires)

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


class TestSeasonsDB:
    def test_upsert_and_get_seasons(self, db: Database):
        content = db.upsert_content(title="Show", media_type="show", tmdb_id=100, size_bytes=50_000_000_000)
        db.upsert_season(content.id, 1, episode_count=10, size_bytes=20_000_000_000)
        db.upsert_season(content.id, 2, episode_count=12, size_bytes=25_000_000_000)

        seasons = db.get_seasons(content.id)
        assert len(seasons) == 2
        assert seasons[0].season_number == 1
        assert seasons[0].episode_count == 10
        assert seasons[0].size_bytes == 20_000_000_000
        assert seasons[1].season_number == 2
        assert seasons[1].episode_count == 12

    def test_upsert_season_updates_existing(self, db: Database):
        content = db.upsert_content(title="Show", media_type="show", tmdb_id=100, size_bytes=50_000_000_000)
        db.upsert_season(content.id, 1, episode_count=8, size_bytes=15_000_000_000)
        db.upsert_season(content.id, 1, episode_count=10, size_bytes=20_000_000_000)

        seasons = db.get_seasons(content.id)
        assert len(seasons) == 1
        assert seasons[0].episode_count == 10
        assert seasons[0].size_bytes == 20_000_000_000

    def test_delete_seasons(self, db: Database):
        content = db.upsert_content(title="Show", media_type="show", tmdb_id=100, size_bytes=50_000_000_000)
        db.upsert_season(content.id, 1, episode_count=10, size_bytes=20_000_000_000)
        db.upsert_season(content.id, 2, episode_count=12, size_bytes=25_000_000_000)

        db.delete_seasons(content.id)
        seasons = db.get_seasons(content.id)
        assert len(seasons) == 0

    def test_update_season_size(self, db: Database):
        content = db.upsert_content(title="Show", media_type="show", tmdb_id=100, size_bytes=50_000_000_000)
        db.upsert_season(content.id, 1, episode_count=10, size_bytes=20_000_000_000)

        db.update_season_size(content.id, 1, 0)
        seasons = db.get_seasons(content.id)
        assert seasons[0].size_bytes == 0

    def test_get_seasons_empty(self, db: Database):
        content = db.upsert_content(title="Movie", media_type="movie", tmdb_id=200, size_bytes=5_000_000_000)
        seasons = db.get_seasons(content.id)
        assert len(seasons) == 0


class TestPosterDB:
    def test_update_and_read_poster_path(self, db: Database):
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=300, size_bytes=5_000_000_000)
        assert content.poster_path is None

        db.update_content_poster(content.id, "/abc123.jpg")
        updated = db.get_content(content.id)
        assert updated.poster_path == "/abc123.jpg"

    def test_poster_path_in_active_content(self, db: Database):
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=300, size_bytes=5_000_000_000)
        db.update_content_poster(content.id, "/poster.jpg")

        active = db.get_all_active_content()
        found = [c for c in active if c.id == content.id]
        assert len(found) == 1
        assert found[0].poster_path == "/poster.jpg"


class TestChestWithPosters:
    def test_chest_includes_poster_url(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5_000_000_000)
        db.update_content_poster(content.id, "/abc.jpg")
        db.set_ownership(content.id, user.id)

        resp = auth_client.get("/api/treasure/chest")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["poster_url"] == "https://image.tmdb.org/t/p/w300/abc.jpg"

    def test_chest_null_poster_url_when_missing(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=2, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)

        resp = auth_client.get("/api/treasure/chest")
        data = resp.json()
        assert data["items"][0]["poster_url"] is None


class TestChestWithSeasons:
    def test_chest_includes_seasons_for_shows(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Show", media_type="show", tmdb_id=10, size_bytes=50_000_000_000)
        db.set_ownership(content.id, user.id)
        db.upsert_season(content.id, 1, episode_count=8, size_bytes=20_000_000_000)
        db.upsert_season(content.id, 2, episode_count=10, size_bytes=30_000_000_000)

        resp = auth_client.get("/api/treasure/chest")
        data = resp.json()
        item = data["items"][0]
        assert item["media_type"] == "show"
        assert len(item["seasons"]) == 2
        assert item["seasons"][0]["season_number"] == 1
        assert item["seasons"][0]["episode_count"] == 8

    def test_chest_null_seasons_for_movies(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=11, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)

        resp = auth_client.get("/api/treasure/chest")
        data = resp.json()
        assert data["items"][0]["seasons"] is None


class TestViewAs:
    def test_admin_view_as_user(self, admin_client, app_and_db):
        _, db, user, admin, _ = app_and_db
        content = db.upsert_content(title="User Film", media_type="movie", tmdb_id=20, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)

        resp = admin_client.get(f"/api/treasure/chest?view_as={user.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["title"] == "User Film"
        assert data["view_as"]["user_id"] == user.id
        assert data["view_as"]["admin_username"] == "captain"

    def test_non_admin_view_as_ignored(self, auth_client, app_and_db):
        _, db, user, admin, _ = app_and_db
        # User tries to view_as admin - should be ignored, returns own content
        resp = auth_client.get(f"/api/treasure/chest?view_as={admin.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert "view_as" not in data

    def test_admin_view_as_treasure_summary(self, admin_client, app_and_db):
        _, db, user, _, _ = app_and_db
        resp = admin_client.get(f"/api/treasure?view_as={user.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["username"] == "testpirate"
        assert data["view_as"]["username"] == "testpirate"


class TestAdminForceScuttle:
    def test_admin_force_scuttle(self, admin_client, app_and_db):
        _, db, user, admin, _ = app_and_db
        content = db.upsert_content(
            title="Delete Me", media_type="movie", tmdb_id=30,
            size_bytes=5_000_000_000, radarr_id=None,
        )
        db.set_ownership(content.id, user.id)

        # Force scuttle (no radarr_id so it'll mark deleted without file removal)
        resp = admin_client.post(f"/api/admin/scuttle/{content.id}?force=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True

        # Verify it's deleted
        updated = db.get_content(content.id)
        assert updated.status == "deleted"

    def test_non_admin_cannot_force_scuttle(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Keep", media_type="movie", tmdb_id=31, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)

        resp = auth_client.post(f"/api/admin/scuttle/{content.id}?force=true")
        assert resp.status_code == 403

    def test_admin_force_scuttle_nonexistent(self, admin_client):
        resp = admin_client.post("/api/admin/scuttle/9999?force=true")
        assert resp.status_code == 404


class TestSeasonScuttleEndpoint:
    def test_season_scuttle_requires_auth(self, app_and_db):
        app, _, _, _, _ = app_and_db
        client = TestClient(app)
        resp = client.post("/api/treasure/1/scuttle-season/1")
        assert resp.status_code == 401

    def test_season_scuttle_movie_rejected(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=40, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)

        resp = auth_client.post(f"/api/treasure/{content.id}/scuttle-season/1")
        assert resp.status_code == 400
        assert "show" in resp.json()["detail"].lower()

    def test_season_scuttle_no_sonarr_id(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Show", media_type="show", tmdb_id=41, size_bytes=50_000_000_000)
        db.set_ownership(content.id, user.id)

        resp = auth_client.post(f"/api/treasure/{content.id}/scuttle-season/1")
        assert resp.status_code == 400
        assert "sonarr" in resp.json()["detail"].lower()


class TestPlunderWithPosters:
    def test_plunder_includes_poster_url(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Shared Film", media_type="movie", tmdb_id=50, size_bytes=5_000_000_000)
        db.update_content_poster(content.id, "/shared.jpg")
        db.set_ownership(content.id, user.id)
        db.promote_content(content.id)

        resp = auth_client.get("/api/plunder")
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["poster_url"] == "https://image.tmdb.org/t/p/w300/shared.jpg"
