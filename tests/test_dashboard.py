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


class TestBuryContent:
    def test_bury_owned_content(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Bury Me", media_type="movie", tmdb_id=60, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)

        resp = auth_client.post(f"/api/treasure/{content.id}/bury")
        assert resp.status_code == 200
        data = resp.json()
        assert data["buried"] is True

        # Verify DB state
        ownership = db.get_ownership(content.id)
        assert ownership.status == "buried"
        assert ownership.buried_at is not None

    def test_unbury_buried_content(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Unbury Me", media_type="movie", tmdb_id=61, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)
        db.bury_content(content.id)

        resp = auth_client.post(f"/api/treasure/{content.id}/bury")
        assert resp.status_code == 200
        data = resp.json()
        assert data["buried"] is False

        ownership = db.get_ownership(content.id)
        assert ownership.status == "owned"
        assert ownership.buried_at is None

    def test_bury_not_owned(self, auth_client, app_and_db):
        _, db, user, admin, _ = app_and_db
        content = db.upsert_content(title="Not Mine", media_type="movie", tmdb_id=62, size_bytes=5_000_000_000)
        db.set_ownership(content.id, admin.id)

        resp = auth_client.post(f"/api/treasure/{content.id}/bury")
        assert resp.status_code == 403

    def test_buried_content_in_chest(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Buried Gold", media_type="movie", tmdb_id=63, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)
        db.bury_content(content.id)

        resp = auth_client.get("/api/treasure/chest")
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["is_buried"] is True
        assert data["items"][0]["can_scuttle"] is True  # buried can still be scuttled

    def test_buried_excluded_from_retention(self, db: Database):
        """Buried content should not appear in retention-eligible content."""
        user = db.upsert_user(plex_user_id="rtest", plex_username="rtest", quota_bytes=500_000_000_000)
        content = db.upsert_content(title="Buried", media_type="movie", tmdb_id=64, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)
        db.bury_content(content.id)
        db.add_watch_event(content.id, user.id, "2020-01-01T00:00:00Z", completed=True)

        eligible = db.get_retention_eligible_content(user.id, scuttle_days=1, min_retention_days=0)
        assert len(eligible) == 0

    def test_buried_still_promotable(self, db: Database):
        """Buried content should appear in promotion-eligible content."""
        user = db.upsert_user(plex_user_id="ptest", plex_username="ptest", quota_bytes=500_000_000_000)
        content = db.upsert_content(title="Buried Promo", media_type="movie", tmdb_id=65, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)
        db.bury_content(content.id)

        candidates = db.get_owned_content_for_promotion()
        content_ids = [c.content.id for c in candidates]
        assert content.id in content_ids


class TestPlunderFiltering:
    def test_plunder_shows_owned_promoted(self, auth_client, app_and_db):
        """Promoted content owned by user should appear in plunder."""
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="My Promoted", media_type="movie", tmdb_id=70, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)
        db.promote_content(content.id)

        resp = auth_client.get("/api/plunder")
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["title"] == "My Promoted"

    def test_plunder_shows_watched_promoted(self, auth_client, app_and_db):
        """Promoted content user watched should appear in plunder."""
        _, db, user, admin, _ = app_and_db
        content = db.upsert_content(title="Watched Promo", media_type="movie", tmdb_id=71, size_bytes=5_000_000_000)
        db.set_ownership(content.id, admin.id)
        db.promote_content(content.id)
        db.add_watch_event(content.id, user.id, "2025-01-01T00:00:00Z", completed=True)

        resp = auth_client.get("/api/plunder")
        data = resp.json()
        assert len(data["items"]) == 1

    def test_plunder_hides_irrelevant(self, auth_client, app_and_db):
        """Promoted content user didn't own or watch should not appear."""
        _, db, user, admin, _ = app_and_db
        content = db.upsert_content(title="Not Mine", media_type="movie", tmdb_id=72, size_bytes=5_000_000_000)
        db.set_ownership(content.id, admin.id)
        db.promote_content(content.id)

        resp = auth_client.get("/api/plunder")
        data = resp.json()
        assert len(data["items"]) == 0


class TestServerMessage:
    def test_treasure_includes_server_message(self, auth_client, app_and_db):
        resp = auth_client.get("/api/treasure")
        data = resp.json()
        assert "server_message" in data
        assert len(data["server_message"]) > 0

    def test_admin_can_set_server_message(self, admin_client, app_and_db):
        _, db, _, _, _ = app_and_db
        resp = admin_client.put("/api/admin/settings", json={"server_message": "Test message!"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["server_message"] == "Test message!"

    def test_treasure_shows_custom_message(self, auth_client, admin_client, app_and_db):
        admin_client.put("/api/admin/settings", json={"server_message": "Custom MOTD"})
        resp = auth_client.get("/api/treasure")
        data = resp.json()
        assert data["server_message"] == "Custom MOTD"

    def test_promotion_threshold_in_treasure(self, auth_client, app_and_db):
        resp = auth_client.get("/api/treasure")
        data = resp.json()
        assert "promotion_threshold" in data
        assert data["promotion_threshold"] == 2


class TestAdminActivityFeed:
    def test_activity_feed_returns_events(self, admin_client, app_and_db):
        _, db, user, admin, _ = app_and_db
        # Create a watch event
        content = db.upsert_content(title="Watched", media_type="movie", tmdb_id=80, size_bytes=5_000_000_000)
        db.set_ownership(content.id, admin.id)
        db.add_watch_event(content.id, user.id, "2025-01-01T00:00:00Z", completed=True)

        resp = admin_client.get("/api/admin/activity?limit=10")
        assert resp.status_code == 200
        data = resp.json()
        assert "events" in data
        assert len(data["events"]) > 0
        assert data["events"][0]["type"] == "watch"
        assert data["events"][0]["actor"] == "testpirate"
        assert data["events"][0]["owner_username"] == "captain"

    def test_activity_feed_requires_admin(self, auth_client):
        resp = auth_client.get("/api/admin/activity")
        assert resp.status_code == 403


class TestPlankWithPosters:
    def test_plank_includes_owner_username(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Planked", media_type="movie", tmdb_id=90, size_bytes=5_000_000_000)
        db.set_ownership(content.id, user.id)
        db.plank_content(content.id)

        resp = auth_client.get("/api/plank")
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["owner_username"] == "testpirate"
