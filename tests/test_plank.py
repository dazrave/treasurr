"""Tests for the Walk the Plank feature (Phase 1.6)."""

import os
import tempfile

import pytest

from treasurr.app import create_app
from treasurr.config import Config, QuotaConfig, SafetyConfig
from treasurr.db import Database
from treasurr.engine.deletion import scuttle_content
from treasurr.engine.plank import rescue_content, run_plank_checks
from treasurr.engine.promotion import run_promotions


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(path)
    yield database
    os.unlink(path)


@pytest.fixture
def config():
    return Config(
        db_path=":memory:",
        quotas=QuotaConfig(
            plank_days=14,
            plank_mode="adrift",
            plank_rescue_action="promote",
            promotion_threshold=2,
        ),
        safety=SafetyConfig(max_deletions_per_hour=100),
    )


@pytest.fixture
def config_instant():
    """Config with plank disabled (instant delete)."""
    return Config(
        db_path=":memory:",
        quotas=QuotaConfig(plank_days=0),
        safety=SafetyConfig(max_deletions_per_hour=100),
    )


@pytest.fixture
def config_anchored():
    return Config(
        db_path=":memory:",
        quotas=QuotaConfig(
            plank_days=14,
            plank_mode="anchored",
            plank_rescue_action="promote",
            promotion_threshold=2,
        ),
        safety=SafetyConfig(max_deletions_per_hour=100),
    )


@pytest.fixture
def config_adopt():
    return Config(
        db_path=":memory:",
        quotas=QuotaConfig(
            plank_days=14,
            plank_mode="adrift",
            plank_rescue_action="adopt",
            promotion_threshold=2,
        ),
        safety=SafetyConfig(max_deletions_per_hour=100),
    )


def _days_ago(n: int) -> str:
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()


class TestScuttlePlanks:
    @pytest.mark.asyncio
    async def test_scuttle_puts_content_on_plank(self, db: Database, config: Config):
        """Scuttle with plank_days > 0 should plank, not delete."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)

        result = await scuttle_content(db, config, content.id, user.id)

        assert result.success
        assert result.walked_plank
        assert result.freed_bytes == 0

        # Content should still be active, ownership on plank
        updated = db.get_content(content.id)
        assert updated.status == "active"
        ownership = db.get_ownership(content.id)
        assert ownership.status == "plank"
        assert ownership.plank_started_at is not None

    @pytest.mark.asyncio
    async def test_scuttle_instant_when_plank_zero(self, db: Database, config_instant: Config):
        """Scuttle with plank_days = 0 should delete immediately."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)

        result = await scuttle_content(db, config_instant, content.id, user.id)

        assert result.success
        assert not result.walked_plank
        assert result.freed_bytes == 5000

        updated = db.get_content(content.id)
        assert updated.status == "deleted"

    @pytest.mark.asyncio
    async def test_already_planked_cannot_plank_again(self, db: Database, config: Config):
        """Content already on the plank should not be planked again."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)

        await scuttle_content(db, config, content.id, user.id)
        result = await scuttle_content(db, config, content.id, user.id)

        assert not result.success
        assert "already walking the plank" in result.message


class TestPlankExpiry:
    @pytest.mark.asyncio
    async def test_expired_plank_gets_deleted(self, db: Database, config: Config):
        """Content past the plank period should be actually deleted."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(title="Old Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.plank_content(content.id)

        # Backdate plank_started_at to 15 days ago
        with db.connection() as conn:
            conn.execute(
                "UPDATE content_ownership SET plank_started_at = ? WHERE content_id = ?",
                (_days_ago(15), content.id),
            )

        results = await run_plank_checks(db, config)
        assert results["expired"] == 1

        updated = db.get_content(content.id)
        assert updated.status == "deleted"

    @pytest.mark.asyncio
    async def test_non_expired_plank_stays(self, db: Database, config: Config):
        """Content within the plank period should remain."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(title="Recent Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.plank_content(content.id)

        # Plank started 5 days ago (within 14-day period)
        with db.connection() as conn:
            conn.execute(
                "UPDATE content_ownership SET plank_started_at = ? WHERE content_id = ?",
                (_days_ago(5), content.id),
            )

        results = await run_plank_checks(db, config)
        assert results["expired"] == 0

        updated = db.get_content(content.id)
        assert updated.status == "active"
        ownership = db.get_ownership(content.id)
        assert ownership.status == "plank"


class TestRescue:
    @pytest.mark.asyncio
    async def test_owner_rescues_own_content(self, db: Database, config: Config):
        """Owner should always be able to rescue their planked content."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.plank_content(content.id)

        result = await rescue_content(db, config, content.id, user.id)

        assert result.success
        assert result.action == "rescued"

        ownership = db.get_ownership(content.id)
        assert ownership.status == "owned"
        assert ownership.plank_started_at is None

    @pytest.mark.asyncio
    async def test_adrift_non_owner_watches_auto_rescue_promote(self, db: Database, config: Config):
        """In adrift+promote mode, a non-owner viewer auto-rescues and promotes."""
        owner = db.upsert_user(plex_user_id="1", plex_username="owner", quota_bytes=100_000)
        watcher = db.upsert_user(plex_user_id="2", plex_username="watcher", quota_bytes=100_000)

        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, owner.id)
        db.plank_content(content.id)

        # Watcher has completed watching it
        db.add_watch_event(content.id, watcher.id, "2026-01-01", completed=True)

        results = await run_plank_checks(db, config)
        assert results["rescued"] == 1

        ownership = db.get_ownership(content.id)
        assert ownership.status == "promoted"

    @pytest.mark.asyncio
    async def test_adrift_non_owner_watches_auto_rescue_adopt(self, db: Database, config_adopt: Config):
        """In adrift+adopt mode, a non-owner viewer auto-rescues and adopts."""
        owner = db.upsert_user(plex_user_id="1", plex_username="owner", quota_bytes=100_000)
        watcher = db.upsert_user(plex_user_id="2", plex_username="watcher", quota_bytes=100_000)

        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, owner.id)
        db.plank_content(content.id)

        db.add_watch_event(content.id, watcher.id, "2026-01-01", completed=True)

        results = await run_plank_checks(db, config_adopt)
        assert results["rescued"] == 1

        ownership = db.get_ownership(content.id)
        assert ownership.status == "owned"
        assert ownership.owner_user_id == watcher.id

    @pytest.mark.asyncio
    async def test_non_owner_cannot_rescue_in_anchored_mode(self, db: Database, config_anchored: Config):
        """In anchored mode, only the owner can rescue."""
        owner = db.upsert_user(plex_user_id="1", plex_username="owner", quota_bytes=100_000)
        other = db.upsert_user(plex_user_id="2", plex_username="other", quota_bytes=100_000)

        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, owner.id)
        db.plank_content(content.id)

        result = await rescue_content(db, config_anchored, content.id, other.id)

        assert not result.success
        assert "anchored" in result.message.lower()

        ownership = db.get_ownership(content.id)
        assert ownership.status == "plank"


class TestQuotaWithPlank:
    def test_anchored_plank_counts_against_quota(self, db: Database):
        """In anchored mode, plank content counts against owner's quota."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.plank_content(content.id)

        summary = db.get_quota_summary(user.id, plank_mode="anchored")
        assert summary.used_bytes == 5000

    def test_adrift_plank_does_not_count_against_quota(self, db: Database):
        """In adrift mode, plank content does NOT count against owner's quota."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.plank_content(content.id)

        summary = db.get_quota_summary(user.id, plank_mode="adrift")
        assert summary.used_bytes == 0


class TestPlankExclusions:
    @pytest.mark.asyncio
    async def test_plank_content_excluded_from_promotion(self, db: Database, config: Config):
        """Plank content should not be eligible for promotion."""
        owner = db.upsert_user(plex_user_id="1", plex_username="owner", quota_bytes=100_000)
        viewer1 = db.upsert_user(plex_user_id="2", plex_username="v1", quota_bytes=100_000)
        viewer2 = db.upsert_user(plex_user_id="3", plex_username="v2", quota_bytes=100_000)

        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, owner.id)
        db.plank_content(content.id)

        # Even with enough viewers, plank content shouldn't promote
        db.add_watch_event(content.id, viewer1.id, "2026-01-01", completed=True)
        db.add_watch_event(content.id, viewer2.id, "2026-01-02", completed=True)

        promoted = await run_promotions(db, config)
        assert promoted == 0

        ownership = db.get_ownership(content.id)
        assert ownership.status == "plank"

    @pytest.mark.asyncio
    async def test_auto_scuttle_triggers_plank_not_instant(self, db: Database, config: Config):
        """Auto-scuttle (retention engine) should plank, not instant-delete."""
        from treasurr.engine.retention import run_retention_checks

        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        db.update_user_auto_scuttle(user.id, 7)

        content = db.upsert_content(title="Old Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.add_watch_event(content.id, user.id, _days_ago(10), completed=True)
        with db.connection() as conn:
            conn.execute("UPDATE content SET added_at = ? WHERE id = ?", (_days_ago(10), content.id))

        scuttled = await run_retention_checks(db, config)
        assert scuttled == 1

        # Should be planked, not deleted
        updated = db.get_content(content.id)
        assert updated.status == "active"
        ownership = db.get_ownership(content.id)
        assert ownership.status == "plank"


class TestPlankSettings:
    def test_plank_settings_persist(self, db: Database):
        """Plank admin settings should be saved and retrievable."""
        db.set_setting("plank_mode", "anchored")
        db.set_setting("plank_days", "7")
        db.set_setting("plank_rescue_action", "adopt")

        assert db.get_setting("plank_mode") == "anchored"
        assert db.get_setting("plank_days") == "7"
        assert db.get_setting("plank_rescue_action") == "adopt"

    @pytest.mark.asyncio
    async def test_plank_days_from_db_overrides_config(self, db: Database, config: Config):
        """plank_days from DB settings should override config."""
        db.set_setting("plank_days", "0")  # Override: disable plank

        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)

        result = await scuttle_content(db, config, content.id, user.id)

        assert result.success
        assert not result.walked_plank  # Instant delete because DB override

        updated = db.get_content(content.id)
        assert updated.status == "deleted"


class TestPlankDB:
    def test_get_plank_content(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        c1 = db.upsert_content(title="Film1", media_type="movie", tmdb_id=1, size_bytes=5000)
        c2 = db.upsert_content(title="Film2", media_type="movie", tmdb_id=2, size_bytes=3000)
        db.set_ownership(c1.id, user.id)
        db.set_ownership(c2.id, user.id)
        db.plank_content(c1.id)

        plank = db.get_plank_content()
        assert len(plank) == 1
        assert plank[0].content.title == "Film1"

    def test_get_user_plank_content(self, db: Database):
        u1 = db.upsert_user(plex_user_id="1", plex_username="p1", quota_bytes=100_000)
        u2 = db.upsert_user(plex_user_id="2", plex_username="p2", quota_bytes=100_000)
        c1 = db.upsert_content(title="Film1", media_type="movie", tmdb_id=1, size_bytes=5000)
        c2 = db.upsert_content(title="Film2", media_type="movie", tmdb_id=2, size_bytes=3000)
        db.set_ownership(c1.id, u1.id)
        db.set_ownership(c2.id, u2.id)
        db.plank_content(c1.id)
        db.plank_content(c2.id)

        plank = db.get_user_plank_content(u1.id)
        assert len(plank) == 1
        assert plank[0].content.title == "Film1"

    def test_rescue_clears_plank(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.plank_content(content.id)

        ownership = db.get_ownership(content.id)
        assert ownership.status == "plank"
        assert ownership.plank_started_at is not None

        db.rescue_content(content.id)

        ownership = db.get_ownership(content.id)
        assert ownership.status == "owned"
        assert ownership.plank_started_at is None

    def test_adopt_changes_owner(self, db: Database):
        u1 = db.upsert_user(plex_user_id="1", plex_username="p1", quota_bytes=100_000)
        u2 = db.upsert_user(plex_user_id="2", plex_username="p2", quota_bytes=100_000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, u1.id)
        db.plank_content(content.id)

        db.adopt_content(content.id, u2.id)

        ownership = db.get_ownership(content.id)
        assert ownership.owner_user_id == u2.id
        assert ownership.status == "owned"
        assert ownership.plank_started_at is None

    def test_plank_in_global_stats(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.plank_content(content.id)

        stats = db.get_global_stats()
        assert stats["plank_bytes"] == 5000
        assert stats["plank_count"] == 1


class TestPlankAPI:
    @pytest.fixture
    def app_and_db(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        config = Config(
            db_path=path,
            quotas=QuotaConfig(
                default_bytes=500_000_000_000,
                promotion_threshold=2,
                plank_days=14,
                plank_mode="adrift",
                plank_rescue_action="promote",
            ),
            safety=SafetyConfig(max_deletions_per_hour=10),
        )
        app = create_app(config)
        db = app.state.db

        from datetime import datetime, timedelta, timezone
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
    def auth_client(self, app_and_db):
        from fastapi.testclient import TestClient
        app, _, _, _, _ = app_and_db
        client = TestClient(app)
        client.cookies.set("treasurr_session", "test-token-123")
        return client

    @pytest.fixture
    def admin_client(self, app_and_db):
        from fastapi.testclient import TestClient
        app, _, _, _, _ = app_and_db
        client = TestClient(app)
        client.cookies.set("treasurr_session", "admin-token-123")
        return client

    def test_scuttle_returns_walked_plank(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)

        resp = auth_client.post(f"/api/treasure/{content.id}/scuttle")
        assert resp.status_code == 200
        data = resp.json()
        assert data["walked_plank"] is True
        assert data["plank_days"] == 14

    def test_get_plank_list(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Planked Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.plank_content(content.id)

        resp = auth_client.get("/api/plank")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["title"] == "Planked Film"
        assert data["plank_mode"] == "adrift"

    def test_rescue_endpoint(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.plank_content(content.id)

        resp = auth_client.post(f"/api/treasure/{content.id}/rescue")
        assert resp.status_code == 200
        data = resp.json()
        assert data["action"] == "rescued"

        ownership = db.get_ownership(content.id)
        assert ownership.status == "owned"

    def test_rescue_not_planked_fails(self, auth_client, app_and_db):
        _, db, user, _, _ = app_and_db
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)

        resp = auth_client.post(f"/api/treasure/{content.id}/rescue")
        assert resp.status_code == 400

    def test_plank_requires_auth(self, app_and_db):
        from fastapi.testclient import TestClient
        app, _, _, _, _ = app_and_db
        client = TestClient(app)
        resp = client.get("/api/plank")
        assert resp.status_code == 401

    def test_admin_settings_include_plank(self, admin_client):
        resp = admin_client.get("/api/admin/settings")
        assert resp.status_code == 200
        data = resp.json()
        assert "plank_mode" in data
        assert "plank_days" in data
        assert "plank_rescue_action" in data

    def test_admin_update_plank_settings(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"plank_mode": "anchored", "plank_days": 7, "plank_rescue_action": "adopt"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["plank_mode"] == "anchored"
        assert data["plank_days"] == 7
        assert data["plank_rescue_action"] == "adopt"

    def test_admin_invalid_plank_mode_rejected(self, admin_client):
        resp = admin_client.put(
            "/api/admin/settings",
            json={"plank_mode": "invalid"},
        )
        assert resp.status_code == 400

    def test_treasure_summary_includes_plank_info(self, auth_client):
        resp = auth_client.get("/api/treasure")
        assert resp.status_code == 200
        data = resp.json()
        assert "plank_days" in data
        assert "plank_mode" in data
