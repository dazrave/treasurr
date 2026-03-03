"""Tests for the retention/auto-scuttle engine."""

import os
import tempfile

import pytest

from treasurr.config import Config, QuotaConfig, SafetyConfig
from treasurr.db import Database
from treasurr.engine.retention import run_retention_checks


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
            min_retention_days=0,
            plank_days=0,  # Instant delete for retention tests
        ),
        safety=SafetyConfig(max_deletions_per_hour=100),
    )


def _days_ago(n: int) -> str:
    """Return an ISO date string for n days ago."""
    from datetime import datetime, timedelta, timezone

    return (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()


class TestRetentionEngine:
    @pytest.mark.asyncio
    async def test_scuttles_expired_content(self, db: Database, config: Config):
        """Content watched 31 days ago with 30-day timer should be scuttled."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        db.update_user_auto_scuttle(user.id, 30)

        content = db.upsert_content(title="Old Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.add_watch_event(content.id, user.id, _days_ago(31), completed=True)
        # Backdate content added_at to ensure min retention is also passed
        with db.connection() as conn:
            conn.execute("UPDATE content SET added_at = ? WHERE id = ?", (_days_ago(31), content.id))

        scuttled = await run_retention_checks(db, config)
        assert scuttled == 1

        updated = db.get_content(content.id)
        assert updated.status == "deleted"

    @pytest.mark.asyncio
    async def test_does_not_scuttle_recent_watch(self, db: Database, config: Config):
        """Content watched 15 days ago with 30-day timer should NOT be scuttled."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        db.update_user_auto_scuttle(user.id, 30)

        content = db.upsert_content(title="Recent Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.add_watch_event(content.id, user.id, _days_ago(15), completed=True)

        scuttled = await run_retention_checks(db, config)
        assert scuttled == 0

        updated = db.get_content(content.id)
        assert updated.status == "active"

    @pytest.mark.asyncio
    async def test_disabled_auto_scuttle(self, db: Database, config: Config):
        """User with auto_scuttle_days=0 should have nothing scuttled."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        # auto_scuttle_days defaults to 0

        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.add_watch_event(content.id, user.id, _days_ago(365), completed=True)

        scuttled = await run_retention_checks(db, config)
        assert scuttled == 0

    @pytest.mark.asyncio
    async def test_admin_min_retention_blocks_scuttle(self, db: Database):
        """Content added 10 days ago with 14-day admin minimum should NOT be scuttled."""
        min_config = Config(
            db_path=":memory:",
            quotas=QuotaConfig(min_retention_days=14, plank_days=0),
            safety=SafetyConfig(max_deletions_per_hour=100),
        )

        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        db.update_user_auto_scuttle(user.id, 7)

        content = db.upsert_content(title="New Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.add_watch_event(content.id, user.id, _days_ago(10), completed=True)
        # Content was added 10 days ago (< 14 day admin minimum)
        with db.connection() as conn:
            conn.execute("UPDATE content SET added_at = ? WHERE id = ?", (_days_ago(10), content.id))

        scuttled = await run_retention_checks(db, min_config)
        assert scuttled == 0

    @pytest.mark.asyncio
    async def test_promoted_content_not_scuttled(self, db: Database, config: Config):
        """Promoted content should not be auto-scuttled (only owned)."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        db.update_user_auto_scuttle(user.id, 7)

        content = db.upsert_content(title="Promoted Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.promote_content(content.id)
        db.add_watch_event(content.id, user.id, _days_ago(30), completed=True)

        scuttled = await run_retention_checks(db, config)
        assert scuttled == 0

    @pytest.mark.asyncio
    async def test_unwatched_content_not_scuttled(self, db: Database, config: Config):
        """Content never watched should not be auto-scuttled."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        db.update_user_auto_scuttle(user.id, 7)

        content = db.upsert_content(title="Unwatched Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        # No watch event added

        scuttled = await run_retention_checks(db, config)
        assert scuttled == 0

    @pytest.mark.asyncio
    async def test_min_retention_from_db_setting(self, db: Database, config: Config):
        """min_retention_days from DB settings should override config."""
        db.set_setting("min_retention_days", "30")

        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        db.update_user_auto_scuttle(user.id, 7)

        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)
        db.add_watch_event(content.id, user.id, _days_ago(10), completed=True)
        with db.connection() as conn:
            conn.execute("UPDATE content SET added_at = ? WHERE id = ?", (_days_ago(10), content.id))

        scuttled = await run_retention_checks(db, config)
        assert scuttled == 0


class TestAutoScuttleDB:
    def test_update_auto_scuttle(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        updated = db.update_user_auto_scuttle(user.id, 30)
        assert updated.auto_scuttle_days == 30

    def test_get_users_with_auto_scuttle(self, db: Database):
        db.upsert_user(plex_user_id="1", plex_username="active", quota_bytes=100)
        u2 = db.upsert_user(plex_user_id="2", plex_username="scuttler", quota_bytes=100)
        db.update_user_auto_scuttle(u2.id, 14)

        users = db.get_users_with_auto_scuttle()
        assert len(users) == 1
        assert users[0].auto_scuttle_days == 14

    def test_retention_eligible_no_watch(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=100)
        db.set_ownership(content.id, user.id)
        eligible = db.get_retention_eligible_content(user.id, 7, 0)
        assert len(eligible) == 0

    def test_onboarded_flag(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        assert user.onboarded is False
        db.update_user_onboarded(user.id)
        updated = db.get_user(user.id)
        assert updated.onboarded is True
