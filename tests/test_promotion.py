"""Tests for the promotion engine."""

import os
import tempfile

import pytest

from treasurr.config import Config, QuotaConfig
from treasurr.db import Database
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
            promotion_threshold=2,
            promotion_exclude_requester=True,
        ),
    )


class TestPromotionEngine:
    @pytest.mark.asyncio
    async def test_promotes_with_enough_viewers(self, db: Database, config: Config):
        owner = db.upsert_user(plex_user_id="1", plex_username="owner", quota_bytes=10000)
        viewer1 = db.upsert_user(plex_user_id="2", plex_username="v1", quota_bytes=10000)
        viewer2 = db.upsert_user(plex_user_id="3", plex_username="v2", quota_bytes=10000)

        content = db.upsert_content(title="Popular Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, owner.id)

        # Two non-owner viewers
        db.add_watch_event(content.id, viewer1.id, "2026-01-01", completed=True)
        db.add_watch_event(content.id, viewer2.id, "2026-01-02", completed=True)

        promoted = await run_promotions(db, config)
        assert promoted == 1

        ownership = db.get_ownership(content.id)
        assert ownership.status == "promoted"

        # Quota should be freed
        summary = db.get_quota_summary(owner.id)
        assert summary.used_bytes == 0

    @pytest.mark.asyncio
    async def test_no_promotion_below_threshold(self, db: Database, config: Config):
        owner = db.upsert_user(plex_user_id="1", plex_username="owner", quota_bytes=10000)
        viewer1 = db.upsert_user(plex_user_id="2", plex_username="v1", quota_bytes=10000)

        content = db.upsert_content(title="Niche Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, owner.id)

        # Only one non-owner viewer (threshold is 2)
        db.add_watch_event(content.id, viewer1.id, "2026-01-01", completed=True)

        promoted = await run_promotions(db, config)
        assert promoted == 0

        ownership = db.get_ownership(content.id)
        assert ownership.status == "owned"

    @pytest.mark.asyncio
    async def test_owner_watch_not_counted(self, db: Database, config: Config):
        owner = db.upsert_user(plex_user_id="1", plex_username="owner", quota_bytes=10000)
        viewer1 = db.upsert_user(plex_user_id="2", plex_username="v1", quota_bytes=10000)

        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, owner.id)

        # Owner watches + one other viewer  - should NOT promote (only 1 non-owner)
        db.add_watch_event(content.id, owner.id, "2026-01-01", completed=True)
        db.add_watch_event(content.id, viewer1.id, "2026-01-02", completed=True)

        promoted = await run_promotions(db, config)
        assert promoted == 0

    @pytest.mark.asyncio
    async def test_already_promoted_not_reprocessed(self, db: Database, config: Config):
        owner = db.upsert_user(plex_user_id="1", plex_username="owner", quota_bytes=10000)
        viewer1 = db.upsert_user(plex_user_id="2", plex_username="v1", quota_bytes=10000)
        viewer2 = db.upsert_user(plex_user_id="3", plex_username="v2", quota_bytes=10000)

        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, owner.id)
        db.add_watch_event(content.id, viewer1.id, "2026-01-01", completed=True)
        db.add_watch_event(content.id, viewer2.id, "2026-01-02", completed=True)

        # First run promotes
        await run_promotions(db, config)
        # Second run should find nothing to promote
        promoted = await run_promotions(db, config)
        assert promoted == 0

    @pytest.mark.asyncio
    async def test_promotion_log_created(self, db: Database, config: Config):
        owner = db.upsert_user(plex_user_id="1", plex_username="owner", quota_bytes=10000)
        viewer1 = db.upsert_user(plex_user_id="2", plex_username="v1", quota_bytes=10000)
        viewer2 = db.upsert_user(plex_user_id="3", plex_username="v2", quota_bytes=10000)

        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, owner.id)
        db.add_watch_event(content.id, viewer1.id, "2026-01-01", completed=True)
        db.add_watch_event(content.id, viewer2.id, "2026-01-02", completed=True)

        await run_promotions(db, config)

        logs = db.get_recent_promotions()
        assert len(logs) == 1
        assert logs[0].size_freed_bytes == 5000
        assert logs[0].unique_viewers == 2
