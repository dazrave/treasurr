"""Tests for split-the-loot promotion mode and plunder cap."""

import os
import tempfile

import pytest

from treasurr.config import Config, QuotaConfig
from treasurr.db import Database
from treasurr.engine.promotion import run_promotions
from treasurr.engine.quota import get_user_quota


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(path)
    yield database
    os.unlink(path)


@pytest.fixture
def split_config():
    return Config(
        db_path=":memory:",
        quotas=QuotaConfig(
            promotion_threshold=2,
            promotion_mode="split_the_loot",
        ),
    )


@pytest.fixture
def full_config():
    return Config(
        db_path=":memory:",
        quotas=QuotaConfig(
            promotion_threshold=2,
            promotion_mode="full_plunder",
        ),
    )


@pytest.fixture
def capped_config():
    return Config(
        db_path=":memory:",
        quotas=QuotaConfig(
            promotion_threshold=2,
            promotion_mode="full_plunder",
            shared_plunder_max_bytes=5000,
        ),
    )


def _setup_promotable_content(db, size_bytes=9000, viewer_count=3):
    """Create owner + viewers + content ready for promotion."""
    owner = db.upsert_user(plex_user_id="owner", plex_username="owner", quota_bytes=100_000)
    viewers = []
    for i in range(viewer_count):
        v = db.upsert_user(plex_user_id=f"v{i}", plex_username=f"viewer{i}", quota_bytes=100_000)
        viewers.append(v)

    content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=size_bytes)
    db.set_ownership(content.id, owner.id)

    # Owner watches too
    db.add_watch_event(content.id, owner.id, "2026-01-01", completed=True)
    for i, v in enumerate(viewers):
        db.add_watch_event(content.id, v.id, f"2026-01-0{i + 2}", completed=True)

    return owner, viewers, content


class TestSplitTheLoopPromotion:
    @pytest.mark.asyncio
    async def test_split_3_viewers_9000_bytes(self, db: Database, split_config: Config):
        """9000 bytes, 3 non-owner viewers + owner = 4 total. Each gets 2250."""
        owner, viewers, content = _setup_promotable_content(db, size_bytes=9000, viewer_count=3)

        promoted = await run_promotions(db, split_config)
        assert promoted == 1

        # Check splits created  - all 4 viewers (including owner) should have splits
        for user in [owner, *viewers]:
            total = db.get_user_split_total(user.id)
            assert total == 9000 // 4  # 2250 each

    @pytest.mark.asyncio
    async def test_new_viewer_recalculates_splits(self, db: Database, split_config: Config):
        """When a new viewer watches promoted split content, shares should recalculate."""
        owner, viewers, content = _setup_promotable_content(db, size_bytes=9000, viewer_count=3)
        await run_promotions(db, split_config)

        # Add a 5th viewer
        new_viewer = db.upsert_user(plex_user_id="v_new", plex_username="new_viewer", quota_bytes=100_000)
        db.add_watch_event(content.id, new_viewer.id, "2026-01-10", completed=True)

        # Run promotions again  - second pass should recalculate
        await run_promotions(db, split_config)

        # Now 5 viewers: 9000 // 5 = 1800 each
        for user in [owner, *viewers, new_viewer]:
            total = db.get_user_split_total(user.id)
            assert total == 9000 // 5  # 1800 each

    @pytest.mark.asyncio
    async def test_split_mode_quota_includes_splits(self, db: Database, split_config: Config):
        """In split mode, user quota should include split shares."""
        owner, viewers, content = _setup_promotable_content(db, size_bytes=9000, viewer_count=3)
        await run_promotions(db, split_config)

        summary = get_user_quota(db, owner.id, include_splits=True)
        assert summary is not None
        # Owner content is promoted so used_bytes = 0, but split_bytes = 2250
        assert summary.used_bytes == 0
        assert summary.split_bytes == 9000 // 4
        assert summary.total_used_bytes == 9000 // 4

    @pytest.mark.asyncio
    async def test_full_plunder_ignores_splits(self, db: Database, full_config: Config):
        """In full plunder mode, even if splits exist they shouldn't affect quota."""
        owner, viewers, content = _setup_promotable_content(db, size_bytes=9000, viewer_count=3)

        # Set mode to split via DB setting, promote, then switch back
        db.set_setting("promotion_mode", "split_the_loot")
        await run_promotions(db, Config(db_path=":memory:", quotas=QuotaConfig(promotion_threshold=2, promotion_mode="split_the_loot")))

        # In full_plunder mode, splits should be ignored
        summary = get_user_quota(db, owner.id, include_splits=False)
        assert summary is not None
        assert summary.split_bytes == 0
        assert summary.total_used_bytes == 0


class TestPlunderCap:
    @pytest.mark.asyncio
    async def test_cap_blocks_promotion(self, db: Database, capped_config: Config):
        """When plunder cap is reached, new promotions should be blocked."""
        # First content fills the cap
        owner = db.upsert_user(plex_user_id="owner", plex_username="owner", quota_bytes=100_000)
        v1 = db.upsert_user(plex_user_id="v1", plex_username="v1", quota_bytes=100_000)
        v2 = db.upsert_user(plex_user_id="v2", plex_username="v2", quota_bytes=100_000)

        c1 = db.upsert_content(title="Big Film", media_type="movie", tmdb_id=1, size_bytes=4000)
        db.set_ownership(c1.id, owner.id)
        db.add_watch_event(c1.id, v1.id, "2026-01-01", completed=True)
        db.add_watch_event(c1.id, v2.id, "2026-01-02", completed=True)

        # This should promote (4000 < 5000 cap)
        promoted = await run_promotions(db, capped_config)
        assert promoted == 1

        # Second content would exceed cap
        c2 = db.upsert_content(title="Another Film", media_type="movie", tmdb_id=2, size_bytes=3000)
        db.set_ownership(c2.id, owner.id)
        db.add_watch_event(c2.id, v1.id, "2026-01-03", completed=True)
        db.add_watch_event(c2.id, v2.id, "2026-01-04", completed=True)

        # This should NOT promote (4000 + 3000 = 7000 > 5000 cap)
        promoted = await run_promotions(db, capped_config)
        assert promoted == 0

        ownership = db.get_ownership(c2.id)
        assert ownership.status == "owned"

    @pytest.mark.asyncio
    async def test_unlimited_cap(self, db: Database, full_config: Config):
        """When cap is 0 (unlimited), all promotions should proceed."""
        owner, viewers, content = _setup_promotable_content(db, size_bytes=999_999_999)
        promoted = await run_promotions(db, full_config)
        assert promoted == 1

    @pytest.mark.asyncio
    async def test_cap_from_db_setting(self, db: Database, full_config: Config):
        """Cap can be set via DB setting overriding config."""
        owner = db.upsert_user(plex_user_id="owner", plex_username="owner", quota_bytes=100_000)
        v1 = db.upsert_user(plex_user_id="v1", plex_username="v1", quota_bytes=100_000)
        v2 = db.upsert_user(plex_user_id="v2", plex_username="v2", quota_bytes=100_000)

        c1 = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(c1.id, owner.id)
        db.add_watch_event(c1.id, v1.id, "2026-01-01", completed=True)
        db.add_watch_event(c1.id, v2.id, "2026-01-02", completed=True)

        # Set a very small cap via DB setting
        db.set_setting("shared_plunder_max_bytes", "1")

        promoted = await run_promotions(db, full_config)
        assert promoted == 0


class TestQuotaSplitsDB:
    def test_upsert_and_get_split(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=1000)
        db.upsert_quota_split(content.id, user.id, 500)
        assert db.get_user_split_total(user.id) == 500

    def test_upsert_overwrites(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=1000)
        db.upsert_quota_split(content.id, user.id, 500)
        db.upsert_quota_split(content.id, user.id, 300)
        assert db.get_user_split_total(user.id) == 300

    def test_delete_splits_for_content(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=1000)
        db.upsert_quota_split(content.id, user.id, 500)
        db.delete_splits_for_content(content.id)
        assert db.get_user_split_total(user.id) == 0

    def test_recalculate_splits(self, db: Database):
        u1 = db.upsert_user(plex_user_id="1", plex_username="p1", quota_bytes=100)
        u2 = db.upsert_user(plex_user_id="2", plex_username="p2", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=1000)
        db.recalculate_splits(content.id, [u1.id, u2.id], 1000)
        assert db.get_user_split_total(u1.id) == 500
        assert db.get_user_split_total(u2.id) == 500

    def test_split_total_excludes_deleted_content(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        c1 = db.upsert_content(title="Active", media_type="movie", tmdb_id=1, size_bytes=1000)
        c2 = db.upsert_content(title="Deleted", media_type="movie", tmdb_id=2, size_bytes=1000)
        db.upsert_quota_split(c1.id, user.id, 500)
        db.upsert_quota_split(c2.id, user.id, 500)
        db.update_content_status(c2.id, "deleted")
        assert db.get_user_split_total(user.id) == 500
