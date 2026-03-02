"""Tests for the database layer."""

import os
import tempfile

import pytest

from treasurr.db import Database


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(path)
    yield database
    os.unlink(path)


class TestUsers:
    def test_upsert_and_get_user(self, db: Database):
        user = db.upsert_user(
            plex_user_id="123",
            plex_username="testpirate",
            email="pirate@sea.com",
            quota_bytes=500_000_000_000,
        )
        assert user.plex_user_id == "123"
        assert user.plex_username == "testpirate"
        assert user.email == "pirate@sea.com"
        assert user.quota_bytes == 500_000_000_000

        fetched = db.get_user(user.id)
        assert fetched is not None
        assert fetched.plex_username == "testpirate"

    def test_upsert_updates_username(self, db: Database):
        db.upsert_user(plex_user_id="123", plex_username="old_name", quota_bytes=100)
        updated = db.upsert_user(plex_user_id="123", plex_username="new_name", quota_bytes=100)
        assert updated.plex_username == "new_name"

    def test_get_user_by_plex_id(self, db: Database):
        db.upsert_user(plex_user_id="456", plex_username="sailor", quota_bytes=100)
        user = db.get_user_by_plex_id("456")
        assert user is not None
        assert user.plex_username == "sailor"

    def test_get_nonexistent_user(self, db: Database):
        assert db.get_user(999) is None
        assert db.get_user_by_plex_id("nonexistent") is None

    def test_get_all_users(self, db: Database):
        db.upsert_user(plex_user_id="1", plex_username="alpha", quota_bytes=100)
        db.upsert_user(plex_user_id="2", plex_username="beta", quota_bytes=100)
        users = db.get_all_users()
        assert len(users) == 2

    def test_update_user_quota(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="pirate", quota_bytes=100)
        updated = db.update_user_quota(user.id, quota_bytes=200, bonus_bytes=50)
        assert updated.quota_bytes == 200
        assert updated.bonus_bytes == 50


class TestContent:
    def test_upsert_and_get_content(self, db: Database):
        content = db.upsert_content(
            title="Pirates of the Caribbean",
            media_type="movie",
            tmdb_id=22,
            size_bytes=5_000_000_000,
        )
        assert content.title == "Pirates of the Caribbean"
        assert content.tmdb_id == 22
        assert content.size_bytes == 5_000_000_000

        fetched = db.get_content(content.id)
        assert fetched is not None
        assert fetched.title == "Pirates of the Caribbean"

    def test_upsert_updates_title(self, db: Database):
        db.upsert_content(title="Old Title", media_type="movie", tmdb_id=1)
        updated = db.upsert_content(title="New Title", media_type="movie", tmdb_id=1)
        assert updated.title == "New Title"

    def test_get_content_by_tmdb(self, db: Database):
        db.upsert_content(title="Test", media_type="show", tmdb_id=42)
        content = db.get_content_by_tmdb(42, "show")
        assert content is not None
        assert content.title == "Test"

    def test_update_content_size(self, db: Database):
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=100)
        db.update_content_size(content.id, 999)
        updated = db.get_content(content.id)
        assert updated.size_bytes == 999

    def test_update_content_status(self, db: Database):
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1)
        db.update_content_status(content.id, "deleted")
        updated = db.get_content(content.id)
        assert updated.status == "deleted"

    def test_get_all_active_content(self, db: Database):
        db.upsert_content(title="Active", media_type="movie", tmdb_id=1)
        c2 = db.upsert_content(title="Deleted", media_type="movie", tmdb_id=2)
        db.update_content_status(c2.id, "deleted")
        active = db.get_all_active_content()
        assert len(active) == 1
        assert active[0].title == "Active"


class TestOwnership:
    def test_set_and_get_ownership(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1)
        ownership = db.set_ownership(content.id, user.id)
        assert ownership.owner_user_id == user.id
        assert ownership.status == "owned"

        fetched = db.get_ownership(content.id)
        assert fetched is not None
        assert fetched.owner_user_id == user.id

    def test_get_user_owned_content(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        c1 = db.upsert_content(title="Film A", media_type="movie", tmdb_id=1, size_bytes=100)
        c2 = db.upsert_content(title="Film B", media_type="movie", tmdb_id=2, size_bytes=200)
        db.set_ownership(c1.id, user.id)
        db.set_ownership(c2.id, user.id)
        owned = db.get_user_owned_content(user.id)
        assert len(owned) == 2

    def test_promote_content(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1)
        db.set_ownership(content.id, user.id)
        db.promote_content(content.id)
        ownership = db.get_ownership(content.id)
        assert ownership.status == "promoted"
        assert ownership.promoted_at is not None


class TestWatchEvents:
    def test_add_and_count_viewers(self, db: Database):
        user1 = db.upsert_user(plex_user_id="1", plex_username="p1", quota_bytes=100)
        user2 = db.upsert_user(plex_user_id="2", plex_username="p2", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1)

        db.add_watch_event(content.id, user1.id, "2026-01-01", completed=True)
        db.add_watch_event(content.id, user2.id, "2026-01-02", completed=True)

        viewers = db.get_unique_viewers(content.id)
        assert viewers == 2

    def test_exclude_owner_from_viewers(self, db: Database):
        user1 = db.upsert_user(plex_user_id="1", plex_username="owner", quota_bytes=100)
        user2 = db.upsert_user(plex_user_id="2", plex_username="viewer", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1)

        db.add_watch_event(content.id, user1.id, "2026-01-01", completed=True)
        db.add_watch_event(content.id, user2.id, "2026-01-02", completed=True)

        viewers = db.get_unique_viewers(content.id, exclude_user_id=user1.id)
        assert viewers == 1

    def test_incomplete_watch_not_counted(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1)
        db.add_watch_event(content.id, user.id, "2026-01-01", completed=False)
        viewers = db.get_unique_viewers(content.id)
        assert viewers == 0


class TestQuota:
    def test_quota_summary(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=1000)
        c1 = db.upsert_content(title="A", media_type="movie", tmdb_id=1, size_bytes=300)
        c2 = db.upsert_content(title="B", media_type="movie", tmdb_id=2, size_bytes=200)
        db.set_ownership(c1.id, user.id)
        db.set_ownership(c2.id, user.id)

        summary = db.get_quota_summary(user.id)
        assert summary is not None
        assert summary.used_bytes == 500
        assert summary.available_bytes == 500
        assert summary.owned_count == 2

    def test_promoted_content_not_counted(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=1000)
        c1 = db.upsert_content(title="A", media_type="movie", tmdb_id=1, size_bytes=300)
        c2 = db.upsert_content(title="B", media_type="movie", tmdb_id=2, size_bytes=200)
        db.set_ownership(c1.id, user.id)
        db.set_ownership(c2.id, user.id)
        db.promote_content(c1.id)

        summary = db.get_quota_summary(user.id)
        assert summary.used_bytes == 200
        assert summary.owned_count == 1

    def test_quota_with_bonus(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=1000)
        db.update_user_quota(user.id, bonus_bytes=500)
        summary = db.get_quota_summary(user.id)
        assert summary.total_bytes == 1500


class TestLogs:
    def test_promotion_log(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=100)
        db.log_promotion(content.id, user.id, 3, 100)
        promotions = db.get_recent_promotions()
        assert len(promotions) == 1
        assert promotions[0].unique_viewers == 3

    def test_deletion_log(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=100)
        db.log_deletion(content.id, user.id, "Film", 100)
        deletions = db.get_recent_deletions()
        assert len(deletions) == 1
        assert deletions[0].title == "Film"


class TestGlobalStats:
    def test_global_stats(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        c1 = db.upsert_content(title="A", media_type="movie", tmdb_id=1, size_bytes=100)
        c2 = db.upsert_content(title="B", media_type="movie", tmdb_id=2, size_bytes=200)
        c3 = db.upsert_content(title="C", media_type="movie", tmdb_id=3, size_bytes=300)
        db.set_ownership(c1.id, user.id)
        db.set_ownership(c2.id, user.id)
        db.promote_content(c2.id)

        stats = db.get_global_stats()
        assert stats["total_bytes"] == 600
        assert stats["owned_bytes"] == 100
        assert stats["promoted_bytes"] == 200
        assert stats["unowned_bytes"] == 300
        assert stats["user_count"] == 1
        assert stats["content_count"] == 3
