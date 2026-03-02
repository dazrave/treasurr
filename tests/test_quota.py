"""Tests for the quota engine."""

import os
import tempfile

import pytest

from treasurr.db import Database
from treasurr.engine.quota import format_bytes, get_user_quota, has_sufficient_quota


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(path)
    yield database
    os.unlink(path)


class TestGetUserQuota:
    def test_empty_quota(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=500_000)
        summary = get_user_quota(db, user.id)
        assert summary is not None
        assert summary.used_bytes == 0
        assert summary.available_bytes == 500_000

    def test_with_owned_content(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=1000)
        c = db.upsert_content(title="X", media_type="movie", tmdb_id=1, size_bytes=400)
        db.set_ownership(c.id, user.id)
        summary = get_user_quota(db, user.id)
        assert summary.used_bytes == 400
        assert summary.available_bytes == 600
        assert summary.usage_percent == 40.0

    def test_nonexistent_user(self, db: Database):
        assert get_user_quota(db, 999) is None


class TestHasSufficientQuota:
    def test_has_space(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=1000)
        assert has_sufficient_quota(db, user.id, 500) is True

    def test_no_space(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        c = db.upsert_content(title="X", media_type="movie", tmdb_id=1, size_bytes=80)
        db.set_ownership(c.id, user.id)
        assert has_sufficient_quota(db, user.id, 50) is False

    def test_exact_fit(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100)
        assert has_sufficient_quota(db, user.id, 100) is True


class TestFormatBytes:
    def test_bytes(self):
        assert format_bytes(500) == "500.0 B"

    def test_kilobytes(self):
        assert format_bytes(1536) == "1.5 KB"

    def test_megabytes(self):
        assert format_bytes(10_485_760) == "10.0 MB"

    def test_gigabytes(self):
        assert format_bytes(5_368_709_120) == "5.0 GB"

    def test_terabytes(self):
        assert format_bytes(1_099_511_627_776) == "1.0 TB"

    def test_zero(self):
        assert format_bytes(0) == "0.0 B"
