"""Tests for tag-based ownership sync."""

import os
import tempfile
from unittest.mock import AsyncMock, patch

import pytest

from treasurr.config import Config, QuotaConfig, ApiConfig
from treasurr.db import Database
from treasurr.sync.clients import ArrMedia
from treasurr.sync.tag_sync import _build_tag_user_map, _apply_tag_ownership, sync_tag_ownership


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
        quotas=QuotaConfig(),
        sonarr=ApiConfig(url="http://sonarr:8989", key="test"),
        radarr=ApiConfig(url="http://radarr:7878", key="test"),
    )


class TestBuildTagUserMap:
    def test_overseerr_format(self):
        tags = [
            {"id": 7, "label": "1 - dazrave"},
            {"id": 8, "label": "2 - alice"},
        ]
        users = {"dazrave": 10, "alice": 20}
        result = _build_tag_user_map(tags, users)
        assert result == {7: 10, 8: 20}

    def test_plain_username_fallback(self):
        tags = [{"id": 5, "label": "dazrave"}]
        users = {"dazrave": 10}
        result = _build_tag_user_map(tags, users)
        assert result == {5: 10}

    def test_case_insensitive(self):
        tags = [{"id": 7, "label": "1 - DazRave"}]
        users = {"dazrave": 10}
        result = _build_tag_user_map(tags, users)
        assert result == {7: 10}

    def test_unmatched_tags_ignored(self):
        tags = [
            {"id": 7, "label": "1 - dazrave"},
            {"id": 99, "label": "keep"},
            {"id": 100, "label": "3 - unknown_user"},
        ]
        users = {"dazrave": 10}
        result = _build_tag_user_map(tags, users)
        assert result == {7: 10}

    def test_empty_tags(self):
        result = _build_tag_user_map([], {"dazrave": 10})
        assert result == {}

    def test_missing_id_skipped(self):
        tags = [{"label": "1 - dazrave"}]
        users = {"dazrave": 10}
        result = _build_tag_user_map(tags, users)
        assert result == {}


class TestApplyTagOwnership:
    def test_no_owner_assigns(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="dazrave", quota_bytes=10000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)

        result = _apply_tag_ownership(db, content.id, user.id)
        assert result == {"resolved": 1}

        ownership = db.get_ownership(content.id)
        assert ownership.owner_user_id == user.id

    def test_same_owner_skips(self, db: Database):
        user = db.upsert_user(plex_user_id="1", plex_username="dazrave", quota_bytes=10000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user.id)

        result = _apply_tag_ownership(db, content.id, user.id)
        assert result == {"skipped": 0}

    def test_different_owner_updates(self, db: Database):
        user1 = db.upsert_user(plex_user_id="1", plex_username="alice", quota_bytes=10000)
        user2 = db.upsert_user(plex_user_id="2", plex_username="bob", quota_bytes=10000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user1.id)

        result = _apply_tag_ownership(db, content.id, user2.id)
        assert result == {"updated": 1}

        ownership = db.get_ownership(content.id)
        assert ownership.owner_user_id == user2.id

    def test_promoted_not_overridden(self, db: Database):
        user1 = db.upsert_user(plex_user_id="1", plex_username="alice", quota_bytes=10000)
        user2 = db.upsert_user(plex_user_id="2", plex_username="bob", quota_bytes=10000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user1.id)
        db.promote_content(content.id)

        result = _apply_tag_ownership(db, content.id, user2.id)
        assert result == {"skipped": 1}

        ownership = db.get_ownership(content.id)
        assert ownership.status == "promoted"

    def test_plank_not_overridden(self, db: Database):
        user1 = db.upsert_user(plex_user_id="1", plex_username="alice", quota_bytes=10000)
        user2 = db.upsert_user(plex_user_id="2", plex_username="bob", quota_bytes=10000)
        content = db.upsert_content(title="Film", media_type="movie", tmdb_id=1, size_bytes=5000)
        db.set_ownership(content.id, user1.id)
        db.plank_content(content.id)

        result = _apply_tag_ownership(db, content.id, user2.id)
        assert result == {"skipped": 1}

        ownership = db.get_ownership(content.id)
        assert ownership.status == "plank"


class TestSyncTagOwnership:
    @pytest.mark.asyncio
    async def test_disabled_returns_early(self, db: Database, config: Config):
        db.set_setting("tag_ownership_enabled", "false")

        result = await sync_tag_ownership(db, config)
        assert result["disabled"] is True
        assert result["resolved"] == 0

    @pytest.mark.asyncio
    async def test_resolves_radarr_movie(self, db: Database, config: Config):
        user = db.upsert_user(plex_user_id="1", plex_username="dazrave", quota_bytes=10000)
        content = db.upsert_content(
            title="Test Movie", media_type="movie", tmdb_id=1,
            radarr_id=42, size_bytes=5000,
        )

        mock_tags = [{"id": 7, "label": "1 - dazrave"}]
        mock_movies = [ArrMedia(id=42, title="Test Movie", tmdb_id=1, size_bytes=5000, path="/movies/test", tags=(7,))]

        with patch("treasurr.sync.tag_sync.RadarrClient") as MockRadarr, \
             patch("treasurr.sync.tag_sync.SonarrClient") as MockSonarr:
            MockSonarr.return_value.get_tags = AsyncMock(return_value=[])
            MockRadarr.return_value.get_tags = AsyncMock(return_value=mock_tags)
            MockRadarr.return_value.get_all_movies = AsyncMock(return_value=mock_movies)

            result = await sync_tag_ownership(db, config)

        assert result["resolved"] == 1
        ownership = db.get_ownership(content.id)
        assert ownership.owner_user_id == user.id

    @pytest.mark.asyncio
    async def test_resolves_sonarr_series(self, db: Database, config: Config):
        user = db.upsert_user(plex_user_id="1", plex_username="alice", quota_bytes=10000)
        content = db.upsert_content(
            title="Test Show", media_type="show", tmdb_id=2,
            sonarr_id=99, size_bytes=8000,
        )

        mock_tags = [{"id": 3, "label": "5 - alice"}]
        mock_series = [ArrMedia(id=99, title="Test Show", tmdb_id=2, size_bytes=8000, path="/tv/test", tags=(3,))]

        with patch("treasurr.sync.tag_sync.SonarrClient") as MockSonarr, \
             patch("treasurr.sync.tag_sync.RadarrClient") as MockRadarr:
            MockSonarr.return_value.get_tags = AsyncMock(return_value=mock_tags)
            MockSonarr.return_value.get_all_series = AsyncMock(return_value=mock_series)
            MockRadarr.return_value.get_tags = AsyncMock(return_value=[])

            result = await sync_tag_ownership(db, config)

        assert result["resolved"] == 1
        ownership = db.get_ownership(content.id)
        assert ownership.owner_user_id == user.id

    @pytest.mark.asyncio
    async def test_no_user_tags_noop(self, db: Database, config: Config):
        db.upsert_user(plex_user_id="1", plex_username="dazrave", quota_bytes=10000)

        with patch("treasurr.sync.tag_sync.RadarrClient") as MockRadarr, \
             patch("treasurr.sync.tag_sync.SonarrClient") as MockSonarr:
            MockSonarr.return_value.get_tags = AsyncMock(return_value=[{"id": 1, "label": "keep"}])
            MockRadarr.return_value.get_tags = AsyncMock(return_value=[{"id": 2, "label": "upgrade"}])

            result = await sync_tag_ownership(db, config)

        assert result["resolved"] == 0
        assert result["updated"] == 0
        assert result["skipped"] == 0

    @pytest.mark.asyncio
    async def test_content_not_in_db_skipped(self, db: Database, config: Config):
        db.upsert_user(plex_user_id="1", plex_username="dazrave", quota_bytes=10000)

        mock_tags = [{"id": 7, "label": "1 - dazrave"}]
        mock_movies = [ArrMedia(id=999, title="Unknown Movie", tmdb_id=999, size_bytes=5000, path="/movies/unknown", tags=(7,))]

        with patch("treasurr.sync.tag_sync.RadarrClient") as MockRadarr, \
             patch("treasurr.sync.tag_sync.SonarrClient") as MockSonarr:
            MockSonarr.return_value.get_tags = AsyncMock(return_value=[])
            MockRadarr.return_value.get_tags = AsyncMock(return_value=mock_tags)
            MockRadarr.return_value.get_all_movies = AsyncMock(return_value=mock_movies)

            result = await sync_tag_ownership(db, config)

        assert result["skipped"] == 1
        assert result["resolved"] == 0
