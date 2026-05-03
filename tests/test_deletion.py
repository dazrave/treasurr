"""Regression tests for the scuttle deletion path.

These cover the long-standing bug where scuttled content kept being
re-grabbed by Radarr/Sonarr because:
  1. Movies/shows were deleted without `addImportListExclusion=true`, so
     Overseerr's import-list sync would happily re-add them.
  2. Per-season scuttle deleted episode files but left the episodes
     `monitored: true`, so Sonarr re-downloaded them on the next search.
"""

from __future__ import annotations

import os
import tempfile
from unittest.mock import AsyncMock, patch

import pytest

from treasurr.config import ApiConfig, Config, QuotaConfig, SafetyConfig
from treasurr.db import Database
from treasurr.engine.deletion import _execute_deletion, scuttle_season


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(path)
    yield database
    os.unlink(path)


@pytest.fixture
def config_with_arrs():
    return Config(
        db_path=":memory:",
        quotas=QuotaConfig(plank_days=0),
        safety=SafetyConfig(max_deletions_per_hour=100),
        sonarr=ApiConfig(url="http://sonarr:8989/api/v3", key="s"),
        radarr=ApiConfig(url="http://radarr:7878/api/v3", key="r"),
        overseerr=ApiConfig(url="http://overseerr:5055/api/v1", key="o"),
    )


class TestExecuteDeletionExclusion:
    @pytest.mark.asyncio
    async def test_movie_delete_passes_import_list_exclusion(
        self, db: Database, config_with_arrs: Config
    ):
        """Radarr delete must include addImportListExclusion=True so Overseerr
        can't silently re-add the movie via import-list sync."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(
            title="Film", media_type="movie", tmdb_id=1, radarr_id=42, size_bytes=5000,
        )
        db.set_ownership(content.id, user.id)

        with patch("treasurr.engine.deletion.RadarrClient") as MockRadarr:
            instance = MockRadarr.return_value
            instance.unmonitor = AsyncMock()
            instance.delete = AsyncMock()

            result = await _execute_deletion(db, config_with_arrs, content.id, user.id)

        assert result.success
        instance.delete.assert_awaited_once()
        kwargs = instance.delete.await_args.kwargs
        assert kwargs.get("delete_files") is True
        assert kwargs.get("add_import_list_exclusion") is True

    @pytest.mark.asyncio
    async def test_show_delete_passes_import_list_exclusion(
        self, db: Database, config_with_arrs: Config
    ):
        """Sonarr delete must also pass addImportListExclusion=True."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(
            title="Show", media_type="show", tmdb_id=2, sonarr_id=99, size_bytes=8000,
        )
        db.set_ownership(content.id, user.id)

        with patch("treasurr.engine.deletion.SonarrClient") as MockSonarr:
            instance = MockSonarr.return_value
            instance.unmonitor = AsyncMock()
            instance.delete = AsyncMock()

            result = await _execute_deletion(db, config_with_arrs, content.id, user.id)

        assert result.success
        instance.delete.assert_awaited_once()
        kwargs = instance.delete.await_args.kwargs
        assert kwargs.get("delete_files") is True
        assert kwargs.get("add_import_list_exclusion") is True

    @pytest.mark.asyncio
    async def test_overseerr_request_is_declined_on_scuttle(
        self, db: Database, config_with_arrs: Config
    ):
        """If Treasurr knows the Overseerr request id, scuttle should
        decline the request as belt-and-braces against re-grab."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(
            title="Film", media_type="movie", tmdb_id=3, radarr_id=12, size_bytes=5000,
            overseerr_request_id=777,
        )
        db.set_ownership(content.id, user.id)

        with patch("treasurr.engine.deletion.RadarrClient") as MockRadarr, \
             patch("treasurr.engine.deletion.OverseerrClient") as MockOverseerr:
            MockRadarr.return_value.unmonitor = AsyncMock()
            MockRadarr.return_value.delete = AsyncMock()
            MockOverseerr.return_value.decline_request = AsyncMock()

            result = await _execute_deletion(db, config_with_arrs, content.id, user.id)

        assert result.success
        MockOverseerr.return_value.decline_request.assert_awaited_once_with(777)

    @pytest.mark.asyncio
    async def test_scuttle_succeeds_when_overseerr_decline_fails(
        self, db: Database, config_with_arrs: Config
    ):
        """Overseerr decline is best-effort — it must not break the scuttle."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(
            title="Film", media_type="movie", tmdb_id=4, radarr_id=13, size_bytes=5000,
            overseerr_request_id=888,
        )
        db.set_ownership(content.id, user.id)

        with patch("treasurr.engine.deletion.RadarrClient") as MockRadarr, \
             patch("treasurr.engine.deletion.OverseerrClient") as MockOverseerr:
            MockRadarr.return_value.unmonitor = AsyncMock()
            MockRadarr.return_value.delete = AsyncMock()
            MockOverseerr.return_value.decline_request = AsyncMock(
                side_effect=RuntimeError("overseerr unreachable"),
            )

            result = await _execute_deletion(db, config_with_arrs, content.id, user.id)

        assert result.success
        assert db.get_content(content.id).status == "deleted"


class TestScuttleSeasonUnmonitor:
    @pytest.mark.asyncio
    async def test_season_scuttle_unmonitors_episodes_and_season(
        self, db: Database, config_with_arrs: Config
    ):
        """After deleting episode files, Sonarr must be told to stop
        monitoring those episodes AND the season as a whole. Otherwise
        Sonarr's next RSS sweep treats them as 'wanted, missing'."""
        user = db.upsert_user(plex_user_id="1", plex_username="p", quota_bytes=100_000)
        content = db.upsert_content(
            title="Show", media_type="show", tmdb_id=5, sonarr_id=55, size_bytes=12_000,
        )
        db.set_ownership(content.id, user.id)
        db.upsert_season(content.id, 1, episode_count=2, size_bytes=6000)
        db.upsert_season(content.id, 2, episode_count=1, size_bytes=6000)

        episodes = [
            {"id": 101, "seasonNumber": 1, "hasFile": True,
             "episodeFile": {"id": 9001, "size": 3000}},
            {"id": 102, "seasonNumber": 1, "hasFile": True,
             "episodeFile": {"id": 9002, "size": 3000}},
            {"id": 201, "seasonNumber": 2, "hasFile": True,
             "episodeFile": {"id": 9003, "size": 6000}},
        ]

        with patch("treasurr.engine.deletion.SonarrClient") as MockSonarr:
            instance = MockSonarr.return_value
            instance.get_episodes = AsyncMock(return_value=episodes)
            instance.delete_episode_file = AsyncMock()
            instance.unmonitor_episodes = AsyncMock()
            instance.unmonitor_season = AsyncMock()

            result = await scuttle_season(db, config_with_arrs, content.id, 1, user.id)

        assert result.success
        # Both season-1 episode files deleted (and only those)
        deleted_file_ids = [c.args[0] for c in instance.delete_episode_file.await_args_list]
        assert sorted(deleted_file_ids) == [9001, 9002]
        # Episodes for season 1 unmonitored (and only those)
        instance.unmonitor_episodes.assert_awaited_once_with([101, 102])
        # Season-level monitor flag flipped
        instance.unmonitor_season.assert_awaited_once_with(55, 1)
