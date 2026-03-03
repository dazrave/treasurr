"""HTTP clients for Tautulli, Overseerr/Seer, Sonarr, and Radarr APIs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from treasurr.config import ApiConfig

logger = logging.getLogger(__name__)


class ApiError(Exception):
    """Raised when an external API call fails."""

    def __init__(self, service: str, message: str, status_code: int | None = None) -> None:
        self.service = service
        self.status_code = status_code
        super().__init__(f"{service}: {message} (HTTP {status_code})")


@dataclass(frozen=True)
class TautulliUser:
    user_id: str
    username: str
    email: str


@dataclass(frozen=True)
class TautulliWatchRecord:
    user_id: str
    rating_key: str
    grandparent_rating_key: str | None
    title: str
    media_type: str  # 'movie' or 'episode'
    watched_at: str
    percent_complete: int


@dataclass(frozen=True)
class OverseerrRequest:
    request_id: int
    tmdb_id: int
    media_type: str  # 'movie' or 'tv'
    title: str
    requested_by_user_id: int
    requested_by_username: str
    requested_by_email: str
    status: int


@dataclass(frozen=True)
class ArrMedia:
    id: int
    title: str
    tmdb_id: int
    size_bytes: int
    path: str


class TautulliClient:
    """Client for Tautulli API v2."""

    def __init__(self, config: ApiConfig) -> None:
        self._base_url = config.url.rstrip("/")
        self._api_key = config.key

    async def _get(self, cmd: str, **params: Any) -> dict:
        params["apikey"] = self._api_key
        params["cmd"] = cmd
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(self._base_url, params=params)
            if resp.status_code != 200:
                raise ApiError("tautulli", f"cmd={cmd} failed", resp.status_code)
            data = resp.json()
            return data.get("response", {}).get("data", {})

    async def get_users(self) -> list[TautulliUser]:
        data = await self._get("get_users_table", length=500)
        users = []
        for row in data.get("data", []):
            users.append(TautulliUser(
                user_id=str(row.get("user_id", "")),
                username=row.get("friendly_name", row.get("username", "")),
                email=row.get("email", ""),
            ))
        return users

    async def get_history(self, length: int = 500, start: int = 0) -> list[TautulliWatchRecord]:
        data = await self._get("get_history", length=length, start=start)
        records = []
        for row in data.get("data", []):
            percent = row.get("percent_complete", 0)
            if isinstance(percent, str):
                try:
                    percent = int(percent)
                except ValueError:
                    percent = 0

            media_type = row.get("media_type", "")
            records.append(TautulliWatchRecord(
                user_id=str(row.get("user_id", "")),
                rating_key=str(row.get("rating_key", "")),
                grandparent_rating_key=str(row.get("grandparent_rating_key", "")) or None,
                title=row.get("full_title", row.get("title", "")),
                media_type=media_type,
                watched_at=str(row.get("date", "")),
                percent_complete=percent,
            ))
        return records

    async def get_server_id(self) -> str:
        data = await self._get("get_server_info")
        return str(data.get("pms_identifier", ""))


class OverseerrClient:
    """Client for Overseerr/Seer API v1."""

    def __init__(self, config: ApiConfig) -> None:
        self._base_url = config.url.rstrip("/")
        self._api_key = config.key
        self._type = config.type  # 'overseerr' or 'seer'

    async def _get(self, path: str, **params: Any) -> Any:
        headers = {"X-Api-Key": self._api_key}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{self._base_url}{path}", headers=headers, params=params)
            if resp.status_code != 200:
                raise ApiError("overseerr", f"GET {path} failed", resp.status_code)
            return resp.json()

    async def get_requests(self, take: int = 100, skip: int = 0) -> list[OverseerrRequest]:
        data = await self._get("/request", take=take, skip=skip, sort="added", filter="all")
        requests = []
        for item in data.get("results", []):
            media = item.get("media", {})
            requested_by = item.get("requestedBy", {})
            media_type = media.get("mediaType", "movie")
            # Title can be in multiple places depending on Overseerr/Seer version
            title = (
                media.get("title")
                or media.get("originalTitle")
                or media.get("name")
                or item.get("title")
                or item.get("name")
                or "Unknown"
            )
            requests.append(OverseerrRequest(
                request_id=item.get("id", 0),
                tmdb_id=media.get("tmdbId", 0),
                media_type=media_type,
                title=title,
                requested_by_user_id=requested_by.get("id", 0),
                requested_by_username=requested_by.get("displayName", requested_by.get("username", "")),
                requested_by_email=requested_by.get("email", ""),
                status=item.get("status", 0),
            ))
        return requests

    async def get_all_requests(self) -> list[OverseerrRequest]:
        all_requests: list[OverseerrRequest] = []
        skip = 0
        take = 100
        while True:
            batch = await self.get_requests(take=take, skip=skip)
            all_requests.extend(batch)
            if len(batch) < take:
                break
            skip += take
        return all_requests

    async def get_user(self, user_id: int) -> dict:
        return await self._get(f"/user/{user_id}")


class SonarrClient:
    """Client for Sonarr API v3."""

    def __init__(self, config: ApiConfig) -> None:
        self._base_url = config.url.rstrip("/")
        self._api_key = config.key

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = {"X-Api-Key": self._api_key}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.request(method, f"{self._base_url}{path}", headers=headers, **kwargs)
            if resp.status_code not in (200, 201):
                raise ApiError("sonarr", f"{method} {path} failed", resp.status_code)
            if resp.content:
                return resp.json()
            return None

    async def get_all_series(self) -> list[ArrMedia]:
        data = await self._request("GET", "/series")
        results = []
        for item in data:
            stats = item.get("statistics", {})
            results.append(ArrMedia(
                id=item["id"],
                title=item.get("title", ""),
                tmdb_id=item.get("tmdbId", 0),
                size_bytes=stats.get("sizeOnDisk", 0),
                path=item.get("path", ""),
            ))
        return results

    async def get_series(self, series_id: int) -> ArrMedia:
        data = await self._request("GET", f"/series/{series_id}")
        stats = data.get("statistics", {})
        return ArrMedia(
            id=data["id"],
            title=data.get("title", ""),
            tmdb_id=data.get("tmdbId", 0),
            size_bytes=stats.get("sizeOnDisk", 0),
            path=data.get("path", ""),
        )

    async def unmonitor(self, series_id: int) -> None:
        data = await self._request("GET", f"/series/{series_id}")
        data["monitored"] = False
        await self._request("PUT", f"/series/{series_id}", json=data)

    async def delete(self, series_id: int, delete_files: bool = True) -> None:
        await self._request("DELETE", f"/series/{series_id}", params={"deleteFiles": delete_files})

    async def lookup_by_tmdb(self, tmdb_id: int) -> ArrMedia | None:
        data = await self._request("GET", "/series/lookup", params={"term": f"tmdb:{tmdb_id}"})
        if data and len(data) > 0:
            item = data[0]
            stats = item.get("statistics", {})
            return ArrMedia(
                id=item.get("id", 0),
                title=item.get("title", ""),
                tmdb_id=item.get("tmdbId", tmdb_id),
                size_bytes=stats.get("sizeOnDisk", 0),
                path=item.get("path", ""),
            )
        return None


class RadarrClient:
    """Client for Radarr API v3."""

    def __init__(self, config: ApiConfig) -> None:
        self._base_url = config.url.rstrip("/")
        self._api_key = config.key

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = {"X-Api-Key": self._api_key}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.request(method, f"{self._base_url}{path}", headers=headers, **kwargs)
            if resp.status_code not in (200, 201):
                raise ApiError("radarr", f"{method} {path} failed", resp.status_code)
            if resp.content:
                return resp.json()
            return None

    async def get_all_movies(self) -> list[ArrMedia]:
        data = await self._request("GET", "/movie")
        results = []
        for item in data:
            movie_file = item.get("movieFile", {})
            results.append(ArrMedia(
                id=item["id"],
                title=item.get("title", ""),
                tmdb_id=item.get("tmdbId", 0),
                size_bytes=movie_file.get("size", item.get("sizeOnDisk", 0)),
                path=item.get("path", ""),
            ))
        return results

    async def get_movie(self, movie_id: int) -> ArrMedia:
        data = await self._request("GET", f"/movie/{movie_id}")
        movie_file = data.get("movieFile", {})
        return ArrMedia(
            id=data["id"],
            title=data.get("title", ""),
            tmdb_id=data.get("tmdbId", 0),
            size_bytes=movie_file.get("size", data.get("sizeOnDisk", 0)),
            path=data.get("path", ""),
        )

    async def unmonitor(self, movie_id: int) -> None:
        data = await self._request("GET", f"/movie/{movie_id}")
        data["monitored"] = False
        await self._request("PUT", f"/movie/{movie_id}", json=data)

    async def delete(self, movie_id: int, delete_files: bool = True) -> None:
        await self._request("DELETE", f"/movie/{movie_id}", params={"deleteFiles": delete_files})

    async def get_diskspace(self) -> list[dict]:
        """Get disk space info from Radarr. Returns list of {path, freeSpace, totalSpace}."""
        data = await self._request("GET", "/diskspace")
        return [
            {
                "path": item.get("path", ""),
                "free_bytes": item.get("freeSpace", 0),
                "total_bytes": item.get("totalSpace", 0),
            }
            for item in (data or [])
        ]

    async def lookup_by_tmdb(self, tmdb_id: int) -> ArrMedia | None:
        data = await self._request("GET", f"/movie/lookup/tmdb", params={"tmdbId": tmdb_id})
        if data:
            item = data if isinstance(data, dict) else data[0]
            movie_file = item.get("movieFile", {})
            return ArrMedia(
                id=item.get("id", 0),
                title=item.get("title", ""),
                tmdb_id=item.get("tmdbId", tmdb_id),
                size_bytes=movie_file.get("size", item.get("sizeOnDisk", 0)),
                path=item.get("path", ""),
            )
        return None
