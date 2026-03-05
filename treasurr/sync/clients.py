"""HTTP clients for Tautulli, Overseerr/Seer, Sonarr, and Radarr APIs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from treasurr.config import ApiConfig, PlexConfig

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
    tags: tuple[int, ...] = ()


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

    async def get_media_title(self, tmdb_id: int, media_type: str) -> str:
        """Look up a title from Overseerr's TMDB-backed media info."""
        try:
            path = f"/{'movie' if media_type == 'movie' else 'tv'}/{tmdb_id}"
            data = await self._get(path)
            return (
                data.get("title")
                or data.get("name")
                or data.get("originalTitle")
                or data.get("originalName")
                or ""
            )
        except Exception:
            return ""

    async def get_media_info(self, tmdb_id: int, media_type: str) -> dict:
        """Look up media info including poster path from Overseerr."""
        try:
            path = f"/{'movie' if media_type == 'movie' else 'tv'}/{tmdb_id}"
            data = await self._get(path)
            return {
                "title": (
                    data.get("title")
                    or data.get("name")
                    or data.get("originalTitle")
                    or data.get("originalName")
                    or ""
                ),
                "poster_path": data.get("posterPath", ""),
            }
        except Exception:
            return {"title": "", "poster_path": ""}

    async def get_user(self, user_id: int) -> dict:
        return await self._get(f"/user/{user_id}")

    async def get_service_settings(self, service: str) -> list[dict]:
        """Get Overseerr settings for a service ('sonarr' or 'radarr')."""
        return await self._get(f"/settings/{service}")

    async def enable_tag_requests(self, service: str, server_id: int) -> None:
        """Enable tagRequests on an Overseerr service server config."""
        headers = {"X-Api-Key": self._api_key}
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Read current config
            resp = await client.get(
                f"{self._base_url}/settings/{service}/{server_id}",
                headers=headers,
            )
            if resp.status_code != 200:
                raise ApiError("overseerr", f"GET /settings/{service}/{server_id} failed", resp.status_code)
            config = resp.json()
            config["tagRequests"] = True
            # Write back
            resp = await client.put(
                f"{self._base_url}/settings/{service}/{server_id}",
                headers=headers,
                json=config,
            )
            if resp.status_code not in (200, 201):
                raise ApiError("overseerr", f"PUT /settings/{service}/{server_id} failed", resp.status_code)

    async def decline_request(self, request_id: int) -> None:
        """Decline a pending request."""
        headers = {"X-Api-Key": self._api_key}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{self._base_url}/request/{request_id}/decline",
                headers=headers,
            )
            if resp.status_code not in (200, 201, 204):
                raise ApiError("overseerr", f"POST /request/{request_id}/decline failed", resp.status_code)


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

    async def get_tags(self) -> list[dict]:
        """Fetch all tags. Returns [{"id": 7, "label": "1 - dazrave"}, ...]."""
        data = await self._request("GET", "/tag")
        return [{"id": t["id"], "label": t.get("label", "")} for t in (data or [])]

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
                tags=tuple(item.get("tags", [])),
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

    async def get_episodes(self, series_id: int) -> list[dict]:
        """Fetch all episodes for a series. Returns raw episode data."""
        data = await self._request("GET", "/episode", params={
            "seriesId": series_id,
            "includeEpisodeFile": "true",
        })
        return data or []

    async def get_queue(self) -> list[dict]:
        """Fetch the download queue. Returns list of queue records."""
        data = await self._request("GET", "/queue", params={"pageSize": 100})
        return (data or {}).get("records", [])

    async def delete_episode_file(self, file_id: int) -> None:
        """Delete a single episode file by its file ID."""
        await self._request("DELETE", f"/episodefile/{file_id}")

    async def delete_queue_item(self, queue_id: int) -> None:
        """Cancel a download queue item and blocklist it."""
        await self._request(
            "DELETE", f"/queue/{queue_id}",
            params={"removeFromClient": "true", "blocklist": "true"},
        )

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

    async def get_tags(self) -> list[dict]:
        """Fetch all tags. Returns [{"id": 7, "label": "1 - dazrave"}, ...]."""
        data = await self._request("GET", "/tag")
        return [{"id": t["id"], "label": t.get("label", "")} for t in (data or [])]

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
                tags=tuple(item.get("tags", [])),
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

    async def get_queue(self) -> list[dict]:
        """Fetch the download queue. Returns list of queue records."""
        data = await self._request("GET", "/queue", params={"pageSize": 100})
        return (data or {}).get("records", [])

    async def delete_queue_item(self, queue_id: int) -> None:
        """Cancel a download queue item and blocklist it."""
        await self._request(
            "DELETE", f"/queue/{queue_id}",
            params={"removeFromClient": "true", "blocklist": "true"},
        )

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


class PlexClient:
    """Client for Plex Media Server API - collection management."""

    def __init__(self, config: PlexConfig) -> None:
        self._base_url = config.url.rstrip("/")
        self._token = config.token

    def _headers(self) -> dict:
        return {
            "X-Plex-Token": self._token,
            "Accept": "application/json",
        }

    async def _get(self, path: str, **params: Any) -> Any:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{self._base_url}{path}",
                headers=self._headers(),
                params=params,
            )
            if resp.status_code != 200:
                raise ApiError("plex", f"GET {path} failed", resp.status_code)
            return resp.json()

    async def _put(self, path: str, **params: Any) -> None:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.put(
                f"{self._base_url}{path}",
                headers=self._headers(),
                params=params,
            )
            if resp.status_code not in (200, 201):
                raise ApiError("plex", f"PUT {path} failed", resp.status_code)

    async def _post(self, path: str, **params: Any) -> Any:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{self._base_url}{path}",
                headers=self._headers(),
                params=params,
            )
            if resp.status_code not in (200, 201):
                raise ApiError("plex", f"POST {path} failed", resp.status_code)
            if resp.content:
                return resp.json()
            return None

    async def _delete(self, path: str) -> None:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.delete(
                f"{self._base_url}{path}",
                headers=self._headers(),
            )
            if resp.status_code not in (200, 204):
                raise ApiError("plex", f"DELETE {path} failed", resp.status_code)

    async def get_libraries(self) -> list[dict]:
        """Get all Plex libraries."""
        data = await self._get("/library/sections")
        return [
            {
                "key": lib["key"],
                "title": lib.get("title", ""),
                "type": lib.get("type", ""),
            }
            for lib in data.get("MediaContainer", {}).get("Directory", [])
        ]

    async def get_collections(self, library_key: str) -> list[dict]:
        """Get all collections in a library."""
        data = await self._get(f"/library/sections/{library_key}/collections")
        return [
            {
                "ratingKey": c["ratingKey"],
                "title": c.get("title", ""),
                "childCount": c.get("childCount", 0),
            }
            for c in data.get("MediaContainer", {}).get("Metadata", [])
        ]

    async def create_collection(self, library_key: str, title: str, rating_keys: list[str]) -> str | None:
        """Create a collection with the given items. Returns the collection ratingKey."""
        if not rating_keys:
            return None
        machine_id = await self._get_machine_id()
        uri = f"server://{machine_id}/com.plexapp.plugins.library/library/metadata/{','.join(rating_keys)}"
        data = await self._post(
            f"/library/sections/{library_key}/collections",
            type=18,
            title=title,
            smart=0,
            uri=uri,
        )
        metadata = data.get("MediaContainer", {}).get("Metadata", []) if data else []
        if metadata:
            return metadata[0].get("ratingKey")
        return None

    async def update_collection_items(self, collection_key: str, rating_keys: list[str]) -> None:
        """Replace collection items by removing all then adding new ones."""
        # Get current items
        try:
            data = await self._get(f"/library/collections/{collection_key}/children")
            current = [m["ratingKey"] for m in data.get("MediaContainer", {}).get("Metadata", [])]
        except Exception:
            current = []

        # Remove items not in new set
        for key in current:
            if key not in rating_keys:
                try:
                    await self._delete(f"/library/collections/{collection_key}/children/{key}")
                except Exception as e:
                    logger.warning("Failed to remove item %s from collection: %s", key, e)

        # Add items not in current set
        if rating_keys:
            machine_id = await self._get_machine_id()
            for key in rating_keys:
                if key not in current:
                    uri = f"server://{machine_id}/com.plexapp.plugins.library/library/metadata/{key}"
                    try:
                        await self._put(f"/library/collections/{collection_key}/items", uri=uri)
                    except Exception as e:
                        logger.warning("Failed to add item %s to collection: %s", key, e)

    async def delete_collection(self, collection_key: str) -> None:
        """Delete a collection."""
        await self._delete(f"/library/collections/{collection_key}")

    async def find_collection_by_title(self, library_key: str, title: str) -> dict | None:
        """Find a collection by its title in a library."""
        collections = await self.get_collections(library_key)
        for c in collections:
            if c["title"] == title:
                return c
        return None

    async def search_by_title(self, library_key: str, title: str) -> list[dict]:
        """Search for media by title in a library."""
        data = await self._get(f"/library/sections/{library_key}/search", query=title)
        return [
            {
                "ratingKey": m["ratingKey"],
                "title": m.get("title", ""),
                "type": m.get("type", ""),
            }
            for m in data.get("MediaContainer", {}).get("Metadata", [])
        ]

    async def _get_machine_id(self) -> str:
        """Get the Plex server machine identifier."""
        data = await self._get("/")
        return data.get("MediaContainer", {}).get("machineIdentifier", "")
