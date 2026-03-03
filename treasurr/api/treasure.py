"""User treasure chest, plunder, and plank endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request

from treasurr.api.auth import get_current_user, get_effective_user
from treasurr.db import Database
from treasurr.engine.deletion import scuttle_content, scuttle_season
from treasurr.engine.plank import rescue_content
from treasurr.engine.quota import format_bytes

router = APIRouter(prefix="/api", tags=["treasure"])

TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w300"

DEFAULT_SERVER_MESSAGE = (
    "Ahoy, crew! Welcome aboard. This be yer treasure chest - where ye can see "
    "what's takin' up space on the ship. When enough of the crew watches somethin', "
    "it becomes shared plunder and stops countin' against yer quota. Keep things "
    "tidy and everyone sails happy!"
)


def _get_db(request: Request) -> Database:
    return request.app.state.db


def _get_config(request: Request):
    return request.app.state.config


async def _require_user(request: Request):
    user = await get_current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


async def _require_effective_user(request: Request):
    """Get the effective user (supports view_as for admins)."""
    effective, real = await get_effective_user(request)
    if effective is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return effective, real


@router.get("/treasure")
async def get_treasure_summary(request: Request) -> dict:
    """Get the current user's quota summary."""
    effective, real = await _require_effective_user(request)
    db = _get_db(request)
    config = _get_config(request)

    db_settings = db.get_all_settings()
    promotion_mode = db_settings.get("promotion_mode", config.quotas.promotion_mode)
    min_retention_days = int(db_settings.get("min_retention_days", str(config.quotas.min_retention_days)))
    display_mode = db_settings.get("display_mode", config.quotas.display_mode)
    plank_mode = db_settings.get("plank_mode", config.quotas.plank_mode)
    plank_days = int(db_settings.get("plank_days", str(config.quotas.plank_days)))
    promotion_threshold = int(db_settings.get(
        "promotion_threshold", str(config.quotas.promotion_threshold),
    ))

    include_splits = promotion_mode == "split_the_loot"
    summary = db.get_quota_summary(effective.id, include_splits=include_splits, plank_mode=plank_mode)
    if summary is None:
        raise HTTPException(status_code=404, detail="User not found")

    # Calculate reserved bytes from download queue
    reserved_bytes = 0
    try:
        queue_raw = db.get_setting("download_queue", "[]")
        queue_items = json.loads(queue_raw)
        content_items = db.get_user_owned_content(effective.id)
        owned_arr_ids = set()
        for ci in content_items:
            if ci.content.sonarr_id:
                owned_arr_ids.add(("sonarr", ci.content.sonarr_id))
            if ci.content.radarr_id:
                owned_arr_ids.add(("radarr", ci.content.radarr_id))
        for qi in queue_items:
            if (qi.get("arr_type"), qi.get("arr_id")) in owned_arr_ids:
                reserved_bytes += qi.get("sizeleft_bytes", 0)
    except (json.JSONDecodeError, TypeError):
        pass

    server_message = db.get_setting("server_message", DEFAULT_SERVER_MESSAGE)

    result = {
        "user_id": effective.id,
        "username": effective.plex_username,
        "quota_bytes": summary.quota_bytes,
        "bonus_bytes": summary.bonus_bytes,
        "total_bytes": summary.total_bytes,
        "used_bytes": summary.total_used_bytes,
        "available_bytes": summary.available_bytes,
        "usage_percent": summary.usage_percent,
        "owned_count": summary.owned_count,
        "split_bytes": summary.split_bytes,
        "quota_display": format_bytes(summary.total_bytes),
        "used_display": format_bytes(summary.total_used_bytes),
        "available_display": format_bytes(summary.available_bytes),
        "auto_scuttle_days": effective.auto_scuttle_days,
        "min_retention_days": min_retention_days,
        "display_mode": display_mode,
        "promotion_mode": promotion_mode,
        "plank_days": plank_days,
        "plank_mode": plank_mode,
        "onboarded": effective.onboarded,
        "promotion_threshold": promotion_threshold,
        "server_message": server_message,
        "reserved_bytes": reserved_bytes,
        "reserved_display": format_bytes(reserved_bytes) if reserved_bytes > 0 else "",
    }

    # Add view_as metadata if admin is viewing as another user
    if real.id != effective.id:
        result["view_as"] = {
            "user_id": effective.id,
            "username": effective.plex_username,
            "admin_username": real.plex_username,
        }

    return result


def _derive_quality(size_bytes: int, media_type: str) -> tuple[str, str]:
    """Derive quality label and note from file size and media type."""
    if media_type == "show":
        if size_bytes > 30_000_000_000:
            return "4K", "4K Ultra HD - best quality, larger file"
        if size_bytes > 8_000_000_000:
            return "1080p HD", "Full HD - great quality"
        if size_bytes > 2_000_000_000:
            return "720p HD", "HD - good quality, smaller file"
        return "SD", "Standard definition - smallest file"

    # movie
    if size_bytes > 15_000_000_000:
        return "4K", "4K Ultra HD - best quality, larger file"
    if size_bytes > 4_000_000_000:
        return "1080p HD", "Full HD - great quality"
    if size_bytes > 1_000_000_000:
        return "720p HD", "HD - good quality, smaller file"
    return "SD", "Standard definition - smallest file"


def _poster_url(poster_path: str | None) -> str | None:
    """Build full TMDB poster URL from relative path."""
    if not poster_path:
        return None
    return f"{TMDB_IMAGE_BASE}{poster_path}"


@router.get("/treasure/chest")
async def get_treasure_chest(request: Request) -> dict:
    """Get the current user's owned content list with poster and season data."""
    effective, real = await _require_effective_user(request)
    db = _get_db(request)
    config = _get_config(request)
    items = db.get_user_owned_content(effective.id)

    # Load download queue for ghost cards
    download_map: dict[tuple[str, int], dict] = {}
    try:
        queue_raw = db.get_setting("download_queue", "[]")
        queue_items = json.loads(queue_raw)
        for qi in queue_items:
            key = (qi.get("arr_type"), qi.get("arr_id"))
            if key[1] is not None:
                download_map[key] = qi
    except (json.JSONDecodeError, TypeError):
        pass

    result_items = []
    for item in items:
        # Check if this item is currently downloading
        downloading = False
        download_info = {}
        if item.content.sonarr_id:
            dl = download_map.get(("sonarr", item.content.sonarr_id))
            if dl:
                downloading = True
                download_info = dl
        if not downloading and item.content.radarr_id:
            dl = download_map.get(("radarr", item.content.radarr_id))
            if dl:
                downloading = True
                download_info = dl

        # Skip zero-size items unless they are downloading
        if item.content.size_bytes <= 0 and not downloading:
            continue

        quality, quality_note = _derive_quality(
            item.content.size_bytes or download_info.get("size_bytes", 0),
            item.content.media_type,
        )

        entry = {
            "content_id": item.content.id,
            "title": item.content.title,
            "media_type": item.content.media_type,
            "size_bytes": item.content.size_bytes,
            "size_display": format_bytes(item.content.size_bytes) if item.content.size_bytes > 0 else "",
            "poster_url": _poster_url(item.content.poster_path),
            "quality": quality,
            "quality_note": quality_note,
            "status": item.ownership.status,
            "owned_at": item.ownership.owned_at,
            "promoted_at": item.ownership.promoted_at,
            "buried_at": item.ownership.buried_at,
            "unique_viewers": item.unique_viewers,
            "can_scuttle": item.ownership.status in ("owned", "buried"),
            "can_bury": item.ownership.status == "owned",
            "is_buried": item.ownership.status == "buried",
        }

        # Download ghost card info
        if downloading:
            entry["downloading"] = True
            entry["download_progress"] = download_info.get("progress", 0)
            entry["download_eta"] = download_info.get("eta", "")
            entry["download_size_bytes"] = download_info.get("size_bytes", 0)
            entry["download_size_display"] = format_bytes(download_info.get("size_bytes", 0))
        else:
            entry["downloading"] = False

        # Include season data for shows
        if item.content.media_type == "show":
            seasons = db.get_seasons(item.content.id)
            entry["seasons"] = [
                {
                    "season_number": s.season_number,
                    "episode_count": s.episode_count,
                    "size_bytes": s.size_bytes,
                    "size_display": format_bytes(s.size_bytes),
                }
                for s in seasons
            ]
        else:
            entry["seasons"] = None

        result_items.append(entry)

    response = {"items": result_items}

    if real.id != effective.id:
        response["view_as"] = {
            "user_id": effective.id,
            "username": effective.plex_username,
            "admin_username": real.plex_username,
        }

    return response


@router.post("/treasure/{content_id}/scuttle")
async def scuttle(request: Request, content_id: int) -> dict:
    """Delete content owned by the current user."""
    effective, _ = await _require_effective_user(request)
    db = _get_db(request)
    config = _get_config(request)

    # If buried, unbury first so scuttle can proceed
    ownership = db.get_ownership(content_id)
    if ownership and ownership.status == "buried" and ownership.owner_user_id == effective.id:
        db.unbury_content(content_id)

    result = await scuttle_content(db, config, content_id, effective.id)

    if not result.success:
        raise HTTPException(status_code=400, detail=result.message)

    response = {
        "success": True,
        "message": result.message,
        "walked_plank": result.walked_plank,
    }
    if result.walked_plank:
        db_settings = db.get_all_settings()
        plank_days = int(db_settings.get("plank_days", str(config.quotas.plank_days)))
        response["plank_days"] = plank_days
    else:
        response["freed_bytes"] = result.freed_bytes
        response["freed_display"] = format_bytes(result.freed_bytes)

    return response


@router.post("/treasure/{content_id}/scuttle-season/{season_number}")
async def scuttle_season_endpoint(request: Request, content_id: int, season_number: int) -> dict:
    """Delete all episode files for a specific season."""
    effective, _ = await _require_effective_user(request)
    db = _get_db(request)
    config = _get_config(request)

    result = await scuttle_season(db, config, content_id, season_number, effective.id)

    if not result.success:
        raise HTTPException(status_code=400, detail=result.message)

    return {
        "success": True,
        "message": result.message,
        "freed_bytes": result.freed_bytes,
        "freed_display": format_bytes(result.freed_bytes),
    }


@router.get("/plank")
async def get_plank_content(request: Request) -> dict:
    """Get all content currently walking the plank."""
    await _require_user(request)
    db = _get_db(request)
    config = _get_config(request)
    items = db.get_plank_content()

    db_settings = db.get_all_settings()
    plank_days = int(db_settings.get("plank_days", str(config.quotas.plank_days)))
    plank_mode = db_settings.get("plank_mode", config.quotas.plank_mode)

    # Resolve owner usernames
    owner_cache: dict[int, str] = {}

    def _get_owner_username(owner_id: int) -> str:
        if owner_id not in owner_cache:
            user = db.get_user(owner_id)
            owner_cache[owner_id] = user.plex_username if user else "Unknown"
        return owner_cache[owner_id]

    return {
        "plank_mode": plank_mode,
        "plank_days": plank_days,
        "items": [
            {
                "content_id": item.content.id,
                "title": item.content.title,
                "media_type": item.content.media_type,
                "size_bytes": item.content.size_bytes,
                "size_display": format_bytes(item.content.size_bytes),
                "poster_url": _poster_url(item.content.poster_path),
                "owner_user_id": item.ownership.owner_user_id,
                "owner_username": _get_owner_username(item.ownership.owner_user_id),
                "plank_started_at": item.ownership.plank_started_at,
                "unique_viewers": item.unique_viewers,
            }
            for item in items
        ],
    }


@router.post("/treasure/{content_id}/rescue")
async def rescue(request: Request, content_id: int) -> dict:
    """Rescue content from the plank."""
    user = await _require_user(request)
    db = _get_db(request)
    config = _get_config(request)

    result = await rescue_content(db, config, content_id, user.id)

    if not result.success:
        raise HTTPException(status_code=400, detail=result.message)

    return {
        "success": True,
        "message": result.message,
        "action": result.action,
    }


@router.post("/treasure/{content_id}/bury")
async def bury_toggle(request: Request, content_id: int) -> dict:
    """Toggle bury/unbury on content to protect from auto-cleanup."""
    user = await _require_user(request)
    db = _get_db(request)

    ownership = db.get_ownership(content_id)
    if ownership is None:
        raise HTTPException(status_code=404, detail="Content not found")
    if ownership.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="You don't own this content")

    if ownership.status == "buried":
        db.unbury_content(content_id)
        return {"buried": False, "message": "Treasure unburied - auto-cleanup can remove it again"}
    if ownership.status == "owned":
        db.bury_content(content_id)
        return {"buried": True, "message": "Treasure buried! Protected from auto-cleanup"}

    raise HTTPException(status_code=400, detail="Content must be owned to bury/unbury")


@router.get("/plunder")
async def get_shared_plunder(request: Request) -> dict:
    """Get promoted content relevant to the current user."""
    user = await _require_user(request)
    db = _get_db(request)
    items = db.get_relevant_promoted_content(user.id)

    return {
        "items": [
            {
                "content_id": item.id,
                "title": item.title,
                "media_type": item.media_type,
                "size_bytes": item.size_bytes,
                "size_display": format_bytes(item.size_bytes),
                "poster_url": _poster_url(item.poster_path),
            }
            for item in items
        ],
    }


@router.get("/activity")
async def get_activity(request: Request) -> dict:
    """Get recent promotions and deletions."""
    await _require_user(request)
    db = _get_db(request)

    promotions = db.get_recent_promotions(limit=10)
    deletions = db.get_recent_deletions(limit=10)

    # Merge and sort by time
    events = []
    for p in promotions:
        content = db.get_content(p.content_id)
        title = content.title if content else "Unknown"
        events.append({
            "type": "promotion",
            "title": title,
            "size_bytes": p.size_freed_bytes,
            "size_display": format_bytes(p.size_freed_bytes),
            "viewers": p.unique_viewers,
            "at": p.promoted_at,
        })
    for d in deletions:
        events.append({
            "type": "deletion",
            "title": d.title,
            "size_bytes": d.size_bytes,
            "size_display": format_bytes(d.size_bytes),
            "at": d.deleted_at,
        })

    events.sort(key=lambda e: e["at"], reverse=True)

    return {"events": events[:20]}


@router.put("/treasure/retention")
async def set_retention(request: Request) -> dict:
    """Set the user's auto-scuttle retention period."""
    user = await _require_user(request)
    db = _get_db(request)
    body = await request.json()

    days = int(body.get("auto_scuttle_days", 0))
    valid_days = (0, 7, 14, 30, 60, 90)
    if days not in valid_days:
        raise HTTPException(status_code=400, detail=f"auto_scuttle_days must be one of: {valid_days}")

    updated = db.update_user_auto_scuttle(user.id, days)
    if updated is None:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "auto_scuttle_days": updated.auto_scuttle_days,
        "message": "Auto-scuttle disabled" if days == 0 else f"Content will auto-delete {days} days after you watch it",
    }


@router.get("/treasure/unclaimed")
async def get_unclaimed(request: Request) -> dict:
    """Get content on the server that nobody has claimed."""
    await _require_user(request)
    db = _get_db(request)
    items = db.get_unclaimed_content()

    return {
        "items": [
            {
                "content_id": item.id,
                "title": item.title,
                "media_type": item.media_type,
                "size_bytes": item.size_bytes,
                "size_display": format_bytes(item.size_bytes),
                "poster_url": _poster_url(item.poster_path),
            }
            for item in items
        ],
    }


@router.post("/treasure/{content_id}/claim")
async def claim_content(request: Request, content_id: int) -> dict:
    """Claim unclaimed content, adding it to the user's chest."""
    user = await _require_user(request)
    db = _get_db(request)

    content = db.get_content(content_id)
    if content is None:
        raise HTTPException(status_code=404, detail="Content not found")

    if content.status != "active":
        raise HTTPException(status_code=400, detail="Content is not active")

    existing = db.get_ownership(content_id)
    if existing is not None:
        raise HTTPException(status_code=400, detail="Content already has an owner")

    # Check quota
    config = _get_config(request)
    db_settings = db.get_all_settings()
    plank_mode = db_settings.get("plank_mode", config.quotas.plank_mode)
    summary = db.get_quota_summary(user.id, plank_mode=plank_mode)
    if summary and (summary.available_bytes < content.size_bytes):
        raise HTTPException(
            status_code=400,
            detail="Not enough quota to claim this content ("
            + format_bytes(content.size_bytes) + " needed, "
            + format_bytes(summary.available_bytes) + " available)",
        )

    db.claim_content(content_id, user.id)

    return {
        "success": True,
        "message": f"'{content.title}' has been claimed! It's now in your treasure chest.",
        "content_id": content_id,
    }


@router.post("/treasure/onboarded")
async def mark_onboarded(request: Request) -> dict:
    """Mark the current user as having completed onboarding."""
    user = await _require_user(request)
    db = _get_db(request)
    db.update_user_onboarded(user.id)
    return {"onboarded": True}
