"""User treasure chest, plunder, and plank endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from treasurr.api.auth import get_current_user
from treasurr.db import Database
from treasurr.engine.deletion import scuttle_content
from treasurr.engine.plank import rescue_content
from treasurr.engine.quota import format_bytes

router = APIRouter(prefix="/api", tags=["treasure"])


def _get_db(request: Request) -> Database:
    return request.app.state.db


def _get_config(request: Request):
    return request.app.state.config


async def _require_user(request: Request):
    user = await get_current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


@router.get("/treasure")
async def get_treasure_summary(request: Request) -> dict:
    """Get the current user's quota summary."""
    user = await _require_user(request)
    db = _get_db(request)
    config = _get_config(request)

    db_settings = db.get_all_settings()
    promotion_mode = db_settings.get("promotion_mode", config.quotas.promotion_mode)
    min_retention_days = int(db_settings.get("min_retention_days", str(config.quotas.min_retention_days)))
    display_mode = db_settings.get("display_mode", config.quotas.display_mode)
    plank_mode = db_settings.get("plank_mode", config.quotas.plank_mode)
    plank_days = int(db_settings.get("plank_days", str(config.quotas.plank_days)))

    include_splits = promotion_mode == "split_the_loot"
    summary = db.get_quota_summary(user.id, include_splits=include_splits, plank_mode=plank_mode)
    if summary is None:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "user_id": user.id,
        "username": user.plex_username,
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
        "auto_scuttle_days": user.auto_scuttle_days,
        "min_retention_days": min_retention_days,
        "display_mode": display_mode,
        "promotion_mode": promotion_mode,
        "plank_days": plank_days,
        "plank_mode": plank_mode,
        "onboarded": user.onboarded,
    }


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


@router.get("/treasure/chest")
async def get_treasure_chest(request: Request) -> dict:
    """Get the current user's owned content list."""
    user = await _require_user(request)
    db = _get_db(request)
    items = db.get_user_owned_content(user.id)

    result_items = []
    for item in items:
        # Skip zero-size items (requested but not yet downloaded)
        if item.content.size_bytes <= 0:
            continue
        quality, quality_note = _derive_quality(item.content.size_bytes, item.content.media_type)
        result_items.append({
            "content_id": item.content.id,
            "title": item.content.title,
            "media_type": item.content.media_type,
            "size_bytes": item.content.size_bytes,
            "size_display": format_bytes(item.content.size_bytes),
            "quality": quality,
            "quality_note": quality_note,
            "status": item.ownership.status,
            "owned_at": item.ownership.owned_at,
            "promoted_at": item.ownership.promoted_at,
            "unique_viewers": item.unique_viewers,
            "can_scuttle": item.ownership.status == "owned",
        })

    return {"items": result_items}


@router.post("/treasure/{content_id}/scuttle")
async def scuttle(request: Request, content_id: int) -> dict:
    """Delete content owned by the current user."""
    user = await _require_user(request)
    db = _get_db(request)
    config = _get_config(request)

    result = await scuttle_content(db, config, content_id, user.id)

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
                "owner_user_id": item.ownership.owner_user_id,
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


@router.get("/plunder")
async def get_shared_plunder(request: Request) -> dict:
    """Get all promoted (shared plunder) content."""
    await _require_user(request)
    db = _get_db(request)
    items = db.get_promoted_content()

    return {
        "items": [
            {
                "content_id": item.id,
                "title": item.title,
                "media_type": item.media_type,
                "size_bytes": item.size_bytes,
                "size_display": format_bytes(item.size_bytes),
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


@router.post("/treasure/onboarded")
async def mark_onboarded(request: Request) -> dict:
    """Mark the current user as having completed onboarding."""
    user = await _require_user(request)
    db = _get_db(request)
    db.update_user_onboarded(user.id)
    return {"onboarded": True}
