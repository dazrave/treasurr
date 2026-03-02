"""User treasure chest and plunder endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from treasurr.api.auth import get_current_user
from treasurr.db import Database
from treasurr.engine.deletion import scuttle_content
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
    summary = db.get_quota_summary(user.id)
    if summary is None:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "user_id": user.id,
        "username": user.plex_username,
        "quota_bytes": summary.quota_bytes,
        "bonus_bytes": summary.bonus_bytes,
        "total_bytes": summary.total_bytes,
        "used_bytes": summary.used_bytes,
        "available_bytes": summary.available_bytes,
        "usage_percent": summary.usage_percent,
        "owned_count": summary.owned_count,
        "quota_display": format_bytes(summary.total_bytes),
        "used_display": format_bytes(summary.used_bytes),
        "available_display": format_bytes(summary.available_bytes),
    }


@router.get("/treasure/chest")
async def get_treasure_chest(request: Request) -> dict:
    """Get the current user's owned content list."""
    user = await _require_user(request)
    db = _get_db(request)
    items = db.get_user_owned_content(user.id)

    return {
        "items": [
            {
                "content_id": item.content.id,
                "title": item.content.title,
                "media_type": item.content.media_type,
                "size_bytes": item.content.size_bytes,
                "size_display": format_bytes(item.content.size_bytes),
                "status": item.ownership.status,
                "owned_at": item.ownership.owned_at,
                "promoted_at": item.ownership.promoted_at,
                "unique_viewers": item.unique_viewers,
                "can_scuttle": item.ownership.status == "owned",
            }
            for item in items
        ],
    }


@router.post("/treasure/{content_id}/scuttle")
async def scuttle(request: Request, content_id: int) -> dict:
    """Delete content owned by the current user."""
    user = await _require_user(request)
    db = _get_db(request)
    config = _get_config(request)

    result = await scuttle_content(db, config, content_id, user.id)

    if not result.success:
        raise HTTPException(status_code=400, detail=result.message)

    return {
        "success": True,
        "message": result.message,
        "freed_bytes": result.freed_bytes,
        "freed_display": format_bytes(result.freed_bytes),
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
