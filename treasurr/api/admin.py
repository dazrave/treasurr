"""Admin endpoints for crew management and system stats."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from treasurr.api.auth import get_current_user
from treasurr.engine.quota import format_bytes
from treasurr.sync.scheduler import run_full_sync

router = APIRouter(prefix="/api/admin", tags=["admin"])


def _get_db(request: Request):
    return request.app.state.db


def _get_config(request: Request):
    return request.app.state.config


async def _require_admin(request: Request):
    user = await get_current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


@router.get("/crew")
async def get_crew(request: Request) -> dict:
    """Get all users with their quota summaries."""
    await _require_admin(request)
    db = _get_db(request)
    users = db.get_all_users()

    crew = []
    for user in users:
        summary = db.get_quota_summary(user.id)
        crew.append({
            "id": user.id,
            "username": user.plex_username,
            "email": user.email,
            "is_admin": user.is_admin,
            "quota_bytes": user.quota_bytes,
            "bonus_bytes": user.bonus_bytes,
            "used_bytes": summary.used_bytes if summary else 0,
            "used_display": format_bytes(summary.used_bytes) if summary else "0 B",
            "usage_percent": summary.usage_percent if summary else 0,
            "owned_count": summary.owned_count if summary else 0,
            "created_at": user.created_at,
        })

    return {"crew": crew}


@router.put("/crew/{user_id}")
async def update_crew_member(request: Request, user_id: int) -> dict:
    """Update a crew member's quota or bonus."""
    await _require_admin(request)
    db = _get_db(request)
    body = await request.json()

    quota_bytes = body.get("quota_bytes")
    bonus_bytes = body.get("bonus_bytes")

    if quota_bytes is None and bonus_bytes is None:
        raise HTTPException(status_code=400, detail="Provide quota_bytes or bonus_bytes")

    user = db.update_user_quota(user_id, quota_bytes=quota_bytes, bonus_bytes=bonus_bytes)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    if quota_bytes is not None:
        db.add_quota_transaction(user_id, quota_bytes, "admin_grant")
    if bonus_bytes is not None:
        db.add_quota_transaction(user_id, bonus_bytes, "admin_bonus")

    return {
        "id": user.id,
        "username": user.plex_username,
        "quota_bytes": user.quota_bytes,
        "bonus_bytes": user.bonus_bytes,
    }


@router.post("/sync")
async def trigger_sync(request: Request) -> dict:
    """Manually trigger a full sync."""
    await _require_admin(request)
    db = _get_db(request)
    config = _get_config(request)

    results = await run_full_sync(db, config)
    return {"message": "Sync complete", "results": results}


@router.get("/stats")
async def get_stats(request: Request) -> dict:
    """Get global storage statistics."""
    await _require_admin(request)
    db = _get_db(request)
    stats = db.get_global_stats()

    return {
        "total_bytes": stats["total_bytes"],
        "total_display": format_bytes(stats["total_bytes"]),
        "owned_bytes": stats["owned_bytes"],
        "owned_display": format_bytes(stats["owned_bytes"]),
        "promoted_bytes": stats["promoted_bytes"],
        "promoted_display": format_bytes(stats["promoted_bytes"]),
        "unowned_bytes": stats["unowned_bytes"],
        "unowned_display": format_bytes(stats["unowned_bytes"]),
        "user_count": stats["user_count"],
        "content_count": stats["content_count"],
    }
