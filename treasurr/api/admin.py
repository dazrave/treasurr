"""Admin endpoints for crew management and system stats."""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request

from treasurr.api.auth import get_current_user
from treasurr.engine.deletion import _execute_deletion, scuttle_content
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


@router.get("/tiers")
async def get_tiers(request: Request) -> dict:
    """Return configured quota tiers."""
    await _require_admin(request)
    config = _get_config(request)
    return {
        "tiers": [
            {"name": t.name, "bytes": t.bytes, "display": format_bytes(t.bytes)}
            for t in config.quotas.tiers
        ]
    }


@router.get("/crew")
async def get_crew(request: Request) -> dict:
    """Get all users with their quota summaries and activity data."""
    await _require_admin(request)
    db = _get_db(request)
    users = db.get_all_users()
    activity = db.get_user_activity()

    crew = []
    for user in users:
        summary = db.get_quota_summary(user.id)
        user_activity = activity.get(user.id, {})
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
            "last_request_at": user_activity.get("last_request_at"),
            "request_count": user_activity.get("request_count", 0),
        })

    return {"crew": crew}


@router.put("/crew/bulk")
async def bulk_update_crew(request: Request) -> dict:
    """Bulk update quota for multiple crew members."""
    await _require_admin(request)
    db = _get_db(request)
    body = await request.json()

    user_ids = body.get("user_ids", [])
    quota_bytes = body.get("quota_bytes")

    if not user_ids:
        raise HTTPException(status_code=400, detail="Provide at least one user_id")
    if quota_bytes is None or quota_bytes < 0:
        raise HTTPException(status_code=400, detail="Provide a valid quota_bytes value")

    updated = db.bulk_update_quota(user_ids, quota_bytes)
    return {"updated": updated, "quota_bytes": quota_bytes}


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


@router.get("/settings")
async def get_settings(request: Request) -> dict:
    """Return current settings with defaults from config."""
    await _require_admin(request)
    db = _get_db(request)
    config = _get_config(request)

    db_settings = db.get_all_settings()

    from treasurr.api.treasure import DEFAULT_SERVER_MESSAGE

    return {
        "promotion_mode": db_settings.get("promotion_mode", config.quotas.promotion_mode),
        "shared_plunder_max_bytes": int(db_settings.get("shared_plunder_max_bytes", str(config.quotas.shared_plunder_max_bytes))),
        "min_retention_days": int(db_settings.get("min_retention_days", str(config.quotas.min_retention_days))),
        "display_mode": db_settings.get("display_mode", config.quotas.display_mode),
        "plank_mode": db_settings.get("plank_mode", config.quotas.plank_mode),
        "plank_days": int(db_settings.get("plank_days", str(config.quotas.plank_days))),
        "plank_rescue_action": db_settings.get("plank_rescue_action", config.quotas.plank_rescue_action),
        "server_message": db_settings.get("server_message", DEFAULT_SERVER_MESSAGE),
    }


@router.put("/settings")
async def update_settings(request: Request) -> dict:
    """Update settings with validation."""
    await _require_admin(request)
    db = _get_db(request)
    body = await request.json()

    valid_promotion_modes = ("full_plunder", "split_the_loot")
    valid_display_modes = ("exact", "round_up", "percentage")

    if "promotion_mode" in body:
        if body["promotion_mode"] not in valid_promotion_modes:
            raise HTTPException(status_code=400, detail=f"Invalid promotion_mode. Must be one of: {valid_promotion_modes}")
        db.set_setting("promotion_mode", body["promotion_mode"])

    if "shared_plunder_max_bytes" in body:
        val = int(body["shared_plunder_max_bytes"])
        if val < 0:
            raise HTTPException(status_code=400, detail="shared_plunder_max_bytes cannot be negative")
        db.set_setting("shared_plunder_max_bytes", str(val))

    if "min_retention_days" in body:
        val = int(body["min_retention_days"])
        if val < 0:
            raise HTTPException(status_code=400, detail="min_retention_days cannot be negative")
        db.set_setting("min_retention_days", str(val))

    if "display_mode" in body:
        if body["display_mode"] not in valid_display_modes:
            raise HTTPException(status_code=400, detail=f"Invalid display_mode. Must be one of: {valid_display_modes}")
        db.set_setting("display_mode", body["display_mode"])

    valid_plank_modes = ("anchored", "adrift", "disabled")
    if "plank_mode" in body:
        if body["plank_mode"] not in valid_plank_modes:
            raise HTTPException(status_code=400, detail=f"Invalid plank_mode. Must be one of: {valid_plank_modes}")
        db.set_setting("plank_mode", body["plank_mode"])

    if "plank_days" in body:
        val = int(body["plank_days"])
        if val < 0:
            raise HTTPException(status_code=400, detail="plank_days cannot be negative")
        db.set_setting("plank_days", str(val))

    valid_rescue_actions = ("promote", "adopt")
    if "plank_rescue_action" in body:
        if body["plank_rescue_action"] not in valid_rescue_actions:
            raise HTTPException(status_code=400, detail=f"Invalid plank_rescue_action. Must be one of: {valid_rescue_actions}")
        db.set_setting("plank_rescue_action", body["plank_rescue_action"])

    if "server_message" in body:
        msg = str(body["server_message"]).strip()
        if len(msg) > 1000:
            raise HTTPException(status_code=400, detail="Server message must be under 1000 characters")
        db.set_setting("server_message", msg)

    # Return updated settings
    return await get_settings(request)


@router.get("/stats")
async def get_stats(request: Request) -> dict:
    """Get global storage statistics."""
    await _require_admin(request)
    db = _get_db(request)
    config = _get_config(request)
    stats = db.get_global_stats()

    db_settings = db.get_all_settings()
    shared_plunder_max_bytes = int(db_settings.get(
        "shared_plunder_max_bytes", str(config.quotas.shared_plunder_max_bytes),
    ))
    display_mode = db_settings.get("display_mode", config.quotas.display_mode)

    promoted_bytes = stats["promoted_bytes"]
    if shared_plunder_max_bytes > 0:
        plunder_cap_percent = round((promoted_bytes / shared_plunder_max_bytes) * 100, 1)
    else:
        plunder_cap_percent = 0.0
    plunder_cap_warning = plunder_cap_percent > 90.0

    # Disk space from Radarr (synced periodically)
    disk_raw = db.get_setting("disk_space", "{}")
    try:
        disk_space = json.loads(disk_raw)
    except (json.JSONDecodeError, TypeError):
        disk_space = {}

    # Per-user breakdown for storage bar
    users = db.get_all_users()
    activity = db.get_user_activity()
    user_storage = []
    for user in users:
        summary = db.get_quota_summary(user.id)
        used = summary.used_bytes if summary else 0
        if used > 0:
            user_storage.append({
                "user_id": user.id,
                "username": user.plex_username,
                "used_bytes": used,
                "used_display": format_bytes(used),
                "quota_bytes": user.quota_bytes,
            })

    return {
        "total_bytes": stats["total_bytes"],
        "total_display": format_bytes(stats["total_bytes"]),
        "owned_bytes": stats["owned_bytes"],
        "owned_display": format_bytes(stats["owned_bytes"]),
        "promoted_bytes": promoted_bytes,
        "promoted_display": format_bytes(promoted_bytes),
        "unowned_bytes": stats["unowned_bytes"],
        "unowned_display": format_bytes(stats["unowned_bytes"]),
        "plank_bytes": stats["plank_bytes"],
        "plank_display": format_bytes(stats["plank_bytes"]),
        "plank_count": stats["plank_count"],
        "user_count": stats["user_count"],
        "content_count": stats["content_count"],
        "shared_plunder_max_bytes": shared_plunder_max_bytes,
        "shared_plunder_max_display": format_bytes(shared_plunder_max_bytes),
        "plunder_cap_percent": plunder_cap_percent,
        "plunder_cap_warning": plunder_cap_warning,
        "display_mode": display_mode,
        "disk_total_bytes": disk_space.get("total_bytes", 0),
        "disk_total_display": format_bytes(disk_space.get("total_bytes", 0)),
        "disk_free_bytes": disk_space.get("free_bytes", 0),
        "disk_free_display": format_bytes(disk_space.get("free_bytes", 0)),
        "user_storage": user_storage,
    }


@router.get("/activity")
async def get_admin_activity(request: Request) -> dict:
    """Get admin activity feed for the Ship's Log."""
    await _require_admin(request)
    db = _get_db(request)

    limit = int(request.query_params.get("limit", "50"))
    events = db.get_admin_activity_feed(limit=min(limit, 200))

    return {
        "events": [
            {
                "type": ev["type"],
                "at": ev["at"],
                "actor": ev["actor"],
                "title": ev["title"],
                "media_type": ev["media_type"],
                "owner_username": ev["owner_username"],
                "size_bytes": ev["size_bytes"],
                "size_display": format_bytes(ev["size_bytes"]) if ev["size_bytes"] else None,
                "viewers": ev["viewers"],
            }
            for ev in events
        ],
    }


@router.post("/scuttle/{content_id}")
async def admin_force_scuttle(request: Request, content_id: int) -> dict:
    """Admin force-scuttle: delete any content, bypassing ownership check."""
    admin = await _require_admin(request)
    db = _get_db(request)
    config = _get_config(request)

    content = db.get_content(content_id)
    if content is None:
        raise HTTPException(status_code=404, detail="Content not found")

    if content.status != "active":
        raise HTTPException(status_code=400, detail="Content is not active")

    force = request.query_params.get("force", "false").lower() == "true"

    if force:
        # Skip plank, delete immediately
        ownership = db.get_ownership(content_id)
        user_id = ownership.owner_user_id if ownership else admin.id
        result = await _execute_deletion(db, config, content_id, user_id, content)
    else:
        # Use normal scuttle flow (respects plank)
        ownership = db.get_ownership(content_id)
        if ownership is None:
            raise HTTPException(status_code=400, detail="Content has no owner")
        result = await scuttle_content(db, config, content_id, ownership.owner_user_id)

    if not result.success:
        raise HTTPException(status_code=400, detail=result.message)

    return {
        "success": True,
        "message": result.message,
        "freed_bytes": result.freed_bytes,
        "freed_display": format_bytes(result.freed_bytes),
        "walked_plank": result.walked_plank,
    }
