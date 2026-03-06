"""External API v1 - authenticated via API keys for programmatic access."""

from __future__ import annotations

import hashlib

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from treasurr.engine.quota import format_bytes

router = APIRouter(prefix="/api/v1", tags=["external"])


def _get_db(request: Request):
    return request.app.state.db


def _get_config(request: Request):
    return request.app.state.config


async def _require_api_key(request: Request) -> dict:
    """Validate Bearer token against stored API key hashes."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or invalid Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = auth_header[7:]
    key_hash = hashlib.sha256(token.encode()).hexdigest()
    db = _get_db(request)
    api_key = db.get_api_key_by_hash(key_hash)

    if api_key is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )

    db.touch_api_key(api_key["id"])
    return api_key


def _resolve_tier_name(quota_bytes: int, config) -> str | None:
    """Match quota bytes to a configured tier name."""
    for tier in config.quotas.tiers:
        if tier.bytes == quota_bytes:
            return tier.name
    return None


def _format_content(content, db, config) -> dict:
    """Format a content item for API response."""
    result = {
        "id": content.id,
        "title": content.title,
        "media_type": content.media_type,
        "tmdb_id": content.tmdb_id,
        "size_bytes": content.size_bytes,
        "size_display": format_bytes(content.size_bytes),
        "status": content.status,
        "added_at": content.added_at,
        "poster_url": f"https://image.tmdb.org/t/p/w500{content.poster_path}" if content.poster_path else None,
    }

    if content.media_type == "show":
        seasons = db.get_seasons(content.id)
        result["seasons"] = [
            {
                "season_number": s.season_number,
                "episode_count": s.episode_count,
                "size_bytes": s.size_bytes,
                "size_display": format_bytes(s.size_bytes),
            }
            for s in seasons
        ]

    return result


def _format_user(user, db, config) -> dict:
    """Format a user for API response."""
    summary = db.get_quota_summary(user.id)
    return {
        "id": user.id,
        "username": user.plex_username,
        "quota_bytes": user.quota_bytes,
        "bonus_bytes": user.bonus_bytes,
        "used_bytes": summary.used_bytes if summary else 0,
        "available_bytes": summary.available_bytes if summary else user.quota_bytes + user.bonus_bytes,
        "usage_percent": summary.usage_percent if summary else 0.0,
        "owned_count": summary.owned_count if summary else 0,
        "tier": _resolve_tier_name(user.quota_bytes, config),
        "is_admin": user.is_admin,
        "created_at": user.created_at,
    }


# --- Read Endpoints ---


@router.get("/users")
async def list_users(request: Request) -> dict:
    """List all users with quota information."""
    await _require_api_key(request)
    db = _get_db(request)
    config = _get_config(request)
    users = db.get_all_users()
    items = [_format_user(u, db, config) for u in users]
    return {"items": items, "total": len(items)}


@router.get("/users/{user_id}")
async def get_user(request: Request, user_id: int) -> dict:
    """Get a single user with quota details."""
    await _require_api_key(request)
    db = _get_db(request)
    config = _get_config(request)
    user = db.get_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return _format_user(user, db, config)


@router.get("/users/{user_id}/content")
async def get_user_content(request: Request, user_id: int) -> dict:
    """Get content owned by a user."""
    await _require_api_key(request)
    db = _get_db(request)
    config = _get_config(request)
    user = db.get_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    owned = db.get_user_owned_content(user_id)
    items = []
    for oc in owned:
        item = _format_content(oc.content, db, config)
        item["ownership_status"] = oc.ownership.status
        item["owned_at"] = oc.ownership.owned_at
        items.append(item)
    return {"items": items, "total": len(items)}


@router.get("/content/leaving")
async def get_leaving_content(request: Request) -> dict:
    """Get content currently on the plank (pending deletion)."""
    await _require_api_key(request)
    db = _get_db(request)
    config = _get_config(request)
    plank_items = db.get_plank_content()
    items = []
    for oc in plank_items:
        item = _format_content(oc.content, db, config)
        owner = db.get_user(oc.ownership.owner_user_id)
        item["owner"] = owner.plex_username if owner else None
        item["plank_started_at"] = oc.ownership.plank_started_at
        items.append(item)
    return {"items": items, "total": len(items)}


@router.get("/content/latest")
async def get_latest_content(request: Request) -> dict:
    """Get recently added content."""
    await _require_api_key(request)
    db = _get_db(request)
    config = _get_config(request)

    limit = min(int(request.query_params.get("limit", "20")), 100)
    content_list = db.get_latest_content(limit)
    items = [_format_content(c, db, config) for c in content_list]
    return {"items": items, "total": len(items)}


@router.get("/content/shared")
async def get_shared_content(request: Request) -> dict:
    """Get promoted/shared plunder content."""
    await _require_api_key(request)
    db = _get_db(request)
    config = _get_config(request)
    promoted = db.get_promoted_content()
    items = [_format_content(c, db, config) for c in promoted]
    return {"items": items, "total": len(items)}


@router.get("/content/{content_id}")
async def get_content(request: Request, content_id: int) -> dict:
    """Get a single content item with ownership info."""
    await _require_api_key(request)
    db = _get_db(request)
    config = _get_config(request)
    content = db.get_content(content_id)
    if content is None:
        raise HTTPException(status_code=404, detail="Content not found")

    item = _format_content(content, db, config)
    ownership = db.get_ownership(content_id)
    if ownership:
        owner = db.get_user(ownership.owner_user_id)
        item["owner"] = {
            "user_id": ownership.owner_user_id,
            "username": owner.plex_username if owner else None,
            "status": ownership.status,
            "owned_at": ownership.owned_at,
        }
    else:
        item["owner"] = None
    return item


@router.get("/content")
async def list_content(request: Request) -> dict:
    """List all active content, optionally filtered by media_type."""
    await _require_api_key(request)
    db = _get_db(request)
    config = _get_config(request)
    all_content = db.get_all_active_content()

    media_type = request.query_params.get("media_type")
    if media_type:
        all_content = [c for c in all_content if c.media_type == media_type]

    items = []
    for c in all_content:
        item = _format_content(c, db, config)
        ownership = db.get_ownership(c.id)
        if ownership:
            owner = db.get_user(ownership.owner_user_id)
            item["owner"] = owner.plex_username if owner else None
        else:
            item["owner"] = None
        items.append(item)
    return {"items": items, "total": len(items)}


@router.get("/tiers")
async def get_tiers(request: Request) -> dict:
    """Get available quota tiers."""
    await _require_api_key(request)
    config = _get_config(request)
    return {
        "tiers": [
            {"name": t.name, "bytes": t.bytes, "display": format_bytes(t.bytes)}
            for t in config.quotas.tiers
        ],
    }


@router.get("/stats")
async def get_stats(request: Request) -> dict:
    """Get global storage statistics."""
    await _require_api_key(request)
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


# --- Write Endpoints ---


@router.put("/users/{user_id}/tier")
async def set_user_tier(request: Request, user_id: int) -> dict:
    """Set a user's quota to a named tier."""
    await _require_api_key(request)
    db = _get_db(request)
    config = _get_config(request)
    body = await request.json()

    tier_name = body.get("tier")
    if not tier_name:
        raise HTTPException(status_code=400, detail="tier is required")

    tier_match = None
    for tier in config.quotas.tiers:
        if tier.name == tier_name:
            tier_match = tier
            break

    if tier_match is None:
        valid = [t.name for t in config.quotas.tiers]
        raise HTTPException(status_code=400, detail=f"Invalid tier. Valid tiers: {valid}")

    user = db.update_user_quota(user_id, quota_bytes=tier_match.bytes)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    db.add_quota_transaction(user_id, tier_match.bytes, "api_tier_change")
    return _format_user(user, db, config)


@router.put("/users/{user_id}/quota")
async def set_user_quota(request: Request, user_id: int) -> dict:
    """Set a user's quota_bytes and/or bonus_bytes."""
    await _require_api_key(request)
    db = _get_db(request)
    config = _get_config(request)
    body = await request.json()

    quota_bytes = body.get("quota_bytes")
    bonus_bytes = body.get("bonus_bytes")

    if quota_bytes is None and bonus_bytes is None:
        raise HTTPException(status_code=400, detail="Provide quota_bytes or bonus_bytes")

    if quota_bytes is not None and quota_bytes < 0:
        raise HTTPException(status_code=400, detail="quota_bytes must be non-negative")
    if bonus_bytes is not None and bonus_bytes < 0:
        raise HTTPException(status_code=400, detail="bonus_bytes must be non-negative")

    user = db.update_user_quota(user_id, quota_bytes=quota_bytes, bonus_bytes=bonus_bytes)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    if quota_bytes is not None:
        db.add_quota_transaction(user_id, quota_bytes, "api_quota_change")
    if bonus_bytes is not None:
        db.add_quota_transaction(user_id, bonus_bytes, "api_quota_change")

    return _format_user(user, db, config)


@router.delete("/users/{user_id}/quota")
async def reset_user_quota(request: Request, user_id: int) -> dict:
    """Reset a user's quota to the default and clear bonus."""
    await _require_api_key(request)
    db = _get_db(request)
    config = _get_config(request)

    default_bytes = config.quotas.default_bytes
    user = db.update_user_quota(user_id, quota_bytes=default_bytes, bonus_bytes=0)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    db.add_quota_transaction(user_id, default_bytes, "api_quota_reset")
    return _format_user(user, db, config)
