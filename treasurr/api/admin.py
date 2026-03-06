"""Admin endpoints for crew management and system stats."""

from __future__ import annotations

import hashlib
import json
import secrets
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request, UploadFile

from treasurr.api.auth import get_current_user
from treasurr.email import send_email
from treasurr.engine.deletion import _execute_deletion, scuttle_content
from treasurr.engine.quota import format_bytes
from treasurr.sync.clients import OverseerrClient, RadarrClient, SonarrClient
from treasurr.sync.scheduler import run_full_sync
from treasurr.sync.tag_sync import _build_tag_user_map

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


@router.post("/api-keys")
async def create_api_key(request: Request) -> dict:
    """Generate a new API key. Returns the plaintext key once."""
    await _require_admin(request)
    db = _get_db(request)
    body = await request.json()

    name = str(body.get("name", "")).strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name is required")
    if len(name) > 100:
        raise HTTPException(status_code=400, detail="Name must be under 100 characters")

    plaintext_key = secrets.token_urlsafe(48)
    key_hash = hashlib.sha256(plaintext_key.encode()).hexdigest()
    record = db.create_api_key(name, key_hash)

    return {
        "id": record["id"],
        "name": record["name"],
        "key": plaintext_key,
        "created_at": record["created_at"],
        "warning": "Save this key now. It cannot be retrieved again.",
    }


@router.get("/api-keys")
async def list_api_keys(request: Request) -> dict:
    """List all API keys (hash never exposed)."""
    await _require_admin(request)
    db = _get_db(request)
    keys = db.list_api_keys()
    return {"keys": keys}


@router.delete("/api-keys/{key_id}")
async def revoke_api_key(request: Request, key_id: int) -> dict:
    """Revoke an API key."""
    await _require_admin(request)
    db = _get_db(request)
    deleted = db.revoke_api_key(key_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="API key not found")
    return {"message": "API key revoked"}


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
        "instance_name": db_settings.get("instance_name", "TREASURR"),
        "instance_tagline": db_settings.get("instance_tagline", "Your treasure. Your crew. Your plunder."),
        "custom_css": db_settings.get("custom_css", ""),
        "logo_filename": db_settings.get("logo_filename", ""),
        "promotion_mode": db_settings.get("promotion_mode", config.quotas.promotion_mode),
        "shared_plunder_max_bytes": int(db_settings.get("shared_plunder_max_bytes", str(config.quotas.shared_plunder_max_bytes))),
        "min_retention_days": int(db_settings.get("min_retention_days", str(config.quotas.min_retention_days))),
        "display_mode": db_settings.get("display_mode", config.quotas.display_mode),
        "plank_mode": db_settings.get("plank_mode", config.quotas.plank_mode),
        "plank_days": int(db_settings.get("plank_days", str(config.quotas.plank_days))),
        "plank_rescue_action": db_settings.get("plank_rescue_action", config.quotas.plank_rescue_action),
        "server_message": db_settings.get("server_message", DEFAULT_SERVER_MESSAGE),
        "stale_content_days": int(db_settings.get("stale_content_days", "0")),
        "smtp_enabled": db_settings.get("smtp_enabled", "false") == "true",
        "smtp_host": db_settings.get("smtp_host", ""),
        "smtp_port": int(db_settings.get("smtp_port", "587")),
        "smtp_from": db_settings.get("smtp_from", ""),
        "smtp_username": db_settings.get("smtp_username", ""),
        "smtp_password_set": bool(db_settings.get("smtp_password", "")),
        "webhook_secret_set": bool(db_settings.get("webhook_secret", "")),
        "tag_ownership_enabled": db_settings.get("tag_ownership_enabled", "true") == "true",
    }


@router.put("/settings")
async def update_settings(request: Request) -> dict:
    """Update settings with validation."""
    await _require_admin(request)
    db = _get_db(request)
    body = await request.json()

    # Branding settings
    if "instance_name" in body:
        name = str(body["instance_name"]).strip()
        if len(name) > 50:
            raise HTTPException(status_code=400, detail="Instance name must be under 50 characters")
        db.set_setting("instance_name", name)

    if "instance_tagline" in body:
        tagline = str(body["instance_tagline"]).strip()
        if len(tagline) > 100:
            raise HTTPException(status_code=400, detail="Tagline must be under 100 characters")
        db.set_setting("instance_tagline", tagline)

    if "custom_css" in body:
        css = str(body["custom_css"])
        if len(css) > 10000:
            raise HTTPException(status_code=400, detail="Custom CSS must be under 10000 characters")
        db.set_setting("custom_css", css)

    valid_promotion_modes = ("full_plunder", "split_the_loot", "disabled")
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

    valid_rescue_actions = ("promote", "adopt", "disabled")
    if "plank_rescue_action" in body:
        if body["plank_rescue_action"] not in valid_rescue_actions:
            raise HTTPException(status_code=400, detail=f"Invalid plank_rescue_action. Must be one of: {valid_rescue_actions}")
        db.set_setting("plank_rescue_action", body["plank_rescue_action"])

    if "server_message" in body:
        msg = str(body["server_message"]).strip()
        if len(msg) > 1000:
            raise HTTPException(status_code=400, detail="Server message must be under 1000 characters")
        db.set_setting("server_message", msg)

    if "stale_content_days" in body:
        val = int(body["stale_content_days"])
        if val < 0:
            raise HTTPException(status_code=400, detail="stale_content_days cannot be negative")
        db.set_setting("stale_content_days", str(val))

    # SMTP settings
    if "smtp_enabled" in body:
        db.set_setting("smtp_enabled", "true" if body["smtp_enabled"] else "false")
    if "smtp_host" in body:
        db.set_setting("smtp_host", str(body["smtp_host"]).strip())
    if "smtp_port" in body:
        db.set_setting("smtp_port", str(int(body["smtp_port"])))
    if "smtp_from" in body:
        db.set_setting("smtp_from", str(body["smtp_from"]).strip())
    if "smtp_username" in body:
        db.set_setting("smtp_username", str(body["smtp_username"]).strip())
    if "smtp_password" in body:
        pw = str(body["smtp_password"]).strip()
        if pw:  # Only update if non-empty (don't clear on form reload)
            db.set_setting("smtp_password", pw)
    if "webhook_secret" in body:
        secret = str(body["webhook_secret"]).strip()
        if secret:
            db.set_setting("webhook_secret", secret)

    if "tag_ownership_enabled" in body:
        db.set_setting("tag_ownership_enabled", "true" if body["tag_ownership_enabled"] else "false")

    # Return updated settings
    return await get_settings(request)


@router.post("/settings/test-email")
async def test_email(request: Request) -> dict:
    """Send a test email to the admin's email address."""
    admin = await _require_admin(request)
    db = _get_db(request)

    if not admin.email:
        raise HTTPException(status_code=400, detail="No email address on your account")

    subject = "Treasurr Test Email"
    html = """<html><body style="background:#0e1117;color:#e6edf3;padding:32px;font-family:sans-serif;">
    <h1 style="color:#c9a84c;">Treasurr</h1>
    <p>If you're reading this, email notifications are working correctly.</p>
    <p style="color:#8b949e;">Your treasure. Your crew. Your plunder.</p>
    </body></html>"""
    text = "Treasurr Test Email\n\nIf you're reading this, email notifications are working correctly."

    success = await send_email(db, admin.email, subject, html, text)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to send test email. Check SMTP settings.")

    return {"message": f"Test email sent to {admin.email}"}


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
                "email": user.email,
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


@router.get("/tag-status")
async def get_tag_status(request: Request) -> dict:
    """Check tag ownership status across Overseerr and arr services."""
    await _require_admin(request)
    db = _get_db(request)
    config = _get_config(request)

    all_users = db.get_all_users()
    users_by_username: dict[str, int] = {
        u.plex_username.lower(): u.id for u in all_users
    }
    username_by_id: dict[int, str] = {u.id: u.plex_username for u in all_users}

    result: dict = {
        "overseerr_sonarr_tag_requests": False,
        "overseerr_radarr_tag_requests": False,
        "sonarr_tags": [],
        "radarr_tags": [],
        "unmatched_tags": [],
        "tagged_content_count": 0,
        "untagged_content_count": 0,
        "pending_changes": [],
    }

    # Check Overseerr settings
    try:
        overseerr = OverseerrClient(config.overseerr)
        for server in await overseerr.get_service_settings("sonarr"):
            if server.get("tagRequests"):
                result["overseerr_sonarr_tag_requests"] = True
        for server in await overseerr.get_service_settings("radarr"):
            if server.get("tagRequests"):
                result["overseerr_radarr_tag_requests"] = True
    except Exception as e:
        result["overseerr_error"] = str(e)

    all_content = db.get_all_active_content()
    tagged_ids: set[int] = set()

    # Check Sonarr tags
    try:
        sonarr = SonarrClient(config.sonarr)
        sonarr_tags = await sonarr.get_tags()
        sonarr_tag_map = _build_tag_user_map(sonarr_tags, users_by_username)

        for tag in sonarr_tags:
            label = tag.get("label", "")
            tag_id = tag.get("id")
            if " - " not in label:
                continue
            username = label.split(" - ", 1)[1].strip().lower()
            if tag_id in sonarr_tag_map:
                matched_user = username_by_id.get(sonarr_tag_map[tag_id], username)
                result["sonarr_tags"].append({"label": label, "matched_user": matched_user})
            else:
                result["unmatched_tags"].append({"label": label, "service": "sonarr"})

        if sonarr_tag_map:
            all_series = await sonarr.get_all_series()
            for series in all_series:
                user_tag_ids = [t for t in series.tags if t in sonarr_tag_map]
                if not user_tag_ids:
                    continue
                content = db.get_content_by_arr_id(sonarr_id=series.id)
                if content is None:
                    continue
                tagged_ids.add(content.id)
                tag_user_id = sonarr_tag_map[user_tag_ids[0]]
                ownership = db.get_ownership(content.id)
                if ownership and ownership.owner_user_id != tag_user_id and ownership.status == "owned":
                    result["pending_changes"].append({
                        "title": content.title,
                        "media_type": content.media_type,
                        "current_owner": username_by_id.get(ownership.owner_user_id, "Unknown"),
                        "tag_owner": username_by_id.get(tag_user_id, "Unknown"),
                    })
    except Exception as e:
        result["sonarr_error"] = str(e)

    # Check Radarr tags
    try:
        radarr = RadarrClient(config.radarr)
        radarr_tags = await radarr.get_tags()
        radarr_tag_map = _build_tag_user_map(radarr_tags, users_by_username)

        for tag in radarr_tags:
            label = tag.get("label", "")
            tag_id = tag.get("id")
            if " - " not in label:
                continue
            username = label.split(" - ", 1)[1].strip().lower()
            if tag_id in radarr_tag_map:
                matched_user = username_by_id.get(radarr_tag_map[tag_id], username)
                result["radarr_tags"].append({"label": label, "matched_user": matched_user})
            else:
                result["unmatched_tags"].append({"label": label, "service": "radarr"})

        if radarr_tag_map:
            all_movies = await radarr.get_all_movies()
            for movie in all_movies:
                user_tag_ids = [t for t in movie.tags if t in radarr_tag_map]
                if not user_tag_ids:
                    continue
                content = db.get_content_by_arr_id(radarr_id=movie.id)
                if content is None:
                    continue
                tagged_ids.add(content.id)
                tag_user_id = radarr_tag_map[user_tag_ids[0]]
                ownership = db.get_ownership(content.id)
                if ownership and ownership.owner_user_id != tag_user_id and ownership.status == "owned":
                    result["pending_changes"].append({
                        "title": content.title,
                        "media_type": content.media_type,
                        "current_owner": username_by_id.get(ownership.owner_user_id, "Unknown"),
                        "tag_owner": username_by_id.get(tag_user_id, "Unknown"),
                    })
    except Exception as e:
        result["radarr_error"] = str(e)

    result["tagged_content_count"] = len(tagged_ids)
    result["untagged_content_count"] = len(all_content) - len(tagged_ids)

    return result


@router.post("/tag-status/enable")
async def enable_tag_requests(request: Request) -> dict:
    """Enable tagRequests on Overseerr and activate tag ownership sync."""
    await _require_admin(request)
    db = _get_db(request)
    config = _get_config(request)

    overseerr = OverseerrClient(config.overseerr)
    enabled_services = []

    try:
        for server in await overseerr.get_service_settings("sonarr"):
            if not server.get("tagRequests"):
                await overseerr.enable_tag_requests("sonarr", server.get("id", 0))
                enabled_services.append("sonarr")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to enable Sonarr tags: {e}")

    try:
        for server in await overseerr.get_service_settings("radarr"):
            if not server.get("tagRequests"):
                await overseerr.enable_tag_requests("radarr", server.get("id", 0))
                enabled_services.append("radarr")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to enable Radarr tags: {e}")

    db.set_setting("tag_ownership_enabled", "true")

    return {
        "success": True,
        "enabled_services": enabled_services,
        "message": "Tag requests enabled. Ownership will sync from arr tags on the next cycle.",
    }


BRANDING_DIR = Path("/app/data/branding")
ALLOWED_LOGO_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg", ".webp", ".ico"}
MAX_LOGO_SIZE = 512 * 1024  # 512 KB


@router.post("/branding/logo")
async def upload_logo(request: Request, file: UploadFile) -> dict:
    """Upload a logo image for branding."""
    await _require_admin(request)
    db = _get_db(request)

    if file.filename is None:
        raise HTTPException(status_code=400, detail="No filename provided")

    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_LOGO_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type. Allowed: {', '.join(sorted(ALLOWED_LOGO_EXTENSIONS))}",
        )

    content = await file.read()
    if len(content) > MAX_LOGO_SIZE:
        raise HTTPException(status_code=400, detail="File too large. Maximum size is 512 KB.")

    BRANDING_DIR.mkdir(parents=True, exist_ok=True)

    # Remove any existing logo files
    for existing in BRANDING_DIR.glob("logo.*"):
        existing.unlink()

    filename = f"logo{ext}"
    (BRANDING_DIR / filename).write_bytes(content)
    db.set_setting("logo_filename", filename)

    return {"filename": filename}


@router.delete("/branding/logo")
async def delete_logo(request: Request) -> dict:
    """Remove the uploaded logo."""
    await _require_admin(request)
    db = _get_db(request)

    logo_filename = db.get_setting("logo_filename", "")
    if logo_filename:
        logo_path = BRANDING_DIR / logo_filename
        if logo_path.exists():
            logo_path.unlink()
    db.set_setting("logo_filename", "")

    return {"message": "Logo removed"}
