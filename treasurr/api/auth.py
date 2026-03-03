"""Plex OAuth authentication."""

from __future__ import annotations

import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Callable

import httpx
from fastapi import APIRouter, Cookie, HTTPException, Request, Response

from treasurr.db import Database
from treasurr.models import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["auth"])

PLEX_AUTH_URL = "https://app.plex.tv/auth#"
PLEX_PINS_URL = "https://plex.tv/api/v2/pins"
PLEX_USER_URL = "https://plex.tv/api/v2/user"
PLEX_RESOURCES_URL = "https://plex.tv/api/v2/resources"

SESSION_DURATION_DAYS = 30


def _get_db(request: Request) -> Database:
    return request.app.state.db


def _get_config(request: Request) -> Any:
    return request.app.state.config


async def get_current_user(request: Request) -> User | None:
    """Extract the current user from session cookie."""
    token = request.cookies.get("treasurr_session")
    if not token:
        return None

    db = _get_db(request)
    session = db.get_session(token)
    if session is None:
        return None

    if session["expires_at"] < datetime.now(timezone.utc).isoformat():
        db.delete_session(token)
        return None

    return db.get_user(session["user_id"])


async def get_effective_user(request: Request) -> tuple[User | None, User | None]:
    """Return (effective_user, real_user). If admin is using view_as, effective_user is the target.

    Returns (effective_user, real_user) where real_user is the actual authenticated user.
    If not using view_as, both are the same.
    """
    real_user = await get_current_user(request)
    if real_user is None:
        return None, None

    view_as = request.query_params.get("view_as")
    if view_as and real_user.is_admin:
        db = _get_db(request)
        target = db.get_user(int(view_as))
        if target:
            return target, real_user

    return real_user, real_user


def require_auth(f: Callable) -> Callable:
    """Decorator that requires authentication."""
    @wraps(f)
    async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
        user = await get_current_user(request)
        if user is None:
            raise HTTPException(status_code=401, detail="Not authenticated")
        request.state.user = user
        return await f(request, *args, **kwargs)
    return wrapper


def require_admin(f: Callable) -> Callable:
    """Decorator that requires admin authentication."""
    @wraps(f)
    async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
        user = await get_current_user(request)
        if user is None:
            raise HTTPException(status_code=401, detail="Not authenticated")
        if not user.is_admin:
            raise HTTPException(status_code=403, detail="Admin access required")
        request.state.user = user
        return await f(request, *args, **kwargs)
    return wrapper


@router.get("/plex")
async def plex_auth_init(request: Request) -> dict:
    """Create a Plex PIN and return the auth URL for the frontend to open."""
    config = _get_config(request)
    client_id = config.plex_client_id or "treasurr-app"

    headers = {
        "Accept": "application/json",
        "X-Plex-Client-Identifier": client_id,
        "X-Plex-Product": "Treasurr",
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            PLEX_PINS_URL,
            headers=headers,
            data={"strong": "true"},
        )
        if resp.status_code != 201:
            raise HTTPException(status_code=502, detail="Failed to create Plex PIN")
        pin_data = resp.json()

    pin_id = pin_data["id"]
    pin_code = pin_data["code"]

    auth_url = (
        f"https://app.plex.tv/auth#?clientID={client_id}"
        f"&code={pin_code}"
        f"&context%5Bdevice%5D%5Bproduct%5D=Treasurr"
    )

    return {"auth_url": auth_url, "pin_id": pin_id}


@router.post("/plex/callback")
async def plex_auth_callback(request: Request, response: Response) -> dict:
    """Check if a Plex PIN has been claimed and create a session."""
    body = await request.json()
    pin_id = body.get("pin_id")
    if not pin_id:
        raise HTTPException(status_code=400, detail="pin_id required")

    config = _get_config(request)
    client_id = config.plex_client_id or "treasurr-app"
    db = _get_db(request)

    headers = {
        "Accept": "application/json",
        "X-Plex-Client-Identifier": client_id,
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(
            f"{PLEX_PINS_URL}/{pin_id}",
            headers=headers,
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail="Failed to check Plex PIN")
        pin_data = resp.json()

    auth_token = pin_data.get("authToken")
    if not auth_token:
        return {"authenticated": False, "message": "Waiting for Plex authorization..."}

    # Get user info from Plex
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(
            PLEX_USER_URL,
            headers={
                **headers,
                "X-Plex-Token": auth_token,
            },
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail="Failed to get Plex user info")
        plex_user = resp.json()

    plex_user_id = str(plex_user.get("id", ""))
    username = plex_user.get("username", plex_user.get("title", ""))
    email = plex_user.get("email", "")

    # Check if this user is the server owner (admin)
    is_admin = await _is_server_owner(auth_token, client_id)

    user = db.upsert_user(
        plex_user_id=plex_user_id,
        plex_username=username,
        email=email,
        quota_bytes=config.quotas.default_bytes,
        is_admin=is_admin,
    )

    # Create session
    session_token = secrets.token_urlsafe(48)
    expires_at = (datetime.now(timezone.utc) + timedelta(days=SESSION_DURATION_DAYS)).isoformat()
    db.create_session(session_token, user.id, auth_token, expires_at)

    response.set_cookie(
        key="treasurr_session",
        value=session_token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=SESSION_DURATION_DAYS * 86400,
    )

    return {
        "authenticated": True,
        "user": {
            "id": user.id,
            "username": user.plex_username,
            "is_admin": user.is_admin,
        },
    }


@router.get("/me")
async def get_me(request: Request) -> dict:
    """Get the current authenticated user."""
    user = await get_current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {
        "id": user.id,
        "username": user.plex_username,
        "email": user.email,
        "is_admin": user.is_admin,
        "onboarded": user.onboarded,
    }


@router.post("/logout")
async def logout(request: Request, response: Response) -> dict:
    """Destroy the current session."""
    token = request.cookies.get("treasurr_session")
    if token:
        db = _get_db(request)
        db.delete_session(token)
    response.delete_cookie("treasurr_session")
    return {"message": "Logged out"}


async def _is_server_owner(plex_token: str, client_id: str) -> bool:
    """Check if the authenticated user owns any Plex servers."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                PLEX_RESOURCES_URL,
                headers={
                    "Accept": "application/json",
                    "X-Plex-Client-Identifier": client_id,
                    "X-Plex-Token": plex_token,
                },
            )
            if resp.status_code != 200:
                return False
            resources = resp.json()
            for r in resources:
                if r.get("provides") == "server" and r.get("owned"):
                    return True
    except Exception as e:
        logger.warning("Failed to check server ownership: %s", e)
    return False
