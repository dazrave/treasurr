"""FastAPI application factory."""

from __future__ import annotations

import asyncio
import html
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse

from treasurr.api.admin import BRANDING_DIR, router as admin_router
from treasurr.api.auth import router as auth_router
from treasurr.api.external import router as external_router
from treasurr.api.treasure import router as treasure_router
from treasurr.api.webhook import router as webhook_router
from treasurr.config import Config
from treasurr.db import Database
from treasurr.sync.scheduler import start_scheduler

logger = logging.getLogger(__name__)

FRONTEND_DIR = Path(__file__).parent / "frontend"

# Cache template file contents (read once at first request)
_template_cache: dict[str, str] = {}


def _read_template(name: str) -> str:
    """Read and cache an HTML template file."""
    if name not in _template_cache:
        _template_cache[name] = (FRONTEND_DIR / name).read_text()
    return _template_cache[name]


def _render_template(template: str, db: Database) -> str:
    """Replace branding placeholders in an HTML template."""
    settings = db.get_all_settings()

    instance_name = settings.get("instance_name", "TREASURR")
    instance_tagline = settings.get(
        "instance_tagline", "Your treasure. Your crew. Your plunder.",
    )
    custom_css = settings.get("custom_css", "")
    logo_filename = settings.get("logo_filename", "")

    if logo_filename:
        logo_html = (
            '<img src="/branding/' + html.escape(logo_filename)
            + '" alt="Logo" style="height:28px; width:auto;">'
        )
    else:
        logo_html = "&#9875;"

    css_block = custom_css if custom_css else ""

    result = template.replace("{{INSTANCE_NAME}}", html.escape(instance_name))
    result = result.replace("{{INSTANCE_TAGLINE}}", html.escape(instance_tagline))
    result = result.replace("{{LOGO_HTML}}", logo_html)
    result = result.replace("{{CUSTOM_CSS}}", css_block)

    return result


def create_app(config: Config) -> FastAPI:
    """Create and configure the FastAPI application."""
    db = Database(config.db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        logger.info("Treasurr starting up...")
        task = asyncio.create_task(start_scheduler(db, config))
        yield
        task.cancel()

    app = FastAPI(
        title="Treasurr",
        description="Your treasure. Your crew. Your plunder.",
        version="0.2.0",
        lifespan=lifespan,
    )

    app.state.db = db
    app.state.config = config

    # Register API routers
    app.include_router(auth_router)
    app.include_router(treasure_router)
    app.include_router(admin_router)
    app.include_router(webhook_router)
    app.include_router(external_router)

    @app.get("/branding/{filename}")
    async def branding_file(filename: str) -> FileResponse:
        filepath = BRANDING_DIR / filename
        if not filepath.exists() or not filepath.is_file():
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="File not found")
        return FileResponse(filepath)

    @app.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        template = _read_template("index.html")
        return HTMLResponse(_render_template(template, db))

    @app.get("/admin", response_class=HTMLResponse)
    async def admin_page() -> HTMLResponse:
        template = _read_template("admin.html")
        return HTMLResponse(_render_template(template, db))

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "version": "0.1.0"}

    return app
