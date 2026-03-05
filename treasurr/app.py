"""FastAPI application factory."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse

from treasurr.api.admin import router as admin_router
from treasurr.api.auth import router as auth_router
from treasurr.api.treasure import router as treasure_router
from treasurr.api.webhook import router as webhook_router
from treasurr.config import Config
from treasurr.db import Database
from treasurr.sync.scheduler import start_scheduler

logger = logging.getLogger(__name__)

FRONTEND_DIR = Path(__file__).parent / "frontend"


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

    @app.get("/", response_class=HTMLResponse)
    async def index() -> FileResponse:
        return FileResponse(FRONTEND_DIR / "index.html")

    @app.get("/admin", response_class=HTMLResponse)
    async def admin_page() -> FileResponse:
        return FileResponse(FRONTEND_DIR / "admin.html")

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "version": "0.1.0"}

    return app
