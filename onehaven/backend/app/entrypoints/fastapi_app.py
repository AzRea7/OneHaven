# app/entrypoints/fastapi_app.py
from __future__ import annotations

from fastapi import FastAPI

from ..db import engine
from ..models import Base
from .api.routers import health, jobs, leads, integrations, outcomes, metrics, debug


def create_app() -> FastAPI:
    app = FastAPI(title="OneHaven - Lead Truth Engine")

    @app.on_event("startup")
    async def _startup() -> None:
        # Single place where DB tables are created in dev.
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    # Routers
    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(leads.router)
    app.include_router(integrations.router)
    app.include_router(outcomes.router)
    app.include_router(metrics.router)
    app.include_router(debug.router)
    
    return app
