from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db import ensure_schema
from app.entrypoints.api.routers import debug, health, integrations, jobs, leads, metrics, outcomes


@asynccontextmanager
async def lifespan(_: FastAPI):
    # enforce schema invariant on startup
    ensure_schema()
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="OneHaven", lifespan=lifespan)

    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(leads.router)
    app.include_router(integrations.router)
    app.include_router(outcomes.router)
    app.include_router(metrics.router)
    app.include_router(debug.router)

    return app
