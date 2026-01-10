# app/entrypoints/api/routers/debug.py
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..deps import require_api_key
from ....config import settings
from ....db import get_session
from ....models import JobRun
from ....service_layer.estimates import snapshot_global_stats, reset_global_stats

router = APIRouter(tags=["debug"])


@router.get("/debug/estimates/stats", dependencies=[Depends(require_api_key)])
def debug_estimates_stats(reset: bool = Query(default=False)) -> dict[str, Any]:
    if reset:
        reset_global_stats()
    return {"estimate_cache": snapshot_global_stats()}


@router.get("/debug/config", dependencies=[Depends(require_api_key)])
def debug_config() -> dict[str, Any]:
    """
    IMPORTANT: This reads the *running server's* settings, not your shell's.
    Safe to expose because we redact secrets.
    """
    def _redact(v: str | None) -> str | None:
        if not v:
            return v
        if len(v) <= 8:
            return "***"
        return v[:4] + "***" + v[-4:]

    return {
        "INGESTION_SOURCE": settings.INGESTION_SOURCE,
        "HAVEN_DB_URL": settings.HAVEN_DB_URL,
        "RESO_BASE_URL": settings.RESO_BASE_URL,
        "REALCOMP_RESO_BASE_URL": settings.REALCOMP_RESO_BASE_URL,
        "REALCOMP_TOKEN_URL": settings.REALCOMP_TOKEN_URL,
        "REALCOMP_CLIENT_ID": _redact(settings.REALCOMP_CLIENT_ID),
        "REALCOMP_CLIENT_SECRET": "***" if settings.REALCOMP_CLIENT_SECRET else None,
        "API_KEY_SET": bool(settings.API_KEY),
    }


@router.get("/debug/job_runs/latest", dependencies=[Depends(require_api_key)])
async def debug_job_runs_latest(limit: int = Query(default=5, ge=1, le=50), session: AsyncSession = Depends(get_session)) -> dict[str, Any]:
    rows = (
        (await session.execute(select(JobRun).order_by(JobRun.id.desc()).limit(int(limit))))
        .scalars()
        .all()
    )

    def _row(r: JobRun) -> dict[str, Any]:
        return {
            "id": r.id,
            "job_name": r.job_name,
            "status": r.status,
            "started_at": str(r.started_at),
            "finished_at": str(r.finished_at) if r.finished_at else None,
            "error": (r.error or "")[:1200],
            "detail": (r.detail or "")[:1200],
        }

    return {"items": [_row(r) for r in rows]}
