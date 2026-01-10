# app/entrypoints/api/routers/health.py
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request

from ..deps import require_api_key
from ....config import settings

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/debug/config", dependencies=[Depends(require_api_key)])
def debug_config() -> dict[str, Any]:
    def _redact(v: str | None) -> str | None:
        if not v:
            return v
        if len(v) <= 8:
            return "***"
        return v[:4] + "***" + v[-4:]

    return {
        "ENV": settings.ENV,
        "INGESTION_SOURCE": settings.INGESTION_SOURCE,
        "HAVEN_DB_URL": settings.HAVEN_DB_URL,
        "RESO_BASE_URL": settings.RESO_BASE_URL,
        "REALCOMP_RESO_BASE_URL": settings.REALCOMP_RESO_BASE_URL,
        "REALCOMP_TOKEN_URL": settings.REALCOMP_TOKEN_URL,
        "REALCOMP_CLIENT_ID": _redact(settings.REALCOMP_CLIENT_ID),
        "REALCOMP_CLIENT_SECRET_SET": bool(settings.REALCOMP_CLIENT_SECRET),
    }


@router.get("/debug/routes", dependencies=[Depends(require_api_key)])
def debug_routes(request: Request) -> dict[str, Any]:
    """
    Shows what this running server has actually mounted.
    If /debug/config is missing, you’re editing the wrong files or wrong process.
    """
    routes: list[str] = []
    for r in request.app.routes:
        methods = getattr(r, "methods", None)
        path = getattr(r, "path", None)
        if path:
            if methods:
                routes.append(f"{sorted(list(methods))} {path}")
            else:
                routes.append(path)
    return {"count": len(routes), "routes": sorted(routes)}
