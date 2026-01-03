# app/entrypoints/api/deps.py
from __future__ import annotations

from fastapi import Header, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ...config import settings
from ...db import get_session


def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    if settings.API_KEY:
        if not x_api_key or x_api_key != settings.API_KEY:
            raise HTTPException(status_code=401, detail="Invalid API key")


def session_dep() -> AsyncSession:
    # FastAPI dependency wrapper for typing/consistency
    return Depends(get_session)  # type: ignore
