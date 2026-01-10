from __future__ import annotations

from fastapi import Header, HTTPException, status

from ...config import settings
from ...db import get_session  # re-exported for routers that import from deps.py


async def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    if not x_api_key or x_api_key != settings.API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid_api_key")
