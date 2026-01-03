# app/jobs/refresh.py
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession
from ..service_layer.use_cases.refresh import refresh_region_use_case


async def refresh_region(*args, **kwargs):
    # Backwards-compatible import path for scripts/tests
    return await refresh_region_use_case(*args, **kwargs)
