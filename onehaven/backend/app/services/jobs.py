# app/services/jobs.py
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from ..jobs.refresh import refresh_region
from ..integrations.services.outbox import dispatch_pending_events


async def run_refresh_job(session: AsyncSession, region: str, max_price: float | None = None) -> dict:
    """
    Scheduler-safe wrapper around refresh_region().
    Returns a small dict summary.
    """
    return await refresh_region(session, region=region, max_price=max_price)


async def run_dispatch_job(session: AsyncSession, batch_size: int = 50) -> dict:
    """
    Scheduler-safe wrapper around outbox dispatch.
    """
    return await dispatch_pending_events(session=session, batch_size=batch_size)
