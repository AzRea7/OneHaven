# backend/app/jobs/scheduler.py
from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from ..config import settings
from ..db import async_session_maker
from ..services.jobs import run_refresh_job, run_dispatch_job  

log = logging.getLogger(__name__)


async def _run_refresh() -> None:
    async with async_session_maker() as session:
        await run_refresh_job(session=session, region=settings.SCHED_REFRESH_REGION)
        await session.commit()


async def _run_dispatch() -> None:
    async with async_session_maker() as session:
        await run_dispatch_job(session=session, batch_size=settings.SCHED_DISPATCH_BATCH_SIZE)
        await session.commit()


def build_scheduler() -> AsyncIOScheduler:
    """
    Must be created/started within an asyncio event loop.
    """
    scheduler = AsyncIOScheduler(timezone=str(getattr(settings, "TZ", "America/Detroit")))

    scheduler.add_job(
        _run_refresh,
        trigger=IntervalTrigger(minutes=settings.SCHED_REFRESH_INTERVAL_MINUTES),
        id="refresh",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        _run_dispatch,
        trigger=IntervalTrigger(minutes=settings.SCHED_DISPATCH_INTERVAL_MINUTES),
        id="dispatch",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    return scheduler


async def run_forever() -> None:
    scheduler = build_scheduler()
    scheduler.start()
    log.info("Scheduler started")

    # Keep the loop alive
    stop = asyncio.Event()
    await stop.wait()
