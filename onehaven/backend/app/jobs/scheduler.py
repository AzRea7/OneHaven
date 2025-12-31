import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ..config import settings
from ..db import async_session
from .refresh import refresh_region
from .dispatch import run_dispatch

log = logging.getLogger("haven.scheduler")


async def _run_refresh() -> None:
    async with async_session() as session:
        res = await refresh_region(session, settings.SCHED_REFRESH_REGION)
        await session.commit()
        log.info("refresh_done", extra={"result": res})


async def _run_dispatch() -> None:
    async with async_session() as session:
        res = await run_dispatch(session, batch_size=50)
        await session.commit()
        log.info("dispatch_done", extra={"result": res})


def build_scheduler() -> AsyncIOScheduler:
    sched = AsyncIOScheduler()

    sched.add_job(
        lambda: asyncio.create_task(_run_refresh()),
        "interval",
        minutes=settings.SCHED_REFRESH_INTERVAL_MINUTES,
        id="refresh",
        replace_existing=True,
    )

    sched.add_job(
        lambda: asyncio.create_task(_run_dispatch()),
        "interval",
        minutes=settings.SCHED_DISPATCH_INTERVAL_MINUTES,
        id="dispatch",
        replace_existing=True,
    )

    return sched
