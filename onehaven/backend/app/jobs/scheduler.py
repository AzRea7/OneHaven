import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ..config import settings
from ..db import async_session
from ..services.jobruns import start_job, finish_job_success, finish_job_fail
from .refresh import refresh_region
from ..integrations.jobs.dispatch import run_dispatch

log = logging.getLogger("onehaven.scheduler")


async def _run_refresh() -> None:
    async with async_session() as session:
        jr = await start_job(session, "refresh")
        try:
            result = await refresh_region(session, settings.SCHED_REFRESH_REGION)
            await finish_job_success(session, jr, result)
            await session.commit()
            log.info("refresh_done", extra={"result": result})
        except Exception as e:
            await finish_job_fail(session, jr, e)
            await session.commit()
            log.exception("refresh_failed")
            # Do not re-raise, scheduler should keep running


async def _run_dispatch() -> None:
    async with async_session() as session:
        jr = await start_job(session, "dispatch")
        try:
            result = await run_dispatch(session=session, batch_size=50)
            await finish_job_success(session, jr, result)
            await session.commit()
            log.info("dispatch_done", extra={"result": result})
        except Exception as e:
            await finish_job_fail(session, jr, e)
            await session.commit()
            log.exception("dispatch_failed")
            # Do not re-raise


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
