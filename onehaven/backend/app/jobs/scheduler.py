# onehaven/backend/app/jobs/scheduler.py
from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ..config import settings
from ..db import async_session_maker
from ..services.jobruns import start_job, finish_job_fail, finish_job_success
from .refresh import refresh_region
from ..integrations.services.outbox import dispatch_pending_events

log = logging.getLogger(__name__)


async def _run_refresh() -> None:
    async with async_session_maker() as session:
        jr = await start_job(session, "refresh_sched")

        try:
            region = getattr(settings, "SCHED_REFRESH_REGION", None)
            max_price = getattr(settings, "SCHED_REFRESH_MAX_PRICE", None)

            result = await refresh_region(session, region=region, max_price=max_price)
            await session.commit()

            await finish_job_success(session, jr, result)
            await session.commit()

        except Exception as e:
            await session.rollback()
            await finish_job_fail(session, jr, e)
            await session.commit()
            raise


async def _run_dispatch() -> None:
    async with async_session_maker() as session:
        jr = await start_job(session, "dispatch_sched")

        try:
            result = await dispatch_pending_events(session)
            await session.commit()

            await finish_job_success(session, jr, result)
            await session.commit()

        except Exception as e:
            await session.rollback()
            await finish_job_fail(session, jr, e)
            await session.commit()
            raise


def build_scheduler() -> AsyncIOScheduler:
    sched = AsyncIOScheduler(timezone="America/Detroit")

    refresh_minutes = int(getattr(settings, "SCHED_REFRESH_INTERVAL_MINUTES", 1440))
    dispatch_minutes = int(getattr(settings, "SCHED_DISPATCH_INTERVAL_MINUTES", 5))

    sched.add_job(_run_refresh, "interval", minutes=refresh_minutes, id="_run_refresh", replace_existing=True)
    sched.add_job(_run_dispatch, "interval", minutes=dispatch_minutes, id="_run_dispatch", replace_existing=True)

    return sched


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    sched = build_scheduler()
    sched.start()
    log.info("Scheduler started")

    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
