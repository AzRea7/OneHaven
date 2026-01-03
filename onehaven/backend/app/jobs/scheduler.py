# app/jobs/scheduler.py
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select, func

from ..config import settings
from ..db import async_session
from ..models import Integration, OutboxEvent, OutboxStatus
from ..jobs.refresh import refresh_region
from ..integrations.jobs.dispatch import run_dispatch

log = logging.getLogger(__name__)


async def _run_refresh() -> None:
    async with async_session() as session:
        res = await refresh_region(
            session,
            region=settings.SCHED_REFRESH_REGION,
            zips=None,
            max_price=None,
            per_zip_limit=200,
        )
        await session.commit()
        log.info("refresh done: %s", res)


async def _run_dispatch_quiet() -> None:
    """
    Quiet-by-default posture:
    - If there are no enabled integrations, do nothing.
    - If there are no pending outbox events, do nothing.
    """
    async with async_session() as session:
        enabled_sinks = (
            await session.execute(select(func.count()).select_from(Integration).where(Integration.enabled == True))  # noqa: E712
        ).scalar_one()

        if int(enabled_sinks) == 0:
            return

        pending = (
            await session.execute(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.status == OutboxStatus.pending)
                .where(
                    (OutboxEvent.next_attempt_at.is_(None))
                    | (OutboxEvent.next_attempt_at <= datetime.utcnow())
                )
            )
        ).scalar_one()

        if int(pending) == 0:
            return

    async with async_session() as session:
        res = await run_dispatch(session=session, batch_size=settings.SCHED_DISPATCH_BATCH_SIZE)
        await session.commit()
        log.info("dispatch done: %s", res)


def build_scheduler() -> AsyncIOScheduler:
    sched = AsyncIOScheduler()

    # Refresh cadence (daily default in config)
    sched.add_job(
        lambda: asyncio.create_task(_run_refresh()),
        "interval",
        minutes=settings.SCHED_REFRESH_INTERVAL_MINUTES,
    )

    # Dispatch cadence
    sched.add_job(
        lambda: asyncio.create_task(_run_dispatch_quiet()),
        "interval",
        minutes=settings.SCHED_DISPATCH_INTERVAL_MINUTES,
    )

    return sched
