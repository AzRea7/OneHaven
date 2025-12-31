# app/jobs/scheduler.py
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select, func

from ..db import async_session
from ..models import Integration, OutboxEvent, OutboxStatus
from ..jobs.refresh import run_refresh_job
from ..integrations.jobs.dispatch import dispatch_outbox_once

log = logging.getLogger(__name__)


async def _run_refresh() -> None:
    async with async_session() as session:
        # tweak defaults if you want
        await run_refresh_job(session=session, zips=None, max_price=None)
        await session.commit()


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
            return  # ✅ QUIET

        pending = (
            await session.execute(
                select(func.count())
                .select_from(OutboxEvent)
                .where(OutboxEvent.status == OutboxStatus.pending)
                .where(OutboxEvent.next_attempt_at <= datetime.utcnow())
            )
        ).scalar_one()

        if int(pending) == 0:
            return  # ✅ QUIET

    # do actual dispatch outside the count transaction
    async with async_session() as session:
        await dispatch_outbox_once(session=session)
        await session.commit()


def build_scheduler() -> AsyncIOScheduler:
    sched = AsyncIOScheduler()

    # refresh cadence (example: every 60 minutes)
    sched.add_job(lambda: asyncio.create_task(_run_refresh()), "interval", minutes=60)

    # dispatch cadence (every 5 minutes)
    sched.add_job(lambda: asyncio.create_task(_run_dispatch_quiet()), "interval", minutes=5)

    return sched
