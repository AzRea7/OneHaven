# app/services/jobruns.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ..models import JobRun, JobRunStatus


async def start_job(session: AsyncSession, job_name: str, meta: dict[str, Any] | None = None) -> JobRun:
    jr = JobRun(
        job_name=job_name,
        started_at=datetime.utcnow(),
        status=JobRunStatus.running,
        meta_json=json.dumps(meta or {}),
    )
    session.add(jr)
    await session.flush()
    return jr


async def finish_job_success(session: AsyncSession, jr: JobRun, summary: dict[str, Any]) -> None:
    jr.status = JobRunStatus.success
    jr.finished_at = datetime.utcnow()
    jr.summary_json = json.dumps(summary)
    jr.error = None
    await session.flush()


async def finish_job_fail(session: AsyncSession, jr: JobRun, err: Exception) -> None:
    jr.status = JobRunStatus.failed
    jr.finished_at = datetime.utcnow()
    jr.error = str(err)
    await session.flush()
