# app/entrypoints/api/routers/jobs.py
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ..deps import require_api_key
from ....db import get_session
from ....services.jobruns import start_job, finish_job_success, finish_job_fail
from ....schemas import DispatchResult
from ....integrations.jobs.dispatch import run_dispatch
from ....service_layer.use_cases.refresh import refresh_region_use_case

router = APIRouter(tags=["jobs"])


@router.post("/jobs/refresh", dependencies=[Depends(require_api_key)])
async def jobs_refresh(
    region: str | None = Query(None),
    zips: str | None = Query(None, description="Comma-separated zips"),
    city: str | None = Query(None),
    max_price: float | None = Query(None),
    per_zip_limit: int = Query(200, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    zip_list: list[str] = []
    if zips:
        zip_list = [z.strip() for z in zips.split(",") if z.strip()]

    jr = await start_job(session, "refresh_api")
    try:
        res = await refresh_region_use_case(
            session=session,
            region=region,
            zips=zip_list or None,
            city=city,
            max_price=max_price,
            per_zip_limit=per_zip_limit,
        )
        await finish_job_success(session, jr, res)
        await session.commit()
        return res
    except Exception as e:
        await finish_job_fail(session, jr, e)
        await session.commit()
        raise


@router.post("/jobs/dispatch", response_model=DispatchResult, dependencies=[Depends(require_api_key)])
async def dispatch_outbox(
    batch_size: int = Query(50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> DispatchResult:
    jr = await start_job(session, "dispatch_api")
    try:
        result = await run_dispatch(session=session, batch_size=batch_size)
        await finish_job_success(session, jr, result)
        await session.commit()
        return DispatchResult(**result)
    except Exception as e:
        await finish_job_fail(session, jr, e)
        await session.commit()
        raise
