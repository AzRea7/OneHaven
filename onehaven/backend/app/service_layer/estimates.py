# app/service_layer/estimates.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import EstimateCache, EstimateKind, Property


@dataclass(frozen=True)
class EstimateResult:
    value: float | None
    source: str
    raw: Any | None = None


Fetcher = Callable[[Property], Awaitable[EstimateResult]]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_fresh(row: EstimateCache, ttl_days: int) -> bool:
    if not row.estimated_at:
        return False
    return row.estimated_at >= (_utcnow() - timedelta(days=ttl_days))


async def get_or_fetch_estimate(
    session: AsyncSession,
    *,
    prop: Property,
    kind: EstimateKind,
    ttl_days: int,
    fetcher: Fetcher,
) -> EstimateCache:
    """
    Return an EstimateCache row (always).
    If cached and fresh => return cached row.
    Else call fetcher(prop), write/update cache row, return it.

    NOTE: We cache even "None" values to avoid repeated calls.
    """
    q = select(EstimateCache).where(
        EstimateCache.property_id == prop.id,
        EstimateCache.kind == kind,
    )
    row = (await session.execute(q)).scalars().first()

    if row and _is_fresh(row, ttl_days):
        return row

    result = await fetcher(prop)

    now = _utcnow()
    if row:
        row.value = result.value
        row.source = result.source
        row.raw_json = None if result.raw is None else str(result.raw)[:20000]
        row.estimated_at = now
        await session.flush()
        return row

    row = EstimateCache(
        property_id=prop.id,
        kind=kind,
        value=result.value,
        source=result.source,
        raw_json=None if result.raw is None else str(result.raw)[:20000],
        estimated_at=now,
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    await session.flush()
    return row
