# app/services/estimates.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import EstimateCache, EstimateKind, Property


@dataclass(frozen=True)
class EstimateResult:
    value: float | None
    source: str
    raw: dict[str, Any] | None = None


def _utcnow() -> datetime:
    return datetime.utcnow()


async def get_cached_estimate(
    session: AsyncSession,
    *,
    property_id: int,
    kind: EstimateKind,
) -> EstimateCache | None:
    row = (
        await session.execute(
            select(EstimateCache)
            .where(EstimateCache.property_id == property_id)
            .where(EstimateCache.kind == kind)
        )
    ).scalars().first()
    return row


async def set_cached_estimate(
    session: AsyncSession,
    *,
    property_id: int,
    kind: EstimateKind,
    value: float | None,
    source: str,
    ttl_days: int,
    raw: dict[str, Any] | None,
) -> EstimateCache:
    now = _utcnow()
    exp = now + timedelta(days=ttl_days)

    row = await get_cached_estimate(session, property_id=property_id, kind=kind)
    raw_json = json.dumps(raw)[:50000] if raw is not None else None

    if row:
        row.value = value
        row.source = source
        row.fetched_at = now
        row.expires_at = exp
        row.raw_json = raw_json
        await session.flush()
        return row

    row = EstimateCache(
        property_id=property_id,
        kind=kind,
        value=value,
        source=source,
        fetched_at=now,
        expires_at=exp,
        raw_json=raw_json,
    )
    session.add(row)
    await session.flush()
    return row


async def get_or_fetch_estimate(
    session: AsyncSession,
    *,
    prop: Property,
    kind: EstimateKind,
    ttl_days: int,
    fetcher: Callable[[Property], Awaitable[EstimateResult]],
    force_refresh: bool = False,
) -> EstimateResult:
    """
    Central enrichment contract:
    - property anchored
    - TTL cached
    - fetcher is an adapter call (RentCast today, MLS tomorrow, your ML model later)
    """
    cached = await get_cached_estimate(session, property_id=prop.id, kind=kind)
    now = _utcnow()

    if not force_refresh and cached and cached.expires_at > now:
        return EstimateResult(value=cached.value, source=cached.source, raw=None)

    res = await fetcher(prop)

    await set_cached_estimate(
        session,
        property_id=prop.id,
        kind=kind,
        value=res.value,
        source=res.source,
        ttl_days=ttl_days,
        raw=res.raw,
    )
    return res
