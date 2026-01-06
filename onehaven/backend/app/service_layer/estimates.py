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


@dataclass
class EstimateStats:
    """
    Per-run counters (returned from refresh response)
    """
    hits: int = 0
    misses: int = 0
    fetch_success: int = 0
    fetch_fail: int = 0

    def snapshot(self) -> dict[str, int]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "fetch_success": self.fetch_success,
            "fetch_fail": self.fetch_fail,
        }


# Global counters for debug endpoint (process-lifetime, not persisted)
_GLOBAL = {
    "hits": 0,
    "misses": 0,
    "fetch_success": 0,
    "fetch_fail": 0,
}


def snapshot_global_stats() -> dict[str, int]:
    return dict(_GLOBAL)


def reset_global_stats() -> None:
    for k in list(_GLOBAL.keys()):
        _GLOBAL[k] = 0


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
    stats: EstimateStats | None = None,
) -> EstimateCache:
    """
    Return an EstimateCache row (always).
    If cached and fresh => return cached row.
    Else call fetcher(prop), write/update cache row, return it.

    We cache even "None" values to avoid repeated calls for bad inputs.
    """
    q = select(EstimateCache).where(
        EstimateCache.property_id == prop.id,
        EstimateCache.kind == kind,
    )
    row = (await session.execute(q)).scalars().first()

    if row and _is_fresh(row, ttl_days):
        _GLOBAL["hits"] += 1
        if stats:
            stats.hits += 1
        return row

    _GLOBAL["misses"] += 1
    if stats:
        stats.misses += 1

    try:
        result = await fetcher(prop)
        _GLOBAL["fetch_success"] += 1
        if stats:
            stats.fetch_success += 1
    except Exception:
        _GLOBAL["fetch_fail"] += 1
        if stats:
            stats.fetch_fail += 1
        # still write a cache row with None so we don’t hammer vendor endpoints
        result = EstimateResult(value=None, source="error", raw=None)

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
