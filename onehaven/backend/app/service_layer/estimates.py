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
    # Backward compatible: value remains the “main” numeric (we treat it as p50)
    value: float | None
    source: str
    raw: Any | None = None

    # New: store percentiles in columns
    p10: float | None = None
    p50: float | None = None
    p90: float | None = None

    def normalized(self) -> "EstimateResult":
        """
        Ensure p50 matches value if either one is present.
        """
        p50 = self.p50 if self.p50 is not None else self.value
        return EstimateResult(
            value=p50,
            source=self.source,
            raw=self.raw,
            p10=self.p10,
            p50=p50,
            p90=self.p90,
        )


Fetcher = Callable[[Property], Awaitable[EstimateResult]]


@dataclass
class EstimateStats:
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


_GLOBAL = {"hits": 0, "misses": 0, "fetch_success": 0, "fetch_fail": 0}


def snapshot_global_stats() -> dict[str, int]:
    return dict(_GLOBAL)


def reset_global_stats() -> None:
    for k in list(_GLOBAL.keys()):
        _GLOBAL[k] = 0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_fresh(row: EstimateCache, ttl_days: int) -> bool:
    if not getattr(row, "estimated_at", None):
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
        result = (await fetcher(prop)).normalized()
        _GLOBAL["fetch_success"] += 1
        if stats:
            stats.fetch_success += 1
    except Exception:
        _GLOBAL["fetch_fail"] += 1
        if stats:
            stats.fetch_fail += 1
        result = EstimateResult(value=None, source="error", raw=None).normalized()

    now = _utcnow()

    # Compatibility: keep value aligned to p50
    p50 = result.p50
    p10 = result.p10
    p90 = result.p90

    if row:
        row.value = p50
        # New percentile columns (safe even if NULL)
        if hasattr(row, "p10"):
            row.p10 = p10
        if hasattr(row, "p50"):
            row.p50 = p50
        if hasattr(row, "p90"):
            row.p90 = p90

        row.source = result.source
        row.raw_json = None if result.raw is None else str(result.raw)[:20000]
        row.estimated_at = now
        await session.flush()
        return row

    row = EstimateCache(
        property_id=prop.id,
        kind=kind,
        value=p50,
        source=result.source,
        raw_json=None if result.raw is None else str(result.raw)[:20000],
        estimated_at=now,
        created_at=str(now),
        updated_at=str(now),
    )

    # New columns (if present in model/db)
    if hasattr(row, "p10"):
        row.p10 = p10
    if hasattr(row, "p50"):
        row.p50 = p50
    if hasattr(row, "p90"):
        row.p90 = p90

    session.add(row)
    await session.flush()
    return row
