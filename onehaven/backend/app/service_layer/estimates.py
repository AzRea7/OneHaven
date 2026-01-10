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
    """
    What fetchers return. We then persist this into EstimateCache.

    Backward compatible:
      - `value` is the main numeric (treat it as p50)
    Forward compatible:
      - optional p10/p50/p90 for uncertainty bands
    """
    value: float | None
    source: str
    raw: Any | None = None

    p10: float | None = None
    p50: float | None = None
    p90: float | None = None

    def normalized(self) -> "EstimateResult":
        """
        Ensure p50 matches `value` if one is present.
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
_GLOBAL = {"hits": 0, "misses": 0, "fetch_success": 0, "fetch_fail": 0}


def snapshot_global_stats() -> dict[str, int]:
    return dict(_GLOBAL)


def reset_global_stats() -> None:
    for k in list(_GLOBAL.keys()):
        _GLOBAL[k] = 0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_aware_utc(dt: datetime) -> datetime:
    """
    SQLite + SQLAlchemy sometimes gives back a naive datetime even if you *meant* UTC.
    If naive, assume it's UTC and attach tzinfo so comparisons don’t explode.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_fresh(row: EstimateCache, ttl_days: int) -> bool:
    est_at = getattr(row, "estimated_at", None)
    if not est_at:
        return False
    est_at = _ensure_aware_utc(est_at)
    return est_at >= (_utcnow() - timedelta(days=ttl_days))


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
        res = fetcher(prop)
        if hasattr(res, "__await__"):
            res = await res  # type: ignore[misc]
        result = res.normalized() if hasattr(res, "normalized") else res  # safety
        _GLOBAL["fetch_success"] += 1
        if stats:
            stats.fetch_success += 1
    except Exception as e:
        _GLOBAL["fetch_fail"] += 1
        if stats:
            stats.fetch_fail += 1
        # still write a cache row with None so we don’t hammer endpoints
        result = EstimateResult(value=None, source=f"error:{type(e).__name__}", raw=None).normalized()

    now = _utcnow()

    # Persist (update or insert)
    if row:
        # Core value
        row.value = result.value
        row.source = result.source

        # Optional percentiles if your EstimateCache has these columns.
        # If your model doesn’t have them, these setattr calls are harmless (AttributeError => ignore).
        try:
            row.p10 = result.p10
            row.p50 = result.p50
            row.p90 = result.p90
        except Exception:
            pass

        # Raw payload (bounded)
        raw = getattr(result, "raw", None)
        row.raw_json = None if raw is None else str(raw)[:20000]

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
    )
    # Optional percentiles on insert too
    try:
        row.p10 = result.p10
        row.p50 = result.p50
        row.p90 = result.p90
    except Exception:
        pass

    session.add(row)
    await session.flush()
    return row
