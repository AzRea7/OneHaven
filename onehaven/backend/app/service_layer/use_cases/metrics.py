# onehaven/backend/app/services/metrics.py
from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...models import Lead, OutcomeEvent, OutcomeType, Property

# Stage order used for "reached stage" logic.
_STAGE: dict[OutcomeType, int] = {
    OutcomeType.contacted: 1,
    OutcomeType.responded: 2,
    OutcomeType.appointment_set: 3,
    OutcomeType.under_contract: 4,
    OutcomeType.closed: 5,
    OutcomeType.dead: 99,  # terminal loss (exclude from "success" but still terminal)
}

@dataclass(frozen=True)
class _Bucket:
    label: str
    lo: float
    hi: float

def _quantile_buckets(scores: list[float], k: int = 5) -> list[_Bucket]:
    """
    Returns k buckets based on score quantiles.
    Example labels: q1(0.051-0.063), q2(0.063-0.071), ...
    """
    scores = [float(s) for s in scores if s is not None]
    if not scores:
        return []

    scores_sorted = sorted(scores)
    n = len(scores_sorted)

    # If very small n, fall back to a single bucket.
    if n < k:
        lo = scores_sorted[0]
        hi = scores_sorted[-1]
        return [_Bucket(label=f"all({lo:.3f}-{hi:.3f})", lo=lo, hi=hi)]

    cuts: list[float] = []
    for i in range(1, k):
        idx = int(round(i * (n - 1) / k))
        cuts.append(scores_sorted[idx])

    edges = [scores_sorted[0]] + cuts + [scores_sorted[-1]]
    buckets: list[_Bucket] = []
    for i in range(k):
        lo = edges[i]
        hi = edges[i + 1]
        # Ensure monotonic; if duplicates occur, widen label still works.
        buckets.append(_Bucket(label=f"q{i+1}({lo:.3f}-{hi:.3f})", lo=lo, hi=hi))
    return buckets

def _assign_bucket(score: float, buckets: list[_Bucket]) -> str:
    # inclusive on the upper edge for last bucket
    for i, b in enumerate(buckets):
        if i == len(buckets) - 1:
            if score >= b.lo and score <= b.hi:
                return b.label
        if score >= b.lo and score < b.hi:
            return b.label
    # fallback
    return buckets[-1].label if buckets else "all"

def _max_stage(types: Iterable[OutcomeType]) -> int:
    m = 0
    for t in types:
        m = max(m, _STAGE.get(t, 0))
    return m

async def conversion_by_bucket(session: AsyncSession, zip: str, strategy: str) -> list[dict]:
    stmt = (
        select(Lead)
        .join(Property, Property.id == Lead.property_id)
        .where(Property.zipcode == zip)
        .where(Lead.strategy == strategy)
    )
    leads = (await session.execute(stmt)).scalars().all()
    if not leads:
        return []

    lead_ids = [l.id for l in leads]
    evs = (await session.execute(select(OutcomeEvent).where(OutcomeEvent.lead_id.in_(lead_ids)))).scalars().all()

    # events per lead
    types_by_lead: dict[int, list[OutcomeType]] = {}
    for e in evs:
        types_by_lead.setdefault(e.lead_id, []).append(e.outcome_type)

    # quantile buckets based on observed scores
    scores = [float(l.rank_score) for l in leads if l.rank_score is not None]
    buckets = _quantile_buckets(scores, k=5)

    # group leads by bucket
    leads_by_bucket: dict[str, list[Lead]] = {}
    for l in leads:
        b = _assign_bucket(float(l.rank_score or 0.0), buckets)
        leads_by_bucket.setdefault(b, []).append(l)

    out: list[dict] = []
    for b in sorted(leads_by_bucket.keys()):
        ls = leads_by_bucket[b]
        n = len(ls)

        # Funnel: treat as reached stage (closed implies earlier stages)
        reached_contacted = 0
        reached_responded = 0
        reached_appointment = 0
        reached_contract = 0
        reached_close = 0

        for l in ls:
            m = _max_stage(types_by_lead.get(l.id, []))
            if m >= _STAGE[OutcomeType.contacted]:
                reached_contacted += 1
            if m >= _STAGE[OutcomeType.responded]:
                reached_responded += 1
            if m >= _STAGE[OutcomeType.appointment_set]:
                reached_appointment += 1
            if m >= _STAGE[OutcomeType.under_contract]:
                reached_contract += 1
            if m == _STAGE[OutcomeType.closed]:
                reached_close += 1

        out.append(
            {
                "bucket": b,
                "count": n,
                "contacted_rate": reached_contacted / n if n else 0.0,
                "responded_rate": reached_responded / n if n else 0.0,
                "appointment_rate": reached_appointment / n if n else 0.0,
                "contract_rate": reached_contract / n if n else 0.0,
                "close_rate": reached_close / n if n else 0.0,
            }
        )

    return out

async def time_to_contact_by_bucket(session: AsyncSession, zip: str, strategy: str) -> list[dict]:
    stmt = (
        select(Lead)
        .join(Property, Property.id == Lead.property_id)
        .where(Property.zipcode == zip)
        .where(Lead.strategy == strategy)
    )
    leads = (await session.execute(stmt)).scalars().all()
    if not leads:
        return []

    lead_ids = [l.id for l in leads]
    evs = (await session.execute(select(OutcomeEvent).where(OutcomeEvent.lead_id.in_(lead_ids)))).scalars().all()

    # earliest contact time per lead
    contact_time: dict[int, datetime] = {}
    for e in evs:
        if e.outcome_type == OutcomeType.contacted:
            prev = contact_time.get(e.lead_id)
            if prev is None or e.occurred_at < prev:
                contact_time[e.lead_id] = e.occurred_at

    scores = [float(l.rank_score) for l in leads if l.rank_score is not None]
    buckets = _quantile_buckets(scores, k=5)

    mins_by_bucket: dict[str, list[float]] = {}
    for l in leads:
        ct = contact_time.get(l.id)
        if not ct:
            continue
        minutes = (ct - l.created_at).total_seconds() / 60.0
        b = _assign_bucket(float(l.rank_score or 0.0), buckets)
        mins_by_bucket.setdefault(b, []).append(minutes)

    out: list[dict] = []
    for b in sorted(mins_by_bucket.keys()):
        arr = mins_by_bucket[b]
        out.append(
            {
                "bucket": b,
                "median_minutes_to_contact": float(statistics.median(arr)) if arr else None,
                "count_with_contact": len(arr),
            }
        )
    return out

async def roi_vs_realized(session: AsyncSession, zip: str, strategy: str) -> dict:
    stmt = (
        select(OutcomeEvent)
        .join(Lead, Lead.id == OutcomeEvent.lead_id)
        .join(Property, Property.id == Lead.property_id)
        .where(Property.zipcode == zip)
        .where(Lead.strategy == strategy)
        .where(OutcomeEvent.outcome_type == OutcomeType.closed)
        .where(OutcomeEvent.realized_profit.isnot(None))
    )
    closed = (await session.execute(stmt)).scalars().all()
    profits = [e.realized_profit for e in closed if e.realized_profit is not None]
    if not profits:
        return {"count_closed_with_profit": 0, "avg_realized_profit": None, "median_realized_profit": None}

    avg = sum(profits) / len(profits)
    med = float(statistics.median(profits))
    return {"count_closed_with_profit": len(profits), "avg_realized_profit": float(avg), "median_realized_profit": med}
