from __future__ import annotations
import statistics
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Lead, OutcomeEvent, OutcomeType, Property


def _bucket(score: float) -> str:
    # simple buckets: 0-0.2, 0.2-0.4, ... 0.8-1.0
    b = min(int(score * 5), 4)  # 0..4
    lo = b * 0.2
    hi = lo + 0.2
    return f"{lo:.1f}-{hi:.1f}"


async def conversion_by_bucket(session: AsyncSession, zip: str, strategy: str) -> list[dict]:
    # fetch leads in zip/strategy
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

    # index events per lead
    ev_by_lead: dict[int, set[OutcomeType]] = {}
    for e in evs:
        ev_by_lead.setdefault(e.lead_id, set()).add(e.outcome_type)

    buckets: dict[str, list[Lead]] = {}
    for l in leads:
        buckets.setdefault(_bucket(l.rank_score), []).append(l)

    out = []
    for b, ls in sorted(buckets.items()):
        n = len(ls)
        def rate(target: OutcomeType) -> float:
            hit = 0
            for l in ls:
                if target in ev_by_lead.get(l.id, set()):
                    hit += 1
            return hit / n if n else 0.0

        out.append(
            dict(
                bucket=b,
                count=n,
                responded_rate=rate(OutcomeType.responded),
                appointment_rate=rate(OutcomeType.appointment_set),
                contract_rate=rate(OutcomeType.under_contract),
                close_rate=rate(OutcomeType.closed),
            )
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

    contact_time: dict[int, datetime] = {}
    for e in evs:
        if e.outcome_type == OutcomeType.contacted:
            # earliest contact
            prev = contact_time.get(e.lead_id)
            if prev is None or e.occurred_at < prev:
                contact_time[e.lead_id] = e.occurred_at

    buckets: dict[str, list[float]] = {}
    for l in leads:
        ct = contact_time.get(l.id)
        if not ct:
            continue
        minutes = (ct - l.created_at).total_seconds() / 60.0
        buckets.setdefault(_bucket(l.rank_score), []).append(minutes)

    out = []
    for b, arr in sorted(buckets.items()):
        out.append(
            dict(
                bucket=b,
                median_minutes_to_contact=float(statistics.median(arr)) if arr else None,
                count_with_contact=len(arr),
            )
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
