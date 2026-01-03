# app/entrypoints/api/routers/leads.py
from __future__ import annotations

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from ....db import get_session
from ....models import Lead, Property, LeadSource
from ....schemas import LeadOut

router = APIRouter(tags=["leads"])


@router.get("/leads/top", response_model=list[LeadOut])
async def top_leads(
    zip: str = Query(..., min_length=5, max_length=10),
    strategy: str = Query("rental"),
    limit: int = Query(25, ge=1, le=200),
    max_price: float | None = Query(default=None, ge=0),
    source: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> list[LeadOut]:
    stmt = (
        select(Lead, Property)
        .join(Property, Property.id == Lead.property_id)
        .where(Property.zipcode == zip)
        .where(Lead.strategy == strategy)
        .order_by(desc(Lead.rank_score))
        .limit(limit)
    )

    if max_price is not None:
        stmt = stmt.where(Lead.list_price.isnot(None)).where(Lead.list_price <= max_price)

    if source is not None:
        try:
            stmt = stmt.where(Lead.source == LeadSource(source))
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid source: {source}")

    rows = (await session.execute(stmt)).all()

    out: list[LeadOut] = []
    for lead, prop in rows:
        out.append(
            LeadOut(
                id=lead.id,
                property_id=lead.property_id,
                source=lead.source.value,
                status=lead.status.value,
                strategy=lead.strategy.value,
                rank_score=lead.rank_score,
                deal_score=lead.deal_score,
                motivation_score=lead.motivation_score,
                explain=lead.explain_json or "",
                address_line=prop.address_line,
                city=prop.city,
                state=prop.state,
                zipcode=prop.zipcode,
                list_price=lead.list_price,
                arv_estimate=lead.arv_estimate,
                rent_estimate=lead.rent_estimate,
                rehab_estimate=lead.rehab_estimate,
                created_at=lead.created_at,
            )
        )
    return out
