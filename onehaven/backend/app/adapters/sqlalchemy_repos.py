# app/adapters/sqlalchemy_repos.py
from __future__ import annotations

from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Property, Lead, EstimateCache, EstimateKind, Strategy


class SqlAlchemyRepos:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_property_by_addr(self, *, address_line: str, city: str, state: str, zipcode: str) -> Property | None:
        q = (
            select(Property)
            .where(Property.address_line == address_line)
            .where(Property.city == city)
            .where(Property.state == state)
            .where(Property.zipcode == zipcode)
        )
        return (await self.session.execute(q)).scalars().first()

    async def upsert_property(self, payload: dict) -> Property:
        # reuse your existing service function for now, but behind repo seam
        from ..services.ingest import upsert_property as _upsert_property
        return await _upsert_property(self.session, payload)

    async def upsert_lead(self, *, prop: Property, payload: dict, strategy: Strategy) -> Lead:
        from ..services.ingest import create_or_update_lead as _upsert_lead
        lead, _ = await _upsert_lead(
            self.session,
            prop=prop,
            strategy=strategy,
            source=payload["source"],
            source_ref=payload.get("source_ref", ""),
            list_price=payload.get("list_price"),
            rent_estimate=None,
            provenance=payload.get("provenance") or {},
        )
        return lead

    async def get_estimate_cache(self, *, property_id: int, kind: EstimateKind) -> EstimateCache | None:
        q = (
            select(EstimateCache)
            .where(EstimateCache.property_id == property_id)
            .where(EstimateCache.kind == kind)
            .order_by(EstimateCache.created_at.desc())
        )
        return (await self.session.execute(q)).scalars().first()

    async def list_top_leads(self, *, zipcode: str, strategy: Strategy, limit: int) -> list[Lead]:
        q = (
            select(Lead)
            .where(Lead.zipcode == zipcode)
            .where(Lead.strategy == strategy)
            .order_by(Lead.rank_score.desc())
            .limit(limit)
        )
        return list((await self.session.execute(q)).scalars().all())
