# app/adapters/repos/leads.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...models import Lead, LeadSource, LeadStatus, Property, Strategy


class LeadRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(
        self,
        *,
        prop: Property,
        strategy: Strategy,
        source: LeadSource,
        source_ref: str | None,
        list_price: float | None,
        rent_estimate: float | None,
        provenance: dict[str, Any] | None = None,
    ) -> tuple[Lead, bool]:
        """
        Upsert a lead by (property_id, strategy, source).
        """
        q = select(Lead).where(
            Lead.property_id == prop.id,
            Lead.strategy == strategy,
            Lead.source == source,
        )
        existing = (await self.session.execute(q)).scalars().first()

        prov_json = None
        if provenance is not None:
            try:
                prov_json = json.dumps(provenance)[:20000]
            except Exception:
                prov_json = None

        if existing:
            existing.source_ref = source_ref or existing.source_ref
            existing.list_price = list_price if list_price is not None else existing.list_price
            existing.rent_estimate = rent_estimate if rent_estimate is not None else existing.rent_estimate
            existing.updated_at = datetime.utcnow()
            if hasattr(existing, "score_json") and prov_json is not None:
                existing.score_json = prov_json
            await self.session.flush()
            return existing, False

        lead = Lead(
            property_id=prop.id,
            strategy=strategy,
            source=source,
            source_ref=source_ref,
            list_price=list_price,
            rent_estimate=rent_estimate,
            status=LeadStatus.new,
            deal_score=0.0,
            motivation_score=0.0,
            rank_score=0.0,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        if hasattr(lead, "score_json") and prov_json is not None:
            lead.score_json = prov_json

        self.session.add(lead)
        await self.session.flush()
        return lead, True
