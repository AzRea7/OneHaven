# app/adapters/repos/leads.py
from __future__ import annotations

import json
from typing import Optional, Tuple, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...models import Lead, LeadStatus, Strategy, Property


class LeadRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert(
        self,
        *,
        # Accept either prop=Property or property_id=int
        prop: Optional[Property] = None,
        property_id: Optional[int] = None,
        strategy: Strategy,
        list_price: Optional[float] = None,
        max_price_rule: Optional[float] = None,
        score: Optional[float] = None,
        status: Optional[LeadStatus] = None,
        reasons_json: Optional[str] = None,
        raw_json: Optional[str] = None,
        # provenance fields that callers may send (even if DB doesn't have columns)
        source: Optional[str] = None,
        source_ref: Optional[str] = None,
        **_extra: Any,  # swallow unexpected kwargs from newer call sites
    ) -> Tuple[Lead, bool]:
        """
        Upsert a Lead.

        Compatibility layer:
        - Some callers pass prop=<Property>
        - Others pass property_id=<int>
        - Some pass source/source_ref even though Lead table has no such columns.

        Natural key used: (property_id, strategy)
        """

        if property_id is None:
            if prop is None:
                raise TypeError("LeadRepository.upsert requires either property_id=... or prop=...")
            if getattr(prop, "id", None) is None:
                raise ValueError("prop.id is None; make sure the property was flushed/inserted before upserting leads")
            property_id = int(prop.id)

        q = select(Lead).where(
            Lead.property_id == property_id,
            Lead.strategy == strategy,
        )
        lead = (await self.session.execute(q)).scalars().first()

        was_created = False
        if lead is None:
            lead = Lead(property_id=property_id, strategy=strategy)
            self.session.add(lead)
            was_created = True

        # Update numeric/business fields
        if list_price is not None:
            lead.list_price = float(list_price)
        if max_price_rule is not None:
            lead.max_price_rule = float(max_price_rule)
        if score is not None:
            lead.score = float(score)
        if status is not None:
            lead.status = status
        if reasons_json is not None:
            lead.reasons_json = reasons_json

        # Raw payload (plus provenance) — because DB doesn't have dedicated columns
        if raw_json is not None:
            lead.raw_json = raw_json

        # If we still don't have raw_json, but we have provenance, store it minimally.
        if (lead.raw_json is None or lead.raw_json == "") and (source or source_ref):
            lead.raw_json = json.dumps(
                {"source": source, "source_ref": source_ref},
                ensure_ascii=False,
            )
        elif (source or source_ref) and lead.raw_json:
            # Try to merge provenance into existing raw_json (best effort).
            try:
                data = json.loads(lead.raw_json)
                if isinstance(data, dict):
                    if source is not None:
                        data.setdefault("source", source)
                    if source_ref is not None:
                        data.setdefault("source_ref", source_ref)
                    lead.raw_json = json.dumps(data, ensure_ascii=False)
            except Exception:
                # If raw_json isn't valid JSON, don't break refresh; just leave it.
                pass

        await self.session.flush()
        return lead, was_created
