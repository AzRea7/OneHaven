# app/adapters/repos/leads.py
from __future__ import annotations

from typing import Any, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...models import Lead, Property, Strategy, LeadStatus


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
        # provenance fields callers may send
        source: Optional[str] = None,
        source_ref: Optional[str] = None,
        **_extra: Any,  # swallow unexpected kwargs from newer call sites
    ) -> Tuple[Lead, bool]:
        """
        Upsert a Lead using natural key: (property_id, strategy).

        IMPORTANT:
        Return flag MUST represent whether we created a *new row*.
        Do NOT infer "created" from lead.id after flush/commit.
        """

        if property_id is None:
            if prop is None:
                raise TypeError("LeadRepository.upsert requires either property_id=... or prop=...")
            if getattr(prop, "id", None) is None:
                # make it loud; a missing property id means property wasn't flushed/inserted
                raise ValueError("prop.id is None; property must be persisted before upserting leads")
            property_id = int(prop.id)

        q = select(Lead).where(
            Lead.property_id == property_id,
            Lead.strategy == strategy.value if isinstance(strategy, Strategy) else str(strategy),
        )
        existing = (await self.session.execute(q)).scalars().first()

        was_created = existing is None
        lead = existing
        print("DEBUG LeadRepo.upsert was_created =", was_created)


        if lead is None:
            lead = Lead(
                property_id=property_id,
                strategy=(strategy.value if isinstance(strategy, Strategy) else str(strategy)),
                status=LeadStatus.new.value,
            )
            self.session.add(lead)

        # Update business fields
        if list_price is not None:
            lead.list_price = float(list_price)
        if max_price_rule is not None and hasattr(lead, "max_price_rule"):
            lead.max_price_rule = float(max_price_rule)
        if score is not None and hasattr(lead, "score"):
            lead.score = float(score)
        if status is not None:
            lead.status = status.value if hasattr(status, "value") else str(status)
        if reasons_json is not None and hasattr(lead, "reasons_json"):
            lead.reasons_json = reasons_json

        # Optional raw/provenance capture
        if raw_json is not None and hasattr(lead, "raw_json"):
            lead.raw_json = raw_json
        elif hasattr(lead, "raw_json") and (lead.raw_json is None or lead.raw_json == "") and (source or source_ref):
            # Store a minimal provenance marker if caller provided it
            lead.raw_json = f'{{"source": {source!r}, "source_ref": {source_ref!r}}}'

        return lead, was_created
