import json
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Property, Lead, LeadSource, Strategy
from .entity_resolution import canonicalize_address
from .normalize import normalize_property_type, is_allowed_type
from .outbox import enqueue_event


async def upsert_property(session: AsyncSession, payload: dict) -> Property | None:
    addr = payload.get("address") or payload.get("addressLine") or payload.get("street") or ""
    city = payload.get("city") or ""
    state = payload.get("state") or "MI"
    zipcode = payload.get("zip") or payload.get("zipcode") or payload.get("zipCode") or ""

    if not addr or not city or not zipcode:
        return None

    canon = canonicalize_address(addr, city, state, zipcode)

    stmt = select(Property).where(
        Property.address_line == canon.address_line,
        Property.city == canon.city,
        Property.state == canon.state,
        Property.zipcode == canon.zipcode,
    )
    existing = (await session.execute(stmt)).scalars().first()

    raw_type = payload.get("propertyType") or payload.get("property_type")
    norm_type = normalize_property_type(raw_type)

    if norm_type and not is_allowed_type(norm_type):
        return None

    if existing:
        existing.property_type = existing.property_type or norm_type
        existing.beds = existing.beds or payload.get("bedrooms") or payload.get("beds")
        existing.baths = existing.baths or payload.get("bathrooms") or payload.get("baths")
        existing.sqft = existing.sqft or payload.get("squareFeet") or payload.get("sqft")
        return existing

    p = Property(
        address_line=canon.address_line,
        city=canon.city,
        state=canon.state,
        zipcode=canon.zipcode,
        property_type=norm_type,
        beds=payload.get("bedrooms") or payload.get("beds"),
        baths=payload.get("bathrooms") or payload.get("baths"),
        sqft=payload.get("squareFeet") or payload.get("sqft"),
    )
    session.add(p)
    await session.flush()
    return p


async def create_or_update_lead(
    session: AsyncSession,
    property_id: int,
    source: LeadSource,
    strategy: Strategy,
    source_ref: str | None,
    provenance: dict,
) -> Lead:
    stmt = select(Lead).where(
        Lead.property_id == property_id,
        Lead.source == source,
        Lead.strategy == strategy,
    )
    lead = (await session.execute(stmt)).scalars().first()
    now = datetime.utcnow()

    if lead:
        lead.updated_at = now
        lead.provenance_json = json.dumps(provenance)

        await enqueue_event(
            session,
            "lead.upserted",
            {
                "lead_id": lead.id,
                "property_id": property_id,
                "source": source.value,
                "strategy": strategy.value,
                "updated_at": lead.updated_at.isoformat(),
            },
        )
        return lead

    lead = Lead(
        property_id=property_id,
        source=source,
        strategy=strategy,
        source_ref=source_ref,
        provenance_json=json.dumps(provenance),
        created_at=now,
        updated_at=now,
    )
    session.add(lead)
    await session.flush()

    await enqueue_event(
        session,
        "lead.created",
        {
            "lead_id": lead.id,
            "property_id": property_id,
            "source": source.value,
            "strategy": strategy.value,
            "created_at": lead.created_at.isoformat(),
        },
    )
    return lead
