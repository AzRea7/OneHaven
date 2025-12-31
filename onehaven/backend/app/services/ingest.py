# app/services/ingest.py
import json
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Property, Lead, LeadSource, Strategy
from .entity_resolution import canonicalize_address
from .normalize import normalize_property_type, is_allowed_type
from ..integrations.services.outbox import enqueue_event

from ..scoring.deal import estimate_arv, estimate_rehab, estimate_rent, deal_score
from ..scoring.motivation import MotivationSignals, motivation_score
from ..scoring.ranker import rank_score, explain
from .features import years_since, equity_proxy, vacancy_proxy


def _num(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


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

    raw_type = payload.get("propertyType") or payload.get("property_type") or payload.get("PropertyType")
    norm_type = normalize_property_type(raw_type)

    if norm_type and not is_allowed_type(norm_type):
        return None

    if existing:
        existing.property_type = existing.property_type or norm_type
        existing.beds = existing.beds or payload.get("bedrooms") or payload.get("beds") or payload.get("BedroomsTotal")
        existing.baths = existing.baths or payload.get("bathrooms") or payload.get("baths") or payload.get("BathroomsTotal")
        existing.sqft = existing.sqft or payload.get("squareFeet") or payload.get("sqft") or payload.get("LivingArea")
        return existing

    p = Property(
        address_line=canon.address_line,
        city=canon.city,
        state=canon.state,
        zipcode=canon.zipcode,
        property_type=norm_type,
        beds=payload.get("bedrooms") or payload.get("beds") or payload.get("BedroomsTotal"),
        baths=payload.get("bathrooms") or payload.get("baths") or payload.get("BathroomsTotal"),
        sqft=payload.get("squareFeet") or payload.get("sqft") or payload.get("LivingArea"),
        lat=_num(payload.get("lat") or payload.get("Latitude")),
        lon=_num(payload.get("lon") or payload.get("Longitude")),
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
) -> tuple[Lead, bool]:
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
        return lead, False

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
    return lead, True


async def score_lead(session: AsyncSession, lead: Lead, prop: Property, is_auction: bool) -> None:
    list_price = lead.list_price

    lead.arv_estimate = estimate_arv(list_price)
    lead.rehab_estimate = estimate_rehab(prop.sqft)
    lead.rent_estimate = estimate_rent(prop.beds, prop.sqft)

    absentee = vacancy_proxy(prop.owner_mailing, prop.address_line) >= 1.0
    yrs_held = years_since(prop.last_sale_date)
    equity = equity_proxy(lead.arv_estimate, list_price)

    mot = motivation_score(
        MotivationSignals(
            is_auction=is_auction,
            absentee=absentee,
            years_held=yrs_held,
            equity_frac=equity,
        )
    )

    deal = deal_score(
        list_price=list_price,
        arv=lead.arv_estimate,
        rehab=lead.rehab_estimate,
        rent=lead.rent_estimate,
        strategy=lead.strategy.value,
    )

    lead.motivation_score = float(mot)
    lead.deal_score = float(deal)
    lead.rank_score = float(rank_score(deal, mot, lead.strategy.value))
    lead.explain = explain(deal, mot, is_auction=is_auction, absentee=absentee, equity=equity)

    lead.updated_at = datetime.utcnow()
    await session.flush()
