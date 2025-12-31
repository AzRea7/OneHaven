from __future__ import annotations

from app.db import engine, async_session
from app.models import Base, LeadSource, Strategy
from app.services.ingest import upsert_property, create_or_update_lead, score_lead


async def seed_demo() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    demo_payloads = [
        {
            "addressLine": "123 DEMO ST",
            "city": "BIRMINGHAM",
            "state": "MI",
            "zipCode": "48009",
            "propertyType": "Single Family",
            "bedrooms": 3,
            "bathrooms": 2,
            "squareFeet": 1400,
            "listPrice": 240000,
        },
        {
            "addressLine": "456 TEST AVE",
            "city": "TROY",
            "state": "MI",
            "zipCode": "48084",
            "propertyType": "Single Family",
            "bedrooms": 4,
            "bathrooms": 3,
            "squareFeet": 2200,
            "listPrice": 415000,
        },
    ]

    async with async_session() as session:
        for payload in demo_payloads:
            prop = await upsert_property(session, payload)
            if not prop:
                continue

            res = await create_or_update_lead(
                session=session,
                property_id=prop.id,
                source=LeadSource.manual,
                strategy=Strategy.rental,
                source_ref=None,
                provenance={"seed": True},
            )
            lead = res[0] if isinstance(res, tuple) else res
            lead.list_price = float(payload["listPrice"])

            await score_lead(session, lead, prop, is_auction=False)

        await session.commit()
