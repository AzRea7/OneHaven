import pytest
from sqlalchemy import select

from app.db import async_session, engine
from app.models import Base, OutboxEvent, LeadSource, Strategy
from app.services.ingest import upsert_property, create_or_update_lead, score_lead
from onehaven.backend.app.service_layer.use_cases.outcomes import update_lead_status
from app.models import LeadStatus

@pytest.mark.asyncio
async def test_outcome_creates_outbox_event():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        prop = await upsert_property(session, {
            "addressLine": "999 OUTBOX ST",
            "city": "BIRMINGHAM",
            "state": "MI",
            "zipCode": "48009",
            "propertyType": "Single Family",
            "bedrooms": 3,
            "bathrooms": 2,
            "squareFeet": 1500,
            "listPrice": 250000,
        })
        res = await create_or_update_lead(
            session=session,
            property_id=prop.id,
            source=LeadSource.manual,
            strategy=Strategy.rental,
            source_ref=None,
            provenance={"test": True},
        )
        lead = res[0] if isinstance(res, tuple) else res
        lead.list_price = 250000.0
        await score_lead(session, lead, prop, is_auction=False)

        # Create status change (should enqueue outbox)
        await update_lead_status(session, lead.id, LeadStatus.under_contract, None, "test", "manual")
        await session.commit()

    async with async_session() as session:
        events = (await session.execute(select(OutboxEvent))).scalars().all()

    assert len(events) > 0
