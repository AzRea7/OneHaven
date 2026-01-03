import pytest
from sqlalchemy import select

from app.models import Base, OutboxEvent, LeadSource, Strategy, LeadStatus
from app.adapters.repos.properties import PropertyRepository
from app.adapters.repos.leads import LeadRepository
from app.service_layer.scoring import score_lead
from app.service_layer.use_cases.outcomes import update_lead_status


@pytest.mark.asyncio
async def test_outcome_creates_outbox_event(engine, async_session_maker):
    # ensure schema exists (already created in conftest, but safe)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session_maker() as session:
        prop_repo = PropertyRepository(session)
        lead_repo = LeadRepository(session)

        prop = await prop_repo.upsert_from_payload(
            {
                "addressLine": "999 OUTBOX ST",
                "city": "BIRMINGHAM",
                "state": "MI",
                "zipCode": "48009",
                "propertyType": "Single Family",
                "bedrooms": 3,
                "bathrooms": 2,
                "squareFeet": 1500,
                "listPrice": 250000,
            }
        )

        lead, _created = await lead_repo.upsert(
            prop=prop,
            source=LeadSource.manual,
            strategy=Strategy.rental,
            source_ref=None,
            list_price=250000.0,
            rent_estimate=2000.0,
            provenance={"test": True},
        )

        await score_lead(session, lead, prop, is_auction=False)

        await update_lead_status(session, lead.id, LeadStatus.under_contract, None, "test", "manual")
        await session.commit()

    async with async_session_maker() as session:
        events = (await session.execute(select(OutboxEvent))).scalars().all()

    assert len(events) > 0
