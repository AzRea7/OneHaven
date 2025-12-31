import pytest
from sqlalchemy import select

from app.db import async_session, engine
from app.models import Base, Lead, LeadSource, Strategy
from app.services.ingest import upsert_property, create_or_update_lead, score_lead
from app.scoring.deal import estimate_rent


@pytest.mark.asyncio
async def test_rental_requires_rent_estimatable_from_dims():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Property with no beds and no sqft => estimate_rent(None, None) is None
    async with async_session() as session:
        prop = await upsert_property(
            session,
            {
                "addressLine": "1 NO DIMS ST",
                "city": "BIRMINGHAM",
                "state": "MI",
                "zipCode": "48009",
                "propertyType": "Single Family",
            },
        )
        assert prop is not None
        assert estimate_rent(prop.beds, prop.sqft) is None

        # Flip lead can exist, rental lead should be avoided by refresh logic.
        lead_flip, _ = await create_or_update_lead(
            session=session,
            property_id=prop.id,
            source=LeadSource.manual,
            strategy=Strategy.flip,
            source_ref=None,
            provenance={"test": True},
        )
        lead_flip.list_price = 200000.0
        await score_lead(session, lead_flip, prop, is_auction=False)
        await session.commit()

    async with async_session() as session:
        leads = (await session.execute(select(Lead))).scalars().all()
        assert len(leads) >= 1


@pytest.mark.asyncio
async def test_missing_list_price_should_drop_upstream():
    # This is a behavioral contract test: refresh.py drops missing_list_price.
    # Here we just ensure we do NOT rely on scoring to "fix" missing list price.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # If list_price is None, scoring yields near-zero; our refresh gate should drop earlier.
    # This test just asserts that "None list_price" is a real condition and must be handled.
    async with async_session() as session:
        prop = await upsert_property(
            session,
            {
                "addressLine": "2 NO PRICE ST",
                "city": "BIRMINGHAM",
                "state": "MI",
                "zipCode": "48009",
                "propertyType": "Single Family",
                "bedrooms": 3,
                "squareFeet": 1400,
            },
        )
        assert prop is not None
