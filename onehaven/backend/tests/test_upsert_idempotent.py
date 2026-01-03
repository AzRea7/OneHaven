import pytest
from app.db import async_session
from app.adapters.repos.properties import PropertyRepository


@pytest.mark.asyncio
async def test_upsert_property_idempotent():
    payload = {
        "addressLine": "123 DEMO ST",
        "city": "BIRMINGHAM",
        "state": "MI",
        "zipCode": "48009",
        "propertyType": "Single Family",
        "bedrooms": 3,
        "bathrooms": 2,
        "squareFeet": 1400,
    }

    async with async_session() as session:
        repo = PropertyRepository(session)
        p1 = await repo.upsert_from_payload(payload)
        await session.commit()

    async with async_session() as session:
        repo = PropertyRepository(session)
        p2 = await repo.upsert_from_payload(payload)
        await session.commit()

    assert p1 is not None
    assert p2 is not None
    assert p1.id == p2.id
