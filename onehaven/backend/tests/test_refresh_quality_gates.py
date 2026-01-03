import pytest
from sqlalchemy import select

from app.models import Base, Lead, Strategy, LeadSource
from app.service_layer.use_cases import refresh as refresh_uc
from app.adapters.ingestion.base import RawLead, IngestionProvider
from app.service_layer.estimates import EstimateResult


class _FakeProvider(IngestionProvider):
    async def fetch(self, **kwargs):
        return [
            RawLead(
                source=LeadSource.manual,
                source_ref="fake-1",
                payload={
                    "addressLine": "1 NO RENT ST",
                    "city": "BIRMINGHAM",
                    "state": "MI",
                    "zipCode": "48009",
                    "propertyType": "Single Family",
                    "bedrooms": 3,
                    "bathrooms": 2,
                    "squareFeet": 1500,
                    "listPrice": 250000,
                },
            )
        ]


@pytest.mark.asyncio
async def test_refresh_blocks_rental_when_rent_enrichment_missing(engine, async_session_maker, monkeypatch):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    monkeypatch.setattr(refresh_uc, "_provider", lambda: _FakeProvider())

    async def _fake_value(_prop):
        return EstimateResult(value=300000.0, source="fake", raw=None)

    async def _fake_rent(_prop):
        return EstimateResult(value=None, source="fake", raw=None)

    monkeypatch.setattr(refresh_uc, "fetch_value", _fake_value)
    monkeypatch.setattr(refresh_uc, "fetch_rent_long_term", _fake_rent)

    async with async_session_maker() as session:
        res = await refresh_uc.refresh_region_use_case(session, zips=["48009"], strategy=Strategy.rental)
        await session.commit()

    assert res["created_leads"] == 1

    async with async_session_maker() as session:
        lead = (await session.execute(select(Lead).limit(1))).scalars().first()
        assert lead is not None
        assert lead.strategy == Strategy.rental
        assert lead.rent_estimate is None
        assert "blocked: missing rent_estimate" in (lead.explain_json or "")
        assert lead.rank_score == 0.0
