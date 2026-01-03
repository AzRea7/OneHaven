# tests/test_estimate_cache_roundtrip.py
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import EstimateKind
from app.service_layer.estimates import get_or_fetch_estimate
from app.adapters.clients.rentcast_avm import fetch_rent_long_term


@pytest.mark.asyncio
async def test_estimate_cache_writes_row(async_session_maker, seeded_property):
    async with async_session_maker() as session:  # type: AsyncSession
        row = await get_or_fetch_estimate(
            session,
            prop=seeded_property,
            kind=EstimateKind.rent_long_term,
            ttl_days=90,
            fetcher=fetch_rent_long_term,
        )
        assert row is not None
        # value might be None if API key missing, but cache row should exist
        assert row.kind == EstimateKind.rent_long_term
