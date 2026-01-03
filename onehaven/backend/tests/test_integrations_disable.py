import json
import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Integration, IntegrationType
from app.integrations.services.disable import disable_integration


@pytest.mark.asyncio
async def test_integration_name_unique_soft_guard(async_session_maker):
    async with async_session_maker() as session:  # type: AsyncSession
        i1 = Integration(
            name="dup",
            type=IntegrationType.webhook,
            enabled=True,
            config_json=json.dumps({"url": "https://example.com", "secret": "x"}),
        )
        session.add(i1)
        await session.commit()

        i2 = Integration(
            name="dup",
            type=IntegrationType.webhook,
            enabled=True,
            config_json=json.dumps({"url": "https://example.com", "secret": "x"}),
        )
        session.add(i2)

        with pytest.raises(IntegrityError):
            await session.commit()


@pytest.mark.asyncio
async def test_disable_integration_flag(async_session_maker):
    async with async_session_maker() as session:  # type: AsyncSession
        i = Integration(
            name="toggle",
            type=IntegrationType.webhook,
            enabled=True,
            config_json=json.dumps({"url": "https://example.com", "secret": "x"}),
        )
        session.add(i)
        await session.commit()

        await disable_integration(session, name="toggle")
        await session.commit()

        # refresh and verify
        await session.refresh(i)
        assert i.enabled is False
