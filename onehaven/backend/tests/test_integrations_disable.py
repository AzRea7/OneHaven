# tests/test_integrations_disable.py
import json
import pytest
from sqlalchemy import select

from app.db import async_session, engine
from app.models import Base, Integration, IntegrationType


@pytest.mark.asyncio
async def test_integration_name_unique_soft_guard():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        i1 = Integration(
            name="dup",
            type=IntegrationType.webhook,
            enabled=True,
            config_json=json.dumps({"url": "https://example.com", "secret": "x"}),
        )
        session.add(i1)
        await session.commit()

    async with async_session() as session:
        # second insert with same name should fail if you applied DB unique constraint
        i2 = Integration(
            name="dup",
            type=IntegrationType.webhook,
            enabled=True,
            config_json=json.dumps({"url": "https://example.com", "secret": "x"}),
        )
        session.add(i2)
        failed = False
        try:
            await session.commit()
        except Exception:
            failed = True
            await session.rollback()

    assert failed is True


@pytest.mark.asyncio
async def test_disable_integration_flag():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        i = Integration(
            name="toggle",
            type=IntegrationType.webhook,
            enabled=True,
            config_json=json.dumps({"url": "https://example.com", "secret": "x"}),
        )
        session.add(i)
        await session.commit()

    async with async_session() as session:
        row = (await session.execute(select(Integration).where(Integration.name == "toggle"))).scalars().first()
        assert row is not None
        row.enabled = False
        await session.commit()

    async with async_session() as session:
        row2 = (await session.execute(select(Integration).where(Integration.name == "toggle"))).scalars().first()
        assert row2 is not None
        assert row2.enabled is False
