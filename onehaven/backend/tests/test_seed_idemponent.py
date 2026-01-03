import pytest
from sqlalchemy import select

from app.db import async_session, engine
from app.models import Base, Integration
from app.service_layer.demo_seed import seed_demo


@pytest.mark.asyncio
async def test_seed_idempotent_runs_twice():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        await seed_demo(session, enable_demo_webhooks=False)
        await session.commit()

    async with async_session() as session:
        await seed_demo(session, enable_demo_webhooks=False)
        await session.commit()

    async with async_session() as session:
        rows = (await session.execute(select(Integration))).scalars().all()

    assert len(rows) >= 2
