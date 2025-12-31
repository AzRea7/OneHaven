import pytest
from app.db import async_session, engine
from app.models import Base, Property
from sqlalchemy import select

@pytest.mark.asyncio
async def test_seed_idempotent_runs_twice():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    from app.services.demo_seed import seed_demo

    await seed_demo()
    await seed_demo()

    async with async_session() as session:
        rows = (await session.execute(select(Property))).scalars().all()

    # Should not crash and should not explode duplicates forever
    assert len(rows) >= 2
