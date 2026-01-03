# tests/conftest.py
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.pool import StaticPool

from app.models import Base  
from app.models import Property

@pytest.fixture(autouse=True)
async def _reset_db(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield


@pytest.fixture
async def seeded_property(async_session_maker):
    async with async_session_maker() as session:  # type: AsyncSession
        p = Property(
            address_line="123 Main St",
            city="Birmingham",
            state="MI",
            zipcode="48009",
            lat=None,
            lon=None,
            property_type="single_family",
            beds=3,
            baths=2.0,
            sqft=1500,
        )
        session.add(p)
        await session.commit()
        await session.refresh(p)
        return p


@pytest.fixture
async def engine():
    """
    Fresh in-memory DB per test. StaticPool makes all connections share the same
    in-memory database for the lifetime of this engine fixture.
    """
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def async_session_maker(engine):
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=True)
