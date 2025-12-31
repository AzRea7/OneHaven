from __future__ import annotations

from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from .config import settings

engine: AsyncEngine = create_async_engine(settings.HAVEN_DB_URL, echo=False, future=True)

# Canonical async session factory
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Back-compat alias used by scheduler.py
# (scheduler imports: from ..db import async_session_maker)
async_session_maker = AsyncSessionLocal


async def get_session() -> AsyncSession:
    """
    FastAPI dependency that yields a session.
    """
    async with AsyncSessionLocal() as session:
        yield session


@asynccontextmanager
async def async_session() -> AsyncSession:
    """
    Convenience context manager used in tests and scripts.
    """
    async with AsyncSessionLocal() as session:
        yield session
