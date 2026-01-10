# app/db.py
from __future__ import annotations

import os
import sqlite3
from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .config import settings
from .models import Base


def _get_db_url() -> str:
    """
    Canonical DB URL resolution.

    Your repo uses HAVEN_DB_URL (confirmed). Support additional names to prevent future drift:
      - DB_URI
      - DATABASE_URL

    Order:
      1) settings.HAVEN_DB_URL (current)
      2) settings.DB_URI / settings.DATABASE_URL (future-proof)
      3) env vars HAVEN_DB_URL / DB_URI / DATABASE_URL (override for CI/prod)
      4) default sqlite local file
    """
    for attr in ("HAVEN_DB_URL", "DB_URI", "DATABASE_URL"):
        if hasattr(settings, attr):
            v = getattr(settings, attr)
            if isinstance(v, str) and v.strip():
                return v.strip()

    for env in ("HAVEN_DB_URL", "DB_URI", "DATABASE_URL"):
        v = os.getenv(env)
        if v and v.strip():
            return v.strip()

    return "sqlite+aiosqlite:///./haven.db"


def _sqlite_path_from_uri(db_uri: str) -> str | None:
    if not db_uri.startswith("sqlite"):
        return None
    if ":///" not in db_uri:
        return None
    return db_uri.split(":///", 1)[1]


def _apply_sqlite_pragmas(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.commit()
    finally:
        conn.close()


def _run_sqlite_migrations(db_path: str) -> None:
    from scripts.migrate_sqlite import main as migrate_main

    migrate_main(db_path)


DATABASE_URL = _get_db_url()

_db_path = _sqlite_path_from_uri(DATABASE_URL)
if _db_path:
    _apply_sqlite_pragmas(_db_path)

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def ensure_schema() -> None:
    # Create missing tables then apply idempotent column migrations.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    if _db_path:
        _run_sqlite_migrations(_db_path)


@asynccontextmanager
async def async_session() -> AsyncIterator[AsyncSession]:
    async with async_session_maker() as session:
        yield session


# ✅ Compatibility: your existing FastAPI deps import get_session
async def get_session() -> AsyncIterator[AsyncSession]:
    async with async_session_maker() as session:
        yield session
