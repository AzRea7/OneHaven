from __future__ import annotations

import argparse
import asyncio
import json

from sqlalchemy.ext.asyncio import AsyncSession

from app.db import async_session_maker, engine
from app.models import Base, Integration, IntegrationType


async def _ensure_schema() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def _upsert_integration(session: AsyncSession, name: str, url: str, enabled: bool) -> None:
    # naive idempotent behavior: uniqueness on name
    from sqlalchemy import select

    existing = (await session.execute(select(Integration).where(Integration.name == name))).scalars().first()
    cfg = {"url": url, "secret": None}

    if existing:
        existing.type = IntegrationType.webhook
        existing.enabled = enabled
        existing.config_json = json.dumps(cfg)
    else:
        session.add(
            Integration(
                name=name,
                type=IntegrationType.webhook,
                enabled=enabled,
                config_json=json.dumps(cfg),
            )
        )
    await session.flush()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--enable", action="store_true", help="Enable demo webhook integrations (NOT recommended by default)")
    parser.add_argument("--url", default="https://example.com", help="Demo webhook URL")
    args = parser.parse_args()

    await _ensure_schema()

    async with async_session_maker() as session:
        await _upsert_integration(session, name="demo_webhook_1", url=args.url, enabled=args.enable)
        await _upsert_integration(session, name="demo_webhook_2", url=args.url, enabled=False)  # always disabled
        await session.commit()

    print(f"Seeded demo integrations. enabled={args.enable} url={args.url}")


if __name__ == "__main__":
    asyncio.run(main())
