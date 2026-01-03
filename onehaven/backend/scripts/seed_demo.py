from __future__ import annotations

import argparse
import asyncio

from app.db import async_session_maker, engine
from app.models import Base
from app.service_layer.demo_seed import seed_demo


async def _ensure_schema() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--enable", action="store_true", help="Enable demo webhook integrations (NOT recommended by default)")
    parser.add_argument("--url", default="https://example.com", help="Demo webhook URL")
    args = parser.parse_args()

    await _ensure_schema()

    async with async_session_maker() as session:
        res = await seed_demo(session, url=args.url, enable_demo_webhooks=args.enable)
        await session.commit()

    print(f"Seeded demo integrations: {res}")


if __name__ == "__main__":
    asyncio.run(main())
