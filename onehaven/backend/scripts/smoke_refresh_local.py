# scripts/smoke_refresh_local.py
import asyncio
import os

from app.db import async_session
from app.jobs.refresh import refresh_region
from app.models import Strategy


async def main():
    zips = os.environ.get("ZIPS", "48362").split(",")
    async with async_session() as session:
        res = await refresh_region(
            session,
            zips=zips,
            max_price=float(os.environ.get("MAX_PRICE", "650000")),
            per_zip_limit=int(os.environ.get("PER_ZIP", "50")),
            strategy=Strategy.rental,
        )
        print(res)


if __name__ == "__main__":
    asyncio.run(main())
