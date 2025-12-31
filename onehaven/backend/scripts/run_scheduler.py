import asyncio
import logging

from app.jobs.scheduler import build_scheduler
from app.db import engine
from app.models import Base

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    # Ensure tables exist (dev)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    sched = build_scheduler()
    sched.start()
    logging.info("Scheduler started")

    # Keep alive forever
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
