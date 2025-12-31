import asyncio
import logging

from app.jobs.scheduler import build_scheduler

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    sched = build_scheduler()
    sched.start()
    # Keep the process alive forever
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
