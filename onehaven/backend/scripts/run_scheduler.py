from __future__ import annotations

import asyncio
import logging

from app.jobs.scheduler import build_scheduler


def _quiet_logging() -> None:
    # Root defaults
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(name)s:%(message)s",
    )

    # Quiet the usual offenders
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


async def main() -> None:
    _quiet_logging()

    scheduler = build_scheduler()
    scheduler.start()
    logging.getLogger(__name__).info("Scheduler started")

    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        scheduler.shutdown()
        logging.getLogger(__name__).info("Scheduler stopped")


if __name__ == "__main__":
    asyncio.run(main())
