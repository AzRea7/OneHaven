# scripts/smoke_top_leads.py
import asyncio
from sqlalchemy import select

from app.db import async_session
from app.models import Lead, Strategy


async def main():
    async with async_session() as session:
        rows = (await session.execute(
            select(Lead).where(Lead.zipcode == "48362").where(Lead.strategy == Strategy.rental).order_by(Lead.rank_score.desc()).limit(10)
        )).scalars().all()

        for l in rows:
            print(l.id, l.address_line, l.list_price, l.rent_estimate, l.rank_score, l.explain_json)


if __name__ == "__main__":
    asyncio.run(main())
