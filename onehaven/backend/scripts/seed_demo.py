import asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import AsyncSessionLocal, engine
from app.models import Base, Property, Lead, LeadSource, Strategy
from app.services.ingest import score_lead

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:  # type: AsyncSession
        p = Property(
            address_line="123 DEMO ST",
            city="BIRMINGHAM",
            state="MI",
            zipcode="48009",
            beds=3,
            baths=2.0,
            sqft=1400,
            property_type="single_family",
        )
        session.add(p)
        await session.flush()

        l = Lead(
            property_id=p.id,
            source=LeadSource.manual,
            strategy=Strategy.rental,
            list_price=240000.0,
        )
        session.add(l)
        await session.flush()

        await score_lead(session, l, p, is_auction=False)
        await session.commit()

if __name__ == "__main__":
    asyncio.run(main())
