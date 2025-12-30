from sqlalchemy.ext.asyncio import AsyncSession
from ..services.outbox import dispatch_pending_events


async def run_dispatch(session: AsyncSession, batch_size: int = 50) -> dict:
    return await dispatch_pending_events(session=session, batch_size=batch_size)
