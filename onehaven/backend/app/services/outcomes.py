from __future__ import annotations

from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Lead, LeadStatus, OutcomeEvent, OutcomeType
from ..integrations.services.outbox import enqueue_event


async def add_outcome_event(
    session: AsyncSession,
    lead_id: int,
    outcome_type: OutcomeType,
    occurred_at: datetime | None,
    notes: str | None,
    contract_price: float | None,
    realized_profit: float | None,
    source: str,
) -> OutcomeEvent:
    lead = (await session.execute(select(Lead).where(Lead.id == lead_id))).scalars().first()
    if not lead:
        raise ValueError(f"Lead {lead_id} not found")

    ev = OutcomeEvent(
        lead_id=lead_id,
        outcome_type=outcome_type,
        occurred_at=occurred_at or datetime.utcnow(),
        source=source,
        notes=notes,
        contract_price=contract_price,
        realized_profit=realized_profit,
    )
    session.add(ev)
    await session.flush()

    await enqueue_event(
        session,
        "outcome.created",
        {
            "outcome_id": ev.id,
            "lead_id": ev.lead_id,
            "outcome_type": ev.outcome_type.value,
            "occurred_at": ev.occurred_at.isoformat(),
            "source": ev.source,
        },
    )

    return ev


async def update_lead_status(
    session: AsyncSession,
    lead_id: int,
    status: LeadStatus,
    occurred_at: datetime | None,
    notes: str | None,
    source: str,
) -> OutcomeEvent:
    lead = (await session.execute(select(Lead).where(Lead.id == lead_id))).scalars().first()
    if not lead:
        raise ValueError(f"Lead {lead_id} not found")

    lead.status = status
    lead.updated_at = datetime.utcnow()

    # Record outcome event as well
    ev = OutcomeEvent(
        lead_id=lead_id,
        outcome_type=OutcomeType(status.value) if status.value in OutcomeType.__members__ else OutcomeType.responded,
        occurred_at=occurred_at or datetime.utcnow(),
        source=source,
        notes=notes,
        contract_price=None,
        realized_profit=None,
    )
    session.add(ev)
    await session.flush()

    await enqueue_event(
        session,
        "lead.status_changed",
        {
            "lead_id": lead.id,
            "status": lead.status.value,
            "occurred_at": ev.occurred_at.isoformat(),
        },
    )
    await enqueue_event(
        session,
        "outcome.created",
        {
            "outcome_id": ev.id,
            "lead_id": ev.lead_id,
            "outcome_type": ev.outcome_type.value,
            "occurred_at": ev.occurred_at.isoformat(),
            "source": ev.source,
        },
    )

    return ev
