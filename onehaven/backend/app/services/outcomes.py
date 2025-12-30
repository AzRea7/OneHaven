from __future__ import annotations
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Lead, LeadStatus, OutcomeEvent, OutcomeType


def _now_or(ts: datetime | None) -> datetime:
    return ts or datetime.utcnow()


def _map_outcome_to_lead_status(outcome: OutcomeType) -> LeadStatus | None:
    # Only map certain outcomes to the lead's status column.
    if outcome == OutcomeType.contacted:
        return LeadStatus.contacted
    if outcome == OutcomeType.under_contract:
        return LeadStatus.under_contract
    if outcome == OutcomeType.closed:
        return LeadStatus.closed
    if outcome == OutcomeType.dead:
        return LeadStatus.dead
    return None


async def add_outcome_event(
    session: AsyncSession,
    lead_id: int,
    outcome_type: OutcomeType,
    occurred_at: datetime | None = None,
    notes: str | None = None,
    contract_price: float | None = None,
    realized_profit: float | None = None,
    source: str = "manual",
) -> OutcomeEvent:
    lead = (await session.execute(select(Lead).where(Lead.id == lead_id))).scalars().first()
    if not lead:
        raise ValueError(f"Lead {lead_id} not found")

    ev = OutcomeEvent(
        lead_id=lead_id,
        outcome_type=outcome_type,
        occurred_at=_now_or(occurred_at),
        notes=notes,
        contract_price=contract_price,
        realized_profit=realized_profit,
        source=source,
    )
    session.add(ev)

    # Update Lead.status when appropriate
    new_status = _map_outcome_to_lead_status(outcome_type)
    if new_status:
        lead.status = new_status
        lead.updated_at = datetime.utcnow()

    await session.flush()
    return ev


async def update_lead_status(
    session: AsyncSession,
    lead_id: int,
    status: LeadStatus,
    occurred_at: datetime | None = None,
    notes: str | None = None,
    source: str = "manual",
) -> OutcomeEvent:
    # Record status changes as outcomes too (keeps training data consistent)
    outcome_map = {
        LeadStatus.contacted: OutcomeType.contacted,
        LeadStatus.under_contract: OutcomeType.under_contract,
        LeadStatus.closed: OutcomeType.closed,
        LeadStatus.dead: OutcomeType.dead,
        LeadStatus.qualified: OutcomeType.responded,  # optional: treat qualified as "responded-ish"
        LeadStatus.new: OutcomeType.responded,        # not used normally
    }
    outcome = outcome_map.get(status, OutcomeType.responded)
    return await add_outcome_event(
        session=session,
        lead_id=lead_id,
        outcome_type=outcome,
        occurred_at=occurred_at,
        notes=notes,
        source=source,
    )
