# app/services/outcomes.py
from __future__ import annotations

from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Lead, LeadStatus, OutcomeEvent, OutcomeType
from ..integrations.services.outbox import enqueue_event

# funnel order for stage sanity
_STAGE_ORDER: dict[OutcomeType, int] = {
    OutcomeType.contacted: 1,
    OutcomeType.responded: 2,
    OutcomeType.appointment_set: 3,
    OutcomeType.under_contract: 4,
    OutcomeType.closed: 5,
    OutcomeType.dead: 99,
}

_TERMINAL: set[OutcomeType] = {OutcomeType.closed, OutcomeType.dead}


def _implied_lead_status(outcome_type: OutcomeType) -> LeadStatus | None:
    if outcome_type in (
        OutcomeType.contacted,
        OutcomeType.responded,
        OutcomeType.appointment_set,
        OutcomeType.under_contract,
    ):
        return LeadStatus.contacted
    if outcome_type == OutcomeType.closed:
        return LeadStatus.closed
    if outcome_type == OutcomeType.dead:
        return LeadStatus.dead
    return None


async def update_lead_status(
    session: AsyncSession,
    lead_id: int,
    status: LeadStatus,
    occurred_at: datetime | None = None,
    notes: str | None = None,
    source: str = "manual",
) -> None:
    """
    Simple operational status update (queue workflow).
    Also emits an outbox event so downstream systems can mirror your status.
    """
    lead = (await session.execute(select(Lead).where(Lead.id == lead_id))).scalars().first()
    if not lead:
        raise ValueError(f"Lead {lead_id} not found")

    lead.status = status
    lead.updated_at = datetime.utcnow()
    await session.flush()

    await enqueue_event(
        session,
        "lead.status_changed",
        {
            "lead_id": lead_id,
            "status": status.value,
            "occurred_at": (occurred_at or datetime.utcnow()).isoformat(),
            "source": source,
            "notes": notes,
        },
    )


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

    now = occurred_at or datetime.utcnow()

    existing = (
        await session.execute(
            select(OutcomeEvent)
            .where(OutcomeEvent.lead_id == lead_id)
            .order_by(OutcomeEvent.occurred_at.asc())
        )
    ).scalars().all()

    existing_types = [e.outcome_type for e in existing]
    existing_terminal = next((t for t in existing_types if t in _TERMINAL), None)

    # terminal consistency rule
    if existing_terminal and outcome_type in _TERMINAL and outcome_type != existing_terminal:
        raise ValueError(
            f"Lead {lead_id} already has terminal outcome '{existing_terminal.value}'. "
            f"Refusing to add conflicting terminal '{outcome_type.value}'."
        )

    # warn for backwards stage logging (non-fatal)
    max_stage = 0
    for t in existing_types:
        max_stage = max(max_stage, _STAGE_ORDER.get(t, 0))

    new_stage = _STAGE_ORDER.get(outcome_type, 0)
    if outcome_type not in _TERMINAL and new_stage and new_stage < max_stage:
        notes = (notes or "").strip()
        notes = (notes + " | WARN: logged out-of-order").strip(" |")

    ev = OutcomeEvent(
        lead_id=lead_id,
        outcome_type=outcome_type,
        occurred_at=now,
        source=source,
        notes=notes,
        contract_price=contract_price,
        realized_profit=realized_profit,
    )
    session.add(ev)
    await session.flush()

    implied = _implied_lead_status(outcome_type)
    if implied is not None:
        lead.status = implied
        lead.updated_at = datetime.utcnow()
        await session.flush()

    await enqueue_event(
        session,
        "lead.outcome",
        {
            "lead_id": lead_id,
            "outcome_type": outcome_type.value,
            "occurred_at": now.isoformat(),
            "source": source,
            "contract_price": contract_price,
            "realized_profit": realized_profit,
            "notes": notes,
        },
    )

    return ev
