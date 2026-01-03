# app/integrations/services/disable.py
from __future__ import annotations

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Integration


async def set_integration_enabled(
    session: AsyncSession,
    *,
    integration_id: int | None = None,
    name: str | None = None,
    enabled: bool,
) -> Optional[Integration]:
    """
    Generic toggle for integrations.

    - You can target by `integration_id` OR `name`
    - Does NOT commit (caller controls transaction boundaries)
    - Flushes so the change is visible inside the same transaction
    """
    if integration_id is None and name is None:
        raise ValueError("Provide integration_id or name")

    if integration_id is not None and name is not None:
        raise ValueError("Provide only one of integration_id or name")

    if integration_id is not None:
        integ = (
            (await session.execute(select(Integration).where(Integration.id == integration_id)))
            .scalars()
            .first()
        )
    else:
        integ = (
            (await session.execute(select(Integration).where(Integration.name == name)))
            .scalars()
            .first()
        )

    if not integ:
        return None

    integ.enabled = bool(enabled)
    await session.flush()
    return integ


async def disable_integration(
    session: AsyncSession,
    *,
    integration_id: int | None = None,
    name: str | None = None,
) -> bool:
    """
    Backwards-compatible convenience wrapper expected by tests:
    `from app.integrations.services.disable import disable_integration`

    Returns True if something was disabled, False if not found.
    """
    integ = await set_integration_enabled(
        session,
        integration_id=integration_id,
        name=name,
        enabled=False,
    )
    return integ is not None
