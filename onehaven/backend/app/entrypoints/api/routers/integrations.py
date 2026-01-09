# onehaven/backend/app/entrypoints/api/routers/integrations.py
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ....models import Integration, IntegrationType
from ....schemas import IntegrationCreate, IntegrationOut
from ..deps import get_session, require_api_key

router = APIRouter(tags=["integrations"])

@router.post("/integrations", response_model=IntegrationOut, dependencies=[Depends(require_api_key)])
async def create_integration(
    body: IntegrationCreate,
    session: AsyncSession = Depends(get_session),
) -> IntegrationOut:
    if body.type != "webhook":
        raise HTTPException(status_code=400, detail="Only webhook integrations supported in v0")

    existing = (await session.execute(select(Integration).where(Integration.name == body.name))).scalars().first()
    if existing:
        raise HTTPException(
            status_code=409,
            detail="Integration name already exists. Use PATCH to update/disable.",
        )

    cfg = {"url": body.url, "secret": body.secret}
    integ = Integration(
        name=body.name,
        type=IntegrationType.webhook,
        enabled=body.enabled,
        config_json=json.dumps(cfg),
    )
    session.add(integ)
    await session.commit()

    return IntegrationOut(
        id=integ.id,
        name=integ.name,
        type=integ.type.value,
        enabled=integ.enabled,
        created_at=integ.created_at,
    )

@router.patch("/integrations/{integration_id}", response_model=IntegrationOut, dependencies=[Depends(require_api_key)])
async def update_integration(
    integration_id: int,
    enabled: bool | None = None,
    url: str | None = None,
    secret: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> IntegrationOut:
    integ = (await session.execute(select(Integration).where(Integration.id == integration_id))).scalars().first()
    if not integ:
        raise HTTPException(status_code=404, detail="Integration not found")

    if enabled is not None:
        integ.enabled = bool(enabled)

    if integ.type == IntegrationType.webhook and (url is not None or secret is not None):
        cfg = json.loads(integ.config_json or "{}")
        if url is not None:
            cfg["url"] = url
        if secret is not None:
            cfg["secret"] = secret
        integ.config_json = json.dumps(cfg)

    await session.commit()

    return IntegrationOut(
        id=integ.id,
        name=integ.name,
        type=integ.type.value,
        enabled=integ.enabled,
        created_at=integ.created_at,
    )

@router.get("/integrations", response_model=list[IntegrationOut], dependencies=[Depends(require_api_key)])
async def list_integrations(
    session: AsyncSession = Depends(get_session),
) -> list[IntegrationOut]:
    rows = (await session.execute(select(Integration).order_by(Integration.id.asc()))).scalars().all()
    return [
        IntegrationOut(
            id=i.id,
            name=i.name,
            type=i.type.value,
            enabled=i.enabled,
            created_at=i.created_at,
        )
        for i in rows
    ]
