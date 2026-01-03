# app/service_layer/demo_seed.py
from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Integration, IntegrationType


async def seed_demo(
    session: AsyncSession,
    *,
    url: str = "https://example.com",
    enable_demo_webhooks: bool = False,
) -> dict[str, Any]:
    """
    Idempotent demo seed:
    - creates/updates two webhook integrations
    - safe to run multiple times
    """
    async def upsert(name: str, enabled: bool) -> None:
        row = (await session.execute(select(Integration).where(Integration.name == name))).scalars().first()
        cfg = {"url": url, "secret": None}

        if row:
            row.type = IntegrationType.webhook
            row.enabled = enabled
            row.config_json = json.dumps(cfg)
        else:
            session.add(
                Integration(
                    name=name,
                    type=IntegrationType.webhook,
                    enabled=enabled,
                    config_json=json.dumps(cfg),
                )
            )
        await session.flush()

    await upsert("demo_webhook_1", enable_demo_webhooks)
    await upsert("demo_webhook_2", False)

    return {"seeded": 2, "enable_demo_webhooks": enable_demo_webhooks, "url": url}
