# app/integrations/services/outbox.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...models import OutboxEvent, OutboxStatus, Integration, IntegrationType
from ..webhook import WebhookSink


async def enqueue_event(session: AsyncSession, event_type: str, payload: dict[str, Any]) -> OutboxEvent:
    ev = OutboxEvent(event_type=event_type, payload_json=json.dumps(payload), status=OutboxStatus.pending)
    session.add(ev)
    await session.flush()
    return ev


async def _build_sinks(session: AsyncSession) -> list[WebhookSink]:
    sinks: list[WebhookSink] = []
    rows = (await session.execute(select(Integration).where(Integration.enabled == True))).scalars().all()  # noqa: E712
    for integ in rows:
        if integ.type != IntegrationType.webhook:
            continue
        cfg = json.loads(integ.config_json or "{}")
        url = cfg.get("url")
        if not url:
            continue
        secret = cfg.get("secret")
        sinks.append(WebhookSink(url=url, secret=secret))
    return sinks


async def dispatch_pending_events(session: AsyncSession, batch_size: int = 50, max_attempts: int = 10) -> dict:
    sinks = await _build_sinks(session)
    if not sinks:
        return {"delivered": 0, "failed": 0, "sinks": 0, "events": 0, "skipped_no_sinks": 1}

    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.status == OutboxStatus.pending)
        .where(OutboxEvent.attempts < max_attempts)
        .order_by(OutboxEvent.id.asc())
        .limit(batch_size)
    )
    events = (await session.execute(stmt)).scalars().all()

    delivered = 0
    failed = 0

    for ev in events:
        payload = json.loads(ev.payload_json)
        ok_all = True
        last_err = None

        for sink in sinks:
            res = await sink.deliver(ev.event_type, {"event_id": ev.id, **payload})
            if not res.ok:
                ok_all = False
                last_err = res.error

        ev.attempts += 1
        ev.last_error = last_err

        if ok_all:
            ev.status = OutboxStatus.delivered
            ev.delivered_at = datetime.utcnow()
            ev.last_error = None
            delivered += 1
        else:
            # If we’ve hit max attempts, flip to failed so it stops retrying forever
            if ev.attempts >= max_attempts:
                ev.status = OutboxStatus.failed
            else:
                ev.status = OutboxStatus.pending
            failed += 1

    await session.flush()
    return {"delivered": delivered, "failed": failed, "sinks": len(sinks), "events": len(events), "skipped_no_sinks": 0}
