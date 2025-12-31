from __future__ import annotations

import asyncio
import json
import os
import random
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from ...models import OutboxEvent, OutboxStatus, Integration, IntegrationType
from ..webhook import WebhookSink


# Tuning knobs (env-overridable)
_OUTBOX_BATCH_SIZE = int(os.getenv("OUTBOX_BATCH_SIZE", "50"))
_OUTBOX_MAX_ATTEMPTS = int(os.getenv("OUTBOX_MAX_ATTEMPTS", "10"))

# Global delivery pacing. If you have N sinks enabled, you will do at most ~RPS requests per second *per process*.
# Default conservative so you don't accidentally become the villain in someone else's logs.
_OUTBOX_WEBHOOK_RPS = float(os.getenv("OUTBOX_WEBHOOK_RPS", "2.0"))

# Backoff configuration
_BACKOFF_BASE_SECONDS = float(os.getenv("OUTBOX_BACKOFF_BASE_SECONDS", "5.0"))
_BACKOFF_CAP_SECONDS = float(os.getenv("OUTBOX_BACKOFF_CAP_SECONDS", "3600.0"))  # 1 hour cap


async def enqueue_event(session: AsyncSession, event_type: str, payload: dict[str, Any]) -> OutboxEvent:
    ev = OutboxEvent(
        event_type=event_type,
        payload_json=json.dumps(payload),
        status=OutboxStatus.pending,
        attempts=0,
        last_error=None,
        next_attempt_at=None,
    )
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


def _compute_backoff_seconds(attempts_after_increment: int) -> float:
    """
    Exponential backoff with jitter.
    attempts_after_increment: 1,2,3,... (after we increment attempts)
    """
    exp = _BACKOFF_BASE_SECONDS * (2 ** max(0, attempts_after_increment - 1))
    capped = min(exp, _BACKOFF_CAP_SECONDS)
    jitter = random.uniform(0.0, min(_BACKOFF_BASE_SECONDS, capped))
    return capped + jitter


async def dispatch_pending_events(
    session: AsyncSession,
    batch_size: int | None = None,
    max_attempts: int | None = None,
    rps: float | None = None,
) -> dict[str, Any]:
    """
    Dispatch outbox events to enabled sinks.
    Quiet-by-default:
      - If there are no enabled sinks, returns immediately without doing any HTTP calls.
    Reliability:
      - Exponential backoff + jitter on failures, stored in next_attempt_at.
      - Marks as failed if max attempts reached.
      - Rate-limits webhook delivery.
    """
    batch_size = batch_size or _OUTBOX_BATCH_SIZE
    max_attempts = max_attempts or _OUTBOX_MAX_ATTEMPTS
    rps = rps or _OUTBOX_WEBHOOK_RPS

    sinks = await _build_sinks(session)
    if not sinks:
        return {"delivered": 0, "failed": 0, "sinks": 0, "events": 0, "skipped_no_sinks": 1}

    now = datetime.utcnow()

    stmt = (
        select(OutboxEvent)
        .where(OutboxEvent.status == OutboxStatus.pending)
        .where(OutboxEvent.attempts < max_attempts)
        .where(or_(OutboxEvent.next_attempt_at.is_(None), OutboxEvent.next_attempt_at <= now))
        .order_by(OutboxEvent.id.asc())
        .limit(batch_size)
    )
    events = (await session.execute(stmt)).scalars().all()

    delivered = 0
    failed = 0

    # Convert RPS into per-request delay
    delay = 0.0 if rps <= 0 else (1.0 / rps)

    for ev in events:
        payload = json.loads(ev.payload_json)

        ok_all = True
        last_err = None

        for sink in sinks:
            res = await sink.deliver(ev.event_type, {"event_id": ev.id, **payload})
            if delay > 0:
                await asyncio.sleep(delay)

            if not res.ok:
                ok_all = False
                last_err = res.error

        # update attempt accounting
        ev.attempts += 1
        ev.last_error = last_err

        if ok_all:
            ev.status = OutboxStatus.delivered
            ev.delivered_at = datetime.utcnow()
            ev.next_attempt_at = None
            delivered += 1
        else:
            if ev.attempts >= max_attempts:
                ev.status = OutboxStatus.failed
                ev.next_attempt_at = None
                failed += 1
            else:
                backoff_s = _compute_backoff_seconds(ev.attempts)
                ev.next_attempt_at = datetime.utcnow() + timedelta(seconds=backoff_s)

        await session.flush()

    return {
        "delivered": delivered,
        "failed": failed,
        "sinks": len(sinks),
        "events": len(events),
        "skipped_no_sinks": 0,
    }
