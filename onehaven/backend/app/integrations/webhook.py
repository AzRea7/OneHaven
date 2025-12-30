from __future__ import annotations

import hmac
import hashlib
import json
from typing import Any

import httpx

from .base import LeadSink, SinkDeliveryResult


class WebhookSink(LeadSink):
    def __init__(self, url: str, secret: str | None = None, timeout_s: int = 20) -> None:
        self.url = url
        self.secret = secret
        self.timeout_s = timeout_s

    def _sign(self, body: bytes) -> str | None:
        if not self.secret:
            return None
        digest = hmac.new(self.secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        return digest

    async def deliver(self, event_type: str, payload: dict[str, Any]) -> SinkDeliveryResult:
        body = json.dumps({"type": event_type, "data": payload}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        sig = self._sign(body)
        if sig:
            headers["X-Haven-Signature"] = sig

        try:
            async with httpx.AsyncClient(timeout=self.timeout_s) as client:
                r = await client.post(self.url, content=body, headers=headers)
                if 200 <= r.status_code < 300:
                    return SinkDeliveryResult(ok=True)
                return SinkDeliveryResult(ok=False, error=f"HTTP {r.status_code}: {r.text[:500]}")
        except Exception as e:
            return SinkDeliveryResult(ok=False, error=str(e))
