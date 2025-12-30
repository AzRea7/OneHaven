from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol, Any


@dataclass(frozen=True)
class SinkDeliveryResult:
    ok: bool
    error: str | None = None


class LeadSink(Protocol):
    async def deliver(self, event_type: str, payload: dict[str, Any]) -> SinkDeliveryResult:
        ...
