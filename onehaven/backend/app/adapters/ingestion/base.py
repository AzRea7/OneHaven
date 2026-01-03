# app/adapters/ingestion/base.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ...models import LeadSource


@dataclass(frozen=True)
class RawLead:
    payload: dict[str, Any]
    source: LeadSource | None = None
    source_ref: str | None = None


class IngestionProvider(Protocol):
    async def fetch(
        self,
        *,
        region: str | None,
        zips: list[str],
        city: str | None,
        per_zip_limit: int,
    ) -> list[RawLead]:
        raise NotImplementedError
