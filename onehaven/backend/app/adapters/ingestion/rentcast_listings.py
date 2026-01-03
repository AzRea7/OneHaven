# app/adapters/ingestion/rentcast_listings.py
from __future__ import annotations

from typing import Any

from ..clients.rentcast_listings import RentCastConnector
from ...models import LeadSource
from .base import IngestionProvider, RawLead


class RentCastListingsProvider(IngestionProvider):
    def __init__(self) -> None:
        self._rc = RentCastConnector()

    async def fetch(
        self,
        *,
        region: str | None,
        zips: list[str],
        city: str | None,
        per_zip_limit: int,
    ) -> list[RawLead]:
        # RentCast ingestion is zip-driven; ignore city/region here.
        out: list[RawLead] = []

        for zipcode in zips:
            listings = await self._rc.fetch_listings(zipcode, limit=per_zip_limit)
            for payload in listings:
                payload = payload or {}
                # Ensure canonical-ish keys exist if RentCast uses different casing
                # (Your pipeline already normalizes address fields, but keep stable access)
                source_ref = str(payload.get("id") or payload.get("listingId") or payload.get("ListingId") or "")
                out.append(
                    RawLead(
                        payload=payload,
                        source=LeadSource.rentcast_listing,
                        source_ref=source_ref,
                    )
                )

        return out
