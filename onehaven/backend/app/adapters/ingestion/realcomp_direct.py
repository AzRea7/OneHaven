# app/adapters/ingestion/realcomp_direct.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...models import LeadSource
from ..clients.realcomp_reso import RealcompResoClient
from .base import RawLead


@dataclass
class RealcompDirectProvider:
    """Direct Realcomp ingestion (OAuth2 -> RESO Web API -> RawLead)."""

    client: RealcompResoClient

    @classmethod
    def from_settings(cls) -> "RealcompDirectProvider":
        return cls(client=RealcompResoClient())

    async def fetch(self, *, zips: list[str], max_price: float | None = None, limit_per_zip: int = 200) -> list[RawLead]:
        out: list[RawLead] = []
        for z in zips:
            items = await self.client.search_property_listings(zipcode=z, max_price=max_price, limit=limit_per_zip)
            for it in items:
                out.append(self._to_raw_lead(it))
        return out

    def _to_raw_lead(self, item: dict[str, Any]) -> RawLead:
        listing_id = (
            item.get("ListingKey")
            or item.get("ListingId")
            or item.get("ListingNumber")
            or item.get("listingId")
            or item.get("id")
        )
        if listing_id is None:
            listing_id = f"unknown::{hash(str(item))}"

        payload = dict(item)
        payload.setdefault("mls_context", {})
        payload["mls_context"].update({"provider_mode": "realcomp_direct", "mls_name": "realcomp"})
        return RawLead(source=LeadSource.realcomp_direct, source_ref=str(listing_id), payload=payload)