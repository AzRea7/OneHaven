# app/adapters/ingestion/mls_grid.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...config import settings
from ...models import LeadSource
from ..clients.reso_web_api import ResoWebApiClient
from .base import IngestionProvider, RawLead


@dataclass
class MlsGridProvider(IngestionProvider):
    """
    "MLS Grid mode" ingestion.

    IMPORTANT:
    - This is just a mode label + a RESO Web API client usage.
    - It does NOT grant MLS rights. You must point RESO_BASE_URL + RESO_ACCESS_TOKEN
      to an endpoint you are licensed to access.
    """

    client: ResoWebApiClient

    @classmethod
    def from_settings(cls) -> "MlsGridProvider":
        return cls(client=ResoWebApiClient(base_url=settings.RESO_BASE_URL, token=settings.RESO_ACCESS_TOKEN))

    async def fetch(
        self,
        *,
        region: str | None,
        zips: list[str],
        city: str | None,
        per_zip_limit: int,
    ) -> list[RawLead]:
        out: list[RawLead] = []

        # Prefer city query if provided (optional)
        if city:
            items = await self.client.search_property_listings(city=city, max_price=None, limit=per_zip_limit)
            for it in items:
                out.append(self._to_raw_lead(it))
            return out

        for z in zips:
            items = await self.client.search_property_listings(zipcode=z, max_price=None, limit=per_zip_limit)
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
        ) or f"unknown::{hash(str(item))}"

        payload = dict(item)
        payload.setdefault("mls_context", {})
        payload["mls_context"].update(
            {
                "provider_mode": "mls_grid",
                "mls_name": settings.MLS_PRIMARY_NAME,  # e.g. "realcomp" or "michric"
            }
        )

        # Ensure these common keys exist for downstream checks
        # (doesn't overwrite if the upstream already has them)
        payload.setdefault("addressLine", payload.get("UnparsedAddress") or payload.get("StreetAddress") or "")
        payload.setdefault("addressLine1", payload.get("addressLine"))
        payload.setdefault("zipCode", payload.get("PostalCode") or payload.get("ZipCode") or payload.get("zipCode"))
        payload.setdefault("listPrice", payload.get("ListPrice") or payload.get("listPrice"))
        payload.setdefault("propertyType", payload.get("PropertyType") or payload.get("PropertySubType") or payload.get("propertyType"))

        return RawLead(payload=payload, source=LeadSource.mls_grid, source_ref=str(listing_id))

