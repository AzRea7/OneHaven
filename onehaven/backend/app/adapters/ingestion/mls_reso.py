# app/adapters/ingestion/mls_reso.py
from __future__ import annotations

from typing import Any

from ...config import settings
from ...models import LeadSource
from ..clients.reso_web_api import ResoWebApiClient
from .base import IngestionProvider, RawLead


class MlsResoProvider(IngestionProvider):
    """
    Thin adapter: RESO OData listings -> RawLead payload.
    """

    def __init__(self) -> None:
        self._client = ResoWebApiClient(base_url=settings.RESO_BASE_URL, token=settings.RESO_ACCESS_TOKEN)

    @classmethod
    def from_settings(cls) -> "MlsResoProvider":
        return cls()

    async def fetch(
        self,
        *,
        region: str | None,
        zips: list[str],
        city: str | None,
        per_zip_limit: int,
    ) -> list[RawLead]:
        out: list[RawLead] = []

        if city:
            rows = await self._client.search_property_listings(city=city, max_price=None, limit=per_zip_limit)
            out.extend(self._rows_to_raw(rows))
            return out

        for z in zips:
            rows = await self._client.search_property_listings(zipcode=z, max_price=None, limit=per_zip_limit)
            out.extend(self._rows_to_raw(rows))

        return out

    def _rows_to_raw(self, rows: list[dict[str, Any]]) -> list[RawLead]:
        out: list[RawLead] = []
        for r in rows:
            payload = dict(r)

            # Ensure common keys exist (helps refresh acceptance checks)
            payload.setdefault("addressLine", r.get("UnparsedAddress") or r.get("StreetAddress") or r.get("Address") or "")
            payload.setdefault("addressLine1", payload.get("addressLine"))
            payload.setdefault("city", r.get("City") or r.get("PostalCity") or r.get("city"))
            payload.setdefault("state", r.get("StateOrProvince") or r.get("state"))
            payload.setdefault("zipCode", r.get("PostalCode") or r.get("ZipCode") or r.get("zipCode"))

            payload.setdefault("listPrice", r.get("ListPrice") or r.get("listPrice"))
            payload.setdefault("propertyType", r.get("PropertyType") or r.get("PropertySubType") or r.get("propertyType"))

            source_ref = str(r.get("ListingId") or r.get("ListingKey") or "")
            out.append(RawLead(payload=payload, source=LeadSource.mls_reso, source_ref=source_ref))
        return out
