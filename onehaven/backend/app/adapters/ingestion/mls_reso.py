# app/adapters/ingestion/mls_reso.py
from __future__ import annotations

from typing import Any

from ..clients.reso_web_api import ResoWebApiClient
from ...models import LeadSource
from .base import IngestionProvider, RawLead


class MlsResoProvider(IngestionProvider):
    """
    Thin adapter: RESO OData listings -> canonical RawLead payload.
    """

    def __init__(self) -> None:
        self._client = ResoWebApiClient()

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
            rows = await self._client.query_listings(city=city, top=per_zip_limit)
            out.extend(self._rows_to_raw(rows))
            return out

        for z in zips:
            rows = await self._client.query_listings(zipcode=z, top=per_zip_limit)
            out.extend(self._rows_to_raw(rows))

        return out

    def _rows_to_raw(self, rows: list[dict[str, Any]]) -> list[RawLead]:
        out: list[RawLead] = []
        for r in rows:
            payload = {
                "addressLine": r.get("UnparsedAddress") or r.get("StreetAddress") or r.get("AddressLine1"),
                "city": r.get("City"),
                "state": r.get("StateOrProvince"),
                "zipCode": r.get("PostalCode"),
                "listPrice": r.get("ListPrice"),
                "bedrooms": r.get("BedroomsTotal"),
                "bathrooms": r.get("BathroomsTotalInteger") or r.get("BathroomsTotalDecimal"),
                "sqft": r.get("LivingArea"),
                "propertyType": r.get("PropertyType"),
                "latitude": r.get("Latitude"),
                "longitude": r.get("Longitude"),
                "reso": {"raw": r},
            }
            source_ref = str(r.get("ListingId") or r.get("ListingKey") or "")
            out.append(RawLead(payload=payload, source=LeadSource.mls_reso, source_ref=source_ref))
        return out
