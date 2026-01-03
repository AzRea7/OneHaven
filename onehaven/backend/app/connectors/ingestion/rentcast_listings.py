# app/connectors/ingestion/rentcast_listings.py
from __future__ import annotations

from typing import AsyncIterator

from .base import IngestionAdapter, RawLead
from ..rentcast import RentCastConnector


class RentCastListingsAdapter(IngestionAdapter):
    def __init__(self) -> None:
        self.rc = RentCastConnector()

    async def iter_sale_listings(
        self,
        *,
        zipcode: str,
        per_zip_limit: int = 200,
        max_price: float | None = None,
        city: str | None = None,
    ) -> AsyncIterator[RawLead]:
        rows = await self.rc.fetch_sale_listings(zipcode=zipcode, limit=per_zip_limit, max_price=max_price)
        for r in rows:
            yield RawLead(
                address_line=str(r.get("addressLine") or r.get("address") or "").strip(),
                city=str(r.get("city") or "").strip(),
                state=str(r.get("state") or "").strip(),
                zipcode=str(r.get("zipCode") or r.get("zipcode") or "").strip(),
                list_price=float(r.get("price") or r.get("listPrice") or 0) or None,
                bedrooms=r.get("bedrooms"),
                bathrooms=r.get("bathrooms"),
                sqft=r.get("squareFootage") or r.get("sqft"),
                year_built=r.get("yearBuilt"),
                property_type=r.get("propertyType"),
                lat=r.get("latitude"),
                lon=r.get("longitude"),
                source="rentcast",
                source_ref=str(r.get("id") or r.get("listingId") or ""),
                provenance=r if isinstance(r, dict) else None,
            )
