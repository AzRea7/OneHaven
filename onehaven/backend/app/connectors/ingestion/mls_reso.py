# app/connectors/ingestion/mls_reso.py
from __future__ import annotations

from typing import AsyncIterator, Any

from .base import IngestionAdapter, RawLead
from ..reso_web_api import ResoWebApiClient


class MlsResoListingsAdapter(IngestionAdapter):
    def __init__(self, client: ResoWebApiClient) -> None:
        self.client = client

    async def iter_sale_listings(
        self,
        *,
        zipcode: str,
        per_zip_limit: int = 200,
        max_price: float | None = None,
        city: str | None = None,
    ) -> AsyncIterator[RawLead]:
        # Example OData query style; adjust for your MLS Grid endpoint conventions.
        # You should implement a method in ResoWebApiClient like query_listings(...)
        data = await self.client.query_listings(
            zipcode=zipcode,
            top=per_zip_limit,
            max_price=max_price,
            city=city,
        )

        # Common RESO fields (vary by feed): UnparsedAddress, City, StateOrProvince, PostalCode, ListPrice, BedroomsTotal...
        for item in data:
            yield RawLead(
                address_line=str(item.get("UnparsedAddress") or item.get("StreetAddress") or "").strip(),
                city=str(item.get("City") or "").strip(),
                state=str(item.get("StateOrProvince") or "").strip(),
                zipcode=str(item.get("PostalCode") or "").strip(),
                list_price=_to_float(item.get("ListPrice")),
                bedrooms=_to_float(item.get("BedroomsTotal")),
                bathrooms=_to_float(item.get("BathroomsTotalInteger") or item.get("BathroomsTotal")),
                sqft=_to_float(item.get("LivingArea") or item.get("BuildingAreaTotal")),
                year_built=_to_int(item.get("YearBuilt")),
                property_type=str(item.get("PropertyType") or item.get("PropertySubType") or ""),
                lat=_to_float(item.get("Latitude")),
                lon=_to_float(item.get("Longitude")),
                source="mls_reso",
                source_ref=str(item.get("ListingKey") or item.get("ListingId") or ""),
                provenance=item if isinstance(item, dict) else None,
            )


def _to_float(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _to_int(v: Any) -> int | None:
    try:
        if v is None or v == "":
            return None
        return int(float(v))
    except Exception:
        return None
