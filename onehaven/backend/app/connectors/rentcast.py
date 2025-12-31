import httpx
from .base import RawLead
from ..config import settings


def _canonicalize_listing_payload(item: dict) -> dict:
    """
    Map provider-specific keys into the keys our ingestion expects:
      address/addressLine/street, city, state, zip/zipcode/zipCode,
      propertyType/property_type, bedrooms/bathrooms/squareFeet, price/listPrice
    """
    # Handle possible nested address objects
    addr = item.get("address") or {}
    if isinstance(addr, dict):
        address_line = (
            addr.get("addressLine")
            or addr.get("addressLine1")
            or addr.get("street")
            or addr.get("line1")
            or ""
        )
        city = addr.get("city") or item.get("city") or ""
        state = addr.get("state") or item.get("state") or "MI"
        zipc = addr.get("zipCode") or addr.get("zip") or item.get("zipCode") or item.get("zip") or ""
    else:
        address_line = item.get("addressLine") or item.get("street") or item.get("formattedAddress") or item.get("address") or ""
        city = item.get("city") or ""
        state = item.get("state") or "MI"
        zipc = item.get("zipCode") or item.get("zip") or item.get("zipcode") or ""

    payload = dict(item)
    payload.setdefault("addressLine", address_line)
    payload.setdefault("city", city)
    payload.setdefault("state", state)
    payload.setdefault("zipCode", zipc)

    # Normalize common fields
    if "listPrice" not in payload and "price" in payload:
        payload["listPrice"] = payload.get("price")

    # Make sure property type key exists (even if None)
    if "propertyType" not in payload and "property_type" in payload:
        payload["propertyType"] = payload.get("property_type")

    return payload


class RentCastConnector:
    """
    Minimal stub. Replace endpoint paths with whatever your plan/product supports.
    """

    def __init__(self) -> None:
        self.base = settings.RENTCAST_BASE_URL
        self.key = settings.RENTCAST_API_KEY

    async def fetch_listings(self, zipcode: str, limit: int = 200) -> list[RawLead]:
        if not self.key:
            return []

        headers = {"X-Api-Key": self.key}
        url = f"{self.base}/listings/sale"
        params = {"zipCode": zipcode, "limit": limit}

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()

        items = data if isinstance(data, list) else data.get("listings", [])
        leads: list[RawLead] = []

        for item in items:
            canon = _canonicalize_listing_payload(item)
            leads.append(
                RawLead(
                    source="rentcast_listing",
                    source_ref=str(item.get("id") or item.get("listingId") or ""),
                    payload=canon,
                    provenance={"zip": zipcode, "provider": "rentcast"},
                )
            )
        return leads
