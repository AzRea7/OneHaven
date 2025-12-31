from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import httpx

from .base import RawLead
from ..config import settings


def _write_sample(name: str, obj: dict, max_bytes: int = 200_000) -> None:
    """
    Writes a small number of diagnostic JSON samples for payloads that fail
    core-field requirements (addressLine/city/zipCode).

    This is intentionally local-only and should be gitignored (data/).
    """
    os.makedirs("data/rentcast_samples", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = f"data/rentcast_samples/{ts}_{name}.json"
    blob = json.dumps(obj, indent=2, default=str)[:max_bytes]
    with open(path, "w", encoding="utf-8") as f:
        f.write(blob)


def _canonicalize_listing_payload(item: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize RentCast listing payload into the canonical keys used by ingestion:
      addressLine, city, state, zipCode, listPrice, propertyType

    Handles common patterns:
      - item["address"] can be dict or string
      - address fields might be flattened or nested
      - list price/property type keys vary by endpoint/version
    """
    address_line = ""
    city = ""
    state = "MI"
    zipc = ""

    addr = item.get("address")
    if isinstance(addr, dict):
        address_line = (
            addr.get("addressLine")
            or addr.get("addressLine1")
            or addr.get("street")
            or addr.get("line1")
            or ""
        )
        city = addr.get("city") or ""
        state = addr.get("state") or state
        zipc = addr.get("zipCode") or addr.get("zip") or addr.get("zipcode") or ""
    elif isinstance(addr, str):
        # Sometimes formatted full address string appears here.
        address_line = addr

    # Fallbacks (flattened or alternative keys)
    address_line = (
        address_line
        or item.get("addressLine")
        or item.get("addressLine1")
        or item.get("street")
        or item.get("formattedAddress")
        or item.get("formatted_address")
        or ""
    )
    city = city or item.get("city") or item.get("addressCity") or item.get("address_city") or ""
    state = item.get("state") or item.get("addressState") or item.get("address_state") or state
    zipc = zipc or item.get("zipCode") or item.get("zip") or item.get("zipcode") or item.get("addressZip") or ""

    payload: dict[str, Any] = dict(item)
    payload["addressLine"] = (address_line or "").strip()
    payload["city"] = (city or "").strip()
    payload["state"] = (state or "MI").strip()
    payload["zipCode"] = (zipc or "").strip()

    # Normalize list price field
    if "listPrice" not in payload or payload.get("listPrice") in (None, ""):
        payload["listPrice"] = (
            payload.get("price")
            or payload.get("listingPrice")
            or payload.get("list_price")
        )

    # Normalize property type
    if "propertyType" not in payload or payload.get("propertyType") in (None, ""):
        payload["propertyType"] = (
            payload.get("property_type")
            or payload.get("PropertyType")
            or payload.get("type")
        )

    return payload


class RentCastConnector:
    def __init__(self) -> None:
        self.base = settings.RENTCAST_BASE_URL.rstrip("/")
        self.key = settings.RENTCAST_API_KEY

    async def fetch_listings(self, zipcode: str, limit: int = 200) -> list[RawLead]:
        """
        Fetch on-market sale listings for a zip.

        Produces RawLead objects with:
          source="rentcast_listing"
          payload normalized to canonical keys
          provenance with zip/provider
        """
        if not self.key:
            return []

        headers = {"X-Api-Key": self.key}
        url = f"{self.base}/listings/sale"
        params = {"zipCode": zipcode, "limit": limit}

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()

        # RentCast endpoints sometimes return list; sometimes dict with "listings" or "results"
        items = data if isinstance(data, list) else data.get("listings", data.get("results", []))

        leads: list[RawLead] = []
        sampled_missing = 0

        for item in items:
            canon = _canonicalize_listing_payload(item)

            # If still missing, snapshot a few raw items so mapping fixes are evidence-based
            if not (canon.get("addressLine") and canon.get("city") and canon.get("zipCode")):
                if sampled_missing < 3:
                    _write_sample(
                        f"missing_{zipcode}_{sampled_missing}",
                        {"raw": item, "canon": canon, "endpoint": url, "params": params},
                    )
                    sampled_missing += 1

            leads.append(
                RawLead(
                    source="rentcast_listing",
                    source_ref=str(item.get("id") or item.get("listingId") or item.get("propertyId") or ""),
                    payload=canon,
                    provenance={"zip": zipcode, "provider": "rentcast", "endpoint": "/listings/sale"},
                )
            )

        return leads
