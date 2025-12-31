from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import httpx

from .base import RawLead
from ..config import settings


def _write_sample(name: str, obj: dict, max_bytes: int = 200_000) -> None:
    os.makedirs("data/rentcast_samples", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = f"data/rentcast_samples/{ts}_{name}.json"
    blob = json.dumps(obj, indent=2, default=str)[:max_bytes]
    with open(path, "w", encoding="utf-8") as f:
        f.write(blob)


def _pick(item: dict[str, Any], keys: list[str]) -> Any:
    for k in keys:
        if k in item and item.get(k) not in (None, "", []):
            return item.get(k)
    return None


def _canonicalize_listing_payload(item: dict[str, Any]) -> dict[str, Any]:
    addr = item.get("address") or {}

    payload = dict(item)

    payload["addressLine"] = (
        addr.get("addressLine")
        or addr.get("addressLine1")
        or addr.get("street")
        or item.get("addressLine")
        or item.get("formattedAddress")
        or ""
    )
    payload["city"] = addr.get("city") or item.get("city") or ""
    payload["state"] = addr.get("state") or item.get("state") or "MI"
    payload["zipCode"] = (
        addr.get("zipCode")
        or addr.get("zip")
        or item.get("zipCode")
        or item.get("zipcode")
        or ""
    )

    payload["listPrice"] = _pick(payload, ["listPrice", "price", "listingPrice"])
    payload["propertyType"] = _pick(payload, ["propertyType", "property_type", "type"])

    payload["bedrooms"] = _pick(payload, ["bedrooms", "beds"])
    payload["bathrooms"] = _pick(payload, ["bathrooms", "baths"])
    payload["squareFeet"] = _pick(payload, ["squareFeet", "sqft", "livingArea"])

    payload["latitude"] = _pick(payload, ["latitude", "lat"])
    payload["longitude"] = _pick(payload, ["longitude", "lon"])

    payload["lat"] = payload.get("latitude")
    payload["lon"] = payload.get("longitude")

    return payload


class RentCastConnector:
    def __init__(self) -> None:
        self.base = settings.RENTCAST_BASE_URL.rstrip("/")
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

        for i, item in enumerate(items):
            canon = _canonicalize_listing_payload(item)

            if not (canon.get("addressLine") and canon.get("city") and canon.get("zipCode")):
                if i < 3:
                    _write_sample(
                        f"missing_core_{zipcode}_{i}",
                        {"raw": item, "canon": canon},
                    )
                continue

            leads.append(
                RawLead(
                    source="rentcast_listing",
                    source_ref=str(item.get("id") or item.get("listingId") or ""),
                    payload=canon,
                    provenance={
                        "zip": zipcode,
                        "provider": "rentcast",
                        "endpoint": "/listings/sale",
                    },
                )
            )

        return leads
