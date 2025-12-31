# onehaven/backend/app/connectors/rentcast.py
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
    Writes diagnostic JSON samples for payloads that fail core-field requirements
    (or for investigating missing sqft/lat/lon key mapping).
    Local-only; keep data/ gitignored.
    """
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
    """
    Normalize RentCast listing payload into canonical keys used across ingestion:
      addressLine, city, state, zipCode,
      listPrice, propertyType,
      bedrooms, bathrooms, squareFeet,
      latitude, longitude

    Why be aggressive here?
    Because your data-quality gates + scoring depend on these fields.
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
        payload["listPrice"] = _pick(payload, ["price", "listingPrice", "list_price", "ListPrice"])

    # Normalize property type
    if "propertyType" not in payload or payload.get("propertyType") in (None, ""):
        payload["propertyType"] = _pick(payload, ["property_type", "PropertyType", "type", "propertyType"])

    # Normalize beds/baths/sqft — these are the fields currently missing in your quality output
    # (your seed_demo uses bedrooms/bathrooms/squareFeet; we make RentCast match that)
    if "bedrooms" not in payload or payload.get("bedrooms") in (None, ""):
        payload["bedrooms"] = _pick(payload, ["bedrooms", "beds", "BedroomsTotal", "Bedrooms"])

    if "bathrooms" not in payload or payload.get("bathrooms") in (None, ""):
        payload["bathrooms"] = _pick(payload, ["bathrooms", "baths", "BathroomsTotal", "BathroomsTotalInteger"])

    if "squareFeet" not in payload or payload.get("squareFeet") in (None, ""):
        payload["squareFeet"] = _pick(payload, ["squareFeet", "sqft", "livingArea", "LivingArea"])

    # Normalize lat/lon — store in both common variants to maximize compatibility
    lat = _pick(payload, ["latitude", "lat", "Latitude"])
    lon = _pick(payload, ["longitude", "lon", "Longitude"])

    if "latitude" not in payload or payload.get("latitude") in (None, ""):
        payload["latitude"] = lat
    if "longitude" not in payload or payload.get("longitude") in (None, ""):
        payload["longitude"] = lon

    # Also copy into short keys some systems use
    if "lat" not in payload or payload.get("lat") in (None, ""):
        payload["lat"] = lat
    if "lon" not in payload or payload.get("lon") in (None, ""):
        payload["lon"] = lon

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

        items = data if isinstance(data, list) else data.get("listings", data.get("results", []))

        leads: list[RawLead] = []
        sampled_missing_core = 0
        sampled_missing_geo = 0
        sampled_missing_sqft = 0

        for item in items:
            canon = _canonicalize_listing_payload(item)

            # If still missing core address fields, sample a few for debugging
            if not (canon.get("addressLine") and canon.get("city") and canon.get("zipCode")):
                if sampled_missing_core < 3:
                    _write_sample(
                        f"missing_core_{zipcode}_{sampled_missing_core}",
                        {"raw": item, "canon": canon, "endpoint": url, "params": params},
                    )
                    sampled_missing_core += 1

            # Optional: sample missing sqft/geo, since those are your current pain points
            if canon.get("squareFeet") in (None, "") and sampled_missing_sqft < 2:
                _write_sample(
                    f"missing_sqft_{zipcode}_{sampled_missing_sqft}",
                    {"raw": item, "canon": canon},
                )
                sampled_missing_sqft += 1

            if (canon.get("latitude") in (None, "") or canon.get("longitude") in (None, "")) and sampled_missing_geo < 2:
                _write_sample(
                    f"missing_geo_{zipcode}_{sampled_missing_geo}",
                    {"raw": item, "canon": canon},
                )
                sampled_missing_geo += 1

            leads.append(
                RawLead(
                    source="rentcast_listing",
                    source_ref=str(item.get("id") or item.get("listingId") or item.get("propertyId") or ""),
                    payload=canon,
                    provenance={"zip": zipcode, "provider": "rentcast", "endpoint": "/listings/sale"},
                )
            )

        return leads
