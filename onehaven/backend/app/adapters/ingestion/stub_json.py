# app/adapters/ingestion/stub_json.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...config import settings
from .base import IngestionProvider, RawLead


def _as_list_of_dicts(payload: Any) -> list[dict[str, Any]]:
    """
    Accept either:
      - list[dict]
      - {"value": list[dict]} (common RESO/OData shape)
    """
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        v = payload.get("value")
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
    return []


def _coerce_str(x: Any) -> str | None:
    if x is None:
        return None
    try:
        s = str(x).strip()
        return s if s else None
    except Exception:
        return None


def _coerce_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


@dataclass
class StubJsonProvider(IngestionProvider):
    """
    Offline ingestion provider for development/testing.

    Reads listing payloads from fixtures:
      backend/data/stub_listings/<zip>.json

    The fixture can be:
      - list of dict payloads
      - {"value": [dict, ...]} (RESO-like)
    """

    fixtures_dir: Path

    @classmethod
    def from_settings(cls) -> "StubJsonProvider":
        # uvicorn is typically launched from backend/, so this is backend/data/stub_listings
        return cls(fixtures_dir=Path("data") / "stub_listings")

    async def fetch(
        self,
        *,
        region: str | None = None,
        zips: list[str] | None = None,
        city: str | None = None,
        per_zip_limit: int = 200,
        **_: Any,
    ) -> list[RawLead]:
        """
        Matches the contract expected by refresh_region_use_case().

        Returns: list[RawLead] where RawLead.payload is already in your canonical payload schema
        (or close enough for PropertyRepository.upsert_from_payload()).
        """
        target_zips = zips or []
        out: list[RawLead] = []

        for z in target_zips:
            path = self.fixtures_dir / f"{z}.json"
            if not path.exists():
                # Dev-friendly: missing fixture zip means "no listings"
                continue

            raw = json.loads(path.read_text(encoding="utf-8"))
            items = _as_list_of_dicts(raw)

            # optional city filter for fixtures
            if city:
                city_l = city.strip().lower()
                items = [
                    it
                    for it in items
                    if str(it.get("city") or it.get("City") or "").strip().lower() == city_l
                ]

            for it in items[: int(per_zip_limit)]:
                payload = self._canonicalize(it, fallback_zip=z)
                out.append(RawLead(payload=payload, source="stub_json", source_ref=str(payload.get("listingId") or "")))

        return out

    # -------------------------
    # Canonicalization
    # -------------------------

    def _canonicalize(self, it: dict[str, Any], *, fallback_zip: str) -> dict[str, Any]:
        """
        Convert fixture/RESO-ish items into your canonical ingestion payload shape:
          addressLine, city, state, zipCode, listPrice, propertyType, beds, baths, sqft, latitude, longitude, listingId
        """
        # Allow both "pretty" fixture keys and RESO-ish keys
        address = (
            it.get("addressLine")
            or it.get("addressLine1")
            or it.get("UnparsedAddress")
            or it.get("StreetNumber")  # sometimes split; ok if absent
        )
        city = it.get("city") or it.get("City")
        state = it.get("state") or it.get("StateOrProvince") or it.get("State")
        zip_code = it.get("zipCode") or it.get("PostalCode") or fallback_zip

        # price keys vary a lot
        list_price = (
            it.get("listPrice")
            or it.get("ListPrice")
            or it.get("price")
            or it.get("Price")
        )

        beds = it.get("beds") or it.get("BedroomsTotal") or it.get("Bedrooms")
        baths = it.get("baths") or it.get("BathroomsTotalInteger") or it.get("BathroomsTotal")
        sqft = it.get("sqft") or it.get("LivingArea") or it.get("BuildingAreaTotal") or it.get("SquareFeet")

        lat = it.get("latitude") or it.get("Latitude")
        lon = it.get("longitude") or it.get("Longitude")

        prop_type = it.get("propertyType") or it.get("PropertyType") or it.get("PropertySubType")

        listing_id = (
            it.get("listingId")
            or it.get("ListingKey")
            or it.get("ListingId")
            or it.get("id")
        )

        # Coerce into your expected types where safe
        payload: dict[str, Any] = {
            "addressLine": _coerce_str(address),
            "city": _coerce_str(city),
            "state": _coerce_str(state),
            "zipCode": _coerce_str(zip_code),
            "listPrice": _coerce_float(list_price),
            "propertyType": _coerce_str(prop_type),
            "beds": _coerce_float(beds),
            "baths": _coerce_float(baths),
            "sqft": _coerce_float(sqft),
            "latitude": _coerce_float(lat),
            "longitude": _coerce_float(lon),
            "listingId": _coerce_str(listing_id) or f"stub::{fallback_zip}::{hash(json.dumps(it, default=str))}",
            # keep the original around for debugging / future canonicalization evolution
            "raw": it,
        }

        return payload
