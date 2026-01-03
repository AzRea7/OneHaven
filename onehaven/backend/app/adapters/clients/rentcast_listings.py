# app/adapters/clients/rentcast_listings.py
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import httpx

from ...config import settings


def _write_sample(prefix: str, sample: Any) -> None:
    """
    Used by tests/debugging to snapshot sample payloads.
    Safe for both dict and list payloads.
    """
    os.makedirs("data/rentcast_samples", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    path = os.path.join("data/rentcast_samples", f"{prefix}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=2, default=str)


def _canonicalize_listing_payload(item: dict[str, Any]) -> dict[str, Any]:
    payload = dict(item)

    payload["addressLine"] = (
        item.get("addressLine")
        or item.get("addressLine1")
        or item.get("formattedAddress")
        or ""
    )
    payload["listPrice"] = item.get("listPrice") or item.get("price")
    payload["sqft"] = item.get("sqft") or item.get("squareFootage")

    payload["lat"] = item.get("lat") or item.get("latitude")
    payload["lon"] = item.get("lon") or item.get("longitude")

    return payload


class RentCastConnector:
    """
    Low-level HTTP client for RentCast LISTINGS (sale listings ingestion).
    Returns list[dict] (raw-ish payloads), not RawLead.
    """

    def __init__(self) -> None:
        self._api_key = settings.RENTCAST_API_KEY or None
        self._base_url = (settings.RENTCAST_BASE_URL or "").rstrip("/")

    def _headers(self) -> dict[str, str]:
        if not self._api_key:
            raise RuntimeError("RENTCAST_API_KEY is not set")
        # IMPORTANT: RentCast expects X-Api-Key (this exact casing works reliably)
        return {"accept": "application/json", "X-Api-Key": self._api_key}

    def _build_url(self, path_without_version: str) -> str:
        """
        Works whether RENTCAST_BASE_URL is:
          - https://api.rentcast.io
          - https://api.rentcast.io/v1
        """
        base = self._base_url
        p = "/" + path_without_version.lstrip("/")

        if base.endswith("/v1"):
            return f"{base}{p}"
        return f"{base}/v1{p}"

    async def fetch_listings(self, zipcode: str, limit: int = 200) -> list[dict[str, Any]]:
        if not self._api_key:
            return []

        url = self._build_url("/listings/sale")
        params = {"zipCode": zipcode, "limit": limit}

        # --- DEBUG (prints once per call, redacted) ---
        headers = self._headers()
        safe = dict(headers)
        if "X-Api-Key" in safe:
            safe["X-Api-Key"] = f"<redacted len={len(headers['X-Api-Key'])}>"
        if "X-API-Key" in safe:
            safe["X-API-Key"] = f"<redacted len={len(headers['X-API-Key'])}>"

        print("RentCast DEBUG url:", url)
        print("RentCast DEBUG params:", params)
        print("RentCast DEBUG headers:", safe)
        # --------------------------------------------

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.get(url, headers=headers, params=params)
                r.raise_for_status()
                data = r.json()
        except httpx.HTTPStatusError as e:
            try:
                _write_sample(
                    "rentcast_listings_http_error",
                    {
                        "url": url,
                        "params": params,
                        "status": e.response.status_code,
                        "body": e.response.text,
                    },
                )
            except Exception:
                pass
            raise
        except Exception as e:
            try:
                _write_sample(
                    "rentcast_listings_exception",
                    {"url": url, "params": params, "error": repr(e)},
                )
            except Exception:
                pass
            raise

        rows: list[dict[str, Any]] = []
        if isinstance(data, dict) and isinstance(data.get("listings"), list):
            rows = [x for x in data["listings"] if isinstance(x, dict)]
        elif isinstance(data, list):
            rows = [x for x in data if isinstance(x, dict)]

        return [_canonicalize_listing_payload(x) for x in rows]
