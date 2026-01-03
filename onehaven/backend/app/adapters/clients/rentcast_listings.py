# app/adapters/clients/rentcast_listings.py
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import httpx

from ...config import settings


def _write_sample(prefix: str, sample: dict[str, Any]) -> None:
    """
    Used by tests/debugging to snapshot sample payloads.
    """
    os.makedirs("data/rentcast_samples", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    path = os.path.join("data/rentcast_samples", f"{prefix}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=2, default=str)


class RentCastConnector:
    """
    Low-level HTTP client for RentCast LISTINGS (sale listings ingestion).
    IMPORTANT: returns raw JSON dicts; does NOT return RawLead.
    """

    def __init__(self) -> None:
        if not settings.RENTCAST_API_KEY:
            # Don't hard fail at import time; only fail when called.
            self._api_key = None
        else:
            self._api_key = settings.RENTCAST_API_KEY

        self._base_url = settings.RENTCAST_BASE_URL.rstrip("/")

    def _headers(self) -> dict[str, str]:
        if not self._api_key:
            raise RuntimeError("RENTCAST_API_KEY is not set")
        return {"accept": "application/json", "X-Api-Key": self._api_key}

    async def fetch_listings(self, zipcode: str, limit: int = 200) -> list[dict[str, Any]]:
        """
        Fetch active sale listings for a zipcode.
        NOTE: This must match your actual RentCast connector behavior/endpoints.
        If your old connector used a different endpoint, keep that path.
        """
        url = f"{self._base_url}/v1/listings/sale"
        params = {"zipCode": zipcode, "limit": limit}

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(url, headers=self._headers(), params=params)
            r.raise_for_status()
            data = r.json()

        # RentCast sometimes returns {"listings":[...]} or a raw list
        if isinstance(data, dict) and "listings" in data and isinstance(data["listings"], list):
            return data["listings"]
        if isinstance(data, list):
            return data
        return []
