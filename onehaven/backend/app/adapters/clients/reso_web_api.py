# app/adapters/clients/reso_web_api.py
from __future__ import annotations

from typing import Any

import httpx

from ...config import settings


class ResoWebApiClient:
    """
    Minimal RESO Web API client.
    Your real implementation can expand query building and auth.
    """

    def __init__(self) -> None:
        self.base_url = (settings.RESO_BASE_URL or "").rstrip("/")
        self.access_token = settings.RESO_ACCESS_TOKEN

    def _headers(self) -> dict[str, str]:
        if not self.access_token:
            raise RuntimeError("RESO_ACCESS_TOKEN is not set")
        return {"Authorization": f"Bearer {self.access_token}", "accept": "application/json"}

    async def query_listings(self, *, zipcode: str | None = None, city: str | None = None, top: int = 200) -> list[dict[str, Any]]:
        """
        Generic listings query.
        NOTE: You must align the endpoint path and query with your provider (MLS Grid or your MLS).
        """
        if not self.base_url:
            raise RuntimeError("RESO_BASE_URL is not set")

        # Example RESO OData path (provider-specific)
        url = f"{self.base_url}/Property"

        # Provider-specific OData $filter
        filters: list[str] = []
        if zipcode:
            filters.append(f"PostalCode eq '{zipcode}'")
        if city:
            filters.append(f"City eq '{city}'")

        params: dict[str, Any] = {"$top": top}
        if filters:
            params["$filter"] = " and ".join(filters)

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(url, headers=self._headers(), params=params)
            r.raise_for_status()
            data = r.json()

        # Typical OData: {"value":[...]}
        if isinstance(data, dict) and isinstance(data.get("value"), list):
            return data["value"]
        if isinstance(data, list):
            return data
        return []
