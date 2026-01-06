# app/adapters/clients/reso_web_api.py
from __future__ import annotations

from typing import Any

from ...config import settings
from .http_resilience import resilient_request


class ResoWebApiClient:
    """Minimal RESO Web API client (OData-ish)."""

    def __init__(self, *, base_url: str | None = None, access_token: str | None = None) -> None:
        self.base_url = ((base_url if base_url is not None else settings.RESO_BASE_URL) or "").rstrip("/")
        self.access_token = access_token if access_token is not None else settings.RESO_ACCESS_TOKEN

    def _headers(self) -> dict[str, str]:
        if not self.access_token:
            return {"accept": "application/json"}
        return {"accept": "application/json", "authorization": f"Bearer {self.access_token}"}

    async def search_property_listings(
        self,
        *,
        zipcode: str,
        max_price: float | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        if not self.base_url:
            return []

        url = f"{self.base_url}/Property"

        filters: list[str] = [f"PostalCode eq '{zipcode}'"]
        if max_price is not None:
            filters.append(f"ListPrice le {float(max_price)}")

        params: dict[str, Any] = {"$top": int(limit), "$filter": " and ".join(filters)}

        resp = await resilient_request("GET", url, headers=self._headers(), params=params)
        data = resp.json()

        items = data.get("value") if isinstance(data, dict) else None
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        return []
