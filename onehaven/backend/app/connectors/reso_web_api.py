from __future__ import annotations
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class ResoConfig:
    base_url: str         # e.g. https://<mls>/reso/odata or https://api.mlsgrid.com/v2/Reso/OData
    token_url: str        # OAuth token endpoint
    client_id: str
    client_secret: str
    scope: str | None = None


class ResoWebApiClient:
    """
    RESO Web API is OData-based (commonly OData v4) with OAuth in front. :contentReference[oaicite:5]{index=5}
    Field names are standardized by RESO Data Dictionary (e.g., ListPrice, StandardStatus). :contentReference[oaicite:6]{index=6}
    """
    def __init__(self, cfg: ResoConfig) -> None:
        self.cfg = cfg
        self._token: str | None = None

    async def _get_token(self) -> str:
        # Client credentials is common; some MLS use auth-code/password flows.
        data = {
            "grant_type": "client_credentials",
            "client_id": self.cfg.client_id,
            "client_secret": self.cfg.client_secret,
        }
        if self.cfg.scope:
            data["scope"] = self.cfg.scope

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(self.cfg.token_url, data=data)
            r.raise_for_status()
            js = r.json()
        token = js.get("access_token")
        if not token:
            raise RuntimeError("OAuth token response missing access_token")
        self._token = token
        return token

    async def _auth_headers(self) -> dict[str, str]:
        if not self._token:
            await self._get_token()
        return {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}

    async def query_property(self, odata_query: str) -> dict[str, Any]:
        """
        Example odata_query:
          /Property?$filter=PostalCode eq '48009' and StandardStatus eq 'Active'&$top=50
        """
        url = self.cfg.base_url.rstrip("/") + "/" + odata_query.lstrip("/")
        headers = await self._auth_headers()
        async with httpx.AsyncClient(timeout=45) as client:
            r = await client.get(url, headers=headers)
            if r.status_code == 401:
                # token expired, refresh once
                await self._get_token()
                headers = await self._auth_headers()
                r = await client.get(url, headers=headers)
            r.raise_for_status()
            return r.json()

    @staticmethod
    def map_reso_property_to_haven(item: dict[str, Any]) -> dict[str, Any]:
        """
        Map common RESO DD fields → Haven payload keys that upsert_property understands.
        Field names vary slightly by MLS, but DD standard helps a lot. :contentReference[oaicite:7]{index=7}
        """
        # Common DD-ish fields:
        # - UnparsedAddress / StreetNumber/StreetName
        # - City, StateOrProvince, PostalCode
        # - ListPrice, BedroomsTotal, BathroomsTotal, LivingArea
        # - PropertyType, PropertySubType
        return {
            "address": item.get("UnparsedAddress") or item.get("StreetAddress") or item.get("StreetName"),
            "city": item.get("City"),
            "state": item.get("StateOrProvince"),
            "zipCode": item.get("PostalCode"),
            "price": item.get("ListPrice"),
            "beds": item.get("BedroomsTotal"),
            "baths": item.get("BathroomsTotalInteger") or item.get("BathroomsTotal"),
            "sqft": item.get("LivingArea"),
            "propertyType": item.get("PropertyType") or item.get("PropertySubType"),
            "lat": item.get("Latitude"),
            "lon": item.get("Longitude"),
            "standardStatus": item.get("StandardStatus"),
            "listingId": item.get("ListingId") or item.get("ListingID") or item.get("ListingKey"),
        }
