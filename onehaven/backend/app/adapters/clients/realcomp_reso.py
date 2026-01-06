# app/adapters/clients/realcomp_reso.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from ...config import settings
from .http_resilience import resilient_request
from .reso_web_api import ResoWebApiClient


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class _Token:
    access_token: str
    expires_at: datetime


class RealcompResoClient:
    """Direct Realcomp mode (OAuth2 -> RESO Web API)."""

    def __init__(self) -> None:
        self.token_url = settings.REALCOMP_TOKEN_URL
        self.client_id = settings.REALCOMP_CLIENT_ID
        self.client_secret = settings.REALCOMP_CLIENT_SECRET
        self.scope = settings.REALCOMP_SCOPE
        self.reso_base_url = settings.REALCOMP_RESO_BASE_URL
        self._token: _Token | None = None

    async def _get_token(self) -> str:
        if self._token and self._token.expires_at > _utcnow() + timedelta(seconds=30):
            return self._token.access_token

        if not (self.token_url and self.client_id and self.client_secret):
            raise RuntimeError("realcomp_oauth_not_configured")

        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.scope:
            data["scope"] = self.scope

        resp = await resilient_request(
            "POST",
            self.token_url,
            headers={"accept": "application/json", "content-type": "application/x-www-form-urlencoded"},
            data=data,
        )
        payload = resp.json()
        token = payload.get("access_token")
        expires_in = payload.get("expires_in", 3600)
        if not token:
            raise httpx.HTTPError(f"realcomp_token_missing: {payload}")

        self._token = _Token(access_token=str(token), expires_at=_utcnow() + timedelta(seconds=int(expires_in)))
        return self._token.access_token

    async def search_property_listings(
        self,
        *,
        zipcode: str,
        max_price: float | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        token = await self._get_token()
        client = ResoWebApiClient(base_url=self.reso_base_url, access_token=token)
        return await client.search_property_listings(zipcode=zipcode, max_price=max_price, limit=limit)
