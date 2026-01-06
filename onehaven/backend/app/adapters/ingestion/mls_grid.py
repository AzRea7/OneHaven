# onehaven/backend/app/adapters/ingestion/mls_grid.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from ...config import settings
from ...models import LeadSource
from ..clients.reso_web_api import ResoWebApiClient
from .base import RawLead


@dataclass
class MlsGridProvider:
    """
    "MLS Grid mode" ingestion.

    Reality: if your MLS Grid subscription does NOT include Realcomp, then this file
    should be used with a RESO Web API endpoint you DO have rights to access
    (e.g., MichRIC RESO Web API, Realcomp direct API, Bridge, etc.).
    """

    client: ResoWebApiClient

    @classmethod
    def from_settings(cls) -> "MlsGridProvider":
        return cls(client=ResoWebApiClient(base_url=settings.RESO_BASE_URL, token=settings.RESO_ACCESS_TOKEN))

    def fetch(self, *, zips: list[str], max_price: float | None = None, limit_per_zip: int = 200) -> list[RawLead]:
        out: list[RawLead] = []
        for z in zips:
            items = self.client.search_property_listings(zipcode=z, max_price=max_price, limit=limit_per_zip)
            for it in items:
                out.append(self._to_raw_lead(it))
        return out

    def _to_raw_lead(self, item: dict[str, Any]) -> RawLead:
        """
        Keep this mapping LIGHT. Your pipeline's normalizer handles canonicalization.
        We *only* ensure stable keys for downstream + attach source refs.
        """
        listing_id = (
            item.get("ListingKey")
            or item.get("ListingId")
            or item.get("ListingNumber")
            or item.get("listingId")
            or item.get("id")
        )
        if listing_id is None:
            # worst case: deterministic fallback
            listing_id = f"unknown::{hash(str(item))}"

        payload = dict(item)
        payload.setdefault("mls_context", {})
        payload["mls_context"].update(
            {
                "provider_mode": "mls_grid",
                "mls_name": settings.MLS_PRIMARY_NAME,  # e.g. "realcomp" or "michric"
            }
        )

        return RawLead(
            source=LeadSource.mls_grid,
            source_ref=str(listing_id),
            payload=payload,
        )
