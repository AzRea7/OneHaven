# app/adapters/ingestion/stub_json.py
from __future__ import annotations

import glob
import json
import os
from dataclasses import dataclass
from typing import Any

from ...models import LeadSource
from .base import IngestionProvider, RawLead


@dataclass
class StubJsonProvider(IngestionProvider):
    """
    Offline ingestion provider. Loads JSON listing payloads from disk so you can
    develop without any vendor API access.

    Put files here:
      backend/data/stub_listings/*.json

    Each file can be:
      - a list[dict] of listings, or
      - a dict with {"value": [ ... ]} like typical OData
    """

    directory: str

    @classmethod
    def from_settings(cls) -> "StubJsonProvider":
        # relative to backend/
        return cls(directory=os.getenv("STUB_LISTINGS_DIR", "data/stub_listings"))

    async def fetch(
        self,
        *,
        region: str | None,
        zips: list[str],
        city: str | None,
        per_zip_limit: int,
    ) -> list[RawLead]:
        paths = sorted(glob.glob(os.path.join(self.directory, "*.json")))
        out: list[RawLead] = []

        for p in paths:
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue

            items: list[dict[str, Any]] = []
            if isinstance(data, dict) and isinstance(data.get("value"), list):
                items = [x for x in data["value"] if isinstance(x, dict)]
            elif isinstance(data, list):
                items = [x for x in data if isinstance(x, dict)]

            # naive filter by zip if present
            for it in items[: max(0, int(per_zip_limit))]:
                zip_code = (
                    it.get("zipCode")
                    or it.get("PostalCode")
                    or (it.get("Address") or {}).get("PostalCode")
                )
                if zips and zip_code and str(zip_code) not in set(map(str, zips)):
                    continue

                listing_id = it.get("listingId") or it.get("ListingKey") or it.get("id") or os.path.basename(p)
                out.append(
                    RawLead(
                        payload=_canonicalize(it),
                        source=LeadSource.manual,  # treat as manual/offline source
                        source_ref=str(listing_id),
                    )
                )
        return out


def _canonicalize(item: dict[str, Any]) -> dict[str, Any]:
    """
    Convert “RESO-ish” payloads to the fields your pipeline expects:
      addressLine, city, state, zipCode, listPrice, propertyType, etc.
    """
    out = dict(item)

    # Address harmonization
    if "addressLine" not in out:
        out["addressLine"] = out.get("UnparsedAddress") or out.get("StreetAddress") or out.get("Address")
        if isinstance(out["addressLine"], dict):
            out["addressLine"] = out["addressLine"].get("UnparsedAddress") or out["addressLine"].get("StreetName")

    if "city" not in out:
        out["city"] = out.get("City") or (out.get("Address") or {}).get("City")

    if "state" not in out:
        out["state"] = out.get("StateOrProvince") or (out.get("Address") or {}).get("StateOrProvince")

    if "zipCode" not in out:
        out["zipCode"] = out.get("PostalCode") or (out.get("Address") or {}).get("PostalCode")

    if "listPrice" not in out:
        out["listPrice"] = out.get("ListPrice")

    if "propertyType" not in out:
        out["propertyType"] = out.get("PropertyType") or out.get("PropertySubType")

    return out
