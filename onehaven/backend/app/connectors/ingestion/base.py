# app/connectors/ingestion/base.py
from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator, Protocol, Optional


@dataclass(frozen=True)
class RawLead:
    # Canonical ingestion payload shape (pipeline input)
    address_line: str
    city: str
    state: str
    zipcode: str

    list_price: float | None
    bedrooms: float | None = None
    bathrooms: float | None = None
    sqft: float | None = None
    year_built: int | None = None

    property_type: str | None = None  # raw upstream type string
    lat: float | None = None
    lon: float | None = None

    source: str = "unknown"
    source_ref: str = ""
    provenance: dict | None = None


class IngestionAdapter(Protocol):
    async def iter_sale_listings(
        self,
        *,
        zipcode: str,
        per_zip_limit: int = 200,
        max_price: float | None = None,
        city: str | None = None,
    ) -> AsyncIterator[RawLead]:
        """Yield sale listings as RawLead in canonical shape."""
        ...
