# app/adapters/repos/properties.py
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...domain.address import normalize_address_fields, require_address_identity
from ...domain.parsing import to_float, to_int, get_first
from ...models import Property


class PropertyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_from_payload(self, payload: dict[str, Any]) -> Property:
        """
        Upsert property by address identity.
        Required: addressLine, city, stateCode, zipCode (after normalization).
        """
        p = normalize_address_fields(payload)
        address_line, city, state, zipcode = require_address_identity(p)

        # Optional enrich fields
        lat = to_float(get_first(p, "latitude", "lat"))
        lon = to_float(get_first(p, "longitude", "lon", "lng"))
        beds = to_int(get_first(p, "bedrooms", "beds"))
        baths = to_float(get_first(p, "bathrooms", "baths"))
        sqft = to_int(get_first(p, "squareFootage", "sqft", "livingArea"))

        raw_type = get_first(p, "propertyType", "homeType", "type")
        prop_type = str(raw_type).strip().lower() if raw_type is not None else None

        q = select(Property).where(
            Property.address_line == address_line,
            Property.city == city,
            Property.state == state,
            Property.zipcode == zipcode,
        )
        existing = (await self.session.execute(q)).scalars().first()

        if existing:
            existing.lat = lat if lat is not None else existing.lat
            existing.lon = lon if lon is not None else existing.lon
            existing.beds = beds if beds is not None else existing.beds
            existing.baths = baths if baths is not None else existing.baths
            existing.sqft = sqft if sqft is not None else existing.sqft
            existing.property_type = prop_type if prop_type is not None else existing.property_type
            await self.session.flush()
            return existing

        prop = Property(
            address_line=address_line,
            city=city,
            state=state,
            zipcode=zipcode,
            lat=lat,
            lon=lon,
            beds=beds,
            baths=baths,
            sqft=sqft,
            property_type=prop_type,
            created_at=datetime.utcnow(),
        )
        self.session.add(prop)
        await self.session.flush()
        return prop
