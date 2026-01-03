# app/adapters/repos/properties.py
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...models import Property


class PropertyRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert_from_payload(self, payload: dict[str, Any]) -> Property:
        """
        Upsert using:
          1) (source, source_listing_id) if available
          2) (address_line1, city, state, zip_code) fallback

        NOTE: Our Property model maps these Python attrs to your SQLite columns:
          address_line1 -> address_line
          zip_code      -> zipcode
          latitude      -> lat
          longitude     -> lon
          bedrooms      -> beds
          bathrooms     -> baths
        """

        # Accept both snake_case and RentCast keys
        address_line1 = (payload.get("address_line1") or payload.get("addressLine1") or payload.get("address_line") or "").strip()
        city = (payload.get("city") or "").strip()
        state = (payload.get("state") or "").strip()
        zip_code = (payload.get("zip_code") or payload.get("zipCode") or payload.get("zipcode") or "").strip()

        if not (address_line1 and city and state and zip_code):
            raise ValueError(f"Missing required address fields: {address_line1=}, {city=}, {state=}, {zip_code=}")

        source = (payload.get("source") or "unknown").strip()
        source_listing_id = payload.get("source_listing_id") or payload.get("id")

        prop: Property | None = None

        # 1) Match on provider id if present
        if source_listing_id:
            q = select(Property).where(
                Property.source == source,
                Property.source_listing_id == str(source_listing_id),
            )
            prop = (await self.session.execute(q)).scalars().first()

        # 2) Fallback match on normalized address key
        if prop is None:
            q = select(Property).where(
                Property.address_line1 == address_line1,
                Property.city == city,
                Property.state == state,
                Property.zip_code == zip_code,
            )
            prop = (await self.session.execute(q)).scalars().first()

        if prop is None:
            prop = Property()
            self.session.add(prop)

        # Always update core identity fields
        prop.source = source
        prop.source_listing_id = str(source_listing_id) if source_listing_id else prop.source_listing_id

        prop.address_line1 = address_line1
        prop.city = city
        prop.state = state
        prop.zip_code = zip_code

        # Optional: location
        lat = payload.get("latitude", payload.get("lat"))
        lon = payload.get("longitude", payload.get("lon"))
        if lat is not None:
            prop.latitude = float(lat)
        if lon is not None:
            prop.longitude = float(lon)

        # Optional: attributes
        prop.property_type = payload.get("property_type") or payload.get("propertyType") or prop.property_type

        beds = payload.get("bedrooms", payload.get("beds"))
        if beds is not None:
            try:
                prop.bedrooms = int(float(beds))
            except (TypeError, ValueError):
                pass

        baths = payload.get("bathrooms", payload.get("baths"))
        if baths is not None:
            try:
                prop.bathrooms = float(baths)
            except (TypeError, ValueError):
                pass

        sqft = payload.get("sqft") or payload.get("squareFootage")
        if sqft is not None:
            try:
                prop.sqft = int(float(sqft))
            except (TypeError, ValueError):
                pass

        await self.session.flush()
        return prop
