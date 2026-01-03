# backend/app/adapters/clients/rentcast_avm.py
from __future__ import annotations

from typing import Any

import httpx

from ...config import settings
from ...models import Property
from ...service_layer.estimates import EstimateResult


def _get_attr(obj: Any, *names: str) -> Any:
    """
    Robust attribute getter so we don't explode when the ORM/model evolves.

    Example: Property used to have address_line/zipcode and now has address_line1/zip_code.
    """
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v is not None:
                return v
    return None


def _addr_string(prop: Property) -> str:
    # Prefer the new names, but support old ones.
    address = _get_attr(prop, "address_line1", "address_line", "addressLine", "address")
    city = _get_attr(prop, "city")
    state = _get_attr(prop, "state")
    zipcode = _get_attr(prop, "zip_code", "zipcode", "zip", "zipCode")

    parts: list[str] = []
    for x in (address, city, state, zipcode):
        if x is None:
            continue
        s = str(x).strip()
        if s:
            parts.append(s)

    return ", ".join(parts)


def _coerce_float(x: Any) -> float | None:
    try:
        return float(x)
    except Exception:
        return None


async def fetch_rent_long_term(prop: Property, comp_count: int = 5) -> EstimateResult:
    """
    Calls RentCast long-term rent AVM.
    Returns EstimateResult(value=<rent>, source="rentcast", raw=<json>)
    """
    if not settings.RENTCAST_API_KEY:
        return EstimateResult(value=None, source="disabled", raw=None)

    url = f"{settings.RENTCAST_BASE_URL.rstrip('/')}/avm/rent/long-term"
    headers = {"X-Api-Key": settings.RENTCAST_API_KEY, "accept": "application/json"}

    params: dict[str, Any] = {"compCount": comp_count}

    # Priority: address; fallback: lat/lon if present
    addr = _addr_string(prop)
    if addr:
        params["address"] = addr

    lat = _get_attr(prop, "lat", "latitude")
    lon = _get_attr(prop, "lon", "longitude")
    if lat is not None and lon is not None:
        params["latitude"] = lat
        params["longitude"] = lon

    prop_type = _get_attr(prop, "property_type", "propertyType")
    if prop_type:
        params["propertyType"] = prop_type

    beds = _get_attr(prop, "beds", "bedrooms")
    if beds is not None:
        params["bedrooms"] = beds

    baths = _get_attr(prop, "baths", "bathrooms")
    if baths is not None:
        params["bathrooms"] = baths

    sqft = _get_attr(prop, "sqft", "square_footage", "squareFootage")
    if sqft is not None:
        params["squareFootage"] = sqft

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        return EstimateResult(value=None, source=f"rentcast_error:{type(e).__name__}", raw=None)

    # RentCast usually returns "rent" for rent endpoints
    rent = data.get("rent") if isinstance(data, dict) else None
    return EstimateResult(
        value=_coerce_float(rent),
        source="rentcast",
        raw=data if isinstance(data, dict) else None,
    )


async def fetch_value(prop: Property, comp_count: int = 5) -> EstimateResult:
    """
    Calls RentCast value AVM.
    Returns EstimateResult(value=<price>, source="rentcast", raw=<json>)
    """
    if not settings.RENTCAST_API_KEY:
        return EstimateResult(value=None, source="disabled", raw=None)

    url = f"{settings.RENTCAST_BASE_URL.rstrip('/')}/avm/value"
    headers = {"X-Api-Key": settings.RENTCAST_API_KEY, "accept": "application/json"}

    params: dict[str, Any] = {"compCount": comp_count}

    addr = _addr_string(prop)
    if addr:
        params["address"] = addr

    lat = _get_attr(prop, "lat", "latitude")
    lon = _get_attr(prop, "lon", "longitude")
    if lat is not None and lon is not None:
        params["latitude"] = lat
        params["longitude"] = lon

    prop_type = _get_attr(prop, "property_type", "propertyType")
    if prop_type:
        params["propertyType"] = prop_type

    beds = _get_attr(prop, "beds", "bedrooms")
    if beds is not None:
        params["bedrooms"] = beds

    baths = _get_attr(prop, "baths", "bathrooms")
    if baths is not None:
        params["bathrooms"] = baths

    sqft = _get_attr(prop, "sqft", "square_footage", "squareFootage")
    if sqft is not None:
        params["squareFootage"] = sqft

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        return EstimateResult(value=None, source=f"rentcast_error:{type(e).__name__}", raw=None)

    price = data.get("price") if isinstance(data, dict) else None
    return EstimateResult(
        value=_coerce_float(price),
        source="rentcast",
        raw=data if isinstance(data, dict) else None,
    )


# -------------------------------------------------------------------
# Backwards-compatible exports (your refresh use case imports these)
# -------------------------------------------------------------------

async def fetch_rent_long_term_avm(prop: Property, comp_count: int = 5) -> EstimateResult:
    # Legacy name used elsewhere in the codebase
    return await fetch_rent_long_term(prop, comp_count=comp_count)


async def fetch_value_avm(prop: Property, comp_count: int = 5) -> EstimateResult:
    # Legacy name used elsewhere in the codebase
    return await fetch_value(prop, comp_count=comp_count)
