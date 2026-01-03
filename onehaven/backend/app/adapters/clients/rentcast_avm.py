# app/adapters/rentcast_avm.py
from __future__ import annotations

from typing import Any

import httpx

from ...config import settings
from ...models import Property
from ...services.estimates import EstimateResult


def _addr_string(prop: Property) -> str:
    return f"{prop.address_line}, {prop.city}, {prop.state}, {prop.zipcode}"


def _coerce_float(x: Any) -> float | None:
    try:
        return float(x)
    except Exception:
        return None


async def fetch_rent_long_term(prop: Property, comp_count: int = 5) -> EstimateResult:
    if not settings.RENTCAST_API_KEY:
        return EstimateResult(value=None, source="disabled", raw=None)

    url = f"{settings.RENTCAST_BASE_URL.rstrip('/')}/avm/rent/long-term"
    headers = {"X-Api-Key": settings.RENTCAST_API_KEY, "accept": "application/json"}

    params: dict[str, Any] = {"compCount": comp_count}

    # Priority: address; fallback: lat/lon if you have it
    params["address"] = _addr_string(prop)
    if prop.lat is not None and prop.lon is not None:
        params["latitude"] = prop.lat
        params["longitude"] = prop.lon

    if prop.property_type:
        params["propertyType"] = prop.property_type
    if prop.beds is not None:
        params["bedrooms"] = prop.beds
    if prop.baths is not None:
        params["bathrooms"] = prop.baths
    if prop.sqft is not None:
        params["squareFootage"] = prop.sqft

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        return EstimateResult(value=None, source=f"rentcast_error:{type(e).__name__}", raw=None)

    rent = data.get("rent") or data.get("price")
    return EstimateResult(value=_coerce_float(rent), source="rentcast", raw=data if isinstance(data, dict) else None)


async def fetch_value(prop: Property, comp_count: int = 5) -> EstimateResult:
    if not settings.RENTCAST_API_KEY:
        return EstimateResult(value=None, source="disabled", raw=None)

    url = f"{settings.RENTCAST_BASE_URL.rstrip('/')}/avm/value"
    headers = {"X-Api-Key": settings.RENTCAST_API_KEY, "accept": "application/json"}

    params: dict[str, Any] = {"compCount": comp_count}
    params["address"] = _addr_string(prop)
    if prop.lat is not None and prop.lon is not None:
        params["latitude"] = prop.lat
        params["longitude"] = prop.lon

    if prop.property_type:
        params["propertyType"] = prop.property_type
    if prop.beds is not None:
        params["bedrooms"] = prop.beds
    if prop.baths is not None:
        params["bathrooms"] = prop.baths
    if prop.sqft is not None:
        params["squareFootage"] = prop.sqft

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        return EstimateResult(value=None, source=f"rentcast_error:{type(e).__name__}", raw=None)

    price = data.get("price")
    return EstimateResult(value=_coerce_float(price), source="rentcast", raw=data if isinstance(data, dict) else None)
