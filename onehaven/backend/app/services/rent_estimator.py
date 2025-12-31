from __future__ import annotations

from typing import Any
import httpx

from ..config import settings


async def fetch_rent_estimate(
    *,
    address: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    property_type: str | None = None,
    bedrooms: float | None = None,
    bathrooms: float | None = None,
    square_feet: float | None = None,
    comp_count: int = 5,
) -> float | None:
    """
    RentCast Long-Term Rent Estimate
    GET /v1/avm/rent/long-term
    """

    if not settings.RENTCAST_API_KEY:
        return None

    params: dict[str, Any] = {"compCount": comp_count}

    if address:
        params["address"] = address
    elif latitude is not None and longitude is not None:
        params["latitude"] = latitude
        params["longitude"] = longitude
    else:
        return None

    if property_type:
        params["propertyType"] = property_type
    if bedrooms is not None:
        params["bedrooms"] = bedrooms
    if bathrooms is not None:
        params["bathrooms"] = bathrooms
    if square_feet is not None:
        params["squareFootage"] = square_feet

    headers = {
        "X-Api-Key": settings.RENTCAST_API_KEY,
        "accept": "application/json",
    }

    url = f"{settings.RENTCAST_BASE_URL.rstrip('/')}/avm/rent/long-term"

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception:
        return None

    rent = data.get("rent") or data.get("price")
    try:
        return float(rent) if rent is not None else None
    except Exception:
        return None


async def fetch_value_estimate(
    *,
    address: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    property_type: str | None = None,
    bedrooms: float | None = None,
    bathrooms: float | None = None,
    square_feet: float | None = None,
    comp_count: int = 5,
) -> float | None:
    """
    RentCast Value / ARV Estimate
    GET /v1/avm/value
    """

    if not settings.RENTCAST_API_KEY:
        return None

    params: dict[str, Any] = {"compCount": comp_count}

    if address:
        params["address"] = address
    elif latitude is not None and longitude is not None:
        params["latitude"] = latitude
        params["longitude"] = longitude
    else:
        return None

    if property_type:
        params["propertyType"] = property_type
    if bedrooms is not None:
        params["bedrooms"] = bedrooms
    if bathrooms is not None:
        params["bathrooms"] = bathrooms
    if square_feet is not None:
        params["squareFootage"] = square_feet

    headers = {
        "X-Api-Key": settings.RENTCAST_API_KEY,
        "accept": "application/json",
    }

    url = f"{settings.RENTCAST_BASE_URL.rstrip('/')}/avm/value"

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception:
        return None

    price = data.get("price")
    try:
        return float(price) if price is not None else None
    except Exception:
        return None
