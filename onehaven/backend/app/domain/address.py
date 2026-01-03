# app/domain/address.py
from __future__ import annotations

from typing import Any

from .parsing import get_first, get_nested


def normalize_address_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Take a connector payload and produce canonical address fields:
      addressLine, city, stateCode, zipCode

    Supports typical RentCast-ish keys and some nested variants.
    """
    addr_line = get_first(payload, "addressLine", "address", "streetAddress", "street")
    if not addr_line:
        addr_line = (
            get_nested(payload, "address.addressLine")
            or get_nested(payload, "address.line")
            or get_nested(payload, "address.line1")
        )

    city = get_first(payload, "city") or get_nested(payload, "address.city")

    state = get_first(payload, "stateCode", "state", "province")
    if not state:
        state = get_nested(payload, "address.state") or get_nested(payload, "address.stateCode")

    zipc = get_first(payload, "zipCode", "zipcode", "postalCode")
    if not zipc:
        zipc = (
            get_nested(payload, "address.zip")
            or get_nested(payload, "address.zipCode")
            or get_nested(payload, "address.postalCode")
        )

    out = dict(payload)
    if addr_line is not None:
        out["addressLine"] = str(addr_line).strip()
    if city is not None:
        out["city"] = str(city).strip()
    if state is not None:
        out["stateCode"] = str(state).strip()
    if zipc is not None:
        out["zipCode"] = str(zipc).strip()

    return out


def require_address_identity(p: dict[str, Any]) -> tuple[str, str, str, str]:
    """
    Returns (address_line, city, state, zipcode) or raises ValueError with a tiny hint.
    """
    address_line = (p.get("addressLine") or "").strip()
    city = (p.get("city") or "").strip()
    state = (p.get("stateCode") or p.get("state") or "").strip()
    zipcode = (p.get("zipCode") or "").strip()

    if not (address_line and city and state and zipcode):
        hint = {
            "addressLine": bool(address_line),
            "city": bool(city),
            "state": bool(state),
            "zipCode": bool(zipcode),
            "keys": sorted(list(p.keys()))[:25],
        }
        raise ValueError(f"Missing required address fields for property upsert. hint={hint}")

    return address_line, city, state, zipcode
