# backend/app/services/ingest.py
from __future__ import annotations

import inspect
import json
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Lead, LeadSource, LeadStatus, Property, Strategy
from ..scoring.deal import estimate_arv, estimate_rehab, deal_score
from ..scoring.ranker import rank_score, explain as explain_ranker


def _to_int(x: Any) -> int | None:
    if x is None or x == "":
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def _to_float(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def _get(payload: dict[str, Any], *keys: str) -> Any:
    """Return first non-empty key from payload."""
    for k in keys:
        v = payload.get(k)
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _get_nested(payload: dict[str, Any], path: str) -> Any:
    """
    Tiny dot-path getter: "address.line1" or "address.city"
    """
    cur: Any = payload
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def _normalize_address_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Take a connector payload and produce canonical address fields:
      addressLine, city, stateCode, zipCode

    Supports typical RentCast-ish keys and some nested variants.
    """
    # Try nested address blobs first if present
    addr_line = _get(payload, "addressLine", "address", "streetAddress", "street")
    if not addr_line:
        addr_line = _get_nested(payload, "address.addressLine") or _get_nested(payload, "address.line") or _get_nested(payload, "address.line1")

    city = _get(payload, "city")
    if not city:
        city = _get_nested(payload, "address.city")

    state = _get(payload, "stateCode", "state", "province")
    if not state:
        state = _get_nested(payload, "address.state") or _get_nested(payload, "address.stateCode")

    zipc = _get(payload, "zipCode", "zipcode", "postalCode")
    if not zipc:
        zipc = _get_nested(payload, "address.zip") or _get_nested(payload, "address.zipCode") or _get_nested(payload, "address.postalCode")

    out = dict(payload)
    if addr_line is not None:
        out["addressLine"] = str(addr_line).strip()
    if city is not None:
        out["city"] = str(city).strip()
    if state is not None:
        # store canonical two-letter state if we can
        out["stateCode"] = str(state).strip()
    if zipc is not None:
        out["zipCode"] = str(zipc).strip()

    return out


async def upsert_property(session: AsyncSession, payload: dict[str, Any]) -> Property:
    """
    Upsert property by address identity.
    Required: addressLine, city, stateCode, zipCode (after normalization).
    """
    p = _normalize_address_fields(payload)

    address_line = (p.get("addressLine") or "").strip()
    city = (p.get("city") or "").strip()
    state = (p.get("stateCode") or p.get("state") or "").strip()
    zipcode = (p.get("zipCode") or "").strip()

    if not (address_line and city and state and zipcode):
        # IMPORTANT: include a tiny debug hint without dumping the entire payload
        hint = {
            "addressLine": bool(address_line),
            "city": bool(city),
            "state": bool(state),
            "zipCode": bool(zipcode),
            "keys": sorted(list(p.keys()))[:25],
        }
        raise ValueError(f"Missing required address fields for property upsert. hint={hint}")

    # Optional enrich fields
    lat = _to_float(_get(p, "latitude", "lat"))
    lon = _to_float(_get(p, "longitude", "lon", "lng"))
    beds = _to_int(_get(p, "bedrooms", "beds"))
    baths = _to_float(_get(p, "bathrooms", "baths"))
    sqft = _to_int(_get(p, "squareFootage", "sqft", "livingArea"))

    raw_type = _get(p, "propertyType", "homeType", "type")
    prop_type = str(raw_type).strip().lower() if raw_type is not None else None

    # Find existing
    q = select(Property).where(
        Property.address_line == address_line,
        Property.city == city,
        Property.state == state,
        Property.zipcode == zipcode,
    )
    existing = (await session.execute(q)).scalars().first()

    if existing:
        # update mutable attrs
        existing.lat = lat if lat is not None else existing.lat
        existing.lon = lon if lon is not None else existing.lon
        existing.beds = beds if beds is not None else existing.beds
        existing.baths = baths if baths is not None else existing.baths
        existing.sqft = sqft if sqft is not None else existing.sqft
        existing.property_type = prop_type if prop_type is not None else existing.property_type
        await session.flush()
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
    session.add(prop)
    await session.flush()  # assigns prop.id
    return prop


async def create_or_update_lead(
    session: AsyncSession,
    *,
    prop: Property,
    strategy: Strategy,
    source: LeadSource,
    source_ref: str | None,
    list_price: float | None,
    rent_estimate: float | None,
    provenance: dict[str, Any] | None = None,
) -> tuple[Lead, bool]:
    """
    Upsert a lead by (property_id, strategy, source).
    """
    q = select(Lead).where(
        Lead.property_id == prop.id,
        Lead.strategy == strategy,
        Lead.source == source,
    )
    existing = (await session.execute(q)).scalars().first()

    prov_json = None
    if provenance is not None:
        try:
            prov_json = json.dumps(provenance)[:20000]  # cap for sqlite sanity
        except Exception:
            prov_json = None

    if existing:
        existing.source_ref = source_ref or existing.source_ref
        existing.list_price = list_price if list_price is not None else existing.list_price
        existing.rent_estimate = rent_estimate if rent_estimate is not None else existing.rent_estimate
        existing.updated_at = datetime.utcnow()
        if hasattr(existing, "score_json") and prov_json is not None:
            existing.score_json = prov_json
        await session.flush()
        return existing, False

    lead = Lead(
        property_id=prop.id,
        strategy=strategy,
        source=source,
        source_ref=source_ref,
        list_price=list_price,
        rent_estimate=rent_estimate,
        status=LeadStatus.new,
        deal_score=0.0,
        motivation_score=0.0,
        rank_score=0.0,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    if hasattr(lead, "score_json") and prov_json is not None:
        lead.score_json = prov_json

    session.add(lead)
    await session.flush()
    return lead, True


def _call_motivation_score(prop: Property, lead: Lead) -> float:
    """
    Your motivation scorer signature changed while refactoring.
    This wrapper calls it safely no matter if it's:
      motivation_score(prop)
      motivation_score(lead, prop)
      motivation_score(lead=..., prop=...)
    """
    try:
        from ..scoring.motivation import motivation_score  # local import to avoid cycles
    except Exception:
        return 0.2

    try:
        sig = inspect.signature(motivation_score)
    except Exception:
        # best effort
        try:
            return float(motivation_score(prop))  # type: ignore
        except Exception:
            return 0.2

    params = sig.parameters
    try:
        if "lead" in params and "prop" in params:
            return float(motivation_score(lead=lead, prop=prop))  # type: ignore
        if len(params) == 2:
            return float(motivation_score(lead, prop))  # type: ignore
        return float(motivation_score(prop))  # type: ignore
    except Exception:
        return 0.2


async def score_lead(session: AsyncSession, lead: Lead, prop: Property, **kwargs: Any) -> None:
    """
    Score lead using current scoring modules.
    Accepts extra kwargs (is_auction, etc.) for backwards compatibility.
    """
    # Deal side
    arv = estimate_arv(lead.list_price)
    rehab = estimate_rehab(prop.sqft)
    dscore = float(deal_score(lead.list_price, arv, rehab, lead.rent_estimate, strategy=str(lead.strategy.value)))

    # Motivation side (safe wrapper across signature changes)
    mscore = float(_call_motivation_score(prop, lead))

    # Rank
    rscore = float(rank_score(dscore, mscore, strategy=str(lead.strategy.value)))

    lead.deal_score = dscore
    lead.motivation_score = mscore
    lead.rank_score = rscore
    lead.updated_at = datetime.utcnow()

    # Explanation: compact but information-dense
    drivers = {
        "gross_yield": None,
        "dscr_proxy": None,
        "coc_proxy": None,
        "rent_sanity": None,
        "price_to_arv": None,
        "base_discount": None,
    }
    try:
        if lead.list_price and lead.rent_estimate and lead.list_price > 0:
            drivers["gross_yield"] = (lead.rent_estimate * 12.0) / lead.list_price
        if lead.list_price and arv and arv > 0:
            drivers["price_to_arv"] = lead.list_price / arv
            drivers["base_discount"] = max((arv - lead.list_price) / arv, 0.0)
    except Exception:
        pass

    try:
        ex = explain_ranker(
            dscore,
            mscore,
            is_auction=bool(kwargs.get("is_auction", False)),
            absentee=False,
            equity=None,
            drivers=drivers,
        )
    except Exception:
        ex = f"deal={dscore:.2f} | motivation={mscore:.2f}"

    if hasattr(lead, "explain_json"):
        lead.explain_json = ex

    await session.flush()
