from __future__ import annotations

from collections import defaultdict
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ..connectors.rentcast import RentCastConnector
from ..connectors.wayne_auction import WayneAuctionConnector
from ..models import LeadSource, Strategy
from ..services.ingest import upsert_property, create_or_update_lead, score_lead
from ..services.normalize import is_disallowed_type
from ..services.rent_estimator import fetch_rent_estimate


SE_MICHIGAN_ZIPS = [
    "48009", "48084", "48301", "48067", "48306", "48304", "48302",
    "48226", "48201", "48362", "48363", "48360", "48359",
    "48348", "48346", "48350",
]


def _coerce_float(x: Any) -> float | None:
    try:
        return float(x)
    except Exception:
        return None


def _missing_core_fields(payload: dict[str, Any]) -> bool:
    return not all(
        payload.get(k)
        for k in ("addressLine", "city", "state", "zipCode")
    )


async def refresh_region(
    session: AsyncSession,
    *,
    region: str | None = None,
    zips: list[str] | None = None,
    city: str | None = None,
    max_price: float | None = None,
    per_zip_limit: int = 200,
) -> dict[str, Any]:

    drop_reasons: dict[str, int] = defaultdict(int)

    target_zips = zips or SE_MICHIGAN_ZIPS

    created = updated = dropped = 0

    rc = RentCastConnector()
    _wayne = WayneAuctionConnector()

    for zipcode in target_zips:
        try:
            raw_leads = await rc.fetch_listings(zipcode, limit=per_zip_limit)
        except Exception:
            drop_reasons["rentcast_fetch_error"] += 1
            continue

        for rl in raw_leads:
            payload = rl.payload or {}

            if _missing_core_fields(payload):
                dropped += 1
                drop_reasons["missing_core"] += 1
                continue

            lp = _coerce_float(payload.get("listPrice"))
            if lp is None:
                dropped += 1
                drop_reasons["missing_price"] += 1
                continue

            if max_price and lp > max_price:
                dropped += 1
                drop_reasons["over_max_price"] += 1
                continue

            raw_type = payload.get("propertyType")
            disallowed, _, _ = is_disallowed_type(raw_type)
            if disallowed:
                dropped += 1
                drop_reasons["disallowed_type"] += 1
                continue

            # ---- RENT ESTIMATE (CORRECT ENDPOINT)
            addr = f"{payload['addressLine']}, {payload['city']}, {payload['state']}, {payload['zipCode']}"

            rent_f = await fetch_rent_estimate(
                address=addr,
                property_type=payload.get("propertyType"),
                bedrooms=_coerce_float(payload.get("bedrooms")),
                bathrooms=_coerce_float(payload.get("bathrooms")),
                square_feet=_coerce_float(payload.get("squareFeet")),
            )

            prop = await upsert_property(session, payload)

            lead, was_created = await create_or_update_lead(
                session,
                prop=prop,
                strategy=Strategy.rental,
                source=LeadSource.rentcast_listing,
                source_ref=str(payload.get("id")),
                list_price=lp,
                rent_estimate=rent_f,
                provenance=payload,
            )

            created += int(was_created)
            updated += int(not was_created)

            await score_lead(session, lead, prop, is_auction=False)

    return {
        "created_leads": created,
        "updated_leads": updated,
        "dropped": dropped,
        "drop_reasons": dict(drop_reasons),
        "target_zips": target_zips,
    }
