# backend/app/jobs/refresh.py
from __future__ import annotations

from collections import defaultdict
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ..connectors.rentcast import RentCastConnector
from ..connectors.wayne_auction import WayneAuctionConnector
from ..models import LeadSource, Strategy
from ..services.ingest import upsert_property, create_or_update_lead, score_lead
from ..services.normalize import is_disallowed_type

SE_MICHIGAN_ZIPS: list[str] = [
    "48009", "48084", "48301", "48067", "48306", "48304", "48302", "48226", "48201",
    # Lake Orion / Clarkston area
    "48362", "48363", "48360", "48359", "48348", "48346", "48350",
]

CITY_TO_ZIPS: dict[str, list[str]] = {
    "lake orion": ["48359", "48360", "48361", "48362"],
    "orion": ["48359", "48360", "48361", "48362"],
    "clarkston": ["48346", "48348"],
    "independence": ["48346", "48348"],
}

REGION_TO_ZIPS: dict[str, list[str]] = {
    "se_michigan": SE_MICHIGAN_ZIPS,
}


def _missing_core_fields(payload: dict[str, Any]) -> bool:
    addr = (payload.get("addressLine") or "").strip()
    city = (payload.get("city") or "").strip()
    zipc = (payload.get("zipCode") or "").strip()
    return not (addr and city and zipc)


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

    # Resolve target zips
    if zips:
        target_zips = [z.strip() for z in zips if z.strip()]
    elif city:
        target_zips = CITY_TO_ZIPS.get(city.strip().lower(), [])
        if not target_zips:
            return {
                "created_leads": 0,
                "updated_leads": 0,
                "dropped": 0,
                "drop_reasons": {"unknown_city": 1, "city": city},
                "target_zips": [],
            }
    else:
        rk = (region or "se_michigan").strip().lower()
        target_zips = REGION_TO_ZIPS.get(rk, [])
        if not target_zips:
            return {
                "created_leads": 0,
                "updated_leads": 0,
                "dropped": 0,
                "drop_reasons": {"unknown_region": 1, "region": region},
                "target_zips": [],
            }

    created = updated = dropped = 0

    rc = RentCastConnector()
    wayne = WayneAuctionConnector()

    for zipcode in target_zips:
        # --- RentCast sale listings
        try:
            raw_leads = await rc.fetch_listings(zipcode, limit=per_zip_limit)
        except Exception as e:
            drop_reasons[f"rentcast_error::{type(e).__name__}"] += 1
            continue

        for rl in raw_leads:
            payload = rl.payload or {}

            if _missing_core_fields(payload):
                dropped += 1
                drop_reasons["missing_core_fields"] += 1
                continue

            # Price filter
            lp = payload.get("listPrice")
            lp_f = None
            if lp is not None:
                try:
                    lp_f = float(lp)
                except Exception:
                    lp_f = None

            if max_price is not None and lp_f is not None and lp_f > max_price:
                dropped += 1
                drop_reasons["over_max_price"] += 1
                continue

            # Strict type filtering
            raw_type = payload.get("propertyType")
            disallowed, norm, reason_key = is_disallowed_type(raw_type)
            if disallowed:
                dropped += 1
                drop_reasons["invalid_or_disallowed_property"] += 1
                drop_reasons[f"raw_type::{raw_type}"] += 1
                if reason_key:
                    drop_reasons[reason_key] += 1
                continue

            prop = await upsert_property(session, payload)
            if not prop:
                dropped += 1
                drop_reasons["property_upsert_failed"] += 1
                continue

            for strat in (Strategy.rental, Strategy.flip):
                lead, was_created = await create_or_update_lead(
                    session=session,
                    property_id=prop.id,
                    source=LeadSource.rentcast_listing,
                    strategy=strat,
                    source_ref=rl.source_ref,
                    provenance=rl.provenance,
                )
                if lp_f is not None:
                    lead.list_price = lp_f

                await score_lead(session, lead, prop, is_auction=False)

                created += 1 if was_created else 0
                updated += 0 if was_created else 1

        # --- Wayne auctions (optional; can be disabled if you want)
        wayne_leads = await wayne.fetch_by_zip(zipcode, limit=min(100, per_zip_limit))
        for rl in wayne_leads:
            payload = rl.payload or {}
            if _missing_core_fields(payload):
                dropped += 1
                drop_reasons["missing_core_fields"] += 1
                continue

            # Wayne payload sets propertyType=single_family, but keep the gate anyway
            raw_type = payload.get("propertyType")
            disallowed, norm, reason_key = is_disallowed_type(raw_type)
            if disallowed:
                dropped += 1
                drop_reasons["invalid_or_disallowed_property"] += 1
                drop_reasons[f"raw_type::{raw_type}"] += 1
                if reason_key:
                    drop_reasons[reason_key] += 1
                continue

            prop = await upsert_property(session, payload)
            if not prop:
                dropped += 1
                drop_reasons["property_upsert_failed"] += 1
                continue

            for strat in (Strategy.rental, Strategy.flip):
                lead, was_created = await create_or_update_lead(
                    session=session,
                    property_id=prop.id,
                    source=LeadSource.wayne_auction,
                    strategy=strat,
                    source_ref=rl.source_ref,
                    provenance=rl.provenance,
                )
                await score_lead(session, lead, prop, is_auction=True)

                created += 1 if was_created else 0
                updated += 0 if was_created else 1

    return {
        "created_leads": created,
        "updated_leads": updated,
        "dropped": dropped,
        "drop_reasons": dict(drop_reasons),
        "target_zips": target_zips,
    }
