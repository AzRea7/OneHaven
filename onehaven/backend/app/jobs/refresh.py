# backend/app/jobs/refresh.py
from __future__ import annotations

from collections import defaultdict
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ..connectors.rentcast import RentCastConnector
from ..connectors.wayne_auction import WayneAuctionConnector
from ..models import LeadSource, Strategy
from ..scoring.deal import estimate_rent, gross_yield
from ..services.ingest import upsert_property, create_or_update_lead, score_lead
from ..services.normalize import is_disallowed_type

SE_MICHIGAN_ZIPS: list[str] = [
    "48009",
    "48084",
    "48301",
    "48067",
    "48306",
    "48304",
    "48302",
    "48226",
    "48201",
    # Lake Orion / Clarkston area
    "48362",
    "48363",
    "48360",
    "48359",
    "48348",
    "48346",
    "48350",
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


def _coerce_float(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def _coerce_int(x: Any) -> int | None:
    if x is None or x == "":
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def _quality_gate_drop_reason_for_missing_dims(
    beds: int | None,
    baths: float | None,
    sqft: int | None,
) -> str | None:
    """
    Decide whether we DROP based on missing beds/baths/sqft.
    Policy:
      - If 2+ of (beds,baths,sqft) missing => DROP (too incomplete to trust)
      - If <=1 missing => keep (but we may penalize later)
    """
    missing = 0
    if beds is None:
        missing += 1
    if baths is None:
        missing += 1
    if sqft is None:
        missing += 1

    if missing >= 2:
        return "missing_beds_baths_sqft"
    return None


def _apply_rank_penalty(lead, penalty_mult: float, tag: str) -> None:
    """
    Multiply scores to keep ordering consistent while making “bad” leads sink.
    Also annotate explain string so UI/debug makes the issue obvious.
    """
    try:
        lead.rank_score = float(lead.rank_score or 0.0) * penalty_mult
        lead.deal_score = float(lead.deal_score or 0.0) * penalty_mult
        lead.motivation_score = float(lead.motivation_score or 0.0) * max(0.25, penalty_mult)
    except Exception:
        # Don't fail ingestion on penalty logic
        pass

    ex = (lead.explain or "").strip()
    if ex:
        lead.explain = f"{ex} | DQ:{tag}"
    else:
        lead.explain = f"DQ:{tag}"


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

    # Tunable “truth engine” thresholds
    RENT_SANITY_MIN_GROSS_YIELD = 0.04  # 4% annual gross yield => very weak for MI rentals
    RENT_SANITY_PENALTY_MULT = 0.10     # crush it if rent/price is absurd
    PARTIAL_DIMS_PENALTY_MULT = 0.65    # mild penalty if only 1 of beds/baths/sqft missing

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

            # Price filter (and also: missing list price => DROP)
            lp_f = _coerce_float(payload.get("listPrice"))
            if lp_f is None:
                dropped += 1
                drop_reasons["missing_list_price"] += 1
                continue

            if max_price is not None and lp_f > max_price:
                dropped += 1
                drop_reasons["over_max_price"] += 1
                continue

            # Strict type filtering
            raw_type = payload.get("propertyType")
            disallowed, _norm, reason_key = is_disallowed_type(raw_type)
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

            # Dims gate (beds/baths/sqft)
            beds_i = _coerce_int(prop.beds)
            sqft_i = _coerce_int(prop.sqft)
            baths_f = None
            try:
                baths_f = float(prop.baths) if prop.baths is not None else None
            except Exception:
                baths_f = None

            drop_reason = _quality_gate_drop_reason_for_missing_dims(beds_i, baths_f, sqft_i)
            if drop_reason:
                dropped += 1
                drop_reasons[drop_reason] += 1
                continue

            # Create both strategies, but allow strategy-specific gating
            for strat in (Strategy.rental, Strategy.flip):
                # Rental: require we can produce a rent estimate (from beds/sqft in v0)
                if strat == Strategy.rental:
                    rent_guess = estimate_rent(beds_i, sqft_i)
                    if rent_guess is None:
                        dropped += 1
                        drop_reasons["missing_rent_estimate_rental"] += 1
                        continue

                lead, was_created = await create_or_update_lead(
                    session=session,
                    property_id=prop.id,
                    source=LeadSource.rentcast_listing,
                    strategy=strat,
                    source_ref=rl.source_ref,
                    provenance=rl.provenance,
                )

                lead.list_price = lp_f

                await score_lead(session, lead, prop, is_auction=False)

                # If only ONE dim missing, keep but penalize
                missing_dims = sum(
                    [
                        1 if beds_i is None else 0,
                        1 if baths_f is None else 0,
                        1 if sqft_i is None else 0,
                    ]
                )
                if missing_dims == 1:
                    _apply_rank_penalty(lead, PARTIAL_DIMS_PENALTY_MULT, "partial_dims")

                # Rental sanity: punish absurd rent/price
                if strat == Strategy.rental:
                    gy = gross_yield(lead.list_price, lead.rent_estimate)
                    if gy is not None and gy < RENT_SANITY_MIN_GROSS_YIELD:
                        _apply_rank_penalty(lead, RENT_SANITY_PENALTY_MULT, "rent_sanity_bad")
                        drop_reasons["rent_sanity_penalized"] += 1

                created += 1 if was_created else 0
                updated += 0 if was_created else 1

        # --- Wayne auctions
        wayne_leads = await wayne.fetch_by_zip(zipcode, limit=min(100, per_zip_limit))
        for rl in wayne_leads:
            payload = rl.payload or {}
            if _missing_core_fields(payload):
                dropped += 1
                drop_reasons["missing_core_fields"] += 1
                continue

            # Keep the gate anyway (even though Wayne sets single_family)
            raw_type = payload.get("propertyType")
            disallowed, _norm, reason_key = is_disallowed_type(raw_type)
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

            beds_i = _coerce_int(prop.beds)
            sqft_i = _coerce_int(prop.sqft)
            baths_f = None
            try:
                baths_f = float(prop.baths) if prop.baths is not None else None
            except Exception:
                baths_f = None

            drop_reason = _quality_gate_drop_reason_for_missing_dims(beds_i, baths_f, sqft_i)
            if drop_reason:
                dropped += 1
                drop_reasons[drop_reason] += 1
                continue

            # Auctions often won’t have list price/rent; we keep them but penalize via scoring defaults.
            # You can tighten later once you have auction price + ARV comps.
            for strat in (Strategy.rental, Strategy.flip):
                if strat == Strategy.rental:
                    rent_guess = estimate_rent(beds_i, sqft_i)
                    if rent_guess is None:
                        dropped += 1
                        drop_reasons["missing_rent_estimate_rental"] += 1
                        continue

                lead, was_created = await create_or_update_lead(
                    session=session,
                    property_id=prop.id,
                    source=LeadSource.wayne_auction,
                    strategy=strat,
                    source_ref=rl.source_ref,
                    provenance=rl.provenance,
                )
                await score_lead(session, lead, prop, is_auction=True)

                missing_dims = sum(
                    [
                        1 if beds_i is None else 0,
                        1 if baths_f is None else 0,
                        1 if sqft_i is None else 0,
                    ]
                )
                if missing_dims == 1:
                    _apply_rank_penalty(lead, PARTIAL_DIMS_PENALTY_MULT, "partial_dims")

                created += 1 if was_created else 0
                updated += 0 if was_created else 1

    return {
        "created_leads": created,
        "updated_leads": updated,
        "dropped": dropped,
        "drop_reasons": dict(drop_reasons),
        "target_zips": target_zips,
    }
