# app/jobs/refresh.py
from collections import defaultdict
from sqlalchemy.ext.asyncio import AsyncSession

from ..connectors.rentcast import RentCastConnector
from ..connectors.wayne_auction import WayneAuctionConnector
from ..models import LeadSource, Strategy
from ..services.ingest import upsert_property, create_or_update_lead, score_lead
from ..services.normalize import normalize_property_type

SE_MICHIGAN_ZIPS = ["48009", "48084", "48301", "48067", "48306", "48304", "48302", "48226", "48201"]


def _missing_core_fields(payload: dict) -> bool:
    addr = payload.get("address") or payload.get("addressLine") or payload.get("street") or ""
    city = payload.get("city") or ""
    zipc = payload.get("zip") or payload.get("zipcode") or payload.get("zipCode") or ""
    return not (addr and city and zipc)


def _raw_type(payload: dict) -> str | None:
    return payload.get("propertyType") or payload.get("property_type") or payload.get("PropertyType")


def _list_price(payload: dict) -> float | None:
    v = payload.get("listPrice") or payload.get("price") or payload.get("listingPrice")
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


async def refresh_region(session: AsyncSession, region: str, max_price: float | None = None) -> dict:
    drop_reasons = defaultdict(int)
    drop_by_raw_type = defaultdict(int)
    drop_by_norm_type = defaultdict(int)

    created = 0
    updated = 0
    dropped = 0

    rentcast = RentCastConnector()
    wayne = WayneAuctionConnector()

    zips = SE_MICHIGAN_ZIPS if region == "se_michigan" else SE_MICHIGAN_ZIPS

    for z in zips:
        # 1) On-market listings
        listings = await rentcast.fetch_listings(zipcode=z, limit=200)
        for raw in listings:
            if _missing_core_fields(raw.payload):
                dropped += 1
                drop_reasons["missing_address_city_zip"] += 1
                continue

            lp = _list_price(raw.payload)
            if max_price is not None and lp is not None and lp > max_price:
                dropped += 1
                drop_reasons["above_max_price"] += 1
                continue

            prop = await upsert_property(session, raw.payload)
            if not prop:
                dropped += 1
                drop_reasons["invalid_or_disallowed_property"] += 1
                rt = _raw_type(raw.payload)
                if rt:
                    drop_by_raw_type[str(rt)] += 1
                    nt = normalize_property_type(str(rt))
                    if nt:
                        drop_by_norm_type[str(nt)] += 1
                continue

            lead, was_created = await create_or_update_lead(
                session=session,
                property_id=prop.id,
                source=LeadSource.rentcast_listing,
                strategy=Strategy.rental,
                source_ref=raw.source_ref,
                provenance=raw.provenance,
            )

            if lp is not None:
                lead.list_price = lp

            await score_lead(session, lead, prop, is_auction=False)

            created += 1 if was_created else 0
            updated += 0 if was_created else 1

        # 2) Auctions (usually no list_price; max_price doesn’t apply unless you enrich later)
        auctions = await wayne.fetch_by_zip(zipcode=z, limit=200)
        for raw in auctions:
            if _missing_core_fields(raw.payload):
                dropped += 1
                drop_reasons["missing_address_city_zip"] += 1
                continue

            prop = await upsert_property(session, raw.payload)
            if not prop:
                dropped += 1
                drop_reasons["invalid_or_disallowed_property"] += 1
                rt = _raw_type(raw.payload)
                if rt:
                    drop_by_raw_type[str(rt)] += 1
                    nt = normalize_property_type(str(rt))
                    if nt:
                        drop_by_norm_type[str(nt)] += 1
                continue

            lead, was_created = await create_or_update_lead(
                session=session,
                property_id=prop.id,
                source=LeadSource.wayne_auction,
                strategy=Strategy.flip,
                source_ref=raw.source_ref,
                provenance=raw.provenance,
            )

            await score_lead(session, lead, prop, is_auction=True)

            created += 1 if was_created else 0
            updated += 0 if was_created else 1

    for k, v in drop_by_raw_type.items():
        drop_reasons[f"raw_type::{k}"] += v
    for k, v in drop_by_norm_type.items():
        drop_reasons[f"norm_type::{k}"] += v

    return {
        "created_leads": created,
        "updated_leads": updated,
        "dropped": dropped,
        "drop_reasons": dict(drop_reasons),
    }
