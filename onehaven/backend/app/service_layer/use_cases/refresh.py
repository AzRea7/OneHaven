# app/service_layer/use_cases/refresh.py
from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from ...config import settings
from ...models import LeadSource, Strategy, EstimateKind
from ...domain.normalize import is_disallowed_type

from ..estimates import get_or_fetch_estimate, EstimateResult, EstimateStats
from ..scoring import score_lead

from ...adapters.repos.properties import PropertyRepository
from ...adapters.repos.leads import LeadRepository

from ...adapters.ingestion.base import IngestionProvider, RawLead
from ...adapters.ingestion.rentcast_listings import RentCastListingsProvider
from ...adapters.ingestion.mls_reso import MlsResoProvider
from ...adapters.ingestion.mls_grid import MlsGridProvider
from ...adapters.ingestion.realcomp_direct import RealcompDirectProvider


SE_MICHIGAN_ZIPS = [
    "48009", "48084", "48301", "48067", "48306", "48304", "48302",
    "48226", "48201", "48362", "48363", "48360", "48359",
    "48348", "48346", "48350",
]


def _coerce_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _first(payload: dict[str, Any], *keys: str) -> Any:
    """Return the first non-empty payload value for any of the provided keys."""
    for k in keys:
        if k in payload and payload.get(k) not in (None, ""):
            return payload.get(k)
    return None


def _build_ingestion_provider() -> IngestionProvider:
    src = settings.INGESTION_SOURCE

    if src == "rentcast_listings":
        return RentCastListingsProvider.from_settings()
    if src == "mls_reso":
        return MlsResoProvider.from_settings()
    if src == "mls_grid":
        return MlsGridProvider.from_settings()
    if src == "realcomp_direct":
        return RealcompDirectProvider.from_settings()

    raise ValueError(f"Unknown INGESTION_SOURCE={src}")


# -------------------------------------------------------------------
# Provider factory (IMPORTANT: tests monkeypatch this symbol)
# -------------------------------------------------------------------
def _provider() -> IngestionProvider:
    return _build_ingestion_provider()


def _missing_core_fields(payload: dict[str, Any]) -> bool:
    # Accept either camelCase or snake_case
    address = _first(payload, "addressLine", "address_line", "address_line1", "address")
    city = _first(payload, "city")
    state = _first(payload, "state")
    zipc = _first(payload, "zipCode", "zipcode", "zip_code")

    return not all([address, city, state, zipc])


# -------------------------------------------------------------------
# Module-level fetchers (IMPORTANT: tests monkeypatch these symbols)
# -------------------------------------------------------------------
async def fetch_value(prop) -> EstimateResult:
    from ...adapters.clients.rentcast_avm import fetch_value as _fetch_value
    return await _fetch_value(prop)


async def fetch_rent_long_term(prop) -> EstimateResult:
    from ...adapters.clients.rentcast_avm import fetch_rent_long_term_avm as _fetch_rent
    return await _fetch_rent(prop)


async def refresh_region_use_case(
    session: AsyncSession,
    *,
    region: str | None = None,
    zips: list[str] | None = None,
    city: str | None = None,
    max_price: float | None = None,
    per_zip_limit: int = 200,
    strategy: Strategy = Strategy.rental,
    ttl_days_rent: int = 45,
    ttl_days_value: int = 60,
) -> dict[str, Any]:
    """
    End-to-end refresh:
      1) INGEST (provider-specific)
      2) ENRICH (EstimateCache: rent + value)
      3) SCORE (enforce rental gating)
    """
    drop_reasons: dict[str, int] = defaultdict(int)
    target_zips = zips or SE_MICHIGAN_ZIPS

    created = updated = dropped = 0
    provider = _provider()

    prop_repo = PropertyRepository(session)
    lead_repo = LeadRepository(session)

    ingested: list[tuple[Any, Any]] = []

    # -------------------------
    # Phase 1: INGEST
    # -------------------------
    raw_leads: list[RawLead] = await provider.fetch(
        region=region,
        zips=target_zips,
        city=city,
        per_zip_limit=per_zip_limit,
    )

    # keep your debug if you want; tests showed you were printing it
    # print(f"DEBUG raw_leads: {len(raw_leads)} provider: {type(provider).__name__}")

    for rl in raw_leads:
        payload = rl.payload or {}

        if _missing_core_fields(payload):
            dropped += 1
            drop_reasons["missing_core"] += 1
            continue

        # list price can be camelCase or snake_case
        lp = _coerce_float(_first(payload, "listPrice", "list_price", "price"))
        if lp is None:
            dropped += 1
            drop_reasons["missing_price"] += 1
            continue

        if max_price is not None and lp > max_price:
            dropped += 1
            drop_reasons["over_max_price"] += 1
            continue

        raw_type = _first(payload, "propertyType", "property_type", "type")
        if raw_type and is_disallowed_type(str(raw_type)):
            dropped += 1
            drop_reasons[f"raw_type::{raw_type}"] += 1
            continue

        # upsert property
        prop = await prop_repo.upsert_from_payload(payload)

        # upsert lead (repo swallows extra kwargs if schema differs)
        lead, was_created = await lead_repo.upsert(
            prop=prop,
            strategy=strategy,
            source=str(rl.source.value) if rl.source else None,
            source_ref=rl.source_ref,
            list_price=lp,
            rent_estimate=None,
            provenance=payload,
        )

        created += int(bool(was_created))
        updated += int(not bool(was_created))
        ingested.append((lead, prop))

    # -------------------------
    # Phase 2: ENRICH (cached)
    # -------------------------
    est_stats = EstimateStats()

    for lead, prop in ingested:
        value_row = await get_or_fetch_estimate(
            session,
            prop=prop,
            kind=EstimateKind.value,
            ttl_days=ttl_days_value,
            fetcher=fetch_value,
            stats=est_stats,
        )
        if value_row.value is not None:
            lead.arv_estimate = float(value_row.value)

        if lead.strategy == Strategy.rental or lead.strategy == Strategy.rental.value:
            rent_row = await get_or_fetch_estimate(
                session,
                prop=prop,
                kind=EstimateKind.rent_long_term,
                ttl_days=ttl_days_rent,
                fetcher=fetch_rent_long_term,
                stats=est_stats,
            )
            if rent_row.value is not None:
                lead.rent_estimate = float(rent_row.value)

    # -------------------------
    # Phase 3: SCORE (hard gate)
    # -------------------------
    for lead, prop in ingested:
        is_rental = lead.strategy == Strategy.rental or lead.strategy == Strategy.rental.value

        # “no theater” rule
        if is_rental and lead.rent_estimate is None:
            drop_reasons["missing_rent_enrichment"] += 1
            lead.deal_score = 0.0
            lead.motivation_score = 0.0
            lead.rank_score = 0.0
            lead.explain_json = "blocked: missing rent_estimate (rental strategy)"
            continue

        await score_lead(session, lead, prop, is_auction=False)

    return {
        "created_leads": created,
        "updated_leads": updated,
        "dropped": dropped,
        "drop_reasons": dict(drop_reasons),
        "target_zips": target_zips,
        "strategy": strategy.value if hasattr(strategy, "value") else str(strategy),
        "phases": {"ingested": len(ingested), "enriched": len(ingested), "scored": len(ingested)},
        "ingestion_source": settings.INGESTION_SOURCE,
        "estimate_cache": est_stats.snapshot(),
    }
