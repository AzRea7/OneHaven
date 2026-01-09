# app/service_layer/use_cases/refresh.py
from __future__ import annotations

from collections import defaultdict
from typing import Any

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


def _missing_core_fields(payload: dict[str, Any]) -> bool:
    return not all(payload.get(k) for k in ("addressLine", "city", "state", "zipCode"))


# -------------------------------------------------------------------
# Provider indirection (IMPORTANT: tests monkeypatch this symbol)
# -------------------------------------------------------------------
def _provider() -> IngestionProvider:
    return _build_ingestion_provider()


# -------------------------------------------------------------------
# Module-level fetchers (IMPORTANT: tests monkeypatch these symbols)
# -------------------------------------------------------------------
async def fetch_value(prop) -> EstimateResult:
    from ...adapters.clients.rentcast_avm import fetch_value as _fetch_value
    return await _fetch_value(prop)


async def fetch_rent_long_term(prop) -> EstimateResult:
    from ...adapters.clients.rentcast_avm import fetch_rent_long_term_avm as _fetch_rent
    return await _fetch_rent(prop)


def _eval_disallowed_property_type(raw_type: Any) -> tuple[bool, str | None, str | None]:
    """
    Compatibility wrapper because is_disallowed_type used to return bool, and now returns a tuple:
      (disallowed: bool, norm_type: str|None, reason_key: str|None)

    Returns: (disallowed, norm_type, reason_key)
    """
    if raw_type is None:
        return (False, None, None)

    res = is_disallowed_type(str(raw_type))

    # New contract: tuple
    if isinstance(res, tuple) and len(res) == 3:
        disallowed, norm_type, reason_key = res
        return (bool(disallowed), norm_type, reason_key)

    # Old contract: bool
    return (bool(res), None, None)


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

        if max_price is not None and lp > max_price:
            dropped += 1
            drop_reasons["over_max_price"] += 1
            continue

        raw_type = payload.get("propertyType")
        disallowed, norm_type, reason_key = _eval_disallowed_property_type(raw_type)
        if disallowed:
            dropped += 1
            # Prefer the reason_key from normalize (it’s designed for drop_reasons counters)
            drop_reasons[reason_key or f"raw_type::{raw_type}"] += 1
            continue

        # Optional: if normalize produced a stable normalized type, persist it through ingestion
        # without destroying the original payload structure too much.
        if norm_type:
            payload = dict(payload)
            payload["propertyTypeNorm"] = norm_type

        prop = await prop_repo.upsert_from_payload(payload)

        lead, was_created = await lead_repo.upsert(
            prop=prop,
            strategy=strategy,
            source=str(rl.source.value) if rl.source else None,
            source_ref=rl.source_ref,
            list_price=lp,
            rent_estimate=None,
            provenance=payload,
        )

        created += int(was_created)
        updated += int(not was_created)
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

        if lead.strategy == Strategy.rental:
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
        # “no theater” rule
        if lead.strategy == Strategy.rental and lead.rent_estimate is None:
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
        "strategy": strategy.value,
        "phases": {"ingested": len(ingested), "enriched": len(ingested), "scored": len(ingested)},
        "ingestion_source": settings.INGESTION_SOURCE,
        "estimate_cache": est_stats.snapshot(),
    }
