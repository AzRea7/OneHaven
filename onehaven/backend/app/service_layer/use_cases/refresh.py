# app/service_layer/use_cases/refresh.py
from __future__ import annotations

from collections import defaultdict
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ...config import settings
from ...models import LeadSource, Strategy, EstimateKind
from ...domain.normalize import is_disallowed_type

from ..estimates import get_or_fetch_estimate, EstimateResult
from ..scoring import score_lead

from ...adapters.repos.properties import PropertyRepository
from ...adapters.repos.leads import LeadRepository

from ...adapters.ingestion.base import IngestionProvider, RawLead
from ...adapters.ingestion.rentcast_listings import RentCastListingsProvider
from ...adapters.ingestion.mls_reso import MlsResoProvider


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


def _missing_core_fields(payload: dict[str, Any]) -> bool:
    # strict: canonical identity fields must exist
    return not all(payload.get(k) for k in ("addressLine", "city", "state", "zipCode"))


def _provider() -> IngestionProvider:
    """
    Feature-flagged ingestion. The refresh pipeline does not care
    whether listings come from RentCast, MLS RESO, or future sources.
    """
    if settings.INGESTION_SOURCE == "mls_reso":
        return MlsResoProvider()
    return RentCastListingsProvider()


# -------------------------------------------------------------------
# Module-level fetchers (IMPORTANT: tests monkeypatch these symbols)
# -------------------------------------------------------------------
async def fetch_value(prop) -> EstimateResult:
    """
    Fetch value estimate (AVM).
    Tests will monkeypatch refresh_uc.fetch_value to force deterministic behavior.
    """
    from ...adapters.clients.rentcast_avm import fetch_value_avm
    return await fetch_value_avm(prop)


async def fetch_rent_long_term(prop) -> EstimateResult:
    """
    Fetch long-term rent estimate (AVM).
    Tests will monkeypatch refresh_uc.fetch_rent_long_term to force deterministic behavior.
    """
    from ...adapters.clients.rentcast_avm import fetch_rent_long_term_avm
    return await fetch_rent_long_term_avm(prop)


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
    Canonical 3-phase refresh:
      1) ingest   -> normalize -> upsert property + lead (sale truth only)
      2) enrich   -> rent/value via EstimateCache (property anchored)
      3) score    -> deterministic scoring; rentals hard-gated on rent

    External APIs only happen in Phase 2.
    """
    drop_reasons: dict[str, int] = defaultdict(int)
    target_zips = zips or SE_MICHIGAN_ZIPS

    created = updated = dropped = 0
    provider = _provider()

    prop_repo = PropertyRepository(session)
    lead_repo = LeadRepository(session)

    # -------------------------
    # Phase 1: INGEST
    # -------------------------
    ingested: list[tuple[Any, Any]] = []

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
        disallowed, norm_type, reason_key = is_disallowed_type(raw_type)
        if disallowed:
            dropped += 1
            drop_reasons["disallowed_type"] += 1
            if reason_key:
                drop_reasons[reason_key] += 1
            if norm_type:
                drop_reasons[f"norm_type::{norm_type}"] += 1
            continue

        # Property identity anchor
        prop = await prop_repo.upsert_from_payload(payload)

        # Lead is sale-truth only at ingest: enrichment owns rent/value
        lead, was_created = await lead_repo.upsert(
            prop=prop,
            strategy=strategy,
            source=rl.source or LeadSource.rentcast_listing,
            source_ref=rl.source_ref or None,
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
    for lead, prop in ingested:
        value_row = await get_or_fetch_estimate(
            session,
            prop=prop,
            kind=EstimateKind.value,
            ttl_days=ttl_days_value,
            fetcher=fetch_value,  # module-level symbol (monkeypatchable)
        )
        if value_row.value is not None:
            lead.arv_estimate = float(value_row.value)

        if lead.strategy == Strategy.rental:
            rent_row = await get_or_fetch_estimate(
                session,
                prop=prop,
                kind=EstimateKind.rent_long_term,
                ttl_days=ttl_days_rent,
                fetcher=fetch_rent_long_term,  # module-level symbol (monkeypatchable)
            )
            if rent_row.value is not None:
                lead.rent_estimate = float(rent_row.value)

    # -------------------------
    # Phase 3: SCORE (hard gate)
    # -------------------------
    for lead, prop in ingested:
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
    }
