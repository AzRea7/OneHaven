# app/service_layer/use_cases/refresh.py
from __future__ import annotations

from collections import defaultdict
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ...config import settings
from ...models import EstimateKind, Strategy
from ...domain.normalize import is_disallowed_type
from ..estimates import EstimateStats, get_or_fetch_estimate
from ..scoring import score_lead

from ...adapters.repos.properties import PropertyRepository
from ...adapters.repos.leads import LeadRepository

from ...adapters.ingestion.base import IngestionProvider, RawLead
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

    # ✅ RentCast is not an option for you right now.
    if src == "rentcast_listings":
        raise ValueError(
            "INGESTION_SOURCE=rentcast_listings is disabled (no RentCast access). "
            "Use INGESTION_SOURCE=realcomp_direct, mls_reso, mls_grid, or stub_json."
        )

    if src == "mls_reso":
        return MlsResoProvider.from_settings()
    if src == "mls_grid":
        return MlsGridProvider.from_settings()
    if src == "realcomp_direct":
        return RealcompDirectProvider.from_settings()

    # ✅ Offline: run the pipeline using local sample payloads
    if src == "stub_json":
        from ...adapters.ingestion.stub_json import StubJsonProvider
        return StubJsonProvider.from_settings()

    raise ValueError(f"Unknown INGESTION_SOURCE={src}")


def _missing_core_fields(payload: dict[str, Any]) -> bool:
    # Align with your canonical payload format
    return not all(payload.get(k) for k in ("addressLine", "city", "state", "zipCode", "listPrice"))


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
    drop_reasons: dict[str, int] = defaultdict(int)
    target_zips = zips or SE_MICHIGAN_ZIPS

    created = updated = dropped = 0
    provider = _build_ingestion_provider()

    prop_repo = PropertyRepository(session)
    lead_repo = LeadRepository(session)

    # -------------------------
    # Phase 1: INGEST
    # -------------------------
    raw_leads: list[RawLead] = await provider.fetch(
        region=region,
        zips=target_zips,
        city=city,
        per_zip_limit=per_zip_limit,
    )

    ingested: list[tuple[Any, dict[str, Any]]] = []

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
        if raw_type is not None:
            res = is_disallowed_type(str(raw_type))
            # some versions return bool, some return (bool, norm, reason)
            if isinstance(res, tuple):
                disallowed = bool(res[0])
                norm_type = res[1]
                reason_key = res[2]
            else:
                disallowed = bool(res)
                norm_type = None
                reason_key = None

            if disallowed:
                dropped += 1
                drop_reasons[f"raw_type::{raw_type}"] += 1
                if norm_type:
                    drop_reasons[f"norm_type::{norm_type}"] += 1
                if reason_key:
                    drop_reasons[reason_key] += 1
                continue

        prop, was_created = await prop_repo.upsert_from_payload(payload)
        if was_created:
            created += 1
        else:
            updated += 1

        ingested.append((prop, payload))

    # -------------------------
    # Phase 2: ENRICH (A: cache)
    # -------------------------
    est_stats = EstimateStats()

    async def fetch_value(prop) -> Any:
        # ✅ Adapter-only local model
        from ...adapters.ml_models.local_fallback import predict_local_value
        return await predict_local_value(prop)

    async def fetch_rent(prop) -> Any:
        from ...adapters.ml_models.local_fallback import predict_local_rent_long_term
        return await predict_local_rent_long_term(prop)

    for prop, payload in ingested:
        rent_row = await get_or_fetch_estimate(
            session,
            prop=prop,
            kind=EstimateKind.rent_long_term,
            ttl_days=ttl_days_rent,
            fetcher=fetch_rent,
            stats=est_stats,
        )
        value_row = await get_or_fetch_estimate(
            session,
            prop=prop,
            kind=EstimateKind.value,
            ttl_days=ttl_days_value,
            fetcher=fetch_value,
            stats=est_stats,
        )

        rent_p50 = rent_row.p50 if rent_row.p50 is not None else rent_row.value
        value_p50 = value_row.p50 if value_row.p50 is not None else value_row.value

        # -------------------------
        # Phase 3: SCORE + UPSERT LEAD
        # -------------------------
        score = score_lead(prop=prop, rent=rent_p50, value=value_p50, strategy=strategy)

        # Keep your existing repo contract: upsert(...). Don’t invent new methods.
        await lead_repo.upsert(
            prop=prop,
            strategy=strategy,
            list_price=_coerce_float(payload.get("listPrice")),
            score=score.rank_score,
            reasons_json=(score.reasons_json if hasattr(score, "reasons_json") else None),
            raw_json=None,
            source=str(settings.INGESTION_SOURCE),
            source_ref=str(payload.get("listingId") or payload.get("ListingKey") or payload.get("id") or ""),
        )

    return {
        "created_leads": created,
        "updated_leads": updated,
        "dropped": dropped,
        "drop_reasons": dict(drop_reasons),
        "target_zips": target_zips,
        "estimate_cache": est_stats.snapshot(),
    }
