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

    # RentCast is intentionally not assumed available.
    if src == "rentcast_listings":
        raise ValueError(
            "INGESTION_SOURCE=rentcast_listings is disabled in this setup (no RentCast access). "
            "Use INGESTION_SOURCE=realcomp_direct (recommended), mls_reso, or mls_grid."
        )

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
# A) Local model fallback -> else vendor -> cache percentiles
# -------------------------------------------------------------------
async def fetch_value(prop) -> EstimateResult:
    # 1) local model first (pure adapter)
    from ...adapters.ml_models.local_fallback import predict_local_value

    local = await predict_local_value(prop)
    # local.raw already contains p10/p50/p90; we also pass them into EstimateResult fields
    if local.value is not None and isinstance(local.raw, dict):
        return EstimateResult(
            value=local.value,
            source=local.source,
            raw=local.raw,
            p10=local.raw.get("p10"),
            p50=local.raw.get("p50"),
            p90=local.raw.get("p90"),
        )

    # 2) optional vendor fallback (only if configured)
    if settings.RENTCAST_API_KEY:
        from ...adapters.clients.rentcast_avm import fetch_value as vendor_fetch_value

        v = await vendor_fetch_value(prop)
        # vendor may not have percentiles; store value as p50
        return EstimateResult(value=v.value, source=v.source, raw=v.raw, p50=v.value)

    # 3) no vendor configured
    return EstimateResult(
        value=None,
        source="local_model:no_signal",
        raw=local.raw,
    )


async def fetch_rent_long_term(prop) -> EstimateResult:
    from ...adapters.ml_models.local_fallback import predict_local_rent_long_term

    local = await predict_local_rent_long_term(prop)
    if local.value is not None and isinstance(local.raw, dict):
        return EstimateResult(
            value=local.value,
            source=local.source,
            raw=local.raw,
            p10=local.raw.get("p10"),
            p50=local.raw.get("p50"),
            p90=local.raw.get("p90"),
        )

    if settings.RENTCAST_API_KEY:
        from ...adapters.clients.rentcast_avm import fetch_rent_long_term_avm as vendor_fetch_rent

        v = await vendor_fetch_rent(prop)
        return EstimateResult(value=v.value, source=v.source, raw=v.raw, p50=v.value)

    return EstimateResult(
        value=None,
        source="local_model:no_signal",
        raw=local.raw,
    )


def _eval_disallowed_property_type(raw_type: Any) -> tuple[bool, str | None, str | None]:
    if raw_type is None:
        return (False, None, None)

    res = is_disallowed_type(str(raw_type))

    if isinstance(res, tuple) and len(res) == 3:
        disallowed, norm_type, reason_key = res
        return (bool(disallowed), norm_type, reason_key)

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
            # keep both raw + normalized reason keys for analysis
            if raw_type is not None:
                drop_reasons[f"raw_type::{raw_type}"] += 1
            if norm_type is not None:
                drop_reasons[f"norm_type::{norm_type}"] += 1
            if reason_key:
                drop_reasons[reason_key] += 1
            continue

        prop, was_created = await prop_repo.upsert_from_payload(payload)
        if was_created:
            created += 1
        else:
            updated += 1

        ingested.append((prop, rl))

    # -------------------------
    # Phase 2: ENRICH (cache)
    # -------------------------
    est_stats = EstimateStats()

    for prop, rl in ingested:
        rent_row = await get_or_fetch_estimate(
            session,
            prop=prop,
            kind=EstimateKind.rent_long_term,
            ttl_days=ttl_days_rent,
            fetcher=fetch_rent_long_term,
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

        # -------------------------
        # Phase 3: SCORE
        # -------------------------
        score = score_lead(prop=prop, rent=rento(rent_row), value=valueo(value_row), strategy=strategy)

        await lead_repo.upsert_lead(
            property_id=prop.id,
            strategy=strategy,
            source=LeadSource.realcomp if settings.INGESTION_SOURCE == "realcomp_direct" else LeadSource.mls,
            lead_score=score.rank_score,
            reasons=score.reasons,
        )

    return {
        "created_leads": created,
        "updated_leads": updated,
        "dropped": dropped,
        "drop_reasons": dict(drop_reasons),
        "target_zips": target_zips,
        "estimate_cache": est_stats.snapshot(),
    }


def rento(row):
    # helper: tolerate missing value/p50
    return getattr(row, "p50", None) if getattr(row, "p50", None) is not None else getattr(row, "value", None)


def valueo(row):
    return getattr(row, "p50", None) if getattr(row, "p50", None) is not None else getattr(row, "value", None)
