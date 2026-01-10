# app/service_layer/use_cases/refresh.py
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import inspect
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
from ...adapters.ingestion.stub_json import StubJsonProvider


SE_MICHIGAN_ZIPS = [
    "48009", "48084", "48301", "48067", "48306", "48304", "48302",
    "48226", "48201", "48362", "48363", "48360", "48359",
    "48348", "48346", "48350",
]


@dataclass
class RefreshResult:
    created_leads: int
    updated_leads: int
    dropped: int
    drop_reasons: dict[str, int]
    target_zips: list[str]


def _coerce_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _build_ingestion_provider() -> IngestionProvider:
    """
    Provider builder that will NOT brick local dev.

    - RentCast is disabled (no access) -> always fall back to stub_json
    - Unknown sources -> stub_json in dev/local/test, error in prod-like
    """
    src = (settings.INGESTION_SOURCE or "").strip()

    # Never allow RentCast to brick dev
    if src == "rentcast_listings":
        src = "stub_json"

    if src == "stub_json":
        return StubJsonProvider.from_settings()
    if src == "realcomp_direct":
        return RealcompDirectProvider.from_settings()
    if src == "mls_reso":
        return MlsResoProvider.from_settings()
    if src == "mls_grid":
        return MlsGridProvider.from_settings()

    if settings.ENV.lower() in ("dev", "local", "test"):
        return StubJsonProvider.from_settings()

    raise ValueError(
        f"Unknown INGESTION_SOURCE={src!r}. Use realcomp_direct, mls_reso, mls_grid, or stub_json."
    )


def _missing_core_fields(payload: dict[str, Any]) -> bool:
    # Canonical payload contract expected by PropertyRepository.upsert_from_payload
    return not all(payload.get(k) for k in ("addressLine", "city", "state", "zipCode", "listPrice"))


def _normalize_upsert_result(res: Any) -> tuple[Any, bool | None]:
    """
    PropertyRepository.upsert_from_payload historically returned either:
      - (prop, was_created: bool)
      - prop
    Support both.
    """
    if isinstance(res, tuple) and len(res) == 2:
        return res[0], bool(res[1])
    return res, None


async def _maybe_await(x: Any) -> Any:
    if hasattr(x, "__await__"):
        return await x
    return x


async def _call_score_lead(
    *,
    session: AsyncSession,
    lead: Any,
    prop: Any,
    rent: float | None,
    value: float | None,
    strategy: Strategy,
) -> Any:
    """
    Your scoring layer has had a couple signature variants across branches.

    Newer:  score_lead(session, lead, prop=..., rent=..., value=..., strategy=...)
            (and it MUTATES lead, often returning None)

    Older:  score_lead(prop=..., rent=..., value=..., strategy=...)
            (returns a score object)

    We detect which one is present and do the right thing.

    IMPORTANT:
      - If score_lead mutates lead and returns None, we will read lead.rank_score
        after calling it.
    """
    sig = inspect.signature(score_lead)
    params = list(sig.parameters.keys())

    # Newer signature: (session, lead, ...)
    if len(params) >= 2 and params[0] == "session" and params[1] == "lead":
        out = score_lead(session, lead, prop=prop, rent=rent, value=value, strategy=strategy)
        await _maybe_await(out)
        # scoring likely mutated lead in-place; return the lead as the "score carrier"
        return lead

    # Older signature: score object returned
    out = score_lead(prop=prop, rent=rent, value=value, strategy=strategy)
    return await _maybe_await(out)


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

      1) Ingest raw listings (provider)
      2) Canonicalize + upsert Property records
      3) Fetch/compute estimates (A: local->vendor->cache, but vendor disabled now)
      4) Upsert Lead + run scoring (score_lead mutates lead.rank_score/deal_score/etc)
    """
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

    ingested: list[tuple[Any, dict[str, Any], RawLead]] = []

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
                    drop_reasons[str(reason_key)] += 1
                continue

        upsert_res = await prop_repo.upsert_from_payload(payload)
        prop, was_created = _normalize_upsert_result(upsert_res)

        if was_created is True:
            created += 1
        elif was_created is False:
            updated += 1

        ingested.append((prop, payload, rl))

    # -------------------------
    # Phase 2: ENRICH (cache p10/p50/p90)
    # -------------------------
    est_stats = EstimateStats()

    # NOTE: get_or_fetch_estimate() expects fetcher to be awaitable; keep these async.
    async def fetch_value(prop: Any) -> Any:
        from ...adapters.ml_models.local_fallback import predict_local_value
        return await _maybe_await(predict_local_value(prop))

    async def fetch_rent(prop: Any) -> Any:
        from ...adapters.ml_models.local_fallback import predict_local_rent_long_term
        return await _maybe_await(predict_local_rent_long_term(prop))

    # -------------------------
    # Phase 3: SCORE + UPSERT LEADS
    # -------------------------
    for prop, payload, rl in ingested:
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

        rent_p50 = rent_row.p50 if getattr(rent_row, "p50", None) is not None else getattr(rent_row, "value", None)
        value_p50 = value_row.p50 if getattr(value_row, "p50", None) is not None else getattr(value_row, "value", None)

        list_price = _coerce_float(payload.get("listPrice"))
        source_ref = str(payload.get("listingId") or payload.get("ListingKey") or payload.get("id") or "")
        source = getattr(rl, "source", None)
        source_str = str(getattr(source, "value", source) or settings.INGESTION_SOURCE)

        # ✅ Create/ensure the Lead row exists (do NOT pass score= into ORM constructor)
        lead, _ = await lead_repo.upsert(
            prop=prop,
            strategy=strategy,
            list_price=list_price,
            source=source_str,
            source_ref=source_ref,
            reasons_json=None,
            raw_json=None,
        )

        # ✅ Score it (may mutate lead, or may return a score object depending on branch)
        score_carrier = await _call_score_lead(
            session=session,
            lead=lead,
            prop=prop,
            rent=rent_p50,
            value=value_p50,
            strategy=strategy,
        )

        # ✅ Persist rank_score/reasons_json after scoring.
        #    If score_lead mutated lead, score_carrier is lead.
        rank_score = getattr(score_carrier, "rank_score", None)
        reasons_json = getattr(score_carrier, "reasons_json", None)

        # Upsert again to persist rank_score (repo maps `score=` to rank_score for back-compat)
        await lead_repo.upsert(
            prop=prop,
            strategy=strategy,
            list_price=list_price,
            score=rank_score,  # treated as rank_score
            reasons_json=reasons_json,
            raw_json=None,
            source=source_str,
            source_ref=source_ref,
        )

    return {
        "created_leads": created,
        "updated_leads": updated,
        "dropped": dropped,
        "drop_reasons": dict(drop_reasons),
        "target_zips": target_zips,
        "estimate_cache": est_stats.snapshot(),
    }
