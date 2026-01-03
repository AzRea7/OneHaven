# app/service_layer/use_cases/refresh.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...connectors.ingestion.base import IngestionAdapter, RawLead
from ...domain.types import Strategy as DomainStrategy, Enrichment
from ...domain.scoring import compute_score, DealInputs
from ...models import Strategy as DbStrategy, LeadSource, EstimateKind
from ...services.estimates import get_or_fetch_estimate
from ...adapters.rentcast_avm import fetch_rent_long_term, fetch_value
from ..unit_of_work import UnitOfWork


@dataclass(frozen=True)
class RefreshResult:
    created_leads: int
    updated_leads: int
    dropped: int
    drop_reasons: dict[str, int]
    phases: dict[str, int]


async def run_refresh(
    uow: UnitOfWork,
    *,
    adapter: IngestionAdapter,
    zips: list[str],
    strategy: DbStrategy,
    per_zip_limit: int = 200,
    max_price: float | None = None,
    ttl_days_rent: int = 45,
    ttl_days_value: int = 60,
) -> RefreshResult:
    drop_reasons: dict[str, int] = {}
    created = updated = dropped = 0

    ingested: list[tuple[Any, Any, RawLead]] = []  # (lead, prop, raw)

    # -------------------------
    # Phase 1: INGEST
    # -------------------------
    for zipcode in zips:
        async for raw in adapter.iter_sale_listings(
            zipcode=zipcode, per_zip_limit=per_zip_limit, max_price=max_price
        ):
            # normalize payload for your existing upsert_property()
            payload = {
                "addressLine": raw.address_line,
                "city": raw.city,
                "state": raw.state,
                "zipCode": raw.zipcode,
                "latitude": raw.lat,
                "longitude": raw.lon,
                "bedrooms": raw.bedrooms,
                "bathrooms": raw.bathrooms,
                "squareFootage": raw.sqft,
                "yearBuilt": raw.year_built,
                "propertyType": raw.property_type,
            }

            # enforce your existing disallowed type rule (kept for now)
            from ...services.normalize import is_disallowed_type
            ok, reason_key, norm_type = is_disallowed_type(raw.property_type)
            if not ok:
                dropped += 1
                drop_reasons[reason_key or "disallowed_type"] = drop_reasons.get(reason_key or "disallowed_type", 0) + 1
                continue

            prop = await uow.repos.upsert_property(payload)
            lead_payload = {
                "source": LeadSource.rentcast_listing if raw.source == "rentcast" else LeadSource.mls_reso,
                "source_ref": raw.source_ref,
                "list_price": raw.list_price,
                "provenance": raw.provenance or {},
            }
            lead = await uow.repos.upsert_lead(prop=prop, payload=lead_payload, strategy=strategy)

            # naive created/updated counts (optional: implement “was_created” later via repo)
            updated += 1
            ingested.append((lead, prop, raw))

    # -------------------------
    # Phase 2: ENRICH (cached)
    # -------------------------
    enriched = 0
    for lead, prop, raw in ingested:
        rent_row = await get_or_fetch_estimate(
            uow.repos.session,
            prop=prop,
            kind=EstimateKind.rent_long_term,
            ttl_days=ttl_days_rent,
            fetcher=fetch_rent_long_term,
        )
        val_row = await get_or_fetch_estimate(
            uow.repos.session,
            prop=prop,
            kind=EstimateKind.value,
            ttl_days=ttl_days_value,
            fetcher=fetch_value,
        )

        lead.rent_estimate = rent_row.value
        lead.rent_source = rent_row.source
        lead.rent_estimated_at = rent_row.fetched_at

        lead.arv_estimate = val_row.value
        lead.arv_source = val_row.source
        lead.arv_estimated_at = val_row.fetched_at
        enriched += 1

    # -------------------------
    # Phase 3: SCORE (domain kernel)
    # -------------------------
    scored = 0
    for lead, prop, raw in ingested:
        enrichment = Enrichment(
            rent_estimate=lead.rent_estimate,
            arv_estimate=lead.arv_estimate,
            rent_source=lead.rent_source,
            arv_source=lead.arv_source,
        )
        score = compute_score(
            strategy=DomainStrategy(strategy.value),
            deal=DealInputs(
                list_price=lead.list_price,
                bedrooms=prop.bedrooms,
                bathrooms=prop.bathrooms,
                sqft=prop.sqft,
            ),
            enrichment=enrichment,
        )
        lead.deal_score = score.deal_score
        lead.motivation_score = score.motivation_score
        lead.rank_score = score.rank_score
        lead.explain_json = score.explain
        scored += 1

    await uow.commit()

    return RefreshResult(
        created_leads=created,
        updated_leads=updated,
        dropped=dropped,
        drop_reasons=drop_reasons,
        phases={"ingested": len(ingested), "enriched": enriched, "scored": scored},
    )
